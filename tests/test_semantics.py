"""窗口语义：确定性排序、并列、缺失值、帧边界、两次执行完全一致。"""
from __future__ import annotations

import os

from windowed.types import (
    AggFunc,
    EngineConfig,
    FrameMode,
    FrameSpec,
    LeadLagKind,
    LeadLagSpec,
    QuerySpec,
    RankFunc,
    RowIn,
)

from .conftest import run_sync

ALL_AGGS = tuple(AggFunc)


def _naive_window(vals, i, mode, n):
    if mode == FrameMode.ALL:
        ww = list(vals)
    elif mode == FrameMode.PRECEDING:
        ww = vals[max(0, i - n): i + 1]
    else:
        ww = vals[max(0, i - n): i + n + 1]
    w = [v for v in ww if v is not None]
    return {
        "frame_rows": len(ww),
        "sum": sum(w) if w else None,
        "count": len(w),
        "avg": sum(w) / len(w) if w else None,
        "min": min(w) if w else None,
        "max": max(w) if w else None,
    }


def test_sort_order_nulls_numbers_strings_then_seq(services):
    storage, engine, _ = services
    # 写入顺序故意打乱；并列键按写入顺序(seq)决胜
    storage.commit_batch("ds", None, [
        RowIn("p", "b", 5.0),
        RowIn("p", 2, 2.5),
        RowIn("p", None, 9.0),
        RowIn("p", 1, 1.0),
        RowIn("p", 2, 2.6),       # 与上一行 sort_key 并列
        RowIn("p", None, 8.0),    # 两个 NULL 排序键并列，按 seq
        RowIn("p", "a", 4.0),
    ])
    _, rows = run_sync(engine, storage,
                       QuerySpec("ds", "p", rankings=(RankFunc.ROW_NUMBER,)))
    keys = [r["sort_key"] for r in rows]
    vals = [r["value"] for r in rows]
    assert keys == [None, None, 1, 2, 2, "a", "b"]
    assert vals == [9.0, 8.0, 1.0, 2.5, 2.6, 4.0, 5.0]
    assert [r["row_number"] for r in rows] == [1, 2, 3, 4, 5, 6, 7]


def test_rank_ties_and_gaps(services):
    storage, engine, _ = services
    storage.commit_batch("ds", None, [
        RowIn("p", 1, 1.0), RowIn("p", 1, 2.0),
        RowIn("p", 2, 3.0),
        RowIn("p", 3, 4.0), RowIn("p", 3, 5.0),
    ])
    _, rows = run_sync(engine, storage, QuerySpec(
        "ds", "p",
        rankings=(RankFunc.ROW_NUMBER, RankFunc.RANK, RankFunc.DENSE_RANK)))
    assert [r["row_number"] for r in rows] == [1, 2, 3, 4, 5]
    assert [r["rank"] for r in rows] == [1, 1, 3, 4, 4]
    assert [r["dense_rank"] for r in rows] == [1, 1, 2, 3, 3]


def test_null_values_in_aggregates(services):
    storage, engine, _ = services
    storage.commit_batch("ds", None, [
        RowIn("p", 1, None), RowIn("p", 2, 10.0), RowIn("p", 3, None),
    ])
    _, rows = run_sync(engine, storage, QuerySpec("ds", "p", aggregates=ALL_AGGS))
    for r in rows:
        assert r["frame_rows"] == 3          # 窗口行数计入 NULL
        assert r["count"] == 1               # count 只数非 NULL
        assert r["sum"] == 10.0
        assert r["avg"] == 10.0
        assert r["min"] == 10.0 and r["max"] == 10.0
    # 全 NULL 分区：sum/avg/min/max 为 None，count 为 0
    storage.commit_batch("ds2", None, [RowIn("q", 1, None), RowIn("q", 2, None)])
    _, rows = run_sync(engine, storage, QuerySpec("ds2", "q", aggregates=ALL_AGGS))
    for r in rows:
        assert r["count"] == 0 and r["sum"] is None and r["avg"] is None
        assert r["min"] is None and r["max"] is None and r["frame_rows"] == 2


def test_lead_lag_nulls_defaults_and_boundaries(services):
    storage, engine, _ = services
    storage.commit_batch("ds", None, [
        RowIn("p", 1, None), RowIn("p", 2, 20.0), RowIn("p", 3, None),
    ])
    _, rows = run_sync(engine, storage, QuerySpec("ds", "p", lead_lag=(
        LeadLagSpec(LeadLagKind.LAG, 1),
        LeadLagSpec(LeadLagKind.LAG, 2, default=-1.0),
        LeadLagSpec(LeadLagKind.LEAD, 1),
        LeadLagSpec(LeadLagKind.LEAD, 2, default=-2.0),
    )))
    assert [r["lag_1"] for r in rows] == [None, None, 20.0]     # NULL 是真实取值
    assert [r["lag_2"] for r in rows] == [-1.0, -1.0, None]    # 越界给默认值，值为 NULL 保持 NULL
    assert [r["lead_1"] for r in rows] == [20.0, None, None]
    assert [r["lead_2"] for r in rows] == [None, -2.0, -2.0]


def test_frame_boundaries_match_naive(services):
    storage, engine, _ = services
    vals_in = [None, 1.0, 2.0, None, 5.0, -3.0, 2.0]
    storage.commit_batch("ds", None,
                         [RowIn("p", i, v) for i, v in enumerate(vals_in)])
    for mode, n in ((FrameMode.ALL, 0), (FrameMode.PRECEDING, 0),
                    (FrameMode.PRECEDING, 2), (FrameMode.PRECEDING, 100),
                    (FrameMode.AROUND, 0), (FrameMode.AROUND, 1),
                    (FrameMode.AROUND, 3), (FrameMode.AROUND, 100)):
        spec = QuerySpec("ds", "p", frame=FrameSpec(mode, n), aggregates=ALL_AGGS)
        _, rows = run_sync(engine, storage, spec)
        assert len(rows) == len(vals_in)
        for i, r in enumerate(rows):
            exp = _naive_window(vals_in, i, mode, n)
            for k, v in exp.items():
                if isinstance(v, float):
                    assert abs(r[k] - v) < 1e-12, (mode, n, i, k, r[k], v)
                else:
                    assert r[k] == v, (mode, n, i, k, r[k], v)


def test_determinism_same_input_same_output(tmpdir_path):
    """两次独立运行（不同库、相同输入、相同 batch_id）必须字节级一致。"""
    import json
    import time
    from windowed.engine import QueryEngine
    from windowed.storage import Storage

    def build_and_run(path):
        cfg = EngineConfig(db_path=path, batch_size=7)
        st = Storage(cfg)
        en = QueryEngine(st, cfg)
        rows = [RowIn("p", k, v) for k, v in
                [(2, 1.0), (1, None), (2, 2.0), (0, 9.0),
                 ("x", 3.0), (2, None), (1, 7.0)]]
        st.commit_batch("ds", "fixed-batch", rows)
        spec = QuerySpec(
            "ds", "p", frame=FrameSpec(FrameMode.AROUND, 2),
            aggregates=ALL_AGGS,
            rankings=(RankFunc.ROW_NUMBER, RankFunc.RANK, RankFunc.DENSE_RANK),
            lead_lag=(LeadLagSpec(LeadLagKind.LAG, 1),
                      LeadLagSpec(LeadLagKind.LEAD, 1)),
        )
        qid = en.submit(spec)
        for _ in range(2000):
            info = en.get_info(qid)
            if info.status.value in ("done", "failed"):
                break
            time.sleep(0.005)
        assert info.status.value == "done", info
        raw = st.read_results(qid, 0, 100000)
        en.shutdown()
        st.close()
        return raw

    raw1 = build_and_run(os.path.join(tmpdir_path, "a.db"))
    raw2 = build_and_run(os.path.join(tmpdir_path, "b.db"))
    assert raw1 == raw2
    parsed = [json.loads(p) for p in raw1]
    assert [r["sort_key"] for r in parsed] == [0, 1, 1, 2, 2, 2, "x"]
