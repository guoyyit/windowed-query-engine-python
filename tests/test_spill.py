"""大分区、磁盘溢出与有界内存测试。"""
from __future__ import annotations

import os
import random
import sqlite3
import tracemalloc

from windowed.engine import QueryEngine
from windowed.storage import Storage
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

from .conftest import make_services, run_sync, wait_terminal
from windowed.types import QueryStatus

ALL_AGGS = tuple(AggFunc)


def _random_rows(n, seed=7):
    random.seed(seed)
    out = []
    for i in range(n):
        sk = random.choice([None, 0, 1, 1, 2, 3, 3, "a", "b"])
        val = random.choice([None, 0.0, 1.0, 5.0, -3.0, 10.0, 7.0])
        out.append(RowIn("p", sk, val))
    return out


def _run_raw(storage, engine, spec, n):
    qid = engine.submit(spec)
    info = wait_terminal(engine, qid)
    assert info.status == QueryStatus.DONE, (info.status, info.error)
    return storage.read_results(qid, 0, n + 10)


def test_large_partition_spill_matches_memory(tmpdir_path):
    n = 2000
    rows = _random_rows(n)
    specs = [
        QuerySpec("ds", "p",
                  rankings=(RankFunc.ROW_NUMBER, RankFunc.RANK, RankFunc.DENSE_RANK)),
        QuerySpec("ds", "p", lead_lag=(
            LeadLagSpec(LeadLagKind.LAG, 1),
            LeadLagSpec(LeadLagKind.LAG, 5, -1.0),
            LeadLagSpec(LeadLagKind.LEAD, 1),
            LeadLagSpec(LeadLagKind.LEAD, 9))),
        QuerySpec("ds", "p", frame=FrameSpec(FrameMode.PRECEDING, 20),
                  aggregates=ALL_AGGS),
        QuerySpec("ds", "p", frame=FrameSpec(FrameMode.AROUND, 12),
                  aggregates=ALL_AGGS),
        QuerySpec("ds", "p", aggregates=ALL_AGGS),
        QuerySpec("ds", "p", frame=FrameSpec(FrameMode.AROUND, 30),
                  aggregates=(AggFunc.SUM, AggFunc.MIN, AggFunc.MAX),
                  rankings=(RankFunc.RANK,),
                  lead_lag=(LeadLagSpec(LeadLagKind.LAG, 4),
                            LeadLagSpec(LeadLagKind.LEAD, 6))),
    ]
    raws = {}
    for name, memwin in (("mem", 10 ** 9), ("disk", 8)):
        st, en, _ = make_services(os.path.join(tmpdir_path, f"{name}.db"),
                                  max_in_memory_window=memwin, batch_size=37)
        st.commit_batch("ds", "same-batch", rows)
        raws[name] = [_run_raw(st, en, spec, n) for spec in specs]
        en.shutdown()
        st.close()
    for a, b in zip(raws["mem"], raws["disk"]):
        assert a == b


def test_large_frame_and_offsets_use_spill(tmpdir_path):
    # n 较小但窗口/偏移远大于阈值，强制全部走溢出结构
    n = 60
    vals = [None if i % 7 == 0 else float(i % 13 - 6) for i in range(n)]
    st, en, _ = make_services(os.path.join(tmpdir_path, "bigframe.db"),
                              max_in_memory_window=4, batch_size=8)
    st.commit_batch("ds", None, [RowIn("p", i, v) for i, v in enumerate(vals)])

    def naive_aggs(lo, hi):
        w = [v for v in vals[lo:hi] if v is not None]
        return (sum(w) if w else None, min(w) if w else None,
                max(w) if w else None, len(w))

    _, rows = run_sync(en, st, QuerySpec(
        "ds", "p", frame=FrameSpec(FrameMode.PRECEDING, 5000),
        aggregates=(AggFunc.SUM, AggFunc.MIN, AggFunc.MAX, AggFunc.COUNT)))
    for i, r in enumerate(rows):
        s, mn, mx, c = naive_aggs(0, i + 1)  # n 超出分区起点，窗口即整个前缀
        assert r["sum"] == s and r["min"] == mn and r["max"] == mx and r["count"] == c

    _, rows = run_sync(en, st, QuerySpec(
        "ds", "p", frame=FrameSpec(FrameMode.AROUND, 2000),
        aggregates=(AggFunc.SUM, AggFunc.MIN, AggFunc.MAX)))
    total = naive_aggs(0, n)
    for r in rows:  # n 超出两端，每行窗口都是全分区
        assert (r["sum"], r["min"], r["max"]) == total[:3]

    _, rows = run_sync(en, st, QuerySpec("ds", "p", lead_lag=(
        LeadLagSpec(LeadLagKind.LAG, 3000),
        LeadLagSpec(LeadLagKind.LEAD, 3000, default=-9.0))))
    # offset 超出分区：lag 全为默认 None；lead 全为 -9.0
    assert all(r["lag_3000"] is None for r in rows)
    assert all(r["lead_3000"] == -9.0 for r in rows)
    en.shutdown()
    st.close()


def test_no_spill_residue_after_done(tmpdir_path):
    st, en, cfg = make_services(os.path.join(tmpdir_path, "res.db"),
                                max_in_memory_window=2, batch_size=4)
    st.commit_batch("ds", None, [
        RowIn("p", i, float(i) if i % 3 else None) for i in range(40)])
    spec = QuerySpec("ds", "p", frame=FrameSpec(FrameMode.AROUND, 5),
                     aggregates=ALL_AGGS,
                     lead_lag=(LeadLagSpec(LeadLagKind.LAG, 3),
                               LeadLagSpec(LeadLagKind.LEAD, 4)))
    info, rows = run_sync(en, st, spec)
    assert len(rows) == 40
    conn = sqlite3.connect(cfg.db_path)
    for tbl in ("spill", "spill_text"):
        cnt = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]
        assert cnt == 0, (tbl, cnt)
    conn.close()
    en.shutdown()
    st.close()


def _scan_current_memory(db_path, n, memwin, sample_at):
    """执行一次 AROUND 小窗口 + 小偏移查询，在读到第 sample_at 行时采样当前堆内存。"""
    cfg = EngineConfig(db_path=db_path, batch_size=64,
                       max_in_memory_window=memwin, default_timeout_ms=30_000)
    storage = Storage(cfg)
    engine = QueryEngine(storage, cfg)
    storage.commit_batch("ds", None, [
        RowIn("p", i % 50, None if i % 11 == 0 else float(i % 17 - 8))
        for i in range(n)])
    sample = {}

    def hook(seq):
        if not sample and seq >= sample_at:
            sample["bytes"] = tracemalloc.get_traced_memory()[0]

    storage.row_read_hook = hook
    tracemalloc.start()
    spec = QuerySpec("ds", "p", frame=FrameSpec(FrameMode.AROUND, 3),
                     aggregates=ALL_AGGS,
                     lead_lag=(LeadLagSpec(LeadLagKind.LAG, 2),
                               LeadLagSpec(LeadLagKind.LEAD, 2)))
    qid = engine.submit(spec)
    info = wait_terminal(engine, qid, timeout=60)
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    storage.row_read_hook = None
    assert info.status == QueryStatus.DONE, (info.status, info.error)
    engine.shutdown()
    storage.close()
    return sample["bytes"], peak


def test_memory_does_not_grow_with_partition_size(tmpdir_path):
    # 磁盘溢出模式：分区扩大 4 倍，扫描中途的实时堆内存增量必须有界
    mid1, peak1 = _scan_current_memory(os.path.join(tmpdir_path, "m1.db"),
                                       4_000, memwin=64, sample_at=2_000)
    mid2, peak2 = _scan_current_memory(os.path.join(tmpdir_path, "m2.db"),
                                       16_000, memwin=64, sample_at=8_000)
    # 允许抖动，但绝不能随 4 倍行数近线性增长；阈值给 2MB 宽限
    assert mid2 < mid1 + 2_000_000, (mid1, mid2, peak1, peak2)


def test_streaming_order_and_count_large_partition(tmpdir_path):
    n = 5000
    st, en, _ = make_services(os.path.join(tmpdir_path, "stream.db"),
                              max_in_memory_window=16, batch_size=128)
    st.commit_batch("ds", None, [
        RowIn("p", i, float(i % 100 - 50)) for i in range(n)])
    info, rows = run_sync(en, st, QuerySpec(
        "ds", "p", frame=FrameSpec(FrameMode.AROUND, 3),
        aggregates=(AggFunc.SUM, AggFunc.COUNT)), timeout=60)
    assert info.result_count == n
    assert [r["row_index"] for r in rows] == list(range(n))
    vals = [float(i % 100 - 50) for i in range(n)]
    for i in (0, 1, 2, n // 2, n - 3, n - 2, n - 1):
        w = [v for v in vals[max(0, i - 3): i + 4] if v is not None]
        assert rows[i]["sum"] == sum(w)
        assert rows[i]["count"] == len(w)
    en.shutdown()
    st.close()
