"""查询取消与超时：不得留下部分可见结果，不得影响后续查询。"""
from __future__ import annotations

import os
import sqlite3
import threading
import time

from windowed.types import (
    AggFunc,
    FrameMode,
    FrameSpec,
    QuerySpec,
    QueryStatus,
    RowIn,
)

from .conftest import make_services, run_sync, wait_terminal

ALL_AGGS = tuple(AggFunc)


def _count_rows(db_path, table, qid=None):
    conn = sqlite3.connect(db_path)
    try:
        if qid is None:
            return conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        col = "query_id" if table == "result_rows" else "owner"
        return conn.execute(
            f"SELECT COUNT(*) FROM {table} WHERE {col}=?", (qid,)
        ).fetchone()[0]
    finally:
        conn.close()


def test_cancel_running_query_leaves_no_partial_results(tmpdir_path):
    db = os.path.join(tmpdir_path, "cancel.db")
    st, en, cfg = make_services(db, batch_size=4, max_in_memory_window=2)
    st.commit_batch("ds", None, [RowIn("p", i, float(i)) for i in range(200)])

    started = threading.Event()
    release = threading.Event()

    def hook(seq):
        if seq >= 5:
            started.set()
            release.wait(timeout=15)

    st.row_read_hook = hook
    spec = QuerySpec("ds", "p", frame=FrameSpec(FrameMode.AROUND, 3),
                     aggregates=ALL_AGGS)
    qid = en.submit(spec)
    assert started.wait(timeout=10)
    assert en.cancel(qid) is True
    release.set()
    st.row_read_hook = None

    info = wait_terminal(en, qid)
    assert info.status == QueryStatus.CANCELLED, info
    # 不留部分结果、不留溢出数据
    assert st.read_results(qid, 0, 10) == []
    assert _count_rows(db, "result_rows", qid) == 0
    assert _count_rows(db, "spill") == 0
    assert _count_rows(db, "spill_text") == 0

    # 后续查询不受影响，结果完整正确
    _, rows = run_sync(en, st, spec)
    assert len(rows) == 200
    vals = [float(i) for i in range(200)]
    for i in (0, 1, 100, 199):
        w = vals[max(0, i - 3): i + 4]
        assert rows[i]["sum"] == sum(w)
    en.shutdown()
    st.close()


def test_cancel_queued_query(tmpdir_path):
    """单 worker 时取消排在队列里的查询：不得被执行出结果。"""
    db = os.path.join(tmpdir_path, "queued.db")
    st, en, cfg = make_services(db, query_workers=1)
    st.commit_batch("ds", None, [RowIn("p", i, float(i)) for i in range(50)])

    block = threading.Event()
    entered = threading.Event()

    def hook(seq):
        if not entered.is_set():
            entered.set()
            block.wait(timeout=15)

    st.row_read_hook = hook
    q1 = en.submit(QuerySpec("ds", "p", aggregates=(AggFunc.SUM,)))
    assert entered.wait(timeout=10)
    q2 = en.submit(QuerySpec("ds", "p", aggregates=(AggFunc.SUM,)))
    assert en.cancel(q2) is True
    block.set()
    st.row_read_hook = None

    info1 = wait_terminal(en, q1)
    info2 = wait_terminal(en, q2)
    assert info1.status == QueryStatus.DONE
    assert info2.status == QueryStatus.CANCELLED
    assert st.read_results(q2, 0, 10) == []
    en.shutdown()
    st.close()


def test_timeout_aborts_and_cleans(tmpdir_path):
    db = os.path.join(tmpdir_path, "timeout.db")
    st, en, cfg = make_services(db, batch_size=4, max_in_memory_window=2)
    st.commit_batch("ds", None, [RowIn("p", i, float(i)) for i in range(40)])

    def slow_hook(seq):
        time.sleep(0.05)  # 每行 50ms，远超 100ms 超时

    st.row_read_hook = slow_hook
    spec = QuerySpec("ds", "p", frame=FrameSpec(FrameMode.AROUND, 2),
                     aggregates=ALL_AGGS, timeout_ms=100)
    qid = en.submit(spec)
    info = wait_terminal(en, qid, timeout=30)
    st.row_read_hook = None
    assert info.status == QueryStatus.TIMEOUT, info
    assert st.read_results(qid, 0, 10) == []
    assert _count_rows(db, "result_rows", qid) == 0
    assert _count_rows(db, "spill") == 0

    # 超时后服务仍正常
    _, rows = run_sync(en, st, QuerySpec("ds", "p", aggregates=(AggFunc.COUNT,)))
    assert len(rows) == 40 and rows[0]["count"] == 40
    en.shutdown()
    st.close()


def test_cancel_unknown_or_finished_query(tmpdir_path):
    st, en, _ = make_services(os.path.join(tmpdir_path, "c2.db"))
    st.commit_batch("ds", None, [RowIn("p", 1, 1.0)])
    assert en.cancel("no-such-query") is False
    info, _ = run_sync(en, st, QuerySpec("ds", "p", aggregates=(AggFunc.SUM,)))
    # 已完成的查询无法再取消
    assert en.cancel(info.query_id) is False
    en.shutdown()
    st.close()
