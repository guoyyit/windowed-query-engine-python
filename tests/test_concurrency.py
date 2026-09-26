"""并发写入、并发查询与一致快照测试。"""
from __future__ import annotations

import threading

from windowed.types import (
    AggFunc,
    QuerySpec,
    QueryStatus,
    RowIn,
)

from .conftest import make_services, run_sync, wait_terminal


def test_snapshot_isolation_during_concurrent_write(services):
    """查询读到的是提交时刻的快照：扫描中途的并发写入不可见。"""
    storage, engine, _ = services
    storage.commit_batch("ds", None, [RowIn("p", i, 1.0) for i in range(100)])

    read_half = threading.Event()
    committed = threading.Event()

    def hook(seq):
        if seq == 50:  # 读到第 50 行时暂停，等待并发写入完成
            read_half.set()
            assert committed.wait(timeout=10)

    storage.row_read_hook = hook
    spec = QuerySpec("ds", "p", aggregates=(AggFunc.SUM, AggFunc.COUNT))
    qid = engine.submit(spec)
    assert read_half.wait(timeout=10)
    # 在查询扫描进行到一半时写入 100 行新数据
    storage.commit_batch("ds", None, [RowIn("p", 100 + i, 1000.0) for i in range(100)])
    committed.set()
    storage.row_read_hook = None

    info = wait_terminal(engine, qid)
    assert info.status == QueryStatus.DONE
    assert info.version == 1  # 快照在写入之前
    payloads = storage.read_results(qid, 0, 1000)
    import json
    parsed = [json.loads(p) for p in payloads]
    assert len(parsed) == 100                       # 新写入的 100 行不可见
    assert all(r["count"] == 100 for r in parsed)
    assert all(r["sum"] == 100.0 for r in parsed)   # 不是 100 + 100*1000
    assert all(r["frame_rows"] == 100 for r in parsed)

    # 之后的查询能看到新数据
    _, rows2 = run_sync(engine, storage, spec)
    assert len(rows2) == 200
    assert rows2[0]["count"] == 200
    assert rows2[0]["sum"] == 100.0 + 100 * 1000.0


def test_concurrent_writes_atomic_and_versioned(services):
    """多线程并发提交：每批原子可见，版本号唯一且单调，总行数精确。"""
    storage, engine, _ = services
    n_threads, batches_per_thread, rows_per_batch = 8, 25, 20
    versions = []
    lock = threading.Lock()
    errors = []

    def worker(tid):
        try:
            for b in range(batches_per_thread):
                rows = [
                    RowIn("p", tid * 100000 + b * 100 + r, float(r))
                    for r in range(rows_per_batch)
                ]
                v = storage.commit_batch("ds", f"t{tid}-b{b}", rows)
                with lock:
                    versions.append(v)
        except Exception as e:  # noqa: BLE001
            with lock:
                errors.append(e)

    threads = [threading.Thread(target=worker, args=(t,)) for t in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    total_batches = n_threads * batches_per_thread
    assert len(versions) == total_batches
    assert len(set(versions)) == total_batches          # 版本号无重复
    assert sorted(versions) == list(range(1, total_batches + 1))

    _, rows = run_sync(engine, storage,
                       QuerySpec("ds", "p", aggregates=(AggFunc.COUNT,)))
    assert len(rows) == total_batches * rows_per_batch  # 无丢失、无重复
    assert rows[0]["count"] == total_batches * rows_per_batch


def test_idempotent_batch_replay_concurrent(services):
    """相同 batch_id 并发重放：只提交一次，返回相同版本。"""
    storage, engine, _ = services
    rows = [RowIn("p", i, float(i)) for i in range(50)]
    results = []

    def replay():
        results.append(storage.commit_batch("ds", "same-batch", rows))

    threads = [threading.Thread(target=replay) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len(set(results)) == 1
    _, got = run_sync(engine, storage,
                      QuerySpec("ds", "p", aggregates=(AggFunc.COUNT,)))
    assert len(got) == 50  # 只出现一次


def test_concurrent_queries_each_see_consistent_snapshot(tmpdir_path):
    """写入与多个查询并发：每个查询的结果行数等于其快照版本时已提交的行数。"""
    st, en, _ = make_services(tmpdir_path + "/cc.db", query_workers=4)
    stop = threading.Event()
    committed_counts = []  # committed_counts[v] = 版本 v 提交后的累计行数
    lock = threading.Lock()

    def writer():
        v_rows = 0
        batch = 0
        while not stop.is_set():
            st.commit_batch("ds", f"w-{batch}", [RowIn("p", batch, 1.0)])
            batch += 1
            v_rows += 1
            with lock:
                committed_counts.append(v_rows)

    wt = threading.Thread(target=writer)
    wt.start()
    try:
        infos = []
        for _ in range(12):
            qid = en.submit(QuerySpec("ds", "p", aggregates=(AggFunc.COUNT,)))
            infos.append(qid)
        for qid in infos:
            info = wait_terminal(en, qid, timeout=30)
            assert info.status == QueryStatus.DONE
            with lock:
                expected = committed_counts[info.version - 1] if info.version >= 1 else 0
            assert info.result_count == expected, (info.version, info.result_count, expected)
    finally:
        stop.set()
        wt.join()
        en.shutdown()
        st.close()
