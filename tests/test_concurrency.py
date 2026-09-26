"""Concurrent writes/queries: snapshot isolation and serial write atomicity."""

import threading

from wqe.config import Config
from wqe.engine import Engine
from wqe.models import FrameType, FuncKind, FuncSpec, QuerySpec, WindowSpec


def _spec(part="p"):
    return QuerySpec(
        partition_key=part,
        functions=(
            FuncSpec(kind=FuncKind.COUNT, alias="c", window=WindowSpec(FrameType.PARTITION)),
            FuncSpec(kind=FuncKind.SUM, alias="s", window=WindowSpec(FrameType.PARTITION)),
        ),
    )


def _fetch(engine, qid):
    rows = []
    for page in engine.iter_result_pages(qid, page_size=256):
        rows.extend(page)
    return rows


def test_query_sees_consistent_snapshot(tmp_path):
    cfg = Config(db_path=str(tmp_path / "t.db"), page_size=64, default_timeout_s=0)
    with Engine(cfg) as engine:
        engine.write_batch("p", [(i, 1) for i in range(2000)])

        def reader(out, idx):
            res = engine.submit_query(_spec(), timeout_s=0)
            rows = _fetch(engine, res.query_id)
            out[idx] = (len(rows), rows[0].outputs)

        for _ in range(5):
            outs: dict = {}
            readers = [
                threading.Thread(target=reader, args=(outs, i)) for i in range(4)
            ]
            for r in readers:
                r.start()
            # Concurrent appends while readers are computing.
            for k in range(20):
                engine.write_batch("p", [(50_000 + k, 1)] * 25)
            for r in readers:
                r.join()

            for count, head in outs.values():
                # Each query sees exactly one committed snapshot: its row
                # count equals its COUNT aggregate and no mixed view.
                assert head["c"] == count
                assert head["s"] == count


def test_concurrent_writers_batches_are_atomic(tmp_path):
    cfg = Config(db_path=str(tmp_path / "t.db"), page_size=128, default_timeout_s=0)
    with Engine(cfg) as engine:
        def writer(base):
            for _ in range(20):
                engine.write_batch(
                    f"part{base % 4}",
                    [((base * 7) % 500, base)] * 50,
                )

        threads = [threading.Thread(target=writer, args=(b,)) for b in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Total committed rows must be exactly 8 * 20 * 50 (no lost batches).
        conn = engine.storage.begin_snapshot()
        try:
            total = 0
            for p in range(4):
                total += engine.storage.count_rows(conn, f"part{p}")
        finally:
            engine.storage.release(conn)
        assert total == 8 * 20 * 50


def test_snapshot_does_not_see_later_writes(tmp_path):
    """A snapshot taken before a write must not include that write."""
    cfg = Config(db_path=str(tmp_path / "t.db"), page_size=64, default_timeout_s=0)
    with Engine(cfg) as engine:
        engine.write_batch("p", [(i, 1) for i in range(500)])

        started = threading.Event()
        release = threading.Event()
        result_holder = {}

        def reader():
            conn = engine.storage.begin_snapshot()
            started.set()
            release.wait(2.0)
            try:
                pages = engine.storage.iter_partition(conn, "p", _spec().null_order, 128)
                count = sum(len(pg) for pg in pages)
                result_holder["count"] = count
            finally:
                engine.storage.release(conn)

        t = threading.Thread(target=reader)
        t.start()
        assert started.wait(2.0)
        # Write while the reader holds an open snapshot transaction.
        engine.write_batch("pnew", [(1, 9)] * 300)
        engine.write_batch("p", [(999, 1)] * 100)
        release.set()
        t.join()
        # Snapshot fixed at BEGIN: exactly the original 500 rows.
        assert result_holder["count"] == 500


def test_many_concurrent_queries_are_isolated(tmp_path):
    cfg = Config(db_path=str(tmp_path / "t.db"), page_size=32, default_timeout_s=0)
    with Engine(cfg) as engine:
        # Distinct partitions with known sizes.
        for p in range(8):
            engine.write_batch(f"k{p}", [(i, 1) for i in range(100 * (p + 1))])

        results = {}
        errors = []

        def reader(p):
            try:
                res = engine.submit_query(_spec(f"k{p}"), timeout_s=0)
                rows = _fetch(engine, res.query_id)
                results[p] = (res.status.value, len(rows),
                              rows[0].outputs["c"] if rows else None)
            except Exception as exc:  # noqa: BLE001  # pragma: no cover
                errors.append(exc)

        threads = [threading.Thread(target=reader, args=(p,)) for p in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not errors
        for p in range(8):
            status, count, agg_count = results[p]
            assert status == "committed"
            assert count == 100 * (p + 1)
            assert agg_count == count
