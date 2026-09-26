"""Large partitions: correctness at scale and bounded memory."""

import gc
import tracemalloc

import pytest

from wqe.config import Config
from wqe.engine import Engine
from wqe.models import FrameType, FuncKind, FuncSpec, QuerySpec, WindowSpec


def _build(engine, n):
    batch = 1000
    rows = [(i % 97, i % 1000) for i in range(n)]  # many ties, bounded values
    for start in range(0, n, batch):
        engine.write_batch("big", rows[start:start + batch])


@pytest.mark.parametrize("n", [2_000, 20_000])
def test_large_partition_correctness(tmp_path, n):
    cfg = Config(db_path=str(tmp_path / "t.db"), page_size=512, default_timeout_s=0)
    with Engine(cfg) as engine:
        _build(engine, n)
        spec = QuerySpec(
            partition_key="big",
            functions=(
                FuncSpec(kind=FuncKind.ROW_NUMBER, alias="rn"),
                FuncSpec(kind=FuncKind.SUM, alias="ps", window=WindowSpec(FrameType.PARTITION)),
                FuncSpec(kind=FuncKind.COUNT, alias="pc", window=WindowSpec(FrameType.PARTITION)),
                FuncSpec(kind=FuncKind.SUM, alias="rs",
                         window=WindowSpec(FrameType.ROWS_PRECEDING, n=4)),
                FuncSpec(kind=FuncKind.MAX, alias="am",
                         window=WindowSpec(FrameType.ROWS_AROUND, n=8)),
            ),
        )
        result = engine.submit_query(spec, timeout_s=0)
        assert result.status.value == "committed"

        # Fetch through storage in pages rather than materialising everything.
        conn = engine.storage.begin_snapshot()
        try:
            count = engine.storage.count_rows(conn, "big")
            assert count == n
            pages = engine.storage.iter_partition(conn, "big", spec.null_order, page_size=1024)
            ordered_values = [row.value for page in pages for row in page]
        finally:
            engine.storage.release(conn)
        assert len(ordered_values) == n

        # Independently expected partition aggregates.
        non_null = [v for v in ordered_values if v is not None]
        exp_sum, exp_count = sum(non_null), len(non_null)

        idx = 0
        for page in engine.iter_result_pages(result.query_id, page_size=1024):
            for r in page:
                assert r.outputs["rn"] == idx + 1
                assert r.outputs["ps"] == exp_sum
                assert r.outputs["pc"] == exp_count
                # ROWS_PRECEDING n=4 over ordered values
                lo = max(0, idx - 4)
                exp_rs = sum(v for v in ordered_values[lo:idx + 1] if v is not None)
                # SUM semantics: 0 for frames that contain only NULL values.
                assert r.outputs["rs"] == exp_rs
                # ROWS_AROUND n=8: clipped [idx-8, idx+8]
                alo = max(0, idx - 8)
                ahi = min(n, idx + 9)
                exp_max = max(
                    (v for v in ordered_values[alo:ahi] if v is not None),
                    default=None,
                )
                assert r.outputs["am"] == exp_max
                idx += 1
        assert idx == n


def test_memory_is_bounded_independent_of_partition_size(tmp_path):
    def peak_for(n):
        cfg = Config(db_path=str(tmp_path / f"m{n}.db"), page_size=256,
                     default_timeout_s=0)
        with Engine(cfg) as engine:
            _build(engine, n)
            spec = QuerySpec(
                partition_key="big",
                functions=(
                    FuncSpec(kind=FuncKind.ROW_NUMBER, alias="rn"),
                    FuncSpec(kind=FuncKind.SUM, alias="ps", window=WindowSpec(FrameType.PARTITION)),
                    FuncSpec(kind=FuncKind.AVG, alias="pa", window=WindowSpec(FrameType.PARTITION)),
                    FuncSpec(kind=FuncKind.SUM, alias="rs",
                             window=WindowSpec(FrameType.ROWS_PRECEDING, n=10)),
                    FuncSpec(kind=FuncKind.MIN, alias="mn",
                             window=WindowSpec(FrameType.ROWS_AROUND, n=16)),
                ),
            )
            gc.collect()
            tracemalloc.start()
            result = engine.submit_query(spec, timeout_s=0)
            _, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            assert result.status.value == "committed"
        gc.collect()
        return peak

    small = peak_for(5_000)
    big = peak_for(50_000)
    # 10x more rows must not lead to anywhere near 10x Python heap growth.
    # Allow generous slack for interpreter/DB noise but require sublinearity.
    assert big < small * 3 + 4_000_000, (small, big)
