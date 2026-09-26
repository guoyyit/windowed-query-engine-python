"""Window semantics: ties, NULLs, boundary clipping, determinism."""

import pytest

from wqe.config import Config
from wqe.engine import Engine
from wqe.models import FrameType, FuncKind, FuncSpec, NullOrder, QuerySpec, WindowSpec


@pytest.fixture
def engine(tmp_path):
    cfg = Config(db_path=str(tmp_path / "t.db"), page_size=3, default_timeout_s=0)
    with Engine(cfg) as eng:
        yield eng


def _q(*funcs, null_order=NullOrder.NULLS_LAST):
    return QuerySpec(partition_key="p", functions=tuple(funcs), null_order=null_order)


def _fetch_all(engine, query_id):
    rows = []
    for page in engine.iter_result_pages(query_id, page_size=37):
        rows.extend(page)
    return rows


class _Result:
    def __init__(self, raw, engine):
        self.status = raw.status
        self.error = raw.error
        self.rows = _fetch_all(engine, raw.query_id)


def _run(engine, spec):
    raw = engine.submit_query(spec, timeout_s=0)
    return _Result(raw, engine)


def test_tie_breaking_by_commit_order(engine):
    # Equal sort keys keep insertion order (seq) and rank ties stably.
    engine.write_batch("p", [(5, "b"), (5, "a"), (5, "c"), (3, "z")])
    spec = _q(
        FuncSpec(kind=FuncKind.ROW_NUMBER, alias="rn"),
        FuncSpec(kind=FuncKind.RANK, alias="rk"),
        FuncSpec(kind=FuncKind.DENSE_RANK, alias="dr"),
    )
    rows = _run(engine, spec).rows
    keys = [(r.sort_key, r.value) for r in rows]
    assert keys == [(3, "z"), (5, "b"), (5, "a"), (5, "c")]
    assert [r.outputs["rn"] for r in rows] == [1, 2, 3, 4]
    assert [r.outputs["rk"] for r in rows] == [1, 2, 2, 2]
    assert [r.outputs["dr"] for r in rows] == [1, 2, 2, 2]


def test_rank_after_multi_groups(engine):
    engine.write_batch("p", [(1, 1), (1, 2), (2, 3), (3, 4), (3, 5), (4, 6)])
    spec = _q(
        FuncSpec(kind=FuncKind.RANK, alias="rk"),
        FuncSpec(kind=FuncKind.DENSE_RANK, alias="dr"),
    )
    rows = _run(engine, spec).rows
    assert [r.outputs["rk"] for r in rows] == [1, 1, 3, 4, 4, 6]
    assert [r.outputs["dr"] for r in rows] == [1, 1, 2, 3, 3, 4]


def test_null_sort_keys_first_and_last(engine):
    engine.write_batch("p", [(None, 1), (2, 2), (1, 3), (None, 4)])
    for order, expected in (
        (NullOrder.NULLS_LAST, [1, 2, None, None]),
        (NullOrder.NULLS_FIRST, [None, None, 1, 2]),
    ):
        spec = _q(FuncSpec(kind=FuncKind.ROW_NUMBER, alias="rn"), null_order=order)
        rows = _run(engine, spec).rows
        assert [r.sort_key for r in rows] == expected


def test_null_sort_keys_form_one_tie_group(engine):
    engine.write_batch("p", [(None, 1), (1, 2), (None, 3)])
    spec = _q(
        FuncSpec(kind=FuncKind.RANK, alias="rk"),
        FuncSpec(kind=FuncKind.DENSE_RANK, alias="dr"),
    )
    rows = _run(engine, spec).rows  # NULLS LAST
    # (1) rank1; two NULLs share rank 2
    assert [(r.outputs["rk"], r.outputs["dr"]) for r in rows] == [(1, 1), (2, 2), (2, 2)]


def test_lag_lead_distinguish_missing_row_from_null_value(engine):
    engine.write_batch("p", [(1, 10), (2, None), (3, 30)])
    spec = _q(
        FuncSpec(kind=FuncKind.LAG, alias="lag", offset=1, default="D"),
        FuncSpec(kind=FuncKind.LEAD, alias="lead", offset=1, default="D"),
    )
    rows = _run(engine, spec).rows
    assert rows[0].outputs["lag"] == "D"          # no preceding row
    assert rows[0].outputs["lead"] is None         # row exists, value NULL
    assert rows[1].outputs["lag"] == 10
    assert rows[1].outputs["lead"] == 30
    assert rows[2].outputs["lead"] == "D"          # no following row
    assert rows[2].outputs["lag"] is None          # preceding row value NULL


def test_partition_aggregates_ignore_null_values(engine):
    engine.write_batch("p", [(1, 10), (2, None), (3, 20), (4, None)])
    spec = _q(
        FuncSpec(kind=FuncKind.SUM, alias="s", window=WindowSpec(FrameType.PARTITION)),
        FuncSpec(kind=FuncKind.COUNT, alias="c", window=WindowSpec(FrameType.PARTITION)),
        FuncSpec(kind=FuncKind.AVG, alias="a", window=WindowSpec(FrameType.PARTITION)),
        FuncSpec(kind=FuncKind.MIN, alias="mn", window=WindowSpec(FrameType.PARTITION)),
        FuncSpec(kind=FuncKind.MAX, alias="mx", window=WindowSpec(FrameType.PARTITION)),
    )
    out = _run(engine, spec).rows[0].outputs
    assert out["s"] == 30
    assert out["c"] == 2
    assert out["a"] == 15.0
    assert out["mn"] == 10
    assert out["mx"] == 20


def test_all_null_partition_aggregates(engine):
    engine.write_batch("p", [(1, None), (2, None)])
    spec = _q(
        FuncSpec(kind=FuncKind.SUM, alias="s", window=WindowSpec(FrameType.PARTITION)),
        FuncSpec(kind=FuncKind.COUNT, alias="c", window=WindowSpec(FrameType.PARTITION)),
        FuncSpec(kind=FuncKind.AVG, alias="a", window=WindowSpec(FrameType.PARTITION)),
        FuncSpec(kind=FuncKind.MIN, alias="mn", window=WindowSpec(FrameType.PARTITION)),
        FuncSpec(kind=FuncKind.MAX, alias="mx", window=WindowSpec(FrameType.PARTITION)),
    )
    out = _run(engine, spec).rows[0].outputs
    assert out["s"] is None and out["c"] == 0 and out["a"] is None
    assert out["mn"] is None and out["mx"] is None


def test_rows_preceding_boundary_and_nulls(engine):
    engine.write_batch("p", [(1, 10), (2, None), (3, 20)])
    spec = _q(
        FuncSpec(kind=FuncKind.SUM, alias="s", window=WindowSpec(FrameType.ROWS_PRECEDING, n=1)),
        FuncSpec(kind=FuncKind.COUNT, alias="c", window=WindowSpec(FrameType.ROWS_PRECEDING, n=1)),
    )
    rows = _run(engine, spec).rows
    assert [r.outputs["s"] for r in rows] == [10, 10, 20]
    assert [r.outputs["c"] for r in rows] == [1, 1, 1]


def test_rows_around_boundary_clipping(engine):
    engine.write_batch("p", [(1, 10), (2, 20), (3, 30), (4, 40)])
    spec = _q(
        FuncSpec(kind=FuncKind.SUM, alias="s", window=WindowSpec(FrameType.ROWS_AROUND, n=2)),
        FuncSpec(kind=FuncKind.MIN, alias="mn", window=WindowSpec(FrameType.ROWS_AROUND, n=2)),
        FuncSpec(kind=FuncKind.MAX, alias="mx", window=WindowSpec(FrameType.ROWS_AROUND, n=2)),
    )
    rows = _run(engine, spec).rows
    # frames: [0..2], [0..3], [0..4 clipped -> 0..3], [1..3]
    assert [r.outputs["s"] for r in rows] == [60, 100, 100, 90]
    assert [r.outputs["mn"] for r in rows] == [10, 10, 10, 20]
    assert [r.outputs["mx"] for r in rows] == [30, 40, 40, 40]


def test_rows_around_zero_is_current_row(engine):
    engine.write_batch("p", [(1, 10), (2, 20)])
    spec = _q(
        FuncSpec(kind=FuncKind.SUM, alias="s", window=WindowSpec(FrameType.ROWS_AROUND, n=0)),
        FuncSpec(kind=FuncKind.SUM, alias="p", window=WindowSpec(FrameType.ROWS_PRECEDING, n=0)),
    )
    rows = _run(engine, spec).rows
    assert [r.outputs["s"] for r in rows] == [10, 20]
    assert [r.outputs["p"] for r in rows] == [10, 20]


def test_repeated_runs_are_identical(engine):
    engine.write_batch("p", [(2, 20), (1, 10), (2, 25), (1, 5)])
    spec = _q(
        FuncSpec(kind=FuncKind.RANK, alias="rk"),
        FuncSpec(kind=FuncKind.SUM, alias="s", window=WindowSpec(FrameType.ROWS_AROUND, n=2)),
    )
    first = [r.outputs for r in _run(engine, spec).rows]
    second = [r.outputs for r in _run(engine, spec).rows]
    assert first == second


def test_empty_partition(engine):
    spec = _q(FuncSpec(kind=FuncKind.SUM, alias="s", window=WindowSpec(FrameType.PARTITION)))
    result = _run(engine, spec)
    assert result.rows == []


def test_negative_and_string_sort_keys_are_deterministic(engine):
    engine.write_batch("p", [(-5, 1), (-100, 2), (None, 3), (0, 4), (-5, 5), (3, 6)])
    spec = _q(FuncSpec(kind=FuncKind.ROW_NUMBER, alias="rn"))
    rows = _run(engine, spec).rows
    assert [r.sort_key for r in rows] == [-100, -5, -5, 0, 3, None]
    # twice -> identical
    assert [r.sort_key for r in _run(engine, spec).rows] == [-100, -5, -5, 0, 3, None]


def test_string_keys_lexicographic(engine):
    engine.write_batch("p", [("b", 1), ("a", 2), ("b", 3), ("", 4)])
    spec = _q(FuncSpec(kind=FuncKind.ROW_NUMBER, alias="rn"))
    rows = _run(engine, spec).rows
    # numbers group first if mixed; pure strings sort lexicographically,
    # empty string first, ties by insertion seq.
    assert [r.sort_key for r in rows] == ["", "a", "b", "b"]
    assert [r.value for r in rows] == [4, 2, 1, 3]


def test_frame_wider_than_partition_and_page(tmp_path):
    cfg = Config(db_path=str(tmp_path / "t.db"), page_size=3, default_timeout_s=0)
    with Engine(cfg) as engine:
        n = 12
        engine.write_batch("p", [(i, i + 1) for i in range(n)])
        spec = QuerySpec(
            partition_key="p",
            functions=(
                FuncSpec(kind=FuncKind.SUM, alias="pre",
                         window=WindowSpec(FrameType.ROWS_PRECEDING, n=100)),
                FuncSpec(kind=FuncKind.MAX, alias="ar",
                         window=WindowSpec(FrameType.ROWS_AROUND, n=100)),
                FuncSpec(kind=FuncKind.MIN, alias="arn",
                         window=WindowSpec(FrameType.ROWS_AROUND, n=4)),
                FuncSpec(kind=FuncKind.AVG, alias="aravg",
                         window=WindowSpec(FrameType.ROWS_AROUND, n=2)),
            ),
        )
        res = engine.submit_query(spec, timeout_s=0)
        rows = []
        for page in engine.iter_result_pages(res.query_id, page_size=2):
            rows.extend(page)
        # Frame wider than partition: preceding covers the whole prefix,
        # around covers the whole partition for every row.
        assert [r.outputs["pre"] for r in rows] == [
            sum(range(1, i + 2)) for i in range(n)
        ]
        assert [r.outputs["ar"] for r in rows] == [n] * n
        for i, r in enumerate(rows):
            lo = max(0, i - 4)
            assert r.outputs["arn"] == lo + 1
            alo, ahi = max(0, i - 2), min(n, i + 3)
            seg = list(range(alo + 1, ahi + 1))
            assert r.outputs["aravg"] == sum(seg) / len(seg)
