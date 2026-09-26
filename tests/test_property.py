"""Property tests: streaming engine vs a brute-force in-memory reference."""

import random

import pytest

from wqe.config import Config
from wqe.engine import Engine
from wqe.models import FrameType, FuncKind, FuncSpec, NullOrder, QuerySpec, WindowSpec


def _order_tuples(raw, null_order):
    """raw: list of (seq, sort_key). Returns seq in expected sorted order."""
    def key(item):
        seq, k = item
        # Uniform 4-tuple: (null group, type group, sort value, seq).
        if k is None:
            null_grp = 0 if null_order is NullOrder.NULLS_FIRST else 2
            return (null_grp, 0, 0, seq)
        null_grp = 1 if null_order is NullOrder.NULLS_FIRST else 0
        type_grp = 0 if isinstance(k, (int, float)) and not isinstance(k, bool) else 1
        return (null_grp, type_grp, k, seq)

    ordered = sorted(raw, key=key)
    return [seq for seq, _ in ordered]


def _reference(rows_by_seq, order_seqs, funcs):
    """rows_by_seq: seq -> (sort_key, value); returns list of output dicts."""
    keys = [rows_by_seq[s][0] for s in order_seqs]
    vals = [rows_by_seq[s][1] for s in order_seqs]
    n = len(keys)
    out = [{} for _ in range(n)]

    # ranks
    distinct = 0
    prev = object()
    run_start = 0
    for i, k in enumerate(keys):
        is_tie = i > 0 and ((k is None and prev is None) or (k is not None and prev is not None and k == prev))
        if i == 0:
            distinct = 1
            run_start = 0
        elif not is_tie:
            distinct += 1
            run_start = i
        prev = k
        for f in funcs:
            if f.kind is FuncKind.ROW_NUMBER:
                out[i][f.alias] = i + 1
            elif f.kind is FuncKind.RANK:
                out[i][f.alias] = run_start + 1
            elif f.kind is FuncKind.DENSE_RANK:
                out[i][f.alias] = distinct

    def non_null(vs):
        return [v for v in vs if v is not None]

    for f in funcs:
        if f.kind in (FuncKind.LAG, FuncKind.LEAD):
            for i in range(n):
                j = i - f.offset if f.kind is FuncKind.LAG else i + f.offset
                out[i][f.alias] = f.default if j < 0 or j >= n else vals[j]
        elif f.window is not None:
            for i in range(n):
                if f.window.frame is FrameType.PARTITION:
                    lo, hi = 0, n
                elif f.window.frame is FrameType.ROWS_PRECEDING:
                    lo, hi = max(0, i - f.window.n), i + 1
                else:
                    lo, hi = max(0, i - f.window.n), min(n, i + f.window.n + 1)
                vs = non_null(vals[lo:hi])
                if f.kind is FuncKind.SUM:
                    out[i][f.alias] = sum(vs) if vs else None
                elif f.kind is FuncKind.COUNT:
                    out[i][f.alias] = len(vs)
                elif f.kind is FuncKind.AVG:
                    out[i][f.alias] = sum(vs) / len(vs) if vs else None
                elif f.kind is FuncKind.MIN:
                    out[i][f.alias] = min(vs) if vs else None
                elif f.kind is FuncKind.MAX:
                    out[i][f.alias] = max(vs) if vs else None
    return out


@pytest.mark.parametrize("seed", list(range(25)))
def test_matches_reference(tmp_path, seed):
    rng = random.Random(seed)
    n = rng.randint(0, 60)
    raw_rows = []
    key_pool = [rng.randint(-3, 3) for _ in range(5)] + [rng.choice(["a", "b", ""]) for _ in range(2)] + [None]
    for _ in range(n):
        k = rng.choice(key_pool)
        v = rng.choice([None, rng.randint(-5, 5), float(rng.randint(1, 4))])
        raw_rows.append((k, v))

    null_order = rng.choice([NullOrder.NULLS_LAST, NullOrder.NULLS_FIRST])
    cfg = Config(
        db_path=str(tmp_path / f"t{seed}.db"),
        page_size=rng.choice([1, 2, 7]),
        default_timeout_s=0,
    )
    with Engine(cfg) as engine:
        # Write in random-sized batches.
        i = 0
        while i < n:
            step = rng.randint(1, 9)
            engine.write_batch("p", raw_rows[i:i + step])
            i += step

        pre_n = rng.randint(0, 3)
        around_n = rng.randint(0, 4)
        lag_off = rng.randint(1, 3)
        lead_off = rng.randint(1, 3)
        funcs = [
            FuncSpec(kind=FuncKind.ROW_NUMBER, alias="rn"),
            FuncSpec(kind=FuncKind.RANK, alias="rk"),
            FuncSpec(kind=FuncKind.DENSE_RANK, alias="dr"),
            FuncSpec(kind=FuncKind.LAG, alias="lag", offset=lag_off, default="D"),
            FuncSpec(kind=FuncKind.LEAD, alias="lead", offset=lead_off, default="D"),
        ]
        for kind, alias in (
            (FuncKind.SUM, "s"), (FuncKind.COUNT, "c"), (FuncKind.AVG, "a"),
            (FuncKind.MIN, "mn"), (FuncKind.MAX, "mx"),
        ):
            funcs.append(FuncSpec(kind=kind, alias=f"p_{alias}",
                                  window=WindowSpec(FrameType.PARTITION)))
            funcs.append(FuncSpec(kind=kind, alias=f"pre_{alias}",
                                  window=WindowSpec(FrameType.ROWS_PRECEDING, n=pre_n)))
            funcs.append(FuncSpec(kind=kind, alias=f"ar_{alias}",
                                  window=WindowSpec(FrameType.ROWS_AROUND, n=around_n)))

        spec = QuerySpec(partition_key="p", functions=tuple(funcs), null_order=null_order)
        result = engine.submit_query(spec, timeout_s=0)
        assert result.status.value == "committed"

        got = []
        for page in engine.iter_result_pages(result.query_id, page_size=5):
            for r in page:
                got.append((r.seq, r.sort_key, r.value, dict(r.outputs)))

        # Reference
        rows_by_seq = {i + 1: raw_rows[i] for i in range(n)}
        ordered = _order_tuples(
            [(seq, rows_by_seq[seq][0]) for seq in sorted(rows_by_seq)],
            null_order,
        )
        expected = _reference(rows_by_seq, ordered, funcs)

        assert [g[0] for g in got] == ordered
        for idx, (seq, sk, val, outputs) in enumerate(got):
            assert sk == rows_by_seq[seq][0]
            assert val == rows_by_seq[seq][1]
            for alias, exp_val in expected[idx].items():
                assert outputs[alias] == exp_val, (
                    f"seed={seed} row={idx} alias={alias} "
                    f"got={outputs[alias]!r} expected={exp_val!r}"
                )
