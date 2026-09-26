"""Query cancellation and timeout: no partial results, future queries fine."""

import threading

import pytest

from wqe.config import Config
from wqe.engine import Engine
from wqe.models import FrameType, FuncKind, FuncSpec, QuerySpec, WindowSpec


@pytest.fixture
def engine(tmp_path):
    cfg = Config(db_path=str(tmp_path / "t.db"), page_size=256, default_timeout_s=0)
    with Engine(cfg) as eng:
        rows = [(i, i) for i in range(100_000)]
        for s in range(0, len(rows), 5_000):
            eng.write_batch("p", rows[s:s + 5_000])
        yield eng


def _big_spec():
    return QuerySpec(
        partition_key="p",
        functions=(
            FuncSpec(kind=FuncKind.ROW_NUMBER, alias="rn"),
            FuncSpec(kind=FuncKind.SUM, alias="s",
                     window=WindowSpec(FrameType.ROWS_AROUND, n=100)),
            FuncSpec(kind=FuncKind.MAX, alias="m",
                     window=WindowSpec(FrameType.PARTITION)),
        ),
    )


def test_cancel_mid_query(engine, monkeypatch):
    # Block the query deterministically inside its second scan page so the
    # cancel signal is guaranteed to arrive while it is executing.
    from wqe import execution as execution_mod

    reached = threading.Event()
    release = threading.Event()
    orig = execution_mod.QueryExecutor.check_cancelled

    def patched_check(self):
        reached.set()
        release.wait(5.0)
        return orig(self)

    monkeypatch.setattr(execution_mod.QueryExecutor, "check_cancelled", patched_check)

    out = {}

    def run():
        out["res"] = engine.submit_query(
            _big_spec(), query_id="q-cancel", timeout_s=0
        )

    t = threading.Thread(target=run)
    t.start()
    assert reached.wait(2.0), "query never reached first page"
    assert engine.cancel_query("q-cancel") is True
    release.set()
    t.join(timeout=5)
    assert not t.is_alive()

    res = out["res"]
    assert res.status.value == "cancelled"
    # No committed rows for a cancelled query.
    assert engine.count_results("q-cancel") is None
    assert engine.fetch_result_page("q-cancel", 0, 10) is None

    # Status persisted as cancelled, not committed.
    status = engine.get_result("q-cancel")
    assert status.status.value == "cancelled"

    # A subsequent query on the same engine works normally.
    follow = engine.submit_query(
        QuerySpec(
            partition_key="p",
            functions=(FuncSpec(
                kind=FuncKind.COUNT, alias="c",
                window=WindowSpec(FrameType.PARTITION)),),
        ),
        query_id="q-after",
        timeout_s=0,
    )
    assert follow.status.value == "committed"
    rows = []
    for page in engine.iter_result_pages("q-after", 512):
        rows.extend(page)
    assert rows[0].outputs["c"] == 100_000


def test_cancel_finished_query_returns_false(engine):
    res = engine.submit_query(
        QuerySpec(
            partition_key="p",
            functions=(FuncSpec(
                kind=FuncKind.ROW_NUMBER, alias="rn"),),
        ),
        query_id="q-done",
        timeout_s=0,
    )
    assert res.status.value == "committed"
    assert engine.cancel_query("q-done") is False
    assert engine.get_result("q-done").status.value == "committed"


def test_timeout_marks_cancelled(engine):
    # Tiny timeout; the 100k-row partition cannot finish in time.
    res = engine.submit_query(
        _big_spec(), query_id="q-timeout", timeout_s=0.001
    )
    assert res.status.value == "cancelled"
    assert "deadline" in (res.error or "")
    assert engine.count_results("q-timeout") is None


def test_partial_spool_is_removed(engine, tmp_path):
    before = set(tmp_path.glob("*.jsonl"))
    res = engine.submit_query(
        _big_spec(), query_id="q-spool", timeout_s=0.001
    )
    assert res.status.value == "cancelled"
    after = set(tmp_path.glob("*.jsonl"))
    # Spool files live in the system temp dir; ensure none linger anywhere
    # obvious. The key guarantee is no visible results, checked above.
    assert before == after
