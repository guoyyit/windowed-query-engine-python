"""Crash recovery: committed data/results survive; unfinished queries don't."""

import os
import signal
import subprocess
import sys

from wqe.config import Config
from wqe.engine import Engine
from wqe.models import FrameType, FuncKind, FuncSpec, QuerySpec, WindowSpec

SCRIPT = r'''
import os, sys, time
sys.path.insert(0, {cwd!r})
from wqe.config import Config
from wqe.engine import Engine
from wqe.models import *

mode = sys.argv[1]
db = sys.argv[2]
cfg = Config(db_path=db, page_size=128, default_timeout_s=0)
eng = Engine(cfg)
if mode == "write":
    # Two atomic batches in two partitions.
    eng.write_batch("p", [(i, i) for i in range(100)])
    eng.write_batch("p", [(i, i) for i in range(100, 250)])
    spec = QuerySpec(
        partition_key="p",
        functions=(FuncSpec(kind=FuncKind.COUNT, alias="c",
                            window=WindowSpec(FrameType.PARTITION)),),
    )
    res = eng.submit_query(spec, query_id="qdone", timeout_s=0)
    # Touch the result count so pages are definitely persisted.
    assert eng.count_results("qdone") == 250
    print("committed", flush=True)
    if mode == "write":
        # Register a second query then crash hard mid-execution.
        eng.storage.register_query("qhalf", spec)
        eng._cancel_events  # ensure attribute exists
        print("ready_to_crash", flush=True)
        time.sleep(30)
elif mode == "reopen":
    print("opened", flush=True)
    time.sleep(30)
eng.close()
'''


def _start(db_path, mode):
    cwd = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    code = SCRIPT.format(cwd=cwd)
    proc = subprocess.Popen(
        [sys.executable, "-c", code, mode, str(db_path)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return proc


def _wait_line(proc, needle, timeout=20):
    deadline = time.time() + timeout
    lines = []
    while time.time() < deadline:
        line = proc.stdout.readline()
        if not line:
            if proc.poll() is not None:
                err = proc.stderr.read()
                raise AssertionError(f"process exited early: {err}")
            time.sleep(0.02)
            continue
        lines.append(line.strip())
        if needle in line:
            return
    raise AssertionError(f"never saw {needle!r}; got {lines}")


import time


def test_committed_data_and_results_survive_hard_kill(tmp_path):
    db = tmp_path / "r.db"
    proc = _start(db, "write")
    try:
        _wait_line(proc, "ready_to_crash")
        # SIGKILL: no atexit handlers, no graceful close -- a real crash.
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=5)
    finally:
        if proc.poll() is None:
            proc.kill()

    # Reopen with a fresh process: recovery runs on startup.
    proc2 = _start(db, "reopen")
    try:
        _wait_line(proc2, "opened", timeout=20)
    finally:
        proc2.send_signal(signal.SIGKILL)
        proc2.wait(timeout=5)

    # Verify in-process with our own engine instance.
    cfg = Config(db_path=str(db), page_size=128, default_timeout_s=0)
    with Engine(cfg) as eng:
        conn = eng.storage.begin_snapshot()
        try:
            assert eng.storage.count_rows(conn, "p") == 250
        finally:
            eng.storage.release(conn)

        # Completed query still committed with exactly its 250 rows.
        assert eng.get_result("qdone").status.value == "committed"
        assert eng.count_results("qdone") == 250
        rows = []
        for page in eng.iter_result_pages("qdone", 64):
            rows.extend(page)
        assert len(rows) == 250
        assert all(r.outputs["c"] == 250 for r in rows)

        # Unfinished query was recovered to cancelled, never committed.
        status = eng.get_result("qhalf")
        assert status.status.value == "cancelled"
        assert eng.count_results("qhalf") is None


def test_reopen_is_idempotent(tmp_path):
    db = tmp_path / "r2.db"
    cfg = Config(db_path=str(db), default_timeout_s=0)
    with Engine(cfg) as eng:
        eng.write_batch("x", [(1, 1)])
    # Reopen twice; recovery must not touch committed rows.
    for _ in range(2):
        with Engine(Config(db_path=str(db), default_timeout_s=0)) as eng:
            conn = eng.storage.begin_snapshot()
            try:
                assert eng.storage.count_rows(conn, "x") == 1
            finally:
                eng.storage.release(conn)


def test_write_and_query_after_recovery(tmp_path):
    db = str(tmp_path / "r3.db")
    with Engine(Config(db_path=db, page_size=64, default_timeout_s=0)) as eng:
        eng.write_batch("p", [(1, 10), (2, 20)])

    # Simulate a crash: register a running query directly then reopen.
    with Engine(Config(db_path=db, page_size=64, default_timeout_s=0)) as eng:
        eng.storage.register_query(
            "orphan",
            QuerySpec(partition_key="p", functions=(
                FuncSpec(kind=FuncKind.ROW_NUMBER, alias="rn"),)),
        )
        # close WITHOUT finishing -> status remains "running" on disk
        eng.storage.recover = lambda: None  # disable recovery on this close

    # New process/engine runs recovery on startup.
    with Engine(Config(db_path=db, page_size=64, default_timeout_s=0)) as eng:
        assert eng.get_result("orphan").status.value == "cancelled"
        # Existing data intact.
        conn = eng.storage.begin_snapshot()
        try:
            assert eng.storage.count_rows(conn, "p") == 2
        finally:
            eng.storage.release(conn)
        # New writes and a new query both work.
        eng.write_batch("p", [(3, 30)])
        spec = QuerySpec(
            partition_key="p",
            functions=(FuncSpec(
                kind=FuncKind.SUM, alias="s",
                window=WindowSpec(FrameType.PARTITION)),),
        )
        res = eng.submit_query(spec, query_id="newq", timeout_s=0)
        assert res.status.value == "committed"
        rows = []
        for page in eng.iter_result_pages("newq", 64):
            rows.extend(page)
        assert rows[0].outputs["s"] == 60
