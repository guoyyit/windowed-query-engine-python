"""崩溃恢复：已提交数据/结果不丢失不重复，未完成计算不被当作完成结果返回。"""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys

from windowed.types import (
    AggFunc,
    FrameMode,
    FrameSpec,
    QuerySpec,
    QueryStatus,
    RankFunc,
    RowIn,
)

from .conftest import make_services, run_sync, wait_terminal

ALL_AGGS = tuple(AggFunc)


def test_committed_data_and_done_results_survive_restart(tmpdir_path):
    db = os.path.join(tmpdir_path, "restart.db")
    st, en, cfg = make_services(db, batch_size=8)
    rows = [RowIn("p", i, None if i % 5 == 0 else float(i)) for i in range(60)]
    st.commit_batch("ds", "b1", rows)
    st.commit_batch("ds", "b2", [RowIn("p2", 1, 5.0)])
    info, before = run_sync(en, st, QuerySpec(
        "ds", "p", frame=FrameSpec(FrameMode.AROUND, 2),
        aggregates=ALL_AGGS, rankings=(RankFunc.RANK,)))
    qid = info.query_id
    en.shutdown()
    st.close()

    # 模拟正常重启
    st2, en2, _ = make_services(db, batch_size=8)
    info2 = st2.get_query(qid)
    assert info2.status == QueryStatus.DONE
    assert info2.result_count == 60
    after = [json.loads(p) for p in st2.read_results(qid, 0, 1000)]
    assert after == before                 # 结果原样保留
    assert st2.current_version() == 2
    _, p2 = run_sync(en2, st2, QuerySpec("ds", "p2", aggregates=(AggFunc.SUM,)))
    assert p2[0]["sum"] == 5.0
    en2.shutdown()
    st2.close()


def test_replay_same_batch_after_restart_no_duplicates(tmpdir_path):
    db = os.path.join(tmpdir_path, "replay.db")
    st, en, _ = make_services(db)
    rows = [RowIn("p", i, float(i)) for i in range(30)]
    v1 = st.commit_batch("ds", "dup-batch", rows)
    en.shutdown()
    st.close()

    st2, en2, _ = make_services(db)
    v2 = st2.commit_batch("ds", "dup-batch", rows)  # 崩溃后重放
    assert v1 == v2
    _, got = run_sync(en2, st2, QuerySpec("ds", "p", aggregates=(AggFunc.COUNT,)))
    assert len(got) == 30
    assert got[0]["count"] == 30
    en2.shutdown()
    st2.close()


def test_recovered_unfinished_query_not_returned_as_done(tmpdir_path):
    db = os.path.join(tmpdir_path, "mark.db")
    st, en, cfg = make_services(db, batch_size=8, max_in_memory_window=2)
    st.commit_batch("ds", None, [RowIn("p", i, float(i)) for i in range(50)])
    info, _ = run_sync(en, st, QuerySpec(
        "ds", "p", frame=FrameSpec(FrameMode.AROUND, 2), aggregates=ALL_AGGS))
    done_qid = info.query_id

    # 手工构造一次"崩溃在运行中"的查询：RUNNING + 部分结果 + 溢出数据
    stuck = QuerySpec("ds", "p", frame=FrameSpec(FrameMode.PRECEDING, 10),
                      aggregates=ALL_AGGS)
    st.create_query("stuck1", stuck, version=st.current_version())
    st.set_query_status("stuck1", QueryStatus.RUNNING)
    st.insert_result_rows("stuck1", 0, ['{"partial":1}', '{"partial":2}'])
    conn = sqlite3.connect(db)
    conn.execute("INSERT INTO spill(owner,name,idx,value) VALUES('stuck1','x',0,1.0)")
    conn.commit()
    conn.close()
    en.shutdown()
    st.close()

    st2, en2, cfg2 = make_services(db, batch_size=8, max_in_memory_window=2)
    info_stuck = st2.get_query("stuck1")
    assert info_stuck.status == QueryStatus.FAILED
    assert "restarted" in (info_stuck.error or "")
    # 部分结果与溢出数据不得残留、不得可见
    assert st2.read_results("stuck1", 0, 10) == []
    conn = sqlite3.connect(db)
    assert conn.execute("SELECT COUNT(*) FROM spill").fetchone()[0] == 0
    assert conn.execute("SELECT COUNT(*) FROM result_rows WHERE query_id='stuck1'").fetchone()[0] == 0
    conn.close()
    # 已完成查询仍可用
    assert st2.get_query(done_qid).status == QueryStatus.DONE
    # 重启后可正常提交新查询
    vals = [float(i) for i in range(50)]  # 本测试数据集无 NULL
    _, rows = run_sync(en2, st2, stuck)
    assert len(rows) == 50
    w = [v for v in vals[39:50] if v is not None]  # PRECEDING 10 在末行的窗口
    assert rows[49]["sum"] == sum(w)
    en2.shutdown()
    st2.close()


def test_hard_crash_mid_query_recovers(tmpdir_path):
    """真·崩溃：子进程在查询中途 os._exit，父进程重新打开库完成恢复。"""
    db = os.path.join(tmpdir_path, "hard.db")
    n, bs = 40, 4
    st, en, cfg = make_services(db, batch_size=bs, max_in_memory_window=4)
    st.commit_batch("ds", None,
                    [RowIn("p", i, float(i) if i % 3 else None) for i in range(n)])
    en.shutdown()
    st.close()

    runner = os.path.join(os.path.dirname(__file__), "_crash_runner.py")
    env = dict(os.environ)
    env["PYTHONPATH"] = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    proc = subprocess.run(
        [sys.executable, runner, db, str(n), str(bs)],
        capture_output=True, text=True, timeout=60, env=env,
    )
    assert proc.returncode == 17, (proc.returncode, proc.stderr)

    # 直接验证磁盘上确实留下了 RUNNING 查询（证明不是优雅退出）
    conn = sqlite3.connect(db)
    running_cnt = conn.execute(
        "SELECT COUNT(*) FROM queries WHERE status='running'").fetchone()[0]
    conn.close()
    assert running_cnt >= 1

    st2, en2, _ = make_services(db, batch_size=bs, max_in_memory_window=4)
    conn = sqlite3.connect(db)
    crashed_q = conn.execute(
        "SELECT query_id FROM queries WHERE status='failed' ORDER BY created_at DESC LIMIT 1"
    ).fetchone()[0]
    conn.close()
    assert st2.get_query(crashed_q).status == QueryStatus.FAILED
    assert st2.read_results(crashed_q, 0, 100) == []

    info, rows = run_sync(en2, st2, QuerySpec(
        "ds", "p", frame=FrameSpec(FrameMode.PRECEDING, 5),
        aggregates=ALL_AGGS))
    assert info.result_count == n
    vals = [float(i) if i % 3 else None for i in range(n)]
    for i in (0, n - 1):
        w = [v for v in vals[max(0, i - 5): i + 1] if v is not None]
        assert rows[i]["sum"] == (sum(w) if w else None)
        assert rows[i]["count"] == len(w)
    en2.shutdown()
    st2.close()
