"""HTTP API 端到端测试：写入、提交、分页结果、取消、错误处理、重启一致性。"""
from __future__ import annotations

import os
import threading
import time

from fastapi.testclient import TestClient

from windowed.service import create_app

from windowed.types import EngineConfig, RowIn
from dataclasses import replace


def _client(db, **over):
    defaults = dict(batch_size=64, max_in_memory_window=1000, default_timeout_ms=10_000)
    defaults.update(over)
    cfg = replace(EngineConfig(), db_path=db, **defaults)
    app = create_app(cfg)
    # 必须复用 app 内的 storage/engine，钩子与数据才作用于查询线程
    return TestClient(app), app.state.storage, app.state.engine


def test_health_write_query_results_pagination(tmpdir_path):
    client, st, en = _client(os.path.join(tmpdir_path, "api.db"), batch_size=4)
    with client:
        r = client.get("/health")
        assert r.status_code == 200 and r.json()["version"] == 0

        body = {"rows": [
            {"partition_key": "p", "sort_key": None, "value": 1.0},
            {"partition_key": "p", "sort_key": 1, "value": None},
            {"partition_key": "p", "sort_key": 1, "value": 3.0},
            {"partition_key": "p", "sort_key": 5, "value": 7.0},
        ]}
        r = client.post("/datasets/ds/rows?batch_id=z", json=body)
        assert r.status_code == 200
        assert r.json() == {"dataset": "ds", "accepted": 4, "version": 1}
        # 幂等重放
        assert client.post("/datasets/ds/rows?batch_id=z", json=body).json()["version"] == 1

        q = {
            "dataset": "ds", "partition_key": "p",
            "frame": {"mode": "preceding", "n": 1},
            "aggregates": ["sum", "count", "min", "max"],
            "rankings": ["row_number", "rank"],
            "lead_lag": [{"kind": "lead", "offset": 1}],
        }
        r = client.post("/queries", json=q)
        assert r.status_code == 202
        qid = r.json()["query_id"]
        assert r.json()["version"] == 1

        for _ in range(100):
            stt = client.get(f"/queries/{qid}").json()
            if stt["status"] == "done":
                break
            time.sleep(0.02)
        assert stt["status"] == "done" and stt["result_count"] == 4

        page1 = client.get(f"/queries/{qid}/results?offset=0&limit=2").json()
        page2 = client.get(f"/queries/{qid}/results?offset=2&limit=2").json()
        assert [x["row_index"] for x in page1["rows"]] == [0, 1]
        assert [x["row_index"] for x in page2["rows"]] == [2, 3]
        assert page1["total"] == 4
        row1 = page1["rows"][1]
        assert row1["rank"] == 2 and row1["sum"] == 1.0  # 窗口 [None-row,(1,None)]


def test_api_validation_errors(tmpdir_path):
    client, st, en = _client(os.path.join(tmpdir_path, "v.db"))
    with client:
        assert client.post("/datasets/ds/rows", json={"rows": []}).status_code == 400
        assert client.post("/datasets/ds/rows", json={"rows": [
            {"partition_key": "p", "sort_key": [1, 2]}]}).status_code == 400
        assert client.post("/datasets/ds/rows", json={"rows": [
            {"partition_key": "p", "sort_key": 1, "value": "NaN-ish"}]}).status_code in (400, 422)
        base = {"dataset": "ds", "partition_key": "p"}
        assert client.post("/queries", json={**base, "aggregates": ["oops"]}).status_code == 400
        assert client.post("/queries", json={**base, "rankings": ["oops"]}).status_code == 400
        assert client.post("/queries", json={**base, "lead_lag": [
            {"kind": "sideways"}]}).status_code == 400
        assert client.post("/queries", json={**base, "frame": {"mode": "bad"}}).status_code == 400
        assert client.post("/queries", json={**base, "lead_lag": [
            {"kind": "lag", "offset": 0}]}).status_code == 400
        assert client.post("/queries", json={**base, "timeout_ms": 0}).status_code == 400


def test_api_cancel_hides_partial_results(tmpdir_path):
    db = os.path.join(tmpdir_path, "c.db")
    client, st, en = _client(db, batch_size=4)
    with client:
        st.commit_batch("ds", None, [RowIn("p", i, float(i)) for i in range(200)])
        started = threading.Event()
        release = threading.Event()

        def hook(seq):
            if seq >= 6:
                started.set()
                release.wait(timeout=15)

        st.row_read_hook = hook
        r = client.post("/queries", json={
            "dataset": "ds", "partition_key": "p",
            "frame": {"mode": "around", "n": 2}, "aggregates": ["sum"]})
        qid = r.json()["query_id"]
        assert started.wait(timeout=10)
        cr = client.post(f"/queries/{qid}/cancel")
        assert cr.status_code == 200 and cr.json()["cancel_requested"] is True
        release.set()
        st.row_read_hook = None
        for _ in range(100):
            stt = client.get(f"/queries/{qid}").json()
            if stt["status"] in ("cancelled", "timeout", "failed", "done"):
                break
            time.sleep(0.02)
        assert stt["status"] == "cancelled"
        # 未完成/已取消：结果接口返回 409，绝不泄露部分结果
        rr = client.get(f"/queries/{qid}/results")
        assert rr.status_code == 409 and rr.json()["status"] == "cancelled"


def test_api_timeout_and_recovery_after_restart(tmpdir_path):
    db = os.path.join(tmpdir_path, "r.db")
    # 先在一次进程生命周期内制造超时
    client, st, en = _client(db, batch_size=4)
    with client:
        st.commit_batch("ds", None, [RowIn("p", i, float(i)) for i in range(40)])

        def slow(seq):
            time.sleep(0.05)

        st.row_read_hook = slow
        r = client.post("/queries", json={
            "dataset": "ds", "partition_key": "p", "aggregates": ["sum"],
            "timeout_ms": 80})
        qid = r.json()["query_id"]
        for _ in range(200):
            stt = client.get(f"/queries/{qid}").json()
            if stt["status"] in ("timeout", "done", "cancelled"):
                break
            time.sleep(0.02)
        st.row_read_hook = None
        assert stt["status"] == "timeout"
        assert client.get(f"/queries/{qid}/results").status_code == 409

    # 新进程重新打开同一库：数据仍在，可正常查询
    client2, st2, en2 = _client(db, batch_size=4)
    with client2:
        r = client2.post("/queries", json={
            "dataset": "ds", "partition_key": "p", "aggregates": ["count"]})
        qid2 = r.json()["query_id"]
        for _ in range(200):
            stt = client2.get(f"/queries/{qid2}").json()
            if stt["status"] == "done":
                break
            time.sleep(0.02)
        assert stt["status"] == "done"
        rows = client2.get(f"/queries/{qid2}/results").json()["rows"]
        assert len(rows) == 40 and rows[0]["count"] == 40
        # 旧的超时查询记录仍在，但状态保持 timeout，无结果
        old = client2.get(f"/queries/{qid}").json()
        assert old["status"] == "timeout"
        assert client2.get(f"/queries/{qid}/results").status_code == 409
