"""公共夹具与辅助函数。"""
from __future__ import annotations

import json
import os
import tempfile
import time
from dataclasses import replace

import pytest

from windowed.engine import QueryEngine
from windowed.storage import Storage
from windowed.types import EngineConfig, QueryStatus, TERMINAL_STATES


@pytest.fixture
def tmpdir_path():
    d = tempfile.mkdtemp(prefix="windowed-test-")
    yield d


@pytest.fixture
def services(tmpdir_path):
    """返回 (storage, engine, config)，测试结束统一关闭。"""
    cfg = EngineConfig(
        db_path=os.path.join(tmpdir_path, "w.db"),
        batch_size=64,
        max_in_memory_window=1000,
        default_timeout_ms=10_000,
    )
    storage = Storage(cfg)
    engine = QueryEngine(storage, cfg)
    yield storage, engine, cfg
    engine.shutdown()
    storage.close()


def make_services(db_path: str, **cfg_over):
    defaults = dict(
        batch_size=64,
        max_in_memory_window=1000,
        default_timeout_ms=10_000,
    )
    defaults.update(cfg_over)
    cfg = replace(EngineConfig(), db_path=db_path, **defaults)
    storage = Storage(cfg)
    engine = QueryEngine(storage, cfg)
    return storage, engine, cfg


def wait_terminal(engine, query_id, timeout=15.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        info = engine.get_info(query_id)
        if info is not None and info.status in TERMINAL_STATES:
            return info
        time.sleep(0.005)
    raise AssertionError("查询未在规定时间内结束")


def run_sync(engine, storage, spec, timeout=15.0):
    """提交查询并等待完成，返回 (info, list[dict])；未完成则断言失败。"""
    qid = engine.submit(spec)
    info = wait_terminal(engine, qid, timeout)
    assert info.status == QueryStatus.DONE, (info.status, info.error)
    payloads = storage.read_results(qid, 0, 1_000_000)
    return info, [json.loads(p) for p in payloads]
