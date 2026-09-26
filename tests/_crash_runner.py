"""崩溃恢复测试用的子进程：在查询执行到一半时以 os._exit 立即退出（不执行清理）。

参数：db_path n batch_size
- 打开已有数据库（数据由父进程提前写入）；
- 提交一个查询，读取若干行后阻塞；
- 看门狗线程确认已落盘部分结果后 os._exit(0) 模拟进程崩溃。
"""
from __future__ import annotations

import os
import sys
import threading
import time

from windowed.engine import QueryEngine
from windowed.storage import Storage
from windowed.types import AggFunc, EngineConfig, FrameMode, FrameSpec, QuerySpec, QueryStatus


def main() -> None:
    db_path, n_str, bs_str = sys.argv[1], sys.argv[2], sys.argv[3]
    cfg = EngineConfig(db_path=db_path, batch_size=int(bs_str),
                       max_in_memory_window=4, default_timeout_ms=300_000)
    storage = Storage(cfg)
    engine = QueryEngine(storage, cfg)
    n = int(n_str)

    blocked = threading.Event()

    def hook(seq):
        if seq >= 8 and not blocked.is_set():
            blocked.set()
            time.sleep(1000)  # 查询线程卡在中途，等待看门狗杀死进程

    storage.row_read_hook = hook
    qid = engine.submit(QuerySpec(
        "ds", "p", frame=FrameSpec(FrameMode.PRECEDING, 5),
        aggregates=tuple(AggFunc)))

    # 等查询进入 RUNNING 且至少一批部分结果已落盘（证明崩溃发生在计算中途）
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        info = storage.get_query(qid)
        partial = storage.read_results(qid, 0, 1000)
        if info and info.status == QueryStatus.RUNNING and len(partial) >= 4:
            os._exit(17)  # 模拟崩溃：无 finally、无 atexit、无 SQLite 优雅关闭
        time.sleep(0.02)
    sys.stderr.write("crash runner: preconditions not met\n")
    os._exit(18)


if __name__ == "__main__":
    main()
