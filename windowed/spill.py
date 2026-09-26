"""磁盘溢出的有界内存数据结构。

当滑动窗口或 lead 缓冲超过 config.max_in_memory_window 时，引擎改用本模块中的
结构，其状态全部保存在调用方提供的 SQLite 连接（spill/spill_text 表）里，
内存占用为 O(批大小)，不随分区行数线性增长。

所有结构都带 (owner, name) 前缀：owner 为 query_id，查询结束（含取消/超时/失败）
后由 storage.delete_query_artifacts 统一清理；崩溃重启时由恢复逻辑清理。
"""
from __future__ import annotations

import sqlite3
from typing import Optional


class SpilledQueue:
    """磁盘支撑的 FIFO（用于 lag 等纯 FIFO 场景）。

    内存中只保留 head 下标与长度两个整数。
    """

    def __init__(self, conn: sqlite3.Connection, owner: str, name: str) -> None:
        self._conn = conn
        self._owner = owner
        self._name = name
        self._head = 0
        self._size = 0
        self._next = 0
        cur = conn.execute(
            "SELECT COALESCE(MIN(idx), 0), COUNT(*), COALESCE(MAX(idx) + 1, 0)"
            " FROM spill WHERE owner=? AND name=?",
            (owner, name),
        )
        first, count, nxt = cur.fetchone()
        self._head = int(first)
        self._size = int(count)
        self._next = int(nxt)

    def push(self, value: Optional[float]) -> None:
        self._conn.execute(
            "INSERT INTO spill(owner, name, idx, value) VALUES (?,?,?,?)",
            (self._owner, self._name, self._next, value),
        )
        self._next += 1
        self._size += 1

    def peek(self) -> Optional[float]:
        row = self._conn.execute(
            "SELECT value FROM spill WHERE owner=? AND name=? AND idx=?",
            (self._owner, self._name, self._head),
        ).fetchone()
        return None if row is None else row[0]

    def peek_at(self, j: int) -> Optional[float]:
        """返回距队头 j 个位置的元素（j=0 即队头）。"""
        row = self._conn.execute(
            "SELECT value FROM spill WHERE owner=? AND name=? AND idx=?",
            (self._owner, self._name, self._head + j),
        ).fetchone()
        return None if row is None else row[0]

    def pop(self) -> Optional[float]:
        value = self.peek()
        self._conn.execute(
            "DELETE FROM spill WHERE owner=? AND name=? AND idx=?",
            (self._owner, self._name, self._head),
        )
        self._head += 1
        self._size -= 1
        return value

    def __len__(self) -> int:
        return self._size

    def drop(self) -> None:
        self._conn.execute(
            "DELETE FROM spill WHERE owner=? AND name=?", (self._owner, self._name)
        )


class SpilledTextMap:
    """磁盘支撑的 idx -> 文本 映射（用于溢出模式下延迟发出的结果负载）。"""

    def __init__(self, conn: sqlite3.Connection, owner: str, name: str) -> None:
        self._conn = conn
        self._owner = owner
        self._name = name

    def put(self, idx: int, text: str) -> None:
        # 同一 idx 可能被多次回填（聚合回填、lead 回填），使用 upsert
        self._conn.execute(
            "INSERT INTO spill_text(owner, name, idx, text) VALUES (?,?,?,?)"
            " ON CONFLICT(owner, name, idx) DO UPDATE SET text=excluded.text",
            (self._owner, self._name, idx, text),
        )

    def get(self, idx: int) -> Optional[str]:
        row = self._conn.execute(
            "SELECT text FROM spill_text WHERE owner=? AND name=? AND idx=?",
            (self._owner, self._name, idx),
        ).fetchone()
        return None if row is None else row[0]

    def delete(self, idx: int) -> None:
        self._conn.execute(
            "DELETE FROM spill_text WHERE owner=? AND name=? AND idx=?",
            (self._owner, self._name, idx),
        )

    def drop(self) -> None:
        self._conn.execute(
            "DELETE FROM spill_text WHERE owner=? AND name=?",
            (self._owner, self._name),
        )


class SpilledWindow:
    """磁盘支撑的滑动窗口状态：

    - vals: 当前窗口元素 (pos, value)，淘汰时返回被淘汰的值，供引擎维护 sum/count；
    - minq: 单调递增队列，队首为窗口最小值；
    - maxq: 单调递减队列，队首为窗口最大值。

    NULL value 只进 vals（计入窗口行数），不参与 min/max/sum/avg（SQL 语义）。
    """

    def __init__(self, conn: sqlite3.Connection, owner: str, name: str) -> None:
        self._conn = conn
        self._owner = owner
        self._vals = f"{name}##vals"
        self._minq = f"{name}##min"
        self._maxq = f"{name}##max"

    def add(self, pos: int, value: Optional[float]) -> None:
        self._conn.execute(
            "INSERT INTO spill(owner, name, idx, value) VALUES (?,?,?,?)",
            (self._owner, self._vals, pos, value),
        )
        if value is not None:
            # 单调队列：从队尾删除不优于新值的元素（按值的后缀删除）
            self._conn.execute(
                "DELETE FROM spill WHERE owner=? AND name=? AND value >= ?",
                (self._owner, self._minq, value),
            )
            self._conn.execute(
                "INSERT INTO spill(owner, name, idx, value) VALUES (?,?,?,?)",
                (self._owner, self._minq, pos, value),
            )
            self._conn.execute(
                "DELETE FROM spill WHERE owner=? AND name=? AND value <= ?",
                (self._owner, self._maxq, value),
            )
            self._conn.execute(
                "INSERT INTO spill(owner, name, idx, value) VALUES (?,?,?,?)",
                (self._owner, self._maxq, pos, value),
            )

    def evict_before(self, keep_from: int) -> list[Optional[float]]:
        """淘汰 pos < keep_from 的元素，返回被淘汰值（含 NULL）。"""
        rows = self._conn.execute(
            "DELETE FROM spill WHERE owner=? AND name=? AND idx < ? RETURNING value",
            (self._owner, self._vals, keep_from),
        ).fetchall()
        self._conn.execute(
            "DELETE FROM spill WHERE owner=? AND name=? AND idx < ?",
            (self._owner, self._minq, keep_from),
        )
        self._conn.execute(
            "DELETE FROM spill WHERE owner=? AND name=? AND idx < ?",
            (self._owner, self._maxq, keep_from),
        )
        return [r[0] for r in rows]

    def min(self) -> Optional[float]:
        row = self._conn.execute(
            "SELECT value FROM spill WHERE owner=? AND name=? ORDER BY value ASC LIMIT 1",
            (self._owner, self._minq),
        ).fetchone()
        return None if row is None else float(row[0])

    def max(self) -> Optional[float]:
        row = self._conn.execute(
            "SELECT value FROM spill WHERE owner=? AND name=? ORDER BY value DESC LIMIT 1",
            (self._owner, self._maxq),
        ).fetchone()
        return None if row is None else float(row[0])

    def drop(self) -> None:
        for n in (self._vals, self._minq, self._maxq):
            self._conn.execute(
                "DELETE FROM spill WHERE owner=? AND name=?", (self._owner, n)
            )
