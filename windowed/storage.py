"""持久化存储层。

职责：
- 以 SQLite(WAL) 持久保存已写入的数据行与已提交的计算结果；
- 每次写入批次原子提交并分配单调递增的版本号，查询按版本号读取一致快照
  （行一旦提交永不修改，version <= 快照版本 的行集即为一致快照）；
- batch_id 去重：同一 (dataset, batch_id) 重复提交返回相同版本，不产生重复数据；
- 崩溃恢复：启动时把未完成的查询标记为 failed 并清理其部分结果与溢出数据，
  已提交的数据行与 done 状态的结果保持不变。
"""
from __future__ import annotations

import json
import math
import sqlite3
import threading
import time
from typing import Iterator, Optional

from .types import (
    EngineConfig,
    QueryInfo,
    QuerySpec,
    QueryStatus,
    RowIn,
    normalize_sort_key,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
  k TEXT PRIMARY KEY,
  v INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS batches (
  dataset TEXT NOT NULL,
  batch_id TEXT NOT NULL,
  version INTEGER NOT NULL,
  PRIMARY KEY (dataset, batch_id)
);
CREATE TABLE IF NOT EXISTS rows (
  seq INTEGER PRIMARY KEY AUTOINCREMENT,
  dataset TEXT NOT NULL,
  partition_key TEXT NOT NULL,
  sk_type INTEGER NOT NULL,
  sk_num REAL,
  sk_str TEXT,
  sk_json TEXT NOT NULL,
  value REAL,
  version INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rows_scan
  ON rows(dataset, partition_key, sk_type, sk_num, sk_str, seq);
CREATE TABLE IF NOT EXISTS queries (
  query_id TEXT PRIMARY KEY,
  dataset TEXT NOT NULL,
  partition_key TEXT NOT NULL,
  spec_json TEXT NOT NULL,
  status TEXT NOT NULL,
  error TEXT,
  version INTEGER NOT NULL,
  result_count INTEGER,
  created_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS result_rows (
  query_id TEXT NOT NULL,
  row_index INTEGER NOT NULL,
  payload TEXT NOT NULL,
  PRIMARY KEY (query_id, row_index)
);
CREATE TABLE IF NOT EXISTS spill (
  owner TEXT NOT NULL,
  name TEXT NOT NULL,
  idx INTEGER NOT NULL,
  value REAL,
  PRIMARY KEY (owner, name, idx)
);
CREATE TABLE IF NOT EXISTS spill_text (
  owner TEXT NOT NULL,
  name TEXT NOT NULL,
  idx INTEGER NOT NULL,
  text TEXT NOT NULL,
  PRIMARY KEY (owner, name, idx)
);
"""


class Storage:
    def __init__(self, config: EngineConfig) -> None:
        self._config = config
        self._db_path = config.db_path
        self._wlock = threading.Lock()
        # 测试钩子：每从分区快照读出一行时回调（不用于生产语义）
        self.row_read_hook: Optional[object] = None
        self._wconn = self._connect()
        self._wconn.executescript(_SCHEMA)
        self._wconn.commit()
        self.recover()

    def _connect(self) -> sqlite3.Connection:
        # check_same_thread=False: 写连接由 _wlock 串行化，可被查询线程安全使用
        conn = sqlite3.connect(
            self._db_path, isolation_level=None, timeout=30.0, check_same_thread=False
        )
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def close(self) -> None:
        with self._wlock:
            self._wconn.close()

    @property
    def db_path(self) -> str:
        return self._db_path

    # ---- 写入 ----

    def commit_batch(self, dataset: str, batch_id: Optional[str], rows: list[RowIn]) -> int:
        """原子提交一批行，返回版本号；batch_id 去重保证重复提交不产生重复数据。"""
        records = []
        for r in rows:
            sk_type, sk_num, sk_str = normalize_sort_key(r.sort_key)
            value = r.value
            if value is not None:
                if isinstance(value, bool) or not isinstance(value, (int, float)):
                    raise TypeError(f"value 必须为数值或 None: {value!r}")
                value = float(value)
                if not math.isfinite(value):
                    raise ValueError(f"value 必须为有限数值: {value!r}")
            records.append(
                (dataset, r.partition_key, sk_type, sk_num, sk_str,
                 json.dumps(r.sort_key), value)
            )
        with self._wlock:
            conn = self._wconn
            conn.execute("BEGIN IMMEDIATE")
            try:
                if batch_id is not None:
                    existing = conn.execute(
                        "SELECT version FROM batches WHERE dataset=? AND batch_id=?",
                        (dataset, batch_id),
                    ).fetchone()
                    if existing is not None:
                        conn.execute("COMMIT")
                        return int(existing[0])
                version = self._next_version(conn)
                if batch_id is not None:
                    conn.execute(
                        "INSERT INTO batches(dataset, batch_id, version) VALUES (?,?,?)",
                        (dataset, batch_id, version),
                    )
                conn.executemany(
                    "INSERT INTO rows(dataset, partition_key, sk_type, sk_num, sk_str,"
                    " sk_json, value, version) VALUES (?,?,?,?,?,?,?,?)",
                    [rec + (version,) for rec in records],
                )
                conn.execute("COMMIT")
                return version
            except BaseException:
                conn.execute("ROLLBACK")
                raise

    @staticmethod
    def _next_version(conn: sqlite3.Connection) -> int:
        conn.execute(
            "INSERT INTO meta(k, v) VALUES('version', 0) ON CONFLICT(k) DO NOTHING"
        )
        conn.execute("UPDATE meta SET v = v + 1 WHERE k = 'version'")
        row = conn.execute("SELECT v FROM meta WHERE k = 'version'").fetchone()
        return int(row[0])

    # ---- 快照 ----

    def current_version(self) -> int:
        with self._wlock:
            row = self._wconn.execute(
                "SELECT v FROM meta WHERE k = 'version'"
            ).fetchone()
        return int(row[0]) if row else 0

    def iter_partition(
        self, dataset: str, partition_key: str, version: int
    ) -> Iterator[tuple[int, object, Optional[float]]]:
        """按确定性排序规则流式产出 (seq, sort_key, value)，内存占用有界。"""
        conn = self._connect()
        try:
            cur = conn.execute(
                "SELECT seq, sk_json, value FROM rows"
                " WHERE dataset=? AND partition_key=? AND version<=?"
                " ORDER BY sk_type, sk_num, sk_str, seq",
                (dataset, partition_key, version),
            )
            while True:
                batch = cur.fetchmany(self._config.batch_size)
                if not batch:
                    break
                for seq, sk_json, value in batch:
                    if self.row_read_hook is not None:
                        self.row_read_hook(seq)
                    yield int(seq), json.loads(sk_json), value
        finally:
            conn.close()

    # ---- 查询记录与结果 ----

    def create_query(self, query_id: str, spec: QuerySpec, version: int) -> None:
        spec_json = json.dumps(spec_to_json(spec), ensure_ascii=False, sort_keys=True)
        with self._wlock:
            self._wconn.execute(
                "INSERT INTO queries(query_id, dataset, partition_key, spec_json,"
                " status, error, version, result_count, created_at)"
                " VALUES (?,?,?,?,?,NULL,?,NULL,?)",
                (
                    query_id,
                    spec.dataset,
                    spec.partition_key,
                    spec_json,
                    QueryStatus.PENDING.value,
                    version,
                    time.time(),
                ),
            )

    def set_query_status(
        self,
        query_id: str,
        status: QueryStatus,
        error: Optional[str] = None,
        result_count: Optional[int] = None,
    ) -> None:
        with self._wlock:
            self._wconn.execute(
                "UPDATE queries SET status=?, error=?, result_count=? WHERE query_id=?",
                (status.value, error, result_count, query_id),
            )

    def get_query(self, query_id: str) -> Optional[QueryInfo]:
        with self._wlock:
            row = self._wconn.execute(
                "SELECT query_id, status, dataset, partition_key, version, error,"
                " result_count FROM queries WHERE query_id=?",
                (query_id,),
            ).fetchone()
        if row is None:
            return None
        return QueryInfo(
            query_id=row[0],
            status=QueryStatus(row[1]),
            dataset=row[2],
            partition_key=row[3],
            version=int(row[4]),
            error=row[5],
            result_count=row[6],
        )

    def insert_result_rows(self, query_id: str, start_index: int, payloads: list[str]) -> None:
        if not payloads:
            return
        with self._wlock:
            self._wconn.executemany(
                "INSERT INTO result_rows(query_id, row_index, payload) VALUES (?,?,?)",
                [(query_id, start_index + i, p) for i, p in enumerate(payloads)],
            )

    def read_results(self, query_id: str, offset: int, limit: int) -> list[str]:
        with self._wlock:
            cur = self._wconn.execute(
                "SELECT payload FROM result_rows WHERE query_id=?"
                " ORDER BY row_index LIMIT ? OFFSET ?",
                (query_id, limit, offset),
            )
            return [r[0] for r in cur.fetchall()]

    def delete_query_artifacts(self, query_id: str) -> None:
        """删除某查询的部分结果与溢出数据（取消/超时/失败清理用）。"""
        with self._wlock:
            self._wconn.execute("DELETE FROM result_rows WHERE query_id=?", (query_id,))
            self._wconn.execute("DELETE FROM spill WHERE owner=?", (query_id,))
            self._wconn.execute("DELETE FROM spill_text WHERE owner=?", (query_id,))

    def delete_spill(self, query_id: str) -> None:
        """只删除溢出数据（查询成功后清理临时状态，保留结果）。"""
        with self._wlock:
            self._wconn.execute("DELETE FROM spill WHERE owner=?", (query_id,))
            self._wconn.execute("DELETE FROM spill_text WHERE owner=?", (query_id,))

    def recover(self) -> None:
        """启动恢复：未完成查询标记为 failed，清理其部分结果，已提交数据与结果保持不变。"""
        with self._wlock:
            conn = self._wconn
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "UPDATE queries SET status=?, error='crashed: service restarted'"
                    " WHERE status IN (?, ?)",
                    (
                        QueryStatus.FAILED.value,
                        QueryStatus.PENDING.value,
                        QueryStatus.RUNNING.value,
                    ),
                )
                conn.execute(
                    "DELETE FROM result_rows WHERE query_id IN"
                    " (SELECT query_id FROM queries WHERE status != ?)",
                    (QueryStatus.DONE.value,),
                )
                conn.execute(
                    "DELETE FROM spill WHERE owner IN"
                    " (SELECT query_id FROM queries WHERE status != ?)",
                    (QueryStatus.DONE.value,),
                )
                conn.execute(
                    "DELETE FROM spill_text WHERE owner IN"
                    " (SELECT query_id FROM queries WHERE status != ?)",
                    (QueryStatus.DONE.value,),
                )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise


def spec_to_json(spec: QuerySpec) -> dict:
    return {
        "dataset": spec.dataset,
        "partition_key": spec.partition_key,
        "frame": {"mode": spec.frame.mode.value, "n": spec.frame.n},
        "aggregates": [a.value for a in spec.aggregates],
        "rankings": [r.value for r in spec.rankings],
        "lead_lag": [
            {"kind": l.kind.value, "offset": l.offset, "default": l.default, "name": l.name}
            for l in spec.lead_lag
        ],
        "timeout_ms": spec.timeout_ms,
    }
