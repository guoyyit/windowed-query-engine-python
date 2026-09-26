"""Persistent storage backed by SQLite (WAL journal for file databases).

Tables
------
``rows``
    Committed input rows.  Ordering is fully materialised in index columns
    so that ordered scans need no in-Python sorting:

    * ``sort_null`` -- 0 for non-NULL keys, 1 for NULL keys (the value is
      flipped depending on NULLS FIRST/LAST at query time);
    * ``sort_value`` -- the sort key in a *no-affinity* column, so ints
      stay INTEGER and floats stay REAL and SQLite compares them numerically
      without precision loss; numbers order before text per SQLite rules;
    * ``seq`` -- insertion order, the final, globally unique tie-breaker.

``queries`` / ``results``
    Query lifecycle records and committed output rows.  Results are inserted
    in the *same* transaction that flips status to ``committed``; therefore a
    query is either fully visible or not visible at all.

Snapshot isolation
------------------
Each query reads through one connection inside a single ``BEGIN``
transaction.  In WAL mode SQLite keeps the snapshot of the first read for
the whole transaction, so concurrent writers never produce a mixed
old/new view.  Writes are serialised with an in-process lock.
"""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from collections.abc import Iterator
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # avoid import cycle at runtime
    from .execution import SpooledResult as SpooledResultLike

from .models import (
    NullOrder,
    QuerySpec,
    QueryStatus,
    ResultRow,
    Row,
    Scalar,
)

_SCHEMA = """
CREATE TABLE IF NOT EXISTS rows (
    seq            INTEGER PRIMARY KEY AUTOINCREMENT,
    partition_key  TEXT NOT NULL,
    sort_null      INTEGER NOT NULL,
    sort_value     BLOB,
    sort_json      TEXT NOT NULL,
    value_json     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rows_part_order
    ON rows(partition_key, sort_null, sort_value, seq);

CREATE TABLE IF NOT EXISTS queries (
    query_id    TEXT PRIMARY KEY,
    spec_json   TEXT NOT NULL,
    status      TEXT NOT NULL,
    error       TEXT,
    created_at  REAL NOT NULL,
    finished_at REAL
);

CREATE TABLE IF NOT EXISTS results (
    query_id     TEXT NOT NULL,
    row_index    INTEGER NOT NULL,
    seq          INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    PRIMARY KEY(query_id, row_index)
) WITHOUT ROWID;
"""


def _encode(value: Scalar) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


def _decode(raw: str) -> Scalar:
    return json.loads(raw)


def validate_sort_key(value: Scalar) -> None:
    """Sort keys must be NULL or a scalar that stores deterministically."""
    if value is not None and not isinstance(value, (bool, int, float, str)):
        raise TypeError(f"unsupported sort key type: {type(value).__name__}")


def validate_value(value: Scalar) -> None:
    if value is not None and not isinstance(value, (bool, int, float, str)):
        raise TypeError(f"unsupported value type: {type(value).__name__}")


class Storage:
    """Thread-safe SQLite-backed persistent storage."""

    def __init__(self, db_path: str, busy_timeout_ms: int = 5000) -> None:
        self.db_path = db_path
        self.busy_timeout_ms = busy_timeout_ms
        self._write_lock = threading.Lock()
        self._local = threading.local()
        # For shared-cache in-memory DBs a live connection keeps the data.
        self._sentinel: sqlite3.Connection | None = self._connect()
        self._init_schema()

    # -- connection management ---------------------------------------------

    def _target(self) -> str:
        if self.db_path == ":memory:":
            return (
                f"file:wqe_mem_{id(self)}?mode=memory&cache=shared"
            )
        return self.db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self._target(),
            uri=self.db_path == ":memory:",
            isolation_level=None,  # explicit transaction control
            check_same_thread=False,
            timeout=self.busy_timeout_ms / 1000.0,
        )
        conn.row_factory = sqlite3.Row
        conn.execute(f"PRAGMA busy_timeout = {int(self.busy_timeout_ms)}")
        if self.db_path != ":memory:":
            conn.execute("PRAGMA journal_mode = WAL")
            conn.execute("PRAGMA synchronous = FULL")
        else:
            conn.execute("PRAGMA journal_mode = MEMORY")
        return conn

    def _writer(self) -> sqlite3.Connection:
        conn = getattr(self._local, "writer", None)
        if conn is None:
            conn = self._connect()
            self._local.writer = conn
        return conn

    def _init_schema(self) -> None:
        # ``executescript`` implicitly commits any open transaction, so the
        # DDL script manages its own (implicit) transaction.
        conn = self._writer()
        with self._write_lock:
            conn.executescript(_SCHEMA)

    def begin_snapshot(self) -> sqlite3.Connection:
        """Return a connection whose transaction holds one consistent snapshot.

        SQLite starts deferred transactions lazily: the read snapshot is only
        fixed at the *first* statement.  We therefore issue a trivial read
        immediately after BEGIN so the snapshot is fixed at the moment this
        method returns, regardless of when the caller starts scanning.
        """
        conn = self._connect()
        conn.execute("BEGIN")
        conn.execute("SELECT 1 FROM rows LIMIT 1").fetchall()
        return conn

    @staticmethod
    def release(conn: sqlite3.Connection) -> None:
        """End the snapshot transaction and close the connection."""
        try:
            conn.execute("COMMIT")
        except sqlite3.Error:
            conn.execute("ROLLBACK")
        finally:
            conn.close()

    # -- writes ------------------------------------------------------------

    def insert_rows(
        self, partition_key: str, rows: list[tuple[Scalar, Scalar]]
    ) -> int:
        """Atomically append ``(sort_key, value)`` tuples.

        Returns the number of inserted rows.  The whole batch is one
        transaction: readers see either all of it or none.
        """
        for sort_key, value in rows:
            validate_sort_key(sort_key)
            validate_value(value)
        payload = [
            (
                partition_key,
                # NULL keys live in their own flag group (1 when NULLS LAST,
                # 0 when NULLS FIRST -- chosen at query time); the raw value
                # in that group is never compared to real sort keys.
                1 if sort_key is None else 0,
                sort_key if sort_key is not None else 0,
                _encode(sort_key),
                _encode(value),
            )
            for sort_key, value in rows
        ]
        conn = self._writer()
        with self._write_lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.executemany(
                    "INSERT INTO rows(partition_key, sort_null, sort_value,"
                    " sort_json, value_json) VALUES (?, ?, ?, ?, ?)",
                    payload,
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise
        return len(payload)

    # -- reads -------------------------------------------------------------

    def count_rows(self, conn: sqlite3.Connection, partition_key: str) -> int:
        cur = conn.execute(
            "SELECT count(*) AS c FROM rows WHERE partition_key = ?",
            (partition_key,),
        )
        return int(cur.fetchone()["c"])

    def iter_partition(
        self,
        conn: sqlite3.Connection,
        partition_key: str,
        null_order: NullOrder,
        page_size: int = 1024,
    ) -> Iterator[list[Row]]:
        """Yield ordered pages of rows using keyset pagination.

        Effective ordering key per row is
        ``(flag, sort_value, seq)`` where ``flag`` is 0 for the group that
        sorts first:

        * NULLS LAST  -> non-NULL group gets flag 0, NULL group flag 1;
        * NULLS FIRST -> NULL group gets flag 0, non-NULL group flag 1.

        NULL rows carry a placeholder ``sort_value`` of 0, but they are
        isolated inside their own flag group so it is never compared against
        real keys.  The cursor is the last emitted
        ``(flag, sort_value, seq)`` triple.
        """
        null_first = null_order is NullOrder.NULLS_FIRST
        flag_null = 0 if null_first else 1
        flag_value = 1 if null_first else 0

        # The effective flag is an internal 0/1 constant; inline it safely
        # (avoiding bind-parameter ordering pitfalls between WHERE/ORDER BY).
        eff = f"(CASE WHEN sort_null = 1 THEN {int(flag_null)} ELSE {int(flag_value)} END)"

        sql = (
            "SELECT seq, sort_null, sort_value, sort_json, value_json,"
            f" {eff} AS eff_flag"
            " FROM rows WHERE partition_key = ?"
        )
        order_sql = f" ORDER BY {eff}, sort_value, seq LIMIT ?"

        cur_flag: int | None = None
        cur_value: Scalar = None
        cur_seq: int | None = None
        while True:
            params: list[Scalar] = [partition_key]
            if cur_seq is not None:
                params += [cur_flag, cur_value, cur_seq]
                sql_page = sql + (
                    f" AND ({eff}, sort_value, seq) > (?, ?, ?)"
                )
            else:
                sql_page = sql
            records = conn.execute(
                sql_page + order_sql,
                params + [page_size],
            ).fetchall()
            if not records:
                return
            page: list[Row] = []
            for rec in records:
                sort_key = _decode(rec["sort_json"])
                value = _decode(rec["value_json"])
                page.append(
                    Row(
                        seq=int(rec["seq"]),
                        partition_key=partition_key,
                        sort_key=sort_key,
                        value=value,
                    )
                )
                cur_flag = int(rec["eff_flag"])
                cur_value = rec["sort_value"]
                cur_seq = int(rec["seq"])
            yield page
            if len(records) < page_size:
                return

    # -- query lifecycle ---------------------------------------------------

    def register_query(self, query_id: str, spec: QuerySpec) -> None:
        conn = self._writer()
        with self._write_lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO queries(query_id, spec_json, status,"
                    " created_at) VALUES (?, ?, 'running', ?)",
                    (query_id, _encode_spec(spec), time.time()),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def get_status(self, query_id: str) -> QueryStatus | None:
        conn = self._writer()
        cur = conn.execute(
            "SELECT status FROM queries WHERE query_id = ?", (query_id,)
        )
        rec = cur.fetchone()
        return QueryStatus(rec["status"]) if rec else None

    def get_status_and_error(
        self, query_id: str
    ) -> tuple[QueryStatus | None, str | None]:
        conn = self._writer()
        cur = conn.execute(
            "SELECT status, error FROM queries WHERE query_id = ?",
            (query_id,),
        )
        rec = cur.fetchone()
        if rec is None:
            return None, None
        return QueryStatus(rec["status"]), rec["error"]

    def commit_result_spooled(
        self, query_id: str, spool: SpooledResultLike, page_size: int
    ) -> None:
        """Commit a spooled result page by page in one transaction.

        Only ``page_size`` decoded rows are held at a time, so committing a
        huge result set keeps memory bounded.  Status flips to ``committed``
        in the same transaction: a crash before COMMIT leaves neither result
        rows nor a committed marker.
        """
        conn = self._writer()
        with self._write_lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                start = 0
                for page in spool.iter_pages(page_size):
                    payload = []
                    for i, r in enumerate(page):
                        payload.append(
                            (
                                query_id,
                                start + i,
                                r.seq,
                                _encode(
                                    {
                                        "seq": r.seq,
                                        "sort_key": r.sort_key,
                                        "value": r.value,
                                        "outputs": r.outputs,
                                    }
                                ),
                            )
                        )
                    conn.executemany(
                        "INSERT INTO results(query_id, row_index, seq,"
                        " payload_json) VALUES (?, ?, ?, ?)",
                        payload,
                    )
                    start += len(page)
                conn.execute(
                    "UPDATE queries SET status = 'committed',"
                    " error = NULL, finished_at = ? WHERE query_id = ?",
                    (time.time(), query_id),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def mark_status(
        self,
        query_id: str,
        status: QueryStatus,
        error: str | None = None,
    ) -> None:
        conn = self._writer()
        with self._write_lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "UPDATE queries SET status = ?, error = ?,"
                    " finished_at = ? WHERE query_id = ?",
                    (status.value, error, time.time(), query_id),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def fetch_result_page(
        self, query_id: str, offset: int, limit: int
    ) -> list[ResultRow] | None:
        """Fetch one result window ``[offset, offset+limit)``.

        Returns None when the query is not committed.
        """
        conn = self._writer()
        cur = conn.execute(
            "SELECT status FROM queries WHERE query_id = ?", (query_id,)
        )
        rec = cur.fetchone()
        if rec is None or rec["status"] != QueryStatus.COMMITTED.value:
            return None
        cur = conn.execute(
            "SELECT payload_json FROM results WHERE query_id = ?"
            " ORDER BY row_index LIMIT ? OFFSET ?",
            (query_id, limit, offset),
        )
        rows: list[ResultRow] = []
        for rec in cur:
            item = _decode(rec["payload_json"])
            rows.append(
                ResultRow(
                    seq=item["seq"],
                    sort_key=item["sort_key"],
                    value=item["value"],
                    outputs=item["outputs"],
                )
            )
        return rows

    def count_results(self, query_id: str) -> int | None:
        """Number of committed result rows, or None if not committed."""
        conn = self._writer()
        cur = conn.execute(
            "SELECT status FROM queries WHERE query_id = ?", (query_id,)
        )
        rec = cur.fetchone()
        if rec is None or rec["status"] != QueryStatus.COMMITTED.value:
            return None
        cur = conn.execute(
            "SELECT count(*) AS c FROM results WHERE query_id = ?", (query_id,)
        )
        return int(cur.fetchone()["c"])

    def iter_result_pages(
        self, query_id: str, page_size: int = 1024
    ) -> Iterator[list[ResultRow]]:
        """Stream committed result rows in pages (bounded memory)."""
        conn = self.begin_snapshot()
        try:
            cur = conn.execute(
                "SELECT status FROM queries WHERE query_id = ?", (query_id,)
            )
            rec = cur.fetchone()
            if rec is None or rec["status"] != QueryStatus.COMMITTED.value:
                return
            offset = 0
            while True:
                cur = conn.execute(
                    "SELECT payload_json FROM results WHERE query_id = ?"
                    " ORDER BY row_index LIMIT ? OFFSET ?",
                    (query_id, page_size, offset),
                )
                records = cur.fetchall()
                if not records:
                    return
                page: list[ResultRow] = []
                for rec in records:
                    item = _decode(rec["payload_json"])
                    page.append(
                        ResultRow(
                            seq=item["seq"],
                            sort_key=item["sort_key"],
                            value=item["value"],
                            outputs=item["outputs"],
                        )
                    )
                yield page
                offset += len(records)
                if len(records) < page_size:
                    return
        finally:
            self.release(conn)

    def recover(self) -> None:
        """Invalidate RUNNING queries left behind by a crashed process.

        Committed queries stay committed (their result rows were inserted in
        the same transaction); running queries never have result rows.
        """
        conn = self._writer()
        with self._write_lock:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "UPDATE queries SET status = 'cancelled',"
                    " error = 'interrupted by restart', finished_at = ?"
                    " WHERE status = 'running'",
                    (time.time(),),
                )
                conn.execute("COMMIT")
            except Exception:
                conn.execute("ROLLBACK")
                raise

    def close(self) -> None:
        conn = getattr(self._local, "writer", None)
        if conn is not None:
            conn.close()
            self._local.writer = None
        if self._sentinel is not None:
            self._sentinel.close()
            self._sentinel = None


def _encode_spec(spec: QuerySpec) -> str:
    return _encode(
        {
            "partition_key": spec.partition_key,
            "null_order": spec.null_order.value,
            "functions": [
                {
                    "kind": f.kind.value,
                    "alias": f.alias,
                    "source": f.source,
                    "offset": f.offset,
                    "default": f.default,
                    "window": (
                        None
                        if f.window is None
                        else {"frame": f.window.frame.value, "n": f.window.n}
                    ),
                }
                for f in spec.functions
            ],
        }
    )
