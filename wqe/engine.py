"""Top-level engine: writes, snapshot queries, cancellation and recovery."""

from __future__ import annotations

import threading
import time
import uuid

from .config import Config
from .execution import QueryCancelled, QueryExecutor, SpooledResult
from .models import QueryResult, QuerySpec, QueryStatus, ResultRow, Scalar
from .storage import Storage


class Engine:
    """Owns the storage and coordinates concurrent writes and queries.

    Concurrency model
    -----------------
    * Writes are atomic batches serialised inside :class:`Storage`.
    * Each query acquires one snapshot connection for its whole execution;
      it therefore observes exactly the data committed before the snapshot
      began -- never partial or mixed batches.
    * Cancellation is cooperative (an event polled at page boundaries) and
      the watchdog deadline raises the same exception.  Cancelled/expired
      queries are marked ``cancelled`` and leave no visible result rows.
    """

    def __init__(self, config: Config | None = None) -> None:
        self.config = config or Config()
        self.storage = Storage(self.config.db_path, self.config.busy_timeout_ms)
        self._cancel_events: dict[str, threading.Event] = {}
        self._cancel_lock = threading.Lock()
        # Any RUNNING row on disk means the previous process died mid-query.
        self.storage.recover()

    # -- writes ------------------------------------------------------------

    def write_batch(
        self, partition_key: str, rows: list[tuple[Scalar, Scalar]]
    ) -> int:
        """Atomically append ``(sort_key, value)`` rows to one partition."""
        if not isinstance(partition_key, str) or not partition_key:
            raise ValueError("partition_key must be a non-empty string")
        if not rows:
            return 0
        if len(rows) > self.config.max_batch_rows:
            raise ValueError(
                f"batch has {len(rows)} rows, max is {self.config.max_batch_rows}"
            )
        return self.storage.insert_rows(partition_key, rows)

    # -- queries -----------------------------------------------------------

    def submit_query(
        self,
        spec: QuerySpec,
        *,
        query_id: str | None = None,
        timeout_s: float | None = None,
    ) -> QueryResult:
        """Execute ``spec`` on one snapshot and atomically commit its result.

        Returns a :class:`QueryResult` with status ``committed`` or
        ``cancelled``.  Exceptions from invalid specs propagate to the
        caller and are not persisted as queries.
        """
        if query_id is None:
            query_id = uuid.uuid4().hex
        if timeout_s is None:
            timeout_s = self.config.default_timeout_s
        deadline = (
            time.monotonic() + timeout_s
            if timeout_s and timeout_s > 0
            else None
        )

        self.storage.register_query(query_id, spec)
        event = threading.Event()
        with self._cancel_lock:
            self._cancel_events[query_id] = event

        snapshot = self.storage.begin_snapshot()

        def scan_pages():
            return self.storage.iter_partition(
                snapshot,
                spec.partition_key,
                spec.null_order,
                page_size=self.config.page_size,
            )

        spool: SpooledResult | None = None
        try:
            executor = QueryExecutor(
                spec,
                scan_pages,
                self.config.page_size,
                cancel_event=event,
                deadline=deadline,
            )
            spool = executor.run()
            self._commit_spool(query_id, spool)
            spool = None
            # Return a lightweight placeholder; callers stream rows via
            # iter_result_pages() to keep memory bounded.
            return QueryResult(query_id=query_id, status=QueryStatus.COMMITTED)
        except QueryCancelled as exc:
            self.storage.mark_status(
                query_id, QueryStatus.CANCELLED, str(exc) or "cancelled"
            )
            return QueryResult(
                query_id=query_id,
                status=QueryStatus.CANCELLED,
                error=str(exc) or "cancelled",
            )
        except Exception as exc:  # execution/storage failure
            self.storage.mark_status(query_id, QueryStatus.FAILED, str(exc))
            raise
        finally:
            self.storage.release(snapshot)
            if spool is not None:
                spool.close()
            with self._cancel_lock:
                self._cancel_events.pop(query_id, None)

    def _commit_spool(self, query_id: str, spool: SpooledResult) -> None:
        """Persist the spooled result and flip status in one transaction."""
        # Storage's public commit currently takes a list; stream pages via
        # a dedicated method to keep commit memory bounded too.
        self.storage.commit_result_spooled(query_id, spool, self.config.page_size)

    def get_result(self, query_id: str) -> QueryResult | None:
        """Return a status placeholder (no rows; stream rows separately)."""
        status, error = self.storage.get_status_and_error(query_id)
        if status is None:
            return None
        return QueryResult(query_id=query_id, status=status, error=error)

    def iter_result_pages(
        self, query_id: str, page_size: int | None = None
    ):
        """Stream committed result rows in pages (bounded memory)."""
        return self.storage.iter_result_pages(
            query_id, page_size or self.config.page_size
        )

    def count_results(self, query_id: str) -> int | None:
        """Number of committed result rows, or None if not committed."""
        return self.storage.count_results(query_id)

    def fetch_result_page(
        self, query_id: str, offset: int, limit: int
    ) -> list[ResultRow] | None:
        """Fetch one committed result window; None if not committed."""
        return self.storage.fetch_result_page(query_id, offset, limit)

    def cancel_query(self, query_id: str) -> bool:
        """Signal cooperative cancellation.

        Returns True if a live query was signalled.  A query that already
        finished is unaffected.
        """
        with self._cancel_lock:
            event = self._cancel_events.get(query_id)
        if event is None:
            return False
        event.set()
        return True

    def close(self) -> None:
        self.storage.close()

    def __enter__(self) -> Engine:  # noqa: PYI034
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
