"""Engine configuration."""

from __future__ import annotations

import dataclasses
import os


@dataclasses.dataclass(frozen=True)
class Config:
    """Process-wide configuration knobs.

    Memory model
    ------------
    Query execution never materialises a whole partition.  Input rows are
    scanned in ordered pages of ``page_size`` rows and every operator keeps
    state bounded by ``page_size`` / window width, independently of the
    partition row count.

    All knobs (except ``db_path``) can be overridden through ``WQE_*``
    environment variables, see :meth:`from_env`.
    """

    #: Path of the SQLite database file (``:memory:`` for ephemeral tests).
    db_path: str = dataclasses.field(
        default_factory=lambda: os.environ.get("WQE_DB_PATH", "wqe.db")
    )
    #: Rows fetched from storage per page; bounds per-query memory.
    page_size: int = 1024
    #: Maximum rows accepted in a single write batch.
    max_batch_rows: int = 10_000
    #: Default query timeout in seconds (0 disables the timeout).
    default_timeout_s: float = 30.0
    #: SQLite busy timeout in milliseconds.
    busy_timeout_ms: int = 5_000

    @staticmethod
    def from_env() -> Config:
        """Build a config from ``WQE_*`` environment variables.

        Recognised variables: ``WQE_DB_PATH``, ``WQE_PAGE_SIZE``,
        ``WQE_MAX_BATCH_ROWS``, ``WQE_DEFAULT_TIMEOUT_S``,
        ``WQE_BUSY_TIMEOUT_MS``.
        """

        def _int(name: str, default: int) -> int:
            raw = os.environ.get(name)
            return int(raw) if raw not in (None, "") else default

        def _float(name: str, default: float) -> float:
            raw = os.environ.get(name)
            return float(raw) if raw not in (None, "") else default

        return Config(
            db_path=os.environ.get("WQE_DB_PATH", "wqe.db"),
            page_size=_int("WQE_PAGE_SIZE", 1024),
            max_batch_rows=_int("WQE_MAX_BATCH_ROWS", 10_000),
            default_timeout_s=_float("WQE_DEFAULT_TIMEOUT_S", 30.0),
            busy_timeout_ms=_int("WQE_BUSY_TIMEOUT_MS", 5_000),
        )
