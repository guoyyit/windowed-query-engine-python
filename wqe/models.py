"""Domain types shared across the windowed query engine.

All types in this module are plain ``@dataclass`` definitions.  They contain
no computation; the semantics live in :mod:`wqe.execution`.
"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field

#: A value is either an int, float, str, bool or None (NULL/missing).
Scalar = object | None


class NullOrder(enum.Enum):
    """Where NULL sort keys are placed relative to non-NULL keys."""

    NULLS_FIRST = "nulls_first"
    NULLS_LAST = "nulls_last"


class FrameType(enum.Enum):
    """How the window frame of a row is defined.

    * ``PARTITION`` -- every row in the partition.
    * ``ROWS_PRECEDING`` -- ``n`` preceding rows through the current row.
    * ``ROWS_AROUND`` -- ``n`` rows before and ``n`` rows after the current
      row (clipped at partition boundaries).
    """

    PARTITION = "partition"
    ROWS_PRECEDING = "rows_preceding"
    ROWS_AROUND = "rows_around"


class FuncKind(enum.Enum):
    """Supported window functions."""

    # Ranking
    ROW_NUMBER = "row_number"
    RANK = "rank"
    DENSE_RANK = "dense_rank"
    # Navigation / lead-lag
    LAG = "lag"
    LEAD = "lead"
    # Sliding aggregates
    SUM = "sum"
    COUNT = "count"
    AVG = "avg"
    MIN = "min"
    MAX = "max"


class QueryStatus(enum.Enum):
    """Lifecycle state of a submitted query."""

    RUNNING = "running"
    COMMITTED = "committed"
    CANCELLED = "cancelled"
    FAILED = "failed"


@dataclass(frozen=True)
class Row:
    """One input row.

    ``seq`` is assigned by the storage layer in commit order; it is the
    tie-breaker that makes ordering of equal sort keys deterministic.
    """

    seq: int
    partition_key: str
    sort_key: Scalar
    value: Scalar


@dataclass(frozen=True)
class WindowSpec:
    """Definition of a window frame."""

    frame: FrameType
    n: int = 0  # used only for ROWS_PRECEDING / ROWS_AROUND

    def __post_init__(self) -> None:
        if (
            self.frame in (FrameType.ROWS_PRECEDING, FrameType.ROWS_AROUND)
            and self.n < 0
        ):
            raise ValueError("window n must be >= 0")


@dataclass(frozen=True)
class FuncSpec:
    """One computed function within a query."""

    kind: FuncKind
    alias: str
    #: column the function reads ("value" by default)
    source: str = "value"
    #: offset for LAG / LEAD
    offset: int = 1
    #: result returned at boundary / when the referenced row is missing
    default: Scalar = None
    #: window for aggregate functions (ignored by rank/navigation)
    window: WindowSpec | None = None

    def __post_init__(self) -> None:
        if self.kind in (FuncKind.LAG, FuncKind.LEAD) and self.offset < 1:
            raise ValueError("lag/lead offset must be >= 1")


@dataclass(frozen=True)
class QuerySpec:
    """A complete query: one partition, one ordering, >=1 functions."""

    partition_key: str
    functions: tuple[FuncSpec, ...]
    null_order: NullOrder = NullOrder.NULLS_LAST


@dataclass(frozen=True)
class ResultRow:
    """One output row; ``outputs`` maps function alias -> computed value."""

    seq: int
    sort_key: Scalar
    value: Scalar
    outputs: dict[str, Scalar] = field(default_factory=dict)


@dataclass(frozen=True)
class QueryResult:
    query_id: str
    status: QueryStatus
    rows: tuple[ResultRow, ...] = ()
    error: str | None = None
