"""Query execution: ordered paged scan + streaming window operators.

Execution strategy (bounded memory)
-----------------------------------
A query runs against a single storage snapshot connection and makes at most
two ordered scans of the partition:

1. *partition pass* -- partition-wide aggregates (``PARTITION`` frame) are
   reduced to their final values with O(#functions) state.
2. *emission pass* -- every operator consumes the row stream once.  Rank and
   LAG/LEAD use counters and a ring buffer; fixed ROWS frames use one
   streaming :class:`~wqe.aggregation.Aggregate` each plus a deque of frame
   rows.  The largest piece of retained state is

       O(max(page_size, max frame width, max LAG/LEAD offset)),

   independent of the number of rows in the partition.

Output rows are spooled to a temporary JSON-lines file page by page, so even
the result set never resides wholly in memory.  The engine commits the
spool in one transaction; on cancellation the spool is deleted.

Determinism
-----------
Ordering is ``(sort_key, seq)`` where ``seq`` is the commit-order tie-break.
RANK/DENSE_RANK group NULL keys together as one tie group.  Repeated runs
over identical input batches produce byte-identical output.

Cancellation
------------
:meth:`QueryExecutor.check_cancelled` is polled at every page boundary (and
the deadline clock there too).  Cancellation raises :class:`QueryCancelled`
before any output page is committed.
"""

from __future__ import annotations

import json
import os
import tempfile
import threading
import time
from collections import deque
from collections.abc import Callable, Iterable

from .aggregation import make_aggregate
from .models import (
    FrameType,
    FuncKind,
    FuncSpec,
    QuerySpec,
    ResultRow,
    Row,
    Scalar,
)

PageIterator = Callable[[], Iterable[list[Row]]]

_RANK_FUNCS = frozenset(
    {FuncKind.ROW_NUMBER, FuncKind.RANK, FuncKind.DENSE_RANK}
)
_NAV_FUNCS = frozenset({FuncKind.LAG, FuncKind.LEAD})
_AGG_FUNCS = frozenset(
    {FuncKind.SUM, FuncKind.COUNT, FuncKind.AVG, FuncKind.MIN, FuncKind.MAX}
)


class QueryCancelled(Exception):
    """Raised when a query is cancelled or hits its deadline."""


class SpecError(ValueError):
    """An invalid query specification."""


class SpooledResult:
    """Result rows spooled to a temporary JSON-lines file.

    Memory stays bounded: pages are encoded one at a time and read back one
    page at a time at commit time.
    """

    def __init__(self, path: str, count: int) -> None:
        self.path = path
        self.count = count
        self._closed = False

    def iter_pages(self, page_size: int) -> Iterable[list[ResultRow]]:
        batch: list[ResultRow] = []
        with open(self.path, "r", encoding="utf-8") as fh:
            for line in fh:
                item = json.loads(line)
                batch.append(
                    ResultRow(
                        seq=item["seq"],
                        sort_key=item["sort_key"],
                        value=item["value"],
                        outputs=item["outputs"],
                    )
                )
                if len(batch) >= page_size:
                    yield batch
                    batch = []
        if batch:
            yield batch

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            try:
                os.unlink(self.path)
            except FileNotFoundError:
                pass

    def __enter__(self) -> SpooledResult:  # noqa: PYI034
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


class _FrameSlider:
    """Sliding fixed-ROWS-frame aggregate over a streamed partition.

    ``frame`` is one of ROWS_PRECEDING (``before=n, after=0``) or
    ROWS_AROUND (``before=after=n``).  The slider consumes rows through
    :meth:`add` and exposes the aggregate value for every row whose frame is
    fully observable; :meth:`flush` emits the tail at partition end where
    frames are clipped to the boundary.
    """

    def __init__(self, func: FuncSpec) -> None:
        assert func.window is not None
        self.func = func
        self.agg = make_aggregate(func.kind)
        if func.window.frame is FrameType.ROWS_PRECEDING:
            self.before, self.after = func.window.n, 0
        else:
            self.before = self.after = func.window.n
        # Rows currently accounted for in ``agg``: indices [lo, hi].
        self._frame: deque[tuple[int, Scalar]] = deque()
        self._lo = 0
        self._hi = -1
        self._next_emit = 0
        self._ready: dict[int, Scalar] = {}

    def add(self, index: int, value: Scalar) -> None:
        self.agg.update(value)
        self._frame.append((index, value))
        self._hi = index
        # Emit every output whose required right edge is now observable.
        while self._hi >= self._next_emit + self.after:
            i = self._next_emit
            target_lo = max(0, i - self.before)
            while self._lo < target_lo:
                _, old = self._frame.popleft()
                self.agg.remove(old)
                self._lo += 1
            self._ready[i] = self.agg.result()
            self._next_emit += 1

    def flush(self) -> None:
        """Emit tail rows whose frame is clipped by the partition end."""
        end = self._hi
        while self._next_emit <= end:
            i = self._next_emit
            target_lo = max(0, i - self.before)
            while self._lo < target_lo:
                _, old = self._frame.popleft()
                self.agg.remove(old)
                self._lo += 1
            self._ready[i] = self.agg.result()
            self._next_emit += 1

    def take(self, index: int) -> Scalar:
        return self._ready.pop(index)


class QueryExecutor:
    """Executes one :class:`QuerySpec` against a snapshot's ordered rows."""

    def __init__(
        self,
        spec: QuerySpec,
        scan_pages: PageIterator,
        page_size: int,
        cancel_event: threading.Event | None = None,
        deadline: float | None = None,
        spool_dir: str | None = None,
    ) -> None:
        self.spec = spec
        self.scan_pages = scan_pages
        self.page_size = page_size
        self.cancel_event = cancel_event
        self.deadline = deadline
        self.spool_dir = spool_dir

    # -- cancellation ------------------------------------------------------

    def check_cancelled(self) -> None:
        if self.cancel_event is not None and self.cancel_event.is_set():
            raise QueryCancelled("query cancelled")
        if self.deadline is not None and time.monotonic() >= self.deadline:
            raise QueryCancelled("query deadline exceeded")

    # -- validation --------------------------------------------------------

    def _validate(self) -> None:
        if not self.spec.functions:
            raise SpecError("query must contain at least one function")
        aliases = [f.alias for f in self.spec.functions]
        if len(set(aliases)) != len(aliases):
            raise SpecError("function aliases must be unique")
        for f in self.spec.functions:
            if f.kind in _AGG_FUNCS and f.window is None:
                raise SpecError(f"aggregate {f.alias} requires a window")
            if f.kind in _RANK_FUNCS | _NAV_FUNCS and f.window is not None:
                raise SpecError(f"{f.alias}: rank/lag/lead take no window")
            if f.source != "value":
                raise SpecError(f"{f.alias}: only source 'value' is supported")

    # -- entry point -------------------------------------------------------

    def run(self) -> SpooledResult:
        self._validate()

        # Pass 1: partition-wide aggregate constants.
        partition_values = self._partition_pass()

        # Pass 2: streaming emission, spooled page by page.
        tmp = tempfile.NamedTemporaryFile(  # noqa: SIM115
            mode="w",
            encoding="utf-8",
            suffix=".jsonl",
            dir=self.spool_dir,
            delete=False,
        )
        path = tmp.name
        total = 0
        try:
            total = self._emission_pass(tmp, partition_values)
            tmp.close()
        except BaseException:
            tmp.close()
            try:
                os.unlink(path)
            except FileNotFoundError:
                pass
            raise
        return SpooledResult(path, total)

    # -- pass 1 ------------------------------------------------------------

    def _partition_pass(self) -> dict[str, Scalar]:
        funcs = [
            f
            for f in self.spec.functions
            if f.kind in _AGG_FUNCS and f.window.frame is FrameType.PARTITION
        ]
        if not funcs:
            return {}
        aggs = {f.alias: make_aggregate(f.kind) for f in funcs}
        for page in self.scan_pages():
            self.check_cancelled()
            for row in page:
                for f in funcs:
                    aggs[f.alias].update(row.value)
        return {alias: agg.result() for alias, agg in aggs.items()}

    # -- pass 2 ------------------------------------------------------------

    def _emission_pass(
        self, fh, partition_values: dict[str, Scalar]
    ) -> int:
        rank_aliases = [
            f.alias for f in self.spec.functions if f.kind in _RANK_FUNCS
        ]
        nav_specs = [f for f in self.spec.functions if f.kind in _NAV_FUNCS]
        sliders = [
            _FrameSlider(f)
            for f in self.spec.functions
            if f.kind in _AGG_FUNCS and f.window.frame is not FrameType.PARTITION
        ]

        lookahead = max(
            [s.after for s in sliders]
            + [f.offset for f in nav_specs if f.kind is FuncKind.LEAD]
            + [0]
        )
        lookback = max(
            [f.offset for f in nav_specs if f.kind is FuncKind.LAG] + [0]
        )

        # Ring of recently seen rows for LAG/LEAD: list[index] style access
        # via a deque plus its known base index.
        ring: deque[tuple[int, Row]] = deque()
        rank_ready: dict[int, dict[str, Scalar]] = {}
        emit_next = 0
        total = 0

        prev_key: Scalar = _MISSING
        run_start = 0
        distinct = 0

        def ring_row(idx: int) -> Row | None:
            if idx < 0 or not ring or idx < ring[0][0] or idx > ring[-1][0]:
                return None
            return ring[idx - ring[0][0]][1]

        def emit(index: int, row: Row) -> None:
            outputs: dict[str, Scalar] = dict(partition_values)
            if rank_aliases:
                outputs.update(rank_ready.pop(index))
            for f in nav_specs:
                ref_idx = (
                    index - f.offset
                    if f.kind is FuncKind.LAG
                    else index + f.offset
                )
                ref = ring_row(ref_idx)
                outputs[f.alias] = f.default if ref is None else ref.value
            for slider in sliders:
                outputs[slider.func.alias] = slider.take(index)
            encoded = json.dumps(
                {
                    "seq": row.seq,
                    "sort_key": row.sort_key,
                    "value": row.value,
                    "outputs": outputs,
                },
                ensure_ascii=False,
                separators=(",", ":"),
            )
            fh.write(encoded)
            fh.write("\n")

        index = -1
        for page in self.scan_pages():
            self.check_cancelled()
            for row in page:
                index += 1
                # rank metadata for this row
                key = row.sort_key
                if index == 0:
                    run_start = 0
                    distinct = 1
                elif _keys_equal(key, prev_key):
                    pass  # same tie group
                else:
                    run_start = index
                    distinct += 1
                prev_key = key
                rank_values: dict[str, Scalar] = {}
                for f in self.spec.functions:
                    if f.kind is FuncKind.ROW_NUMBER:
                        rank_values[f.alias] = index + 1
                    elif f.kind is FuncKind.RANK:
                        rank_values[f.alias] = run_start + 1
                    elif f.kind is FuncKind.DENSE_RANK:
                        rank_values[f.alias] = distinct
                if rank_aliases:
                    rank_ready[index] = rank_values

                ring.append((index, row))
                for slider in sliders:
                    slider.add(index, row.value)

                right = index
                while right >= emit_next + lookahead:
                    i = emit_next
                    # To compute both LAG (i - lookback) and LEAD
                    # (i + lookahead) for row i, the ring must currently
                    # span [i - lookback, i + lookahead]; trim only rows that
                    # the *next* emission can no longer need afterwards.
                    emit(i, ring_row(i))
                    emit_next += 1
                    next_base = emit_next - lookback
                    while ring and ring[0][0] < next_base:
                        ring.popleft()

        # Flush the lagging tail (frames clipped at the end, LEAD past end).
        end = index
        for slider in sliders:
            slider.flush()
        while emit_next <= end:
            i = emit_next
            emit(i, ring_row(i))
            emit_next += 1
            next_base = emit_next - lookback
            while ring and ring[0][0] < next_base:
                ring.popleft()
        total = end + 1
        return total


_MISSING = object()


def _keys_equal(a: Scalar, b: Scalar) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return a == b
