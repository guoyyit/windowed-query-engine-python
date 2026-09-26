"""Streaming aggregate state.

All aggregate state objects consume values one at a time (``update`` when a
row enters the frame, ``remove`` when it leaves) so that query execution
never has to hold a whole partition in memory.

State sizes:

* SUM / COUNT / AVG -- O(1)
* MIN / MAX         -- O(width of frame) worst case, O(1) amortised per
                       row, via a monotonic deque.

NULL handling is fixed and documented (see docs/window-semantics.md):

* SUM/AVG/MIN/MAX ignore NULL inputs; an all-NULL (or empty) frame yields
  NULL.
* COUNT counts non-NULL inputs and yields ``0`` for an all-NULL/empty frame.
* AVG divides by the number of non-NULL inputs.

Numeric mixing: ``int`` and ``float`` inputs may be mixed; SUM keeps an
``int`` accumulator when all inputs are ints and becomes float otherwise.
AVG always returns a float (or NULL).  Comparison between incomparable
types raises ``TypeError`` just like plain Python would -- the API layer
validates value types before they reach this module.
"""

from __future__ import annotations

from collections import deque

from .models import FuncKind, Scalar


class Aggregate:
    """Streaming aggregate over a multiset of values."""

    def update(self, value: Scalar) -> None:
        """Add one value (NULL is ignored by every aggregate)."""
        raise NotImplementedError

    def remove(self, value: Scalar) -> None:
        """Remove one value previously added via :meth:`update`."""
        raise NotImplementedError

    def result(self) -> Scalar:
        raise NotImplementedError


def _is_number(value: Scalar) -> bool:
    # bool is a subclass of int; treat booleans as numbers like JSON does.
    return isinstance(value, (int, float)) and not (value is None)


class SumAgg(Aggregate):
    def __init__(self) -> None:
        self._total: float | int = 0
        self._non_null = 0

    def update(self, value: Scalar) -> None:
        if value is None:
            return
        self._total += value  # type: ignore[operator]
        self._non_null += 1

    def remove(self, value: Scalar) -> None:
        if value is None:
            return
        self._total -= value  # type: ignore[operator]
        self._non_null -= 1

    def result(self) -> Scalar:
        return self._total if self._non_null > 0 else None


class CountAgg(Aggregate):
    def __init__(self) -> None:
        self._count = 0

    def update(self, value: Scalar) -> None:
        if value is not None:
            self._count += 1

    def remove(self, value: Scalar) -> None:
        if value is not None:
            self._count -= 1

    def result(self) -> Scalar:
        return self._count


class AvgAgg(Aggregate):
    def __init__(self) -> None:
        self._total: float = 0.0
        self._count = 0

    def update(self, value: Scalar) -> None:
        if value is None:
            return
        self._total += value  # type: ignore[assignment]
        self._count += 1

    def remove(self, value: Scalar) -> None:
        if value is None:
            return
        self._total -= value  # type: ignore[assignment]
        self._count -= 1

    def result(self) -> Scalar:
        if self._count == 0:
            return None
        return self._total / self._count


class MinMaxAgg(Aggregate):
    """Monotonic deque MIN or MAX over a sliding frame.

    Values are kept in deque order.  On insert, dominated tail entries are
    popped; equal values are retained as separate entries so that removing
    one occurrence never evicts another still in the frame.  O(1) amortised
    per insert/remove; the deque length is bounded by the frame width.
    """

    def __init__(self, *, is_max: bool) -> None:
        self._is_max = is_max
        self._deque: deque[Scalar] = deque()

    def _better(self, candidate: Scalar, current: Scalar) -> bool:
        if self._is_max:
            return candidate >= current  # type: ignore[operator]
        return candidate <= current  # type: ignore[operator]

    def update(self, value: Scalar) -> None:
        if value is None:
            return
        dq = self._deque
        while dq and not self._better(dq[-1], value):
            dq.pop()
        dq.append(value)

    def remove(self, value: Scalar) -> None:
        if value is None:
            return
        if self._deque and self._deque[0] == value:
            self._deque.popleft()

    def result(self) -> Scalar:
        return self._deque[0] if self._deque else None


def make_aggregate(kind: FuncKind) -> Aggregate:
    """Factory for the five aggregate functions."""
    if kind is FuncKind.SUM:
        return SumAgg()
    if kind is FuncKind.COUNT:
        return CountAgg()
    if kind is FuncKind.AVG:
        return AvgAgg()
    if kind is FuncKind.MIN:
        return MinMaxAgg(is_max=False)
    if kind is FuncKind.MAX:
        return MinMaxAgg(is_max=True)
    raise ValueError(f"not an aggregate function: {kind}")
