"""Force the order in which async rows complete, without wall-clock time (ITL-1034).

A gated function awaits its turn before it returns: the row keyed ``k`` returns only after
the row before it in the prescribed order has returned. The order is then the order the
rows complete in, whatever the scheduler would otherwise do. Nothing sleeps: rows wait on
``asyncio.Event`` objects, and the timeout on each wait is a deadlock guard that raises a
named error, never the mechanism of an assertion.

The gate is armed only for async runs. A sync run calls each function in input order on a
fresh event loop, so a gate armed there would wait forever for a row that has not started.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Callable, Hashable, Sequence
from typing import Any

from orcapod import function_pod

# Seconds a gated row waits for its predecessor before the gate reports a deadlock. A
# deadlock guard only: in a passing run every wait ends as soon as its predecessor returns.
DEADLOCK_GUARD_SECONDS = 30.0


class CompletionOrderDeadlock(RuntimeError):
    """A gated row waited too long for its predecessor: the order was not realisable."""


class CompletionGate:
    """Make gated rows complete in a prescribed order.

    Attributes:
        completed: The keys of the rows that returned, in the order they returned. Every
            gated call appends to it, armed or not, so a test can assert the order a run
            actually realised.
    """

    def __init__(self) -> None:
        """Build a disarmed gate."""
        self._order: list[Hashable] | None = None
        self._events: dict[Hashable, asyncio.Event] = {}
        self.completed: list[Hashable] = []

    def arm(self, order: Sequence[Hashable]) -> None:
        """Make the next rows complete in ``order``; forget earlier completions.

        Args:
            order: Every key the run will compute, each once, in the order the rows must
                complete in.

        Raises:
            ValueError: If a key appears twice.
        """
        if len(set(order)) != len(order):
            raise ValueError(f"a completion order names a key twice: {list(order)!r}")
        self._order = list(order)
        self._events = {}
        self.completed = []

    def disarm(self) -> None:
        """Let rows complete as the scheduler runs them; forget earlier completions."""
        self._order = None
        self._events = {}
        self.completed = []

    def _event(self, key: Hashable) -> asyncio.Event:
        """Return the event set when the row ``key`` returns, created on first use."""
        if key not in self._events:
            self._events[key] = asyncio.Event()
        return self._events[key]

    async def wait_turn(self, key: Hashable) -> None:
        """Return once every row before ``key`` in the armed order has returned.

        Raises:
            CompletionOrderDeadlock: If the predecessor does not return within the guard.
            ValueError: If the gate is armed and ``key`` is not in its order.
        """
        if self._order is None:
            return
        position = self._order.index(key)
        if position == 0:
            return
        before = self._order[position - 1]
        try:
            await asyncio.wait_for(
                self._event(before).wait(), timeout=DEADLOCK_GUARD_SECONDS
            )
        except TimeoutError as exc:
            raise CompletionOrderDeadlock(
                f"row {key!r} waited for row {before!r}, which never returned; "
                f"order {self._order!r}"
            ) from exc

    def finish(self, key: Hashable) -> None:
        """Record that the row ``key`` returned and release the next row."""
        self.completed.append(key)
        if self._order is not None:
            self._event(key).set()


# ---------------------------------------------------------------------------
# Orders
# ---------------------------------------------------------------------------


def natural(keys: Sequence[Hashable]) -> list[Hashable]:
    """The input order."""
    return list(keys)


def reversed_order(keys: Sequence[Hashable]) -> list[Hashable]:
    """The input order reversed."""
    return list(reversed(keys))


def interleaved(keys: Sequence[Hashable]) -> list[Hashable]:
    """First, last, second, second to last, and so on."""
    out: list[Hashable] = []
    low, high = 0, len(keys) - 1
    while low <= high:
        out.append(keys[low])
        if low != high:
            out.append(keys[high])
        low += 1
        high -= 1
    return out


def one_slow(keys: Sequence[Hashable]) -> list[Hashable]:
    """The input order with the first row completing last."""
    return [*keys[1:], keys[0]]


def seeded(seed: int) -> Callable[[Sequence[Hashable]], list[Hashable]]:
    """A random order, the same for the same ``seed`` and keys."""

    def order(keys: Sequence[Hashable]) -> list[Hashable]:
        """The keys shuffled with ``random.Random(seed)``."""
        shuffled = list(keys)
        random.Random(seed).shuffle(shuffled)
        return shuffled

    return order


ORDERS: dict[str, Callable[[Sequence[Hashable]], list[Hashable]]] = {
    "natural": natural,
    "reversed": reversed_order,
    "interleaved": interleaved,
    "one_slow": one_slow,
    "random_0": seeded(0),
    "random_1": seeded(1),
    "random_2": seeded(2),
}


# ---------------------------------------------------------------------------
# Gated function pods
# ---------------------------------------------------------------------------

GATE = CompletionGate()


def scale(x: int) -> int:
    """What ``gated_scale`` returns for ``x``: the oracle's derivation of ``y``."""
    return 10 * x


FAILING_REMAINDER = 1


def fails(x: int) -> bool:
    """Whether ``gated_fragile`` raises for ``x``."""
    return x % 3 == FAILING_REMAINDER


@function_pod(output_keys="y")
async def gated_scale(x: int) -> int:
    """Return ``10 * x`` once ``GATE`` lets the row keyed ``x`` complete."""
    await GATE.wait_turn(x)
    try:
        return scale(x)
    finally:
        GATE.finish(x)


@function_pod(output_keys="y")
async def gated_fragile(x: int) -> int:
    """As ``gated_scale``, but raise for every ``x`` where ``fails(x)``."""
    await GATE.wait_turn(x)
    try:
        if fails(x):
            raise ValueError(f"gated_fragile fails for x={x}")
        return scale(x)
    finally:
        GATE.finish(x)


def new_gated_scale_pod() -> Any:
    """A fresh function pod computing ``scale`` behind ``GATE``.

    Setting a stream's executor sets it on its pod's data function, so a test that
    changes the executor takes its own pod and leaves ``gated_scale`` untouched.
    """

    @function_pod(output_keys="y")
    async def gated_scale(x: int) -> int:
        """Return ``10 * x`` once ``GATE`` lets the row keyed ``x`` complete."""
        await GATE.wait_turn(x)
        try:
            return scale(x)
        finally:
            GATE.finish(x)

    return gated_scale.pod
