"""A database wrapper that counts the Arrow memory its calls allocate (ITL-919).

The scaling tests use the bytes a database's calls allocate from the Arrow memory pool as a
deterministic measure of the work those calls do: a call that filters, copies or
concatenates the rows already stored allocates in proportion to them, while a call that
touches only the rows it reads or writes does not. Unlike wall-clock time, the count does
not depend on machine load.
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa


class ArrowWorkCounter:
    """Shared counter of Arrow bytes allocated inside wrapped database calls."""

    def __init__(self) -> None:
        self.bytes_allocated = 0
        self._depth = 0

    def measure(self, method: Any) -> Any:
        """Wrap ``method`` so the Arrow bytes it allocates are added to the count."""

        def wrapped(*args: Any, **kwargs: Any) -> Any:
            pool = pa.default_memory_pool()
            outermost = self._depth == 0
            before = pool.total_bytes_allocated()
            self._depth += 1
            try:
                return method(*args, **kwargs)
            finally:
                self._depth -= 1
                if outermost:
                    self.bytes_allocated += pool.total_bytes_allocated() - before

        return wrapped


class CountingDatabase:
    """Delegates to an Arrow database and counts the Arrow bytes its calls allocate.

    ``at()`` returns a counting view sharing the same counter, so every scoped view a
    pipeline creates is counted.

    Args:
        inner: The database to delegate to.
        counter: The counter to add to; a new one when omitted.
    """

    def __init__(self, inner: Any, counter: ArrowWorkCounter | None = None) -> None:
        self._inner = inner
        self.counter = counter if counter is not None else ArrowWorkCounter()

    def at(self, *path_components: str) -> CountingDatabase:
        return CountingDatabase(self._inner.at(*path_components), self.counter)

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._inner, name)
        if callable(attr):
            return self.counter.measure(attr)
        return attr
