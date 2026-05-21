# Polling Source Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `PollingSource[T]` — a protocol-based dynamic data source that continuously emits rows to async pipelines using a user-supplied `DynamicSourceProtocol[T]` implementation.

**Architecture:** `DynamicSourceProtocol[T]` supplies three async methods (`poll`, `fetch`, `close`). `PollingSource[T]` wraps it as a `RootSource`, routing sync access through a lazy-initialized `ArrowTableStream` cache and async access through an `async_iter_data()` polling loop. `StreamBase.async_iter_data()` gains a real default (wrapping `iter_data()`), allowing `SourceNode.async_execute` to be simplified to always call `async_iter_data()`.

**Tech Stack:** Python 3.11+, asyncio, polars, pyarrow, pytest-asyncio

---

## File Map

| Action | Path | What changes |
|---|---|---|
| Modify | `src/orcapod/types.py` | Add `T` TypeVar, `Cursor[T]` dataclass, `PollingConfig` frozen dataclass |
| Modify | `src/orcapod/errors.py` | Add `CursorInvalidatedError` |
| Modify | `src/orcapod/protocols/core_protocols/sources.py` | Add `DynamicSourceProtocol[T]` |
| Modify | `src/orcapod/protocols/core_protocols/__init__.py` | Export `DynamicSourceProtocol` |
| Modify | `src/orcapod/core/streams/base.py` | Replace `async_iter_data` stub with real default |
| Modify | `src/orcapod/core/nodes/source_node.py` | Simplify `async_execute` to use `async_iter_data()` |
| Create | `src/orcapod/core/sources/polling_source.py` | `PollingSource[T]` class |
| Modify | `src/orcapod/core/sources/__init__.py` | Export `PollingSource` |
| Create | `tests/test_channels/test_polling_source.py` | All tests (19 cases) |

---

## Task 1: New Shared Types — `Cursor[T]`, `PollingConfig`, `CursorInvalidatedError`

**Files:**
- Modify: `src/orcapod/types.py`
- Modify: `src/orcapod/errors.py`
- Test: `tests/test_channels/test_polling_source.py` (create)

- [ ] **Step 1: Write the failing tests**

Create `tests/test_channels/test_polling_source.py`:

```python
"""Tests for PollingSource and DynamicSourceProtocol."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pytest

from orcapod.errors import CursorInvalidatedError
from orcapod.types import Cursor, PollingConfig


# ===========================================================================
# Task 1: Type tests
# ===========================================================================


class TestCursor:
    def test_cursor_stores_value(self):
        c = Cursor(value=42)
        assert c.value == 42

    def test_cursor_modified_at_defaults_to_none(self):
        c = Cursor(value="tok")
        assert c.modified_at is None

    def test_cursor_accepts_datetime_modified_at(self):
        ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
        c = Cursor(value=ts, modified_at=ts)
        assert c.modified_at == ts

    def test_cursor_generic_int(self):
        c: Cursor[int] = Cursor(value=7)
        assert c.value == 7

    def test_cursor_generic_str(self):
        c: Cursor[str] = Cursor(value="page_token")
        assert c.value == "page_token"


class TestPollingConfig:
    def test_defaults(self):
        cfg = PollingConfig()
        assert cfg.interval == 1.0
        assert cfg.duration == 0.0
        assert cfg.max_missed_intervals == 5
        assert cfg.max_consecutive_errors == 3
        assert cfg.error_backoff_base == 1.0

    def test_custom_values(self):
        cfg = PollingConfig(interval=0.5, duration=10.0, max_missed_intervals=2)
        assert cfg.interval == 0.5
        assert cfg.duration == 10.0
        assert cfg.max_missed_intervals == 2

    def test_is_frozen(self):
        cfg = PollingConfig()
        with pytest.raises((AttributeError, TypeError)):
            cfg.interval = 99.0  # type: ignore[misc]


class TestCursorInvalidatedError:
    def test_is_exception(self):
        e = CursorInvalidatedError("state lost")
        assert isinstance(e, Exception)
        assert "state lost" in str(e)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_channels/test_polling_source.py -v 2>&1 | head -30
```

Expected: `ImportError` or `FAILED` — `Cursor`, `PollingConfig`, `CursorInvalidatedError` not yet defined.

- [ ] **Step 3: Add `T`, `Cursor[T]`, `PollingConfig` to `types.py`**

At the top of `src/orcapod/types.py`, change the existing `typing` import line from:
```python
from typing import TYPE_CHECKING, Any, Self, TypeAlias
```
to:
```python
from datetime import datetime
from typing import TYPE_CHECKING, Any, Generic, Self, TypeAlias, TypeVar
```

Then add the following block **after** the existing `SchemaLike` alias (around line 55, before the `Schema` class):

```python
T = TypeVar("T")
"""Generic cursor value type for ``Cursor`` and ``DynamicSourceProtocol``."""
```

Then add the following block **at the end of `types.py`**, after the `ColumnInfo` class:

```python
@dataclass
class Cursor(Generic[T]):
    """Marks the current position in a DynamicSource's data stream.

    Args:
        value: Implementation-defined cursor value. May be a datetime
            timestamp, integer offset, string pagination token, or any
            other type meaningful to the implementation.
        modified_at: Optional wall-clock time when the source content at
            this cursor position was last modified. When provided,
            ``PollingSource`` uses this to update its ``last_modified``
            timestamp for downstream staleness detection. When ``None``,
            the framework falls back to its own wall clock.
    """

    value: T
    modified_at: datetime | None = None


@dataclass(frozen=True)
class PollingConfig:
    """Configuration for a ``PollingSource``.

    Args:
        interval: Seconds between ``poll()`` calls, measured start-to-start.
        duration: Total seconds to run. ``0`` means indefinite (run until
            cancelled).
        max_missed_intervals: Maximum consecutive tick windows consumed by a
            single poll+fetch cycle before the source terminates.
            Resets to zero on any clean tick.
        max_consecutive_errors: Maximum consecutive ``poll()``/``fetch()``
            failures before the source closes its channel cleanly.
        error_backoff_base: Base wait in seconds for exponential backoff on
            errors. Wait after the nth error is
            ``error_backoff_base * 2 ** (n - 1)``.
    """

    interval: float = 1.0
    duration: float = 0.0
    max_missed_intervals: int = 5
    max_consecutive_errors: int = 3
    error_backoff_base: float = 1.0
```

- [ ] **Step 4: Add `CursorInvalidatedError` to `errors.py`**

Append to `src/orcapod/errors.py`:

```python
class CursorInvalidatedError(Exception):
    """Raised by a ``DynamicSourceProtocol`` implementation when the previous
    cursor is no longer valid and the source state must be rebuilt from scratch.

    This is a terminal condition for ``PollingSource``. Rows already emitted
    downstream cannot be retracted, so continuing would leave downstream
    operators with a corrupted view. ``PollingSource`` catches this, logs a
    clear error, closes its output channel cleanly, and calls ``close()``.

    If full-reset semantics are required, use a static source re-run instead
    of ``PollingSource``.
    """
```

- [ ] **Step 5: Run tests to verify they pass**

```bash
uv run pytest tests/test_channels/test_polling_source.py::TestCursor tests/test_channels/test_polling_source.py::TestPollingConfig tests/test_channels/test_polling_source.py::TestCursorInvalidatedError -v
```

Expected: all 9 tests `PASSED`.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/types.py src/orcapod/errors.py tests/test_channels/test_polling_source.py
git commit -m "feat(types): add Cursor[T], PollingConfig, CursorInvalidatedError for polling source"
```

---

## Task 2: `DynamicSourceProtocol[T]`

**Files:**
- Modify: `src/orcapod/protocols/core_protocols/sources.py`
- Modify: `src/orcapod/protocols/core_protocols/__init__.py`
- Test: `tests/test_channels/test_polling_source.py` (append)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_channels/test_polling_source.py`:

```python
# ===========================================================================
# Task 2: DynamicSourceProtocol conformance
# ===========================================================================


from orcapod.protocols.core_protocols.sources import DynamicSourceProtocol


class _MinimalImpl:
    """Minimal protocol-conformant implementation for isinstance checks."""

    async def poll(self, cursor: Cursor[int] | None = None) -> bool:
        return False

    async def fetch(self, cursor: Cursor[int] | None = None) -> tuple[Cursor[int], dict]:
        return Cursor(value=0), {}

    async def close(self) -> None:
        pass


class TestDynamicSourceProtocol:
    def test_minimal_impl_satisfies_protocol(self):
        impl = _MinimalImpl()
        assert isinstance(impl, DynamicSourceProtocol)

    def test_missing_poll_fails_isinstance(self):
        class NoPoll:
            async def fetch(self, cursor=None):
                return Cursor(value=0), {}

            async def close(self):
                pass

        assert not isinstance(NoPoll(), DynamicSourceProtocol)

    def test_missing_fetch_fails_isinstance(self):
        class NoFetch:
            async def poll(self, cursor=None):
                return False

            async def close(self):
                pass

        assert not isinstance(NoFetch(), DynamicSourceProtocol)

    def test_missing_close_fails_isinstance(self):
        class NoClose:
            async def poll(self, cursor=None):
                return False

            async def fetch(self, cursor=None):
                return Cursor(value=0), {}

        assert not isinstance(NoClose(), DynamicSourceProtocol)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_channels/test_polling_source.py::TestDynamicSourceProtocol -v 2>&1 | head -20
```

Expected: `ImportError` — `DynamicSourceProtocol` not yet defined.

- [ ] **Step 3: Add `DynamicSourceProtocol[T]` to `sources.py`**

Edit `src/orcapod/protocols/core_protocols/sources.py`. Replace the entire file with:

```python
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Generic, Protocol, runtime_checkable

from orcapod.protocols.core_protocols.streams import StreamProtocol
from orcapod.types import Cursor, T

if TYPE_CHECKING:
    from polars._typing import FrameInitTypes

    from orcapod.protocols.database_protocols import DatabaseRegistryProtocol


@runtime_checkable
class SourceProtocol(StreamProtocol, Protocol):
    """
    Protocol for root sources — streams with no upstream dependencies that
    expose provenance identity and optional field resolution.

    A SourceProtocol is a StreamProtocol where:
    - ``source`` is always ``None`` (no upstream pod)
    - ``upstreams`` is always empty
    - ``source_id`` provides a canonical name for registry and provenance
    - ``resolve_field`` enables lookup of individual field values by record id
    """

    @property
    def source_id(self) -> str: ...

    def resolve_field(self, record_id: str, field_name: str) -> Any: ...

    def to_config(
        self, db_registry: DatabaseRegistryProtocol | None = None
    ) -> dict[str, Any]:
        """Serialize source configuration to a JSON-compatible dict."""
        ...

    @classmethod
    def from_config(
        cls,
        config: dict[str, Any],
        db_registry: DatabaseRegistryProtocol | None = None,
    ) -> "SourceProtocol":
        """Reconstruct a source instance from a config dict."""
        ...


@runtime_checkable
class DynamicSourceProtocol(Protocol[T]):
    """User-supplied protocol for a polling data source.

    Implementations provide three async methods. The framework handles
    scheduling, cursor tracking, cache management, error handling, and
    lifecycle.

    Type parameter ``T`` is the cursor value type (e.g. ``datetime``, ``int``,
    ``str``).

    Cursor contract:
        ``poll()`` returns ``True`` if new data is available since the given
        cursor, ``False`` otherwise. Cursor advancement is tied to data
        reading — ``fetch()`` returns a ``(new_cursor, data)`` tuple, and the
        framework advances the cursor only after a successful fetch.

    Full-state invalidation:
        Raise ``CursorInvalidatedError`` from ``poll()``
        or ``fetch()`` when previous state is no longer valid. This is a
        terminal condition — ``PollingSource`` will close its channel cleanly.

    Example::

        class MyDBSource:
            def __init__(self, db):
                self._db = db

            async def poll(
                self, cursor: Cursor[datetime] | None = None
            ) -> bool:
                latest = await self._db.latest_modified_at()
                return cursor is None or latest > cursor.value

            async def fetch(
                self, cursor: Cursor[datetime] | None = None
            ) -> tuple[Cursor[datetime], pl.DataFrame]:
                since = cursor.value if cursor else None
                df = await self._db.fetch_rows_since(since)
                latest = await self._db.latest_modified_at()
                return Cursor(value=latest, modified_at=latest), df

            async def close(self) -> None:
                await self._db.disconnect()
    """

    async def poll(self, cursor: Cursor[T] | None = None) -> bool:
        """Check whether new data is available.

        Args:
            cursor: The framework's current cursor position, or ``None`` on
                the first call.

        Returns:
            ``True`` if new data is available since *cursor*, ``False`` if
            nothing has changed.

        Raises:
            CursorInvalidatedError: If previous state is no longer valid.
        """
        ...

    async def fetch(
        self, cursor: Cursor[T] | None = None
    ) -> tuple[Cursor[T], "FrameInitTypes"]:
        """Fetch data from the given cursor position onward.

        Called only when ``poll()`` has returned ``True``. Returns both the
        new cursor position and the data so cursor advancement is always
        tied to a successful read.

        Args:
            cursor: The current cursor position, or ``None`` on the first
                call. Implementations that cannot filter by cursor may ignore
                this and return full state.

        Returns:
            A tuple ``(new_cursor, data)`` where *data* is anything accepted
            by ``pl.DataFrame()`` — polars DataFrame, pandas DataFrame,
            PyArrow Table, dict, list, etc.

        Raises:
            CursorInvalidatedError: If previous state is no longer valid.
        """
        ...

    async def close(self) -> None:
        """Release resources held by this source.

        Called on every termination path: normal duration expiry, pipeline
        cancellation, max error threshold exceeded, or
        ``CursorInvalidatedError``. The framework
        guarantees ``close()`` is awaited before the output channel is closed.
        """
        ...
```

- [ ] **Step 4: Export `DynamicSourceProtocol` from `core_protocols/__init__.py`**

In `src/orcapod/protocols/core_protocols/__init__.py`, add the import and `__all__` entry:

Change:
```python
from .sources import SourceProtocol
```
to:
```python
from .sources import DynamicSourceProtocol, SourceProtocol
```

And in `__all__`, add `"DynamicSourceProtocol"` after `"SourceProtocol"`.

- [ ] **Step 5: Run tests to verify they pass**

```bash
uv run pytest tests/test_channels/test_polling_source.py::TestDynamicSourceProtocol -v
```

Expected: all 4 tests `PASSED`.

- [ ] **Step 6: Run existing tests to catch regressions**

```bash
uv run pytest tests/ -x -q 2>&1 | tail -10
```

Expected: all existing tests pass.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/protocols/core_protocols/sources.py \
        src/orcapod/protocols/core_protocols/__init__.py \
        tests/test_channels/test_polling_source.py
git commit -m "feat(protocols): add DynamicSourceProtocol[T] for polling data sources"
```

---

## Task 3: `StreamBase.async_iter_data()` Default + `SourceNode.async_execute` Simplification

**Files:**
- Modify: `src/orcapod/core/streams/base.py:242-253`
- Modify: `src/orcapod/core/nodes/source_node.py:300-326`
- Test: `tests/test_channels/test_polling_source.py` (append)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_channels/test_polling_source.py`:

```python
# ===========================================================================
# Task 3: StreamBase.async_iter_data default + SourceNode.async_execute
# ===========================================================================


from orcapod.channels import Channel
from orcapod.core.nodes.source_node import SourceNode
from orcapod.core.streams.arrow_table_stream import ArrowTableStream


def _make_arrow_stream(n: int = 3) -> ArrowTableStream:
    """Build a minimal ArrowTableStream with `id` tag and `val` data column."""
    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("val", pa.int64(), nullable=False),
    ])
    table = pa.table(
        {"id": pa.array(list(range(n)), type=pa.int64()),
         "val": pa.array([i * 10 for i in range(n)], type=pa.int64())},
        schema=schema,
    )
    return ArrowTableStream(table, tag_columns=["id"])


class TestAsyncIterDataDefault:
    @pytest.mark.asyncio
    async def test_default_yields_same_items_as_iter_data(self):
        stream = _make_arrow_stream(3)
        sync_items = list(stream.iter_data())

        async_items = []
        async for item in stream.async_iter_data():
            async_items.append(item)

        assert len(async_items) == 3
        assert len(async_items) == len(sync_items)

    @pytest.mark.asyncio
    async def test_source_node_async_execute_uses_async_iter(self):
        """SourceNode.async_execute routes through async_iter_data — no regression."""
        stream = _make_arrow_stream(2)
        node = SourceNode(stream)

        ch = Channel(buffer_size=8)
        await node.async_execute(ch.writer)

        rows = await ch.reader.collect()
        assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_source_node_closes_channel_after_exhaustion(self):
        stream = _make_arrow_stream(1)
        node = SourceNode(stream)

        ch = Channel(buffer_size=4)
        await node.async_execute(ch.writer)

        # collect() only returns after channel is closed
        rows = await ch.reader.collect()
        assert len(rows) == 1
```

- [ ] **Step 2: Run tests to verify they fail on the stub**

```bash
uv run pytest tests/test_channels/test_polling_source.py::TestAsyncIterDataDefault -v 2>&1 | head -20
```

Expected: `FAILED` with `NotImplementedError` — the stub raises.

- [ ] **Step 3: Replace the `async_iter_data` stub in `StreamBase`**

In `src/orcapod/core/streams/base.py`, replace lines 242–253:

```python
    async def async_iter_data(
        self,
    ) -> AsyncIterator[tuple[TagProtocol, DataProtocol]]:
        """Async iterator over (tag, data) pairs.

        Subclasses should override this to provide true async iteration.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not implement async_iter_data"
        )
        # Make this an async generator so the return type is correct
        yield  # pragma: no cover
```

with:

```python
    async def async_iter_data(
        self,
    ) -> AsyncIterator[tuple[TagProtocol, DataProtocol]]:
        """Async iterator over (tag, data) pairs.

        Default implementation wraps ``iter_data`` as an async generator.
        Subclasses override this to provide true async streaming behaviour
        (e.g. ``PollingSource``).
        """
        for item in self.iter_data():
            yield item
```

- [ ] **Step 4: Simplify `SourceNode.async_execute`**

In `src/orcapod/core/nodes/source_node.py`, replace the `async_execute` method (lines 300–326) with:

```python
    async def async_execute(
        self,
        output: WritableChannel[tuple[cp.TagProtocol, cp.DataProtocol]],
        *,
        observer: ExecutionObserverProtocol | None = None,
    ) -> None:
        """Push all (tag, data) pairs from the wrapped stream to the output channel.

        Delegates to ``async_iter_data``
        so that dynamic sources (e.g. ``PollingSource``)
        stream continuously without modification to this node.

        Args:
            output: Channel to write results to.
            observer: Optional execution observer for hooks.
        """
        if self.stream is None:
            raise RuntimeError(
                "SourceNode in read-only mode has no stream data available"
            )
        node_label = self.label
        node_hash = ""
        try:
            if observer is not None:
                observer.on_node_start(node_label, node_hash)
            async for tag, data in self.stream.async_iter_data():
                await output.send((tag, data))
            if observer is not None:
                observer.on_node_end(node_label, node_hash)
        finally:
            await output.close()
```

- [ ] **Step 5: Run the new tests**

```bash
uv run pytest tests/test_channels/test_polling_source.py::TestAsyncIterDataDefault -v
```

Expected: all 3 tests `PASSED`.

- [ ] **Step 6: Run the full test suite to confirm no regressions**

```bash
uv run pytest tests/ -x -q 2>&1 | tail -15
```

Expected: all existing tests pass (the simplification is behaviourally equivalent).

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/core/streams/base.py \
        src/orcapod/core/nodes/source_node.py \
        tests/test_channels/test_polling_source.py
git commit -m "feat(streams): add async_iter_data default; simplify SourceNode.async_execute"
```

---

## Task 4: `PollingSource` — Sync Infrastructure

**Files:**
- Create: `src/orcapod/core/sources/polling_source.py`
- Modify: `src/orcapod/core/sources/__init__.py`
- Test: `tests/test_channels/test_polling_source.py` (append)

- [ ] **Step 1: Write the failing tests**

Append to `tests/test_channels/test_polling_source.py`:

```python
# ===========================================================================
# Shared test fixture — FakeDynamicSource
# ===========================================================================


class FakeDynamicSource:
    """Hand-written test double for DynamicSourceProtocol.

    Serves pre-loaded batches sequentially. ``poll()`` returns ``True``
    while un-fetched batches remain; ``False`` once all are consumed.
    """

    def __init__(
        self,
        batches: list[Any],
        *,
        poll_always_false: bool = False,
        poll_raises: Exception | None = None,
        fetch_raises: Exception | None = None,
        fetch_delay: float = 0.0,
        cursor_modified_at: datetime | None = None,
    ) -> None:
        self._batches = batches
        self._batch_index = 0
        self._poll_always_false = poll_always_false
        self._poll_raises = poll_raises
        self._fetch_raises = fetch_raises
        self._fetch_delay = fetch_delay
        self._cursor_modified_at = cursor_modified_at
        self.close_called = False
        self.poll_cursors: list[Cursor[int] | None] = []
        self.fetch_cursors: list[Cursor[int] | None] = []

    async def poll(self, cursor: Cursor[int] | None = None) -> bool:
        self.poll_cursors.append(cursor)
        if self._poll_raises is not None:
            raise self._poll_raises
        if self._poll_always_false:
            return False
        current_idx = cursor.value if cursor is not None else 0
        return current_idx < len(self._batches)

    async def fetch(
        self, cursor: Cursor[int] | None = None
    ) -> tuple[Cursor[int], Any]:
        self.fetch_cursors.append(cursor)
        if self._fetch_raises is not None:
            raise self._fetch_raises
        if self._fetch_delay > 0:
            await asyncio.sleep(self._fetch_delay)
        current_idx = cursor.value if cursor is not None else 0
        if current_idx >= len(self._batches):
            return Cursor(value=current_idx, modified_at=self._cursor_modified_at), {}
        data = self._batches[current_idx]
        self._batch_index = current_idx + 1
        return (
            Cursor(value=current_idx + 1, modified_at=self._cursor_modified_at),
            data,
        )

    async def close(self) -> None:
        self.close_called = True


# ===========================================================================
# Task 4: PollingSource sync mode tests
# ===========================================================================


from orcapod.core.sources.polling_source import PollingSource


def _batch(id_: int, val: int) -> dict:
    return {"id": pa.array([id_], type=pa.int64()), "val": pa.array([val], type=pa.int64())}


class TestPollingSourceSyncMode:
    def test_sync_snapshot_fetches_on_first_iter_data(self):
        """First iter_data() triggers fetch(cursor=None) and returns rows."""
        fake = FakeDynamicSource(batches=[_batch(1, 10)])
        src = PollingSource(fake, tag_columns="id", polling_config=PollingConfig(interval=1.0))

        rows = list(src.iter_data())
        assert len(rows) == 1
        # First fetch always receives cursor=None
        assert fake.fetch_cursors == [None]

    def test_sync_snapshot_fetch_called_once_on_repeated_access(self):
        """Second iter_data() polls first; if poll returns False, cache is served."""
        fake = FakeDynamicSource(batches=[_batch(1, 10)], poll_always_false=True)
        src = PollingSource(fake, tag_columns="id", polling_config=PollingConfig(interval=1.0))

        list(src.iter_data())  # first call — fetches
        list(src.iter_data())  # second call — polls (False) → serves cache

        # fetch called once (first access), poll called once (second access)
        assert len(fake.fetch_cursors) == 1
        assert len(fake.poll_cursors) == 1

    def test_sync_cache_miss_refetches_and_combines(self):
        """Second iter_data() re-fetches when poll returns True."""
        fake = FakeDynamicSource(
            batches=[_batch(1, 10), _batch(2, 20)]
        )
        src = PollingSource(fake, tag_columns="id", polling_config=PollingConfig(interval=1.0))

        rows1 = list(src.iter_data())   # fetches batch 0
        rows2 = list(src.iter_data())   # polls → True, fetches batch 1 → combines

        assert len(rows1) == 1
        assert len(rows2) == 2   # accumulated: batch 0 + batch 1

    def test_cursor_threading_fetch_receives_previous_fetch_cursor(self):
        """fetch() receives the cursor returned by the previous fetch call."""
        fake = FakeDynamicSource(batches=[_batch(1, 10), _batch(2, 20)])
        src = PollingSource(fake, tag_columns="id", polling_config=PollingConfig(interval=1.0))

        list(src.iter_data())   # first fetch → cursor returned is Cursor(value=1)
        list(src.iter_data())   # poll(Cursor(1)), then fetch(Cursor(1))

        # poll should receive the cursor from the first fetch
        assert fake.poll_cursors[0] is not None
        assert fake.poll_cursors[0].value == 1
        # second fetch should receive same cursor
        assert fake.fetch_cursors[1] is not None
        assert fake.fetch_cursors[1].value == 1

    def test_last_modified_updated_from_cursor_modified_at(self):
        """cursor.modified_at updates last_modified; None falls back to wall clock."""
        ts = datetime(2026, 3, 15, 12, 0, 0, tzinfo=timezone.utc)
        fake = FakeDynamicSource(batches=[_batch(1, 10)], cursor_modified_at=ts)
        src = PollingSource(fake, tag_columns="id", polling_config=PollingConfig(interval=1.0))

        list(src.iter_data())
        assert src.last_modified == ts

    def test_last_modified_falls_back_to_wall_clock_when_none(self):
        """When cursor.modified_at is None, last_modified is updated to wall clock."""
        fake = FakeDynamicSource(batches=[_batch(1, 10)], cursor_modified_at=None)
        src = PollingSource(fake, tag_columns="id", polling_config=PollingConfig(interval=1.0))

        before = datetime.now(timezone.utc)
        list(src.iter_data())
        after = datetime.now(timezone.utc)

        assert src.last_modified is not None
        assert before <= src.last_modified <= after

    def test_output_schema_triggers_fetch(self):
        """output_schema() lazily triggers fetch on first access."""
        fake = FakeDynamicSource(batches=[_batch(1, 10)])
        src = PollingSource(fake, tag_columns="id", polling_config=PollingConfig(interval=1.0))

        tag_schema, data_schema = src.output_schema()
        assert "id" in tag_schema
        assert "val" in data_schema

    def test_keys_triggers_fetch(self):
        fake = FakeDynamicSource(batches=[_batch(1, 10)])
        src = PollingSource(fake, tag_columns="id", polling_config=PollingConfig(interval=1.0))

        tag_keys, data_keys = src.keys()
        assert "id" in tag_keys
        assert "val" in data_keys

    def test_identity_structure_schema_independent(self):
        """identity_structure() does not require a fetch (schema-independent)."""
        fake = FakeDynamicSource(batches=[])
        src = PollingSource(
            fake, tag_columns="id", polling_config=PollingConfig(interval=1.0),
            source_id="test_source"
        )
        # Should not raise even though no fetch has occurred
        ident = src.identity_structure()
        assert "test_source" in str(ident)

    def test_to_config_returns_non_reconstructable_descriptor(self):
        fake = FakeDynamicSource(batches=[])
        src = PollingSource(
            fake, tag_columns="id", polling_config=PollingConfig(),
            source_id="my_source"
        )
        cfg = src.to_config()
        assert cfg["source_type"] == "polling_source"
        assert cfg["source_id"] == "my_source"

    def test_from_config_raises(self):
        with pytest.raises(NotImplementedError):
            PollingSource.from_config({"source_type": "polling_source"})
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_channels/test_polling_source.py::TestPollingSourceSyncMode -v 2>&1 | head -20
```

Expected: `ImportError` — `PollingSource` not yet defined.

- [ ] **Step 3: Create `src/orcapod/core/sources/polling_source.py`**

```python
"""Protocol-based polling source for async pipelines.

Provides ``PollingSource``, a ``RootSource``
that wraps a ``DynamicSourceProtocol``
implementation. The framework handles scheduling, cursor tracking, cache management,
error handling, and shutdown; the implementation only supplies ``poll``, ``fetch``,
and ``close``.
"""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Collection
from math import floor
from typing import TYPE_CHECKING, Any, Generic

from orcapod.core.sources.base import RootSource
from orcapod.core.sources.stream_builder import SourceStreamBuilder
from orcapod.errors import CursorInvalidatedError
from orcapod.types import ColumnConfig, Cursor, PollingConfig, T
from orcapod.utils import arrow_utils, polars_data_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    from polars._typing import FrameInitTypes

    from orcapod.core.streams.arrow_table_stream import ArrowTableStream
    from orcapod.protocols.core_protocols.sources import DynamicSourceProtocol
    from orcapod.types import Schema
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level sync executor (mirrors data_function.py pattern)
# ---------------------------------------------------------------------------

_sync_executor = None


def _get_sync_executor():
    global _sync_executor
    if _sync_executor is None:
        from concurrent.futures import ThreadPoolExecutor

        _sync_executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="polling_source_sync"
        )
    return _sync_executor


def _run_sync(coro):
    """Run *coro* synchronously, safe even when called from within a running loop."""
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    else:
        return _get_sync_executor().submit(lambda: asyncio.run(coro)).result()


# ---------------------------------------------------------------------------
# PollingSource
# ---------------------------------------------------------------------------


class PollingSource(RootSource, Generic[T]):
    """A root source that continuously emits data via a polling loop.

    Wraps a ``DynamicSourceProtocol``
    implementation. Under async execution (``async_iter_data``), the framework
    polls on a fixed interval and yields new rows as they arrive. Under sync
    execution (``iter_data``), a single poll+fetch cycle is performed on each
    access and results are served from an accumulated in-memory cache.

    Args:
        impl: User-supplied protocol implementation.
        tag_columns: Column name(s) that form the tag (key) for each row.
            All other columns are data columns.
        polling_config: Scheduling and error configuration.
        source_id: Optional stable identifier for this source.
        label: Optional human-readable label.
        data_context: Optional data context key or instance.
        config: Optional Orcapod framework config.

    Note:
        Sync mode calls ``asyncio.run()`` (or a thread executor when inside a
        running loop). This fails without a shim in Jupyter notebooks. Async
        mode has no such restriction.

    Note:
        The accumulated cache is unbounded for true-delta implementations.
        No eviction policy is implemented in this version.
    """

    def __init__(
        self,
        impl: DynamicSourceProtocol[T],
        tag_columns: str | Collection[str],
        polling_config: PollingConfig = PollingConfig(),
        source_id: str | None = None,
        label: str | None = None,
        data_context: str | Any | None = None,
        config: Any | None = None,
    ) -> None:
        super().__init__(
            source_id=source_id,
            label=label,
            data_context=data_context,
            config=config,
        )
        self._impl: DynamicSourceProtocol[T] = impl
        if isinstance(tag_columns, str):
            self._tag_columns: tuple[str, ...] = (tag_columns,)
        else:
            self._tag_columns = tuple(tag_columns)
        self._polling_config = polling_config
        self._cursor: Cursor[T] | None = None
        self._schema_stream: ArrowTableStream | None = None

    # -------------------------------------------------------------------------
    # Identity
    # -------------------------------------------------------------------------

    def identity_structure(self) -> Any:
        """Schema-independent identity (schema is unknown until first fetch)."""
        return (self.__class__.__name__, self._tag_columns, self._source_id or "")

    # -------------------------------------------------------------------------
    # Sync stream delegation — all route through _get_latest_stream()
    # -------------------------------------------------------------------------

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        return self._get_latest_stream().output_schema(columns=columns, all_info=all_info)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        return self._get_latest_stream().keys(columns=columns, all_info=all_info)

    def iter_data(self):
        return self._get_latest_stream().iter_data()

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table:
        return self._get_latest_stream().as_table(columns=columns, all_info=all_info)

    # -------------------------------------------------------------------------
    # Serialization
    # -------------------------------------------------------------------------

    def to_config(self, db_registry: Any = None) -> dict[str, Any]:
        """Serialize this source to a JSON-compatible dict.

        Delegates to the ``DynamicSourceProtocol`` implementation's
        ``to_config()``. If the implementation returns ``None`` (not
        serializable), returns a non-reconstructable descriptor.
        """
        impl_config = self._impl.to_config()
        if impl_config is None:
            return {
                "source_type": "polling_source",
                "reconstructable": False,
                "source_id": self._source_id,
            }
        return {
            "source_type": "polling_source",
            "impl_class": f"{type(self._impl).__module__}.{type(self._impl).__qualname__}",
            "impl_config": impl_config,
            "tag_columns": list(self._tag_columns),
            "source_id": self._source_id,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any], db_registry: Any = None) -> PollingSource:
        """Reconstruct a ``PollingSource`` from a config dict.

        Uses the stored ``impl_class`` to import and call
        ``DynamicSourceProtocol.from_config(impl_config)``.

        Raises:
            NotImplementedError: If the stored config has ``reconstructable: False``.
        """
        if not config.get("reconstructable", True):
            raise NotImplementedError(
                "PollingSource cannot be reconstructed — "
                "the DynamicSourceProtocol implementation is not serializable."
            )
        import importlib

        module_name, _, class_name = config["impl_class"].rpartition(".")
        module = importlib.import_module(module_name)
        impl_cls = getattr(module, class_name)
        impl = impl_cls.from_config(config["impl_config"])
        return cls(
            impl,
            tag_columns=config["tag_columns"],
            source_id=config.get("source_id"),
        )

    # -------------------------------------------------------------------------
    # Internal sync helpers
    # -------------------------------------------------------------------------

    def _get_latest_stream(self) -> ArrowTableStream:
        """Return the current accumulated stream, fetching/polling as needed."""
        if self._schema_stream is None:
            # First access — no cache yet; fetch immediately
            logger.debug("PollingSource %r: first sync access — fetching", self._source_id)
            new_cursor, data = _run_sync(self._impl.fetch(cursor=None))
            self._schema_stream = self._build_stream(data)
            self._cursor = new_cursor
            self._update_last_modified_from_cursor(new_cursor)
        else:
            # Have cache — poll for updates
            has_new = _run_sync(self._impl.poll(cursor=self._cursor))
            if has_new:
                logger.debug("PollingSource %r: sync poll found new data — fetching", self._source_id)
                new_cursor, data = _run_sync(self._impl.fetch(cursor=self._cursor))
                self._schema_stream = self._combine(self._schema_stream, data)
                self._cursor = new_cursor
                self._update_last_modified_from_cursor(new_cursor)
            else:
                logger.debug("PollingSource %r: sync poll — cache still valid", self._source_id)
        return self._schema_stream

    def _build_stream(self, data: FrameInitTypes) -> ArrowTableStream:
        """Build an ``ArrowTableStream`` from raw ``FrameInitTypes`` data."""
        from orcapod.core.streams.arrow_table_stream import ArrowTableStream

        df = pl.DataFrame(data)

        # Handle Object-dtype columns (same pattern as DataFrameSource)
        object_columns = [c for c in df.columns if df[c].dtype == pl.Object]
        if object_columns:
            sub_table = self.data_context.type_converter.python_dicts_to_arrow_table(
                df.select(object_columns).to_dicts()
            )
            df = df.with_columns([pl.from_arrow(c) for c in sub_table])

        df = polars_data_utils.drop_system_columns(df)

        arrow_table = df.to_arrow()
        arrow_table = arrow_table.cast(arrow_utils.infer_schema_nullable(arrow_table))

        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        result = builder.build(
            arrow_table,
            tag_columns=self._tag_columns,
            source_id=self._source_id,
        )

        if self._source_id is None:
            self._source_id = result.source_id

        return result.stream

    def _combine(
        self, existing: ArrowTableStream, new_stream: ArrowTableStream
    ) -> ArrowTableStream:
        """Append *new_stream* rows to *existing*, raising on schema mismatch.

        Raises:
            InputValidationError: If the column sets differ or any column type
                has changed between batches.
        """
        from orcapod.core.streams.arrow_table_stream import ArrowTableStream

        self._validate_combining_schemas(existing, new_stream)

        combined = pa.concat_tables(
            [
                existing.as_table(all_info=True),
                new_stream.as_table(all_info=True),
            ],
        )
        return ArrowTableStream(table=combined, tag_columns=existing._tag_columns)

    def _update_last_modified_from_cursor(self, cursor: Cursor[T]) -> None:
        """Update ``last_modified`` from cursor or fall back to wall clock."""
        if cursor.modified_at is not None:
            self._set_modified_time(cursor.modified_at)
        else:
            self._update_modified_time()

    # -------------------------------------------------------------------------
    # Async mode — full polling loop
    # -------------------------------------------------------------------------

    async def async_iter_data(self):
        """Async generator that continuously emits (tag, data) pairs.

        Pre-seeds from the cached stream (if any) before entering the polling
        loop. The loop runs until: the configured duration elapses, the maximum
        consecutive error or overrun threshold is exceeded, a
        ``CursorInvalidatedError`` is raised, or the task
        is cancelled.

        ``impl.close()`` is always awaited before returning.
        """
        # Pre-seed from cache
        if self._schema_stream is not None:
            logger.debug("PollingSource %r: pre-seeding %d cached row(s)", self._source_id, len(list(self._schema_stream.iter_data())))
            for item in self._schema_stream.iter_data():
                yield item

        cfg = self._polling_config
        loop = asyncio.get_running_loop()
        start_time = loop.time()
        next_tick = start_time
        consecutive_misses = 0
        consecutive_errors = 0

        logger.info(
            "PollingSource %r starting (interval=%.2fs, duration=%.1fs)",
            self._source_id,
            cfg.interval,
            cfg.duration,
        )

        try:
            while True:
                # 1. Sleep until next scheduled tick
                now = loop.time()
                if next_tick > now:
                    await asyncio.sleep(next_tick - now)

                # 2. Poll + fetch
                try:
                    has_new = await self._impl.poll(cursor=self._cursor)

                    if has_new:
                        logger.debug(
                            "PollingSource %r: new data detected, fetching",
                            self._source_id,
                        )
                        new_cursor, data = await self._impl.fetch(cursor=self._cursor)
                        if self._schema_stream is None:
                            self._schema_stream = self._build_stream(data)
                        else:
                            self._schema_stream = self._combine(self._schema_stream, data)
                        self._cursor = new_cursor
                        self._update_last_modified_from_cursor(new_cursor)

                        # Collect and emit new rows
                        new_stream = self._build_stream(data)
                        rows = list(new_stream.iter_data())
                        logger.debug(
                            "PollingSource %r: emitting %d row(s)",
                            self._source_id,
                            len(rows),
                        )
                        for item in rows:
                            yield item
                    else:
                        logger.debug(
                            "PollingSource %r: poll returned no new data",
                            self._source_id,
                        )

                    consecutive_errors = 0

                except asyncio.CancelledError:
                    raise

                except CursorInvalidatedError:
                    logger.error(
                        "PollingSource %r: cursor invalidated — previous state cannot "
                        "be reconciled with already-emitted rows. Terminating source.",
                        self._source_id,
                    )
                    return

                except Exception as e:
                    consecutive_errors += 1
                    backoff = cfg.error_backoff_base * 2 ** (consecutive_errors - 1)
                    logger.error(
                        "PollingSource %r: poll/fetch error (consecutive=%d, "
                        "backoff=%.1fs): %s",
                        self._source_id,
                        consecutive_errors,
                        backoff,
                        e,
                    )
                    if consecutive_errors >= cfg.max_consecutive_errors:
                        logger.error(
                            "PollingSource %r: max consecutive errors (%d) reached. "
                            "Terminating source.",
                            self._source_id,
                            cfg.max_consecutive_errors,
                        )
                        return
                    await asyncio.sleep(backoff)
                    continue  # retry — do not advance next_tick

                # 3. Tick advancement (start-to-start)
                now = loop.time()
                intervals_consumed = floor((now - next_tick) / cfg.interval)
                if intervals_consumed > 0:
                    consecutive_misses += intervals_consumed
                    logger.warning(
                        "PollingSource %r: tick overrun — consumed %d interval(s) "
                        "(consecutive_misses=%d/%d)",
                        self._source_id,
                        intervals_consumed,
                        consecutive_misses,
                        cfg.max_missed_intervals,
                    )
                    if consecutive_misses >= cfg.max_missed_intervals:
                        logger.error(
                            "PollingSource %r: overrun threshold exceeded. "
                            "Terminating source.",
                            self._source_id,
                        )
                        return
                else:
                    consecutive_misses = 0
                next_tick += (intervals_consumed + 1) * cfg.interval

                # 4. Duration check
                if cfg.duration > 0 and (loop.time() - start_time) >= cfg.duration:
                    logger.info(
                        "PollingSource %r: duration limit (%.1fs) reached. "
                        "Terminating source.",
                        self._source_id,
                        cfg.duration,
                    )
                    return

        except asyncio.CancelledError:
            logger.info(
                "PollingSource %r: cancelled — shutting down cleanly.",
                self._source_id,
            )

        finally:
            logger.debug("PollingSource %r: calling impl.close()", self._source_id)
            await self._impl.close()
            logger.info("PollingSource %r: closed.", self._source_id)
```

- [ ] **Step 4: Export `PollingSource` from `sources/__init__.py`**

In `src/orcapod/core/sources/__init__.py`, add:

```python
from .polling_source import PollingSource
```

And add `"PollingSource"` to `__all__`.

- [ ] **Step 5: Run sync tests**

```bash
uv run pytest tests/test_channels/test_polling_source.py::TestPollingSourceSyncMode -v
```

Expected: all 10 tests `PASSED`.

- [ ] **Step 6: Run full suite**

```bash
uv run pytest tests/ -x -q 2>&1 | tail -10
```

Expected: no regressions.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/core/sources/polling_source.py \
        src/orcapod/core/sources/__init__.py \
        tests/test_channels/test_polling_source.py
git commit -m "feat(sources): add PollingSource sync infrastructure"
```

---

## Task 5: `PollingSource` — Async Polling Loop Tests (Basic)

**Files:**
- Test: `tests/test_channels/test_polling_source.py` (append)

The implementation was already added in Task 4's `polling_source.py`. This task adds and verifies the async tests.

- [ ] **Step 1: Write the async tests**

Append to `tests/test_channels/test_polling_source.py`:

```python
# ===========================================================================
# Task 5: PollingSource async mode tests (basic)
# ===========================================================================


class TestPollingSourceAsyncMode:
    @pytest.mark.asyncio
    async def test_async_streaming_emits_rows(self):
        """Positive polls trigger fetch; rows are emitted to async_iter_data."""
        fake = FakeDynamicSource(batches=[_batch(1, 10), _batch(2, 20)])
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=0.01, duration=0.5),
        )

        items = []
        async for tag, data in src.async_iter_data():
            items.append((tag, data))

        assert len(items) == 2
        assert fake.close_called

    @pytest.mark.asyncio
    async def test_negative_poll_emits_nothing(self):
        """poll() returning False emits nothing; fetch is never called."""
        fake = FakeDynamicSource(batches=[_batch(1, 10)], poll_always_false=True)
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=0.01, duration=0.05),
        )

        items = []
        async for tag, data in src.async_iter_data():
            items.append((tag, data))

        assert len(items) == 0
        assert len(fake.fetch_cursors) == 0

    @pytest.mark.asyncio
    async def test_pre_seeding_yields_cached_rows_first(self):
        """async_iter_data() yields cached rows before entering the polling loop."""
        fake = FakeDynamicSource(batches=[_batch(1, 10)])
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=1.0),
        )

        # Seed the cache synchronously first
        list(src.iter_data())

        # Now supply a second fake with no new data
        fake2 = FakeDynamicSource(batches=[], poll_always_false=True)
        src._impl = fake2

        # async_iter_data should yield the 1 cached row, then stop after duration
        items = []
        src._polling_config = PollingConfig(interval=0.01, duration=0.05)
        async for tag, data in src.async_iter_data():
            items.append((tag, data))

        assert len(items) == 1  # pre-seeded row
        assert fake2.close_called

    @pytest.mark.asyncio
    async def test_cache_combining_accumulates_rows(self):
        """After two delta fetches, async stream contains rows from both batches."""
        fake = FakeDynamicSource(batches=[_batch(1, 10), _batch(2, 20)])
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=0.01, duration=0.3),
        )

        items = []
        async for tag, data in src.async_iter_data():
            items.append((tag, data))

        assert len(items) == 2
        # Internal cache should hold both rows
        assert src._schema_stream is not None
        cached_rows = list(src._schema_stream.iter_data())
        assert len(cached_rows) == 2

    @pytest.mark.asyncio
    async def test_clean_shutdown_on_cancellation(self):
        """CancelledError causes close() to be awaited before terminating."""
        fake = FakeDynamicSource(batches=[], poll_always_false=True)
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=0.01, duration=0.0),
        )

        async def run():
            async for _ in src.async_iter_data():
                pass

        task = asyncio.create_task(run())
        await asyncio.sleep(0.05)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        assert fake.close_called

    @pytest.mark.asyncio
    async def test_duration_limit_terminates_source(self):
        """Source stops naturally after config.duration seconds."""
        fake = FakeDynamicSource(batches=[], poll_always_false=True)
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=0.01, duration=0.08),
        )

        items = []
        async for tag, data in src.async_iter_data():
            items.append((tag, data))

        assert fake.close_called

    @pytest.mark.asyncio
    async def test_indefinite_mode_runs_until_cancelled(self):
        """duration=0 runs until explicitly cancelled."""
        fake = FakeDynamicSource(batches=[], poll_always_false=True)
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=0.01, duration=0.0),
        )

        async def run():
            async for _ in src.async_iter_data():
                pass

        task = asyncio.create_task(run())
        # Task should still be running after a short sleep
        await asyncio.sleep(0.05)
        assert not task.done()
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        assert fake.close_called

    @pytest.mark.asyncio
    async def test_cursor_threaded_through_async_fetches(self):
        """In async mode fetch() receives the cursor from the previous fetch."""
        fake = FakeDynamicSource(batches=[_batch(1, 10), _batch(2, 20)])
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=0.01, duration=0.3),
        )

        async for _ in src.async_iter_data():
            pass

        # First fetch: cursor=None; second fetch: cursor=Cursor(value=1)
        assert fake.fetch_cursors[0] is None
        if len(fake.fetch_cursors) > 1:
            assert fake.fetch_cursors[1] is not None
            assert fake.fetch_cursors[1].value == 1
```

- [ ] **Step 2: Run the async tests**

```bash
uv run pytest tests/test_channels/test_polling_source.py::TestPollingSourceAsyncMode -v
```

Expected: all 8 tests `PASSED`.

- [ ] **Step 3: Commit**

```bash
git add tests/test_channels/test_polling_source.py
git commit -m "test(sources): add async polling loop tests for PollingSource"
```

---

## Task 6: `PollingSource` — Error Handling, Overrun, and Schema Drift

**Files:**
- Test: `tests/test_channels/test_polling_source.py` (append)

- [ ] **Step 1: Write the tests**

Append to `tests/test_channels/test_polling_source.py`:

```python
# ===========================================================================
# Task 6: Error handling, overrun, schema drift tests
# ===========================================================================


class TestPollingSourceErrorHandling:
    @pytest.mark.asyncio
    async def test_error_backoff_retries_and_uses_exponential_wait(self):
        """Exception in poll() increments error counter; errors below threshold → retry."""

        class CountingError(Exception):
            pass

        call_count = 0

        class TransientImpl:
            async def poll(self, cursor=None) -> bool:
                nonlocal call_count
                call_count += 1
                if call_count <= 2:
                    raise CountingError("transient")
                return False  # succeed on 3rd attempt

            async def fetch(self, cursor=None):
                return Cursor(value=0), {}

            async def close(self):
                pass

        src = PollingSource(
            TransientImpl(),
            tag_columns="id",
            polling_config=PollingConfig(
                interval=0.01,
                duration=0.5,
                max_consecutive_errors=5,
                error_backoff_base=0.01,  # very short backoff for tests
            ),
        )

        items = []
        async for tag, data in src.async_iter_data():
            items.append((tag, data))

        # Completed without raising; errors were below threshold
        assert call_count >= 3

    @pytest.mark.asyncio
    async def test_max_consecutive_errors_closes_cleanly(self):
        """Reaching max_consecutive_errors closes the channel without crashing."""
        fake = FakeDynamicSource(
            batches=[],
            poll_raises=RuntimeError("persistent error"),
        )
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(
                interval=0.01,
                max_consecutive_errors=3,
                error_backoff_base=0.01,
            ),
        )

        items = []
        async for tag, data in src.async_iter_data():
            items.append((tag, data))

        assert len(items) == 0
        assert fake.close_called

    @pytest.mark.asyncio
    async def test_cursor_invalidated_error_terminates_without_retry(self):
        """CursorInvalidatedError terminates the source cleanly; no retry."""
        fake = FakeDynamicSource(
            batches=[],
            poll_raises=CursorInvalidatedError("state lost"),
        )
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=0.01),
        )

        items = []
        async for tag, data in src.async_iter_data():
            items.append((tag, data))

        # Only one poll attempt — no retry for invalidation
        assert len(fake.poll_cursors) == 1
        assert fake.close_called

    @pytest.mark.asyncio
    async def test_overrun_terminates_after_threshold(self):
        """Slow fetches that consistently overrun max_missed_intervals terminate the source."""
        fake = FakeDynamicSource(
            batches=[_batch(i, i * 10) for i in range(20)],
            fetch_delay=0.05,  # each fetch takes 50 ms
        )
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(
                interval=0.01,           # 10 ms tick — fetch will always overrun
                max_missed_intervals=3,  # terminate after 3 cumulative misses
                duration=0.0,            # run indefinitely (until overrun)
            ),
        )

        items = []
        async for tag, data in src.async_iter_data():
            items.append((tag, data))

        # Terminated due to overrun; close() must have been called
        assert fake.close_called

    @pytest.mark.asyncio
    async def test_schema_drift_emits_warning(self, caplog):
        """Mismatched columns on second fetch emit a WARNING log."""

        class DriftingImpl:
            _call = 0

            async def poll(self, cursor=None) -> bool:
                return self._call < 2

            async def fetch(self, cursor=None):
                self._call += 1
                if self._call == 1:
                    return (
                        Cursor(value=1),
                        {"id": pa.array([1], type=pa.int64()),
                         "val": pa.array([10], type=pa.int64())},
                    )
                # Second fetch has an extra column
                return (
                    Cursor(value=2),
                    {"id": pa.array([2], type=pa.int64()),
                     "val": pa.array([20], type=pa.int64()),
                     "extra": pa.array(["x"], type=pa.large_string())},
                )

            async def close(self):
                pass

        src = PollingSource(
            DriftingImpl(),
            tag_columns="id",
            polling_config=PollingConfig(interval=0.01, duration=0.3),
        )

        with caplog.at_level(logging.WARNING, logger="orcapod.core.sources.polling_source"):
            async for _ in src.async_iter_data():
                pass

        assert any("schema drift" in r.message for r in caplog.records)
```

- [ ] **Step 2: Run the tests**

```bash
uv run pytest tests/test_channels/test_polling_source.py::TestPollingSourceErrorHandling -v
```

Expected: all 5 tests `PASSED`.

- [ ] **Step 3: Run the full suite**

```bash
uv run pytest tests/ -x -q 2>&1 | tail -10
```

Expected: no regressions.

- [ ] **Step 4: Commit**

```bash
git add tests/test_channels/test_polling_source.py
git commit -m "test(sources): add error handling, overrun, and schema drift tests for PollingSource"
```

---

## Task 7: Full Test Run + Cleanup

**Files:**
- Review/clean: `tests/test_channels/test_polling_source.py`

- [ ] **Step 1: Run the complete polling source test file**

```bash
uv run pytest tests/test_channels/test_polling_source.py -v 2>&1 | tail -40
```

Expected: all tests `PASSED`. Count of tests should be at least 19.

- [ ] **Step 2: Run the complete test suite**

```bash
uv run pytest tests/ -q 2>&1 | tail -15
```

Expected: all tests pass with zero failures.

- [ ] **Step 3: Verify exports are importable from public API**

```bash
uv run python -c "
from orcapod.core.sources import PollingSource
from orcapod.protocols.core_protocols import DynamicSourceProtocol
from orcapod.types import Cursor, PollingConfig
from orcapod.errors import CursorInvalidatedError
print('All public exports OK')
print(f'  PollingSource: {PollingSource}')
print(f'  DynamicSourceProtocol: {DynamicSourceProtocol}')
print(f'  Cursor: {Cursor}')
print(f'  PollingConfig: {PollingConfig}')
print(f'  CursorInvalidatedError: {CursorInvalidatedError}')
"
```

Expected output:
```
All public exports OK
  PollingSource: <class 'orcapod.core.sources.polling_source.PollingSource'>
  DynamicSourceProtocol: <class 'orcapod.protocols.core_protocols.sources.DynamicSourceProtocol'>
  Cursor: <class 'orcapod.types.Cursor'>
  PollingConfig: <class 'orcapod.types.PollingConfig'>
  CursorInvalidatedError: <class 'orcapod.errors.CursorInvalidatedError'>
```

- [ ] **Step 4: Final commit**

```bash
git add .
git commit -m "chore(sources): verify all polling source exports accessible"
```

---

## Notes for Implementors

**`_run_sync` pattern:** Mirrors `data_function.py`. When `asyncio.get_running_loop()` raises `RuntimeError`, there is no running loop and `asyncio.run()` is safe. When already inside a loop (e.g. in a test), offload to a `ThreadPoolExecutor` thread with its own loop.

**Pre-seeding in `async_iter_data`:** The pre-seed iterates `self._schema_stream.iter_data()` before yielding from it, only to get the count for the log message. This double-iterates. A simpler approach is to yield directly and log the count after:
```python
pre_seed_count = 0
for item in self._schema_stream.iter_data():
    yield item
    pre_seed_count += 1
logger.debug("PollingSource %r: pre-seeded %d row(s)", self._source_id, pre_seed_count)
```

**Async emit vs. cache update:** In `async_iter_data`, new rows are converted to stream items by calling `_build_stream(data).iter_data()`. This is slightly redundant with `_combine()` which also calls `_build_stream()`. A future optimization could share the built stream, but correctness takes priority.

**`pa.concat_tables` schema compatibility:** When combining two streams built from the same `tag_columns` and `source_id`, their schemas should be compatible. If schema drift occurred and a column was added, pass `promote_options="default"` to `pa.concat_tables` to fill missing fields with nulls.

**Branch:** `eywalker/plt-1430-dynamic-data-source-protocol-based-polling-source-for-async`
