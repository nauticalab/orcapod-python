# Design: Protocol-Based Polling Source for Async Pipelines

**Issue:** PLT-1430  
**Date:** 2026-05-20  
**Status:** Approved

---

## Overview

All existing Orcapod sources are static snapshots: they produce a fixed set of rows at
construction time and close their output channel after the first batch. Under async
execution this leaves value on the table — an orchestrator can keep a channel open and
push new rows as they arrive.

This document specifies the first dynamic data source: a generic, protocol-based polling
source (`PollingSource`) where the user supplies a small `DynamicSourceProtocol`
implementation. The framework handles scheduling, cursor tracking, cache management,
error handling, and shutdown.

---

## Goals & Success Criteria

- `DynamicSourceProtocol[T]` is defined with `poll`, `fetch`, and `close`.
- `Cursor[T]` is a generic dataclass carrying an opaque cursor value and an optional
  modification timestamp.
- `PollingSource[T](RootSource)` wraps a `DynamicSourceProtocol[T]` implementation and
  emits rows continuously under async execution.
- Sync execution (`iter_data()`) performs a poll-based staleness check against the cursor
  and serves from cache when the source has not changed.
- `StreamBase.async_iter_data()` gains a real default implementation (wrapping
  `iter_data()`), replacing the current `raise NotImplementedError` stub.
- `SourceNode.async_execute` is simplified to always call `async_iter_data()`.
- Tests cover: async streaming, sync snapshot, cursor threading, staleness/cache,
  overrun skip, overrun error, error backoff, clean shutdown, duration limit,
  indefinite mode, schema drift warning, `CursorInvalidatedError` terminal exit, and
  `async_iter_data` default regression.

---

## Scope & Boundaries

In scope:

- `Cursor[T]` dataclass and `DynamicSourceProtocol[T]` protocol.
- `CursorInvalidatedError` exception.
- `PollingConfig` frozen dataclass.
- `PollingSource[T](RootSource, Generic[T])`.
- Default `StreamBase.async_iter_data()` implementation.
- Simplified `SourceNode.async_execute`.
- Tests for all behaviour described here.
- Docstring example showing a minimal `DynamicSourceProtocol` implementation.

Out of scope:

- Database-backed dynamic source (follow-up issue).
- Framework-level deduplication or row-identity tracking.
- Schema enforcement across `fetch()` calls (drift is warned, not rejected).
- Pipeline serialization / `from_config` for `PollingSource` (non-reconstructable,
  same as `DataFrameSource`).

**Natural backpressure:** `PollingSource` inherits backpressure automatically from the
channel. `SourceNode.async_execute` calls `await output.send(...)` for each item yielded
by `async_iter_data()`. If the downstream channel is full (bounded), `send()` suspends,
which suspends the `async for` loop, which suspends the polling loop at the `yield item`
point. No additional mechanism is needed.

---

## New Types

### `T` TypeVar

`T = TypeVar("T")` is defined in `src/orcapod/types.py` alongside `Cursor` and
exported. `DynamicSourceProtocol` in `sources.py` imports it from there.

### `FrameInitTypes`

`FrameInitTypes` is imported from `polars._typing` and represents anything that
`pl.DataFrame(data)` accepts: a `polars.DataFrame`, `pandas.DataFrame`,
`pyarrow.Table`, `dict`, list of dicts, etc. `DataFrameSource` uses the same type
for its `data` parameter.

### `Cursor[T]`

Lives in `src/orcapod/types.py` alongside `NodeConfig`, `PipelineConfig`, etc.

```python
T = TypeVar("T")

@dataclass
class Cursor(Generic[T]):
    """Marks the current position in a DynamicSource's data stream.

    Args:
        value: Implementation-defined cursor value. May be a datetime
            timestamp, integer offset, string pagination token, or any
            other type meaningful to the implementation.
        modified_at: Optional wall-clock time when the source content at
            this cursor position was last modified. When provided, the
            PollingSource uses this to update its last_modified timestamp
            for downstream staleness detection. When None, the framework
            falls back to its own wall clock.
    """
    value: T
    modified_at: datetime | None = None
```

### `CursorInvalidatedError`

Lives in `src/orcapod/errors.py`.

```python
class CursorInvalidatedError(Exception):
    """Raised by a DynamicSourceProtocol implementation when the previous
    cursor is no longer valid and the source state must be rebuilt from scratch.

    This is a terminal condition for PollingSource. Rows already emitted
    downstream cannot be retracted, so continuing would leave downstream
    operators with a corrupted view. The PollingSource catches this, logs a
    clear error, closes its output channel cleanly (allowing downstream to
    drain), and calls close().

    If full-reset semantics are required, use a static source re-run instead
    of PollingSource.
    """
```

### `PollingConfig`

Lives in `src/orcapod/types.py`.

```python
@dataclass(frozen=True)
class PollingConfig:
    """Configuration for a PollingSource.

    Args:
        interval: Seconds between poll() calls, measured start-to-start.
        duration: Total seconds to run. 0 means indefinite (run until
            cancelled).
        max_missed_intervals: Maximum consecutive tick windows that may be
            consumed by a single poll+fetch cycle before the source errors
            out. Resets to zero on any clean tick.
        max_consecutive_errors: Maximum consecutive poll()/fetch() failures
            before the source closes its channel cleanly.
        error_backoff_base: Base wait in seconds for exponential backoff on
            errors. Wait after the nth error is
            error_backoff_base * 2 ** (n - 1).
    """
    interval: float = 1.0
    duration: float = 0.0
    max_missed_intervals: int = 5
    max_consecutive_errors: int = 3
    error_backoff_base: float = 1.0
```

---

## `DynamicSourceProtocol[T]`

Lives in `src/orcapod/protocols/core_protocols/sources.py` alongside the existing
`SourceProtocol`.

```python
@runtime_checkable
class DynamicSourceProtocol(Protocol[T]):
    """User-supplied protocol for a polling data source.

    Implementations provide three async methods. The framework handles
    scheduling, cursor tracking, cache management, error handling, and
    lifecycle — the implementation only needs to know how to check for new
    data, fetch it, and release resources.

    Type parameter T is the cursor value type (e.g. datetime, int, str).

    Deduplication contract:
        Emitting the same row more than once is acceptable (wasteful but not
        incorrect). Deduplication is explicitly not the framework's
        responsibility.

    Cursor contract:
        poll() receives the framework's current cursor and returns True if new
        data is available, False otherwise. Cursor advancement is tied to data
        reading — fetch() returns a (new_cursor, data) tuple, and the framework
        advances the cursor only after a successful fetch. The cursor value is
        opaque to the framework — it is created by fetch() and passed back
        verbatim to the next poll() and fetch() calls. Implementations that
        cannot filter by cursor may ignore it and return full state; the
        framework will combine results correctly.

    Full-state invalidation:
        If the implementation detects that previous state is no longer valid
        (e.g. the underlying dataset was truncated or reset), it should raise
        CursorInvalidatedError from poll() or fetch(). This is a terminal
        condition — the PollingSource will close its channel cleanly and stop.

    Example minimal implementation::

        class MyDBSource:
            def __init__(self, db):
                self._db = db

            async def poll(
                self, cursor: Cursor[datetime] | None = None
            ) -> bool:
                latest = await self._db.latest_modified_at()
                if cursor is None or latest > cursor.value:
                    return True
                return False

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

    async def poll(
        self, cursor: Cursor[T] | None = None
    ) -> bool:
        """Check whether new data is available.

        Args:
            cursor: The framework's current cursor position, or None on the
                first call.

        Returns:
            True if new data is available since cursor, False if nothing has
            changed.

        Raises:
            CursorInvalidatedError: If previous state is no longer valid.
        """
        ...

    async def fetch(
        self, cursor: Cursor[T] | None = None
    ) -> tuple[Cursor[T], FrameInitTypes]:
        """Fetch data from the given cursor position onward.

        Called only when poll() has returned True. The cursor argument is the
        framework's current position — i.e. "give me everything that changed
        since here." Returns both the new cursor position and the data, so
        cursor advancement is always tied to a successful data read.

        Args:
            cursor: The current cursor position, or None on the first call.
                Implementations that cannot filter by cursor may ignore this
                and return full state.

        Returns:
            A tuple of (new_cursor, data) where new_cursor marks the new
            position in the stream and data is anything accepted by
            pl.DataFrame() — polars DataFrame, pandas DataFrame, PyArrow
            Table, dict, list, etc.

        Raises:
            CursorInvalidatedError: If previous state is no longer valid.
        """
        ...

    async def close(self) -> None:
        """Release resources held by this source.

        Called on every termination path: normal duration expiry, pipeline
        cancellation, max error threshold exceeded, or CursorInvalidatedError.
        The framework guarantees close() is awaited before the output channel
        is closed.
        """
        ...
```

---

## `PollingSource[T]`

Lives in `src/orcapod/core/sources/polling_source.py`.

### Constructor

```python
class PollingSource(RootSource, Generic[T]):
    def __init__(
        self,
        impl: DynamicSourceProtocol[T],
        tag_columns: str | Collection[str],
        config: PollingConfig = PollingConfig(),
        source_id: str | None = None,
        label: str | None = None,
        data_context: str | DataContext | None = None,
        config: Config | None = None,
    ) -> None:
```

`tag_columns` specifies which columns in the DataFrame returned by `fetch()` are tag
(key) columns. All remaining columns are data columns.

### Internal State

| Attribute | Type | Purpose |
|---|---|---|
| `_impl` | `DynamicSourceProtocol[T]` | User-supplied implementation |
| `_tag_columns` | `tuple[str, ...]` | Normalised tag column names |
| `_config` | `PollingConfig` | Scheduling and error config |
| `_cursor` | `Cursor[T] \| None` | Current cursor; advances after each successful fetch |
| `_schema_stream` | `ArrowTableStream \| None` | Accumulated cache (None until first fetch) |

### Schema Inference (Lazy)

`output_schema()`, `keys()`, `iter_data()`, and `as_table()` all route through an
internal `_get_latest_stream()` method. On the first call, `_get_latest_stream()` runs
`fetch(cursor=None)` synchronously, which returns `(new_cursor, data)`, and builds the
initial `ArrowTableStream` using the same `pl.DataFrame(data)` → `SourceStreamBuilder`
pipeline that `DataFrameSource` uses. The cursor returned from the first fetch becomes
`_cursor`.

### Sync Mode — `iter_data()`

`iter_data()` delegates to `_get_latest_stream().iter_data()`.

`_get_latest_stream()` logic:

```
if _schema_stream is None:
    # First access — no cache yet; cursor comes from fetch
    new_cursor, df = run_sync(impl.fetch(cursor=None))
    _schema_stream = build_stream(df)
    _cursor = new_cursor
    _update_last_modified(new_cursor)
else:
    # Have cache — check for updates via poll (pure bool check)
    has_new = run_sync(impl.poll(cursor=_cursor))
    if has_new:
        new_cursor, df = run_sync(impl.fetch(cursor=_cursor))
        _schema_stream = _combine(_schema_stream, df)
        _cursor = new_cursor
        _update_last_modified(new_cursor)
    # else: cache still valid, serve as-is
return _schema_stream
```

`run_sync()` uses `asyncio.get_event_loop().run_until_complete()` — the same approach
used by the rest of the sync path in this codebase.

### Cache Combining

When `fetch(cursor=prev)` returns a delta (rows new since `prev`), the new rows are
appended to the existing cached stream:

```python
combined = pa.concat_tables([_schema_stream.as_table(), new_stream.as_table()])
_schema_stream = ArrowTableStream(combined, tag_columns=_tag_columns)
```

This applies in both sync and async modes. The cache always represents the full
accumulated known state.

If an implementation returns full state on every fetch (ignoring the cursor), the cache
will accumulate duplicates. This is documented as wasteful-but-correct, consistent with
the deduplication contract.

**Memory note:** the cache grows unboundedly for long-running sources using true-delta
implementations. This is a conscious trade-off; no eviction policy is implemented in
this version.

### Schema Drift Warning

After each `fetch()`, if the returned DataFrame's columns differ from the cached
stream's schema, a `WARNING`-level log message is emitted identifying which columns
appeared or disappeared. No enforcement is performed.

### `identity_structure()`

Returns `(class_name, tag_columns, source_id)` — schema-independent, since schema is
not known at construction time.

### `to_config()` / `from_config()`

`to_config()` returns a non-reconstructable descriptor (`source_type: "polling_source"`).
`from_config()` raises `NotImplementedError` — same pattern as `DataFrameSource`.

---

## Async Mode — `async_iter_data()`

`PollingSource` overrides `StreamBase.async_iter_data()` with the full polling loop.

### Pre-seeding from Cache

Before entering the polling loop, yield any rows already in `_schema_stream`:

```python
if self._schema_stream is not None:
    for item in self._schema_stream.iter_data():
        yield item
```

This ensures downstream operators receive data immediately on pipeline start (e.g. if
`iter_data()` was called in sync mode first, or the source was used across multiple
pipeline runs).

### Polling Loop

```
log.info("PollingSource %r starting (interval=%.1fs, duration=%.1fs)",
         source_id, config.interval, config.duration)

start_time  = loop.time()
next_tick   = start_time       # first tick fires immediately
consecutive_misses  = 0
consecutive_errors  = 0

try:
    while True:
        # 1. Sleep until next scheduled tick
        now = loop.time()
        if next_tick > now:
            await asyncio.sleep(next_tick - now)

        # 2. Poll + fetch
        try:
            has_new = await impl.poll(cursor=self._cursor)

            if has_new:
                log.debug("PollingSource %r: new data detected, fetching", source_id)
                new_cursor, df = await impl.fetch(cursor=self._cursor)
                _schema_stream = _combine(_schema_stream, df)   # update cache
                _cursor = new_cursor
                _update_last_modified(new_cursor)               # update last_modified
                warn_on_schema_drift(df)
                rows = list(convert(df))
                log.debug("PollingSource %r: emitting %d row(s)", source_id, len(rows))
                for item in rows:
                    yield item
            else:
                log.debug("PollingSource %r: poll returned no new data", source_id)

            consecutive_errors = 0

        except CancelledError:
            raise

        except CursorInvalidatedError:
            log.error("PollingSource %r: cursor invalidated — previous state cannot "
                      "be reconciled with already-emitted rows. Terminating source.",
                      source_id)
            return   # → finally block

        except Exception as e:
            consecutive_errors += 1
            backoff = config.error_backoff_base * 2 ** (consecutive_errors - 1)
            log.error("PollingSource %r: poll/fetch error (consecutive=%d, "
                      "backoff=%.1fs): %s", source_id, consecutive_errors, backoff, e)
            if consecutive_errors >= config.max_consecutive_errors:
                log.error("PollingSource %r: max consecutive errors (%d) reached. "
                          "Terminating source.", source_id, config.max_consecutive_errors)
                return   # → finally block
            await asyncio.sleep(backoff)
            continue   # retry — do not advance next_tick

        # 3. Tick advancement (start-to-start)
        now = loop.time()
        intervals_consumed = floor((now - next_tick) / config.interval)
        if intervals_consumed > 0:
            consecutive_misses += intervals_consumed
            log.warning("PollingSource %r: tick overrun — consumed %d interval(s) "
                        "(consecutive_misses=%d/%d)",
                        source_id, intervals_consumed,
                        consecutive_misses, config.max_missed_intervals)
            if consecutive_misses >= config.max_missed_intervals:
                log.error("PollingSource %r: overrun threshold exceeded. "
                          "Terminating source.", source_id)
                return   # → finally block
        else:
            consecutive_misses = 0
        next_tick += (intervals_consumed + 1) * config.interval

        # 4. Duration check
        if config.duration > 0 and (loop.time() - start_time) >= config.duration:
            log.info("PollingSource %r: duration limit (%.1fs) reached. "
                     "Terminating source.", source_id, config.duration)
            return   # → finally block

except CancelledError:
    log.info("PollingSource %r: cancelled — shutting down cleanly.", source_id)

finally:
    log.debug("PollingSource %r: calling impl.close()", source_id)
    await impl.close()
    log.info("PollingSource %r: closed.", source_id)
```

### `_update_last_modified(cursor)`

```python
if cursor.modified_at is not None:
    self._set_modified_time(cursor.modified_at)   # source-reported time (preferred)
else:
    self._update_modified_time()                  # fall back to wall clock
```

---

## Changes to Existing Classes

### `StreamBase.async_iter_data()`

Replace the current `raise NotImplementedError; yield` stub with a real default:

```python
async def async_iter_data(
    self,
) -> AsyncIterator[tuple[TagProtocol, DataProtocol]]:
    """Async iterator over (tag, data) pairs.

    Default implementation wraps iter_data() as an async generator.
    Subclasses override this to provide true async streaming behaviour
    (e.g. PollingSource).
    """
    for item in self.iter_data():
        yield item
```

### `SourceNode.async_execute`

Simplify to always use `async_iter_data()`:

```python
async def async_execute(
    self,
    output: WritableChannel[tuple[TagProtocol, DataProtocol]],
    *,
    observer: ExecutionObserverProtocol | None = None,
) -> None:
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

The sync fallback loop and the `raise NotImplementedError` handling are removed. All
sources — static and dynamic — go through `async_iter_data()`. Static sources get the
default wrapper at no behavioural cost.

---

## Public API Surface

`PollingSource`, `DynamicSourceProtocol`, `Cursor`, `CursorInvalidatedError`, and
`PollingConfig` are all exported from `src/orcapod/core/sources/__init__.py` and
re-exported from the top-level `src/orcapod/__init__.py`.

---

## Testing Plan

Tests live in `tests/test_channels/test_polling_source.py`. All async tests use
`@pytest.mark.asyncio`. A `FakeDynamicSource` fixture is written by hand (no mocking
framework) for maximum clarity.

| Test | What it verifies |
|---|---|
| `test_async_streaming` | Positive polls (`True`) trigger fetch; rows emitted; channel stays open across multiple ticks |
| `test_sync_snapshot` | `iter_data()` calls `fetch(cursor=None)` exactly once on first call; cursor set from returned tuple |
| `test_sync_cache_hit` | Second `iter_data()` call polls first; returns cached rows when poll returns `False` |
| `test_sync_cache_miss` | Second `iter_data()` call re-fetches and combines when poll returns `True`; cursor advances from fetch tuple |
| `test_cursor_threading` | `fetch()` receives the cursor returned by the previous fetch; first call gets None |
| `test_last_modified_from_cursor` | `cursor.modified_at` from fetch return updates `last_modified`; None falls back to wall clock |
| `test_negative_poll` | `poll()` returning `False` emits nothing and does not advance cursor |
| `test_pre_seeding` | `async_iter_data()` yields cached rows before entering polling loop |
| `test_cache_combining` | After two delta fetches, cache contains all rows from both batches |
| `test_tick_skip_overrun` | A slow tick consuming 2 intervals increments `consecutive_misses` by 2 |
| `test_overrun_error` | Exceeding `max_missed_intervals` closes channel without crashing pipeline |
| `test_error_backoff` | Exception in poll()/fetch() increments error counter; uses exponential backoff |
| `test_max_errors_closes_channel` | Reaching `max_consecutive_errors` closes channel cleanly |
| `test_clean_shutdown` | `CancelledError` causes `close()` to be awaited and channel to close |
| `test_duration_limit` | Source stops after `config.duration` seconds |
| `test_indefinite_mode` | `duration=0` runs until cancelled |
| `test_schema_drift_warning` | Mismatched columns on second fetch emit a WARNING log |
| `test_cursor_invalidated_error` | `CursorInvalidatedError` from poll() terminates source cleanly; does not retry |
| `test_async_iter_data_default` | Plain `RootSource` subclass with only `iter_data()` works via `SourceNode.async_execute` — no regression |

---

## Dependencies & Risks

- **`asyncio.get_event_loop().run_until_complete()`** is used in sync mode to call async
  `poll()`/`fetch()`. This fails if called from within a running event loop (e.g. inside
  an async test or a Jupyter notebook). Document this limitation; a future issue can add
  a `nest_asyncio` or `asyncio.run()` compatibility shim if needed.
- **Cache growth:** The accumulated cache is unbounded for true-delta implementations.
  No eviction is implemented in this version. Document explicitly.
- **Schema drift:** Silent schema changes across `fetch()` calls corrupt downstream
  operators. The `WARNING` log and drift warning are the only safeguards in this version;
  enforcement is deferred.
- **`SourceNode.async_execute` change** removes the sync fallback. Any `RootSource`
  subclass that previously relied on the absence of `async_iter_data()` will now use the
  default wrapper, which is behaviourally equivalent. The regression test above confirms
  this.
- **Backpressure:** Provided naturally by the channel. `await output.send()` suspends
  when the downstream channel buffer is full, which back-propagates through
  `async_iter_data()` at the `yield item` point. No additional mechanism required.
