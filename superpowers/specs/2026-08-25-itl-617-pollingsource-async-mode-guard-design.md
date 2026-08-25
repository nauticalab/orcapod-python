# ITL-617: PollingSource async-mode guard — design spec

**Date:** 2026-08-25
**Issue:** [ITL-617](https://linear.app/metamorphic/issue/ITL-617)
**Priority:** Urgent — silent data loss in a pipeline

---

## Overview

`PollingSource` has shared mutable state (`_cursor`, `_accumulated_stream`) that is mutated
by both the async polling loop (`async_iter_data`) and the sync helper `_get_latest_stream`.
When the async loop is running, any sync call that reaches `_get_latest_stream` can trigger
`_run_sync(poll/fetch)`, advance `_cursor`, and fold new rows into the cache without emitting
them. Those rows are then permanently skipped by the async loop.

The specific trigger that surfaced this bug was `FunctionJobNode.async_execute()` calling
`output_schema(columns={"system_tags": True})` concurrently with the source node's
`async_execute()`. `ITL-615` (PR #255) fixed `output_schema()` and `keys()` individually
by adding a cached-stream bypass before the `_get_latest_stream()` fallback. `ITL-617`
fixes the root — `_get_latest_stream()` itself — and extends the same protection to
`iter_data()` and `as_table()`, both of which route unconditionally through it today.

---

## Goals & Success Criteria

- `_get_latest_stream()` never calls `_run_sync(poll/fetch)` while the async polling loop
  is active.
- `iter_data()` and `as_table()` return from the cached stream when the async loop owns the
  cursor, rather than routing through `_get_latest_stream()` and risking a poll/fetch race.
- Sync-mode refresh behaviour is fully preserved: a second `iter_data()` call in pure sync
  usage still polls and fetches new data if `poll()` returns True.
- A regression test verifies the concurrent scenario end-to-end: calling `output_schema()`,
  `keys()`, `iter_data()`, and `as_table()` during an async run loses no rows.
- All existing tests continue to pass.

---

## Scope & Boundaries

In scope:
- `_async_loop_active` flag on `PollingSource` — set/cleared by `async_iter_data`.
- Guard in `_get_latest_stream()` — skip poll branch when flag is set.
- Top-level guards in `iter_data()` and `as_table()` — return from cache when async-active
  and cache exists (mirrors what `output_schema()` and `keys()` already do).
- New test class in `test_polling_source.py`.
- `DESIGN_ISSUES.md` PS4 entry.

Out of scope:
- PS2 (concurrent iteration race, ITL-625) — not addressed here.
- Sync-mode eviction or cache-size limits.
- Any changes to the `DynamicSourceProtocol`.

---

## Design

### Flag: `_async_loop_active`

A plain `bool` instance attribute, initialised to `False` in `__init__`:

```python
self._async_loop_active: bool = False
```

Python's GIL guarantees visibility across the `ThreadPoolExecutor` thread used by
`_run_sync`, so no explicit lock is needed for this single-field read.

### `async_iter_data()` — set and clear the flag

```python
async def async_iter_data(self):
    self._async_loop_active = True
    # Pre-seed from cache (existing logic, unchanged)
    if self._accumulated_stream is not None:
        ...
    try:
        while True:
            ...  # existing loop body, unchanged
    except asyncio.CancelledError:
        ...
    finally:
        self._async_loop_active = False          # ← NEW: clear before close
        logger.debug("PollingSource %r: calling impl.close()", self._source_id)
        await self._impl.close()
        logger.info("PollingSource %r: closed.", self._source_id)
```

The flag is set *before* the pre-seed so any concurrent sync access during the pre-seed
phase is also safe. It is cleared in `finally` so it is always reset, even on
`CancelledError`, `CursorInvalidatedError`, or any other exception.

### `_get_latest_stream()` — skip poll when async-active

Replace the bare `else:` branch with a conditional:

```python
else:
    if self._async_loop_active:
        # Async loop owns the cursor — return cache without polling so
        # _run_sync cannot advance _cursor and lose rows.
        logger.debug(
            "PollingSource %r: async loop active — returning cache without polling",
            self._source_id,
        )
    else:
        # Sync mode with cache — poll for updates (existing logic unchanged).
        has_new = _run_sync(self._impl.poll, cursor=self._cursor)
        if has_new:
            ...  # existing fetch + combine logic, unchanged
```

### `iter_data()` — top-level guard

```python
def iter_data(self):
    """Iterate over (tag, data) pairs from the current snapshot."""
    if self._async_loop_active and self._accumulated_stream is not None:
        return self._accumulated_stream.iter_data()
    return self._get_latest_stream().iter_data()
```

### `as_table()` — top-level guard

```python
def as_table(self, *, columns=None, all_info=False):
    """Return the accumulated rows as a PyArrow table."""
    if self._async_loop_active and self._accumulated_stream is not None:
        return self._accumulated_stream.as_table(columns=columns, all_info=all_info)
    return self._get_latest_stream().as_table(columns=columns, all_info=all_info)
```

Both guards mirror the pattern already present in `output_schema()` and `keys()`, but gate
on `_async_loop_active` (not just cache presence) so that sync callers still get the
poll-for-refresh path via `_get_latest_stream()`.

---

## Behaviour matrix

| Caller | `_async_loop_active` | `_accumulated_stream` | Result |
|---|---|---|---|
| `iter_data()` (sync, 1st call) | False | None | → `_get_latest_stream()` → first fetch |
| `iter_data()` (sync, 2nd call, poll=True) | False | set | → `_get_latest_stream()` → poll+fetch |
| `iter_data()` (sync, 2nd call, poll=False) | False | set | → `_get_latest_stream()` → returns cache |
| `iter_data()` (called during async run) | **True** | set | → cache directly (**guard**) |
| `as_table()` (called during async run) | **True** | set | → cache directly (**guard**) |
| `_get_latest_stream()` (async-active, cache set) | **True** | set | skip poll, return cache (**guard**) |
| `_get_latest_stream()` (async-active, cache None) | **True** | None | first fetch (no cursor yet; race is PS2) |

---

## Test

One new test class: `TestPollingSourceSyncAccessDuringAsyncRun`.

### `test_output_schema_and_keys_mid_async_run_lose_no_rows`

- Impl: `FakeDynamicSource(batches=[batch1, batch2, batch3], schema_override=None)` — no
  declared schema, so every call to `output_schema()`/`keys()` with `_accumulated_stream`
  populated would previously fall through to `_get_latest_stream()` and consume a row.
- Run `async_iter_data()` as the main coroutine. Use a background task that loops calling
  `src.output_schema()` and `src.keys()` after `_accumulated_stream` is populated.
- Assert all 3 rows are delivered to the async iterator.
- Assert `fake.fetch_cursors` is exactly `[None, Cursor(1), Cursor(2)]` (only the async loop
  fetched; the introspection task fetched nothing).

### `test_iter_data_and_as_table_mid_async_run_lose_no_rows`

- Same 3-batch setup, `schema_override=None`.
- Background task calls `src.iter_data()` and `src.as_table()` after first fetch.
- Assert all 3 rows delivered; `fetch_cursors` shows only 3 fetches by the async loop.

---

## `DESIGN_ISSUES.md` update

New entry **PS4** in the `polling_source.py` section:

```
### PS4 — `_get_latest_stream()`, `iter_data()`, and `as_table()` poll during async run
**Status:** resolved
**Severity:** critical
**Issue:** ITL-617
```

Describes the root cause (shared cursor mutated by sync path while async loop is active)
and the fix (`_async_loop_active` flag + guards at all four call sites).

---

## Implementation checklist

- [ ] `PollingSource.__init__`: add `self._async_loop_active: bool = False`
- [ ] `async_iter_data()`: set flag True at entry; add `self._async_loop_active = False` in `finally` before `impl.close()`
- [ ] `_get_latest_stream()`: add `if self._async_loop_active:` guard in `else` branch
- [ ] `iter_data()`: add top-level guard
- [ ] `as_table()`: add top-level guard
- [ ] `test_polling_source.py`: add `TestPollingSourceSyncAccessDuringAsyncRun` with two tests
- [ ] `DESIGN_ISSUES.md`: add PS4 entry
