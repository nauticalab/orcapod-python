## `src/orcapod/core/sources/polling_source.py`

### PS4 — Concurrent sync access during async run silently loses rows
**Status:** resolved
**Severity:** critical
**Issue:** ITL-617

`_get_latest_stream()`, called by `iter_data()` or `as_table()` while
`async_iter_data()` is running, could advance `_cursor` and fold new rows into
`_accumulated_stream` without the async loop emitting them — permanent silent data
loss. `ITL-615` (PR #255) partially addressed this for `output_schema()` and `keys()`
by caching the stream reference. `ITL-617` fixes the root.

**Fix:** Replaced single `_accumulated_stream: ArrowTableStream | None` with
append-only `_batches: list[ArrowTableStream]` and `_state_lock: threading.Lock`.
All callers (sync and async) use an optimistic lock protocol: snapshot cursor (brief
lock) → perform I/O freely with no lock held → commit only if cursor is unchanged
(brief lock). The async loop tracks its yield position with a per-iterator
``local_batch_idx`` local variable and drains ``_batches`` at the top of each
iteration, ensuring rows committed by a concurrent sync caller are yielded even when
the async loop loses the commit race. The lock is never held across ``await``.
