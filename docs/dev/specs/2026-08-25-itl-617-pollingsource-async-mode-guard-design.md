# ITL-617: PollingSource concurrent cursor safety — design spec

**Date:** 2026-08-25
**Issue:** [ITL-617](https://linear.app/metamorphic/issue/ITL-617)
**Priority:** Urgent — silent data loss in a pipeline

---

## Overview

`PollingSource` has shared mutable state (`_cursor`, `_accumulated_stream`) that can be
mutated by both the async polling loop (`async_iter_data`) and any sync caller that reaches
`_get_latest_stream()` (via `iter_data()` and `as_table()`). When both run concurrently,
the sync caller can advance `_cursor` and fold new rows into the cache without the async
loop emitting them — permanent silent data loss.

`ITL-615` (PR #255) patched `output_schema()` and `keys()` individually by adding a
cached-stream bypass before the `_get_latest_stream()` fallback. ITL-617 fixes the root:
replaces the single-stream cache with an **append-only batch list** and uses **optimistic
locking** on the cursor to make all state mutations race-free, without holding a lock
across `await`.

---

## Goals & Success Criteria

- No rows are silently dropped when a sync caller (`iter_data`, `as_table`) runs
  concurrently with the async polling loop.
- The cursor is never advanced without the corresponding batch being committed to
  `_batches` — no TOCTOU window between check and update.
- No `threading.Lock` is ever held across an `await` statement.
- Sync-mode refresh behaviour is fully preserved: a second `iter_data()` call in pure
  sync usage still polls and fetches new data when `poll()` returns True.
- All existing tests continue to pass.
- A regression test verifies the concurrent scenario end-to-end.

---

## Scope & Boundaries

In scope:
- Replace `_accumulated_stream: ArrowTableStream | None` with `_batches: list[ArrowTableStream]`
  (append-only) and `_state_lock: threading.Lock`
- New `_sync_poll_and_commit()` method implementing the optimistic lock protocol for sync callers
- New `_get_combined_stream()` helper for sync read access
- Per-iterator `local_batch_idx` in `async_iter_data()` with a drain step at the top of each iteration
- `output_schema()` / `keys()`: update cached-stream check from `_accumulated_stream` to `_batches`
- New test class in `test_polling_source.py`
- `DESIGN_ISSUES.md` PS4 entry

Out of scope:
- PS2 (concurrent iteration race, ITL-625) — not addressed here
- Sync-mode eviction or cache-size limits
- Performance optimisation of `_get_combined_stream()` (O(n batches) combine on each sync call)
- Any changes to `DynamicSourceProtocol`

---

## Fetch monotonicity guarantee

`impl.fetch(cursor=X)` provides a **monotonic lower bound**:

> If element A appeared in `fetch(cursor=X)` at time T1, then at any T2 > T1, A is
> guaranteed to appear in `fetch(cursor=X)` — and potentially more elements may also
> be present.

The returned cursor accurately reflects **exactly** what was in the returned batch (not a
superset or subset). Calling `fetch(cursor=X)` at different times may return different-sized
batches (one a strict superset of another), but each batch's cursor correctly identifies
its own boundary.

This guarantee is **sufficient** for optimistic locking to be safe. Consider the worst case:
two concurrent callers both call `fetch(cursor=X)` — Caller A gets `[r1, r2]` with
`new_cursor=Y_A`, Caller B gets `[r1, r2, r3]` with `new_cursor=Y_B`. Caller A wins the
commit race:

- `_batches` gets `[r1, r2]`, `_cursor` advances to `Y_A`
- Caller B discards its result

`r3` is **not** permanently lost: `Y_A` was produced by a fetch that did not include `r3`,
so `Y_A` sits before `r3`'s boundary. The next `fetch(cursor=Y_A)` will include `r3`.
At most one extra poll cycle is needed.

---

## Design

### State changes

Replace:

```python
self._accumulated_stream: ArrowTableStream | None = None
```

With:

```python
import threading

self._batches: list[ArrowTableStream] = []
self._state_lock: threading.Lock = threading.Lock()
```

`_batches` is **append-only**: entries are never removed or modified in-place. Python's GIL
guarantees that `list.append()` is atomic and existing elements are never invalidated, so
readers can safely snapshot `len(self._batches)` or access `self._batches[i]` for
`i < snapshot_len` without holding the lock.

`_state_lock` is held only for two brief, I/O-free sections:

1. Reading `_cursor` (snapshot before I/O)
2. Checking `_cursor` and committing the batch (after I/O)

It is **never** held across `await`.

### Optimistic lock protocol

The same protocol applies to both sync and async callers:

```
1.  [lock]  cursor_snapshot = self._cursor  [release]
2.  If cursor_snapshot is None:
        skip poll — first fetch, go straight to step 3
    Else:
        has_new = poll(cursor=cursor_snapshot)   ← no lock held
        if not has_new: return                   ← nothing to commit
3.  new_cursor, data = fetch(cursor=cursor_snapshot)  ← no lock held
4.  new_stream = _try_build_stream(data)
    Validate new_stream schema (declared + batch consistency)  ← no lock held
5.  [lock]
        if self._cursor == cursor_snapshot:   ← commit only if cursor unchanged
            if new_stream is not None:
                self._batches.append(new_stream)
            self._cursor = new_cursor
            committed = True
        else:
            committed = False   ← lost race; extra rows captured next poll cycle
    [release]
6.  if committed: _update_last_modified_from_cursor(new_cursor)  ← outside lock
```

The cursor check in step 5 prevents TOCTOU: reading and updating `_cursor` happen in the
same lock section with no I/O between them.

### `async_iter_data()` — drain step + optimistic lock

```python
async def async_iter_data(self):
    # local_batch_idx tracks the next _batches index this iterator must yield.
    # Initialised to len(_batches) so the drain loop below covers pre-existing rows.
    local_batch_idx = 0

    cfg = self._polling_config
    loop = asyncio.get_running_loop()
    start_time = loop.time()
    next_tick = start_time
    consecutive_misses = 0
    consecutive_errors = 0

    logger.info(
        "PollingSource %r starting (interval=%.2fs, duration=%.1fs)",
        self._source_id, cfg.interval, cfg.duration,
    )

    try:
        while True:
            # ── 1. Drain: yield any batches not yet emitted by this iterator ──
            # Covers both pre-existing rows (first iteration) and rows committed
            # by concurrent sync callers while this iterator was sleeping/fetching.
            while local_batch_idx < len(self._batches):
                for item in self._batches[local_batch_idx].iter_data():
                    yield item
                local_batch_idx += 1

            # ── 2. Sleep to next scheduled tick ──
            now = loop.time()
            if next_tick > now:
                await asyncio.sleep(next_tick - now)

            # ── 3. Optimistic lock: snapshot cursor ──
            try:
                with self._state_lock:
                    cursor_snapshot = self._cursor

                # ── 4. Poll (native await, no lock held) ──
                has_new = await self._impl.poll(cursor=cursor_snapshot)

                if has_new:
                    logger.debug(
                        "PollingSource %r: new data detected, fetching", self._source_id
                    )
                    # ── 5. Fetch (native await, no lock held) ──
                    new_cursor, data = await self._impl.fetch(cursor=cursor_snapshot)
                    new_stream = self._try_build_stream(data)

                    if new_stream is not None:
                        if self._tag_schema is not None or self._data_schema is not None:
                            self._validate_against_declared_schemas(new_stream)
                        if self._batches:
                            self._validate_combining_schemas(self._batches[0], new_stream)

                    # ── 6. Commit (brief lock) ──
                    committed = False
                    with self._state_lock:
                        if self._cursor == cursor_snapshot:
                            if new_stream is not None:
                                self._batches.append(new_stream)
                            self._cursor = new_cursor
                            committed = True

                    if committed:
                        self._update_last_modified_from_cursor(new_cursor)
                    # If not committed: sync caller already advanced cursor.
                    # That caller's batch is in _batches; the drain step above
                    # will yield it at the top of the next iteration.

                else:
                    logger.debug(
                        "PollingSource %r: poll returned no new data", self._source_id
                    )

                consecutive_errors = 0

            except asyncio.CancelledError:
                raise
            except CursorInvalidatedError:
                logger.error(
                    "PollingSource %r: cursor invalidated — terminating.", self._source_id
                )
                raise
            except InputValidationError:
                raise
            except Exception as e:
                consecutive_errors += 1
                backoff = cfg.error_backoff_base * 2 ** (consecutive_errors - 1)
                logger.error(
                    "PollingSource %r: poll/fetch error (consecutive=%d, backoff=%.1fs): %s",
                    self._source_id, consecutive_errors, backoff, e,
                )
                if consecutive_errors >= cfg.max_consecutive_errors:
                    logger.error(
                        "PollingSource %r: max consecutive errors (%d) reached.",
                        self._source_id, cfg.max_consecutive_errors,
                    )
                    return
                await asyncio.sleep(backoff)
                continue

            # ── 7. Tick advancement and duration check (unchanged) ──
            now = loop.time()
            intervals_consumed = floor((now - next_tick) / cfg.interval)
            if intervals_consumed > 0:
                consecutive_misses += intervals_consumed
                if consecutive_misses >= cfg.max_missed_intervals:
                    logger.error("PollingSource %r: overrun threshold exceeded.", self._source_id)
                    return
            else:
                consecutive_misses = 0
            next_tick += (intervals_consumed + 1) * cfg.interval

            if cfg.duration > 0 and (loop.time() - start_time) >= cfg.duration:
                logger.info("PollingSource %r: duration limit reached.", self._source_id)
                return

    except asyncio.CancelledError:
        logger.info("PollingSource %r: cancelled — shutting down cleanly.", self._source_id)
    finally:
        logger.debug("PollingSource %r: calling impl.close()", self._source_id)
        await self._impl.close()
        logger.info("PollingSource %r: closed.", self._source_id)
```

Key properties:
- `local_batch_idx` is a **local variable** — zero shared state for per-iterator position.
- The **drain step** (step 1) runs at the top of every loop iteration, **before** sleeping.
  This ensures that batches committed by concurrent sync callers (or by an earlier iteration)
  are yielded before the next sleep, regardless of who won the commit race.
- If the async loop loses the commit race (sync caller already advanced cursor), `committed=False`
  and the loop continues. The sync-caller's batch is already in `_batches`; the drain step
  at the top of the **next** iteration yields it. No data loss.
- `await impl.poll()` and `await impl.fetch()` are called with **no lock held** — the event
  loop is never blocked on a `threading.Lock`.
- The pre-seed block from the original implementation is replaced entirely by the drain step:
  `local_batch_idx=0` at entry means the first drain naturally covers any pre-existing rows.

### `_sync_poll_and_commit()` — sync optimistic lock helper

Replaces the mutation logic in `_get_latest_stream()`. Performs one poll+fetch cycle using
the optimistic lock protocol:

```python
def _sync_poll_and_commit(self) -> None:
    """Poll for new data and commit to _batches if the cursor is unchanged.

    Implements the optimistic lock protocol for sync callers: snapshot cursor
    without holding the lock during I/O, then commit only if cursor is
    unchanged. Safe to call concurrently with ``async_iter_data``.
    """
    with self._state_lock:
        cursor_snapshot = self._cursor

    if cursor_snapshot is None:
        # First access — fetch unconditionally (no poll needed)
        logger.debug("PollingSource %r: first sync access — fetching", self._source_id)
        new_cursor, data = _run_sync(self._impl.fetch, cursor=None)
        new_stream = self._try_build_stream(data)
        if new_stream is not None:
            if self._tag_schema is not None or self._data_schema is not None:
                self._validate_against_declared_schemas(new_stream)
        with self._state_lock:
            if self._cursor is None:
                if new_stream is not None:
                    self._batches.append(new_stream)
                self._cursor = new_cursor
                committed = True
            else:
                committed = False
        if committed:
            self._update_last_modified_from_cursor(new_cursor)
    else:
        has_new = _run_sync(self._impl.poll, cursor=cursor_snapshot)
        if has_new:
            logger.debug(
                "PollingSource %r: sync poll found new data — fetching", self._source_id
            )
            new_cursor, data = _run_sync(self._impl.fetch, cursor=cursor_snapshot)
            new_stream = self._try_build_stream(data)
            if new_stream is not None:
                if self._tag_schema is not None or self._data_schema is not None:
                    self._validate_against_declared_schemas(new_stream)
                if self._batches:
                    self._validate_combining_schemas(self._batches[0], new_stream)
            with self._state_lock:
                if self._cursor == cursor_snapshot:
                    if new_stream is not None:
                        self._batches.append(new_stream)
                    self._cursor = new_cursor
                    committed = True
                else:
                    committed = False
            if committed:
                self._update_last_modified_from_cursor(new_cursor)
        else:
            logger.debug(
                "PollingSource %r: sync poll — cache still valid", self._source_id
            )
```

### `_get_combined_stream()` — sync read helper

Builds a single `ArrowTableStream` from all committed batches for sync read methods:

```python
def _get_combined_stream(self) -> ArrowTableStream:
    """Return all committed batches concatenated as a single stream.

    Raises:
        ValueError: If no data has been fetched yet (``_batches`` is empty).
    """
    batches = list(self._batches)  # snapshot — no lock needed (append-only)
    if not batches:
        raise ValueError(
            "PollingSource: no data available yet — first fetch returned empty data."
        )
    result = batches[0]
    for batch in batches[1:]:
        result = self._combine(result, batch)
    return result
```

### `iter_data()` and `as_table()` — updated entry points

```python
def iter_data(self):
    """Iterate over (tag, data) pairs from the current snapshot."""
    self._sync_poll_and_commit()
    return self._get_combined_stream().iter_data()

def as_table(self, *, columns=None, all_info=False):
    """Return the accumulated rows as a PyArrow table."""
    self._sync_poll_and_commit()
    return self._get_combined_stream().as_table(columns=columns, all_info=all_info)
```

### `output_schema()` and `keys()` — minor update

The existing cached-stream bypass changes from checking `_accumulated_stream` to `_batches`.
`_batches[0]` is used (not `_get_combined_stream()`) because all batches share the same
user-facing schema (enforced by `_validate_combining_schemas` on each commit), so any
single batch gives the correct schema and avoids triggering a combine:

```python
# was:
if self._accumulated_stream is not None:
    return self._accumulated_stream.output_schema(columns=columns, all_info=all_info)
# becomes:
if self._batches:
    return self._batches[0].output_schema(columns=columns, all_info=all_info)
```

Apply the same substitution in `keys()`.

---

## Behaviour matrix

| Caller | `_batches` | `_cursor` | Result |
|---|---|---|---|
| `iter_data()` (sync, 1st call) | `[]` | `None` | `_sync_poll_and_commit()` → first fetch → commit → `_get_combined_stream()` |
| `iter_data()` (sync, 2nd call, poll=True) | `[b1]` | `Y` | `_sync_poll_and_commit()` → poll+fetch → commit → combined `[b1, b2]` |
| `iter_data()` (sync, 2nd call, poll=False) | `[b1]` | `Y` | `_sync_poll_and_commit()` → poll returns False → combined `[b1]` |
| `iter_data()` concurrent with async loop | `[b1]` | `Y` | optimistic commit wins or loses; either way combined view is consistent |
| `output_schema()` / `keys()` (cache hit) | `[b1, ...]` | any | → `_batches[0]` directly; no poll triggered |
| async drain step | `[b1, b2, ...]` | — | yields `_batches[local_batch_idx:]` regardless of who committed them |
| async commit (won race) | `[b1]` | `Y_old` | appends `b2`, cursor → `Y_new` |
| async commit (lost race to sync) | `[b1, b2]` | `Y_new` (by sync) | discards fetch; drain step yields `b2` at top of next iteration |

---

## Test

One new test class: `TestPollingSourceSyncAccessDuringAsyncRun`.

### `test_iter_data_and_as_table_concurrent_with_async_run_lose_no_rows`

- Setup: `FakeDynamicSource(batches=[batch1, batch2, batch3], schema_override=None)` — no
  declared schema, so `output_schema()` / `keys()` will fall through to `_batches[0]` once
  the first batch is committed.
- Run `async_iter_data()` as the main coroutine. A background task waits until
  `len(src._batches) >= 1`, then calls `src.iter_data()` and `src.as_table()` in a loop
  for the duration of the async run.
- Assert all 3 rows are delivered to the async iterator (no silent loss).
- Assert `fake.fetch_cursors` contains at most one extra entry beyond `[None, C1, C2]`
  (the sync caller may race on one batch but may not consume a batch that the async loop
  then misses).

### `test_output_schema_and_keys_concurrent_with_async_run_lose_no_rows`

- Same 3-batch setup.
- Background task calls `src.output_schema()` and `src.keys()` (with and without
  `system_tags=True`) after first batch is committed.
- Assert all 3 rows delivered; `fake.fetch_cursors` is exactly
  `[None, Cursor(1), Cursor(2)]` — the schema/keys calls never trigger a fetch.

---

## `DESIGN_ISSUES.md` update

New entry **PS4** in the `polling_source.py` section:

```
### PS4 — Concurrent sync access during async run silently loses rows
**Status:** resolved
**Severity:** critical
**Issue:** ITL-617

`_get_latest_stream()`, called by `iter_data()` or `as_table()` while
`async_iter_data()` is running, could advance `_cursor` and fold new rows into
`_accumulated_stream` without the async loop emitting them — permanent silent data
loss.

**Fix (ITL-617):** Replaced single `_accumulated_stream` with append-only
`_batches: list[ArrowTableStream]` and `_state_lock: threading.Lock`. All callers
(sync and async) use an optimistic lock protocol: snapshot cursor (brief lock) → do
I/O freely with no lock held → commit only if cursor is unchanged (brief lock). The
async loop tracks its yield position with a per-iterator `local_batch_idx` and drains
any concurrently-committed batches at the top of each iteration, so no rows are lost
regardless of which caller wins the commit race.
```

---

## Implementation checklist

- [ ] `polling_source.py`: add `import threading` at top of file
- [ ] `PollingSource.__init__`: replace `self._accumulated_stream = None` with
      `self._batches: list[ArrowTableStream] = []` and
      `self._state_lock: threading.Lock = threading.Lock()`
- [ ] `output_schema()`: change `_accumulated_stream is not None` / `_accumulated_stream.output_schema()`
      to `self._batches` / `self._batches[0].output_schema()`
- [ ] `keys()`: same substitution as `output_schema()`
- [ ] Add `_sync_poll_and_commit()` method
- [ ] Add `_get_combined_stream()` method
- [ ] Remove `_get_latest_stream()` (replaced by `_sync_poll_and_commit` + `_get_combined_stream`)
- [ ] `iter_data()`: call `_sync_poll_and_commit()` then return `_get_combined_stream().iter_data()`
- [ ] `as_table()`: call `_sync_poll_and_commit()` then return `_get_combined_stream().as_table(...)`
- [ ] `async_iter_data()`: replace pre-seed block and poll/fetch/commit block with optimistic
      lock pattern; add `local_batch_idx`; add drain step at top of loop
- [ ] `_combine()` and `_validate_combining_schemas()`: unchanged
- [ ] `test_polling_source.py`: add `TestPollingSourceSyncAccessDuringAsyncRun` with two tests
- [ ] `DESIGN_ISSUES.md`: add PS4 entry
