# ITL-617: PollingSource concurrent cursor safety — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace `PollingSource`'s single shared `_accumulated_stream` cache with an append-only `_batches` list and an optimistic lock on `_cursor`, so concurrent sync callers cannot silently steal rows from the async polling loop.

**Architecture:** All mutation of `_cursor` and `_batches` is protected by `_state_lock: threading.Lock` using the optimistic lock protocol: snapshot cursor (brief lock) → I/O with no lock held → commit only if cursor unchanged (brief lock). The async loop tracks its yield position with a per-iterator `local_batch_idx` local variable and drains `_batches` at the top of each iteration, so rows committed by a concurrent sync caller are still yielded even if the async loop lost the commit race.

**Tech Stack:** Python `threading.Lock`, `asyncio`, PyArrow, pytest-asyncio.

---

## Files

| File | What changes |
|------|-------------|
| `src/orcapod/core/sources/polling_source.py` | Replace `_accumulated_stream` with `_batches` + `_state_lock`; add `_sync_poll_and_commit()`, `_get_combined_stream()`; rewrite `async_iter_data()`; update `iter_data()`, `as_table()`, `output_schema()`, `keys()` |
| `tests/test_channels/test_polling_source.py` | Fix `test_cache_combining_accumulates_rows` (accesses `_accumulated_stream` directly); add `TestPollingSourceSyncAccessDuringAsyncRun` |
| `DESIGN_ISSUES.md` | Add PS4 entry |

---

## Task 1: Create feature branch

**Files:** — (git only)

- [ ] **Step 1: Checkout the feature branch**

```bash
cd /home/kurouto/kurouto-jobs/15fa2ef8-6c81-4b68-81f3-cc9492a3dc2b/orcapod-python
git checkout eywalker/itl-617-pollingsource-sync-introspection-during-an-async-run
```

Expected: branch switches without error (branch already exists from ITL-617 issue).
If the branch doesn't exist yet, create it from main:
```bash
git checkout main && git pull && git checkout -b eywalker/itl-617-pollingsource-sync-introspection-during-an-async-run
```

- [ ] **Step 2: Confirm baseline test suite passes**

```bash
uv run pytest tests/test_channels/test_polling_source.py -v
```

Expected: all tests **PASS**. Record the count — we must not regress any.

---

## Task 2: Write failing concurrent-access tests

Write the new test class first so we have a red light that turns green after the fix. These tests expose the current bug: a sync caller can steal a row that the async loop should emit.

**Files:**
- Modify: `tests/test_channels/test_polling_source.py` (add class at end of file)

- [ ] **Step 1: Append `TestPollingSourceSyncAccessDuringAsyncRun` to the test file**

Add the following class at the very end of `tests/test_channels/test_polling_source.py`:

```python
# ===========================================================================
# ITL-617: Concurrent sync access during async run must not lose rows
# ===========================================================================


class TestPollingSourceSyncAccessDuringAsyncRun:
    """Regression tests for ITL-617.

    A concurrent sync call (``iter_data``, ``as_table``, ``output_schema``,
    ``keys``) must not advance ``_cursor`` in a way that causes the async
    polling loop to skip rows.
    """

    @pytest.mark.asyncio
    async def test_iter_data_and_as_table_concurrent_with_async_run_lose_no_rows(self):
        """iter_data() and as_table() called concurrently with async_iter_data()
        must not cause any rows to be skipped by the async iterator."""
        fake = FakeDynamicSource(
            batches=[_batch(1, 10), _batch(2, 20), _batch(3, 30)],
            schema_override=None,
        )
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=0.02, duration=1.0, max_missed_intervals=100),
        )

        rows_from_async: list = []
        stop_bg = asyncio.Event()

        async def background_sync_calls():
            # Wait until at least one batch is committed, then hammer the
            # sync API from a thread pool (iter_data and as_table both call
            # _sync_poll_and_commit which can race with the async loop).
            while not src._batches:
                await asyncio.sleep(0.005)
            while not stop_bg.is_set():
                await asyncio.to_thread(lambda: list(src.iter_data()))
                await asyncio.to_thread(lambda: src.as_table())
                await asyncio.sleep(0.005)

        bg_task = asyncio.create_task(background_sync_calls())

        async for tag, data in src.async_iter_data():
            rows_from_async.append((tag, data))

        stop_bg.set()
        try:
            await asyncio.wait_for(bg_task, timeout=1.0)
        except asyncio.TimeoutError:
            bg_task.cancel()

        # The async iterator must deliver ALL three rows, regardless of
        # how many times the sync caller raced in.
        assert len(rows_from_async) == 3

    @pytest.mark.asyncio
    async def test_output_schema_and_keys_concurrent_with_async_run_lose_no_rows(self):
        """output_schema() and keys() called concurrently with async_iter_data()
        must not trigger a fetch that advances the cursor past the async loop."""
        fake = FakeDynamicSource(
            batches=[_batch(1, 10), _batch(2, 20), _batch(3, 30)],
            schema_override=None,  # no declared schema → would fall through to fetch
        )
        src = PollingSource(
            fake,
            tag_columns="id",
            polling_config=PollingConfig(interval=0.02, duration=1.0, max_missed_intervals=100),
        )

        rows_from_async: list = []
        stop_bg = asyncio.Event()

        async def background_introspection():
            # Wait until first batch is available (so _batches[0] exists)
            # then hammer output_schema / keys.
            while not src._batches:
                await asyncio.sleep(0.005)
            while not stop_bg.is_set():
                src.output_schema()
                src.keys()
                src.output_schema(columns={"system_tags": True})
                src.keys(columns={"system_tags": True})
                await asyncio.sleep(0.005)

        bg_task = asyncio.create_task(background_introspection())

        async for tag, data in src.async_iter_data():
            rows_from_async.append((tag, data))

        stop_bg.set()
        try:
            await asyncio.wait_for(bg_task, timeout=1.0)
        except asyncio.TimeoutError:
            bg_task.cancel()

        # All 3 rows must be delivered by the async iterator.
        assert len(rows_from_async) == 3
        # output_schema / keys must not have triggered any fetches —
        # they should use _batches[0] bypass once the first batch exists.
        # Exactly 3 fetches: one per batch, all by the async loop.
        assert len(fake.fetch_cursors) == 3
```

- [ ] **Step 2: Run the new tests to confirm they fail (exposing the bug)**

```bash
uv run pytest tests/test_channels/test_polling_source.py::TestPollingSourceSyncAccessDuringAsyncRun -v
```

Expected: both tests **FAIL**.
- `test_iter_data_and_as_table_concurrent_with_async_run_lose_no_rows`: `assert len(rows_from_async) == 3` fails (rows_from_async has 2 items — sync caller stole one batch).
- `test_output_schema_and_keys_concurrent_with_async_run_lose_no_rows`: may fail depending on timing, but `len(fake.fetch_cursors) == 3` may be > 3.

If both tests happen to pass (race didn't trigger) — that's also acceptable; the implementation still needs the fix for correctness.

---

## Task 3: Replace `_accumulated_stream` with `_batches` + `_state_lock` in `__init__`

**Files:**
- Modify: `src/orcapod/core/sources/polling_source.py`

- [ ] **Step 1: Add `import threading` to the module-level imports**

In `polling_source.py`, find the imports block (lines 9–26) and add `threading`:

```python
import asyncio
import dataclasses
import functools
import logging
import threading
from collections.abc import Collection
from math import floor
from typing import TYPE_CHECKING, Any, Generic, TypeVar
```

- [ ] **Step 2: Replace `_accumulated_stream` with `_batches` and `_state_lock` in `__init__`**

In `PollingSource.__init__`, find:
```python
        self._cursor: Cursor[T] | None = None
        self._accumulated_stream: ArrowTableStream | None = None
```

Replace with:
```python
        self._cursor: Cursor[T] | None = None
        self._batches: list[ArrowTableStream] = []
        self._state_lock: threading.Lock = threading.Lock()
```

- [ ] **Step 3: Update the cached-stream bypass in `output_schema()`**

In `output_schema()`, find (around line 383):
```python
        if self._accumulated_stream is not None:
            return self._accumulated_stream.output_schema(columns=columns, all_info=all_info)
        return self._get_latest_stream().output_schema(columns=columns, all_info=all_info)
```

Replace with:
```python
        if self._batches:
            return self._batches[0].output_schema(columns=columns, all_info=all_info)
        return self._get_latest_stream().output_schema(columns=columns, all_info=all_info)
```

- [ ] **Step 4: Update the cached-stream bypass in `keys()`**

In `keys()`, find (around line 417):
```python
        if self._accumulated_stream is not None:
            return self._accumulated_stream.keys(columns=columns, all_info=all_info)
        return self._get_latest_stream().keys(columns=columns, all_info=all_info)
```

Replace with:
```python
        if self._batches:
            return self._batches[0].keys(columns=columns, all_info=all_info)
        return self._get_latest_stream().keys(columns=columns, all_info=all_info)
```

- [ ] **Step 5: Fix the existing test that directly accesses `_accumulated_stream`**

In `tests/test_channels/test_polling_source.py`, find `test_cache_combining_accumulates_rows` (around line 583). It ends with:

```python
        assert src._accumulated_stream is not None
        cached_rows = list(src._accumulated_stream.iter_data())
        assert len(cached_rows) == 2
```

Replace those three lines with:

```python
        assert len(src._batches) == 2
        cached_rows = list(src._get_combined_stream().iter_data())
        assert len(cached_rows) == 2
```

(Note: `_get_combined_stream()` is added in Task 4 — for now this test will still fail. That's expected.)

- [ ] **Step 6: Run the sync test suite to check compile-time errors only**

```bash
uv run pytest tests/test_channels/test_polling_source.py -v --tb=short 2>&1 | head -60
```

Expected: tests fail at runtime (AttributeError on `_accumulated_stream` and `_get_combined_stream` not yet defined), but no import errors. This is expected — we'll fix in the next tasks.

---

## Task 4: Add `_sync_poll_and_commit()` and `_get_combined_stream()`

These are the two new internal helpers that replace `_get_latest_stream()` for the mutation and read paths respectively.

**Files:**
- Modify: `src/orcapod/core/sources/polling_source.py`

- [ ] **Step 1: Add `_sync_poll_and_commit()` method**

Insert this method in the "Internal sync helpers" section, immediately before `_get_latest_stream()`:

```python
    def _sync_poll_and_commit(self) -> None:
        """Poll for new data and commit a new batch under the optimistic lock.

        Implements the check-snapshot → I/O → check-and-commit protocol:
        reads the cursor snapshot under a brief lock, performs poll and fetch
        with no lock held, then commits only if the cursor has not changed.
        If another caller advanced the cursor in between (winning the commit
        race), this call discards its fetched data — the batch is already in
        ``_batches`` and will be visible on the next read.

        Safe to call concurrently with ``async_iter_data``.
        """
        with self._state_lock:
            cursor_snapshot = self._cursor

        if cursor_snapshot is None:
            # First access — no poll needed, fetch unconditionally.
            logger.debug("PollingSource %r: first sync access — fetching", self._source_id)
            new_cursor, data = _run_sync(self._impl.fetch, cursor=None)
            new_stream = self._try_build_stream(data)
            if new_stream is not None:
                if self._tag_schema is not None or self._data_schema is not None:
                    self._validate_against_declared_schemas(new_stream)
            committed = False
            with self._state_lock:
                if self._cursor is None:
                    if new_stream is not None:
                        self._batches.append(new_stream)
                    self._cursor = new_cursor
                    committed = True
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
                committed = False
                with self._state_lock:
                    if self._cursor == cursor_snapshot:
                        if new_stream is not None:
                            self._batches.append(new_stream)
                        self._cursor = new_cursor
                        committed = True
                if committed:
                    self._update_last_modified_from_cursor(new_cursor)
            else:
                logger.debug(
                    "PollingSource %r: sync poll — cache still valid", self._source_id
                )
```

- [ ] **Step 2: Add `_get_combined_stream()` method**

Insert this method immediately after `_sync_poll_and_commit()`:

```python
    def _get_combined_stream(self) -> ArrowTableStream:
        """Return all committed batches concatenated as a single stream.

        Takes a snapshot of ``_batches`` (no lock needed — list is append-only)
        then combines using the existing ``_combine`` helper.

        Returns:
            A single ``ArrowTableStream`` containing all rows from all batches.

        Raises:
            ValueError: If no data has been fetched yet (``_batches`` is empty).
        """
        batches = list(self._batches)  # snapshot — safe, list is append-only
        if not batches:
            raise ValueError(
                "PollingSource: no data available yet — first fetch returned empty data."
            )
        result = batches[0]
        for batch in batches[1:]:
            result = self._combine(result, batch)
        return result
```

- [ ] **Step 3: Run existing sync tests to verify basic behavior is intact**

```bash
uv run pytest tests/test_channels/test_polling_source.py::TestPollingSourceSyncMode -v
```

These tests still call `_get_latest_stream()` via `iter_data()` / `as_table()` (not yet updated). Expected: they may still fail because `iter_data()` / `as_table()` still use the old path. We will fix this in Task 5.

---

## Task 5: Update `iter_data()`, `as_table()`, and remove `_get_latest_stream()`

**Files:**
- Modify: `src/orcapod/core/sources/polling_source.py`

- [ ] **Step 1: Update `iter_data()` to use the new helpers**

Find:
```python
    def iter_data(self):
        """Iterate over (tag, data) pairs from the current snapshot."""
        return self._get_latest_stream().iter_data()
```

Replace with:
```python
    def iter_data(self):
        """Iterate over (tag, data) pairs from the current snapshot."""
        self._sync_poll_and_commit()
        return self._get_combined_stream().iter_data()
```

- [ ] **Step 2: Update `as_table()` to use the new helpers**

Find:
```python
    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table:
        """Return the accumulated rows as a PyArrow table."""
        return self._get_latest_stream().as_table(columns=columns, all_info=all_info)
```

Replace with:
```python
    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> pa.Table:
        """Return the accumulated rows as a PyArrow table."""
        self._sync_poll_and_commit()
        return self._get_combined_stream().as_table(columns=columns, all_info=all_info)
```

- [ ] **Step 3: Delete `_get_latest_stream()`**

Remove the entire `_get_latest_stream` method (lines ~513–546 in the original). It is no longer called anywhere.

- [ ] **Step 4: Run sync-mode tests**

```bash
uv run pytest tests/test_channels/test_polling_source.py::TestPollingSourceSyncMode -v
```

Expected: all **PASS**.

- [ ] **Step 5: Run schema validation tests**

```bash
uv run pytest tests/test_channels/test_polling_source.py::TestPollingSourceSchemaValidation -v
```

Expected: all **PASS** (including `test_sync_three_fetches_no_content_hash_leak`).

---

## Task 6: Rewrite `async_iter_data()`

Replace the pre-seed block and the poll/fetch/commit section with the optimistic lock protocol and per-iterator `local_batch_idx`. All error handling (cancelled, cursor invalidated, schema mismatch, exponential backoff, overrun, duration) is preserved verbatim.

**Files:**
- Modify: `src/orcapod/core/sources/polling_source.py`

- [ ] **Step 1: Replace the body of `async_iter_data()`**

Find the entire `async_iter_data` method (lines ~683–849) and replace it with:

```python
    async def async_iter_data(self):
        """Async generator that continuously emits (tag, data) pairs.

        Uses a per-iterator ``local_batch_idx`` to track the next position in
        the append-only ``_batches`` list. A drain step at the top of each
        loop iteration yields any newly committed batches (including those
        committed by concurrent sync callers) before sleeping.

        The optimistic lock protocol — snapshot cursor (brief lock) → poll and
        fetch with no lock held → commit only if cursor unchanged (brief lock)
        — prevents TOCTOU races without ever holding ``_state_lock`` across an
        ``await``.

        ``impl.close()`` is always awaited before returning or raising.
        """
        # local_batch_idx tracks the next _batches index to yield.
        # Starting at 0 means the first drain covers any pre-existing batches
        # (the pre-seed case from the old implementation).
        local_batch_idx = 0

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
                # ── 1. Drain: yield any batches not yet emitted ──────────────
                # Covers pre-existing rows on first iteration AND rows committed
                # by concurrent sync callers while this iterator was polling.
                while local_batch_idx < len(self._batches):
                    for item in self._batches[local_batch_idx].iter_data():
                        yield item
                    local_batch_idx += 1

                # ── 2. Sleep to next scheduled tick ──────────────────────────
                now = loop.time()
                if next_tick > now:
                    await asyncio.sleep(next_tick - now)

                # ── 3. Poll + fetch + commit (optimistic lock) ───────────────
                try:
                    # Brief lock: snapshot cursor only.
                    with self._state_lock:
                        cursor_snapshot = self._cursor

                    # Poll — no lock held across await.
                    has_new = await self._impl.poll(cursor=cursor_snapshot)

                    if has_new:
                        logger.debug(
                            "PollingSource %r: new data detected, fetching",
                            self._source_id,
                        )
                        # Fetch — no lock held across await.
                        new_cursor, data = await self._impl.fetch(cursor=cursor_snapshot)
                        new_stream = self._try_build_stream(data)

                        if new_stream is not None:
                            if self._tag_schema is not None or self._data_schema is not None:
                                self._validate_against_declared_schemas(new_stream)
                            if self._batches:
                                self._validate_combining_schemas(
                                    self._batches[0], new_stream
                                )

                        # Brief lock: check cursor then commit atomically.
                        committed = False
                        with self._state_lock:
                            if self._cursor == cursor_snapshot:
                                if new_stream is not None:
                                    self._batches.append(new_stream)
                                self._cursor = new_cursor
                                committed = True
                            # else: sync caller already advanced cursor —
                            # their batch is in _batches; drain step above
                            # will yield it at the top of the next iteration.

                        if committed:
                            self._update_last_modified_from_cursor(new_cursor)
                            emitted_count = new_stream.as_table().num_rows if new_stream is not None else 0
                            logger.debug(
                                "PollingSource %r: committed %d row(s)",
                                self._source_id,
                                emitted_count,
                            )
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
                    raise

                except InputValidationError:
                    # Schema mismatches are not transient — propagate immediately.
                    raise

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

                # ── 4. Tick advancement (start-to-start) ─────────────────────
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

                # ── 5. Duration check ─────────────────────────────────────────
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

- [ ] **Step 2: Run all async mode tests**

```bash
uv run pytest tests/test_channels/test_polling_source.py::TestPollingSourceAsyncMode -v
```

Expected: all **PASS**.

Note: `test_pre_seeding_yields_cached_rows_first` seeds the cache with `list(src.iter_data())` then replaces `src._impl`. The drain step at `local_batch_idx=0` yields the pre-seeded row first, so this test still passes.

Note: `test_cache_combining_accumulates_rows` now checks `src._batches` and `src._get_combined_stream()` — it should **PASS** after Task 3 step 5 update.

- [ ] **Step 3: Run error handling tests**

```bash
uv run pytest tests/test_channels/test_polling_source.py::TestPollingSourceErrorHandling -v
```

Expected: all **PASS**.

---

## Task 7: Run the full test suite and verify

**Files:** — (verification only)

- [ ] **Step 1: Run the entire polling source test file**

```bash
uv run pytest tests/test_channels/test_polling_source.py -v
```

Expected: all tests **PASS**, including the two new `TestPollingSourceSyncAccessDuringAsyncRun` tests.

- [ ] **Step 2: Run the broader test suite to check for regressions**

```bash
uv run pytest tests/ -x -q --timeout=120
```

Expected: all tests **PASS**. If any fail, investigate before proceeding.

---

## Task 8: Update `DESIGN_ISSUES.md`

**Files:**
- Modify: `DESIGN_ISSUES.md`

- [ ] **Step 1: Add PS4 entry after PS3 in the `polling_source.py` section**

In `DESIGN_ISSUES.md`, find the line:
```
---

## `src/orcapod/core/nodes/function_node.py`
```

(immediately after the PS3 entry). Insert this new entry before that separator:

```markdown
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

---
```

- [ ] **Step 2: Verify the DESIGN_ISSUES.md renders cleanly**

```bash
uv run python -c "open('DESIGN_ISSUES.md').read(); print('OK')"
```

Expected: `OK` (file is readable, no encoding issues).

---

## Task 9: Commit

**Files:** — (git only)

- [ ] **Step 1: Check which files changed**

```bash
git diff --stat
```

Expected: 3 files — `src/orcapod/core/sources/polling_source.py`, `tests/test_channels/test_polling_source.py`, `DESIGN_ISSUES.md`.

- [ ] **Step 2: Run full tests one final time**

```bash
uv run pytest tests/test_channels/test_polling_source.py -v -q
```

Expected: all **PASS**.

- [ ] **Step 3: Stage and commit**

```bash
git add src/orcapod/core/sources/polling_source.py \
        tests/test_channels/test_polling_source.py \
        DESIGN_ISSUES.md \
        superpowers/specs/2026-08-25-itl-617-pollingsource-async-mode-guard-design.md \
        superpowers/plans/2026-08-25-itl-617-pollingsource-concurrent-cursor-safety.md
git commit -m "$(cat <<'EOF'
fix(polling_source): replace _accumulated_stream with optimistic-lock batch list

Fixes ITL-617: concurrent sync callers (iter_data, as_table) could advance
_cursor while async_iter_data was running, causing the async loop to skip
already-fetched rows — permanent silent data loss.

Replace the single _accumulated_stream with an append-only _batches list and
a threading.Lock. All callers use the optimistic lock protocol: snapshot cursor
(brief lock) → I/O with no lock held → commit only if cursor unchanged (brief
lock). The async loop tracks its position with a per-iterator local_batch_idx
and drains _batches at the top of each iteration to pick up sync-committed
rows. The lock is never held across await.

Closes ITL-617
EOF
)"
```

- [ ] **Step 4: Verify commit looks right**

```bash
git log --oneline -3
git show --stat HEAD
```

Expected: one new commit with the 5 files listed.

---

## Self-Review

**Spec coverage checklist:**

| Spec requirement | Task |
|---|---|
| `_accumulated_stream` → `_batches` (append-only) | Task 3 step 2 |
| `_state_lock: threading.Lock` added | Task 3 step 2 |
| `output_schema()` cached bypass → `_batches[0]` | Task 3 step 3 |
| `keys()` cached bypass → `_batches[0]` | Task 3 step 4 |
| `_sync_poll_and_commit()` — optimistic lock protocol | Task 4 step 1 |
| `_get_combined_stream()` — concat all batches | Task 4 step 2 |
| `iter_data()` uses `_sync_poll_and_commit` + `_get_combined_stream` | Task 5 step 1 |
| `as_table()` uses `_sync_poll_and_commit` + `_get_combined_stream` | Task 5 step 2 |
| `_get_latest_stream()` removed | Task 5 step 3 |
| `async_iter_data()` rewritten: `local_batch_idx`, drain step, opt-lock | Task 6 step 1 |
| Lock never held across `await` | Task 6 step 1 |
| No `_async_loop_active` flag | (design choice — not in plan) |
| `test_cache_combining_accumulates_rows` updated | Task 3 step 5 |
| `TestPollingSourceSyncAccessDuringAsyncRun` with 2 tests | Task 2 step 1 |
| `DESIGN_ISSUES.md` PS4 entry | Task 8 step 1 |

All spec requirements are covered.

**Type consistency check:** `_batches` is typed as `list[ArrowTableStream]`. `_batches[0]` returns `ArrowTableStream`. `_get_combined_stream()` returns `ArrowTableStream`. `_combine(result, batch)` takes two `ArrowTableStream` and returns `ArrowTableStream`. All consistent. ✓

**`_validate_combining_schemas(self._batches[0], new_stream)`** — called in `_sync_poll_and_commit()` and `async_iter_data()` before the commit lock. The method signature is `_validate_combining_schemas(existing: ArrowTableStream, new_stream: ArrowTableStream)`. ✓
