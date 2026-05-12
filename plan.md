# Plan: Unified `process_data` / `async_process_data` + Node `async_execute`

## Goal

Establish `process_data` and `async_process_data` as **the** universal per-data
interface across FunctionPod, FunctionPodStream, FunctionNode, and PersistentFunctionNode.
All iteration paths — sequential, concurrent, and async — route through these methods.
Add `async_execute` to all four Node classes. Add cache-aware `async_call` to
`CachedDataFunction`. Remove `_execute_concurrent` module-level helper.

---

## What exists today

### Class hierarchy

```
_FunctionPodBase (TraceableBase)
  ├── process_data(tag, data)       → calls data_function.call(data)
  ├── FunctionPod
  │     ├── process() → FunctionPodStream
  │     └── async_execute()             → calls data_function.async_call(data) DIRECTLY
  │
  FunctionPodStream (StreamBase)
  │   ├── _iter_data_sequential()    → calls _function_pod.process_data(tag, data) ✓
  │   └── _iter_data_concurrent()    → calls _execute_concurrent(data_function, ...) DIRECTLY
  │
  FunctionNode (StreamBase)
  │   ├── _iter_data_sequential()    → calls _data_function.call(data) DIRECTLY
  │   ├── _iter_data_concurrent()    → calls _execute_concurrent(_data_function, ...) DIRECTLY
  │   └── (no async_execute)
  │
  PersistentFunctionNode (FunctionNode)
      ├── process_data(tag, data)   → calls _data_function.call(data, skip_cache_*=...)
      │                                   then add_pipeline_record(...)
      ├── iter_data()                → Phase 1: replay from DB
      │                                   Phase 2: calls self.process_data(tag, data) ✓
      └── (no async_execute)

OperatorNode (StreamBase)
  ├── run()                             → calls _operator.process(*streams)
  └── (no async_execute)

PersistentOperatorNode (OperatorNode)
  ├── _compute_and_store()              → calls _operator.process() + bulk DB write
  ├── _replay_from_cache()              → loads from DB
  └── (no async_execute)
```

### Module-level helpers

```python
def _executor_supports_concurrent(data_function) -> bool:
    """True if the pf's executor supports concurrent execution."""

def _execute_concurrent(data_function, data) -> list[DataProtocol | None]:
    """Submit all data concurrently via asyncio.gather(pf.async_call(...)).
    Falls back to sequential pf.call() if already inside a running event loop."""
```

### Problems

1. **FunctionPod.async_execute** bypasses `process_data` — calls `data_function.async_call`
   directly (line 317).
2. **FunctionPodStream._iter_data_concurrent** bypasses `process_data` — calls
   `_execute_concurrent(data_function, ...)` directly (line 472).
3. **FunctionNode._iter_data_sequential** bypasses any process_data — calls
   `_data_function.call(data)` directly (line 831).
4. **FunctionNode._iter_data_concurrent** same — calls `_execute_concurrent` directly
   (line 852).
5. **CachedDataFunction.async_call** inherits from `DataFunctionWrapper` — completely
   **bypasses the cache** (no lookup, no recording).
6. **No `async_process_data`** exists anywhere.
7. **No `async_execute`** on any Node class.
8. **`_execute_concurrent`** is a module-level function that takes a raw `data_function`
   and list of bare `data` — no way to route through `process_data`.

---

## Design principles

### A. `process_data` / `async_process_data` is the single per-data entry point

Every class in the function pod hierarchy defines these two methods. **All** iteration and
execution paths go through them — sequential, concurrent, and async. No direct
`data_function.call()` or `data_function.async_call()` calls outside of these methods.

```
_FunctionPodBase.process_data(tag, pkt)         → data_function.call(pkt)
_FunctionPodBase.async_process_data(tag, pkt)    → await data_function.async_call(pkt)

FunctionNode.process_data(tag, pkt)              → self._function_pod.process_data(tag, pkt)
FunctionNode.async_process_data(tag, pkt)        → await self._function_pod.async_process_data(tag, pkt)

PersistentFunctionNode.process_data(tag, pkt)    → cache check → self._function_pod.process_data → pipeline record
PersistentFunctionNode.async_process_data(tag, pkt) → cache check → await self._function_pod.async_process_data → pipeline record
```

Wait — there's a subtlety with PersistentFunctionNode. Today its `process_data` calls
`self._data_function.call(data, skip_cache_lookup=..., skip_cache_insert=...)` directly,
where `self._data_function` is a `CachedDataFunction` (which wraps the original pf).
It does NOT delegate to the pod's `process_data`. That's because PersistentFunctionNode
needs to pass `skip_cache_*` kwargs that the base `process_data` doesn't accept.

The cleanest structure:

```
PersistentFunctionNode.process_data(tag, pkt)
  → self._data_function.call(pkt, skip_cache_*=...)    # CachedDataFunction (sync)
  → self.add_pipeline_record(...)                         # pipeline DB (sync)

PersistentFunctionNode.async_process_data(tag, pkt)
  → await self._data_function.async_call(pkt, skip_cache_*=...)  # CachedDataFunction (async)
  → self.add_pipeline_record(...)                                   # pipeline DB (sync)
```

This is the same as today for the sync path. The `CachedDataFunction` handles the result
cache internally. The `PersistentFunctionNode` handles pipeline records. Neither delegates
to the pod's `process_data` — the pod is bypassed because the `CachedDataFunction`
replaced the raw data function in `__init__`.

### B. Concurrent iteration routes through `async_process_data`

The concurrent path is inherently async — it uses `asyncio.gather`. So it naturally routes
through `async_process_data`. The fallback path (when already inside an event loop) routes
through `process_data` (sync).

For **FunctionPodStream**, the target is the pod:
```python
# concurrent
await self._function_pod.async_process_data(tag, pkt)
# fallback
self._function_pod.process_data(tag, pkt)
```

For **FunctionNode**, the target is `self` — so overrides (PersistentFunctionNode) kick in:
```python
# concurrent
await self.async_process_data(tag, pkt)
# fallback
self.process_data(tag, pkt)
```

This means PersistentFunctionNode's concurrent path **automatically** gets cache checks +
pipeline records via polymorphism. No special handling needed.

### C. `_execute_concurrent` is removed

The module-level `_execute_concurrent(data_function, data)` helper is removed. Its
logic (asyncio.gather with event-loop fallback) is inlined into `_iter_data_concurrent`
methods, but now routes through `process_data` / `async_process_data` instead of raw
`data_function.call` / `data_function.async_call`.

The `_executor_supports_concurrent` helper stays — it's just a predicate check.

### D. Sync and async are cleanly separated execution modes

- Sync: `iter_data()` / `as_table()` / `run()`
- Async: `async_execute(inputs, output)`

They don't populate each other's caches. DB persistence (for Persistent variants) provides
durability that works across both modes.

### E. OperatorNode delegates to operator, PersistentOperatorNode intercepts for storage

Operators are opaque stream transformers — no per-data hook. `OperatorNode` passes through
directly. `PersistentOperatorNode` uses an intermediate channel + `TaskGroup` to forward
results downstream immediately while collecting them for post-hoc DB storage.

### F. DB operations stay synchronous

The `ArrowDatabaseProtocol` is sync. All DB reads/writes within async methods are sync calls.
Acceptable because DB is typically in-process and fast. Async DB protocol is deferred.

---

## Implementation steps

### Step 1: Add `async_process_data` to `_FunctionPodBase`

**File:** `src/orcapod/core/function_pod.py`

Add alongside existing `process_data` (after line 180):

```python
async def async_process_data(
    self, tag: TagProtocol, data: DataProtocol
) -> tuple[TagProtocol, DataProtocol | None]:
    """Async counterpart of ``process_data``."""
    return tag, await self.data_function.async_call(data)
```

### Step 2: Fix `FunctionPod.async_execute` to use `async_process_data`

**File:** `src/orcapod/core/function_pod.py`

Change the `process_one` inner function (lines 315-322):

```python
async def process_one(tag: TagProtocol, data: DataProtocol) -> None:
    try:
        tag, result_data = await self.async_process_data(tag, data)
        if result_data is not None:
            await output.send((tag, result_data))
    finally:
        if sem is not None:
            sem.release()
```

### Step 3: Fix `FunctionPodStream._iter_data_concurrent` to use `async_process_data`

**File:** `src/orcapod/core/function_pod.py`

Replace the `_execute_concurrent` call (lines 454-482) with direct `async_process_data`
routing:

```python
def _iter_data_concurrent(
    self,
) -> Iterator[tuple[TagProtocol, DataProtocol]]:
    """Collect remaining inputs, execute concurrently, and yield results in order."""
    input_iter = self._cached_input_iterator

    all_inputs: list[tuple[int, TagProtocol, DataProtocol]] = []
    to_compute: list[tuple[int, TagProtocol, DataProtocol]] = []
    for i, (tag, data) in enumerate(input_iter):
        all_inputs.append((i, tag, data))
        if i not in self._cached_output_datas:
            to_compute.append((i, tag, data))
    self._cached_input_iterator = None

    if to_compute:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None:
            # Already in event loop — fall back to sequential sync
            results = [
                self._function_pod.process_data(tag, pkt)
                for _, tag, pkt in to_compute
            ]
        else:
            # No event loop — run concurrently via asyncio.run
            async def _gather() -> list[tuple[TagProtocol, DataProtocol | None]]:
                return list(
                    await asyncio.gather(
                        *[
                            self._function_pod.async_process_data(tag, pkt)
                            for _, tag, pkt in to_compute
                        ]
                    )
                )

            results = asyncio.run(_gather())

        for (i, _, _), (tag, output_data) in zip(to_compute, results):
            self._cached_output_datas[i] = (tag, output_data)

    for i, *_ in all_inputs:
        tag, data = self._cached_output_datas[i]
        if data is not None:
            yield tag, data
```

**Note:** The method signature drops the `data_function` parameter — it no longer needs
it since it routes through `self._function_pod`.

The `iter_data` method that calls this also needs updating — remove the `pf` argument:

```python
def iter_data(self) -> Iterator[tuple[TagProtocol, DataProtocol]]:
    if self.is_stale:
        self.clear_cache()
    if self._cached_input_iterator is not None:
        if _executor_supports_concurrent(self._function_pod.data_function):
            yield from self._iter_data_concurrent()
        else:
            yield from self._iter_data_sequential()
    else:
        for i in range(len(self._cached_output_datas)):
            tag, data = self._cached_output_datas[i]
            if data is not None:
                yield tag, data
```

### Step 4: Fix `FunctionNode._iter_data_sequential` to use `process_data`

**File:** `src/orcapod/core/function_pod.py`

Change line 831 from:
```python
output_data = self._data_function.call(data)
self._cached_output_datas[i] = (tag, output_data)
```
to:
```python
tag, output_data = self.process_data(tag, data)
self._cached_output_datas[i] = (tag, output_data)
```

### Step 5: Fix `FunctionNode._iter_data_concurrent` to use `async_process_data`

**File:** `src/orcapod/core/function_pod.py`

Same transformation as Step 3, but routing through `self` instead of `self._function_pod`:

```python
def _iter_data_concurrent(
    self,
) -> Iterator[tuple[TagProtocol, DataProtocol]]:
    """Collect remaining inputs, execute concurrently, and yield results in order."""
    input_iter = self._cached_input_iterator

    all_inputs: list[tuple[int, TagProtocol, DataProtocol]] = []
    to_compute: list[tuple[int, TagProtocol, DataProtocol]] = []
    for i, (tag, data) in enumerate(input_iter):
        all_inputs.append((i, tag, data))
        if i not in self._cached_output_datas:
            to_compute.append((i, tag, data))
    self._cached_input_iterator = None

    if to_compute:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None:
            # Already in event loop — fall back to sequential sync
            results = [
                self.process_data(tag, pkt)
                for _, tag, pkt in to_compute
            ]
        else:
            # No event loop — run concurrently via asyncio.run
            async def _gather() -> list[tuple[TagProtocol, DataProtocol | None]]:
                return list(
                    await asyncio.gather(
                        *[
                            self.async_process_data(tag, pkt)
                            for _, tag, pkt in to_compute
                        ]
                    )
                )

            results = asyncio.run(_gather())

        for (i, _, _), (tag, output_data) in zip(to_compute, results):
            self._cached_output_datas[i] = (tag, output_data)

    for i, *_ in all_inputs:
        tag, data = self._cached_output_datas[i]
        if data is not None:
            yield tag, data
```

**Critical difference from Step 3:** Uses `self.process_data` / `self.async_process_data`
instead of `self._function_pod.*`. This means when `PersistentFunctionNode` inherits this
method, it automatically routes through its overridden `process_data` /
`async_process_data` which include cache checks + pipeline record storage.

### Step 6: Remove `_execute_concurrent`

**File:** `src/orcapod/core/function_pod.py`

Delete the `_execute_concurrent` function (lines 52-82). Its logic is now inlined into the
`_iter_data_concurrent` methods.

### Step 7: Add `process_data` and `async_process_data` to `FunctionNode`

**File:** `src/orcapod/core/function_pod.py`

FunctionNode currently has no `process_data`. Add delegation to the function pod:

```python
def process_data(
    self, tag: TagProtocol, data: DataProtocol
) -> tuple[TagProtocol, DataProtocol | None]:
    """Process a single data by delegating to the function pod."""
    return self._function_pod.process_data(tag, data)

async def async_process_data(
    self, tag: TagProtocol, data: DataProtocol
) -> tuple[TagProtocol, DataProtocol | None]:
    """Async counterpart of ``process_data``."""
    return await self._function_pod.async_process_data(tag, data)
```

### Step 8: Add `FunctionNode.async_execute`

**File:** `src/orcapod/core/function_pod.py`

Sequential streaming through `async_process_data`:

```python
async def async_execute(
    self,
    inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
    output: WritableChannel[tuple[TagProtocol, DataProtocol]],
) -> None:
    """Streaming async execution — process each data via async_process_data."""
    try:
        async for tag, data in inputs[0]:
            tag, result_data = await self.async_process_data(tag, data)
            if result_data is not None:
                await output.send((tag, result_data))
    finally:
        await output.close()
```

### Step 9: Add async cache-aware `async_call` to `CachedDataFunction`

**File:** `src/orcapod/core/data_function.py`

Override `async_call` to mirror the sync `call()` logic (lines 508-533):

```python
async def async_call(
    self,
    data: DataProtocol,
    *,
    skip_cache_lookup: bool = False,
    skip_cache_insert: bool = False,
) -> DataProtocol | None:
    """Async counterpart of ``call`` with cache check and recording."""
    output_data = None
    if not skip_cache_lookup:
        logger.info("Checking for cache...")
        output_data = self.get_cached_output_for_data(data)
        if output_data is not None:
            logger.info(f"Cache hit for {data}!")
    if output_data is None:
        output_data = await self._data_function.async_call(data)
        if output_data is not None:
            if not skip_cache_insert:
                self.record_data(data, output_data)
            output_data = output_data.with_meta_columns(
                **{self.RESULT_COMPUTED_FLAG: True}
            )
    return output_data
```

### Step 10: Add `async_process_data` to `PersistentFunctionNode`

**File:** `src/orcapod/core/function_pod.py`

PersistentFunctionNode already has `process_data` (line 1027-1066) which calls
`self._data_function.call(data, skip_cache_*=...)` (where `_data_function` is a
`CachedDataFunction`) then `self.add_pipeline_record(...)`. Add the async counterpart:

```python
async def async_process_data(
    self,
    tag: TagProtocol,
    data: DataProtocol,
    skip_cache_lookup: bool = False,
    skip_cache_insert: bool = False,
) -> tuple[TagProtocol, DataProtocol | None]:
    """Async counterpart of ``process_data``.

    Uses the CachedDataFunction's async_call for computation + result caching.
    Pipeline record storage is synchronous (DB protocol is sync).
    """
    output_data = await self._data_function.async_call(
        data,
        skip_cache_lookup=skip_cache_lookup,
        skip_cache_insert=skip_cache_insert,
    )

    if output_data is not None:
        result_computed = bool(
            output_data.get_meta_value(
                self._data_function.RESULT_COMPUTED_FLAG, False
            )
        )
        self.add_pipeline_record(
            tag,
            data,
            data_record_id=output_data.datagram_id,
            computed=result_computed,
        )

    return tag, output_data
```

### Step 11: Add `PersistentFunctionNode.async_execute` (two-phase)

**File:** `src/orcapod/core/function_pod.py`

Overrides `FunctionNode.async_execute`:

```python
async def async_execute(
    self,
    inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
    output: WritableChannel[tuple[TagProtocol, DataProtocol]],
) -> None:
    """Two-phase async execution: replay cached, then compute missing."""
    try:
        # Phase 1: emit existing results from DB
        existing = self.get_all_records(columns={"meta": True})
        computed_hashes: set[str] = set()
        if existing is not None and existing.num_rows > 0:
            tag_keys = self._input_stream.keys()[0]
            hash_col = constants.INPUT_DATA_HASH_COL
            computed_hashes = set(
                cast(list[str], existing.column(hash_col).to_pylist())
            )
            data_table = existing.drop([hash_col])
            existing_stream = ArrowTableStream(data_table, tag_columns=tag_keys)
            for tag, data in existing_stream.iter_data():
                await output.send((tag, data))

        # Phase 2: process data not already in the DB
        async for tag, data in inputs[0]:
            input_hash = data.content_hash().to_string()
            if input_hash in computed_hashes:
                continue
            tag, output_data = await self.async_process_data(tag, data)
            if output_data is not None:
                await output.send((tag, output_data))
    finally:
        await output.close()
```

### Step 12: Add `OperatorNode.async_execute`

**File:** `src/orcapod/core/operator_node.py`

Direct pass-through:

```python
async def async_execute(
    self,
    inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
    output: WritableChannel[tuple[TagProtocol, DataProtocol]],
) -> None:
    """Delegate to operator's async_execute."""
    await self._operator.async_execute(inputs, output)
```

### Step 13: Extract `_store_output_stream` from `PersistentOperatorNode._compute_and_store`

**File:** `src/orcapod/core/operator_node.py`

```python
def _store_output_stream(self, stream: StreamProtocol) -> None:
    """Materialize stream and store in the pipeline database with per-row dedup."""
    output_table = stream.as_table(
        columns={"source": True, "system_tags": True},
    )

    arrow_hasher = self.data_context.arrow_hasher
    record_hashes = []
    for batch in output_table.to_batches():
        for i in range(len(batch)):
            record_hashes.append(
                arrow_hasher.hash_table(batch.slice(i, 1)).to_hex()
            )

    output_table = output_table.add_column(
        0,
        self.HASH_COLUMN_NAME,
        pa.array(record_hashes, type=pa.large_string()),
    )

    self._pipeline_database.add_records(
        self.pipeline_path,
        output_table,
        record_id_column=self.HASH_COLUMN_NAME,
        skip_duplicates=True,
    )

    self._cached_output_table = output_table.drop(self.HASH_COLUMN_NAME)
```

Refactor `_compute_and_store`:

```python
def _compute_and_store(self) -> None:
    self._cached_output_stream = self._operator.process(*self._input_streams)
    if self._cache_mode == CacheMode.OFF:
        self._update_modified_time()
        return
    self._store_output_stream(self._cached_output_stream)
    self._update_modified_time()
```

### Step 14: Add `PersistentOperatorNode.async_execute`

**File:** `src/orcapod/core/operator_node.py`

```python
async def async_execute(
    self,
    inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
    output: WritableChannel[tuple[TagProtocol, DataProtocol]],
) -> None:
    """Async execution with cache mode handling.

    REPLAY: emit from DB, close output.
    OFF: delegate to operator, forward results.
    LOG: delegate to operator, forward + collect results, then store in DB.
    """
    try:
        if self._cache_mode == CacheMode.REPLAY:
            self._replay_from_cache()
            assert self._cached_output_stream is not None
            for tag, data in self._cached_output_stream.iter_data():
                await output.send((tag, data))
            return  # finally block closes output

        # OFF or LOG: delegate to operator, forward results downstream
        intermediate = Channel[tuple[TagProtocol, DataProtocol]]()
        collected: list[tuple[TagProtocol, DataProtocol]] = []

        async def forward() -> None:
            async for item in intermediate.reader:
                collected.append(item)
                await output.send(item)

        async with asyncio.TaskGroup() as tg:
            tg.create_task(
                self._operator.async_execute(inputs, intermediate.writer)
            )
            tg.create_task(forward())

        # TaskGroup has completed — all results are in `collected`
        # Store if LOG mode (sync DB write, post-hoc)
        if self._cache_mode == CacheMode.LOG and collected:
            stream = StaticOutputPod._materialize_to_stream(collected)
            self._cached_output_stream = stream
            self._store_output_stream(stream)

        self._update_modified_time()
    finally:
        await output.close()
```

### Step 15: Add imports

**`src/orcapod/core/operator_node.py`** — add:
```python
import asyncio
from collections.abc import Sequence

from orcapod.channels import Channel, ReadableChannel, WritableChannel
from orcapod.core.static_output_pod import StaticOutputPod
```

**`src/orcapod/core/function_pod.py`** — already has all needed imports.

### Step 16: Update regression test for `_execute_concurrent` removal

**File:** `tests/test_core/test_regression_fixes.py`

`TestExecuteConcurrentInRunningLoop` imports and tests `_execute_concurrent` directly.
Since we're removing that function, this test class needs to be rewritten to test the
behavior through the actual classes:

- Test that `FunctionPodStream._iter_data_concurrent` falls back to sequential
  `process_data` when called inside a running event loop.
- Test that `FunctionNode._iter_data_concurrent` does the same.

The tested behavior (event-loop fallback) is preserved — it's just now method-internal
rather than in a standalone helper.

### Step 17: Tests for new functionality

**File:** `tests/test_channels/test_node_async_execute.py` (new)

```
TestProtocolConformance
  - test_function_node_satisfies_async_executable_protocol
  - test_persistent_function_node_satisfies_async_executable_protocol
  - test_operator_node_satisfies_async_executable_protocol
  - test_persistent_operator_node_satisfies_async_executable_protocol

TestCachedDataFunctionAsync
  - test_async_call_cache_miss_computes_and_records
  - test_async_call_cache_hit_returns_cached
  - test_async_call_skip_cache_lookup
  - test_async_call_skip_cache_insert

TestProcessDataRouting
  - test_function_pod_stream_sequential_uses_process_data
  - test_function_pod_stream_concurrent_uses_async_process_data
  - test_function_node_sequential_uses_process_data
  - test_function_node_concurrent_uses_async_process_data
  - test_persistent_function_node_concurrent_uses_overridden_async_process_data
  - test_concurrent_fallback_in_event_loop_uses_sync_process_data

TestFunctionNodeAsyncExecute
  - test_basic_streaming_matches_sync
  - test_empty_input_closes_cleanly
  - test_none_data_filtered_out

TestPersistentFunctionNodeAsyncExecute
  - test_no_cache_processes_all_inputs
  - test_phase1_emits_cached_results
  - test_phase2_skips_cached_computes_new
  - test_pipeline_records_created_for_new_data
  - test_result_cache_populated_for_new_data

TestOperatorNodeAsyncExecute
  - test_unary_op_delegation (SelectDataColumns)
  - test_binary_op_delegation (SemiJoin)
  - test_nary_op_delegation (Join)
  - test_results_match_sync_run

TestPersistentOperatorNodeAsyncExecute
  - test_off_mode_computes_no_db_write
  - test_log_mode_computes_and_stores
  - test_log_mode_results_match_sync
  - test_replay_mode_emits_from_db
  - test_replay_empty_db_returns_empty

TestEndToEnd
  - test_source_to_persistent_function_node_pipeline
  - test_source_to_persistent_operator_node_pipeline
```

### Step 18: Run full test suite

```bash
uv run pytest tests/ -x
```

---

## Summary of all changes

### Call chains after changes

**Sync sequential path:**
```
FunctionPodStream._iter_data_sequential
  → self._function_pod.process_data(tag, pkt)       # already correct
    → data_function.call(pkt)

FunctionNode._iter_data_sequential
  → self.process_data(tag, pkt)                      # CHANGED: was _data_function.call(pkt)
    → self._function_pod.process_data(tag, pkt)
      → data_function.call(pkt)

PersistentFunctionNode._iter_data_sequential (inherited from FunctionNode)
  → self.process_data(tag, pkt)                      # polymorphism kicks in
    → CachedDataFunction.call(pkt, skip_cache_*=...) # cache check + compute + record
    → self.add_pipeline_record(...)                     # pipeline DB
```

**Sync concurrent path:**
```
FunctionPodStream._iter_data_concurrent
  → asyncio.run(gather(
        self._function_pod.async_process_data(tag, pkt) ...   # CHANGED: was _execute_concurrent
    ))
  OR (if event loop running):
    self._function_pod.process_data(tag, pkt) ...             # fallback

FunctionNode._iter_data_concurrent
  → asyncio.run(gather(
        self.async_process_data(tag, pkt) ...                 # CHANGED: was _execute_concurrent
    ))
  OR (if event loop running):
    self.process_data(tag, pkt) ...                           # fallback

PersistentFunctionNode._iter_data_concurrent (inherited from FunctionNode)
  → asyncio.run(gather(
        self.async_process_data(tag, pkt) ...                 # polymorphism kicks in
          → await CachedDataFunction.async_call(pkt)          # cache + compute
          → self.add_pipeline_record(...)                       # pipeline DB
    ))
```

**Async execution path:**
```
FunctionPod.async_execute
  → await self.async_process_data(tag, pkt)          # CHANGED: was data_function.async_call
    → await data_function.async_call(pkt)

FunctionNode.async_execute                              # NEW
  → await self.async_process_data(tag, pkt)
    → await self._function_pod.async_process_data(tag, pkt)
      → await data_function.async_call(pkt)

PersistentFunctionNode.async_execute                    # NEW (two-phase)
  Phase 1: emit from DB
  Phase 2:
    → await self.async_process_data(tag, pkt)         # polymorphic override
      → await CachedDataFunction.async_call(pkt)      # cache + compute
      → self.add_pipeline_record(...)                   # pipeline DB (sync)

OperatorNode.async_execute                              # NEW
  → await operator.async_execute(inputs, output)

PersistentOperatorNode.async_execute                    # NEW
  REPLAY: emit from DB
  OFF/LOG:
    TaskGroup:
      operator.async_execute(inputs, intermediate.writer)
      forward(intermediate.reader → output + collect)
    if LOG: _store_output_stream(materialize(collected)) # sync DB write
```

### Files modified

| File | Changes |
|------|---------|
| `src/orcapod/core/data_function.py` | Add `CachedDataFunction.async_call` override with cache logic |
| `src/orcapod/core/function_pod.py` | (1) Add `_FunctionPodBase.async_process_data` |
| | (2) Fix `FunctionPod.async_execute` to use `async_process_data` |
| | (3) Rewrite `FunctionPodStream._iter_data_concurrent` — route through `_function_pod.async_process_data` / `process_data`, drop `data_function` param |
| | (4) Update `FunctionPodStream.iter_data` — remove `pf` arg to `_iter_data_concurrent` |
| | (5) Fix `FunctionNode._iter_data_sequential` to use `self.process_data` |
| | (6) Rewrite `FunctionNode._iter_data_concurrent` — route through `self.async_process_data` / `self.process_data` |
| | (7) Add `FunctionNode.process_data` + `async_process_data` (delegate to pod) |
| | (8) Add `FunctionNode.async_execute` |
| | (9) Add `PersistentFunctionNode.async_process_data` (cache + pipeline records) |
| | (10) Add `PersistentFunctionNode.async_execute` (two-phase) |
| | (11) Remove `_execute_concurrent` module-level helper |
| `src/orcapod/core/operator_node.py` | (1) Add imports |
| | (2) Add `OperatorNode.async_execute` (pass-through) |
| | (3) Extract `PersistentOperatorNode._store_output_stream` |
| | (4) Refactor `PersistentOperatorNode._compute_and_store` |
| | (5) Add `PersistentOperatorNode.async_execute` (TaskGroup + post-hoc storage) |
| `tests/test_core/test_regression_fixes.py` | Rewrite `TestExecuteConcurrentInRunningLoop` — test through classes instead of removed helper |
| `tests/test_channels/test_node_async_execute.py` | New test file |
