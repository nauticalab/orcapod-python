# Plan: Unified `process_packet` / `async_process_packet` + Node `async_execute`

## Goal

Establish `process_packet` and `async_process_packet` as **the** universal per-packet
interface across FunctionPod, FunctionPodStream, FunctionNode, and PersistentFunctionNode.
All iteration paths — sequential, concurrent, and async — route through these methods.
Add `async_execute` to all four Node classes. Add cache-aware `async_call` to
`CachedPacketFunction`. Remove `_execute_concurrent` module-level helper.

---

## What exists today

### Class hierarchy

```
_FunctionPodBase (TraceableBase)
  ├── process_packet(tag, packet)       → calls packet_function.call(packet)
  ├── FunctionPod
  │     ├── process() → FunctionPodStream
  │     └── async_execute()             → calls packet_function.async_call(packet) DIRECTLY
  │
  FunctionPodStream (StreamBase)
  │   ├── _iter_packets_sequential()    → calls _function_pod.process_packet(tag, packet) ✓
  │   └── _iter_packets_concurrent()    → calls _execute_concurrent(packet_function, ...) DIRECTLY
  │
  FunctionNode (StreamBase)
  │   ├── _iter_packets_sequential()    → calls _packet_function.call(packet) DIRECTLY
  │   ├── _iter_packets_concurrent()    → calls _execute_concurrent(_packet_function, ...) DIRECTLY
  │   └── (no async_execute)
  │
  PersistentFunctionNode (FunctionNode)
      ├── process_packet(tag, packet)   → calls _packet_function.call(packet, skip_cache_*=...)
      │                                   then add_pipeline_record(...)
      ├── iter_packets()                → Phase 1: replay from DB
      │                                   Phase 2: calls self.process_packet(tag, packet) ✓
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
def _executor_supports_concurrent(packet_function) -> bool:
    """True if the pf's executor supports concurrent execution."""

def _execute_concurrent(packet_function, packets) -> list[PacketProtocol | None]:
    """Submit all packets concurrently via asyncio.gather(pf.async_call(...)).
    Falls back to sequential pf.call() if already inside a running event loop."""
```

### Problems

1. **FunctionPod.async_execute** bypasses `process_packet` — calls `packet_function.async_call`
   directly (line 317).
2. **FunctionPodStream._iter_packets_concurrent** bypasses `process_packet` — calls
   `_execute_concurrent(packet_function, ...)` directly (line 472).
3. **FunctionNode._iter_packets_sequential** bypasses any process_packet — calls
   `_packet_function.call(packet)` directly (line 831).
4. **FunctionNode._iter_packets_concurrent** same — calls `_execute_concurrent` directly
   (line 852).
5. **CachedPacketFunction.async_call** inherits from `PacketFunctionWrapper` — completely
   **bypasses the cache** (no lookup, no recording).
6. **No `async_process_packet`** exists anywhere.
7. **No `async_execute`** on any Node class.
8. **`_execute_concurrent`** is a module-level function that takes a raw `packet_function`
   and list of bare `packets` — no way to route through `process_packet`.

---

## Design principles

### A. `process_packet` / `async_process_packet` is the single per-packet entry point

Every class in the function pod hierarchy defines these two methods. **All** iteration and
execution paths go through them — sequential, concurrent, and async. No direct
`packet_function.call()` or `packet_function.async_call()` calls outside of these methods.

```
_FunctionPodBase.process_packet(tag, pkt)         → packet_function.call(pkt)
_FunctionPodBase.async_process_packet(tag, pkt)    → await packet_function.async_call(pkt)

FunctionNode.process_packet(tag, pkt)              → self._function_pod.process_packet(tag, pkt)
FunctionNode.async_process_packet(tag, pkt)        → await self._function_pod.async_process_packet(tag, pkt)

PersistentFunctionNode.process_packet(tag, pkt)    → cache check → self._function_pod.process_packet → pipeline record
PersistentFunctionNode.async_process_packet(tag, pkt) → cache check → await self._function_pod.async_process_packet → pipeline record
```

Wait — there's a subtlety with PersistentFunctionNode. Today its `process_packet` calls
`self._packet_function.call(packet, skip_cache_lookup=..., skip_cache_insert=...)` directly,
where `self._packet_function` is a `CachedPacketFunction` (which wraps the original pf).
It does NOT delegate to the pod's `process_packet`. That's because PersistentFunctionNode
needs to pass `skip_cache_*` kwargs that the base `process_packet` doesn't accept.

The cleanest structure:

```
PersistentFunctionNode.process_packet(tag, pkt)
  → self._packet_function.call(pkt, skip_cache_*=...)    # CachedPacketFunction (sync)
  → self.add_pipeline_record(...)                         # pipeline DB (sync)

PersistentFunctionNode.async_process_packet(tag, pkt)
  → await self._packet_function.async_call(pkt, skip_cache_*=...)  # CachedPacketFunction (async)
  → self.add_pipeline_record(...)                                   # pipeline DB (sync)
```

This is the same as today for the sync path. The `CachedPacketFunction` handles the result
cache internally. The `PersistentFunctionNode` handles pipeline records. Neither delegates
to the pod's `process_packet` — the pod is bypassed because the `CachedPacketFunction`
replaced the raw packet function in `__init__`.

### B. Concurrent iteration routes through `async_process_packet`

The concurrent path is inherently async — it uses `asyncio.gather`. So it naturally routes
through `async_process_packet`. The fallback path (when already inside an event loop) routes
through `process_packet` (sync).

For **FunctionPodStream**, the target is the pod:
```python
# concurrent
await self._function_pod.async_process_packet(tag, pkt)
# fallback
self._function_pod.process_packet(tag, pkt)
```

For **FunctionNode**, the target is `self` — so overrides (PersistentFunctionNode) kick in:
```python
# concurrent
await self.async_process_packet(tag, pkt)
# fallback
self.process_packet(tag, pkt)
```

This means PersistentFunctionNode's concurrent path **automatically** gets cache checks +
pipeline records via polymorphism. No special handling needed.

### C. `_execute_concurrent` is removed

The module-level `_execute_concurrent(packet_function, packets)` helper is removed. Its
logic (asyncio.gather with event-loop fallback) is inlined into `_iter_packets_concurrent`
methods, but now routes through `process_packet` / `async_process_packet` instead of raw
`packet_function.call` / `packet_function.async_call`.

The `_executor_supports_concurrent` helper stays — it's just a predicate check.

### D. Sync and async are cleanly separated execution modes

- Sync: `iter_packets()` / `as_table()` / `run()`
- Async: `async_execute(inputs, output)`

They don't populate each other's caches. DB persistence (for Persistent variants) provides
durability that works across both modes.

### E. OperatorNode delegates to operator, PersistentOperatorNode intercepts for storage

Operators are opaque stream transformers — no per-packet hook. `OperatorNode` passes through
directly. `PersistentOperatorNode` uses an intermediate channel + `TaskGroup` to forward
results downstream immediately while collecting them for post-hoc DB storage.

### F. DB operations stay synchronous

The `ArrowDatabaseProtocol` is sync. All DB reads/writes within async methods are sync calls.
Acceptable because DB is typically in-process and fast. Async DB protocol is deferred.

---

## Implementation steps

### Step 1: Add `async_process_packet` to `_FunctionPodBase`

**File:** `src/orcapod/core/function_pod.py`

Add alongside existing `process_packet` (after line 180):

```python
async def async_process_packet(
    self, tag: TagProtocol, packet: PacketProtocol
) -> tuple[TagProtocol, PacketProtocol | None]:
    """Async counterpart of ``process_packet``."""
    return tag, await self.packet_function.async_call(packet)
```

### Step 2: Fix `FunctionPod.async_execute` to use `async_process_packet`

**File:** `src/orcapod/core/function_pod.py`

Change the `process_one` inner function (lines 315-322):

```python
async def process_one(tag: TagProtocol, packet: PacketProtocol) -> None:
    try:
        tag, result_packet = await self.async_process_packet(tag, packet)
        if result_packet is not None:
            await output.send((tag, result_packet))
    finally:
        if sem is not None:
            sem.release()
```

### Step 3: Fix `FunctionPodStream._iter_packets_concurrent` to use `async_process_packet`

**File:** `src/orcapod/core/function_pod.py`

Replace the `_execute_concurrent` call (lines 454-482) with direct `async_process_packet`
routing:

```python
def _iter_packets_concurrent(
    self,
) -> Iterator[tuple[TagProtocol, PacketProtocol]]:
    """Collect remaining inputs, execute concurrently, and yield results in order."""
    input_iter = self._cached_input_iterator

    all_inputs: list[tuple[int, TagProtocol, PacketProtocol]] = []
    to_compute: list[tuple[int, TagProtocol, PacketProtocol]] = []
    for i, (tag, packet) in enumerate(input_iter):
        all_inputs.append((i, tag, packet))
        if i not in self._cached_output_packets:
            to_compute.append((i, tag, packet))
    self._cached_input_iterator = None

    if to_compute:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None:
            # Already in event loop — fall back to sequential sync
            results = [
                self._function_pod.process_packet(tag, pkt)
                for _, tag, pkt in to_compute
            ]
        else:
            # No event loop — run concurrently via asyncio.run
            async def _gather() -> list[tuple[TagProtocol, PacketProtocol | None]]:
                return list(
                    await asyncio.gather(
                        *[
                            self._function_pod.async_process_packet(tag, pkt)
                            for _, tag, pkt in to_compute
                        ]
                    )
                )

            results = asyncio.run(_gather())

        for (i, _, _), (tag, output_packet) in zip(to_compute, results):
            self._cached_output_packets[i] = (tag, output_packet)

    for i, *_ in all_inputs:
        tag, packet = self._cached_output_packets[i]
        if packet is not None:
            yield tag, packet
```

**Note:** The method signature drops the `packet_function` parameter — it no longer needs
it since it routes through `self._function_pod`.

The `iter_packets` method that calls this also needs updating — remove the `pf` argument:

```python
def iter_packets(self) -> Iterator[tuple[TagProtocol, PacketProtocol]]:
    if self.is_stale:
        self.clear_cache()
    if self._cached_input_iterator is not None:
        if _executor_supports_concurrent(self._function_pod.packet_function):
            yield from self._iter_packets_concurrent()
        else:
            yield from self._iter_packets_sequential()
    else:
        for i in range(len(self._cached_output_packets)):
            tag, packet = self._cached_output_packets[i]
            if packet is not None:
                yield tag, packet
```

### Step 4: Fix `FunctionNode._iter_packets_sequential` to use `process_packet`

**File:** `src/orcapod/core/function_pod.py`

Change line 831 from:
```python
output_packet = self._packet_function.call(packet)
self._cached_output_packets[i] = (tag, output_packet)
```
to:
```python
tag, output_packet = self.process_packet(tag, packet)
self._cached_output_packets[i] = (tag, output_packet)
```

### Step 5: Fix `FunctionNode._iter_packets_concurrent` to use `async_process_packet`

**File:** `src/orcapod/core/function_pod.py`

Same transformation as Step 3, but routing through `self` instead of `self._function_pod`:

```python
def _iter_packets_concurrent(
    self,
) -> Iterator[tuple[TagProtocol, PacketProtocol]]:
    """Collect remaining inputs, execute concurrently, and yield results in order."""
    input_iter = self._cached_input_iterator

    all_inputs: list[tuple[int, TagProtocol, PacketProtocol]] = []
    to_compute: list[tuple[int, TagProtocol, PacketProtocol]] = []
    for i, (tag, packet) in enumerate(input_iter):
        all_inputs.append((i, tag, packet))
        if i not in self._cached_output_packets:
            to_compute.append((i, tag, packet))
    self._cached_input_iterator = None

    if to_compute:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop is not None:
            # Already in event loop — fall back to sequential sync
            results = [
                self.process_packet(tag, pkt)
                for _, tag, pkt in to_compute
            ]
        else:
            # No event loop — run concurrently via asyncio.run
            async def _gather() -> list[tuple[TagProtocol, PacketProtocol | None]]:
                return list(
                    await asyncio.gather(
                        *[
                            self.async_process_packet(tag, pkt)
                            for _, tag, pkt in to_compute
                        ]
                    )
                )

            results = asyncio.run(_gather())

        for (i, _, _), (tag, output_packet) in zip(to_compute, results):
            self._cached_output_packets[i] = (tag, output_packet)

    for i, *_ in all_inputs:
        tag, packet = self._cached_output_packets[i]
        if packet is not None:
            yield tag, packet
```

**Critical difference from Step 3:** Uses `self.process_packet` / `self.async_process_packet`
instead of `self._function_pod.*`. This means when `PersistentFunctionNode` inherits this
method, it automatically routes through its overridden `process_packet` /
`async_process_packet` which include cache checks + pipeline record storage.

### Step 6: Remove `_execute_concurrent`

**File:** `src/orcapod/core/function_pod.py`

Delete the `_execute_concurrent` function (lines 52-82). Its logic is now inlined into the
`_iter_packets_concurrent` methods.

### Step 7: Add `process_packet` and `async_process_packet` to `FunctionNode`

**File:** `src/orcapod/core/function_pod.py`

FunctionNode currently has no `process_packet`. Add delegation to the function pod:

```python
def process_packet(
    self, tag: TagProtocol, packet: PacketProtocol
) -> tuple[TagProtocol, PacketProtocol | None]:
    """Process a single packet by delegating to the function pod."""
    return self._function_pod.process_packet(tag, packet)

async def async_process_packet(
    self, tag: TagProtocol, packet: PacketProtocol
) -> tuple[TagProtocol, PacketProtocol | None]:
    """Async counterpart of ``process_packet``."""
    return await self._function_pod.async_process_packet(tag, packet)
```

### Step 8: Add `FunctionNode.async_execute`

**File:** `src/orcapod/core/function_pod.py`

Sequential streaming through `async_process_packet`:

```python
async def async_execute(
    self,
    inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
    output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
) -> None:
    """Streaming async execution — process each packet via async_process_packet."""
    try:
        async for tag, packet in inputs[0]:
            tag, result_packet = await self.async_process_packet(tag, packet)
            if result_packet is not None:
                await output.send((tag, result_packet))
    finally:
        await output.close()
```

### Step 9: Add async cache-aware `async_call` to `CachedPacketFunction`

**File:** `src/orcapod/core/packet_function.py`

Override `async_call` to mirror the sync `call()` logic (lines 508-533):

```python
async def async_call(
    self,
    packet: PacketProtocol,
    *,
    skip_cache_lookup: bool = False,
    skip_cache_insert: bool = False,
) -> PacketProtocol | None:
    """Async counterpart of ``call`` with cache check and recording."""
    output_packet = None
    if not skip_cache_lookup:
        logger.info("Checking for cache...")
        output_packet = self.get_cached_output_for_packet(packet)
        if output_packet is not None:
            logger.info(f"Cache hit for {packet}!")
    if output_packet is None:
        output_packet = await self._packet_function.async_call(packet)
        if output_packet is not None:
            if not skip_cache_insert:
                self.record_packet(packet, output_packet)
            output_packet = output_packet.with_meta_columns(
                **{self.RESULT_COMPUTED_FLAG: True}
            )
    return output_packet
```

### Step 10: Add `async_process_packet` to `PersistentFunctionNode`

**File:** `src/orcapod/core/function_pod.py`

PersistentFunctionNode already has `process_packet` (line 1027-1066) which calls
`self._packet_function.call(packet, skip_cache_*=...)` (where `_packet_function` is a
`CachedPacketFunction`) then `self.add_pipeline_record(...)`. Add the async counterpart:

```python
async def async_process_packet(
    self,
    tag: TagProtocol,
    packet: PacketProtocol,
    skip_cache_lookup: bool = False,
    skip_cache_insert: bool = False,
) -> tuple[TagProtocol, PacketProtocol | None]:
    """Async counterpart of ``process_packet``.

    Uses the CachedPacketFunction's async_call for computation + result caching.
    Pipeline record storage is synchronous (DB protocol is sync).
    """
    output_packet = await self._packet_function.async_call(
        packet,
        skip_cache_lookup=skip_cache_lookup,
        skip_cache_insert=skip_cache_insert,
    )

    if output_packet is not None:
        result_computed = bool(
            output_packet.get_meta_value(
                self._packet_function.RESULT_COMPUTED_FLAG, False
            )
        )
        self.add_pipeline_record(
            tag,
            packet,
            packet_record_id=output_packet.datagram_id,
            computed=result_computed,
        )

    return tag, output_packet
```

### Step 11: Add `PersistentFunctionNode.async_execute` (two-phase)

**File:** `src/orcapod/core/function_pod.py`

Overrides `FunctionNode.async_execute`:

```python
async def async_execute(
    self,
    inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
    output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
) -> None:
    """Two-phase async execution: replay cached, then compute missing."""
    try:
        # Phase 1: emit existing results from DB
        existing = self.get_all_records(columns={"meta": True})
        computed_hashes: set[str] = set()
        if existing is not None and existing.num_rows > 0:
            tag_keys = self._input_stream.keys()[0]
            hash_col = constants.INPUT_PACKET_HASH_COL
            computed_hashes = set(
                cast(list[str], existing.column(hash_col).to_pylist())
            )
            data_table = existing.drop([hash_col])
            existing_stream = ArrowTableStream(data_table, tag_columns=tag_keys)
            for tag, packet in existing_stream.iter_packets():
                await output.send((tag, packet))

        # Phase 2: process packets not already in the DB
        async for tag, packet in inputs[0]:
            input_hash = packet.content_hash().to_string()
            if input_hash in computed_hashes:
                continue
            tag, output_packet = await self.async_process_packet(tag, packet)
            if output_packet is not None:
                await output.send((tag, output_packet))
    finally:
        await output.close()
```

### Step 12: Add `OperatorNode.async_execute`

**File:** `src/orcapod/core/operator_node.py`

Direct pass-through:

```python
async def async_execute(
    self,
    inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
    output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
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
    inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
    output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
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
            for tag, packet in self._cached_output_stream.iter_packets():
                await output.send((tag, packet))
            return  # finally block closes output

        # OFF or LOG: delegate to operator, forward results downstream
        intermediate = Channel[tuple[TagProtocol, PacketProtocol]]()
        collected: list[tuple[TagProtocol, PacketProtocol]] = []

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

- Test that `FunctionPodStream._iter_packets_concurrent` falls back to sequential
  `process_packet` when called inside a running event loop.
- Test that `FunctionNode._iter_packets_concurrent` does the same.

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

TestCachedPacketFunctionAsync
  - test_async_call_cache_miss_computes_and_records
  - test_async_call_cache_hit_returns_cached
  - test_async_call_skip_cache_lookup
  - test_async_call_skip_cache_insert

TestProcessPacketRouting
  - test_function_pod_stream_sequential_uses_process_packet
  - test_function_pod_stream_concurrent_uses_async_process_packet
  - test_function_node_sequential_uses_process_packet
  - test_function_node_concurrent_uses_async_process_packet
  - test_persistent_function_node_concurrent_uses_overridden_async_process_packet
  - test_concurrent_fallback_in_event_loop_uses_sync_process_packet

TestFunctionNodeAsyncExecute
  - test_basic_streaming_matches_sync
  - test_empty_input_closes_cleanly
  - test_none_packets_filtered_out

TestPersistentFunctionNodeAsyncExecute
  - test_no_cache_processes_all_inputs
  - test_phase1_emits_cached_results
  - test_phase2_skips_cached_computes_new
  - test_pipeline_records_created_for_new_packets
  - test_result_cache_populated_for_new_packets

TestOperatorNodeAsyncExecute
  - test_unary_op_delegation (SelectPacketColumns)
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
FunctionPodStream._iter_packets_sequential
  → self._function_pod.process_packet(tag, pkt)       # already correct
    → packet_function.call(pkt)

FunctionNode._iter_packets_sequential
  → self.process_packet(tag, pkt)                      # CHANGED: was _packet_function.call(pkt)
    → self._function_pod.process_packet(tag, pkt)
      → packet_function.call(pkt)

PersistentFunctionNode._iter_packets_sequential (inherited from FunctionNode)
  → self.process_packet(tag, pkt)                      # polymorphism kicks in
    → CachedPacketFunction.call(pkt, skip_cache_*=...) # cache check + compute + record
    → self.add_pipeline_record(...)                     # pipeline DB
```

**Sync concurrent path:**
```
FunctionPodStream._iter_packets_concurrent
  → asyncio.run(gather(
        self._function_pod.async_process_packet(tag, pkt) ...   # CHANGED: was _execute_concurrent
    ))
  OR (if event loop running):
    self._function_pod.process_packet(tag, pkt) ...             # fallback

FunctionNode._iter_packets_concurrent
  → asyncio.run(gather(
        self.async_process_packet(tag, pkt) ...                 # CHANGED: was _execute_concurrent
    ))
  OR (if event loop running):
    self.process_packet(tag, pkt) ...                           # fallback

PersistentFunctionNode._iter_packets_concurrent (inherited from FunctionNode)
  → asyncio.run(gather(
        self.async_process_packet(tag, pkt) ...                 # polymorphism kicks in
          → await CachedPacketFunction.async_call(pkt)          # cache + compute
          → self.add_pipeline_record(...)                       # pipeline DB
    ))
```

**Async execution path:**
```
FunctionPod.async_execute
  → await self.async_process_packet(tag, pkt)          # CHANGED: was packet_function.async_call
    → await packet_function.async_call(pkt)

FunctionNode.async_execute                              # NEW
  → await self.async_process_packet(tag, pkt)
    → await self._function_pod.async_process_packet(tag, pkt)
      → await packet_function.async_call(pkt)

PersistentFunctionNode.async_execute                    # NEW (two-phase)
  Phase 1: emit from DB
  Phase 2:
    → await self.async_process_packet(tag, pkt)         # polymorphic override
      → await CachedPacketFunction.async_call(pkt)      # cache + compute
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
| `src/orcapod/core/packet_function.py` | Add `CachedPacketFunction.async_call` override with cache logic |
| `src/orcapod/core/function_pod.py` | (1) Add `_FunctionPodBase.async_process_packet` |
| | (2) Fix `FunctionPod.async_execute` to use `async_process_packet` |
| | (3) Rewrite `FunctionPodStream._iter_packets_concurrent` — route through `_function_pod.async_process_packet` / `process_packet`, drop `packet_function` param |
| | (4) Update `FunctionPodStream.iter_packets` — remove `pf` arg to `_iter_packets_concurrent` |
| | (5) Fix `FunctionNode._iter_packets_sequential` to use `self.process_packet` |
| | (6) Rewrite `FunctionNode._iter_packets_concurrent` — route through `self.async_process_packet` / `self.process_packet` |
| | (7) Add `FunctionNode.process_packet` + `async_process_packet` (delegate to pod) |
| | (8) Add `FunctionNode.async_execute` |
| | (9) Add `PersistentFunctionNode.async_process_packet` (cache + pipeline records) |
| | (10) Add `PersistentFunctionNode.async_execute` (two-phase) |
| | (11) Remove `_execute_concurrent` module-level helper |
| `src/orcapod/core/operator_node.py` | (1) Add imports |
| | (2) Add `OperatorNode.async_execute` (pass-through) |
| | (3) Extract `PersistentOperatorNode._store_output_stream` |
| | (4) Refactor `PersistentOperatorNode._compute_and_store` |
| | (5) Add `PersistentOperatorNode.async_execute` (TaskGroup + post-hoc storage) |
| `tests/test_core/test_regression_fixes.py` | Rewrite `TestExecuteConcurrentInRunningLoop` — test through classes instead of removed helper |
| `tests/test_channels/test_node_async_execute.py` | New test file |
