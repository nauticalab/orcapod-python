# Plan: Unified `process_packet` / `async_process_packet` + Node `async_execute`

## Goal

Establish `process_packet` and `async_process_packet` as **the** universal per-packet
interface across FunctionPod, FunctionPodStream, FunctionNode, and PersistentFunctionNode.
Add `async_execute` to all four Node classes. Add cache-aware `async_call` to
`CachedPacketFunction`.

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

---

## Design principles

### A. `process_packet` / `async_process_packet` is the single per-packet entry point

Every class in the function pod hierarchy defines these two methods. All iteration and
execution paths go through them — no direct `packet_function.call()` or
`packet_function.async_call()` calls outside of these methods.

```
_FunctionPodBase.process_packet(tag, pkt)        → packet_function.call(pkt)
_FunctionPodBase.async_process_packet(tag, pkt)   → await packet_function.async_call(pkt)

FunctionNode.process_packet(tag, pkt)             → self._function_pod.process_packet(tag, pkt)
FunctionNode.async_process_packet(tag, pkt)       → await self._function_pod.async_process_packet(tag, pkt)

PersistentFunctionNode.process_packet(tag, pkt)   → cache check → pod.process_packet → pipeline record
PersistentFunctionNode.async_process_packet(tag, pkt) → cache check → await pod.async_process_packet → pipeline record
```

The cache check and pipeline record are sync DB operations in **both** the sync and async
variants. Only the actual computation differs (sync `call` vs async `async_call`).

### B. Sync and async are cleanly separated execution modes

- Sync: `iter_packets()` / `as_table()` / `run()`
- Async: `async_execute(inputs, output)`

They don't populate each other's caches. The DB persistence layer (for Persistent variants)
provides durability that works across both modes.

### C. OperatorNode delegates to operator, PersistentOperatorNode intercepts for storage

Operators are opaque stream transformers — no per-packet hook. The Node can only observe
the complete output. `OperatorNode` passes through directly. `PersistentOperatorNode` uses
an intermediate channel + `TaskGroup` to forward results downstream immediately while
collecting them for post-hoc DB storage.

### D. DB operations stay synchronous

The `ArrowDatabaseProtocol` is sync. All DB reads/writes within async methods are sync calls.
This is acceptable because:
1. DB is typically in-process (InMemoryDatabase, DeltaLake local files)
2. Fast I/O compared to the actual computation
3. Async DB protocol is deferred to future work

---

## Implementation steps

### Step 1: Add `async_process_packet` to `_FunctionPodBase`

**File:** `src/orcapod/core/function_pod.py`

Add alongside existing `process_packet` (line 167):

```python
# Existing (line 167-180):
def process_packet(
    self, tag: TagProtocol, packet: PacketProtocol
) -> tuple[TagProtocol, PacketProtocol | None]:
    """Process a single packet using the pod's packet function."""
    return tag, self.packet_function.call(packet)

# New:
async def async_process_packet(
    self, tag: TagProtocol, packet: PacketProtocol
) -> tuple[TagProtocol, PacketProtocol | None]:
    """Async counterpart of ``process_packet``."""
    return tag, await self.packet_function.async_call(packet)
```

### Step 2: Fix `FunctionPod.async_execute` to use `async_process_packet`

**File:** `src/orcapod/core/function_pod.py`

Change line 317 from:
```python
result_packet = await self.packet_function.async_call(packet)
```
to:
```python
tag, result_packet = await self.async_process_packet(tag, packet)
```

And adjust the surrounding code — we no longer check `result_packet is not None` separately
since `async_process_packet` returns the tuple:

```python
async def async_execute(
    self,
    inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
    output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
    pipeline_config: PipelineConfig | None = None,
) -> None:
    """Streaming async execution with per-packet concurrency control."""
    try:
        pipeline_config = pipeline_config or PipelineConfig()
        max_concurrency = resolve_concurrency(self._node_config, pipeline_config)
        sem = asyncio.Semaphore(max_concurrency) if max_concurrency is not None else None

        async def process_one(tag: TagProtocol, packet: PacketProtocol) -> None:
            try:
                tag, result_packet = await self.async_process_packet(tag, packet)
                if result_packet is not None:
                    await output.send((tag, result_packet))
            finally:
                if sem is not None:
                    sem.release()

        async with asyncio.TaskGroup() as tg:
            async for tag, packet in inputs[0]:
                if sem is not None:
                    await sem.acquire()
                tg.create_task(process_one(tag, packet))
    finally:
        await output.close()
```

### Step 3: Fix `FunctionPodStream._iter_packets_concurrent` to use `process_packet`

**File:** `src/orcapod/core/function_pod.py`

Currently (line 454-482) it calls `_execute_concurrent(packet_function, packets)` which
directly calls `packet_function.async_call`. Change to route through `process_packet`.

The concurrent path collects packets then submits them. We need to adapt
`_execute_concurrent` to work with `process_packet`, or restructure the concurrent path.

**Option:** Change `_iter_packets_concurrent` to call `self._function_pod.process_packet`
for each uncached packet. The concurrency comes from the executor, not from us — so we can
keep it sequential through `process_packet` and let the executor handle batching.

Actually, looking more carefully: `_iter_packets_concurrent` is only used when
`_executor_supports_concurrent(pf)` is True — meaning the executor wants to batch-submit.
The `_execute_concurrent` helper calls `asyncio.run(gather(pf.async_call(...)))`.

To route through `process_packet` while preserving concurrency, we'd need a batch version
of `process_packet`. That's a bigger change. **For now, keep the concurrent path as-is
in FunctionPodStream** — it's a specialized optimization that only triggers with specific
executors. The sequential path already uses `process_packet`.

**Revisit this as a follow-up.** Mark it in the code with a TODO.

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

### Step 5: Fix `FunctionNode._iter_packets_concurrent` to use `process_packet`

**File:** `src/orcapod/core/function_pod.py`

Same issue as Step 3 — the concurrent path (line 837-861) calls `_execute_concurrent`
directly on the packet function. Same resolution: **keep as-is for now, add TODO**.

The concurrent path on FunctionNode is analogous to FunctionPodStream's concurrent path.
Both are executor-driven optimizations that bypass `process_packet`. Fixing them requires
a batch `process_packet` API which is out of scope.

### Step 6: Add `process_packet` and `async_process_packet` to `FunctionNode`

**File:** `src/orcapod/core/function_pod.py`

FunctionNode currently has no `process_packet`. Add it as delegation to the function pod:

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

### Step 7: Add `FunctionNode.async_execute`

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

### Step 8: Add async cache-aware `async_call` to `CachedPacketFunction`

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

`get_cached_output_for_packet` and `record_packet` remain sync — DB protocol is sync.

### Step 9: Override `process_packet` / `async_process_packet` on `PersistentFunctionNode`

**File:** `src/orcapod/core/function_pod.py`

PersistentFunctionNode already has `process_packet` (line 1027-1066). It calls
`self._packet_function.call(packet, skip_cache_lookup=..., skip_cache_insert=...)` and
then `self.add_pipeline_record(...)`.

**Note:** PersistentFunctionNode's `self._packet_function` is a `CachedPacketFunction`
(set in `__init__` at line 997). So calling `self._packet_function.call()` triggers the
cache-aware sync path, and calling `await self._packet_function.async_call()` will trigger
our new cache-aware async path from Step 8.

The existing `process_packet` is correct as-is. Add `async_process_packet`:

```python
async def async_process_packet(
    self,
    tag: TagProtocol,
    packet: PacketProtocol,
    skip_cache_lookup: bool = False,
    skip_cache_insert: bool = False,
) -> tuple[TagProtocol, PacketProtocol | None]:
    """Async counterpart of ``process_packet``.

    Uses the packet function's async_call for computation.
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

### Step 10: Add `PersistentFunctionNode.async_execute` (two-phase)

**File:** `src/orcapod/core/function_pod.py`

Overrides `FunctionNode.async_execute` with the two-phase pattern:

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

**Data flow for Phase 2:**
```
input channel → async_process_packet
                  → CachedPacketFunction.async_call
                      → get_cached_output_for_packet (sync DB read)
                      → if miss: await inner_pf.async_call(packet) (async computation)
                      → record_packet (sync DB write to result store)
                  → add_pipeline_record (sync DB write to pipeline store)
                → output channel
```

### Step 11: Add `OperatorNode.async_execute`

**File:** `src/orcapod/core/operator_node.py`

Direct pass-through delegation:

```python
async def async_execute(
    self,
    inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
    output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
) -> None:
    """Delegate to operator's async_execute."""
    await self._operator.async_execute(inputs, output)
```

The operator's `async_execute` already handles closing `output`. No intermediate channel
needed for the non-persistent case.

### Step 12: Extract `_store_output_stream` from `PersistentOperatorNode._compute_and_store`

**File:** `src/orcapod/core/operator_node.py`

Extract the DB-write portion so both sync and async paths can use it:

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

Refactor `_compute_and_store` to use it:

```python
def _compute_and_store(self) -> None:
    """Compute operator output, optionally store in DB."""
    self._cached_output_stream = self._operator.process(*self._input_streams)

    if self._cache_mode == CacheMode.OFF:
        self._update_modified_time()
        return

    self._store_output_stream(self._cached_output_stream)
    self._update_modified_time()
```

### Step 13: Add `PersistentOperatorNode.async_execute`

**File:** `src/orcapod/core/operator_node.py`

Uses TaskGroup for concurrent forwarding + collection, then post-hoc DB storage:

```python
async def async_execute(
    self,
    inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
    output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
) -> None:
    """Async execution with cache mode handling.

    REPLAY: emit from DB.
    OFF: delegate to operator, forward results.
    LOG: delegate to operator, forward results, then store in DB.
    """
    try:
        if self._cache_mode == CacheMode.REPLAY:
            self._replay_from_cache()
            assert self._cached_output_stream is not None
            for tag, packet in self._cached_output_stream.iter_packets():
                await output.send((tag, packet))
            return  # finally block closes output

        # OFF or LOG: delegate to operator, forward results to downstream
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
        # Store if LOG mode (sync DB write — post-hoc, doesn't block pipeline)
        if self._cache_mode == CacheMode.LOG and collected:
            stream = StaticOutputPod._materialize_to_stream(collected)
            self._cached_output_stream = stream
            self._store_output_stream(stream)

        self._update_modified_time()
    finally:
        await output.close()
```

**Execution timeline:**
```
Time →

[TaskGroup starts]
  operator produces item 1 → forward sends item 1 downstream, appends to collected
  operator produces item 2 → forward sends item 2 downstream, appends to collected
  ...
  operator finishes, closes intermediate
  forward drains, exits
[TaskGroup completes]

# Downstream already has all items at this point
# Now sync DB write (only if LOG mode)
_store_output_stream(materialize(collected))
```

### Step 14: Add imports

**`src/orcapod/core/operator_node.py`** — add:
```python
import asyncio
from collections.abc import Sequence

from orcapod.channels import Channel, ReadableChannel, WritableChannel
from orcapod.core.static_output_pod import StaticOutputPod
```

**`src/orcapod/core/function_pod.py`** — already has all needed imports.

### Step 15: Tests

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

TestFunctionNodeAsyncExecute
  - test_basic_streaming_matches_sync
  - test_empty_input_closes_cleanly
  - test_none_packets_filtered_out
  - test_uses_process_packet (verify delegation to pod)

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

### Step 16: Run tests

```bash
uv run pytest tests/ -x
```

---

## Summary of all changes

### Files modified

| File | Changes |
|------|---------|
| `src/orcapod/core/packet_function.py` | Add `CachedPacketFunction.async_call` override with cache logic |
| `src/orcapod/core/function_pod.py` | (1) Add `_FunctionPodBase.async_process_packet` |
| | (2) Fix `FunctionPod.async_execute` to use `async_process_packet` |
| | (3) Add TODO to `FunctionPodStream._iter_packets_concurrent` |
| | (4) Fix `FunctionNode._iter_packets_sequential` to use `process_packet` |
| | (5) Add TODO to `FunctionNode._iter_packets_concurrent` |
| | (6) Add `FunctionNode.process_packet` + `async_process_packet` (delegate to pod) |
| | (7) Add `FunctionNode.async_execute` |
| | (8) Add `PersistentFunctionNode.async_process_packet` (cache + pipeline records) |
| | (9) Add `PersistentFunctionNode.async_execute` (two-phase) |
| `src/orcapod/core/operator_node.py` | (1) Add imports |
| | (2) Add `OperatorNode.async_execute` (pass-through) |
| | (3) Extract `PersistentOperatorNode._store_output_stream` |
| | (4) Refactor `PersistentOperatorNode._compute_and_store` to use it |
| | (5) Add `PersistentOperatorNode.async_execute` (TaskGroup + post-hoc storage) |
| `tests/test_channels/test_node_async_execute.py` | New test file |

### Files NOT modified (intentional)

| File | Reason |
|------|--------|
| `src/orcapod/protocols/core_protocols/async_executable.py` | Protocol already covers the needed interface |
| `src/orcapod/channels.py` | No changes needed |
| `src/orcapod/core/operators/base.py` | Operators already have async_execute |
| `src/orcapod/core/static_output_pod.py` | Already has async_execute + _materialize_to_stream |

### Call chain after changes

**Sync path (unchanged behavior):**
```
FunctionPodStream._iter_packets_sequential
  → FunctionPod.process_packet(tag, pkt)
    → packet_function.call(pkt)

FunctionNode._iter_packets_sequential
  → FunctionNode.process_packet(tag, pkt)        # NEW: was _packet_function.call(pkt)
    → FunctionPod.process_packet(tag, pkt)
      → packet_function.call(pkt)

PersistentFunctionNode.iter_packets (Phase 2)
  → PersistentFunctionNode.process_packet(tag, pkt)  # unchanged
    → CachedPacketFunction.call(pkt)                  # cache check + compute + record
    → add_pipeline_record(...)
```

**Async path (new):**
```
FunctionPod.async_execute
  → FunctionPod.async_process_packet(tag, pkt)    # NEW: was packet_function.async_call(pkt)
    → await packet_function.async_call(pkt)

FunctionNode.async_execute                         # NEW
  → await FunctionNode.async_process_packet(tag, pkt)
    → await FunctionPod.async_process_packet(tag, pkt)
      → await packet_function.async_call(pkt)

PersistentFunctionNode.async_execute               # NEW
  Phase 1: emit from DB
  Phase 2:
    → await PersistentFunctionNode.async_process_packet(tag, pkt)
      → await CachedPacketFunction.async_call(pkt)  # cache check + compute + record
      → add_pipeline_record(...)                     # sync DB write

OperatorNode.async_execute                         # NEW
  → await operator.async_execute(inputs, output)   # direct delegation

PersistentOperatorNode.async_execute               # NEW
  REPLAY: emit from DB
  OFF/LOG:
    → TaskGroup:
        operator.async_execute(inputs, intermediate.writer)
        forward(intermediate.reader → output)
    → if LOG: _store_output_stream(materialize(collected))  # sync DB write
```

### Known deferred items (TODOs)

1. `FunctionPodStream._iter_packets_concurrent` — still bypasses `process_packet` for
   executor-driven batch concurrency. Needs batch `process_packet` API to fix.
2. `FunctionNode._iter_packets_concurrent` — same issue.
3. Async DB protocol — all DB operations are sync within async methods. When the DB
   protocol gains async support, these can be converted.
