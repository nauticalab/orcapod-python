# Plan: Add `async_execute` to Node classes

## What exists today

| Class | Has `async_execute`? | Inherits from |
|-------|---------------------|---------------|
| `StaticOutputPod` | Yes (barrier-mode default) | `TraceableBase` |
| `UnaryOperator` | Yes (barrier-mode override) | `StaticOutputPod` |
| `BinaryOperator` | Yes (barrier-mode override) | `StaticOutputPod` |
| `FunctionPod` | Yes (streaming, per-packet) | `_FunctionPodBase` → `TraceableBase` |
| `FunctionNode` | **No** | `StreamBase` |
| `PersistentFunctionNode` | **No** | `FunctionNode` |
| `OperatorNode` | **No** | `StreamBase` |
| `PersistentOperatorNode` | **No** | `OperatorNode` |

Pods own the computation logic. Nodes wrap a Pod + input streams and add:
- In-memory result caching (`_cached_output_packets`, `_cached_output_stream`)
- DB persistence (Persistent variants only)
- Two-phase iteration for PersistentFunctionNode (replay cached, then compute missing)
- Cache modes (OFF/LOG/REPLAY) for PersistentOperatorNode

**Channel infrastructure** already exists in `src/orcapod/channels.py`:
`Channel`, `ReadableChannel`, `WritableChannel`, `BroadcastChannel`.

**`AsyncExecutableProtocol`** exists in `protocols/core_protocols/async_executable.py`.

## Design decisions

### 1. Async and sync caches are independent

The sync path uses `_cached_output_packets` / `_cached_output_stream` / `_cached_output_table`.
The async path will **not** populate these caches. Rationale: async execution is channel-based
and push-oriented — items flow through and are gone. There's no meaningful "cache" for
re-iteration. The DB persistence layer (for Persistent variants) is what provides durability
across both modes.

### 2. OperatorNode delegates via concurrent TaskGroup, not sequential await

The naive approach — `await operator.async_execute(inputs, intermediate.writer)` then
read from `intermediate.reader` — works because the operator closes the writer when done,
and then we'd drain the reader. But it defeats streaming: all items buffer before forwarding
starts. Instead, we use `asyncio.TaskGroup` to run the operator and forwarding concurrently:

```
TaskGroup:
  task 1: operator.async_execute(inputs, intermediate.writer)  # produces
  task 2: forward intermediate.reader → output.writer           # consumes
```

This preserves backpressure and streaming semantics.

### 3. PersistentFunctionNode uses async_call for computation, sync for DB bookkeeping

The async path needs to call `await self._packet_function.async_call(packet)` for the
actual computation. But the pipeline record storage (`add_pipeline_record`) is pure DB I/O
(fast, in-process) and stays sync. This mirrors how the sync path works — `process_packet`
calls the sync `call()` then does sync DB writes.

We'll add an `async_process_packet` method that mirrors `process_packet` but uses `async_call`.

### 4. CachedPacketFunction needs async-aware cache logic

Currently `CachedPacketFunction.async_call` is inherited from `PacketFunctionWrapper` and
just delegates to the wrapped function — **completely bypassing the cache**. We must override
it to check the cache, call the inner function's `async_call` on miss, and record the result.

The cache lookup (`get_cached_output_for_packet`) and recording (`record_packet`) are
sync DB operations. Since the DB protocol is sync and these are typically fast in-process
operations, we keep them sync within the async method. The only `await` is on the actual
packet function computation.

### 5. FunctionNode accepts optional PipelineConfig for concurrency control

FunctionPod gets `NodeConfig` directly. FunctionNode wraps a FunctionPod and can access its
`node_config` to resolve concurrency. We'll pass `pipeline_config` as an optional parameter
to `async_execute` (matching FunctionPod's signature).

### 6. PersistentOperatorNode extracts `_store_output_stream` from `_compute_and_store`

`_compute_and_store` currently does both computation and storage. We extract the storage
portion into `_store_output_stream(stream)` so async can reuse it after collecting results.

---

## Implementation steps

### Step 1: `CachedPacketFunction.async_call` with cache support

**File:** `src/orcapod/core/packet_function.py`

Override `async_call` on `CachedPacketFunction`:

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

**Note:** `get_cached_output_for_packet` and `record_packet` remain sync. The DB protocol is
sync and typically in-process. The `await` is only on the expensive computation.

### Step 2: `FunctionNode.async_execute`

**File:** `src/orcapod/core/function_pod.py`

FunctionNode processes packets through its `_packet_function` (which is a plain
`PacketFunctionProtocol` — NOT cached). The async path mirrors the sync `iter_packets`:

```python
async def async_execute(
    self,
    inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
    output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
    pipeline_config: PipelineConfig | None = None,
) -> None:
    """Streaming async execution — process each packet independently."""
    try:
        async for tag, packet in inputs[0]:
            result_packet = await self._packet_function.async_call(packet)
            if result_packet is not None:
                await output.send((tag, result_packet))
    finally:
        await output.close()
```

**No concurrency control at the FunctionNode level** — FunctionNode doesn't own a
`NodeConfig`. If the user needs concurrency control, they use `FunctionPod.async_execute`
directly (which has the semaphore). FunctionNode is sequential by nature (it preserves
ordering for its sync cache). In async mode, sequential is fine as a starting point.

**No sync cache population** — per design decision #1.

### Step 3: `PersistentFunctionNode.async_execute` (two-phase)

**File:** `src/orcapod/core/function_pod.py`

Add `async_process_packet` that mirrors `process_packet` but uses `async_call`:

```python
async def async_process_packet(
    self,
    tag: TagProtocol,
    packet: PacketProtocol,
    skip_cache_lookup: bool = False,
    skip_cache_insert: bool = False,
) -> tuple[TagProtocol, PacketProtocol | None]:
    """Async counterpart of ``process_packet``."""
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
            tag, packet,
            packet_record_id=output_packet.datagram_id,
            computed=result_computed,
        )
    return tag, output_packet
```

Then `async_execute`:

```python
async def async_execute(
    self,
    inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
    output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
    pipeline_config: PipelineConfig | None = None,
) -> None:
    """Two-phase async execution: replay cached, then compute missing."""
    try:
        # Phase 1: emit cached results from DB
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

        # Phase 2: process packets not in the cache
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

**Why `async_process_packet` instead of sync `process_packet`?** Because `process_packet`
calls `self._packet_function.call()` which is synchronous and could be expensive (the whole
point of async is to not block on computation). `async_process_packet` uses `async_call`
which runs the computation in a thread pool or natively async.

**Note:** `self._packet_function` on PersistentFunctionNode is a `CachedPacketFunction`
(set in `__init__`). So `async_call` will use our new cache-aware override from Step 1.

### Step 4: `OperatorNode.async_execute`

**File:** `src/orcapod/core/operator_node.py`

Uses TaskGroup for concurrent production/forwarding:

```python
async def async_execute(
    self,
    inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
    output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
) -> None:
    """Delegate to operator's async_execute, forwarding results."""
    try:
        intermediate = Channel[tuple[TagProtocol, PacketProtocol]]()

        async def forward() -> None:
            async for item in intermediate.reader:
                await output.send(item)

        async with asyncio.TaskGroup() as tg:
            tg.create_task(
                self._operator.async_execute(inputs, intermediate.writer)
            )
            tg.create_task(forward())
    finally:
        await output.close()
```

**Why an intermediate channel instead of passing `output` directly?**
Because the Node layer needs to intercept/observe the results. For base `OperatorNode` the
forwarding is trivial (no transformation). But having the pattern established means
`PersistentOperatorNode` can override with collection + DB storage.

Actually, for base `OperatorNode` the simplest correct implementation is to just pass through:

```python
async def async_execute(
    self,
    inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
    output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
) -> None:
    """Delegate to operator's async_execute."""
    await self._operator.async_execute(inputs, output)
```

This is simpler and avoids the intermediate channel overhead. The operator already closes
the output channel. PersistentOperatorNode overrides this with the intermediate pattern.

### Step 5: `PersistentOperatorNode.async_execute`

**File:** `src/orcapod/core/operator_node.py`

First, extract DB storage from `_compute_and_store`:

```python
def _store_output_stream(self, stream: StreamProtocol) -> None:
    """Store the output stream's data in the pipeline database."""
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
        0, self.HASH_COLUMN_NAME,
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
    self._cached_output_stream = self._operator.process(*self._input_streams)
    if self._cache_mode == CacheMode.OFF:
        self._update_modified_time()
        return
    self._store_output_stream(self._cached_output_stream)
    self._update_modified_time()
```

Then `async_execute`:

```python
async def async_execute(
    self,
    inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
    output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
) -> None:
    try:
        if self._cache_mode == CacheMode.REPLAY:
            self._replay_from_cache()
            assert self._cached_output_stream is not None
            for tag, packet in self._cached_output_stream.iter_packets():
                await output.send((tag, packet))
            return

        # OFF or LOG: delegate to operator, collect results
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

        # Store if LOG mode
        if self._cache_mode == CacheMode.LOG and collected:
            stream = StaticOutputPod._materialize_to_stream(collected)
            self._cached_output_stream = stream
            self._store_output_stream(stream)

        self._update_modified_time()
    finally:
        await output.close()
```

**Important subtlety:** The `finally: await output.close()` must only run if we didn't
already return from the REPLAY branch (which doesn't close output). Actually, wait — in the
REPLAY branch we return early, but `finally` still runs. We need to track whether we've
already closed the output. Better pattern: always close in `finally`, but don't close in
REPLAY's return path. Since REPLAY emits via `output.send()` but doesn't close, the
`finally` block handles closing. This is correct.

Actually, there's a problem: in the OFF/LOG branch, the `forward()` task sends items
through `output`, and then the `finally` block closes `output`. But if the TaskGroup raises
an exception, `finally` still runs and closes `output`. This is the correct behavior.

### Step 6: Add imports to both files

**`src/orcapod/core/function_pod.py`** — already has `asyncio`, `Sequence`,
`ReadableChannel`, `WritableChannel`. No new imports needed.

**`src/orcapod/core/operator_node.py`** — needs:
```python
import asyncio
from collections.abc import Sequence
from orcapod.channels import Channel, ReadableChannel, WritableChannel
from orcapod.core.static_output_pod import StaticOutputPod  # for _materialize_to_stream
```

### Step 7: Tests

**File:** `tests/test_channels/test_node_async_execute.py` (new file)

```
TestProtocolConformance
  - test_function_node_satisfies_protocol
  - test_persistent_function_node_satisfies_protocol
  - test_operator_node_satisfies_protocol
  - test_persistent_operator_node_satisfies_protocol

TestCachedPacketFunctionAsync
  - test_async_call_cache_miss_computes_and_records
  - test_async_call_cache_hit_returns_cached
  - test_async_call_skip_cache_flags

TestFunctionNodeAsyncExecute
  - test_basic_streaming (results match sync iter_packets)
  - test_empty_input
  - test_none_filtered_packets

TestPersistentFunctionNodeAsyncExecute
  - test_phase1_emits_cached_results
  - test_phase2_processes_missing_inputs
  - test_full_two_phase (some cached, some new)
  - test_db_records_created

TestOperatorNodeAsyncExecute
  - test_unary_op_delegation (e.g. SelectPacketColumns)
  - test_binary_op_delegation (e.g. SemiJoin)
  - test_nary_op_delegation (Join)

TestPersistentOperatorNodeAsyncExecute
  - test_off_mode_no_db_writes
  - test_log_mode_stores_results
  - test_replay_mode_reads_from_db
  - test_replay_empty_db_returns_empty
```

### Step 8: Run full test suite

```bash
uv run pytest tests/ -x
```

---

## Files modified

| File | Changes |
|------|---------|
| `src/orcapod/core/packet_function.py` | Add `CachedPacketFunction.async_call` override |
| `src/orcapod/core/function_pod.py` | Add `FunctionNode.async_execute`, `PersistentFunctionNode.async_execute` + `async_process_packet` |
| `src/orcapod/core/operator_node.py` | Add imports, `OperatorNode.async_execute`, `PersistentOperatorNode.async_execute` + `_store_output_stream`, refactor `_compute_and_store` |
| `tests/test_channels/test_node_async_execute.py` | New test file |

## Risk assessment

- **DB protocol is sync** — All DB operations (`get_cached_output_for_packet`,
  `record_packet`, `add_pipeline_record`, `get_all_records`) are sync calls inside
  async methods. This is acceptable because:
  1. DB is typically in-process (InMemoryDatabase, DeltaLake local files)
  2. These are fast I/O operations compared to the actual computation
  3. The async DB protocol question is deferred to future work
  4. If needed later, these can be wrapped in `loop.run_in_executor`

- **No sync cache population** — Async execution doesn't populate the sync iteration cache.
  This means calling `iter_packets()` after `async_execute()` would recompute. This is
  intentional: the two modes are independent.

- **PersistentFunctionNode Phase 1 timing** — Phase 1 emits cached results before consuming
  any input channel items. If a downstream consumer starts processing these while Phase 2
  hasn't started yet, that's fine — channels handle backpressure. But it means the output
  stream interleaves cached and freshly-computed results. This matches the sync behavior.
