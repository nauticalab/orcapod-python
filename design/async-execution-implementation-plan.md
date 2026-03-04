# Async Execution System — Implementation Plan

**Design doc:** `design/async-execution-system.md`

---

## Phase 1: Foundation (Channels, Protocols, Config)

No existing code is modified. All new files.

### Step 1.1 — Channel primitives

**New file:** `src/orcapod/core/execution/channels.py`

- `Channel[T]` — bounded async queue with close/done signaling
- `ReadableChannel[T]` — consumer side: `receive()`, `__aiter__`, `collect()`
- `WritableChannel[T]` — producer side: `send()`, `close()`
- `BroadcastChannel[T]` — fan-out: one writer, multiple independent readers
- `ChannelClosed` exception
- `create_channel(buffer_size: int) -> Channel`

**Tests:** `tests/test_core/test_execution/test_channels.py`
- Single producer / single consumer
- Backpressure (full buffer blocks send)
- Close semantics (receive after close drains then raises)
- Broadcast (multiple readers get all items)
- Cancellation safety

### Step 1.2 — Async execution protocol

**New file:** `src/orcapod/protocols/core_protocols/async_execution.py`

- `AsyncExecutableProtocol` — single `async_execute(inputs, output)` method
- `NodeConfigProtocol` — `max_concurrency` property

**Modify:** `src/orcapod/protocols/core_protocols/__init__.py`
- Export new protocol

### Step 1.3 — Configuration types

**New file:** `src/orcapod/core/execution/config.py`

- `ExecutorType` enum: `SYNCHRONOUS`, `ASYNC_CHANNELS`
- `PipelineConfig` frozen dataclass: `executor`, `channel_buffer_size`, `default_max_concurrency`
- `NodeConfig` frozen dataclass: `max_concurrency`
- `resolve_concurrency(node_config, pipeline_config) -> int | None`

**Tests:** `tests/test_core/test_execution/test_config.py`
- NodeConfig overrides PipelineConfig default
- None means unlimited

### Step 1.4 — Execution module init

**New file:** `src/orcapod/core/execution/__init__.py`

- Re-export public API: `Channel`, `ReadableChannel`, `WritableChannel`,
  `PipelineConfig`, `NodeConfig`, `ExecutorType`

---

## Phase 2: Default `async_execute` on Base Classes

Add default barrier-mode `async_execute` to every base class. No behavioral change to existing
sync execution — this just makes every node async-capable.

### Step 2.1 — Helper: materialize rows to stream

**New file:** `src/orcapod/core/execution/materialization.py`

- `materialize_to_stream(rows: list[tuple[TagProtocol, PacketProtocol]]) -> ArrowTableStream`
  — converts a list of (tag, packet) pairs back into an ArrowTableStream
- `stream_to_rows(stream: StreamProtocol) -> list[tuple[TagProtocol, PacketProtocol]]`
  — the inverse (thin wrapper around `iter_packets`)

**Tests:** `tests/test_core/test_execution/test_materialization.py`
- Round-trip: stream → rows → stream preserves schema and data
- Empty stream round-trip

### Step 2.2 — Default `async_execute` on `StaticOutputPod`

**Modify:** `src/orcapod/core/static_output_pod.py`

- Add `async_execute(self, inputs, output)` method to `StaticOutputPod`:
  - Collects all input channels
  - Materializes to streams
  - Calls `self.static_process(*streams)`
  - Emits results to output channel
  - Closes output

This gives ALL operators (Unary, Binary, NonZeroInput) a working async_execute by default.

**Tests:** `tests/test_core/test_execution/test_barrier_default.py`
- Run a unary operator (e.g., Batch) through async_execute, compare output to static_process
- Run a binary operator (e.g., MergeJoin) through async_execute
- Run a multi-input operator (e.g., Join) through async_execute
- All should produce identical results to sync mode

### Step 2.3 — `async_execute` on `_FunctionPodBase`

**Modify:** `src/orcapod/core/function_pod.py`

- Add `async_execute` to `_FunctionPodBase` (barrier mode by default)
- Add `async_execute` to `FunctionPod` (streaming mode with semaphore)
- Add `async_execute` to `FunctionNode` (streaming with cache check + semaphore)
- Add `node_config` property (defaults to `NodeConfig()`)

**Tests:** `tests/test_core/test_execution/test_function_pod_async.py`
- FunctionPod streaming produces same results as sync
- FunctionNode with cache hits emits without semaphore
- max_concurrency=1 preserves ordering
- max_concurrency=N allows N concurrent invocations

### Step 2.4 — `async_execute` on source nodes

**Modify:** `src/orcapod/core/tracker.py` (SourceNode)

- Add `async_execute` to `SourceNode`: iterates `self.stream.iter_packets()`, sends to output
- No input channels consumed

**Tests:** `tests/test_core/test_execution/test_source_async.py`
- Source pushes all rows to output channel
- Empty source closes immediately

---

## Phase 3: Orchestrator

### Step 3.1 — DAG compilation for async execution

**New file:** `src/orcapod/core/execution/dag.py`

- `CompiledDAG` — nodes, edges, topological order, terminal node
- `compile_for_async(tracker: GraphTracker) -> CompiledDAG`
  — takes an existing compiled GraphTracker and produces the async DAG structure
  — identifies fan-out points (node output feeds multiple downstreams) for broadcast channels

**Tests:** `tests/test_core/test_execution/test_dag.py`
- Linear pipeline: Source → Op → FunctionPod
- Diamond: Source → [Op1, Op2] → Join
- Fan-out detection

### Step 3.2 — Async pipeline orchestrator

**New file:** `src/orcapod/core/execution/orchestrator.py`

- `AsyncPipelineOrchestrator`
  - `run(graph, config) -> StreamProtocol` — entry point, calls `asyncio.run`
  - `_run_async(graph, config)` — creates channels, launches tasks, collects result
  - Error propagation via TaskGroup
  - Timeout support (optional)

**Tests:** `tests/test_core/test_execution/test_orchestrator.py`
- End-to-end: Source → Filter → FunctionPod via async orchestrator
- End-to-end: two Sources → Join → Map via async orchestrator
- Compare results to synchronous execution
- Error in one node cancels all others
- Backpressure: slow consumer throttles producer

---

## Phase 4: Streaming Overrides for Concrete Operators

Each step is independent — can be done in any order or in parallel.

### Step 4.1 — Streaming column selection operators

**Modify:** `src/orcapod/core/operators/column_selection.py`

- Override `async_execute` on `SelectTagColumns`, `SelectPacketColumns`,
  `DropTagColumns`, `DropPacketColumns`
- Each: iterate input, project/drop columns per row, emit

**Tests:** `tests/test_core/test_execution/test_streaming_operators.py`
- Compare streaming async output to sync output for each operator
- Verify row-by-row emission (no buffering)

### Step 4.2 — Streaming mappers

**Modify:** `src/orcapod/core/operators/mappers.py`

- Override `async_execute` on `MapTags`, `MapPackets`
- Each: iterate input, rename columns per row, emit

**Tests:** added to `test_streaming_operators.py`

### Step 4.3 — Streaming filter

**Modify:** `src/orcapod/core/operators/filters.py`

- Override `async_execute` on `PolarsFilter`
- Evaluate predicate per row, emit if passes

**Tests:** added to `test_streaming_operators.py`

### Step 4.4 — Incremental Join

**Modify:** `src/orcapod/core/operators/join.py`

- Override `async_execute` with symmetric hash join
- Concurrent consumption of all inputs via TaskGroup
- Per-row index probing and immediate emission
- System tag extension logic (reuse existing `_extend_system_tag_columns` logic)

**Tests:** `tests/test_core/test_execution/test_incremental_join.py`
- Same result set as sync join (order may differ, compare as sets)
- Interleaved arrival from multiple inputs
- Single-input join (degenerates to pass-through)

### Step 4.5 — Incremental MergeJoin

**Modify:** `src/orcapod/core/operators/merge_join.py`

- Override `async_execute` with symmetric hash join + list merge for colliding columns

**Tests:** `tests/test_core/test_execution/test_incremental_merge_join.py`

### Step 4.6 — Incremental SemiJoin

**Modify:** `src/orcapod/core/operators/semijoin.py`

- Override `async_execute`: buffer right side fully, then stream left

**Tests:** `tests/test_core/test_execution/test_incremental_semijoin.py`

---

## Phase 5: Integration and Wiring

### Step 5.1 — Pipeline-level API

**Determine integration point:** How does a user trigger async execution?

Option A — `GraphTracker` gains a `run(config)` method:
```python
with GraphTracker() as tracker:
    result = source | filter_op | func_pod
tracker.run(PipelineConfig(executor=ExecutorType.ASYNC_CHANNELS))
```

Option B — A top-level `run_pipeline` function:
```python
result = run_pipeline(terminal_stream, config=PipelineConfig(...))
```

The exact API will be determined during implementation. The orchestrator internals are
independent of this choice.

### Step 5.2 — NodeConfig attachment

Allow `NodeConfig` to be attached to operators/function pods:

```python
func_pod = FunctionPod(my_func, node_config=NodeConfig(max_concurrency=4))
filter_op = PolarsFilter(predicate, node_config=NodeConfig(max_concurrency=None))
```

**Modify:** `StaticOutputPod.__init__`, `_FunctionPodBase.__init__`
- Accept optional `node_config: NodeConfig` parameter
- Default: `NodeConfig()` (inherit pipeline default)

### Step 5.3 — End-to-end integration tests

**New file:** `tests/test_core/test_execution/test_integration.py`

- Full pipeline: Source → Filter → FunctionPod → Join → Map
  - Run sync, run async, compare results
- Pipeline with mixed strategies: streaming filter + barrier batch + streaming map
- Pipeline with database-backed FunctionNode
- Concurrency behavior: verify max_concurrency limits are respected

---

## Implementation Order and Dependencies

```
Phase 1 (Foundation)
  ├── 1.1 Channels ──────────────────┐
  ├── 1.2 Protocol ──────────────────┤
  ├── 1.3 Config ────────────────────┤
  └── 1.4 Module init ──────────────┘
                                      │
Phase 2 (Defaults)                    ▼
  ├── 2.1 Materialization helpers ───┐
  ├── 2.2 StaticOutputPod default ───┤ (depends on 1.x + 2.1)
  ├── 2.3 FunctionPod async ─────────┤
  └── 2.4 SourceNode async ─────────┘
                                      │
Phase 3 (Orchestrator)                ▼
  ├── 3.1 DAG compilation ───────────┐ (depends on 2.x)
  └── 3.2 Orchestrator ─────────────┘
                                      │
Phase 4 (Streaming Overrides)         ▼
  ├── 4.1 Column selection ──────────┐
  ├── 4.2 Mappers ───────────────────┤ (all independent, depend on 3.x)
  ├── 4.3 Filter ────────────────────┤
  ├── 4.4 Join ──────────────────────┤
  ├── 4.5 MergeJoin ─────────────────┤
  └── 4.6 SemiJoin ──────────────────┘
                                      │
Phase 5 (Integration)                 ▼
  ├── 5.1 Pipeline API ─────────────┐
  ├── 5.2 NodeConfig attachment ─────┤ (depends on 4.x)
  └── 5.3 Integration tests ────────┘
```

Phases 1–3 must be sequential. Phase 4 steps are independent of each other.
Phase 5 depends on everything above.

---

## Risk Assessment

| Risk | Mitigation |
|---|---|
| Row ordering differs between sync/async | Document clearly; `sort_by_tags` provides determinism |
| Incremental Join correctness | Extensive property-based tests comparing to sync |
| Deadlocks from channel misuse | Strict rule: every node MUST close output channel |
| Per-row Datagram operations are slow | Benchmark; fall back to barrier if perf regresses |
| Breaking existing tests | async_execute is additive; sync path unchanged |
| Fan-out channel memory | Bounded buffers + backpressure limit memory |

---

## What's NOT in Scope

- Distributed execution (network channels, Ray integration) — future work
- Adaptive concurrency tuning — future work
- Checkpointing / fault recovery — future work
- Modifications to `PacketFunctionExecutorProtocol` — orthogonal concern, unchanged
- Changes to hashing / identity — unchanged
- Changes to `CacheMode` semantics — unchanged
