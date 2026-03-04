# Unified Async Channel Execution System

**Status:** Proposed
**Date:** 2026-03-04

---

## Motivation

The current execution model is synchronous and pull-based: each node materializes its full
output before the downstream node begins. This means a pipeline like
`Source → Filter → FunctionPod → Join → Map` processes in discrete stages — Filter waits for
all source rows, FunctionPod waits for all filtered rows, etc.

This design proposes a **push-based async channel execution model** where every pipeline node
is a coroutine that consumes from input channels and produces to output channels. Rows flow
through the pipeline as soon as they're available, enabling:

- **Streaming**: row-by-row operators (Filter, Map, Select, FunctionPod) emit immediately
  without buffering
- **Incremental computation**: multi-input operators (Join) can emit matches as rows arrive
  from any input, using techniques like symmetric hash join
- **Controlled concurrency**: per-node `max_concurrency` limits enable rate-limiting for
  external API calls or GPU inference while allowing trivial operators to run unbounded
- **Backpressure**: bounded channels naturally throttle fast producers when downstream
  consumers are slow

Critically, the design is **backwards-compatible** — every existing synchronous operator works
unchanged via a barrier wrapper, and the executor type is selected at the pipeline level.

---

## Core Design: One Interface for All Nodes

### The Async Execute Protocol

Every pipeline node — source, operator, or function pod — implements a single method:

```python
@runtime_checkable
class AsyncExecutableProtocol(Protocol):
    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
    ) -> None:
        """
        Consume (tag, packet) pairs from input channels, produce to output channel.
        MUST close output channel when done (signals completion to downstream).
        """
        ...
```

The orchestrator sees a **homogeneous DAG** — it doesn't need to know whether a node is an
operator, function pod, or source. It just wires up channels and launches tasks.

### Channel Abstraction

Channels are bounded async queues with close semantics:

```python
@dataclass
class Channel(Generic[T]):
    """Bounded async channel with close/done signaling."""
    _queue: asyncio.Queue[T | _Sentinel]
    _closed: asyncio.Event

    @property
    def reader(self) -> ReadableChannel[T]: ...

    @property
    def writer(self) -> WritableChannel[T]: ...


class ReadableChannel(Protocol[T]):
    """Consumer side of a channel."""

    async def receive(self) -> T:
        """Receive next item. Raises ChannelClosed when done."""
        ...

    def __aiter__(self) -> AsyncIterator[T]: ...
    async def __anext__(self) -> T: ...

    async def collect(self) -> list[T]:
        """Drain all remaining items into a list."""
        ...


class WritableChannel(Protocol[T]):
    """Producer side of a channel."""

    async def send(self, item: T) -> None:
        """Send an item. Blocks if channel buffer is full (backpressure)."""
        ...

    async def close(self) -> None:
        """Signal that no more items will be sent."""
        ...
```

Bounded channels provide natural backpressure: a fast producer blocks on `send()` when the
buffer is full, automatically throttling without explicit flow control.

### Thread Safety

Channels are backed by `asyncio.Queue`, which is **coroutine-safe but not thread-safe**.
This is sufficient because all channel operations happen on the event loop thread:

- `async_execute` methods are coroutines running on the event loop
- Sync `PacketFunction`s run in thread pools via `loop.run_in_executor`, but the result
  is awaited back on the event loop before `output.send()` is called — the channel is
  never touched from a worker thread
- The `async def` signature on `send()`/`receive()` structurally prevents direct calls
  from non-async (thread) contexts

If a future executor needs to push results directly from worker threads (bypassing the event
loop), channels should be swapped to a dual sync/async queue (e.g., `janus`) or use
`loop.call_soon_threadsafe` to marshal back to the event loop. This is not needed for the
current design.

---

## Three Execution Strategies

All three strategies implement the same `async_execute` interface. The differences are purely
in **when** the node reads, **how much** it buffers, and **when** it emits.

### 1. Streaming (Row-by-Row)

**Applies to:** Filter, MapTags, MapPackets, Select/Drop columns, FunctionPod

Zero buffering. Each input row is independently transformed and emitted immediately.

```python
# Example: PolarsFilter
async def async_execute(self, inputs, output):
    async for tag, packet in inputs[0]:
        if self._evaluate_predicate(tag, packet):
            await output.send((tag, packet))
    await output.close()

# Example: FunctionPod with concurrency control
async def async_execute(self, inputs, output):
    sem = asyncio.Semaphore(self.node_config.max_concurrency or _INF)

    async def process_one(tag, packet):
        async with sem:
            result = await self.packet_function.async_call(packet)
            if result is not None:
                await output.send((tag, result))

    async with asyncio.TaskGroup() as tg:
        async for tag, packet in inputs[0]:
            tg.create_task(process_one(tag, packet))

    await output.close()
```

### 2. Incremental (Stateful, Eager Emit)

**Applies to:** Join, MergeJoin, SemiJoin

Maintains internal state (hash indexes). Emits matches as soon as they're found.

```python
# Example: Symmetric Hash Join
async def async_execute(self, inputs, output):
    indexes: list[dict[JoinKey, list[Row]]] = [{} for _ in inputs]

    async def consume(i: int, channel):
        async for tag, packet in channel:
            key = self._extract_join_key(tag)
            indexes[i].setdefault(key, []).append((tag, packet))

            # Probe all OTHER indexes for matches
            other_lists = [indexes[j].get(key, []) for j in range(len(inputs)) if j != i]
            for combo in itertools.product(*other_lists):
                joined = self._merge_rows((tag, packet), *combo)
                await output.send(joined)

    async with asyncio.TaskGroup() as tg:
        for i, ch in enumerate(inputs):
            tg.create_task(consume(i, ch))

    await output.close()
```

For SemiJoin (non-commutative), the right side is buffered first, then left rows are probed:

```python
async def async_execute(self, inputs, output):
    left, right = inputs

    # Phase 1: Build right-side index
    right_keys = set()
    async for tag, packet in right:
        key = self._extract_join_key(tag)
        right_keys.add(key)

    # Phase 2: Stream left, emit matches
    async for tag, packet in left:
        key = self._extract_join_key(tag)
        if key in right_keys:
            await output.send((tag, packet))

    await output.close()
```

### 3. Barrier (Fully Synchronous, Wrapped)

**Applies to:** Batch, or any operator that hasn't implemented `async_execute`

Collects all input, runs existing `static_process`, emits results. This is the **default**
implementation on operator base classes — every existing operator works without modification.

```python
async def async_execute(self, inputs, output):
    # Phase 1: Collect all inputs (the barrier)
    collected = [await ch.collect() for ch in inputs]

    # Phase 2: Materialize into streams, run sync logic
    streams = [self._materialize(rows) for rows in collected]
    result_stream = self.static_process(*streams)

    # Phase 3: Emit results asynchronously
    for tag, packet in result_stream.iter_packets():
        await output.send((tag, packet))

    await output.close()
```

The barrier is a **local** bottleneck — upstream streaming nodes still push rows into the
barrier's input channel as they're produced, and downstream nodes receive rows as soon as
the barrier emits them.

---

## Default Implementations (Backwards Compatibility)

Operator base classes provide a default `async_execute` that wraps `static_process` in the
barrier pattern. Existing operators work without any changes:

```python
class UnaryOperator(StaticOutputPod):
    """Default: barrier mode. Override async_execute for streaming."""

    async def async_execute(self, inputs, output):
        rows = await inputs[0].collect()
        stream = self._materialize_to_stream(rows)
        result = self.static_process(stream)
        for tag, packet in result.iter_packets():
            await output.send((tag, packet))
        await output.close()


class BinaryOperator(StaticOutputPod):
    async def async_execute(self, inputs, output):
        left_rows, right_rows = await asyncio.gather(
            inputs[0].collect(), inputs[1].collect()
        )
        left_stream = self._materialize_to_stream(left_rows)
        right_stream = self._materialize_to_stream(right_rows)
        result = self.static_process(left_stream, right_stream)
        for tag, packet in result.iter_packets():
            await output.send((tag, packet))
        await output.close()


class NonZeroInputOperator(StaticOutputPod):
    async def async_execute(self, inputs, output):
        all_rows = await asyncio.gather(*(ch.collect() for ch in inputs))
        streams = [self._materialize_to_stream(rows) for rows in all_rows]
        result = self.static_process(*streams)
        for tag, packet in result.iter_packets():
            await output.send((tag, packet))
        await output.close()
```

Concrete operators **opt into** better strategies by overriding `async_execute`.

---

## FunctionPod and FunctionNode

FunctionPod fits the streaming pattern naturally — it processes packets independently:

```python
class FunctionPod:
    async def async_execute(self, inputs, output):
        sem = asyncio.Semaphore(self.node_config.max_concurrency or _INF)

        async def process_one(tag, packet):
            async with sem:
                result_packet = await self.packet_function.async_call(packet)
                if result_packet is not None:
                    await output.send((tag, result_packet))

        async with asyncio.TaskGroup() as tg:
            async for tag, packet in inputs[0]:
                tg.create_task(process_one(tag, packet))

        await output.close()
```

FunctionNode adds DB-backed caching — cache hits emit immediately, misses go through the
semaphore:

```python
class FunctionNode:
    async def async_execute(self, inputs, output):
        sem = asyncio.Semaphore(self.node_config.max_concurrency or _INF)

        async def process_one(tag, packet):
            cache_key = self._compute_cache_key(packet)
            cached = await self._db_lookup(cache_key)
            if cached is not None:
                await output.send((tag, cached))
                return

            async with sem:
                result = await self.packet_function.async_call(packet)
                await self._db_store(cache_key, result)
                if result is not None:
                    await output.send((tag, result))

        async with asyncio.TaskGroup() as tg:
            async for tag, packet in inputs[0]:
                tg.create_task(process_one(tag, packet))

        await output.close()
```

### Sync PacketFunctions

Existing synchronous `PacketFunction`s are bridged via `run_in_executor`:

```python
class PythonPacketFunction:
    async def direct_async_call(self, packet):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._thread_pool,
            self._func,
            packet,
        )
```

CPU-bound functions run in a thread pool. Async-native functions (API calls, I/O) can
override `direct_async_call` directly.

---

## Configuration

### Two-Level Config

```python
class ExecutorType(Enum):
    SYNCHRONOUS = "synchronous"        # Current behavior: static_process chain
    ASYNC_CHANNELS = "async_channels"  # New: async_execute with channels

@dataclass(frozen=True)
class PipelineConfig:
    executor: ExecutorType = ExecutorType.SYNCHRONOUS
    channel_buffer_size: int = 64
    default_max_concurrency: int | None = None  # pipeline-wide default

@dataclass(frozen=True)
class NodeConfig:
    max_concurrency: int | None = None  # overrides pipeline default
    # None = inherit from pipeline default
    # 1 = sequential (rate-limited APIs, ordered output)
    # N = up to N packets in-flight concurrently
```

### Concurrency Resolution

```python
def resolve_concurrency(node_config: NodeConfig, pipeline_config: PipelineConfig) -> int | None:
    if node_config.max_concurrency is not None:
        return node_config.max_concurrency
    return pipeline_config.default_max_concurrency
```

Examples:
- `max_concurrency=1`: sequential processing (rate-limited API, preserves ordering)
- `max_concurrency=8`: bounded parallelism (GPU inference, external service)
- `max_concurrency=None` (unlimited): trivial ops (column select, rename)

---

## Orchestrator

The orchestrator builds the DAG, creates channels, and launches all nodes concurrently:

```python
class AsyncPipelineOrchestrator:

    def run(self, graph: CompiledGraph, config: PipelineConfig) -> StreamProtocol:
        """Entry point — runs async pipeline, returns materialized result."""
        return asyncio.run(self._run_async(graph, config))

    async def _run_async(self, graph, config):
        buf = config.channel_buffer_size

        # Create a channel for each edge in the DAG
        channels: dict[EdgeId, Channel] = {
            edge: Channel(buffer_size=buf) for edge in graph.edges
        }

        # Launch every node as a concurrent task
        async with asyncio.TaskGroup() as tg:
            for node in graph.nodes:
                input_chs = [channels[e].reader for e in node.input_edges]
                output_ch = channels[node.output_edge].writer
                tg.create_task(node.async_execute(input_chs, output_ch))

        # Collect terminal output
        terminal_rows = await channels[graph.terminal_edge].collect()
        return self._materialize(terminal_rows)
```

### Source Nodes

Sources have no input channels — they just push their data onto the output channel:

```python
class SourceNode:
    async def async_execute(self, inputs, output):
        # inputs is empty for sources
        for tag, packet in self.stream.iter_packets():
            await output.send((tag, packet))
        await output.close()
```

### Fan-Out (Multiple Consumers)

When a node's output feeds multiple downstream nodes, the channel is **broadcast** — each
downstream gets its own reader over a shared sequence. This avoids duplicating computation
while allowing each consumer to read at its own pace.

---

## Operator Classification

| Operator | Default Strategy | Async Override? |
|---|---|---|
| PolarsFilter | Barrier (inherited) | **Streaming** — evaluate predicate per row |
| MapTags / MapPackets | Barrier (inherited) | **Streaming** — rename per row |
| SelectTagColumns / SelectPacketColumns | Barrier (inherited) | **Streaming** — project per row |
| DropTagColumns / DropPacketColumns | Barrier (inherited) | **Streaming** — project per row |
| FunctionPod | N/A (new) | **Streaming** — transform packet per row |
| FunctionNode | N/A (new) | **Streaming** — cache check + transform per row |
| Join | Barrier (inherited) | **Incremental** — symmetric hash join |
| MergeJoin | Barrier (inherited) | **Incremental** — symmetric hash join with merge |
| SemiJoin | Barrier (inherited) | **Incremental** — buffer right, stream left |
| Batch | Barrier (inherited) | Barrier (inherent) — needs all rows for grouping |

All operators work in barrier mode by default. Streaming/incremental overrides are added
incrementally — the system is correct at every step.

---

## Interaction with Existing Execution Models

### Synchronous Mode (ExecutorType.SYNCHRONOUS)

Unchanged. The existing `static_process` / `DynamicPodStream` / `iter_packets` chain continues
to work exactly as before. `async_execute` is never called.

### Async Mode (ExecutorType.ASYNC_CHANNELS)

The orchestrator calls `async_execute` on every node. The existing `static_process` is used
by the barrier wrapper as an implementation detail — it's not called directly by the
orchestrator.

### PacketFunctionExecutorProtocol

The existing executor protocol (`execute` / `async_execute` for individual packets) remains
unchanged. It controls **how a single packet function invocation runs** (local, Ray, etc.).
The new `async_execute` on nodes controls **how the node participates in the pipeline DAG**.
These are orthogonal concerns:

- `PacketFunctionExecutorProtocol.async_execute(fn, packet)` → single invocation strategy
- `FunctionPod.async_execute(inputs, output)` → pipeline-level data flow

### GraphTracker

The `GraphTracker` continues to build the DAG via `record_*_invocation` calls. The compiled
graph it produces is what the `AsyncPipelineOrchestrator` consumes. The tracker doesn't need
to know about async execution — it only records topology.

---

## Row Ordering Considerations

Streaming and incremental strategies may change row ordering compared to synchronous mode:

- **Streaming with concurrency**: `max_concurrency > 1` on FunctionPod means packets may
  complete out of order. If ordering matters, set `max_concurrency=1`.
- **Incremental Join**: rows are emitted as matches are found, which depends on arrival order
  from upstream. The result set is identical but row order may differ.
- **Barrier**: row order matches synchronous mode exactly.

The `sort_by_tags` option in `ColumnConfig` provides deterministic ordering when needed,
independent of execution strategy.

---

## Error Propagation

When a node raises an exception inside `async_execute`:

1. The `TaskGroup` propagates the exception, cancelling all other tasks
2. Channel close semantics ensure no deadlocks — cancelled producers don't block consumers
3. The orchestrator surfaces the original exception to the caller

This is handled naturally by Python's `asyncio.TaskGroup` semantics.

---

## Future Extensions

- **Distributed execution**: Replace local channels with network channels (e.g., gRPC streams)
  while keeping the same `async_execute` interface
- **Adaptive concurrency**: Auto-tune `max_concurrency` based on throughput/latency metrics
- **Checkpointing**: Persist channel state for fault recovery in long-running pipelines
- **Backpressure metrics**: Expose channel fill levels for monitoring and debugging
