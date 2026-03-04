# Execution Models

orcapod supports multiple execution strategies that produce semantically identical results.
The choice of strategy is an execution concern — neither content hashes nor pipeline hashes
depend on how the pipeline was executed.

## Synchronous Execution (Pull-Based)

The default model. Callers invoke `process()` on a pod, which returns a stream. Iteration
triggers computation lazily.

### Lazy In-Memory

`FunctionPod` → `FunctionPodStream`: processes each packet on demand.

<!--pytest-codeblocks:skip-->
```python
from orcapod import FunctionPod

pod = FunctionPod(packet_function=pf)
result = pod(input_stream)  # Returns FunctionPodStream

# Computation happens during iteration
for tag, packet in result.iter_packets():
    print(packet.as_dict())
```

No database persistence. Suitable for exploration and one-off computations.

### Static with Recomputation

`StaticOutputPod` → `DynamicPodStream`: the operator's `static_process` produces a complete
output stream. `DynamicPodStream` wraps it with timestamp-based staleness detection and
automatic recomputation when upstreams change.

<!--pytest-codeblocks:skip-->
```python
from orcapod.core.operators import Join

joined = Join()(source_a, source_b)  # Returns DynamicPodStream
table = joined.as_table()            # Triggers computation
```

### Database-Backed Incremental

`FunctionNode` / `OperatorNode`: results are persisted. Only inputs whose hashes are not
already in the database are computed.

<!--pytest-codeblocks:skip-->
```python
from orcapod import PersistentFunctionNode

node = PersistentFunctionNode(
    function_pod=pod,
    input_stream=input_stream,
    pipeline_database=db,
)
node.run()  # Phase 1: cached results, Phase 2: compute missing
```

## Asynchronous Execution (Push-Based Channels)

Every pipeline node implements the `AsyncExecutableProtocol`:

<!--pytest-codeblocks:skip-->
```python
async def async_execute(
    inputs: Sequence[ReadableChannel[tuple[Tag, Packet]]],
    output: WritableChannel[tuple[Tag, Packet]],
) -> None
```

Nodes consume `(Tag, Packet)` pairs from input channels and produce them to an output
channel, enabling streaming execution with backpressure.

### Operator Async Strategies

| Strategy | Description | Operators |
|----------|-------------|-----------|
| Barrier mode (default) | Collect all inputs, run `static_process`, emit | Batch |
| Streaming | Process rows individually, zero buffering | Filter, Map, Select, Drop |
| Incremental | Stateful, emit partial results as inputs arrive | Join, MergeJoin, SemiJoin |

### Channels

Channels are bounded async queues with explicit close/done signaling:

- **`WritableChannel`** — `send(item)` blocks when buffer is full (backpressure).
  `close()` signals no more items.
- **`ReadableChannel`** — `receive()` blocks until available. Supports `async for`
  iteration.
- **`BroadcastChannel`** — fans out from one writer to multiple independent readers.

### Configuration

<!--pytest-codeblocks:skip-->
```python
from orcapod.types import PipelineConfig, NodeConfig, ExecutorType

# Pipeline-level configuration
config = PipelineConfig(
    executor=ExecutorType.ASYNC_CHANNELS,
    channel_buffer_size=128,
    default_max_concurrency=4,
)

# Per-node override
node_config = NodeConfig(max_concurrency=1)  # Force sequential
```

## Concurrent Execution with Executors

Executors decouple **what** a function computes from **where** it runs:

<!--pytest-codeblocks:skip-->
```python
from orcapod.core.executors import LocalExecutor, RayExecutor

# Local (default): runs in-process
pf.executor = LocalExecutor()

# Ray: distributed execution
pf.executor = RayExecutor(num_cpus=4, num_gpus=1)
```

### Executor Routing

```
packet_function.call(packet)
    ├── executor is set → executor.execute(packet_function, packet)
    └── executor is None → packet_function.direct_call(packet)
```

### Identity Separation

Executors are **not** part of content or pipeline identity. The same function produces the
same hash regardless of executor. This means cached results from local execution are valid
for Ray execution and vice versa.

## Choosing an Execution Model

| Model | Best For |
|-------|----------|
| Synchronous lazy | Interactive exploration, debugging |
| Synchronous DB-backed | Production with incremental computation |
| Async channels | Pipeline-level parallelism, streaming |
| Ray executor | Distributed computation, GPU workloads |
