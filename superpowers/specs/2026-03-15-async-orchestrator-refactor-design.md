# Async Orchestrator Refactor Design

PLT-922: Refactor AsyncPipelineOrchestrator to use node protocols and orchestrator interface.

## Context

The `SyncPipelineOrchestrator` (PLT-921) introduced node protocols
(`SourceNodeProtocol`, `FunctionNodeProtocol`, `OperatorNodeProtocol`) with
TypeGuard dispatch. The sync orchestrator currently drives per-packet execution
for function nodes — calling `get_cached_results`, `execute_packet`, and firing
observer hooks from the orchestrator side.

The `AsyncPipelineOrchestrator` uses a different pattern: it calls a uniform
`node.async_execute(inputs, output)` on all nodes, letting nodes handle
everything internally via channels.

This refactor aligns both orchestrators on a common design where:

- Nodes own their execution (caching, per-packet logic, persistence).
- Orchestrators are topology schedulers that call `execute` / `async_execute`.
- Observability is achieved via observer injection, not orchestrator-driven hooks.

## Design Decisions

### Slim node protocols

The three node protocols expose only `execute` (sync) and `async_execute`
(async). All per-packet methods (`get_cached_results`, `execute_packet`,
`compute_pipeline_entry_id`) are removed from the protocol surface — they remain
as internal methods on the node classes.

### Observer injection via parameter

Both `execute` and `async_execute` accept an optional `observer` keyword
argument. Nodes call the observer hooks internally (`on_node_start`,
`on_node_end`, `on_packet_start`, `on_packet_end`). The `ExecutionObserver`
protocol itself is unchanged.

### Orchestrators are topology schedulers

Neither orchestrator inspects packet content, manages caches, or drives
per-packet loops. They:

1. Walk the graph in topological order.
2. Call `execute` or `async_execute` on each node with the correct inputs.
3. Collect results into `OrchestratorResult`.

### Tightened async signatures per node type

Instead of a uniform `async_execute(inputs: Sequence[ReadableChannel], output)`
for all nodes, each protocol has a signature matching its arity:

- Source: `async_execute(output)` — no inputs
- Function: `async_execute(input_channel, output)` — single input
- Operator: `async_execute(inputs: Sequence[ReadableChannel], output)` — N inputs

### Deferred: prefer_async and concurrency config

Two features are deferred to follow-up issues:

- **PLT-929**: `prefer_async` flag on `FunctionNode` — allows sync `execute()`
  to internally use the async execution path when the pod/executor supports it.
- **PLT-930**: Move async concurrency config to node-level construction —
  currently `FunctionNode.async_execute` receives `pipeline_config` for
  `max_concurrency`. In this refactor, `pipeline_config` is removed from the
  `async_execute` signature entirely. Nodes use their existing default
  concurrency until PLT-930 adds proper node-level config.

## Revised Node Protocols

```python
@runtime_checkable
class SourceNodeProtocol(Protocol):
    node_type: str  # == "source"

    def execute(
        self, *, observer: ExecutionObserver | None = None
    ) -> list[tuple[TagProtocol, PacketProtocol]]: ...

    async def async_execute(
        self,
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
        *,
        observer: ExecutionObserver | None = None,
    ) -> None: ...


@runtime_checkable
class FunctionNodeProtocol(Protocol):
    node_type: str  # == "function"

    def execute(
        self,
        input_stream: StreamProtocol,
        *,
        observer: ExecutionObserver | None = None,
    ) -> list[tuple[TagProtocol, PacketProtocol]]: ...

    async def async_execute(
        self,
        input_channel: ReadableChannel[tuple[TagProtocol, PacketProtocol]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
        *,
        observer: ExecutionObserver | None = None,
    ) -> None: ...


@runtime_checkable
class OperatorNodeProtocol(Protocol):
    node_type: str  # == "operator"

    def execute(
        self,
        *input_streams: StreamProtocol,
        observer: ExecutionObserver | None = None,
    ) -> list[tuple[TagProtocol, PacketProtocol]]: ...

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
        *,
        observer: ExecutionObserver | None = None,
    ) -> None: ...
```

TypeGuard dispatch functions (`is_source_node`, `is_function_node`,
`is_operator_node`) remain unchanged.

## Sync Orchestrator

The orchestrator simplifies to a pure topology scheduler:

```python
class SyncPipelineOrchestrator:
    def __init__(self, observer=None):
        self._observer = observer

    def run(self, graph, materialize_results=True) -> OrchestratorResult:
        for node in topological_sort(graph):
            if is_source_node(node):
                buffers[node] = node.execute(observer=self._observer)
            elif is_function_node(node):
                stream = self._materialize_as_stream(buffers[pred], pred)
                buffers[node] = node.execute(stream, observer=self._observer)
            elif is_operator_node(node):
                streams = [self._materialize_as_stream(buffers[p], p)
                           for p in sorted_preds]
                buffers[node] = node.execute(*streams, observer=self._observer)
        return OrchestratorResult(node_outputs=buffers)
```

`_materialize_as_stream` is retained — operators need `StreamProtocol` inputs.
`_gather_upstream`, `_gather_upstream_multi`, `_gc_buffers` helpers are retained.
The per-packet methods (`_execute_source`, `_execute_function`,
`_execute_operator`) are removed — each becomes a single `node.execute()` call.

## Async Orchestrator

The async orchestrator preserves the channel-based concurrent execution model
but uses TypeGuard dispatch with tightened per-type signatures.

```python
class AsyncPipelineOrchestrator:
    def __init__(self, observer=None, buffer_size=64):
        self._observer = observer
        self._buffer_size = buffer_size

    def run(self, graph, materialize_results=True) -> OrchestratorResult:
        return asyncio.run(self._run_async(graph, materialize_results))

    async def run_async(self, graph, materialize_results=True) -> OrchestratorResult:
        return await self._run_async(graph, materialize_results)

    async def _run_async(self, graph, materialize_results) -> OrchestratorResult:
        # Wire channels between nodes (same logic as today)
        # For materialize_results=True: tee each output channel to collect items

        async with asyncio.TaskGroup() as tg:
            for node in topo_order:
                if is_source_node(node):
                    tg.create_task(
                        node.async_execute(writer, observer=self._observer)
                    )
                elif is_function_node(node):
                    tg.create_task(
                        node.async_execute(
                            input_reader, writer, observer=self._observer
                        )
                    )
                elif is_operator_node(node):
                    tg.create_task(
                        node.async_execute(
                            input_readers, writer, observer=self._observer
                        )
                    )

        return OrchestratorResult(node_outputs=collected if materialize else {})
```

Key changes from current implementation:

- Takes `graph: nx.DiGraph` instead of `Pipeline` + `PipelineConfig`.
- Returns `OrchestratorResult` instead of `None`.
- TypeGuard dispatch with per-type signatures instead of uniform call.
- Observer injection via constructor + parameter forwarding.
- `buffer_size` is a constructor parameter (not from `PipelineConfig`).
- `materialize_results` controls whether intermediate outputs are collected.

### Result collection

When `materialize_results=True`, each node's output channel is tapped to collect
items into a list as they flow through. This uses a lightweight wrapper that
appends each item to a per-node list before forwarding to downstream readers.
When `materialize_results=False`, no collection occurs and `OrchestratorResult`
has empty `node_outputs`. Terminal sink channels are still drained regardless of
this flag, since nodes write to them unconditionally.

### Channel wiring

Channel wiring logic is preserved from the current implementation:

- Single downstream: plain `Channel(buffer_size=self._buffer_size)`.
- Fan-out (multiple downstreams): `BroadcastChannel` with a reader per
  downstream.
- Terminal nodes (no outgoing edges): sink `Channel` so `async_execute` has
  somewhere to write. Drained after execution.

### Existing node `run()` methods

The existing `run()` method on `SourceNode`, `FunctionNode`, and `OperatorNode`
(the non-orchestrator pull-based execution path) is left intact. It serves a
different purpose — standalone node execution outside of pipeline orchestration.

### Error handling

If a node raises during `async_execute`, `asyncio.TaskGroup` cancels all
sibling tasks and propagates the exception. Observer hooks (`on_node_end`) are
not guaranteed to fire on failure — this matches the sync orchestrator's
behavior where an exception in `node.execute()` also skips `on_node_end`.

## Node Internal Changes

### SourceNode

**New method:** `execute(*, observer=None) -> list[(tag, packet)]`

- Calls `observer.on_node_start(self)` if observer provided.
- Materializes `self.iter_packets()` into a list.
- Populates `_cached_results` so subsequent `iter_packets()` calls return the
  cached version.
- Calls `observer.on_node_end(self)`.
- Returns the list.

**Signature change:** `async_execute(output, *, observer=None) -> None`

- Tightened from `async_execute(inputs, output)` — no `inputs` parameter
  (source has no upstream).
- Adds observer `on_node_start` / `on_node_end` hooks internally.

**Removed from protocol:** `iter_packets()` — replaced by `execute()`. The
method remains on the class for internal use and backward compatibility, but it
is no longer part of `SourceNodeProtocol`.

### FunctionNode

**Signature change:** `execute(input_stream, *, observer=None) -> list[(tag, packet)]`

- The existing `execute` method already takes `input_stream: StreamProtocol` and
  returns `list[(tag, packet)]`.
- Adds `observer` parameter. Internally calls `on_node_start` / `on_node_end`
  and per-packet `on_packet_start` / `on_packet_end(cached=...)` hooks.
- Internally uses `get_cached_results`, `compute_pipeline_entry_id`, and
  `execute_packet` — these are implementation details, not protocol surface.

**Signature change:** `async_execute(input_channel, output, *, observer=None) -> None`

- Tightened from `async_execute(inputs, output, pipeline_config)` — single
  `input_channel` instead of `Sequence[ReadableChannel]`.
- `pipeline_config` parameter removed entirely. Node uses its existing default
  concurrency. Proper node-level concurrency config deferred to PLT-930.
- Adds observer hooks internally.

**Internal execution logic in `execute()`:** The current
`SyncPipelineOrchestrator._execute_function` drives a per-packet loop with
cache lookup (`get_cached_results`) and observer hooks. This logic moves inside
`FunctionNode.execute()`: iterate over the input stream's packets, call
`compute_pipeline_entry_id` to check the pipeline DB, call `execute_packet` for
misses, and fire `on_packet_start` / `on_packet_end(cached=...)` around each
packet. The node's internal `CachedFunctionPod` handles function-level
memoization as before.

**Removed from protocol (kept as class methods):**
`get_cached_results`, `execute_packet`, `compute_pipeline_entry_id`.

### OperatorNode

**Signature change:** `execute(*input_streams, observer=None) -> list[(tag, packet)]`

- The existing `execute` method already takes `*input_streams: StreamProtocol`
  and returns `list[(tag, packet)]`.
- Adds `observer` parameter. Internally calls `on_node_start` / `on_node_end`.
- Internally calls `get_cached_output()` first — if it returns a stream
  (REPLAY mode), materializes it and returns without computing. Otherwise
  delegates to the operator's `process()` and handles persistence.

**Signature change:** `async_execute(inputs, output, *, observer=None) -> None`

- Signature already takes `Sequence[ReadableChannel]` — no arity change.
- Adds observer hooks internally.

**Removed from protocol (kept as class method):** `get_cached_output`.

## Pipeline.run() Changes

```python
def run(self, orchestrator=None, config=None, ...):
    if not self._compiled:
        self.compile()
    if effective_engine is not None:
        self._apply_execution_engine(effective_engine, effective_opts)

    if orchestrator is not None:
        orchestrator.run(self._node_graph)
    else:
        use_async = ...  # same logic as today
        if use_async:
            AsyncPipelineOrchestrator(
                buffer_size=config.channel_buffer_size,
            ).run(self._node_graph)
        else:
            SyncPipelineOrchestrator().run(self._node_graph)

    self.flush()
```

The `_run_async()` helper method is removed. Both orchestrators receive
`self._node_graph` directly. The default async path instantiates
`AsyncPipelineOrchestrator` inline and calls `.run(self._node_graph)`, matching
the sync path pattern.

## File-level Change Summary

| File | Changes |
|------|---------|
| `protocols/node_protocols.py` | Remove `get_cached_results`, `execute_packet`, `compute_pipeline_entry_id` from `FunctionNodeProtocol`. Remove `get_cached_output` from `OperatorNodeProtocol`. Remove `iter_packets` from `SourceNodeProtocol`. Add `execute` and `async_execute` with observer param to all three protocols. |
| `protocols/core_protocols/async_executable.py` | Delete entire file. |
| `protocols/core_protocols/__init__.py` | Remove `AsyncExecutableProtocol` re-export. |
| `core/nodes/source_node.py` | Add `execute(observer=None)` method. Change `async_execute` signature (remove `inputs` param, add `observer`). Add observer hooks. |
| `core/nodes/function_node.py` | Add `observer` param to `execute`. Change `async_execute` signature (single `input_channel`, remove `pipeline_config`, add `observer`). Move per-packet cache lookup + observer hook calls inside `execute()`. |
| `core/nodes/operator_node.py` | Add `observer` param to `execute`. Add `observer` param to `async_execute`. Move observer hook calls inside both methods. |
| `pipeline/sync_orchestrator.py` | Remove `_execute_source`, `_execute_function`, `_execute_operator`. Simplify `run()` to call `node.execute(...)` directly. Pass observer to nodes. |
| `pipeline/async_orchestrator.py` | Change `run` / `run_async` to take `graph` + `materialize_results`. Add TypeGuard dispatch. Tighten per-node `async_execute` calls. Add `buffer_size` constructor param. Add observer support. Return `OrchestratorResult`. |
| `pipeline/graph.py` | Remove `_run_async()`. Update default async path to instantiate `AsyncPipelineOrchestrator` and call `.run(self._node_graph)`. |
| `tests/test_pipeline/test_sync_orchestrator.py` | Update tests for new `node.execute()` path. Observer tests verify hooks fire from inside nodes. |
| `tests/test_pipeline/test_orchestrator.py` | Update async tests for new signature (`graph` instead of `Pipeline`). Add `materialize_results` tests. Add fan-out and terminal node tests. |

## Removals

**From node protocols:**

- `SourceNodeProtocol.iter_packets()`
- `FunctionNodeProtocol.get_cached_results()`
- `FunctionNodeProtocol.compute_pipeline_entry_id()`
- `FunctionNodeProtocol.execute_packet()`
- `OperatorNodeProtocol.get_cached_output()`

**From protocols package:**

- `AsyncExecutableProtocol` (entire file `async_executable.py`)

**From Pipeline:**

- `_run_async()` helper method

**From SyncPipelineOrchestrator:**

- `_execute_source()`, `_execute_function()`, `_execute_operator()` methods

## Testing Strategy

- **Sync orchestrator tests**: Update existing tests to verify `node.execute()`
  is called (not the removed per-packet orchestrator logic). Observer tests
  verify hooks fire from inside nodes with the same events and order.
- **Async orchestrator tests**: Update for new signature (`graph` instead of
  `Pipeline`). Verify `OrchestratorResult` is returned.
- **Sync/async parity tests**: Both orchestrators should produce identical DB
  results. Existing parity tests updated for new signatures.
- **`materialize_results` tests**: Verify `True` collects all node outputs,
  `False` returns empty `node_outputs` (both orchestrators).
- **Fan-out tests**: Verify `BroadcastChannel` wiring when one node fans out to
  multiple downstreams (async orchestrator).
- **Terminal node tests**: Verify sink channels are created and drained for nodes
  with no outgoing edges (async orchestrator).
- **Error propagation tests**: Verify that a node failure in `TaskGroup`
  propagates correctly and doesn't hang.
