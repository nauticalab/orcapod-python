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

### prefer_async flag on FunctionNode

`FunctionNode` gains a `prefer_async` constructor parameter (default `False`).
When `execute()` is called (sync path), the node checks this flag:

- `prefer_async=True` and pod/executor supports async → run async path internally
- Otherwise → run sync path

This allows a sync orchestrator to still leverage async-capable executors
per-node without complicating the orchestrator or `execute` signature.

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
The per-packet `_execute_function` method is removed.

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
has empty `node_outputs`.

## Node Internal Changes

### SourceNode

- `execute(observer=None)`: calls observer hooks, materializes `iter_packets()`,
  returns list.
- `async_execute(output, observer=None)`: tightened signature (no `inputs`
  param), adds observer hooks internally.

### FunctionNode

- `execute(input_stream, observer=None)`: internally handles cache lookup,
  per-packet execution with observer hooks, persistence. Respects
  `prefer_async` flag to choose sync vs async execution path.
- `async_execute(input_channel, output, observer=None)`: tightened signature
  (single input channel), adds observer hooks internally.
- `get_cached_results`, `execute_packet`, `compute_pipeline_entry_id` remain as
  class methods but are removed from the protocol.
- Constructor gains `prefer_async: bool = False` parameter.

### OperatorNode

- `execute(*input_streams, observer=None)`: internally handles cache check,
  operator delegation, persistence, observer hooks.
- `async_execute(inputs, output, observer=None)`: signature unchanged (already
  takes sequence), adds observer hooks internally.
- `get_cached_output` remains as class method but removed from protocol.

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
            AsyncPipelineOrchestrator().run(self._node_graph)
        else:
            SyncPipelineOrchestrator().run(self._node_graph)

    self.flush()
```

The `_run_async` helper method is removed. Both orchestrators receive
`self._node_graph` directly.

## Removals

**From node protocols:**

- `FunctionNodeProtocol.get_cached_results()`
- `FunctionNodeProtocol.compute_pipeline_entry_id()`
- `FunctionNodeProtocol.execute_packet()`
- `OperatorNodeProtocol.get_cached_output()`

**From protocols package:**

- `AsyncExecutableProtocol` (entire file `async_executable.py`)

**From Pipeline:**

- `_run_async()` helper method

## Testing Strategy

- Update existing sync orchestrator tests to use the new `node.execute()` path.
- Update existing async orchestrator tests for the new signature
  (`graph` instead of `Pipeline`).
- Sync/async parity tests remain — both orchestrators should produce identical
  DB results.
- Observer tests verify hooks fire from inside nodes (same events, same order).
- Add tests for `materialize_results=True/False` on async orchestrator.
- Add tests for `prefer_async` flag on `FunctionNode`.
