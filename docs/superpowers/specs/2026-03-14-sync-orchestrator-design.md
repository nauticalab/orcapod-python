# Sync Pipeline Orchestrator Design

## Overview

Design a synchronous pipeline orchestrator that provides fine-grained control over pipeline
execution, including per-packet observability hooks for function nodes. The orchestrator
operates on a graph of nodes, drives execution externally (rather than relying on pull-based
iteration), and returns computed results for all nodes.

This design also introduces node protocols that formalize the interface between orchestrator
and nodes, refactors caching/DB logic out of monolithic `run()`/`iter_packets()` methods into
reusable building blocks, and simplifies `Pipeline.run()` to delegate to an orchestrator.

### Goals

- Sync orchestrator with per-packet hooks for logging, metrics, and debugging.
- Clean separation: orchestrator controls scheduling; nodes handle computation and persistence.
- Node protocols so the orchestrator is decoupled from concrete node classes.
- Orchestrator operates on a graph of nodes, not on the `Pipeline` object directly.
- Orchestrator returns results; pipeline decides how to apply them to node caches.
- Memory-saving mode that skips result accumulation and only persists to databases.

### Out of Scope

- Refactoring `AsyncPipelineOrchestrator` to use the new protocols (deferred until both
  implementations exist and can inform a shared protocol).
- Shared orchestrator base/protocol between sync and async.
- Selective cache population policies in `Pipeline._apply_results()`.

## Design

### Conceptual Model

Two orthogonal axes govern pipeline execution:

|              | Pull (consumer drives)     | Push (producer drives)         |
|--------------|----------------------------|--------------------------------|
| **Sync**     | `iter_packets()` iterators | Orchestrator with buffers      |
| **Async**    | `async for` via `__aiter__`| `async_execute()` with channels|

The existing codebase has sync-pull (`iter_packets()`) and async-push (`async_execute()`).
This design adds **sync-push** via `SyncPipelineOrchestrator`.

The pipeline graph's stream wiring declares **topology** (what connects to what). The
orchestrator creates its own **execution transport** (buffers) to control data flow at
runtime. The stream wiring remains valuable for standalone/ad-hoc use without an orchestrator.

### Post-Execution Contract

- **Orchestrator's job**: execute the graph, invoke DB persistence on nodes via their
  protocol methods during execution, and return computed results for all nodes.
- **Pipeline's job**: receive the orchestrator's results and decide per-node whether to
  populate in-memory caches (making `iter_packets()` / `as_table()` work after execution).
- **DB-backed nodes**: results persisted to DB during orchestrator execution AND optionally
  cached in memory after (via pipeline's `_apply_results`).
- **Non-DB nodes**: results available only if the pipeline populates their cache from the
  orchestrator's returned results.
- **`materialize_results=False` mode**: orchestrator discards buffers after all downstream
  consumers have read them. `OrchestratorResult.node_outputs` is empty. Only DB-persisted
  results survive. This is an explicit trade-off: non-DB nodes lose all data after execution.

### Component Flow

```
Pipeline.run(orchestrator=None)
  │
  │  1. compile() if needed
  │  2. default orchestrator = SyncPipelineOrchestrator()
  │
  ▼
orchestrator.run(node_graph)
  │
  │  For each node in topological order:
  │
  ├── SourceNode:
  │     observer.on_node_start(node)
  │     output = materialize node.iter_packets()
  │     observer.on_node_end(node)
  │
  ├── FunctionNode:
  │     observer.on_node_start(node)
  │     compute entry_ids from upstream buffer
  │     cached = node.get_cached_results(entry_ids)    ← DB read
  │     for each (tag, packet) in upstream buffer:
  │       observer.on_packet_start(node, tag, packet)
  │       if entry_id in cached:
  │         use cached result                          ← DB hit
  │         observer.on_packet_end(..., cached=True)
  │       else:
  │         node.process_packet(tag, packet)           ← compute + DB write
  │         observer.on_packet_end(..., cached=False)
  │     observer.on_node_end(node)
  │
  └── OperatorNode:
        observer.on_node_start(node)
        cached = node.get_cached_output()              ← DB read (REPLAY mode)
        if cached:
          output = materialize cached stream
        else:
          output = node.operator.process(*input_streams)  ← compute
          node.store_output(output)                       ← DB write (LOG mode)
        observer.on_node_end(node)
  │
  ▼
returns OrchestratorResult(node_outputs=buffers)
  │
  ▼
Pipeline._apply_results(result)
  │  for each node, calls node.populate_cache(outputs)
  │
  ▼
Pipeline.flush()  ← flush all databases
```

### Node Protocols

Three protocols formalize the orchestrator-node interface. Each matches a fundamentally
different execution model. The orchestrator dispatches via `TypeGuard` functions that check
the existing `node_type` string attribute — cheap at runtime, with full type narrowing for
static analysis.

#### `SourceNodeProtocol`

Provides data with no computation. The orchestrator materializes its output into a buffer.

```python
class SourceNodeProtocol(Protocol):
    node_type: str  # "source"

    def iter_packets(self) -> Iterator[tuple[TagProtocol, PacketProtocol]]: ...
    def populate_cache(self, results: list[tuple[TagProtocol, PacketProtocol]]) -> None: ...
```

#### `FunctionNodeProtocol`

Per-packet computation with optional DB caching. The orchestrator drives iteration externally,
calling `process_packet()` for each input with observer hooks.

`process_packet()` handles DB persistence internally: it delegates to `CachedFunctionPod`
for result caching and calls `add_pipeline_record()` for pipeline provenance. No separate
`store_output()` method is needed.

```python
class FunctionNodeProtocol(Protocol):
    node_type: str  # "function"

    def get_cached_results(
        self, entry_ids: list[str]
    ) -> dict[str, tuple[TagProtocol, PacketProtocol]]: ...

    def compute_pipeline_entry_id(
        self, tag: TagProtocol, packet: PacketProtocol
    ) -> str: ...

    def process_packet(
        self, tag: TagProtocol, packet: PacketProtocol
    ) -> tuple[TagProtocol, PacketProtocol | None]: ...

    def populate_cache(self, results: list[tuple[TagProtocol, PacketProtocol]]) -> None: ...
```

#### `OperatorNodeProtocol`

Whole-stream computation with cache modes. The orchestrator extracts the operator pod and
invokes it with orchestrator-prepared streams.

`store_output()` is cache-mode-aware: writes to DB in LOG mode, no-op in OFF mode.

```python
class OperatorNodeProtocol(Protocol):
    node_type: str  # "operator"
    operator: OperatorPodProtocol

    def get_cached_output(self) -> StreamProtocol | None: ...
    def store_output(self, results: list[tuple[TagProtocol, PacketProtocol]]) -> None: ...
    def populate_cache(self, results: list[tuple[TagProtocol, PacketProtocol]]) -> None: ...
```

Note: `store_output` accepts a materialized list of (tag, packet) pairs rather than a stream.
This avoids the double-consumption problem where materializing a stream for the buffer would
exhaust it before `store_output` can read it. The implementation wraps the list as an
`ArrowTableStream` internally before writing to the DB.

#### TypeGuard Dispatch

```python
from typing import TypeGuard

def is_source_node(node: GraphNode) -> TypeGuard[SourceNodeProtocol]:
    return node.node_type == "source"

def is_function_node(node: GraphNode) -> TypeGuard[FunctionNodeProtocol]:
    return node.node_type == "function"

def is_operator_node(node: GraphNode) -> TypeGuard[OperatorNodeProtocol]:
    return node.node_type == "operator"
```

The dispatch chain must include an `else` branch that raises `TypeError` for unknown node
types to prevent silent skipping if new node types are added.

### ExecutionObserver Protocol

```python
class ExecutionObserver(Protocol):
    def on_node_start(self, node: GraphNode) -> None: ...
    def on_node_end(self, node: GraphNode) -> None: ...
    def on_packet_start(
        self, node: GraphNode, tag: TagProtocol, packet: PacketProtocol
    ) -> None: ...
    def on_packet_end(
        self,
        node: GraphNode,
        tag: TagProtocol,
        input_packet: PacketProtocol,
        output_packet: PacketProtocol | None,
        cached: bool,
    ) -> None: ...
```

`on_packet_start` / `on_packet_end` are only invoked for function nodes (the only node type
with per-packet granularity). `on_node_start` / `on_node_end` are invoked for all node types.

The `tag` parameter in `on_packet_start` is the **input** tag. In `on_packet_end`, `tag` is
also the **input** tag (the output tag is available via `output_packet` if needed, but since
function nodes pass tags through unchanged, they are the same).

Default implementation: `NoOpObserver` with empty method bodies.

### OrchestratorResult

```python
@dataclass
class OrchestratorResult:
    node_outputs: dict[GraphNode, list[tuple[TagProtocol, PacketProtocol]]]
```

When `materialize_results=False`, `node_outputs` is empty.

### SyncPipelineOrchestrator

```python
class SyncPipelineOrchestrator:
    def __init__(self, observer: ExecutionObserver | None = None) -> None:
        self._observer = observer or NoOpObserver()

    def run(
        self,
        graph: nx.DiGraph,
        materialize_results: bool = True,
    ) -> OrchestratorResult:
        ...
```

#### Execution Logic

```python
def run(self, graph, materialize_results=True):
    topo_order = list(nx.topological_sort(graph))
    buffers: dict[GraphNode, list[tuple[Tag, Packet]]] = {}

    for node in topo_order:
        if is_source_node(node):
            buffers[node] = self._execute_source(node)
        elif is_function_node(node):
            upstream_buffer = self._gather_upstream(node, graph, buffers)
            buffers[node] = self._execute_function(node, upstream_buffer)
        elif is_operator_node(node):
            upstream_buffers = self._gather_upstream_multi(node, graph, buffers)
            buffers[node] = self._execute_operator(node, upstream_buffers)
        else:
            raise TypeError(f"Unknown node type: {node.node_type!r}")

        # Memory-saving: discard buffers no longer needed by any downstream
        if not materialize_results:
            self._gc_buffers(node, graph, buffers)

    return OrchestratorResult(node_outputs=buffers)
```

#### Error Handling

Exceptions from node execution (source iteration, `process_packet`, operator `process`)
propagate immediately. The orchestrator does not attempt partial completion or cleanup.
Buffers for already-completed nodes remain in memory; DB-persisted results from completed
nodes survive. This is consistent with the existing sync execution path where an exception
in `node.run()` halts the pipeline.

#### Source Execution

```python
def _execute_source(self, node):
    self._observer.on_node_start(node)
    output = list(node.iter_packets())
    self._observer.on_node_end(node)
    return output
```

#### Function Execution

```python
def _execute_function(self, node, upstream_buffer):
    self._observer.on_node_start(node)

    # Compute entry IDs for current upstream
    upstream_entries = [
        (tag, packet, node.compute_pipeline_entry_id(tag, packet))
        for tag, packet in upstream_buffer
    ]
    entry_ids = [eid for _, _, eid in upstream_entries]

    # Phase 1: targeted cache lookup (DB read)
    cached = node.get_cached_results(entry_ids=entry_ids)

    output = []
    for tag, packet, entry_id in upstream_entries:
        self._observer.on_packet_start(node, tag, packet)
        if entry_id in cached:
            tag_out, result = cached[entry_id]
            self._observer.on_packet_end(node, tag, packet, result, cached=True)
            output.append((tag_out, result))
        else:
            # process_packet handles DB write internally
            tag_out, result = node.process_packet(tag, packet)
            self._observer.on_packet_end(node, tag, packet, result, cached=False)
            if result is not None:
                output.append((tag_out, result))

    self._observer.on_node_end(node)
    return output
```

#### Operator Execution

```python
def _execute_operator(self, node, upstream_buffers):
    self._observer.on_node_start(node)

    cached = node.get_cached_output()  # DB read (REPLAY mode only)
    if cached is not None:
        output = list(cached.iter_packets())
    else:
        input_streams = [
            self._materialize_as_stream(buf, node) for buf in upstream_buffers
        ]
        result_stream = node.operator.process(*input_streams)
        output = list(result_stream.iter_packets())
        node.store_output(output)  # DB write (LOG mode); no-op for OFF

    self._observer.on_node_end(node)
    return output
```

Note: operator input validation is not performed by the orchestrator. Validation occurs at
compile time (`OperatorNode.__init__` calls `self._operator.validate_inputs()`). The
orchestrator-prepared streams have the same schema as the original inputs, so revalidation
is unnecessary.

#### `_materialize_as_stream`

Wraps a `list[tuple[Tag, Packet]]` buffer as an `ArrowTableStream`. Implementation:

1. Extract Arrow tables from each Tag and Packet datagram.
2. Horizontal-stack tag columns + packet columns (including source info and system tags).
3. Tag column names come from the **upstream** node's output schema (available via
   `upstream.keys()` or from the tag objects themselves).
4. Construct `ArrowTableStream(combined_table, tag_columns=tag_keys)`.

For operators with multiple inputs, each upstream buffer is materialized separately using
that upstream's tag column names.

#### Buffer GC (materialize_results=False)

After processing each node, iterate over its predecessors in the graph. For each predecessor,
check if all of its successors have been processed (i.e., appear earlier in topological
order or are the current node). If so, delete the predecessor's buffer. This is O(edges)
per node in the worst case.

### Node Refactoring

#### FunctionNode Additions

**`get_cached_results(entry_ids: list[str]) -> dict[str, tuple[Tag, Packet]]`**

Factored out of `iter_packets()` Phase 1 logic. Implementation:

1. Fetch all pipeline records from `pipeline_database.get_all_records(pipeline_path)`.
2. Fetch all result records from `result_database.get_all_records(record_path)`.
3. Join on `PACKET_RECORD_ID` (same Polars join as current `iter_packets` Phase 1).
4. Filter to only rows whose pipeline entry ID is in the requested `entry_ids` set.
5. Reconstruct (tag, output_packet) pairs from the filtered rows.
6. Return as `dict[entry_id, (tag, packet)]`.

When no DB is attached, returns `{}`.

Note: this method fetches all records and filters in memory. The database protocol does not
currently support filtered lookups by entry ID. If the DB grows very large, a filtered
lookup method could be added to the database protocol in the future, but that is out of
scope for this design.

**`populate_cache(results: list[tuple[Tag, Packet]]) -> None`**

Populates `_cached_output_packets` dict from externally-provided results so that
`iter_packets()` / `as_table()` work after orchestrated execution. Sets
`_cached_input_iterator = None` and `_needs_iterator = False` to indicate iteration is
complete.

#### OperatorNode Additions

**`get_cached_output() -> StreamProtocol | None`**

Returns the cached output stream when in REPLAY mode and DB records exist. Returns `None`
otherwise. Factored out of `run()`. Wraps the existing `_replay_from_cache()` logic.

**`store_output(results: list[tuple[Tag, Packet]]) -> None`**

Accepts a materialized list of (tag, packet) pairs. If cache mode is LOG, wraps the list
as an `ArrowTableStream` and calls the existing `_store_output_stream()` to write to DB.
No-op for OFF mode. This avoids the double-consumption problem: the orchestrator materializes
the stream into a list first, then passes the list to both the buffer and `store_output`.

**`populate_cache(results: list[tuple[Tag, Packet]]) -> None`**

Wraps results as an `ArrowTableStream` and sets `_cached_output_stream` so that
`iter_packets()` / `as_table()` work after orchestrated execution.

#### SourceNode Additions

**`populate_cache(results: list[tuple[Tag, Packet]]) -> None`**

Adds a `_cached_results: list[tuple[Tag, Packet]] | None` field to `SourceNode`. When
populated, `iter_packets()` returns from this cache instead of delegating to
`self.stream.iter_packets()`. This requires modifying `SourceNode.iter_packets()` to check
the cache first. The existing delegation path remains the default when cache is `None`.

### Pipeline.run() Changes

The current `Pipeline.run()` signature is:

```python
def run(self, config=None, execution_engine=None, execution_engine_opts=None)
```

For this iteration, `orchestrator` is added as a new parameter. The existing parameters
are preserved for backward compatibility. When `orchestrator` is provided, it takes
precedence:

```python
def run(self, orchestrator=None, config=None, execution_engine=None,
        execution_engine_opts=None):
    if not self._compiled:
        self.compile()

    if execution_engine is not None:
        self._apply_execution_engine(execution_engine, execution_engine_opts)

    if orchestrator is not None:
        result = orchestrator.run(self._node_graph)
        self._apply_results(result)
    elif use_async:  # existing logic based on config/engine
        self._run_async(config)
    else:
        orchestrator = SyncPipelineOrchestrator()
        result = orchestrator.run(self._node_graph)
        self._apply_results(result)

    self.flush()
```

The long-term goal is to simplify this to just `run(self, orchestrator=None)` once the
async orchestrator is also refactored. For now, the existing parameters are preserved to
avoid breaking changes.

**`_apply_results(result: OrchestratorResult)`**: walks `result.node_outputs` and calls
`node.populate_cache(outputs)` for each node. Initially unconditional; selective policies
can be added later.

### What Stays Unchanged

- `iter_packets()` on all node types — standalone pull-based path, untouched (except
  `SourceNode.iter_packets()` gains a cache check at the top).
- `async_execute()` on all node types — standalone push-based path, untouched.
- `AsyncPipelineOrchestrator` — untouched for now.
- `FunctionNode.run()` — still works for the non-orchestrated path (consumes `iter_packets()`).
- `OperatorNode.run()` — still works for the non-orchestrated path.

### File Organization

- `src/orcapod/pipeline/observer.py` — `ExecutionObserver` protocol, `NoOpObserver`
- `src/orcapod/pipeline/sync_orchestrator.py` — `SyncPipelineOrchestrator`
- `src/orcapod/pipeline/async_orchestrator.py` — existing `AsyncPipelineOrchestrator`
  (renamed from `orchestrator.py`)
- `src/orcapod/pipeline/result.py` — `OrchestratorResult` dataclass
- `src/orcapod/protocols/node_protocols.py` — `SourceNodeProtocol`, `FunctionNodeProtocol`,
  `OperatorNodeProtocol`, TypeGuard functions

### Testing Strategy

- **Unit tests for SyncPipelineOrchestrator**: linear, diamond, fan-out topologies — same
  test shapes as existing `test_orchestrator.py`.
- **Observer tests**: verify hooks fire in correct order with correct arguments.
- **Cache population tests**: verify `populate_cache` makes `iter_packets()` / `as_table()`
  work after orchestrated execution.
- **materialize_results=False tests**: verify DB persistence without memory accumulation.
  Verify that non-DB node data is inaccessible (explicit trade-off).
- **FunctionNode.get_cached_results tests**: targeted lookup returns correct subset;
  empty DB returns `{}`; entry IDs not in DB are absent from result.
- **OperatorNode.get_cached_output / store_output tests**: cache mode behavior (OFF → no-op,
  LOG → writes, REPLAY → reads).
- **Sync vs async parity tests**: same pipeline produces same DB results regardless of
  orchestrator type.
- **Error propagation tests**: exception in mid-pipeline node halts execution; earlier
  nodes' DB results survive.
