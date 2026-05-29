# Pipeline DAG Public API Design

**Issue:** ENG-517 (part of the execution-graph cleanup)
**Follow-up:** ENG-494 (replace `_hash_graph` with `OrcaDAG[str]`, remove networkx runtime dep)
**Date:** 2026-05-29

---

## Overview

`Pipeline` and `PipelineJob` already build a node-object graph during compilation
(`_node_graph: nx.DiGraph`), but it is private, untyped, and backed by networkx —
a heavy dependency the project is actively migrating away from. External code (tests,
orchestrators) reaches into `_node_graph` directly, bypassing any stable API contract.

This spec defines:

1. A generic `PipelineProtocol[NodeT]` that formalises the public surface of both
   `Pipeline` and `PipelineJob`, including a `dag` property.
2. `AbstractPipelineBase[NodeT]` made generic, with `_node_graph` typed as
   `OrcaDAG[NodeT]` and exposed via a public `dag` property.
3. Migration of all internal call sites from `_node_graph` to `dag`.

The internal hash-string recording graph (`_hash_graph: nx.DiGraph`) is **not**
changed by this spec — that belongs to ENG-494.

---

## Goals & Success Criteria

- `PipelineProtocol[NodeT]` defined in `pipeline_protocols.py`, covering `name`,
  `nodes`, and `dag`.
- `AbstractPipelineBase[NodeT]` is explicitly generic; `Pipeline` is parameterised
  as `AbstractPipelineBase[GraphNode]`, `PipelineJob` as
  `AbstractPipelineBase[JobNode]`.
- `_node_graph` is typed `OrcaDAG[NodeT] | None`; built as `OrcaDAG` (not
  `nx.DiGraph`) in all four compilation / load paths.
- Public `dag` property on `AbstractPipelineBase` raises `RuntimeError` when
  accessed before compilation, returns `OrcaDAG[NodeT]` after.
- All call sites that previously accessed `pipeline._node_graph` now use
  `pipeline.dag`.
- All existing tests pass; new tests cover the `dag` property and
  `PipelineProtocol` structural conformance.

---

## Node-type distinction: `job.dag` vs `job.pipeline.dag`

`PipelineJob` holds two node universes:

- **Job nodes** (`SourceJobNode`, `FunctionJobNode`, `OperatorJobNode`) — live in
  `_persistent_node_map`; used by orchestrators for execution.
- **Blueprint nodes** (`SourceNode`, `FunctionNode`, `OperatorNode`) — live in
  `job.pipeline._persistent_node_map`; used for rendering and serialization.

`job.dag` returns `OrcaDAG[JobNode]` (execution-ready). `job.pipeline.dag` returns
`OrcaDAG[GraphNode]` (blueprint, for rendering).

Currently `PipelineJob.load()` sets `job._node_graph = pipeline._node_graph`
(blueprint nodes) for "rendering use." This is inconsistent with the compiled path,
where `compile()` builds `_node_graph` with job nodes. This spec corrects the load
path: `PipelineJob.load()` will build `OrcaDAG[JobNode]` from `_persistent_node_map`
instead of copying from `pipeline._node_graph`. Callers needing blueprint nodes for
rendering use `job.pipeline.dag`.

Note: `PipelineJob.run()` builds its own internal `exec_dag` (a filtered subset of
the full job-node graph, excluding unbound sources) and never reads `_node_graph`.
`job.dag` exposes the full unfiltered job-node graph; `run()` continues to build
its filtered copy internally.

---

## Architecture

### Protocol layer

Add `PipelineProtocol[NodeT]` to `src/orcapod/protocols/pipeline_protocols.py`:

```python
from typing import Generic, Protocol, TypeVar
from orcapod.pipeline.dag import GraphProtocol  # TYPE_CHECKING import in practice

NodeT = TypeVar("NodeT")

class PipelineProtocol(Protocol[NodeT]):
    """Structural protocol for Pipeline and PipelineJob."""

    @property
    def name(self) -> tuple[str, ...]: ...

    @property
    def nodes(self) -> dict[str, NodeT]: ...

    @property
    def dag(self) -> GraphProtocol[NodeT]: ...
```

Return type of `dag` on the protocol is `GraphProtocol[NodeT]` — the abstract
protocol — keeping the protocol decoupled from the concrete `OrcaDAG`
implementation.

### Base class

`AbstractPipelineBase` gains a `Generic[NodeT]` parent:

```python
class AbstractPipelineBase(Generic[NodeT], AutoRegisteringContextBasedTracker, ABC):
    ...
    _node_graph: OrcaDAG[NodeT] | None
    _nodes: dict[str, NodeT]
    _persistent_node_map: dict[str, NodeT]

    @property
    def dag(self) -> OrcaDAG[NodeT]:
        if self._node_graph is None:
            raise RuntimeError(
                "Pipeline has not been compiled. "
                "Use 'with pipeline:' or call compile() first."
            )
        return self._node_graph
```

The concrete return type on the base class (`OrcaDAG[NodeT]`) is intentionally
more specific than `GraphProtocol[NodeT]` — callers using the concrete class get
the full `OrcaDAG` API; callers typed against `PipelineProtocol` see
`GraphProtocol`.

### Concrete classes

```python
class Pipeline(AbstractPipelineBase[GraphNode], ...): ...
class PipelineJob(AbstractPipelineBase[JobNode], ...): ...
```

No runtime change — purely a type-annotation update at the class declaration site.

---

## Data Flow — Building the OrcaDAG

The same pattern replaces `nx.DiGraph` construction in four places:

```python
from orcapod.pipeline.dag import OrcaDAG

dag: OrcaDAG[NodeT] = OrcaDAG()
for up_hash, down_hash in self._hash_graph.edges():
    up_node = node_map.get(up_hash)
    down_node = node_map.get(down_hash)
    if up_node is not None and down_node is not None:
        dag.add_edge(up_node, down_node)
for node in node_map.values():
    if node not in dag:
        dag.add_node(node)
self._node_graph = dag
```

The build sites:

| Site | File | Node type |
| ---- | ---- | --------- |
| `AbstractPipelineBase.compile()` — step 5 | `base.py` | `NodeT` (GraphNode or JobNode, per subclass) |
| `Pipeline.load()` — node-graph reconstruction block | `graph.py` | `GraphNode` |
| `PipelineJob.from_pipeline()` — blueprint node-graph for extracted `Pipeline` | `job.py` | `GraphNode` |
| `PipelineJob.load()` — currently `job._node_graph = pipeline._node_graph` (wrong type) | `job.py` | **Changed to `OrcaDAG[JobNode]` from `_persistent_node_map`** |

`Pipeline._clone()` only assigns the `_node_graph` reference — no build pattern, no change needed.

---

## Call-site migration

### Production code

| Current call | Replacement |
| ------------ | ----------- |
| `pipeline._node_graph` (passed to orchestrators) | `pipeline.dag` |
| `SyncPipelineOrchestrator().run(pipeline._node_graph)` | `SyncPipelineOrchestrator().run(pipeline.dag)` |
| `AsyncPipelineOrchestrator().run(pipeline._node_graph)` | `AsyncPipelineOrchestrator().run(pipeline.dag)` |

Orchestrators are already typed `graph: GraphProtocol[Any]` — `OrcaDAG` satisfies
this with no further change.

### Test code

| Current pattern | Replacement |
| --------------- | ----------- |
| `pipeline._node_graph` | `pipeline.dag` |
| `nx.topological_sort(pipeline._node_graph)` | `pipeline.dag.topological_sort()` |
| `pipeline._node_graph.nodes()` | `pipeline.dag.nodes()` |
| `pipeline._node_graph.edges()` | `pipeline.dag.edges()` |
| `_build_exec_dag(job)` helper in async integration tests | `job.dag` directly (helper deleted) |
| `for node in nx.topological_sort(pipeline._node_graph)` | `for node in pipeline.dag.topological_sort()` |

Affected test files: `test_pipeline.py`, `test_pipeline_job.py`,
`test_serialization.py`, `test_logging_observer_integration.py`,
`test_graph_rendering.py`, `test_status_observer_integration.py`,
`test_orchestrator.py`, `test_orchestrator_executor_matrix.py`,
`test_sync_orchestrator.py`, `test_pipeline_async_integration.py`.

---

## Error Handling

No new error paths. The `dag` property raises `RuntimeError` on pre-compilation
access, matching the semantics of the existing `if self._node_graph is None` guard
used internally before this change.

---

## Testing

### New tests

Add to `tests/test_pipeline/test_dag.py` (or a new
`tests/test_pipeline/test_pipeline_protocol.py`):

- `pipeline.dag` returns an `OrcaDAG` instance after compilation.
- Node set and edge set of `pipeline.dag` match the expected topology.
- `pipeline.dag.topological_sort()` returns a valid topological order.
- `pipeline.dag.successors(node)` / `predecessors(node)` return the correct
  neighbour sets.
- Accessing `pipeline.dag` before compilation raises `RuntimeError`.
- `Pipeline` satisfies `PipelineProtocol[GraphNode]` structurally.
- `PipelineJob` satisfies `PipelineProtocol[JobNode]` structurally.

### Migrated tests

All existing tests that access `_node_graph` directly are updated to `dag` — no
new assertions, just call-site updates to stop using the private attribute.

---

## Out of Scope

- Replacing `_hash_graph: nx.DiGraph` with `OrcaDAG[str]` — tracked in ENG-494.
- Removing `networkx` from `pyproject.toml` — tracked in ENG-494.
- Updating `graph_renderer.py` / `show_graph()` — renderer still receives
  `_hash_graph` for DOT/Graphviz output; no change in this issue.
- Adding `run()`, `bind()`, or other methods to `PipelineProtocol` — out of scope;
  the protocol covers introspection only for now.
