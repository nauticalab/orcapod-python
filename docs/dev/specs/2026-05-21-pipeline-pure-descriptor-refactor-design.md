# Pipeline Pure-Descriptor Refactor — Design Spec

**Linear issue:** ENG-493  
**Date:** 2026-05-21  
**Status:** Draft

---

## Overview

`Pipeline` currently stores `FunctionNode` and `OperatorNode` objects in `_node_lut` and
`_persistent_node_map`. These node objects carry DB attachment logic, executor references,
and table-scope metadata that are purely execution concerns. The goal of this refactor is to
make `Pipeline` a pure computational descriptor — containing only lightweight identity nodes —
while `PipelineJob` becomes the sole stateful, executable form.

The refactor also eliminates `SourceSpec` (merged into a new lightweight `SourceNode`) and
removes `Pipeline.bind()` in favour of an explicit `PipelineJob` constructor.

---

## Goals & Success Criteria

- `Pipeline._persistent_node_map` contains **only** `SourceNode | FunctionNode | OperatorNode`
  (lightweight, no DB references, no executor).
- `PipelineJob._persistent_node_map` contains **only** `SourceJobNode | FunctionJobNode |
  OperatorJobNode`, with live DB references distributed at construction/`bind()` time.
- `SourceSpec` is **eliminated**; `SourceNode` takes its place as the user-facing input-slot
  declaration.
- `Pipeline.bind()` is **removed**; the replacement is `PipelineJob.from_pipeline(pipeline,
  store=..., sources=...)`.
- `PipelineJob.bind()` is **mutating** — it modifies the job in place and immediately
  distributes the new store/sources to all member JobNodes.
- All content hashes and pipeline hashes are **bit-for-bit identical** to the pre-refactor
  values (DB path stability preserved).
- All existing external behaviours (operator execution, function-pod execution, partial
  execution with unbound slots, serialisation round-trips) are preserved and tested.

---

## Class Hierarchy

### Node hierarchy

```
AbstractNodeBase(TraceableBase, ABC)
│   Shared interface: node_type, label, content_hash, pipeline_hash,
│   output_schema, iter_data — all six concrete types inherit from this.
│
├── SourceNodeBase(AbstractNodeBase)
│   │   Shared state: _name, _tag_schema, _data_schema
│   │   Shared: identity_structure, pipeline_identity_structure,
│   │           content_hash, pipeline_hash, output_schema, label, name
│   ├── SourceNode       ← replaces SourceSpec; iter_data raises UnboundSourceError
│   └── SourceJobNode    ← _concrete: StreamProtocol (mutable, see bind());
│                           overrides content_hash → concrete.content_hash();
│                           iter_data delegates to concrete
│                           .as_node() → SourceNode
│
├── FunctionNodeBase(AbstractNodeBase)
│   │   Shared state: _function_pod, _input_stream, _label
│   │   Shared: identity_structure, pipeline_identity_structure,
│   │           content_hash, pipeline_hash, output_schema, upstreams
│   ├── FunctionNode     ← iter_data raises PipelineJobRequiredError
│   └── FunctionJobNode  ← _pipeline_db, _result_db, _executor, _table_scope;
│                           DB-backed iter_data
│                           .as_node() → FunctionNode
│
└── OperatorNodeBase(AbstractNodeBase)
    │   Shared state: _operator, _input_streams, _label
    │   Shared: identity_structure, pipeline_identity_structure,
    │           content_hash, pipeline_hash, output_schema, upstreams
    ├── OperatorNode     ← iter_data raises PipelineJobRequiredError
    └── OperatorJobNode  ← _pipeline_db, _cache_mode, _table_scope;
                            DB-backed iter_data
                            .as_node() → OperatorNode
```

`AbstractNodeBase` is the single typing anchor. All six concrete node types implement the
same interface. `TraceableBase` provides label management, data context, and the
`content_hash` / `pipeline_hash` caching infrastructure that all nodes share.

### Pipeline hierarchy

```
AbstractPipelineBase          (recording mechanism, graph state: _node_lut, _upstreams,
│                              _graph_edges, _hash_graph, _persistent_node_map, _nodes)
│
├── Pipeline                  ← _persistent_node_map: {hash → lightweight Node}
│                               no store, no sources, not directly runnable
│                               .save() / .load() — blueprint serialisation
│
└── PipelineJob               ← _persistent_node_map: {hash → JobNode with live DB}
                                _store, _sources, _execution_context (mutable)
                                .bind()         — mutating; distributes DB to all JobNodes
                                .run()          — executes JobNode graph directly
                                .as_pipeline()  — explicit downgrade: creates Pipeline with Nodes
                                .save() / .load() — full job serialisation
```

`Pipeline` and `PipelineJob` are **distinct types** — `isinstance(job, Pipeline)` is `False`.
`AbstractPipelineBase` is the shared anchor for code that needs to accept either.

---

## Detailed Specifications

### SourceNodeBase

**Location:** `src/orcapod/core/nodes/source_node.py`

Shared state: `_name: str`, `_tag_schema: Schema`, `_data_schema: Schema`.

```python
class SourceNodeBase(TraceableBase, ABC):
    node_type = "source"

    def __init__(self, name: str, tag_schema: Schema, data_schema: Schema, ...) -> None: ...

    # identity: (name, tag_schema, data_schema)
    def identity_structure(self) -> Any: ...

    # pipeline identity: (tag_schema, data_schema) — matches RootSource base case
    def pipeline_identity_structure(self) -> Any: ...

    # Shared: content_hash, pipeline_hash, output_schema, label, name property
```

**SourceNode** — the new user-facing input-slot declaration (replaces SourceSpec):

```python
class SourceNode(SourceNodeBase):
    def iter_data(self, ...) -> Iterator: raise UnboundSourceError(...)
```

Users write:
```python
slot_a = SourceNode(label="a", tag_schema=..., data_schema=...)
with pipeline:
    result = my_pod(slot_a)
```

**SourceJobNode** — execution node for a concrete source:

```python
class SourceJobNode(SourceNodeBase):
    def __init__(self, name: str, tag_schema: Schema, data_schema: Schema,
                 concrete: StreamProtocol | None = None, ...) -> None: ...

    # Override: data-inclusive hash (concrete source's hash)
    # When concrete is None (unbound), falls back to schema-based content_hash
    # (identical to SourceNode) — consistent with treating the slot as not yet assigned.
    def content_hash(self, hasher=None) -> ContentHash:
        if self._concrete is None:
            return super().content_hash(hasher)   # SourceNodeBase schema-based hash
        return self._concrete.content_hash(hasher)

    # pipeline_hash() INHERITED — schema-based, same as SourceNode with matching schemas
    # This preserves DB path stability.

    def iter_data(self, ...) -> Iterator:
        if self._concrete is None:
            raise UnboundSourceError(
                f"SourceJobNode '{self._name}' has no concrete source bound. "
                "Call job.bind(sources={...}) before running."
            )
        return self._concrete.iter_data(...)

    def as_node(self) -> SourceNode:
        return SourceNode(name=self._name, tag_schema=self._tag_schema,
                          data_schema=self._data_schema)
```

`SourceJobNode._concrete` is a **mutable** field. `bind(sources=...)` updates it in place
(see `_bind_sources()`) so that downstream `FunctionJobNode._input_stream` references — which
point at the same `SourceJobNode` object — automatically see the new concrete without needing
cascading reference updates throughout the graph.

Hash invariant: `SourceJobNode.pipeline_hash() == SourceNode.pipeline_hash()` for the same
schema, ensuring `FunctionJobNode.pipeline_hash() == FunctionNode.pipeline_hash()` throughout
the chain. `content_hash()` diverges intentionally (data-inclusive vs schema-based).

**SourceSpec is removed.** All references updated to `SourceNode`.

---

### FunctionNodeBase

**Location:** `src/orcapod/core/nodes/function_node.py`

Shared state: `_function_pod: FunctionPodProtocol`, `_input_stream: AbstractNodeBase`,
`_label: str | None`.

```python
class FunctionNodeBase(TraceableBase, ABC):
    node_type = "function"

    def __init__(self, function_pod: FunctionPodProtocol,
                 input_stream: AbstractNodeBase,
                 label: str | None = None, ...) -> None: ...

    def identity_structure(self) -> Any:
        return (self._function_pod, self._input_stream)

    def pipeline_identity_structure(self) -> Any:
        return (self._function_pod, self._input_stream)  # pipeline resolver handles routing

    # Shared: content_hash, pipeline_hash, output_schema, upstreams property
```

**FunctionNode** — lightweight; no DB:

```python
class FunctionNode(FunctionNodeBase):
    def iter_data(self, ...) -> Iterator:
        raise PipelineJobRequiredError(
            "FunctionNode cannot iterate data directly. "
            "Wrap a Pipeline in a PipelineJob to execute."
        )
```

**FunctionJobNode** — execution node:

```python
class FunctionJobNode(FunctionNodeBase):
    def __init__(self, function_pod, input_stream, label=None,
                 pipeline_database=None, result_database=None,
                 table_scope="pipeline_hash", executor=None, ...) -> None: ...

    def attach_databases(self, pipeline_database, result_database) -> None:
        """Wire live DB references. Called by PipelineJob.bind()."""
        self._pipeline_database = pipeline_database
        self._result_database = result_database
        self._cached_function_pod = CachedFunctionPod(self._function_pod, result_database)

    def iter_data(self, ...) -> Iterator:
        # Two-phase: yield cached, then compute missing — identical to current FunctionNode

    def as_node(self) -> FunctionNode:
        return FunctionNode(function_pod=self._function_pod,
                            input_stream=self._input_stream, label=self._label)
```

`_table_scope` and `executor` are execution configuration — present on `FunctionJobNode`,
absent from `FunctionNode`.

---

### OperatorNodeBase

**Location:** `src/orcapod/core/nodes/operator_node.py`

Mirrors `FunctionNodeBase` but for operators with multiple input streams.

**OperatorNode** — lightweight, no DB.

**OperatorJobNode** — adds `_pipeline_database`, `_cache_mode`, `_table_scope`;
`attach_databases()` wires the DB; `as_node()` returns `OperatorNode`.

---

### AbstractPipelineBase

**Location:** `src/orcapod/pipeline/base.py` (new file)

```python
class AbstractPipelineBase(AutoRegisteringContextBasedTracker, ABC):
    """Shared recording mechanism and graph state for Pipeline and PipelineJob."""

    def __init__(self, name: str | tuple[str, ...], ...) -> None:
        self._name: tuple[str, ...]
        self._node_lut: dict[str, AbstractNodeBase]        # recording phase
        self._upstreams: dict[str, AbstractNodeBase]       # leaf nodes
        self._graph_edges: list[tuple[str, str]]
        self._hash_graph: nx.DiGraph
        self._persistent_node_map: dict[str, AbstractNodeBase]   # post-compile
        self._nodes: dict[str, AbstractNodeBase]           # label → node
        self._node_graph: nx.DiGraph | None
        self._compiled: bool

    # Shared concrete methods:
    @property
    def name(self) -> tuple[str, ...]: ...
    @property
    def graph(self) -> nx.DiGraph: ...
    @property
    def compiled_nodes(self) -> dict[str, AbstractNodeBase]: ...
    def reset(self) -> None: ...
    def __exit__(self, ...) -> None: ...    # calls compile()
    def __getattr__(self, item) -> AbstractNodeBase: ...   # label lookup

    # Abstract — specialised per subclass:
    @abstractmethod
    def record_function_pod_invocation(self, pod, input_stream, label=None) -> None: ...
    @abstractmethod
    def record_operator_pod_invocation(self, pod, upstreams, label=None) -> None: ...
    @abstractmethod
    def compile(self) -> None: ...
```

---

### Pipeline

**Location:** `src/orcapod/pipeline/graph.py`

Recording creates **lightweight Nodes** in `_node_lut` / `_upstreams`. All leaf inputs must
be `SourceNode` instances — passing a concrete `RootSource` raises `ValueError` with a
message directing users to `PipelineJob`.

`compile()` walks `_graph_edges` topologically:
- Leaf hashes (in `_upstreams`, not in `_node_lut`) → must be `SourceNode`; stored as-is.
- Non-leaf hashes → `FunctionNode` or `OperatorNode` from `_node_lut`; upstream references
  rewired to point at compiled nodes from `_persistent_node_map`.

`Pipeline` has **no** `bind()` method. To run: `PipelineJob.from_pipeline(pipeline, ...)`.

`save()` / `load()` serialise the lightweight Node graph (no DB configuration).

---

### PipelineJob

**Location:** `src/orcapod/pipeline/job.py`

Additional instance state (beyond `AbstractPipelineBase`):

```python
self._store: ArrowDatabaseProtocol | None
self._sources: dict[str, StreamProtocol]    # SourceNode.name → concrete source
self._execution_context: ExecutionContext | None
self._has_run: bool
self._run_id: str | None
self._unresolved_specs: list[str]
```

#### Recording (`with job:`)

Overrides `record_function_pod_invocation` and `record_operator_pod_invocation`.
Intercepts concrete `RootSource` inputs via `_to_node_stream()`:
- Concrete source → auto-creates `SourceNode(name, schema)` + stores concrete in `_sources`.
- `SourceNode` passed directly → used as-is.
- `DynamicPodStream` → upstreams recursively converted.

Creates `FunctionJobNode` / `OperatorJobNode` (without DB) in `_node_lut`.

#### `compile()`

Walks `_graph_edges` topologically. For each hash:
- **Leaf** → `SourceNode` in `_upstreams`. If `source_node.name in _sources`: create
  `SourceJobNode(name, tag_schema, data_schema, concrete=_sources[name])`. Else: create
  `SourceJobNode(name, tag_schema, data_schema, concrete=None)` (unbound slot).
- **Non-leaf** → `FunctionJobNode` or `OperatorJobNode` from `_node_lut`; rewire upstream
  references to point at compiled JobNodes from `_persistent_node_map`.

After building `_persistent_node_map`, if `_store` is set, calls `_distribute_databases()`.

#### `_distribute_databases()`

```python
def _distribute_databases(self) -> None:
    pipeline_db = self._store.at(*self._name)
    result_db = pipeline_db.at("_result")
    for node in self._persistent_node_map.values():
        if isinstance(node, FunctionJobNode):
            node.attach_databases(pipeline_database=pipeline_db, result_database=result_db)
        elif isinstance(node, OperatorJobNode):
            node.attach_databases(pipeline_database=pipeline_db)
```

#### `bind(store=None, sources=None, execution_context=None)` — mutating

```python
def bind(self, store=None, sources=None, execution_context=None) -> None:
    store_changed = store is not None and store is not self._store
    if store is not None:
        self._store = store
    if sources is not None:
        # Validate each source against its SourceNode slot schema
        # Replace SourceJobNode entries in _persistent_node_map
        self._bind_sources(sources)
    if execution_context is not None:
        self._execution_context = execution_context
    if store_changed:
        self._distribute_databases()
```

`_bind_sources(sources)` validates each new source against the corresponding `SourceJobNode`'s
schema (raises `SourceSchemaMismatchError` on mismatch), then **mutates
`SourceJobNode._concrete` in place** and clears the node's hash cache. Because
`FunctionJobNode._input_stream` holds a reference to the same `SourceJobNode` *object*,
downstream nodes automatically see the updated concrete without any cascading object
replacement. No references throughout the JobNode graph need to be updated.

#### `run(observer=None)`

Determines the **runnable subgraph**: all nodes whose transitive upstream `SourceJobNode`s are
all bound (have a concrete source). Unbound branches are collected as `_unresolved_specs`.

Executes the runnable subgraph in topological order using `SyncPipelineOrchestrator`
(unchanged orchestrator logic). Nodes already have live DB references — no additional wiring
needed.

Updates `_has_run`, `_run_id`, `_unresolved_specs` in place. Returns `self`.

#### `as_pipeline() → Pipeline`

Explicit downgrade. Walks `_persistent_node_map` topologically, calling `.as_node()` on each
JobNode to create the corresponding lightweight Node (with lightweight Node upstreams). Returns
a fresh `Pipeline` object carrying only the Node graph.

#### `PipelineJob.from_pipeline(pipeline, store=None, sources=None, execution_context=None)`

Class method. Creates a `PipelineJob` from a compiled `Pipeline`:
1. Copy graph edges, hash graph, name from `pipeline`.
2. Walk `pipeline._persistent_node_map` topologically. For each Node, create the
   corresponding JobNode:
   - `SourceNode` → `SourceJobNode(name, schemas, concrete=sources.get(name))`
   - `FunctionNode` → `FunctionJobNode(function_pod, upstream_job_node, label, table_scope)`
   - `OperatorNode` → `OperatorJobNode(operator, upstream_job_nodes, label, table_scope, cache_mode)`
3. Set `_store`, `_sources`, `_execution_context`.
4. If `_store` is set, call `_distribute_databases()`.

---

## Hash Stability Guarantee

The refactor must produce **identical** `content_hash()` and `pipeline_hash()` values to
pre-refactor for any given pipeline topology and source binding.

| Object | Identity structure | Hash type |
|--------|--------------------|-----------|
| `SourceNode` | `(name, tag_schema, data_schema)` | Same as old `SourceSpec.content_hash()` |
| `SourceNode.pipeline_hash()` | `(tag_schema, data_schema)` | Same as old `SourceSpec.pipeline_hash()` |
| `SourceJobNode.content_hash()` | delegates to `concrete.content_hash()` | Same as old execution-graph `SourceNode(concrete).content_hash()` |
| `SourceJobNode.pipeline_hash()` | `(tag_schema, data_schema)` — **inherited** | Same as `SourceNode.pipeline_hash()` |
| `FunctionNode.content_hash()` | `(function_pod, input_stream)` via content resolver | Identical to old `FunctionNode.content_hash()` after compile-time rewiring |
| `FunctionNode.pipeline_hash()` | `(function_pod, input_stream)` via pipeline resolver | Identical to old `pipeline_hash()` |
| `FunctionJobNode.content_hash()` | Inherited; `input_stream` is `SourceJobNode` → data-inclusive | Same as old execution-graph `FunctionNode.content_hash()` |
| `FunctionJobNode.pipeline_hash()` | Inherited; pipeline resolver → `SourceJobNode.pipeline_hash()` = schema-based | Same as old `pipeline_hash()` |

No DB paths change. The pipeline hash chain is invariant.

---

## Serialisation

### `Pipeline.save()` / `Pipeline.load()`

Format unchanged except:
- Node type `"source"` entries now reconstruct as `SourceNode` (not `SourceNode(stream=SourceSpec(...))`).
- `source_config.source_type == "node"` (was `"spec"`).

Format version bump: `"orcapod_pipeline_version": "0.3"`.

### `PipelineJob.save()` / `PipelineJob.load()`

Saves the Pipeline blueprint (via `as_pipeline().save()`) plus:
- `bindings.sources`: config per bound source.
- `bindings.store`: store config.
- `run`: `run_id`, `status`, `unresolved_specs`.

Format version bump: `"orcapod_pipeline_job_version": "0.2"`.

---

## Removed / Changed Public API

| Before | After |
|--------|-------|
| `SourceSpec(name, tag_schema, data_schema)` | `SourceNode(label, tag_schema, data_schema)` |
| `pipeline.bind(sources, store)` | `PipelineJob.from_pipeline(pipeline, store, sources)` |
| `PipelineJob(name, _pipeline=p, sources=s, store=db)` | `PipelineJob.from_pipeline(p, store=db, sources=s)` |
| `job.bind(sources, store)` → returns new `PipelineJob` | `job.bind(sources, store)` → mutates `job`, returns `None` |
| `pipeline._persistent_node_map` has `FunctionNode` w/ DB | `pipeline._persistent_node_map` has lightweight `FunctionNode` |
| `FunctionNode.attach_databases(...)` on Pipeline nodes | `FunctionJobNode.attach_databases(...)` on PipelineJob nodes only |
| `SourceNode(stream=SourceSpec(...))` in compiled Pipeline | `SourceNode(label, tag_schema, data_schema)` directly |

---

## File Layout Changes

```
src/orcapod/
├── pipeline/
│   ├── base.py          ← NEW: AbstractPipelineBase
│   ├── graph.py         ← Pipeline (stripped of bind(), execution Node creation)
│   └── job.py           ← PipelineJob (JobNode graph, mutating bind, from_pipeline, as_pipeline)
└── core/
    └── nodes/
        ├── __init__.py  ← updated type aliases (AbstractNodeBase, GraphNode, JobNode)
        ├── source_node.py    ← SourceNodeBase, SourceNode (was SourceSpec too), SourceJobNode
        ├── function_node.py  ← FunctionNodeBase, FunctionNode, FunctionJobNode
        └── operator_node.py  ← OperatorNodeBase, OperatorNode, OperatorJobNode

src/orcapod/core/sources/
    source_spec.py       ← DELETED
```

---

## Tests

Updated / new test coverage:

- `Pipeline` with `SourceNode` leaves; verify `SourceSpec` import removed.
- `PipelineJob.from_pipeline(pipeline, store=db, sources={...})` end-to-end.
- `job.bind(store=db)` mutates `job` and immediately wires DB into JobNodes.
- `job.bind(sources={...})` replaces `SourceJobNode` in `_persistent_node_map`.
- `FunctionJobNode.as_node()` returns `FunctionNode` with same `content_hash()`.
- `job.as_pipeline()` returns `Pipeline` whose Nodes match `job`'s topology and hashes.
- Hash stability: `FunctionJobNode.pipeline_hash() == FunctionNode.pipeline_hash()`.
- Partial execution: unbound `SourceJobNode` slots excluded; `unresolved_specs` populated.
- `SourceJobNode(concrete=None).iter_data()` raises `UnboundSourceError`.
- `SourceJobNode(concrete=None).content_hash()` equals `SourceNode(same schemas).content_hash()`.
- `job.bind(sources={name: src})` mutates the existing `SourceJobNode` in place; downstream
  `FunctionJobNode._input_stream` reference remains the same object.
- Save / load round-trip for both `Pipeline` and `PipelineJob`.
- Schema mismatch on `bind(sources={...})` raises `SourceSchemaMismatchError`.
