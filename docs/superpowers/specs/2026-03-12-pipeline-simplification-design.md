# Pipeline & Node Simplification

**Date**: 2026-03-12
**Status**: Approved design, pending implementation

## Overview

Simplify the pipeline architecture by merging redundant class hierarchies:

1. Merge `GraphTracker` into `Pipeline`
2. Merge `PersistentFunctionNode` into `FunctionNode`
3. Merge `PersistentOperatorNode` into `OperatorNode`
4. Make source nodes first-class pipeline members
5. Refactor delegating sources to inherit from `ArrowTableSource`

### Goals

- Fewer classes, less indirection
- Single `FunctionNode` and `OperatorNode` with optional database backing
- Sources accessible via `pipeline.my_source` attribute access
- Clean source inheritance (no internal `_arrow_source` delegation)

### Non-goals

- Changing the tracker manager / tracker protocol system
- Modifying `ArrowTableStream` or the stream protocol
- Changing the serialization JSON format

---

## 1. GraphTracker → Pipeline Merge

### Current state

```
BasicTrackerManager
AutoRegisteringContextBasedTracker (ABC)
  └── GraphTracker
        └── Pipeline
```

`GraphTracker` holds: `_node_lut`, `_upstreams`, `_graph_edges`, `_hash_graph`.
It provides: `record_function_pod_invocation()`, `record_operator_pod_invocation()`,
`reset()`, `nodes` property, `graph` property.

Pipeline inherits all of this and overrides `compile()`.

### Proposed state

```
BasicTrackerManager
AutoRegisteringContextBasedTracker (ABC)
  └── Pipeline
```

- Pipeline inherits from `AutoRegisteringContextBasedTracker` directly.
- `_node_lut`, `_upstreams`, `_graph_edges`, `_hash_graph` become Pipeline attributes.
- `record_function_pod_invocation()` and `record_operator_pod_invocation()` become
  Pipeline methods (implementing the abstract methods from the tracker ABC).
- `reset()`, `nodes`, `graph` move to Pipeline.
- `GraphTracker` class is deleted.

### Files affected

- **Delete from** `src/orcapod/core/tracker.py`: `GraphTracker` class. Keep
  `BasicTrackerManager`, `AutoRegisteringContextBasedTracker`, `DEFAULT_TRACKER_MANAGER`.
- **Modify** `src/orcapod/pipeline/graph.py`: change base class, inline GraphTracker
  state and methods.
- **Update** `tests/test_core/test_tracker.py`: test Pipeline directly instead of
  GraphTracker (or delete if covered by pipeline tests).

---

## 2. PersistentFunctionNode → FunctionNode Merge

### Current state

`FunctionNode` provides core streaming (in-memory, no DB). `PersistentFunctionNode`
extends it with `CachedPacketFunction` wrapping, two-phase iteration, pipeline paths,
DB storage, `from_descriptor()`, `as_source()`, `get_all_records()`.

### Proposed state

Single `FunctionNode` class with optional database parameters:

```python
class FunctionNode(StreamBase):
    def __init__(
        self,
        function_pod: FunctionPodProtocol,
        input_stream: StreamProtocol,
        pipeline_database: ArrowDatabaseProtocol | None = None,
        result_database: ArrowDatabaseProtocol | None = None,
        result_path_prefix: tuple[str, ...] | None = None,
        pipeline_path_prefix: tuple[str, ...] | None = None,
        label: str | None = None,
    ):
```

**Behavior based on `pipeline_database`:**

- `pipeline_database is None` → in-memory only. Simple `iter_packets()` that computes
  each packet via the pod. No `CachedPacketFunction` wrapping. `get_all_records()` and
  `as_source()` raise or return empty. This is used during the recording phase and for
  standalone usage without a pipeline.
- `pipeline_database is not None` → full persistent behavior. Wraps packet function in
  `CachedPacketFunction`, enables two-phase iteration, pipeline path computation, DB
  storage, `get_all_records()`, `as_source()`.

**`attach_databases()` method:**

```python
def attach_databases(
    self,
    pipeline_database: ArrowDatabaseProtocol,
    result_database: ArrowDatabaseProtocol,
    result_path_prefix: tuple[str, ...] | None = None,
    pipeline_path_prefix: tuple[str, ...] | None = None,
) -> None:
    """Wire database backing into this node. Called by Pipeline.compile()."""
```

This method:
1. Sets `_pipeline_database`, `_result_database`, path prefixes
2. Clears `_content_hash_cache` and `_pipeline_hash_cache` (stale after upstream
   rewiring)
3. Clears `_cached_output_packets` (in-memory cache from recording phase)
4. Guards against double-wrapping: if `_packet_function` is already a
   `CachedPacketFunction`, skip wrapping
5. Wraps the raw packet function in `CachedPacketFunction`
6. Computes `pipeline_path` (using `pipeline_hash()` — must be called after
   upstream rewiring)

**Ordering constraint:** `compile()` must rewire upstreams via setters *before*
calling `attach_databases()`, since `pipeline_path` depends on `pipeline_hash()`
which depends on the upstream identity chain.

**Idempotency:** `attach_databases()` is called once during compilation. On
incremental recompile, nodes already in `_persistent_node_map` are reused and
`attach_databases()` is not called again.

The node's content hash is stable through upstream rewiring because `SourceNode`
delegates `identity_structure()` and `pipeline_identity_structure()` to its wrapped
stream — the hash is the same whether the upstream is a bare stream or a SourceNode
wrapping it. Databases are not part of identity computation.

**All methods from `PersistentFunctionNode` move into `FunctionNode`:**
- `from_descriptor()`, `load_status`, `as_source()`, `get_all_records()`
- Two-phase `iter_packets()` and `async_execute()`
- `pipeline_path`, `execution_engine_opts`, `add_pipeline_record()`
- Methods that only apply when database is present guard with
  `if self._pipeline_database is None` checks.

**Delete** `PersistentFunctionNode` class entirely.

### Files affected

- **Modify** `src/orcapod/core/nodes/function_node.py`: merge classes.
- **Modify** `src/orcapod/core/nodes/__init__.py`: remove `PersistentFunctionNode` export.
- **Modify** `src/orcapod/__init__.py`: remove `PersistentFunctionNode` export.
- **Modify** `src/orcapod/core/sources/derived_source.py`: update type annotations.
- **Modify** `src/orcapod/pipeline/orchestrator.py`: update references.
- **Update all imports** (~21 files including tests, demos, test-objectives):
  replace `PersistentFunctionNode` → `FunctionNode`.
- **Update isinstance checks** in `pipeline/graph.py`: use `FunctionNode` directly
  (all function nodes are now `FunctionNode`).

---

## 3. PersistentOperatorNode → OperatorNode Merge

Same pattern as FunctionNode.

```python
class OperatorNode(StreamBase):
    def __init__(
        self,
        operator: OperatorPodProtocol,
        input_streams: tuple[StreamProtocol, ...],
        pipeline_database: ArrowDatabaseProtocol | None = None,
        pipeline_path_prefix: tuple[str, ...] | None = None,
        label: str | None = None,
    ):
```

**Behavior based on `pipeline_database`:**

- `None` → in-memory only. Direct operator execution, no cache modes.
- Not `None` → full persistent behavior: `CacheMode` (OFF/LOG/REPLAY),
  `pipeline_path`, `get_all_records()`, `as_source()`, DB storage.

**`attach_databases()` method:**

```python
def attach_databases(
    self,
    pipeline_database: ArrowDatabaseProtocol,
    pipeline_path_prefix: tuple[str, ...] | None = None,
) -> None:
```

**`attach_databases()` follows the same pattern as FunctionNode:**
1. Sets `_pipeline_database`, path prefix
2. Clears hash caches and in-memory output cache
3. Computes `pipeline_path`

**Note:** OperatorNode uses `content_hash()` (data-inclusive) for `_pipeline_node_hash`,
unlike FunctionNode which uses `pipeline_hash()` (schema-only). This distinction is
preserved in the merged class. OperatorNode does not have a separate `result_database`
since operators don't produce function-style per-packet results.

**All methods from `PersistentOperatorNode` move into `OperatorNode`.**

**Delete** `PersistentOperatorNode` class entirely.

### Files affected

- **Modify** `src/orcapod/core/nodes/operator_node.py`: merge classes.
- **Modify** `src/orcapod/core/nodes/__init__.py`: remove `PersistentOperatorNode` export.
- **Modify** `src/orcapod/core/sources/derived_source.py`: update type annotation
  `PersistentFunctionNode | PersistentOperatorNode` → `FunctionNode | OperatorNode`.
- **Modify** `src/orcapod/pipeline/orchestrator.py`: update references.
- **Update all imports** (~15 files): replace `PersistentOperatorNode` → `OperatorNode`.
- **Update isinstance checks** in `pipeline/graph.py`.

---

## 4. compile() Simplification

### Current flow

1. **Recording** (`with pipeline:`): creates lightweight `FunctionNode`/`OperatorNode`
   (no DB), stores in `_node_lut`.
2. **compile()**: walks topological order, creates *new* `PersistentFunctionNode`/
   `PersistentOperatorNode` with databases, stores in `_persistent_node_map`.

### New flow

1. **Recording**: creates `FunctionNode`/`OperatorNode` (no DB), stores in
   `_node_lut` — unchanged.
2. **compile()**: walks topological order, **mutates existing nodes**:
   - Creates `SourceNode` for leaf streams (must happen first — upstream nodes
     need to exist before downstream nodes can reference them)
   - Rewires `upstreams` via existing setters (pointing to SourceNode or
     already-compiled upstream nodes)
   - Calls `node.attach_databases(...)` to wire in DB backing (must be after
     upstream rewiring — pipeline_path depends on upstream hashes)
   - Stores same node objects in `_persistent_node_map`

### Incremental recompile

`_persistent_node_map` persists across `with` blocks. On subsequent compiles, nodes
already in `_persistent_node_map` are reused (they already have databases attached).
The check `if node_hash in persistent_node_map` skips re-processing.

---

## 5. Source Nodes as First-Class Pipeline Members

### Current behavior

- `compile()` creates `SourceNode` for leaf streams but excludes them from `_nodes`
- `load()` includes them (inconsistency)
- Sources are not accessible via `pipeline.my_source`

### New behavior

- `compile()` includes `SourceNode` in `_nodes` dict alongside function and operator nodes
- Sources participate in label disambiguation (collision handling with `_1`, `_2` suffixes)
- `pipeline.my_source` works via `__getattr__`
- `compiled_nodes` returns all nodes including sources
- Both `compile()` and `load()` follow the same rule

---

## 6. Source Label Cleanup

### Current behavior

`RootSource.computed_label()` returns `self._source_id`, causing the label to default
to the source_id (often a data hash or file path). Delegating sources forward this.

### New behavior

- **Remove** `computed_label()` override from `RootSource` (in `base.py`).
- **Remove** forwarding `computed_label()` overrides from DictSource, ListSource,
  DataFrameSource.
- Sources without explicit label fall back to `__class__.__name__`
  (e.g. `"DictSource"`, `"CSVSource"`).
- `source_id` remains a separate concept used only for provenance tokens.

**SourceNode label delegation:** `SourceNode.computed_label()` currently returns
`self.stream.label`. After removing `RootSource.computed_label()`, `stream.label`
will return `self.__class__.__name__` (e.g. `"DictSource"`) when no explicit label
is set. This flows through correctly: `SourceNode.computed_label()` → `stream.label`
→ class name fallback. No change needed in `SourceNode`.

### Files affected

- **Modify** `src/orcapod/core/sources/base.py`: remove `computed_label()` override.
- **Modify** `src/orcapod/core/sources/dict_source.py`: remove `computed_label()`.
- **Modify** `src/orcapod/core/sources/list_source.py`: remove `computed_label()`.
- **Modify** `src/orcapod/core/sources/data_frame_source.py`: remove `computed_label()`.

---

## 7. Source Inheritance Refactor

### Current pattern

Each delegating source (DictSource, ListSource, CSVSource, DataFrameSource,
DeltaTableSource) creates an internal `_arrow_source: ArrowTableSource` in `__init__`
and delegates ~7 methods to it. This causes:

- Awkward `source_id` threading (passed to both wrapper and inner source)
- `to_config()` reaches into `_arrow_source` private attributes
- Boilerplate delegation for `output_schema`, `keys`, `iter_packets`, `as_table`,
  `source_id`, `computed_label`, `identity_structure`, `resolve_field`

### New pattern

Each source inherits from `ArrowTableSource` directly:

```python
class DictSource(ArrowTableSource):
    def __init__(self, data, tag_columns, source_id=None, ...):
        table = type_converter.python_dicts_to_arrow_table(data)
        super().__init__(table=table, tag_columns=tag_columns,
                         source_id=source_id, ...)
```

**Per-source changes:**

- **DictSource**: converts Python dicts → Arrow table, calls `super().__init__()`.
  Stores no extra state beyond what `ArrowTableSource` holds.
- **ListSource**: converts list elements via tag function → Arrow table, calls
  `super().__init__()`. Stores `_tag_function`, `_tag_function_hash_mode`, `_name`
  for `identity_structure()` override and `to_config()`.
- **CSVSource**: reads CSV → Arrow table, calls `super().__init__()`. Stores
  `_file_path` for `to_config()` and `from_config()`.
- **DataFrameSource**: converts DataFrame → Arrow table, calls `super().__init__()`.
  Stores no extra state.
- **DeltaTableSource**: reads Delta table → Arrow table, calls `super().__init__()`.
  Stores `_delta_table_path` for `to_config()` and `from_config()`.

**Methods eliminated (no longer needed):**
- All `computed_label()` overrides (removed per section 6)
- All `source_id` property overrides (inherited from `ArrowTableSource`)
- All `output_schema()`, `keys()`, `iter_packets()`, `as_table()` delegations
- All `identity_structure()` delegations (except `ListSource` which has custom logic)
- All `resolve_field()` delegations (inherited from `ArrowTableSource`)

**Methods retained per source:**
- `to_config()` — each source serializes its own constructor args
- `from_config()` — CSVSource and DeltaTableSource only
- `identity_structure()` — ListSource only (includes tag function hash)

### Hash stability

`ArrowTableSource.identity_structure()` includes `self.__class__.__name__`. With
inheritance, a `DictSource` will report `"DictSource"` instead of `"ArrowTableSource"`,
changing content hashes. This is acceptable — the project is pre-v0.1.0 with no
deployed pipelines depending on hash stability. The hash change is a one-time break
that makes the identity more accurate (a DictSource *should* identify as DictSource).

### ListSource initialization order

`ListSource` currently computes `_tag_function_hash` using
`self.data_context.semantic_hasher` before creating the `_arrow_source`. With
inheritance, `self.data_context` requires `super().__init__()` to have run. Solution:
accept `data_context` as a parameter, resolve it to a `DataContext` object before
`super().__init__()`, and pass it along. The `DataContext` resolution function
(`contexts.resolve_context()`) does not require an initialized instance.

### Files affected

- **Modify** `src/orcapod/core/sources/dict_source.py`: inherit from ArrowTableSource.
- **Modify** `src/orcapod/core/sources/list_source.py`: inherit from ArrowTableSource.
- **Modify** `src/orcapod/core/sources/csv_source.py`: inherit from ArrowTableSource.
- **Modify** `src/orcapod/core/sources/data_frame_source.py`: inherit from ArrowTableSource.
- **Modify** `src/orcapod/core/sources/delta_table_source.py`: inherit from ArrowTableSource.
- **Update** tests that assert on content hashes of these sources (hashes will change).

### CachedSource

`CachedSource` wraps a `RootSource` and a cache database. It does NOT delegate to
an internal `ArrowTableSource` — it wraps the source itself. This class is unaffected
by the inheritance refactor.

---

## Migration Notes

### Import updates

All references to `PersistentFunctionNode` become `FunctionNode`.
All references to `PersistentOperatorNode` become `OperatorNode`.
All references to `GraphTracker` become `Pipeline` (or removed).

Per CLAUDE.md: this is a greenfield project pre-v0.1.0 — no backward-compatibility
shims, re-exports, or deprecation wrappers needed.

### GraphNode type alias

After merge:

```python
GraphNode: TypeAlias = SourceNode | FunctionNode | OperatorNode
```

Unchanged — the persistent variants no longer exist as separate types.

### Serialization compatibility

The JSON format does not change. `node_type` values remain `"source"`, `"function"`,
`"operator"`. The `from_descriptor()` classmethods move to `FunctionNode` and
`OperatorNode` (same behavior, different class name).

---

## Testing Strategy

- Existing tests for `PersistentFunctionNode` move to test `FunctionNode` (with DB params)
- Existing tests for `PersistentOperatorNode` move to test `OperatorNode` (with DB params)
- Existing `GraphTracker` tests become Pipeline tests
- New tests: `FunctionNode` and `OperatorNode` without databases (standalone mode)
- New tests: source nodes in `pipeline.compiled_nodes` and attribute access
- New tests: source label defaults to class name
- Serialization tests should pass unchanged (JSON format is the same)
