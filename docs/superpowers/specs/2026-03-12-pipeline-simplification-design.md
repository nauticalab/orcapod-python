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

## 7. Source Refactor: Compositional Builder Pattern

### Current pattern

Each delegating source (DictSource, ListSource, CSVSource, DataFrameSource,
DeltaTableSource) creates an internal `_arrow_source: ArrowTableSource` in `__init__`
and delegates ~7 methods to it. This causes:

- Awkward `source_id` threading (passed to both wrapper and inner source)
- `to_config()` reaches into `_arrow_source` private attributes
- Boilerplate delegation for `output_schema`, `keys`, `iter_packets`, `as_table`,
  `source_id`, `computed_label`, `identity_structure`, `resolve_field`

Inheritance from `ArrowTableSource` was considered but rejected due to a fundamental
init order problem: `data_context` (providing `type_converter`, `semantic_hasher`,
`arrow_hasher`) is not available until `super().__init__()` completes via
`DataContextMixin`, but subclasses need these services to build their Arrow table
*before* passing it to `super().__init__()`.

### New pattern: SourceStreamBuilder (composition)

A small builder class extracts the enrichment pipeline from `ArrowTableSource.__init__`
into a reusable component. All sources — including `ArrowTableSource` itself — use the
builder after `super().__init__()` completes.

**New file: `src/orcapod/core/sources/stream_builder.py`**

```python
@dataclass(frozen=True)
class SourceStreamResult:
    """Artifacts produced by SourceStreamBuilder.build()."""
    stream: ArrowTableStream
    schema_hash: str
    table_hash: ContentHash
    source_id: str              # defaulted to table_hash hex if not provided
    tag_columns: tuple[str, ...]
    system_tag_columns: tuple[str, ...]


class SourceStreamBuilder:
    """Builds an enriched ArrowTableStream from a raw Arrow table.

    Encapsulates the full enrichment pipeline:
    1. Drop system columns from raw input
    2. Validate tag_columns and record_id_column
    3. Compute schema hash (via type_converter + semantic_hasher)
    4. Compute table hash (via arrow_hasher)
    5. Default source_id to table hash if not provided
    6. Build per-row source-info strings
    7. Add source-info provenance columns
    8. Add system tag columns (schema hash, source_id, record_id)
    9. Wrap in ArrowTableStream
    """

    def __init__(self, data_context: DataContext, config: Config):
        self._data_context = data_context
        self._config = config

    def build(
        self,
        table: pa.Table,
        tag_columns: Collection[str],
        source_id: str | None = None,
        record_id_column: str | None = None,
        system_tag_columns: Collection[str] = (),
    ) -> SourceStreamResult:
        """Run the full enrichment pipeline and return the result."""
        ...
```

The builder takes `DataContext` and `Config` as constructor args (both available on any
source after `super().__init__()`) and produces a `SourceStreamResult` containing the
enriched stream plus metadata needed by the source.

**RootSource gains default stream delegation and default identity:**

```python
class RootSource(StreamBase):
    _stream: ArrowTableStream  # set by subclass __init__ after builder

    # Default stream delegation — concrete implementations for the 4 abstract
    # stream methods. CachedSource and DerivedSource override these.
    def output_schema(self, *, columns=None, all_info=False):
        return self._stream.output_schema(columns=columns, all_info=all_info)

    def keys(self, *, columns=None, all_info=False):
        return self._stream.keys(columns=columns, all_info=all_info)

    def iter_packets(self):
        return self._stream.iter_packets()

    def as_table(self, *, columns=None, all_info=False):
        return self._stream.as_table(columns=columns, all_info=all_info)

    # Default identity — works for ArrowTableSource, DictSource, CSVSource,
    # DataFrameSource, DeltaTableSource. ListSource, CachedSource, DerivedSource
    # override.
    def identity_structure(self):
        return (self.__class__.__name__, self.output_schema(), self.source_id)
```

These defaults work because:
- CachedSource and DerivedSource already override all 4 stream methods and
  `identity_structure()` — they shadow the defaults.
- `_stream` is always set during `__init__` of builder-based sources before the
  object is usable.
- `identity_structure()` includes `self.__class__.__name__`, so each source type
  (DictSource, CSVSource, etc.) naturally gets a distinct identity.
- `RootSource` remains abstract via `ContentIdentifiableBase` (which had
  `identity_structure` as abstract) — but now it's concrete on RootSource. The class
  is still non-instantiable in practice since it has no `__init__` that sets `_stream`.

**ArrowTableSource becomes:**

```python
class ArrowTableSource(RootSource):
    def __init__(self, table, tag_columns, record_id_column=None, **kwargs):
        super().__init__(**kwargs)
        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        result = builder.build(
            table, tag_columns,
            source_id=self._source_id,
            record_id_column=record_id_column,
        )
        self._stream = result.stream
        self._schema_hash = result.schema_hash
        self._table_hash = result.table_hash
        self._tag_columns = result.tag_columns
        self._system_tag_columns = result.system_tag_columns
        if self._source_id is None:
            self._source_id = result.source_id
    # No output_schema, keys, iter_packets, as_table, identity_structure needed
    # — all inherited from RootSource defaults
```

**DictSource becomes:**

```python
class DictSource(RootSource):
    def __init__(self, data, tag_columns, data_schema=None, **kwargs):
        super().__init__(**kwargs)
        table = self.data_context.type_converter.python_dicts_to_arrow_table(
            [dict(row) for row in data], python_schema=data_schema,
        )
        builder = SourceStreamBuilder(self.data_context, self.orcapod_config)
        result = builder.build(table, tag_columns, source_id=self._source_id)
        self._stream = result.stream
        self._tag_columns = result.tag_columns
        if self._source_id is None:
            self._source_id = result.source_id
    # No delegation methods needed — inherited from RootSource
```

**Per-source changes:**

- **ArrowTableSource**: uses builder instead of inline enrichment. Stores
  `_schema_hash`, `_table_hash`, `_tag_columns` from builder result.
  `resolve_field()` removed (see below). Stream/identity methods inherited.
- **DictSource**: converts dicts → Arrow table, calls builder. Stores `_tag_columns`
  for `to_config()`. No delegation methods.
- **ListSource**: builds rows from elements + tag function, converts to Arrow table,
  calls builder. Keeps custom `identity_structure()` (includes tag function hash).
  `_hash_tag_function()` uses `self.data_context.semantic_hasher` — works because
  `super().__init__()` has already run.
- **CSVSource**: reads CSV → Arrow table, calls builder. Stores `_file_path`,
  `_tag_columns`, `_system_tag_columns`, `_record_id_column` for `to_config()`/
  `from_config()`.
- **DataFrameSource**: converts DataFrame → Arrow table, calls builder.
  Stores `_tag_columns` for `to_config()`.
- **DeltaTableSource**: reads Delta table → Arrow table, calls builder. Stores
  `_delta_table_path`, `_tag_columns`, `_system_tag_columns`, `_record_id_column`
  for `to_config()`/`from_config()`.

**Methods eliminated from all delegating sources:**
- All `computed_label()` overrides (removed per section 6)
- All `source_id` property overrides (no longer needed — `_source_id` set directly)
- All `output_schema()`, `keys()`, `iter_packets()`, `as_table()` delegations
- All `identity_structure()` delegations (except ListSource which has custom logic)
- All `resolve_field()` delegations (see below)

**Methods retained per source:**
- `to_config()` — each source serializes its own constructor args (no longer
  reaches into `_arrow_source` private attributes — stores its own copies)
- `from_config()` — CSVSource and DeltaTableSource only
- `identity_structure()` — ListSource only (includes tag function hash)

### resolve_field: deferred redesign

`resolve_field` is removed from all sources. `RootSource.resolve_field()` raises
`NotImplementedError` (replacing the current `FieldNotResolvableError`). The detailed
lookup logic currently in `ArrowTableSource.resolve_field()` is deleted. This will be
redesigned separately — the current implementation is tightly coupled to
`ArrowTableSource`'s internal `_data_table` and `_record_id_column`, and the right
abstraction for field resolution across source types needs fresh thought.

### Hash stability

Same as before: `identity_structure()` includes `self.__class__.__name__`. Sources
that previously reported as `"ArrowTableSource"` (via delegation) will now report
their own class name. This is an acceptable one-time hash break (pre-v0.1.0).

### Files affected

- **Create** `src/orcapod/core/sources/stream_builder.py`: `SourceStreamBuilder`,
  `SourceStreamResult`, `_make_record_id` (moved from `arrow_table_source.py`).
- **Modify** `src/orcapod/core/sources/base.py`: add default stream delegation
  methods, default `identity_structure()`, change `resolve_field()` to raise
  `NotImplementedError`.
- **Modify** `src/orcapod/core/sources/arrow_table_source.py`: use builder, remove
  enrichment logic, remove `resolve_field()`, remove stream/identity methods.
- **Modify** `src/orcapod/core/sources/dict_source.py`: use builder, remove
  delegation.
- **Modify** `src/orcapod/core/sources/list_source.py`: use builder, remove
  delegation, keep custom `identity_structure()`.
- **Modify** `src/orcapod/core/sources/csv_source.py`: use builder, remove
  delegation.
- **Modify** `src/orcapod/core/sources/data_frame_source.py`: use builder, remove
  delegation.
- **Modify** `src/orcapod/core/sources/delta_table_source.py`: use builder, remove
  delegation.
- **Modify** `src/orcapod/core/sources/cached_source.py`: remove `resolve_field()`
  delegation (inherits `NotImplementedError` from RootSource).
- **Update** tests that assert on content hashes (hashes will change).
- **Update** tests that call `resolve_field()` — expect `NotImplementedError`.

### CachedSource

`CachedSource` wraps a `RootSource` and a cache database. It does NOT use the
builder — it wraps the source itself and overrides all stream methods. Unaffected
by the builder refactor except for `resolve_field()` removal.

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
