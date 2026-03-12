# Pipeline Serialization Design

**Date**: 2026-03-12
**Status**: Approved design, pending implementation

## Overview

Add the ability to save a compiled pipeline to a JSON file and load it back, with support
for a read-only mode where cached data is served without requiring the original function code.

### Goals

1. **`Pipeline.save(path)`** — serialize a compiled pipeline graph to JSON
2. **`Pipeline.load(path, mode="full"|"read_only")`** — reconstruct a pipeline from JSON
3. **Read-only mode** — nodes with unavailable producers serve data from cache databases
4. **Version tracking** — JSON schema version field with loader compatibility check
5. **Extensibility** — registries for databases, sources, and operators; protocol-level
   `to_config()` / `from_config()` for packet functions and operators

### Non-goals (current iteration)

- Archive/bundle packaging (directory with embedded data)
- Inline data embedding in JSON
- JSON schema migration between versions
- Execution engine serialization
- Selective subgraph loading

---

## JSON Schema Structure

### Top-level format

```json
{
  "orcapod_pipeline_version": "0.1.0",
  "pipeline": {
    "name": ["my_pipeline"],
    "databases": {
      "pipeline_database": { ... },
      "function_database": null
    }
  },
  "nodes": {
    "<content_hash>": { ... node descriptor ... }
  },
  "edges": [
    ["<upstream_content_hash>", "<downstream_content_hash>"]
  ]
}
```

Note: `data_context_key` is a per-node property, not a pipeline-level one. Different nodes
in the same pipeline may use different data contexts. Each node descriptor carries its own
`data_context_key`.

**Key decisions:**

- Nodes are keyed by `content_hash` string — matches the existing `GraphTracker._node_lut`
  and `_hash_graph` keys.
- Database paths are stored as-is (preserving absolute or relative as originally configured
  in the database constructor). No automatic path rewriting.
- `orcapod_pipeline_version` enables future format changes. The loader checks this against
  its supported versions and raises immediately on mismatch.
- Edges are a flat list of `[upstream, downstream]` content_hash pairs, matching
  `GraphTracker._graph_edges`.

---

## Node Descriptors

Save/load operates on the **compiled** pipeline graph, which contains `SourceNode`,
`PersistentFunctionNode`, and `PersistentOperatorNode` instances. The non-persistent
variants (`FunctionNode`, `OperatorNode`) lack database awareness and are not serialized.
For brevity, this spec uses "FunctionNode" and "OperatorNode" to refer to their persistent
variants in the context of serialization.

### Common fields (all node types)

```json
{
  "node_type": "source | function | operator",
  "label": "my_node",
  "content_hash": "abc123...",
  "pipeline_hash": "def456...",
  "data_context_key": "std:v0.1:default",
  "output_schema": {
    "tag": {"subject_id": "int64", "session": "large_string"},
    "packet": {"signal": "large_list<float>", "duration": "float64"}
  }
}
```

Schemas are serialized using Arrow type strings via `DataContext.type_converter`, ensuring
consistency with the rest of the system.

### SourceNode descriptor

`SourceNode` wraps a generic `StreamProtocol` (not necessarily a source class). At save
time, the serializer inspects the wrapped stream via `isinstance` checks to determine its
concrete type and extract the appropriate config:

- If the stream is a `RootSource` subclass (CSVSource, DeltaTableSource, etc.), serialize
  using its `to_config()` method.
- If the stream is a `CachedSource`, serialize both the inner source config and the cache
  database config. The `source_type` is set to `"cached"` with a nested `inner_source`
  descriptor.
- If the stream is a bare `ArrowTableStream` or other non-source `StreamProtocol`, mark
  as `reconstructable: false` and store only the output schema metadata.

```json
{
  "node_type": "source",
  "stream_type": "csv | delta_table | dict | arrow_table | list | data_frame | cached | stream",
  "source_id": "my_source",
  "reconstructable": true,
  "source_config": {
    "file_path": "/data/subjects.csv",
    "tag_columns": ["subject_id"],
    "system_tag_columns": [],
    "record_id_column": null
  }
}
```

- `source_config` varies by `stream_type` (e.g., `delta_table_path` for DeltaTableSource,
  `name` + `tag_function_hash_mode` for ListSource).
- `reconstructable` is set at save time: `true` for file-backed sources (CSV, Delta),
  `false` for in-memory sources (Dict, List, DataFrame, ArrowTable) and bare streams.
- In-memory sources and bare streams cannot be reconstructed from JSON alone. The
  descriptor captures metadata but not the data itself. Downstream nodes with cached
  results remain accessible in read-only mode.

### FunctionNode descriptor

```json
{
  "node_type": "function",
  "function_pod": {
    "uri": ["my_module.transform", "schema_hash", "v0", "python"],
    "packet_function": {
      "packet_function_type_id": "python",
      "config": {
        "module_path": "my_module",
        "callable_name": "transform",
        "version": "0.5.2",
        "input_packet_schema": {"signal": "large_list<float>"},
        "output_packet_schema": {"processed": "large_list<float>", "duration": "float64"}
      }
    },
    "node_config": null
  },
  "pipeline_path": ["my_pipeline", "my_module.transform", "schema_hash", "v0", "python", "node:def456"],
  "result_record_path": ["my_pipeline", "_result", "my_module.transform", "schema_hash", "v0", "python"],
  "execution_engine_opts": null
}
```

- `pipeline_path` and `result_record_path` allow the loader to locate cached results in
  the databases without recomputing them.
- `result_record_path` is computed at save time by accessing
  `PersistentFunctionNode._packet_function.record_path` (the `CachedPacketFunction`
  exposes the full record path as a property).
- `packet_function.config` is produced by `PacketFunctionProtocol.to_config()` — each
  implementation controls its own serialization.
- `packet_function_type_id` is used to dispatch to the correct class for `from_config()`.
- `execution_engine_opts` is a pass-through JSON-serializable dict of per-node resource
  overrides (e.g., `{"num_cpus": 4}`). This is distinct from the execution engine itself
  (which is not serialized). Stored so that a re-executed pipeline can apply the same
  resource configuration.

### OperatorNode descriptor

```json
{
  "node_type": "operator",
  "operator": {
    "class_name": "Batch",
    "module_path": "orcapod.core.operators.batch",
    "config": {
      "batch_size": 10,
      "drop_partial_batch": false
    }
  },
  "cache_mode": "LOG",
  "pipeline_path": ["my_pipeline", "Batch", "content_hash_hex", "node:abc123"]
}
```

- `operator.config` is produced by `OperatorPodProtocol.to_config()` — each operator
  controls its own serialization.
- `class_name` is used to look up the class in the operator registry, then
  `cls.from_config(config)` reconstructs it.

---

## Protocol Extensions

### PacketFunctionProtocol

```python
class PacketFunctionProtocol(Protocol):
    # ... existing methods ...

    def to_config(self) -> dict[str, Any]:
        """Serialize this packet function to a JSON-compatible config dict."""
        ...

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> Self:
        """Reconstruct a packet function from a config dict."""
        ...
```

Each `PacketFunction` subclass decides what goes in its config. `PythonPacketFunction`
stores module path + callable name + version + schemas. A hypothetical future
`SQLPacketFunction` might store a query string. The `packet_function_type_id` field
identifies which class to dispatch `from_config()` to.

### OperatorPodProtocol

```python
class OperatorPodProtocol(Protocol):
    # ... existing methods ...

    def to_config(self) -> dict[str, Any]:
        """Serialize this operator to a JSON-compatible config dict."""
        ...

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> Self:
        """Reconstruct an operator from a config dict."""
        ...
```

Each operator owns its serialization. `Batch` stores `batch_size` and
`drop_partial_batch`. `Join` stores nothing. `SemiJoin` stores nothing but its
non-commutative argument order is preserved via the edge ordering in the JSON (the
`upstreams` tuple order matters for binary non-commutative operators).

`PolarsFilter` serialization: Polars expressions that are `pl.Expr` instances can be
serialized via `Expr.meta.serialize()` / `Expr.deserialize()`. However, `PolarsFilter`
also accepts non-Expr predicates (booleans, numpy arrays) and `constraints` dicts. If
`to_config()` encounters non-serializable predicates, it should mark the operator as
non-reconstructable (set a `"reconstructable": false` flag in the config), allowing the
loader to fall back to read-only mode for that node.

### FunctionPodProtocol

`FunctionPod` also gets `to_config()` / `from_config()` that wraps the packet function's
config with pod-level metadata (node_config, etc.).

### ArrowDatabaseProtocol

`to_config()` and `from_config()` become official methods on `ArrowDatabaseProtocol`:

```python
@runtime_checkable
class ArrowDatabaseProtocol(Protocol):
    # ... existing methods (add_record, get_all_records, flush, etc.) ...

    def to_config(self) -> dict[str, Any]:
        """Serialize database configuration to a JSON-compatible dict.

        The returned dict must include a ``"type"`` key identifying the
        database implementation (e.g., ``"delta_table"``, ``"in_memory"``).
        """
        ...

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> Self:
        """Reconstruct a database instance from a config dict."""
        ...
```

Every database implementation (`DeltaTableDatabase`, `InMemoryArrowDatabase`,
`NoOpArrowDatabase`) implements these as concrete methods. The `"type"` key in the
returned config dict is used by the `DATABASE_REGISTRY` during deserialization to
dispatch to the correct class.

### SourceProtocol

`to_config()` and `from_config()` become official methods on `SourceProtocol`:

```python
@runtime_checkable
class SourceProtocol(StreamProtocol, Protocol):
    # ... existing methods (source_id, resolve_field) ...

    def to_config(self) -> dict[str, Any]:
        """Serialize source configuration to a JSON-compatible dict.

        The returned dict must include a ``"source_type"`` key identifying
        the source implementation (e.g., ``"csv"``, ``"delta_table"``).
        For in-memory sources, ``to_config()`` captures metadata but not
        the data itself.
        """
        ...

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> Self:
        """Reconstruct a source instance from a config dict.

        Raises ``NotImplementedError`` for in-memory source types that
        cannot be reconstructed without the original data.
        """
        ...
```

Every source implementation (`CSVSource`, `DeltaTableSource`, `DictSource`, etc.)
implements these as concrete methods. For in-memory sources (DictSource, ListSource,
DataFrameSource, ArrowTableSource), `to_config()` captures metadata (tag_columns,
source_id, etc.) but not the data itself. `from_config()` raises
`NotImplementedError` for these types, which the loader catches and falls back to
read-only.

---

## Registries

Simple dictionaries mapping type strings to classes, used during deserialization:

```python
DATABASE_REGISTRY: dict[str, type] = {
    "delta_table": DeltaTableDatabase,
    "in_memory": InMemoryArrowDatabase,
    "noop": NoOpArrowDatabase,
}
# Note: InMemoryArrowDatabase has no persistent state. Reconstructing it from
# config produces an empty database, so all nodes depending solely on it will
# have load_status UNAVAILABLE in read-only mode. The loader should emit a
# warning when encountering an in_memory database in the JSON.

SOURCE_REGISTRY: dict[str, type] = {
    "csv": CSVSource,
    "delta_table": DeltaTableSource,
    "dict": DictSource,
    "list": ListSource,
    "data_frame": DataFrameSource,
    "arrow_table": ArrowTableSource,
}

OPERATOR_REGISTRY: dict[str, type] = {
    "Join": Join,
    "MergeJoin": MergeJoin,
    "SemiJoin": SemiJoin,
    "Batch": Batch,
    "SelectTagColumns": SelectTagColumns,
    "DropTagColumns": DropTagColumns,
    "SelectPacketColumns": SelectPacketColumns,
    "DropPacketColumns": DropPacketColumns,
    "MapTags": MapTags,
    "MapPackets": MapPackets,
    "PolarsFilter": PolarsFilter,
}
```

Registries are extensible — users with custom implementations can register them before
loading via a `register_database()` / `register_source()` / `register_operator()` function.

Packet functions use a small dispatch mapping from `packet_function_type_id` to the
concrete class:

```python
PACKET_FUNCTION_REGISTRY: dict[str, type] = {
    "python": PythonPacketFunction,
}
```

This is intentionally minimal — unlike operators, packet functions are user-defined and
resolved via `importlib` using the module path stored in their config. The registry maps
the `packet_function_type_id` string to the class that implements `from_config()`. Users
with custom `PacketFunction` subclasses can register them before loading.

---

## Read-Only Node Construction

### LoadStatus enum

```python
class LoadStatus(Enum):
    FULL = "full"              # producer available, fully operational
    READ_ONLY = "read_only"    # producer missing, cache available
    UNAVAILABLE = "unavailable" # producer missing, cache also missing
```

### `from_descriptor()` classmethod

Each node class gains an alternative constructor:

```python
@classmethod
def from_descriptor(
    cls,
    descriptor: dict[str, Any],
    producer: ProducerType | None,  # function_pod, operator, or stream
    upstreams: tuple[StreamProtocol, ...],
    databases: dict[str, ArrowDatabaseProtocol],
) -> Self:
    ...
```

**When producer is `None` (read-only mode):**

- The node stores descriptor metadata (hashes, schemas, uri, pipeline_path)
- `output_schema()` returns schemas reconstructed from the descriptor's Arrow type strings
- `pipeline_hash()` / `content_hash()` return stored hash values from the descriptor
- `iter_packets()` / `as_table()` read from the cache database using the stored
  `pipeline_path`
- `run()` is a no-op
- `process_packet()` raises an error explaining the producer is unavailable
- `load_status` returns `READ_ONLY` if cache exists, `UNAVAILABLE` otherwise

**When producer is available (full mode):**

- The node behaves exactly as it does today
- Stored hashes can be used for validation (warn if recomputed hashes don't match)
- `load_status` returns `FULL`

---

## Pipeline.save() Flow

Called on a compiled pipeline. Precondition: pipeline must be compiled (raises if not).

1. **Serialize pipeline metadata** — name, database configs via `db.to_config()`
2. **Walk nodes in topological order** — for each node in `_persistent_node_map`:
   - Build common fields: label, content_hash, pipeline_hash, data_context_key,
     output_schema (serialized with Arrow type strings)
   - Build type-specific fields:
     - SourceNode: source type, source_id, reconstructable flag, `source.to_config()`
     - PersistentFunctionNode: `function_pod.to_config()`, pipeline_path,
       result_record_path, execution_engine_opts
     - PersistentOperatorNode: `operator.to_config()`, cache_mode, pipeline_path
3. **Serialize edges** from `_graph_edges`
4. **Write JSON** to the given path

## Pipeline.load() Flow

Class method: `Pipeline.load(path, mode="full") -> Pipeline`

1. **Read and validate JSON** — parse file, check `orcapod_pipeline_version` against
   supported versions. Raise on mismatch.
2. **Reconstruct databases** — look up type in `DATABASE_REGISTRY`, call
   `cls.from_config(config)`.
3. **Walk nodes in topological order** (derive order from edges):
   - **SourceNode**: If `reconstructable` is true and `mode != "read_only"`, attempt
     `source_cls.from_config(source_config)`. On failure, fall back to read-only.
     If `reconstructable` is false, use `from_descriptor` directly.
   - **FunctionNode**: If `mode == "full"`, attempt to reconstruct via
     `PacketFunction.from_config()` using `packet_function_type_id` to find the class.
     On failure, fall back to `from_descriptor(function_pod=None)`.
     If `mode == "read_only"`, skip reconstruction, use `from_descriptor` directly.
   - **OperatorNode**: If `mode == "full"` or `mode == "read_only"`, attempt to
     reconstruct via `OPERATOR_REGISTRY[class_name].from_config(config)` (operators are
     built-in, so this should usually succeed). On failure, fall back to
     `from_descriptor(operator=None)`.
4. **Wire upstream references** — use content_hash lookups to connect nodes (same pattern
   as `Pipeline.compile()`).
5. **Build node graph and assign labels** — reconstruct `_node_graph` and `_nodes` dict
   from the loaded nodes.
6. **Return loaded Pipeline** — already in compiled state, with `load_status` available
   on each node.

**Content hash mismatch during load:** When a source is successfully reconstructed in full
mode but its recomputed `content_hash` differs from the stored value (e.g., because the
CSV file was modified), the loader should emit a warning. The node still loads using the
reconstructed source, but downstream cache lookups may miss because the hash-based
pipeline paths have changed. This is expected behavior — the user can re-run the pipeline
to recompute. Full hash validation is documented as future work.

### Mode behavior summary

| Mode | Sources | Operators | Function Pods |
|------|---------|-----------|---------------|
| `"full"` | Reconstruct if possible, degrade gracefully | Reconstruct (usually succeeds) | Attempt import via `from_config()`, degrade to read-only on failure |
| `"read_only"` | Reconstruct if possible, degrade gracefully | Reconstruct (usually succeeds) | Skip reconstruction, always read-only |

Both modes attempt source and operator reconstruction since these are typically file-backed
or built-in. The key difference is whether function pod import is attempted.

---

## Future Work

Documented here for planning purposes. Not in scope for the current iteration.

### Archive/bundle packaging

`Pipeline.export(path)` creates a directory containing:
- `manifest.json` (the pipeline JSON)
- Copies of relevant database tables scoped to this pipeline's data

This makes the pipeline fully self-contained and portable without requiring access to the
original database paths. Natural evolution of `Pipeline.save()` — the JSON format is the
same, only the database paths point to local copies within the bundle.

### Inline data embedding

Small sources and cached results can optionally be embedded directly in the JSON as
base64-encoded Arrow IPC buffers. Controlled by a size threshold parameter on
`Pipeline.save()`. Useful for small pipelines where a single file is more convenient
than a directory bundle.

### JSON schema migration

When `orcapod_pipeline_version` changes, a migration system transforms old JSON formats
to the current format. For now, we just version-check and error on mismatch.

### Hash validation on load

In full mode, after reconstructing all nodes, recompute content_hash and pipeline_hash
for each node and compare against the stored values. A mismatch warns that the underlying
code or data has changed since the pipeline was saved.

### Selective node loading

Load only a subgraph of the pipeline (e.g., everything upstream of a specific node).
Useful for large pipelines where only a portion is of interest.

### Execution engine serialization

Store execution engine configuration (e.g., Ray cluster address, resource requirements)
in the JSON so that `Pipeline.load(...).run()` can re-execute with the same engine setup.
