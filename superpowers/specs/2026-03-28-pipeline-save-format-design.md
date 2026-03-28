# Pipeline Save Format — Normalized Schema with Database Deduplication and Save Levels

**Issue:** ENG-256
**Date:** 2026-03-28
**Status:** Draft

---

## Overview

The current `pipeline.save()` format has two problems:

1. **Database configs are duplicated inline.** Every node that references a database (e.g. `CachedSource.cache_database`) serializes the full config inline. Multiple nodes sharing the same database produce N identical copies.

2. **No verbosity control.** `save()` always produces a full dump. There is no way to produce a lightweight snapshot for consumers like Portolan, nor a portable definition stripped of infrastructure details.

This spec defines the normalized save format addressing both problems, introduces four save levels, and establishes the formal node identity model that underpins the format.

---

## Node Identity Model

Every node carries four identity fields. Understanding their precise meaning is essential to understanding the format.

### The Four Fields

| Field | What it captures | Granularity |
|---|---|---|
| `label` | Human-assigned name within this specific pipeline context | Pipeline-local |
| `pipeline_hash` | Schema + topology fingerprint, ignoring data content. Determines DB path scoping. Merkle chain base for sources. See formal definition below. | Structural |
| `node_uri` | Structured canonical identity of what this node **is** — tuple of strings from general to specific (name → version → type → output schema). At runtime this is also the value of `pod.uri` on the underlying pod/stream object. | Type-level |
| `content_hash` | Full canonical identity fingerprint. Encodes `node_uri` + entire upstream topology + data identity via Merkle chain. See formal definition below. | Instance-level (finest) |

### `node_uri` by Node Type

`node_uri` is a tuple of strings expressing what a node IS, independent of pipeline context. It is identical to `pod.uri` on the underlying runtime object (FunctionPod, operator, or source).

- **Function node**: `["function_name", "function_version", "packet_function_type", "output_schema_repr"]`
- **Operator node**: `["operator_class_name", "operator_config_hash"]`
- **Source node**: `["source_type", "source_id_or_location"]` — e.g. `["delta_table", "/path/to/table"]`, `["dict", "my_source_id"]`

### Formal Definition of `pipeline_hash`

`pipeline_hash` commits to schema and topology only — it ignores data content entirely. It uses the same semantic hasher as `content_hash` but over a reduced identity structure (`pipeline_identity_structure()`) that excludes data.

For **source nodes** (base case):
```
pipeline_hash = semantic_hash(tag_schema, packet_schema)
```

For **non-source nodes**:
```
pipeline_hash = semantic_hash(node_uri, input_asymmetry(input_1.pipeline_hash, input_2.pipeline_hash, ...))
```

`node_uri` contains no data-dependent information — it encodes what the node IS (function name/version, operator class + config hash, etc.) purely structurally. The data-sensitivity in `content_hash` vs `pipeline_hash` comes entirely from what is chained in from upstream: `content_hash` chains upstream `content_hash`es; `pipeline_hash` chains upstream `pipeline_hash`es.

Two nodes with the same `pipeline_hash` will share the same database tables for result storage — this is intentional for same-schema pipelines.

### Formal Definition of `content_hash`

```
content_hash = semantic_hash(node_uri, input_asymmetry(upstream_content_hashes...))
```

For **source nodes** (no upstream inputs — base case of the Merkle chain):
```
content_hash = semantic_hash(node_uri)
```

For **non-source nodes**:
```
content_hash = semantic_hash(node_uri, input_asymmetry(input_1.content_hash, input_2.content_hash, ...))
```

Both `pipeline_hash` and `content_hash` use the project's `SemanticHasher` (not raw SHA-256). `input_asymmetry` maps to the existing `argument_symmetry` mechanism:
- Commutative operators → `frozenset` combination
- Ordered operators → `tuple` combination

### Formal Invariants

These invariants hold **by construction** — `node_uri` is an explicit input to `content_hash`:

```
same content_hash  →  same node_uri         (content identity implies structural identity)
different node_uri →  different content_hash (structural difference forces content difference)
same node_uri      ↛  same content_hash      (same structure, different data = different hash)
```

### Sources and the 1:1 Correspondence

Because source nodes have no upstream inputs, `content_hash = hash(node_uri)`. For sources, `content_hash` and `node_uri` are in **1:1 correspondence**.

Corollary: `node_uri` for inline sources (Dict, List) must include a fingerprint of the actual data content. For external sources (DeltaTable, CSV, PostgreSQL), location + schema fully determines the canonical identity.

### Content Equivalence

`content_hash` is the fingerprint of **content equivalence**: two objects are content-equivalent if one can stand in for the other without any loss of data or functionality. `content_hash` equality is an authoritative claim.

---

## `data_context_key`

Every node belongs to a `DataContext` — a named configuration bundle (semantic hasher, arrow hasher, type converter). The `data_context_key` is the string key identifying which context the node uses, accessed via `node.data_context_key` → `node._data_context.context_key`.

Format: a colon-delimited string, e.g. `"std:v0.1:default"`. The default context used throughout the codebase is `"std:v0.1:default"`.

`data_context_key` is present from `definition` level upward. It is omitted from `minimal` because it is infrastructure detail, not node identity.

---

## `output_schema`

`output_schema` is an object with exactly two keys:
- `"tag"`: maps tag column names to Arrow type strings (e.g. `{"x": "int64", "key": "large_string"}`)
- `"packet"`: maps packet column names to Arrow type strings (e.g. `{"result": "int64", "value": "double"}`)

Arrow type strings follow Arrow's canonical `str(pa.DataType)` format (e.g. `"int64"`, `"large_string"`, `"list<item: int64>"`, `"struct<a: int64, b: string>"`). These are produced by `serialize_schema()` and parsed back by `deserialize_schema()` in `pipeline/serialization.py`.

`output_schema` is present at all four save levels. It is the readable form of the schema information encoded in `pipeline_hash`.

---

## `source_config` — Reconstruction, Not Identity

`source_config` answers one question: **"how do I reconnect to this source?"**

It contains only reconstruction information: file paths, table paths, connection strings, column names. It does **not** contain identity fields (`content_hash`, `pipeline_hash`, `tag_schema`, `packet_schema`) — those belong at the node descriptor level.

On load, if a source cannot be reconstructed, `SourceProxy` is created from the **node descriptor** fields (`content_hash`, `pipeline_hash`, `output_schema`), not from `source_config`.

Verification of successful reconstruction: check that the reconstructed source's `content_hash` matches the stored node descriptor value.

### `reconstructable` field

`reconstructable` is a `bool` evaluated at **save time**. It is `True` if the source type is known to be reconstructable from config alone (i.e. file-backed sources: `csv`, `delta_table`, `cached`). It is `False` for in-memory sources (e.g. `dict`, `list`, `arrow_table`) where the original data is not re-accessible from config.

Loader behavior:
- `reconstructable=True` + `mode="full"`: attempt `resolve_source_from_config(source_config)`. On failure, fall back to `SourceProxy`.
- `reconstructable=False` or `mode="read_only"`: create `SourceProxy` directly from node descriptor fields without attempting reconstruction.

---

## `pipeline_path` — Derived, Never Stored

`pipeline_path` is the storage location for a node's results. It is **never stored** in the save format at any level.

### Canonical Formula

```
pipeline_path = pipeline_name_components + node_uri + (f"schema:{pipeline_hash}", f"instance:{content_hash}")
```

Where:
- `pipeline_name_components` — the pipeline name as a tuple of strings (same as `pipeline.name`)
- `node_uri` — the node's URI tuple (identical to `pod.uri` at runtime)
- `f"schema:{pipeline_hash}"` — groups structurally-identical computations in storage
- `f"instance:{content_hash}"` — isolates each data-distinct run within that group

Example:
```
("my_pipeline", "transform_func", "v0", "python.function.v0", "schema:semantic_v0.1:abc...", "instance:semantic_v0.1:xyz...")
```

The pipeline derives and owns this formula. On load, the pipeline re-derives `pipeline_path` for each node from the identity fields already present in the save format.

**Future direction (ENG-340, ENG-341):** The full architectural goal is to pass each node a pre-scoped database (`db.at(*pipeline_path)`) instead of the path itself, so nodes have no awareness of storage layout at all.

Note: `result_record_path` (the path for function result caching) follows a parallel formula: `result_path_prefix + packet_function.uri`, where `result_path_prefix` is the pipeline-level result prefix passed to `CachedFunctionPod`. It is also never stored.

---

## The Database Registry

All database configs are deduplicated into a top-level `"databases"` registry, keyed by a deterministic short hash of the config.

### Registry Key

```python
import hashlib, json

def make_db_key(config: dict) -> str:
    canonical = json.dumps(config, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(canonical.encode()).hexdigest()[:8]
    return f"db_{digest}"
```

Key properties:
- Same physical database config → always same key (deterministic, cross-session stable)
- 8 hex characters from SHA-256 → collision probability ~1 in 4 billion configs; negligible in practice
- On collision (same 8-char prefix, different configs): append an incrementing integer suffix: `db_a3f9b7c2`, `db_a3f9b7c2_2`, `db_a3f9b7c2_3`, ... (original key is unchanged; conflict keys get the suffix starting at `_2`)

### What Goes in the Registry Per Level

A database config is included in the `"databases"` registry if and only if it is referenced in the save file at that level:

- **`definition`**: databases embedded within a node's own `source_config`, `function_config`, or `operator_config` (e.g. `CachedSource.cache_database`). Pipeline-level databases (`pipeline_database`, `function_database`) are **excluded**. Note: `cache_mode` on operator nodes describes how the operator interacts with the pipeline-level database — it is not an embedded database. Operators that directly embed their own database (analogous to `CachedSource`) would have that database included at `definition` level.
- **`standard`**: definition databases + `pipeline_database` + `function_database`.
- **`full`**: standard databases + observer databases.

Rule: if a database appears in the `"databases"` registry, every reference to it elsewhere in the JSON is replaced by its key string. If `pipeline_database` and `function_database` are the same instance, they share one registry entry.

**`definition` level round-trippability:** A `definition`-level file can reconstruct the full computational graph but not the pipeline-level storage associations. Loading a `definition` file always requires the caller to provide `pipeline_database` and `function_database` explicitly. If `pipeline_database` happened to be the same object as a node-embedded database, its config is still absent from the pipeline block at `definition` level — the registry entry is present (from the node config reference) but the pipeline block's `pipeline_database` key is absent. The loader must always prompt for pipeline-level databases when loading a `definition` file.

### Serialization Interface

Any object that embeds a database extends its `to_config()` / `from_config()` signatures with an optional `db_registry` parameter:

```python
def to_config(self, db_registry: DatabaseRegistry | None = None) -> dict:
    """If db_registry is provided, register embedded databases and emit keys.
    If None (default), embed database configs inline (existing behaviour)."""

@classmethod
def from_config(cls, config: dict, db_registry: DatabaseRegistry | None = None):
    """If db_registry is provided, resolve database key strings to instances.
    If None, expect inline database configs (existing behaviour)."""
```

The pipeline creates a `DatabaseRegistry` at save time, passes it through each node's serialization call, and writes its contents to the top-level `"databases"` block.

---

## `CacheMode` — Operator Node Field

`CacheMode` is an enum controlling operator caching behaviour. Valid values:

| Value | Serialized as | Description |
|---|---|---|
| `CacheMode.OFF` | `"off"` | No cache writes, always compute. Default. |
| `CacheMode.LOG` | `"log"` | Recomputes and writes results to cache as an append-only historical record. |
| `CacheMode.REPLAY` | `"replay"` | Skips computation and flows cached results downstream (for auditing / run comparison). |

`cache_mode` applies to **operator nodes only**. Function nodes do not have a configurable `cache_mode` — their caching is always managed by `CachedFunctionPod`.

---

## Save Levels

`pipeline.save(path, level="standard")` accepts four levels. Each is a strict superset of the previous.

### Level Hierarchy

| Level | Concern | Round-trippable |
|---|---|---|
| `minimal` | *Who* the nodes are and how they connect | No |
| `definition` | *What* each node computes (inner pod/stream definition) | Partial — no pipeline-level databases |
| `standard` | *Where* results are stored (adds pipeline-level database registry) | Yes |
| `full` | *How* it was observed (adds observers + their storage) | Yes |

### `minimal` — Topology + Identity Only

Pure DAG snapshot. No infrastructure details. **Defines the PLT-1161 DAG snapshot schema** (see PLT-1161 Alignment section). Not round-trippable.

```json
{
  "orcapod_pipeline_version": "0.1.0",
  "level": "minimal",
  "pipeline": {
    "name": ["my_pipeline"],
    "run_id": null,
    "snapshot_time": null
  },
  "nodes": {
    "semantic_v0.1:abc...": {
      "node_type": "function",
      "label": "transform",
      "content_hash": "semantic_v0.1:abc...",
      "pipeline_hash": "semantic_v0.1:def...",
      "node_uri": ["transform_func", "v0", "python.function.v0", "..."],
      "output_schema": {
        "tag": {"x": "int64"},
        "packet": {"result": "int64"}
      }
    }
  },
  "edges": [["semantic_v0.1:source_hash...", "semantic_v0.1:abc..."]]
}
```

Each edge is `[upstream_content_hash, downstream_content_hash]` where data flows from upstream to downstream. Self-edges are not permitted.

`pipeline.name` is a tuple of strings of arbitrary length (e.g. `["my_pipeline"]` for a simple name, `["org", "project", "pipeline"]` for a namespaced name).

`run_id` and `snapshot_time` are `null` when saved via `pipeline.save()` outside a run context. When written by PLT-1161 at run start, they are populated with the run identifier and ISO-8601 timestamp respectively.

### `definition` — Full Computational Definition, No Pipeline Storage

Full node configs (inner pod/stream reconstruction info) but no pipeline-level database references. Can be shared and loaded without any infrastructure dependencies; the recipient attaches their own databases.

A `"databases"` block is present only if inner pods/streams embed databases (e.g. `CachedSource.cache_database`); otherwise it is omitted. Pipeline-level databases are absent.

```json
{
  "orcapod_pipeline_version": "0.1.0",
  "level": "definition",
  "databases": {
    "db_a3f9b7c2": { "type": "delta_table", "base_path": "/cache/..." }
  },
  "pipeline": {
    "name": ["my_pipeline"],
    "run_id": null,
    "snapshot_time": null
  },
  "nodes": {
    "semantic_v0.1:abc...": {
      "node_type": "function",
      "label": "transform",
      "content_hash": "semantic_v0.1:abc...",
      "pipeline_hash": "semantic_v0.1:def...",
      "node_uri": ["transform_func", "v0", "python.function.v0", "..."],
      "data_context_key": "std:v0.1:default",
      "output_schema": { "tag": {"x": "int64"}, "packet": {"result": "int64"} },
      "function_config": {
        "packet_function_type_id": "python.function.v0",
        "uri": ["transform_func", "v0", "python.function.v0", "..."],
        "config": {
          "module_path": "mymodule",
          "callable_name": "transform_func",
          "version": "v0",
          "input_packet_schema": {"y": "int64"},
          "output_packet_schema": {"result": "int64"}
        }
      }
    }
  },
  "edges": [["semantic_v0.1:source_hash...", "semantic_v0.1:abc..."]]
}
```

### `standard` (default) — Full Round-Trippable Format

Adds `pipeline_database` and `function_database` references to the `databases` registry. Pipeline can be fully loaded and re-run.

```json
{
  "orcapod_pipeline_version": "0.1.0",
  "level": "standard",
  "databases": {
    "db_a3f9b7c2": { "type": "delta_table", "base_path": "/data/pipeline_db" }
  },
  "pipeline": {
    "name": ["my_pipeline"],
    "run_id": null,
    "snapshot_time": null,
    "pipeline_database": "db_a3f9b7c2",
    "function_database": "db_a3f9b7c2"
  },
  "nodes": {
    "semantic_v0.1:xyz...": {
      "node_type": "operator",
      "label": "join_ab",
      "content_hash": "semantic_v0.1:xyz...",
      "pipeline_hash": "semantic_v0.1:uvw...",
      "node_uri": ["Join", "config_hash_abc"],
      "data_context_key": "std:v0.1:default",
      "output_schema": { "tag": {"key": "large_string"}, "packet": {"value": "int64", "score": "int64"} },
      "operator_config": {
        "class_name": "Join",
        "config": {}
      },
      "cache_mode": "off"
    }
  },
  "edges": [
    ["semantic_v0.1:src_a_hash...", "semantic_v0.1:xyz..."],
    ["semantic_v0.1:src_b_hash...", "semantic_v0.1:xyz..."]
  ]
}
```

### `full` — Standard + Observers

Adds observer configs and observer databases, enabling full reconstruction of pipeline + data + logs.

The observer descriptor schema is **TBD** pending the observer implementation (ENG-290 / PortolanObserver). The known structure is:

```json
{
  "orcapod_pipeline_version": "0.1.0",
  "level": "full",
  "databases": {
    "db_a3f9b7c2": { "type": "delta_table", "base_path": "/data/pipeline_db" },
    "db_b7c2d1e4": { "type": "delta_table", "base_path": "/logs/observer_db" }
  },
  "pipeline": {
    "name": ["my_pipeline"],
    "run_id": null,
    "snapshot_time": null,
    "pipeline_database": "db_a3f9b7c2",
    "function_database": "db_a3f9b7c2",
    "observers": [
      {
        "observer_type": "<observer class name>",
        "database": "db_b7c2d1e4",
        "config": { "...observer-specific fields...": "..." }
      }
    ]
  },
  "nodes": { "...": {} },
  "edges": []
}
```

The observer field schema (fields within each observer object beyond `observer_type`, `database`, `config`) will be finalized when observer serialization is implemented. The `database` field is a registry key and is optional (some observers may not have a database). Multiple observers of the same type are permitted (the list may have duplicates by `observer_type`).

---

## Node Descriptor Field Matrix

Fields present at a lower level carry forward unchanged to all higher levels.

### Shared Fields (All Node Types)

| Field | `minimal` | `definition` | `standard` | `full` |
|---|---|---|---|---|
| `node_type` | ✅ | ✅ | ✅ | ✅ |
| `label` | ✅ | ✅ | ✅ | ✅ |
| `content_hash` | ✅ | ✅ | ✅ | ✅ |
| `pipeline_hash` | ✅ | ✅ | ✅ | ✅ |
| `node_uri` | ✅ | ✅ | ✅ | ✅ |
| `output_schema` | ✅ | ✅ | ✅ | ✅ |
| `data_context_key` | — | ✅ | ✅ | ✅ |

### Source Nodes

| Field | `minimal` | `definition` | `standard` | `full` |
|---|---|---|---|---|
| `source_config` | — | ✅ | ✅ | ✅ |
| `reconstructable` | — | ✅ | ✅ | ✅ |

`source_config` contains reconstruction info only (path, columns, connector settings, etc.). Any embedded database reference is replaced by a registry key string. No identity fields (`content_hash`, `pipeline_hash`, `tag_schema`, `packet_schema`) are included — those are at the node descriptor level.

### Function Nodes

| Field | `minimal` | `definition` | `standard` | `full` |
|---|---|---|---|---|
| `function_config` | — | ✅ | ✅ | ✅ |

`function_config` = packet function type ID, URI tuple, callable reference config (module path, callable name, version, input/output schema). No database references.

### Operator Nodes

| Field | `minimal` | `definition` | `standard` | `full` |
|---|---|---|---|---|
| `operator_config` | — | ✅ | ✅ | ✅ |
| `cache_mode` | — | — | ✅ | ✅ |

`cache_mode` is omitted from `definition` because it describes storage interaction, not the computation itself. Valid values: `"off"`, `"log"`, `"replay"` (see CacheMode section).

---

## Fields Explicitly Not Stored

These fields are deliberately excluded from all save levels:

| Field | Reason |
|---|---|
| `pipeline_path` | Derivable: `pipeline_name_components + node_uri + (f"schema:{pipeline_hash}", f"instance:{content_hash}")`. Pipeline re-derives on load. |
| `result_record_path` | Derivable: `result_path_prefix + packet_function.uri`. Re-derived on load. |
| `content_hash` inside `source_config` | Duplicate of node descriptor `content_hash`. |
| `pipeline_hash` inside `source_config` | Duplicate of node descriptor `pipeline_hash`. |
| `tag_schema` / `packet_schema` inside `source_config` | Duplicate of `output_schema` at node descriptor level. |

---

## Backward Compatibility

None. This is a greenfield pre-v0.1.0 project. Old save files will fail to load with a structure error, which is acceptable at this stage. No migration tooling is provided.

The `"orcapod_pipeline_version"` field holds the **format version**, not the package release version. It remains `"0.1.0"` until the format itself requires a breaking change. Pre-release builds use the same version string.

---

## PLT-1161 Alignment

The `minimal` level defines the PLT-1161 DAG snapshot schema. Both `pipeline.save(level="minimal")` and the PLT-1161 snapshot writer produce output conforming to the same schema, making them interchangeable for consumers like Portolan.

### PLT-1161 Requirements → Minimal Level Fields

| PLT-1161 requirement | Minimal level field |
|---|---|
| Node name (human-readable label) | `nodes[hash].label` |
| Node type (FunctionPod, OperatorPod variant) | `nodes[hash].node_type` |
| Node URI / content hash | `nodes[hash].node_uri`, `nodes[hash].content_hash` |
| Output schema | `nodes[hash].output_schema` |
| Edge list: source → target | `edges` array of `[upstream_hash, downstream_hash]` pairs |
| Pipeline name/path | `pipeline.name` |
| Run ID | `pipeline.run_id` (populated by PLT-1161 at run start; `null` in `pipeline.save()`) |
| Snapshot timestamp | `pipeline.snapshot_time` (populated by PLT-1161; `null` in `pipeline.save()`) |

The only fields present in PLT-1161 snapshots but not in `pipeline.save(level="minimal")` output are `run_id` and `snapshot_time`, which are populated with live values when the snapshot is written at run start. The schema is identical — both cases use the same fields; `pipeline.save()` simply leaves them `null`.

---

## Related Issues

| Issue | Description | Relationship |
|---|---|---|
| ENG-252 | Pipeline save/load (done) | Predecessor implementation |
| ENG-305 | Portolan integration test | Consumer of `minimal` output |
| ENG-320 | Implement normalized save format | Implementation issue for this spec |
| ENG-342 | Fix `pipeline_path` formula in nodes (interim) | Pre-requisite bug fix |
| ENG-341 | `db.at()` path contextualization primitive | Required by ENG-340 |
| ENG-340 | Decouple nodes from `pipeline_path` via ScopedDatabase | Long-term follow-on |
| PLT-1161 | DAG snapshot at run start | Shares `minimal` schema; `run_id`/`snapshot_time` populated at run start |
