# Hashing Reference

This document is a developer-facing reference for every hash computation site in the
orcapod-python framework. For conceptual background on the two identity chains
(`content_hash` and `pipeline_hash`), see [Identity & Hashing](../concepts/identity.md).

---

## Hash Site Index

The table below summarises all 14 hash computation sites across the six usage groups
documented in the sections that follow.

| # | Site | Algorithm | Output format | One-line guarantee |
|---|------|-----------|---------------|--------------------|
| 1 | `content_hash()` | `SemanticAwarePythonHasher` → JSON + SHA-256 | `ContentHash` | Unique per semantic content of the object; data-inclusive |
| 2 | `pipeline_hash()` | `SemanticAwarePythonHasher` → JSON + SHA-256 | `ContentHash` | Unique per pipeline topology + schema; data-exclusive |
| 3 | Schema hash (system tag column naming) | `SemanticAwarePythonHasher.hash_object((tag_schema, data_schema)).to_hex(n)` | Truncated hex `str` | Unique per `(tag_schema, data_schema)` pair; embedded in system tag column names |
| 4 | Default `source_id` | `StarfixArrowHasher.hash_table(table).to_hex(n)` | Truncated hex `str` | Unique per raw table content; used as source identifier when none is provided |
| 5 | Per-row `record_id` (system tag value) | `uuid.uuid5(NAMESPACE, f"{source_id}::{provenance_token}")` | `bytes` (16, UUID v5) | Deterministic per `(source_id, row_identity)`; stable across re-runs of the same source |
| 6 | Join system tag suffix | `stream.pipeline_hash().to_hex(n)` + `:{idx}` appended to column name | Column name suffix | Unique per `(input topology, canonical join position)`; encodes full join lineage in column name |
| 7 | `compute_base_entry_id()` | `StarfixArrowHasher.hash_table(system_tags + INPUT_DATA_HASH_COL)` | `bytes` (`b"method:digest"`) | Unique per `(node, tag lineage, input_data content)` across all recomputation attempts |
| 8 | `compute_pipeline_entry_id()` | `StarfixArrowHasher.hash_table(system_tags + INPUT_DATA_HASH_COL + recomputation_index)` | `bytes` (`b"method:digest"`) | Unique per `(node, tag lineage, input_data content, recomputation attempt)` |
| 9 | Side-effect `record_id` | `StarfixArrowHasher.hash_table(system_tags + INPUT_DATA_HASH_COL + NODE_CONTENT_HASH_COL + recomputation_index=0)` | `bytes` | Unique per `(tag lineage, input_data content, pod version)` |
| 10 | `invocation_hash` | `f"{serialize(pipeline_hash_ch)}::{serialize(record_id_hash_ch)}"` | `str` | Unique per `(pod topology, tag lineage, input_data content, pod version)` |
| 11 | Output schema hash | `SemanticAwarePythonHasher.hash_object(output_data_schema).to_string()` | `str` (ContentHash `.to_string()`) | Unique per output schema definition; embedded in function URI |
| 12 | `run_id` | `uuid.uuid4().hex[:16]` | 16-char hex `str` | Non-deterministic; unique per pipeline execution |
| 13 | `snapshot_hash` | `hashlib.sha256(sorted_leaf_content_hash_strings joined by newline).hexdigest()[:16]` | 16-char hex `str` | Unique per `(DAG leaf topology + data state)` at run time |
| 14 | `datagram_uuid` | `uuid7()` normalised to `stdlib uuid.UUID` | `uuid.UUID` | Unique per datagram instance; time-ordered; not a content hash |

---

## 1. Framework Object Identity

Every class that inherits from `TraceableBase` carries both a `content_hash()` and a
`pipeline_hash()`. Both use `SemanticAwarePythonHasher`, which recursively expands the
object's identity structure into a JSON-serialisable representation and takes its
SHA-256 digest.

### Algorithm — `SemanticAwarePythonHasher`

1. Call `identity_structure()` (for content hash) or `pipeline_identity_structure()` (for pipeline hash) on the object.
2. Recursively expand the structure: `ContentIdentifiable` objects are replaced by their hash via a **resolver callback** (see below); containers (list, dict, tuple, set) are serialised to nested JSON.
3. JSON-serialise the expanded structure and compute `hashlib.sha256(json_bytes).digest()` → `ContentHash`.

**Resolver pattern:** The resolver callback is threaded through the entire recursive hash computation to ensure one consistent context per call:

- **`content_resolver`** (used by `content_hash()`): routes every `ContentIdentifiable` leaf through `leaf.content_hash(hasher)`.
- **`pipeline_resolver`** (used by `pipeline_hash()`): routes `PipelineElementProtocol` objects through `leaf.pipeline_hash(hasher)`; routes all other `ContentIdentifiable` objects (e.g. raw data values) through `leaf.content_hash(hasher)`.

Both hashes are **cached by `hasher_id`** on each object, so repeated calls are free.

---

### 1a. `content_hash()` — data-inclusive identity

The table below shows what each class returns from `identity_structure()`.

| Class | `identity_structure()` return value | Notes |
|-------|--------------------------------------|-------|
| `Datagram` | `self._ensure_data_table()` — the raw Arrow table | Dispatched to `ArrowTableHandler` → `StarfixArrowHasher` |
| `Tag` | User tag columns only (raw Arrow table) | System tag columns (`_tag::*`) excluded — they are provenance metadata, not tag content |
| `Data` | Data columns only (raw Arrow table) | Source info columns (`_source_*`) excluded — they are provenance metadata, not data content |
| `EmptyData` | Raises `EmptyDataAccessError` | `content_hash()` is overridden to return a stored `cached_content_hash` directly |
| `DataFunctionBase` | `self.uri` — `(canonical_function_name, output_schema_hash, major_version, data_function_type_id)` | `output_schema_hash` is site 11; see §5 |
| `FunctionPod` | `(self.data_function,)` when no ctx arg; `(self.data_function, self._ctx_arg_name)` when ctx arg present | `ctx_arg_name` is included here so a ctx-aware pod has a distinct `content_hash` from a regular pod using the same data function |
| `ArrowTableStream` | `(producer, argument_symmetry(upstreams))` | Falls back to table content hash if no producer |
| `RootSource` (`ArrowTableSource`) | Class name + tag columns + table content hash | Data-inclusive base case of the Merkle chain |
| `DerivedSource` | Origin node's `content_hash` | Inherits its generating node's identity |
| Operators (unary) | `(operator_class_name, upstream_stream)` | Stream reference resolved via `content_resolver` |
| Operators (binary/N-ary) | `(operator_class_name, argument_symmetry(streams))` | `frozenset` for commutative (Join, MergeJoin); `tuple` for ordered (SemiJoin) |

**Known exclusions from `identity_structure()`:**

| Class | Excluded | Reason |
|-------|----------|--------|
| `Tag` | System tag columns (`_tag::*`) | Provenance metadata, not content |
| `Data` | Source info columns (`_source_*`) | Provenance metadata, not content |
| `EmptyData` | All data | No payload; `content_hash()` overridden to return stored hash |

---

### 1b. `pipeline_hash()` — schema and topology only

The pipeline hash uses the same `SemanticAwarePythonHasher` with the **`pipeline_resolver`**. The key difference is what each class returns from `pipeline_identity_structure()`.

| Class | `pipeline_identity_structure()` return value | Notes |
|-------|----------------------------------------------|-------|
| `RootSource` | `(tag_schema, data_schema)` | Base case — schemas only, no data content |
| `DerivedSource` | `(tag_schema, data_schema)` | Acts as a new root in the pipeline Merkle chain |
| `DataFunctionBase` | `self.uri` (same as `identity_structure()`) | Function identity is already schema-only |
| `FunctionPod` | `self.data_function` **only** (excludes `ctx_arg_name`) | A ctx-aware pod and a regular pod sharing the same data function share a `pipeline_hash` and therefore the same DB table path |
| `ArrowTableStream` | `(producer, argument_symmetry(upstreams pipeline hashes))` | `pipeline_resolver` routes upstreams through `pipeline_hash()` |
| Operators | `(operator_class_name, argument_symmetry(upstream pipeline hashes))` | Same structure as content identity but using pipeline hashes of upstreams |
| `SideEffectPodStream` | `(pod, argument_symmetry(upstreams))` | Same as `identity_structure()` |

> **Note:** `FunctionPod.ctx_arg_name` IS included in `content_hash()` (via `identity_structure()`). It is excluded only from `pipeline_identity_structure()`. This means a ctx-aware pod and a plain pod using the same underlying data function write to and read from the same pipeline DB table — but they have different `content_hash` values and therefore different per-row memoization keys.

---

## 2. Source Provenance & System Tags

When a source is built by `SourceStreamBuilder`, four hash-related operations happen in
sequence before the stream is returned:

1. **Site 3 — Schema hash:** computed from `(tag_schema, data_schema)`; embedded in system tag column names.
2. **Site 4 — Default `source_id`:** derived from the raw table hash if no explicit `source_id` is given.
3. **Site 5 — Per-row `record_id`:** a UUID v5 computed from `(source_id, row_provenance_token)` and stored per row.
4. Two system tag columns, `_tag_source_id::{schema_hash}` and `_tag_record_id::{schema_hash}`, are appended to every row.

At Join time (site 6), all existing system tag columns are **renamed** by appending a
topology suffix, encoding the full join lineage into each column name.

---

### Site 3 — Schema hash

| Field | Value |
|---|---|
| **Inputs** | `(tag_schema, data_schema)` — Python `Schema` objects mapping column names to Python types |
| **Algorithm** | `SemanticAwarePythonHasher.hash_object((tag_schema, data_schema)).to_hex(schema_n_char)` where `schema_n_char = OrcapodConfig.hashing.schema_n_char` |
| **Output format** | Truncated hex `str` (length = `schema_n_char`; `None` = full digest) |
| **Uniqueness guarantee** | Unique per `(tag_schema, data_schema)` pair; two sources with identical schemas produce identically-named system tag columns, enabling consistent system tag lookup across sources of the same schema |
| **Known exclusions** | No data content; no source identity; purely structural |

---

### Site 4 — Default `source_id`

| Field | Value |
|---|---|
| **Inputs** | Full raw Arrow table (all columns, before any system tag injection) |
| **Algorithm** | `StarfixArrowHasher.hash_table(table).to_hex(path_n_char)` — versioned SHA-256 via the `starfix` Rust crate |
| **Output format** | Truncated hex `str` (length = `path_n_char`) |
| **Uniqueness guarantee** | Unique per raw table content; changes if any cell value changes |
| **Known exclusions** | Used **only** as a fallback when no explicit `source_id` is provided. A user-supplied `source_id` bypasses this computation entirely. |

---

### Site 5 — Per-row `record_id` (system tag value)

| Field | Value |
|---|---|
| **Inputs** | `source_id` string + provenance token: `"{col}={value}"` when `record_id_column` is specified, otherwise `"row_{index}"` |
| **Algorithm** | `uuid.uuid5(_SOURCE_RECORD_ID_NAMESPACE, f"{source_id}::{provenance_token}")` where `_SOURCE_RECORD_ID_NAMESPACE = uuid.uuid5(NAMESPACE_URL, "orcapod::record_id")` is a fixed constant |
| **Output format** | `bytes` (16 bytes, UUID v5 bit pattern), stored in a `pa.binary(16)` Arrow column |
| **Uniqueness guarantee** | Deterministic per `(source_id, row_identity)`; stable across identical re-runs of the same source |
| **Known exclusions** | When `record_id_column` is not specified, the row index is used as the provenance token; `record_id` changes if rows are **reordered** within the source table |

---

### Site 6 — Join system tag suffix

| Field | Value |
|---|---|
| **Inputs** | `stream.pipeline_hash()` of each canonically-ordered input stream + its 0-based canonical position index `idx` |
| **Algorithm** | Streams are first sorted by `stream.pipeline_hash().to_string()` for determinism. For each input at canonical position `idx`, every existing system tag column name has `{BLOCK_SEPARATOR}{stream.pipeline_hash().to_hex(n_char)}:{idx}` appended via `arrow_utils.append_to_system_tags()`. |
| **Output format** | Column name suffix; no separate value is stored |
| **Uniqueness guarantee** | Each post-join system tag column name uniquely identifies `(original schema, input topology, canonical join position)`; no collision even when joining streams with identical schemas |
| **Known exclusions** | `SemiJoin` passes system tags through unchanged. `Batch` changes the column type from `str` to `list[str]` but preserves the column name. |

> **Key insight for entry IDs:** Because `compute_base_entry_id()` (§3) calls
> `tag.as_table(columns={"system_tags": True})`, its preimage captures the full set of
> chained system tag column names. After a two-way join, the preimage contains four system
> tag columns — two per input — each with the join topology embedded in its name. The entry
> ID therefore implicitly commits to the complete join provenance graph without any
> join-awareness in the entry ID computation itself.

---
