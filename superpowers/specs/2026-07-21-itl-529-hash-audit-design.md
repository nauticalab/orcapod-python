# Hash Audit Design — ITL-529

**Date:** 2026-07-21
**Issue:** [ITL-529](https://linear.app/metamorphic/issue/ITL-529/audit-all-hash-computation-sites-document-inputs-algorithm-and)
**Deliverable:** `docs/reference/hashing.md` — a new standalone reference document cataloguing every hash computation site in the orcapod-python framework.

---

## Overview

Orcapod uses fourteen distinct hash computation sites across six usage contexts. There is currently no single place that documents what goes into each hash, which algorithm is used, and what uniqueness guarantee each site provides. This spec defines the structure, content, and format of a new reference document that fills that gap.

## Goals & Success Criteria

- Every hash computation site is identified and catalogued in `docs/reference/hashing.md`.
- Each site documents: inputs, algorithm, output format, uniqueness guarantee, and known exclusions.
- Sites are grouped by usage context so that co-occurring hashes (e.g. all components of an entry ID computation) are documented together.
- A master index table provides a one-page summary of all 14 sites.
- The document is accurate, internally consistent, and cross-linked to `docs/concepts/identity.md`.

## Scope & Boundaries

In scope:
- All 14 hash computation sites identified in this spec.
- The system tag column naming and chaining mechanism (schema hash, join suffix) — these feed directly into entry ID preimages and must be documented as first-class sites.
- `invocation_hash` from `side_effects.py` (ITL-525, already merged).

Out of scope:
- Hash cacher backends (`FileHasher`, `PostgresHashCacher`) — these cache existing hashes.
- User-land code (pod functions, application code outside the framework).
- Changing any hash computation — read-and-document only.

---

## Document Structure

### File

`docs/reference/hashing.md`

### Format decisions

- **Introduction** — one paragraph explaining this is a developer-facing reference (not a tutorial), linking to `docs/concepts/identity.md` for conceptual background.
- **Hash Site Index** — a 14-row summary table: site name, algorithm, output format, one-line guarantee.
- **Six numbered sections**, one per usage group. Groups contain co-occurring hashes so contributors can see the full computation in one place.
  - Sections 1–2 (complex, multi-class): prose + summary tables.
  - Sections 3–6 (self-contained): consistent per-site structured template (5-row table: inputs, algorithm, output format, uniqueness guarantee, known exclusions).

---

## Hash Site Index

| # | Site | Algorithm | Output format | One-line guarantee |
|---|------|-----------|---------------|--------------------|
| 1 | `content_hash()` | `SemanticAwarePythonHasher` → JSON + SHA-256 | `ContentHash` | Unique per semantic content of the object; data-inclusive |
| 2 | `pipeline_hash()` | `SemanticAwarePythonHasher` → JSON + SHA-256 | `ContentHash` | Unique per pipeline topology + schema; data-exclusive |
| 3 | Schema hash (system tag column naming) | `SemanticAwarePythonHasher.hash_object((tag_schema, data_schema)).to_hex(n)` | Truncated hex `str` | Unique per `(tag_schema, data_schema)` pair; embedded in system tag column names |
| 4 | Default `source_id` | `StarfixArrowHasher.hash_table(table).to_hex(n)` | Truncated hex `str` | Unique per raw table content; used as source identifier when none is provided |
| 5 | Per-row `record_id` (system tag value) | `uuid.uuid5(NAMESPACE, f"{source_id}::{provenance_token}")` | `bytes` (16, UUID v5) | Deterministic per `(source_id, row_identity)`; stable across re-runs of the same source |
| 6 | Join system tag suffix | `stream.pipeline_hash().to_hex(n)` + `:{canonical_idx}` appended to column name | Column name suffix | Unique per `(input topology, canonical join position)`; encodes full join lineage in column name |
| 7 | `compute_base_entry_id()` | `StarfixArrowHasher.hash_table()` over `(system_tags + INPUT_DATA_HASH_COL)` | `bytes` (`b"method:digest"`) | Unique per `(node, tag lineage, input_data content)`; recomputation-index-free |
| 8 | `compute_pipeline_entry_id()` | `StarfixArrowHasher.hash_table()` over `(system_tags + INPUT_DATA_HASH_COL + recomputation_index)` | `bytes` (`b"method:digest"`) | Unique per `(node, tag lineage, input_data content, recomputation attempt)` |
| 9 | Side-effect `record_id` | `StarfixArrowHasher.hash_table()` over `(system_tags + INPUT_DATA_HASH_COL + NODE_CONTENT_HASH_COL + recomputation_index=0)` | `ContentHash` → `bytes` | Unique per `(tag lineage, input_data content, pod version)` |
| 10 | `invocation_hash` | Serialization of `pipeline_hash_ch :: record_id_hash_ch` | `str` | Unique per `(pod topology, tag lineage, input_data content, pod version)` |
| 11 | Output schema hash | `SemanticAwarePythonHasher.hash_object(output_data_schema).to_string()` | `str` (ContentHash `.to_string()`) | Unique per output schema definition; embedded in function URI |
| 12 | `run_id` | `uuid.uuid4().hex[:16]` | 16-char hex `str` | Non-deterministic; unique per pipeline execution |
| 13 | `snapshot_hash` | `hashlib.sha256(newline-joined sorted leaf content_hash strings).hexdigest()[:16]` | 16-char hex `str` | Unique per `(DAG leaf topology + data state)` at run time |
| 14 | `datagram_uuid` | `uuid7()` normalised to `stdlib uuid.UUID` | `uuid.UUID` | Unique per datagram instance; time-ordered; not a content hash |

---

## Section Designs

### Section 1: Framework Object Identity (`content_hash` / `pipeline_hash`)

**Format:** Prose + summary tables.

**Opening prose:** Explain the two identity chains (content = data-inclusive, pipeline = topology-only), the `SemanticAwarePythonHasher` algorithm (recursive expansion → JSON serialization → SHA-256 → `ContentHash`), and the resolver pattern (how `content_resolver` and `pipeline_resolver` callbacks ensure nested objects use the correct chain).

**`content_hash()` subsection:**

Per-class `identity_structure()` table:

| Class | `identity_structure()` return value | Notes |
|-------|--------------------------------------|-------|
| `Datagram` | `self._ensure_data_table()` — the raw data Arrow table | Dispatched to `ArrowTableHandler` → `StarfixArrowHasher` |
| `Tag` | Same as `Datagram` — user tag columns only | System tag columns excluded; they are not part of content identity |
| `Data` | Same as `Datagram` — data columns only | Source info columns (`_source_*`) excluded by design |
| `EmptyData` | Raises `EmptyDataAccessError` | `content_hash()` overridden to return `cached_content_hash` directly |
| `DataFunctionBase` | `self.uri` — `(canonical_function_name, output_schema_hash, major_version, data_function_type_id)` | Output schema hash (site 11) is a component |
| `FunctionPod` | `self.data_function` when no ctx arg; `(self.data_function, self._ctx_arg_name)` when ctx arg present | ctx_arg_name included so ctx-aware and regular pods have distinct content hashes |
| `ArrowTableStream` | `(producer, argument_symmetry(upstreams))` | Falls back to table content hash if no producer |
| `RootSource` (via `ArrowTableSource`) | Class name + tag columns + table content hash | Data-inclusive base case |
| `DerivedSource` | Origin node's content hash | Inherits from its generating node |
| Operators (unary) | `(operator_class_name, upstream_stream)` | Stream reference recursed via `content_resolver` |
| Operators (binary/N-ary) | `(operator_class_name, argument_symmetry(streams))` | `frozenset` for commutative (Join, MergeJoin); `tuple` for ordered (SemiJoin) |

Known exclusions table:

| Class | Excluded from `identity_structure()` | Reason |
|-------|--------------------------------------|--------|
| `Tag` | System tag columns (`_tag::*`) | System tags are provenance metadata, not tag content |
| `Data` | Source info columns (`_source_*`) | Source info is provenance metadata, not data content |
| `FunctionPod` | `ctx_arg_name` in pipeline identity (see pipeline section) | Discussed below |
| `EmptyData` | Everything (identity raises) | `EmptyData` has no data payload to hash |

**`pipeline_hash()` subsection:**

Per-class `pipeline_identity_structure()` table:

| Class | `pipeline_identity_structure()` return value | Notes |
|-------|----------------------------------------------|-------|
| `RootSource` | `(tag_schema, data_schema)` | Base case of the Merkle chain; no data content |
| `DerivedSource` | `(tag_schema, data_schema)` | Inherits schema-only identity; acts as a new root |
| `DataFunctionBase` | `self.uri` (same as `identity_structure()`) | Function identity is already schema-only |
| `FunctionPod` | `self.data_function` only (excludes `ctx_arg_name`) | A ctx-aware pod and a regular pod sharing the same data function share a pipeline hash and therefore the same DB table path |
| `ArrowTableStream` | `(producer, argument_symmetry(upstreams pipeline hashes))` | Resolver routes `PipelineElementProtocol` through `pipeline_hash()` |
| Operators | `(operator_class_name, argument_symmetry(upstream pipeline hashes))` | Same structure as content identity but using pipeline hashes of upstreams |
| `SideEffectPodStream` | `(pod, argument_symmetry(upstreams))` | Same as `identity_structure()` |

---

### Section 2: Source Provenance & System Tags

**Format:** Prose overview, then per-site structured template.

**Opening prose:** When a source is built by `SourceStreamBuilder`, four hash-related operations happen in sequence:
1. A schema hash (site 3) is computed from `(tag_schema, data_schema)` — this becomes part of the system tag column names.
2. A default `source_id` (site 4) is derived from the raw table hash — used if no explicit `source_id` is provided.
3. A per-row `record_id` (site 5) is computed as a UUID v5 from `(source_id, provenance_token)` — stored in the `record_id` system tag column.
4. Two system tag columns (`_tag::source_id::{schema_hash}` and `_tag::record_id::{schema_hash}`) are appended to every row.

Then, at Join time, site 6 renames all existing system tag columns by appending `{pipeline_hash}:{idx}` — encoding the join topology into the column names.

**Site 3 — Schema hash:**

| Field | Value |
|---|---|
| Inputs | `(tag_schema, data_schema)` as Python `Schema` objects |
| Algorithm | `SemanticAwarePythonHasher.hash_object((tag_schema, data_schema)).to_hex(schema_n_char)` |
| Output format | Truncated hex `str` (length = `OrcapodConfig.hashing.schema_n_char`) |
| Uniqueness guarantee | Unique per `(tag_schema, data_schema)` pair; embedded in both system tag column names; two sources with identical schemas produce identically-named system tag columns |
| Known exclusions | No data content; no source identity; purely structural |

**Site 4 — Default `source_id`:**

| Field | Value |
|---|---|
| Inputs | Full raw Arrow table (all columns, before any system tag injection) |
| Algorithm | `StarfixArrowHasher.hash_table(table).to_hex(path_n_char)` |
| Output format | Truncated hex `str` (length = `OrcapodConfig.hashing.path_n_char`) |
| Uniqueness guarantee | Unique per raw table content; changes if any cell changes |
| Known exclusions | Used only as fallback when no explicit `source_id` is provided; user-supplied `source_id` bypasses this computation entirely |

**Site 5 — Per-row `record_id` (system tag value):**

| Field | Value |
|---|---|
| Inputs | `source_id` string + provenance token (`"{col}={value}"` if `record_id_column` specified, else `"row_{index}"`) |
| Algorithm | `uuid.uuid5(_SOURCE_RECORD_ID_NAMESPACE, f"{source_id}::{provenance_token}")` where `_SOURCE_RECORD_ID_NAMESPACE = uuid.uuid5(NAMESPACE_URL, "orcapod::record_id")` (a fixed constant) |
| Output format | `bytes` (16 bytes, UUID v5 bit pattern), stored in `pa.binary(16)` Arrow column |
| Uniqueness guarantee | Deterministic per `(source_id, row_identity)`; stable across identical re-runs of the same source |
| Known exclusions | When `record_id_column` is not specified, row index is used — `record_id` changes if rows are reordered within the source table |

**Site 6 — Join system tag suffix:**

| Field | Value |
|---|---|
| Inputs | `stream.pipeline_hash()` of each canonically-ordered input + its 0-based position index `idx` |
| Algorithm | For each input stream at canonical position `idx`: every existing system tag column name has `{BLOCK_SEPARATOR}{stream.pipeline_hash().to_hex(n_char)}:{idx}` appended via `arrow_utils.append_to_system_tags()`. Streams are sorted by `stream.pipeline_hash().to_string()` before indexing. |
| Output format | Column name suffix; no separate value stored |
| Uniqueness guarantee | Each post-join system tag column name uniquely identifies `(original schema, input topology, canonical join position)` — no collision even when joining streams with identical schemas |
| Known exclusions | SemiJoin and Batch do not apply this renaming — SemiJoin passes system tags through unchanged; Batch changes the column type from `str` to `list[str]` but keeps the same name |

**Closing note:** Because the entry ID preimage (Section 3) calls `tag.as_table(columns={"system_tags": True})`, it captures the full set of chained system tag column names. After a two-way join, the preimage contains four system tag columns — two per input — each with the join topology embedded in its name. The entry ID therefore implicitly commits to the complete join provenance graph without any join-awareness in the entry ID computation itself.

---

### Section 3: Pipeline DB Entry Keys

**Format:** Brief prose + per-site structured template.

**Opening prose:** `FunctionJobNode` uses two related Arrow-based hashes as database primary keys. The base entry ID is the stable identity for a `(node, tag, input_data)` triple regardless of how many times the input has been recomputed. The pipeline entry ID is the versioned key that includes a recomputation index — the actual column stored in the pipeline DB.

**Site 7 — `compute_base_entry_id()`:**

| Field | Value |
|---|---|
| Inputs | All system tag columns from `tag.as_table(columns={"system_tags": True})` (including join-chained columns) + `INPUT_DATA_HASH_COL` (`input_data.content_hash().to_prefixed_digest()` as `pa.large_binary()`) |
| Algorithm | `StarfixArrowHasher.hash_table(preimage).to_prefixed_digest()` — single-row Arrow table hash |
| Output format | `bytes` in `b"{method}:{digest}"` format |
| Uniqueness guarantee | Unique per `(node, tag lineage, input_data content)` across all recomputation attempts; used as the in-memory cache key and Phase 1 DB filter |
| Known exclusions | `NODE_CONTENT_HASH_COL` excluded — the node's content hash is already fully determined by the table path (which is scoped by `pipeline_hash`); recomputation index excluded by design |

**Site 8 — `compute_pipeline_entry_id()`:**

| Field | Value |
|---|---|
| Inputs | Same preimage as site 7 + `_PIPELINE_RECOMPUTATION_INDEX_COL` (`pa.int32`, value = `recomputation_index`, default `0`) |
| Algorithm | `StarfixArrowHasher.hash_table(preimage).to_prefixed_digest()` |
| Output format | `bytes` in `b"{method}:{digest}"` format |
| Uniqueness guarantee | Unique per `(node, tag lineage, input_data content, recomputation attempt)` — the primary key for all rows in the pipeline DB |
| Known exclusions | At `recomputation_index=0` this produces a different hash from the pre-ITL-508 implementation (the index column was not previously part of the preimage); existing pipeline DB records were intentionally invalidated when ITL-508 landed |

---

### Section 4: Side-Effect Record ID & Invocation Hash

**Format:** Prose explaining the two-component structure, then per-site template.

**Opening prose:** Side-effect pods (and `FunctionPod` instances with a `ctx_arg_name`) use a parallel but distinct record key scheme. The key difference from Section 3 is the inclusion of `NODE_CONTENT_HASH_COL` — so that changing the pod's implementation invalidates prior delivery records, even for the same input. The `invocation_hash` string is composed from two `ContentHash` components and exposed to the pod function as an idempotency key.

**Site 9 — Side-effect `record_id`:**

| Field | Value |
|---|---|
| Inputs | System tags + `INPUT_DATA_HASH_COL` (as `pa.large_string()`) + `NODE_CONTENT_HASH_COL` (pod `content_hash().to_string()`, as `pa.large_string()`) + `_SIDE_EFFECT_RECOMPUTATION_INDEX_COL` (fixed `0`, `pa.int32`) |
| Algorithm | `StarfixArrowHasher.hash_table(preimage)` → `.to_prefixed_digest()` |
| Output format | `ContentHash` (internally); `.to_prefixed_digest()` → `bytes` when stored in the delivery log |
| Uniqueness guarantee | Unique per `(tag lineage, input_data content, pod version)`; recomputation index is always `0` — side-effect pods do not version recomputations |
| Known exclusions | Unlike sites 7–8, this includes `NODE_CONTENT_HASH_COL` — a deliberate difference ensuring that changing the pod version invalidates the delivery record even for unchanged inputs |

**Site 10 — `invocation_hash`:**

| Field | Value |
|---|---|
| Inputs | `_pipeline_hash_ch` (the pod's `pipeline_hash()` as `ContentHash`) + `_record_id_hash_ch` (the `ContentHash` from site 9) |
| Algorithm | `f"{serialize(pipeline_hash_ch)}::{serialize(record_id_hash_ch)}"` where each component is serialised as `f"{method}:{hex_or_base64_digest}"` via `InvocationHashConfig` (default: hex, full digest) |
| Output format | `str` of the form `"{method}:{digest}::{method}:{digest}"` |
| Uniqueness guarantee | Unique per `(pod topology, tag lineage, input_data content, pod version)`; exposed to pod functions as an idempotency key |
| Known exclusions | When `track_completion=True` (default): `run_id` excluded — hash is run-independent for idempotency. When `track_completion=False` and `pipeline_run_id` is set: `run_id` appended as a third `::` component so that each run produces a distinct hash |

---

### Section 5: Data Function URI Hash

**Format:** Per-site structured template.

**Context:** `DataFunctionBase.uri` is a tuple `(canonical_function_name, output_schema_hash, major_version, data_function_type_id)`. The `output_schema_hash` component (site 11) is the only hash in the URI; the other components are plain strings.

**Site 11 — Output schema hash:**

| Field | Value |
|---|---|
| Inputs | `output_data_schema` — a `Schema` mapping column names to Python types |
| Algorithm | `SemanticAwarePythonHasher.hash_object(output_data_schema).to_string()` |
| Output format | `str` (ContentHash string representation including method prefix, e.g. `"object_v0.1:abcd1234..."`) |
| Uniqueness guarantee | Unique per output schema definition; changing any output column name or type changes this hash and therefore the function's entire URI, `content_hash`, and `pipeline_hash` |
| Known exclusions | Input schema not included (changing input schema alone does not change the URI); function code not included (tracked separately via `major_version`); `data_function_type_id` not included (plain string component) |

---

### Section 6: Pipeline Run Identity

**Format:** Per-site structured template.

**Context:** `PipelineJob.run()` generates three identifiers at execution time. None of these are used in any data-level preimage; they serve logging, observability, and result inspection.

**Site 12 — `run_id`:**

| Field | Value |
|---|---|
| Inputs | None (random) |
| Algorithm | `uuid.uuid4().hex[:16]` |
| Output format | 16-char hex `str` |
| Uniqueness guarantee | Non-deterministic; unique per execution with overwhelming probability |
| Known exclusions | Does not reflect pipeline structure, data content, or any input — not a content hash; two runs with identical pipelines and data produce different `run_id` values |

**Site 13 — `snapshot_hash`:**

| Field | Value |
|---|---|
| Inputs | Sorted `content_hash().to_string()` values of all DAG leaf nodes (nodes with no successors in the execution DAG) |
| Algorithm | `hashlib.sha256("\n".join(sorted_leaf_hashes).encode()).hexdigest()[:16]` |
| Output format | 16-char hex `str`; embedded in `pipeline_uri` as `{name}@{snapshot_hash}` |
| Uniqueness guarantee | Unique per `(leaf node topology + data state)` at run time; changes if any leaf node's schema, code, or source data changes |
| Known exclusions | Covers only leaf (sink) nodes — intermediate nodes not included; truncated to 16 chars (collision-resistant in practice but not cryptographically guaranteed at this length) |

**Site 14 — `datagram_uuid`:**

| Field | Value |
|---|---|
| Inputs | Current wall-clock time (monotonic within a process) |
| Algorithm | `uuid_utils.uuid7()` normalised to `stdlib uuid.UUID` via `uuid.UUID(bytes=uuid7().bytes)` |
| Output format | `uuid.UUID` |
| Uniqueness guarantee | Unique per datagram instance; time-ordered (monotonically increasing within a process) |
| Known exclusions | **Not a content hash** — two datagrams with identical content have different UUIDs; not used in any hash preimage; serves as an object identity token, not a content fingerprint |
