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
