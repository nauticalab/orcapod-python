# ITL-1154 hash site inventory (draft, phase 1)

Draft for the coordinator's review. In phase 2 it moves into `docs/reference/hashing.md`.

**One-line rule** (developer, November 15, 2026): a hash always carries its label, except
where it is written into a name, whose grammar fixes which algorithm produced it.

**Classes**

| Class | Form |
|---|---|
| into a hash | carries its label |
| stored value | carries its label (`<label>:<hex>` text, `<label>:` + digest in binary) |
| in a name | bare hex; the name's grammar fixes the algorithm |
| id from a recipe | no label (`uuid5`, `uuid7`, `uuid4`) |
| display only | logs, observers, error text, in-memory keys and ordering; never persisted, never hashed |

**Status**: **ok** complies today; **change** changes under ITL-1154 (rule 1, 2 or 3, folds,
frozenset); **decide** needs a decision (listed at the end).

**How the inventory was built**

- Code search over `src/orcapod`: `hashlib`, `uuid5`/`uuid4`/`uuid7`, `ContentHash(`,
  `to_hex`, `to_string`, `to_prefixed_digest`, `.digest`, `from_string`,
  `from_prefixed_digest`, `hash_object`, `hash_table`, `hash_table_ordered`, `hash_schema`,
  `hash_file`, `content_hash(`, `pipeline_hash(`, `fold_system_tag_values`,
  `invocation_hash`, `stored_hash_labels` (gone, ITL-944), `hexdigest`,
  `zlib`/`crc32`/`xxhash`/`blake`, the builtin `hash(`, every `identity_structure` and
  `pipeline_identity_structure`.
- A run-time trace (`tests/test_hashing/hash_site_trace.py`, a pytest plugin) logs every
  `ContentHash` that is built or parsed, every `hashlib` constructor, `uuid.uuid5` and starfix
  `ArrowDigester` call made from library code, and every read of a digest (`.digest`,
  `to_hex`, `to_string`, `to_prefixed_digest`, `to_int`, `to_uuid`, `to_base64`, `str()`,
  `display_name`, `==`, `hash()`). Each event is logged with its library call site. Each read
  is linked back to the site that produced that digest. Runs: `tests/test_hashing` (golden
  included; 2882 passed), `tests/test_core/operators` + `tests/test_core/function_pod`
  (4502 passed), and the rest of `tests/` except `tests/test_docs` (16585 passed). Every library
  producer and consumer site the trace saw is in the tables below. The code search found a few
  more sites that no traced test reaches; they are marked *(code only)*.

## 1. Producers: algorithms and labels

| # | Location | What is hashed | Algorithm / label | Result used as | Status |
|---|---|---|---|---|---|
| P1 | `hashing/semantic_hashing/semantic_hasher.py` `_hash_to_content_hash` | a Python value expanded to a tagged JSON tree | SHA-256 of canonical JSON, `semantic_v1` | every structure hash: `content_hash()`, `pipeline_hash()`, schema, type and function hashes | ok (label); the tree changes, see C1, V1 to V3 |
| P2 | `hashing/arrow_hashers.py` `hash_table` | an Arrow table after the semantic visitor (V4) | starfix, unordered columns and struct fields, `arrow_v1` | row data hashes, entry ids, row keys, default source ids, table values in a structure | ok |
| P3 | `hashing/arrow_hashers.py` `hash_table_ordered` | the same, with column and struct-field order | starfix, ordered options in the preimage, labelled `arrow_v1` too | its digest only, inside the pandas value hash (C11) | **decide** (D6) |
| P4 | `hashing/arrow_hashers.py` `hash_schema` | an Arrow schema, fields as in `hash_table` | starfix schema digest, `arrow_v1` | no library consumer (public API, doc §9) | ok |
| P5 | `hashing/hash_utils.py` `hash_file` (via `FileHasher`, `CachedFileHasher`) | a file's bytes | the configured digest function; label = its name: `sha256` (data context), also `md5`, `xxh64`, `crc32`, and `hash_path` (SHA-256 of the **path string**) | `File` value hash: top level, nested (C1), Arrow cell (V4), directory entries (C12) | ok for `sha256`; **decide** (D7) for the others |
| P6 | `hashing/directory_hashers.py` `_hash_dir`, `hash_directory` | a directory tree | Merkle tree over `hashlib.new(algorithm)` entries, `directory_v1` | `Directory` value hash | ok; inner file digest **decide** (D2); configurable `algorithm` under a fixed label **decide** (D8) |
| P7 | `logical_types/numpy_type.py` `array_value_digest` + `array_format.py` `array_digest_v1` | an array: element digest, then order, byte order, struct layout | SHA-256, `ndarray_v1` (`NumpyArrayHandler.HASH_LABEL`) | `numpy.ndarray` value hash | ok (the element digest is a step of the same algorithm) |
| P8 | `logical_types/torch_type.py` `tensor_digest` | a tensor: `array_digest_v1` + flags, strides, class | SHA-256, `torch_tensor_v1` | `torch.Tensor` value hash | ok (same) |
| P9 | `hashing/pandas_value_hash.py` + `hashing/labelled_hash.py` `labelled_content_hash` | descriptor JSON + `\0` + the digest of P3 | SHA-256, `pandas_dataframe_v1` / `pandas_series_v1` | pandas value hash | inner digest **decide** (D1) |
| P10 | `logical_types/polars_type.py` `_digest` / `_values_digest` | descriptor JSON + `\0` + starfix `ArrowDigester.hash_table` called directly | SHA-256, `polars_dataframe_v1` / `polars_series_v1` | polars value hash | ok (starfix is called directly, a step of this algorithm, not the data context's Arrow hasher) |
| P11 | `hashing/semantic_hashing/polars_expr.py` `polars_expr_content_hash` | the canonical tree of an expression | SHA-256 of canonical JSON, `polars_expr_v1` | `PolarsFilter` identity (nested, C1) | ok |
| P12 | `logical_types/ignore_list.py` `IgnoreListHandler.handle` / `hash_ignoring` | wraps P7 to P10 in `ContentHash(HASH_LABEL, …)` | the type's label | value handlers: top level, nested (C1), Arrow cell (V4) | ok |
| P13 | `types.py` `ContentHash.from_string` / `from_prefixed_digest` | parse a stored labelled hash | the stored label | saved job and pipeline files, source and data-function proxies, the operator node's saved run hash, a cached output hash (`function_node.py` `_fetch_joined_records`) | ok |
| P14 | `hashing/hash_cachers.py`, `postgres_hash_cacher.py` `get` | parse a cached file hash | the stored label | `CachedFileHasher` | ok |
| P15 | `core/sources/stream_builder.py` `_make_record_id` | `"{source_id}::{row token}"` | `uuid5`, fixed namespace | the `record_id` system tag | ok (id from a recipe) |
| P16 | `utils/arrow_utils.py` `fold_system_tag_values` (record id) | the members' record ids, hex, joined | `uuid5`, fixed namespace | folded `record_id` system tag of `Batch` / `GroupBy` | ok (id from a recipe) |
| P17 | `utils/arrow_utils.py` `fold_system_tag_values` (source id) | the canonical JSON list of members | SHA-256 hex, **no label** | folded `source_id` system tag value | **change** (folds: `group_v1:<hex>` / `batch_v1:<hex>`) |
| P18 | `pipeline/job.py` `_prepare_run` (snapshot) | the sorted sink-node content hashes | SHA-256 hex, first 16 digits | `<job>@<snapshot>` run URI | output ok (in a name); input **change** (C10) |
| P19 | `logical_types/user_class_identity.py` `identity_string` | a user class's identity record | SHA-256 hex prefix | the extension name `name@<fingerprint>` | ok (in a name) |
| P20 | `semantic_types/universal_converter.py` (synthesized struct class) *(code only)* | the field signature text | MD5 hex, 8 digits | a synthesized Python class name | ok (in a name) |
| P21 | `pipeline/serialization.py` `DatabaseRegistry._make_key` | a database config, canonical JSON | SHA-256 hex, 8 digits | `db_<hex>` key in a saved job | ok (in a name) |
| P22 | `databases/spiraldb_nullability.py` `_encode` / `read_nullability_record` | the nullability path list | SHA-256 hex | chunk key and checksum of a stored metadata record | ok (in a name, checked on read) |
| P23 | random ids: `job.py` `run_id` (`uuid4`), orchestrators' `run_id`, `datagram.py` `uuid7`, `data_function.py` record `uuid7`, observers' `uuid7`, `function_node.py` correlation key `uuid4` | nothing | random | ids | ok (not hashes) |
| P24 | builtin `hash()`: `pipeline/graph.py` `_sanitize_node_id`, `core/streams/base.py` (config cache key), `ContentIdentifiableBase.__hash__` | Python objects | process-dependent | Graphviz node ids, in-memory keys | ok (display only) |
| P25 | `hashing/arrow_utils.py` `get_pyarrow_table_hash`, `hashing/arrow_serialization.py` *(code only)* | Arrow tables | SHA-256, SHA3, BLAKE2b hex | **no caller anywhere in the library** | **decide** (D9) |

## 2. Value encodings inside the structure hasher (rules 2 and 3, frozenset)

| # | Location | Value | Today | Status |
|---|---|---|---|---|
| V1 | `builtin_handlers.py` `BytesHandler` | `bytes`, `bytearray` | the hex string: equals the `str` of its hex (ITL-1152) | **change** (rule 2: `Encoded("bytes", {"hex": …})`) |
| V2 | `builtin_handlers.py` `UUIDHandler` | `uuid.UUID` | its 16 bytes, then V1: equals the bytes and the hex string | **change** (rule 2: `Encoded("uuid", {"hex": …})`) |
| V3 | `semantic_hasher.py` `_expand_structure` | `frozenset` | `{"__type__": "set", …}`, the same as `set` | **change** (tag `frozenset`; `HASHER_TAGS` gains it) |
| V4 | `hashing/visitors.py` `SemanticHashingVisitor.visit_extension`, via `value_handlers` of `contexts/logical_types.json` | a UUID **column** | routed through `UUIDHandler` (V2) and written as a cell (C6) | **change** (rule 3: no column value handler; starfix hashes the stored 16 bytes with the extension identity) |
| V5 | `builtin_handlers.py` `GenericAliasHandler` fallback | a generic alias with no `__origin__` | `"generic_alias:" + repr(obj)` | ok for this issue (not a digest; a `repr` in a hash input, noted) |
| V6 | `TypeObjectHandler` | a type | `"type:<identifier text>"` / `"type:<canonical name>"` | ok (an identifier, not a hash; a user-class name holds P19's fingerprint) |

## 3. Consumers: where each digest goes

### 3.1 Into a hash

| # | Location | Digest consumed | Into | Today | Status |
|---|---|---|---|---|---|
| C1 | `semantic_hasher.py` `_nested_hash` (from `_expand_element`, `_expand_encoded`, a `ContentHash` leaf) | any nested hash. Traced: `semantic_v1`, `arrow_v1`, `polars_expr_v1`, `directory_v1`, `sha256`, `ndarray_v1` and the other value labels | the parent structure hash | `{"__type__":"hash","digest":<hex>}` | **change** (rule 1: `"label"` added) |
| C2 | `builtin_handlers.py` `SchemaHandler` | field type hashes | C1 | via C1 | changes with C1 |
| C3 | `GenericAliasHandler`, `UnionTypeHandler` (`_sorted_hashes` sorts by `to_hex`) | argument and origin type hashes | C1 | via C1; the sort key is ordering only | changes with C1 |
| C4 | `FunctionHandler._with_hashed_default` | default value hashes | C1 | via C1 | changes with C1 |
| C5 | every `identity_structure` / `pipeline_identity_structure` that holds a `ContentHash` or a hashable element: sources, streams, operators, function pods and nodes, `DerivedSource` (origin's content hash), `ArrowTableStream` with no producer (its table, `arrow_v1`), `PolarsFilter` (`polars_expr_v1`), `CachedSource` | upstream and element hashes | C1 | via C1 | changes with C1 |
| C6 | `hashing/visitors.py` `visit_extension` (scalar cell) | the value hash of a logical-type cell. Traced: `sha256` (File), `semantic_v1` (UUID), `ndarray_v1`, `torch_tensor_v1`, `pandas_*_v1`, `polars_*_v1`. `directory_v1` per code | the Arrow table hash | `<identifier>::<digest>` | **change** (rule 1: `<identifier>::<label>:<digest>`) |
| C7 | `hashing/visitors.py` `visit_extension` (container cell, `_encode_container_elements`) | element tokens from C6 | the Arrow table hash | count, marker, u64 length, token | changes with C6 (format kept, developer) |
| C8 | `core/nodes/function_node.py` `_build_record_id_preimage` (used by `compute_base_entry_id`, `compute_pipeline_entry_id`, `side_effects.py` record id) | the input row's data hash (`arrow_v1`) | entry ids, pipeline record id, side-effect log key | raw digest in `__input_data_hash` | **change** (rule 1: prefixed digest) |
| C9 | `core/nodes/operator_node.py` `_store_output_stream` (`hashed_table`) | the node's content hash (`semantic_v1`) | the operator-cache row key `_record_hash` | raw digest in `__node_content_hash` | **change** (rule 1: prefixed digest; the hashed and stored columns become equal) |
| C10 | `pipeline/job.py` `_prepare_run` | sink nodes' content hashes | P18 | `to_hex()` joined by newline | **change** (rule 1: `to_string()`) |
| C11 | `hashing/pandas_value_hash.py` `pandas_frame_content_hash`, `pandas_series_content_hash` | P3's digest (`arrow_v1`, ordered) | P9 | raw digest | **decide** (D1) |
| C12 | `hashing/directory_hashers.py` `_hash_dir` | each file's hash (P5, `sha256`) | P6 entry digests | raw digest | **decide** (D2) |
| C13 | `core/sources/list_source.py` `_hash_tag_function` ("content" mode) | the semantic hash of the tag function's source text | `ListSource.identity_structure` | `to_hex()` as a **plain string** | **change** (rule 1: keep the `ContentHash` object; C1 embeds it) |
| C14 | the source id strings (`source_ids.hash_source_id`, `DerivedSource._default_source_id`) inside identity structures, `source_id` system-tag values and source-info tokens | labelled hash inside a name (rule 5a) | content hashes, record ids, entry ids | `<kind>:<label>:<hex>` | ok |
| C15 | `core/sources/spiraldb_table_source.py` `_key_tokens` | the Arrow hash of a row's key columns | the row's provenance token: record-id recipe (P15) and source-info token | `arrow_v1:<hex>` (labelled, in a name) | **decide** (D3) |
| C16 | names that enter a hash: the function `uri` (output-schema digest) in a function pod's identity; system-tag column names (schema, Join, Batch, GroupBy suffixes) inside the entry-id table | bare digests inside a name | pod identity, entry ids | bare hex | ok (in a name) |

### 3.2 Stored values (labelled)

| # | Location | Value | Status |
|---|---|---|---|
| S1 | `core/result_cache.py` `store` / `lookup`, `function_node.py` `_add_pipeline_record` | `__input_data_hash`, `__output_data_hash` (`to_prefixed_digest`) | ok |
| S2 | `function_node.py` (`_add_pipeline_record`, `_fetch_joined_records`, `_stored_record_count`), `operator_node.py` (`_store_output_stream`, `_filter_by_content_hash`) | `__node_content_hash` | ok |
| S3 | `function_node.py` `compute_base_entry_id`, `compute_pipeline_entry_id` | `__pipeline_base_entry_id`, pipeline record id | ok |
| S4 | `operator_node.py` `_store_output_stream`, `cached_source.py` `_live_records` | `_record_hash` | ok |
| S5 | `side_effects.py` `_execute_side_effect_row` | the side-effect log's `record_id`, `record_id_hash` | ok |
| S6 | `core/streams/arrow_table_stream.py` `_assemble_table`, `function_pod.py` and `function_node.py` `all_content_hashes`, `side_effects.py` `_table_of_rows` | the `_content_hash` column | ok |
| S7 | `core/data_function.py` `PythonDataFunction.__init__` | `__pf_var_function_signature_hash`, `__pf_var_function_content_hash` | ok |
| S8 | `pipeline/graph.py` `save`, `pipeline/job.py` `save` / `as_pipeline`, `core/sources/base.py` `_identity_config` | saved pipeline and job files | ok |
| S9 | `hash_cachers.py` / `postgres_hash_cacher.py` `put` | cached file hashes | ok |
| S10 | `invocation.py` `_serialize_component` | `InvocationContext.invocation_hash`, `<label>:<hex>::<label>:<hex>`, with optional truncation and base64 | ok for this issue (out of scope, side-effect follow-up) |

### 3.3 In a name (bare hex)

| # | Location | Name | Status |
|---|---|---|---|
| N1 | `core/data_function.py` `output_data_schema_hash` | function `uri`, second component | ok |
| N2 | `core/operators/static_output_pod.py` `uri` | operator `uri` | ok |
| N3 | `function_node.py`, `operator_node.py` `node_identity_path`; `side_effects.py` `attach_databases` / table path | `schema:<hex>`, `instance:<hex>` path components | ok |
| N4 | `cached_source.py` `_unversioned_cache_path` | `node:<hex>` | ok |
| N5 | `core/nodes/source_node.py` (slot names) | `<label>:<hex>` | ok |
| N6 | `utils/schema_utils.py` `compute_schema_hash` | source system-tag column name | ok |
| N7 | `join.py` (`_compute_system_tag_suffixes`, `_predict_system_tag_schema`, `static_process`), `merge_join.py`, `batch.py`, `group_by.py` | system-tag column suffixes | ok |
| N8 | `function_node.py` `_bare_digest` | a saved hash shown as a digest | ok |
| N9 | P18 output, P19 to P22 | run URI, extension name, class name, registry key, chunk key | ok |

### 3.4 Id from a recipe

P15, P16, P23 (random ids). All ok.

### 3.5 Display only (never persisted, never hashed)

| # | Location | Use |
|---|---|---|
| X1 | `execute` / `async_execute` of function, operator and source nodes (`node_hash`) | observer events |
| X2 | `function_pod.py` `_build_post_run_payload`, `hooks.py` | `pod_hash` passed to hooks |
| X3 | `pipeline/base.py` `compile`, `_record_invocation`; `pipeline/job.py` `compile` | in-memory graph keys and sort keys (labelled strings) |
| X4 | `join.py` `order_input_streams`, `merge_join.py` `_canonical_order` (sort by `to_hex`) | canonical input order (ordering only). Ties between equal pipeline hashes: kept as they are (developer decision, November 16, 2026): per-row value sort, frozenset identity |
| X5 | `ContentIdentifiableBase` / `ContentIdentifiableMixin` `__hash__`, `__eq__`, `__repr__`; `ContentHash.display_name` | in-memory identity, repr |
| X6 | `operator_node.py` (`_read_run_records`), `source_proxy.py` / `data_function_proxy.py` (`bind`), `pipeline/job.py` `bind`, `errors.py` `EphemeralResultMissingError` (repr) | comparing a stored hash with the computed one; error text |

## 4. Sites that fit no class or need a decision

| # | Site | Question | Recommendation |
|---|---|---|---|
| D1 | C11: the pandas value hash feeds in the **data context's** Arrow hasher digest (`hash_table_ordered`) without a label | Is this a hash fed into another hash (rule 1), or a step of the `pandas_*_v1` algorithm? The inner procedure belongs to another hasher, which a data context can replace, so the pandas label alone does not fix it | Rule 1: feed `to_prefixed_digest()`. Moves every pandas value hash (pandas golden entries, pandas cells) |
| D2 | C12: a directory entry feeds in the file hasher's digest without a label (the file hasher is configurable) | Same question for `directory_v1` | Rule 1: `file_hash.to_prefixed_digest()` in the entry bytes. Moves every `Directory` hash |
| D3 | C15: a SpiralDB row's key token is `arrow_v1:<hex>`, a **labelled** hash inside a name (provenance token, record-id recipe input, source-info token) | Bare (names rule) or labelled (rule 5a, source-id style)? | Keep it labelled, in the same way as a default source id: the token names a row of a source and must say what made it. Record it as a stated case. No change, no golden moves (SpiralDB has no golden entries) |
| D4 | P17: the folded source id | Decided in the scope additions (`group_v1` / `batch_v1`) | Implement as decided |
| D5 | C16: a bare digest inside a name that then enters a hash (function `uri` in pod identity, system-tag column names in entry ids) | Confirm that "in a name" covers a name that is later hashed | Yes: the name's grammar fixes the algorithm; no change |
| D6 | P3: `hash_table_ordered` returns the label `arrow_v1` for a different procedure (ordered options are inside the digest, so no collision, but one label names two procedures) | Own label (`arrow_ordered_v1`)? It matters once D1 feeds the label in | Give it its own label constant beside it. The ordered options are already in its preimage, so no digest moves; it changes what D1 feeds in |
| D7 | P5: `hash_file` labels `md5`, `xxh64`, `crc32`, and `hash_path` (hashes the path string, not the content) | `hash_path` is not a content hash and its label is in neither format | Out of scope for ITL-1154; the data context uses `sha256` only. Suggest a follow-up to drop `hash_path` and restrict the algorithm |
| D8 | P6: `BasicDirectoryHasher(algorithm=…)` is configurable, but the label is always `directory_v1` | A label must name the whole algorithm | Out of scope; follow-up: fix the digest function to SHA-256 or derive the label from it |
| D9 | P25: dead code (`hashing/arrow_utils.py` table hash, `hashing/arrow_serialization.py`), never called | Delete, or keep out of the inventory? | Delete in a separate cleanup PR; list it as "not a hash site (unused)" |

## 5. Rule 1 change list: into-a-hash sites that drop the label today

1. C1 `semantic_hasher._nested_hash`: `{"__type__":"hash","label":…,"digest":…}`. Through it: C2 schema, C3 generic alias and union, C4 function defaults, C5 every identity structure.
2. C6 `visitors.SemanticHashingVisitor.visit_extension` scalar cell: `<identifier>::<label>:<digest>`. Through it: C7 container cells.
3. C8 `function_node._build_record_id_preimage`: `__input_data_hash` as `to_prefixed_digest()` (entry ids, pipeline record ids, side-effect record ids).
4. C9 `operator_node._store_output_stream`: `__node_content_hash` as `to_prefixed_digest()` in the hashed row.
5. C10 `job._prepare_run` snapshot: `to_string()` of each sink hash.
6. C13 `list_source._hash_tag_function` ("content" mode): return the `ContentHash`, not its hex. Found by this inventory; not in the ITL-1154 list.
7. If D1 is accepted: C11 pandas value hash (`to_prefixed_digest()` of the ordered Arrow digest, with D6).
8. If D2 is accepted: C12 directory entry (`to_prefixed_digest()` of the file hash).

No other into-a-hash site drops a label. The trace shows that every `.digest` read in the library
is one of C6, C8, C9, C11, C12, the pandas handlers' own `digest` (P12, wrapping their own
hash), or `invocation._serialize_component` (S10, a stored value with its label). Every `to_hex` read is
C1, C3's sort key, C10, C13, or a name.
