# Comprehensive Specification-Derived Testing Plan

## Context

The orcapod-python codebase has grown complex with many interdependent components. Existing tests were often written by the same agent that implemented the code, risking "self-affirmation" — tests that validate what was built rather than what was specified. This plan creates an independent test suite derived purely from **design documents, protocol definitions, and interface contracts**, organized in a new `test-objective/` root folder.

## Approach: Specification-First Testing

Tests are derived from these specification sources (NOT from reading implementation code):
1. `orcapod-design.md` — the canonical design specification
2. Protocol definitions in `src/orcapod/protocols/` — interface contracts
3. Type annotations and docstrings — method signatures and documented behavior
4. `CLAUDE.md` architecture overview — documented invariants and constraints
5. `DESIGN_ISSUES.md` — known bugs that tests should catch

## Deliverables

### 1. `TESTING_PLAN.md` — comprehensive test case catalog at project root
### 2. `test-objective/` — concrete test implementations at project root

---

## File Structure

```
test-objective/
├── conftest.py                        # Shared fixtures (sources, streams, functions)
├── unit/
│   ├── __init__.py
│   ├── test_types.py                  # Schema, ColumnConfig, ContentHash
│   ├── test_datagram.py               # Datagram core behavior
│   ├── test_tag.py                    # Tag (system tags, ColumnConfig filtering)
│   ├── test_packet.py                 # Packet (source info, provenance)
│   ├── test_stream.py                 # ArrowTableStream construction & iteration
│   ├── test_sources.py                # All source types + error conditions
│   ├── test_source_registry.py        # SourceRegistry CRUD + edge cases
│   ├── test_packet_function.py        # PythonPacketFunction + CachedPacketFunction
│   ├── test_function_pod.py           # FunctionPod, FunctionPodStream
│   ├── test_operators.py              # All operators (Join, MergeJoin, SemiJoin, etc.)
│   ├── test_nodes.py                  # FunctionNode, OperatorNode, Persistent variants
│   ├── test_hashing.py                # SemanticHasher, TypeHandlerRegistry, handlers
│   ├── test_databases.py              # InMemory, DeltaLake, NoOp databases
│   ├── test_schema_utils.py           # Schema extraction, union, intersection
│   ├── test_arrow_utils.py            # Arrow table/schema utilities
│   ├── test_arrow_data_utils.py       # System tags, source info, column helpers
│   ├── test_semantic_types.py         # UniversalTypeConverter, SemanticTypeRegistry
│   ├── test_contexts.py               # DataContext resolution, validation
│   ├── test_tracker.py                # BasicTrackerManager, GraphTracker
│   └── test_lazy_module.py            # LazyModule deferred import behavior
├── integration/
│   ├── __init__.py
│   ├── test_pipeline_flows.py         # End-to-end pipeline scenarios
│   ├── test_caching_flows.py          # DB-backed caching (FunctionNode, OperatorNode)
│   ├── test_hash_invariants.py        # Hash stability & Merkle chain properties
│   ├── test_provenance.py             # System tag lineage through pipelines
│   └── test_column_config_filtering.py # ColumnConfig behavior across all components
└── property/
    ├── __init__.py
    ├── test_schema_properties.py      # Hypothesis-based schema algebra
    ├── test_hash_properties.py        # Hash determinism, collision resistance
    └── test_operator_algebra.py       # Commutativity, associativity, idempotency
```

---

## Unit Test Cases by Module

### 1. `test_types.py` — Schema, ColumnConfig, ContentHash

**Schema:**
- `test_schema_construction_from_dict` — Schema({"a": int, "b": str}) stores correct fields
- `test_schema_construction_with_kwargs` — Schema(fields, x=int) merges kwargs with precedence
- `test_schema_optional_fields` — optional_fields stored as frozenset, not in required_fields
- `test_schema_required_fields` — required_fields = all fields minus optional_fields
- `test_schema_immutability` — Schema is an immutable Mapping (no __setitem__)
- `test_schema_merge_compatible` — Schema.merge() combines non-conflicting schemas
- `test_schema_merge_type_conflict_raises` — Schema.merge() raises ValueError on type conflicts
- `test_schema_with_values_overrides_silently` — with_values() overrides without errors
- `test_schema_select_existing_fields` — select() returns subset
- `test_schema_select_missing_field_raises` — select() raises KeyError on missing field
- `test_schema_drop_existing_fields` — drop() removes fields
- `test_schema_drop_missing_field_silent` — drop() silently ignores missing fields
- `test_schema_is_compatible_with_superset` — returns True when other is superset
- `test_schema_is_not_compatible_with_subset` — returns False when other is subset
- `test_schema_empty` — Schema.empty() returns zero-field schema
- `test_schema_mapping_interface` — __getitem__, __contains__, __iter__, __len__ work correctly

**ContentHash:**
- `test_content_hash_immutability` — frozen dataclass, cannot reassign method/digest
- `test_content_hash_to_hex` — to_hex(8) returns 8-char hex string
- `test_content_hash_to_int` — to_int() returns consistent integer
- `test_content_hash_to_uuid` — to_uuid() returns deterministic UUID
- `test_content_hash_to_base64` — to_base64() returns valid base64
- `test_content_hash_to_string_and_from_string_roundtrip` — from_string(to_string()) == original
- `test_content_hash_display_name` — display_name() returns "method:short_hex" format
- `test_content_hash_equality` — same method+digest are equal
- `test_content_hash_inequality` — different digests are not equal

**ColumnConfig:**
- `test_column_config_defaults` — all fields False by default
- `test_column_config_all` — ColumnConfig.all() sets everything True
- `test_column_config_data_only` — ColumnConfig.data_only() sets everything False
- `test_column_config_handle_config_dict` — handle_config(dict) normalizes to ColumnConfig
- `test_column_config_handle_config_all_info_override` — all_info=True overrides individual fields
- `test_column_config_frozen` — cannot modify after construction

### 2. `test_datagram.py` — Datagram

**Construction:**
- `test_datagram_from_dict` — construct from Python dict
- `test_datagram_from_arrow_table` — construct from pa.Table
- `test_datagram_from_record_batch` — construct from pa.RecordBatch
- `test_datagram_with_meta_info` — meta columns stored separately
- `test_datagram_with_python_schema` — explicit schema used over inference
- `test_datagram_with_record_id` — custom record_id stored as datagram_id

**Dict-like Access:**
- `test_datagram_getitem_existing_key` — returns correct value
- `test_datagram_getitem_missing_key_raises` — raises KeyError
- `test_datagram_contains` — __contains__ returns True/False correctly
- `test_datagram_iter` — __iter__ yields all data column names
- `test_datagram_get_with_default` — get() returns default for missing keys

**Lazy Conversion (key invariant):**
- `test_datagram_dict_access_uses_dict_backing` — dict access doesn't trigger Arrow conversion
- `test_datagram_as_table_triggers_arrow_conversion` — as_table() produces Arrow table
- `test_datagram_dict_arrow_roundtrip_preserves_data` — dict→Arrow→dict preserves values
- `test_datagram_arrow_dict_roundtrip_preserves_data` — Arrow→dict→Arrow preserves values

**Schema Methods:**
- `test_datagram_keys_data_only` — keys() returns only data column names by default
- `test_datagram_keys_all_info` — keys(all_info=True) includes meta columns
- `test_datagram_schema_matches_keys` — schema() field names match keys()
- `test_datagram_arrow_schema_type_consistency` — arrow_schema() types match schema() types

**Format Conversions:**
- `test_datagram_as_dict` — returns plain Python dict
- `test_datagram_as_table` — returns single-row pa.Table
- `test_datagram_as_arrow_compatible_dict` — values are Arrow-compatible

**Data Operations (immutability):**
- `test_datagram_select_returns_new_instance` — original unchanged
- `test_datagram_drop_returns_new_instance` — original unchanged
- `test_datagram_rename_returns_new_instance` — original unchanged
- `test_datagram_update_existing_columns_only` — update() only changes existing columns
- `test_datagram_with_columns_new_only` — with_columns() only adds new columns
- `test_datagram_copy_creates_independent_copy` — mutations to copy don't affect original

**Meta Operations:**
- `test_datagram_get_meta_value_auto_prefixed` — get_meta_value() auto-adds prefix
- `test_datagram_with_meta_columns_returns_new` — immutable update
- `test_datagram_drop_meta_columns_returns_new` — immutable drop

**Content Hashing:**
- `test_datagram_content_hash_deterministic` — same data → same hash
- `test_datagram_content_hash_changes_with_data` — different data → different hash
- `test_datagram_equality_by_content` — equal content → equal datagrams

### 3. `test_tag.py` — Tag

- `test_tag_construction_with_system_tags` — system tags stored separately from data
- `test_tag_system_tags_excluded_from_default_keys` — keys() doesn't show system tags
- `test_tag_system_tags_included_with_column_config` — keys(columns={"system_tags": True}) shows them
- `test_tag_as_dict_excludes_system_tags_by_default` — as_dict() only has data
- `test_tag_as_dict_all_info_includes_system_tags` — as_dict(all_info=True) has everything
- `test_tag_as_table_excludes_system_tags_by_default`
- `test_tag_as_table_all_info_includes_system_tags`
- `test_tag_schema_excludes_system_tags_by_default`
- `test_tag_copy_preserves_system_tags` — copy() includes system tags
- `test_tag_as_datagram_conversion` — as_datagram() returns Datagram (not Tag)
- `test_tag_system_tags_method_returns_copy` — system_tags() returns dict copy, not reference

### 4. `test_packet.py` — Packet

- `test_packet_construction_with_source_info` — source_info stored per data column
- `test_packet_source_info_excluded_from_default_keys` — keys() doesn't show _source_ columns
- `test_packet_source_info_included_with_column_config` — keys(columns={"source": True})
- `test_packet_with_source_info_returns_new` — immutable update
- `test_packet_rename_updates_source_info_keys` — rename() also renames source_info keys
- `test_packet_with_columns_adds_source_info_entry` — new columns get source_info=None
- `test_packet_as_datagram_conversion` — as_datagram() returns Datagram
- `test_packet_as_dict_excludes_source_columns_by_default`
- `test_packet_as_dict_all_info_includes_source_columns`
- `test_packet_copy_preserves_source_info`

### 5. `test_stream.py` — ArrowTableStream

**Construction:**
- `test_stream_from_table_with_tag_columns` — tag/packet column separation
- `test_stream_requires_at_least_one_packet_column` — ValueError if no packet columns
- `test_stream_with_system_tag_columns` — system tag columns tracked
- `test_stream_with_source_info` — source info attached to packet columns
- `test_stream_with_producer` — producer property set
- `test_stream_with_upstreams` — upstreams tuple set

**Schema & Keys:**
- `test_stream_keys_returns_tag_and_packet_keys` — tuple of (tag_keys, packet_keys)
- `test_stream_output_schema_returns_two_schemas` — (tag_schema, packet_schema)
- `test_stream_schema_matches_actual_data` — output_schema() types match as_table() types
- `test_stream_keys_with_column_config` — ColumnConfig filtering works

**Iteration:**
- `test_stream_iter_packets_yields_tag_packet_pairs` — each yield is (Tag, Packet)
- `test_stream_iter_packets_count_matches_rows` — number of yields = number of rows
- `test_stream_iter_packets_tag_keys_correct` — tag column names match
- `test_stream_iter_packets_packet_keys_correct` — packet column names match
- `test_stream_as_table_matches_iter_packets` — table materialization consistent with iteration

**Immutability:**
- `test_stream_immutable` — no mutation methods available

**Format Conversions:**
- `test_stream_as_polars_df` — converts to Polars DataFrame
- `test_stream_as_pandas_df` — converts to Pandas DataFrame
- `test_stream_as_lazy_frame` — converts to Polars LazyFrame

### 6. `test_sources.py` — All Source Types

**ArrowTableSource:**
- `test_arrow_source_from_valid_table` — normal construction succeeds
- `test_arrow_source_empty_table_raises` — ValueError("Table is empty")
- `test_arrow_source_missing_tag_column_raises` — ValueError if tag_columns not in table
- `test_arrow_source_adds_system_tag_column` — system tag column added automatically
- `test_arrow_source_adds_source_info_columns` — _source_ columns added
- `test_arrow_source_source_id_set` — source_id property populated
- `test_arrow_source_producer_is_none` — root sources have no producer
- `test_arrow_source_upstreams_empty` — root sources have no upstreams
- `test_arrow_source_resolve_field_by_record_id` — resolves field value
- `test_arrow_source_resolve_field_missing_raises` — FieldNotResolvableError
- `test_arrow_source_pipeline_identity_structure` — returns (tag_schema, packet_schema)
- `test_arrow_source_iter_packets_yields_correct_pairs`
- `test_arrow_source_as_table_has_all_columns`

**DictSource:**
- `test_dict_source_from_dict_of_lists` — constructs correctly
- `test_dict_source_delegates_to_arrow_table_source` — same behavior as ArrowTableSource
- `test_dict_source_with_tag_columns`

**ListSource:**
- `test_list_source_from_list_of_dicts` — constructs correctly
- `test_list_source_empty_list_raises` — ValueError

**CSVSource:**
- `test_csv_source_from_file` — reads CSV correctly
- `test_csv_source_with_tag_columns`

**DataFrameSource:**
- `test_dataframe_source_from_polars` — constructs from Polars DataFrame
- `test_dataframe_source_from_pandas` — constructs from Pandas DataFrame

**DerivedSource:**
- `test_derived_source_before_run_raises` — ValueError before upstream has computed
- `test_derived_source_after_run_yields_records` — produces records from upstream node

### 7. `test_source_registry.py` — SourceRegistry

- `test_registry_register_and_get` — register then retrieve
- `test_registry_register_empty_id_raises` — ValueError
- `test_registry_register_none_source_raises` — ValueError
- `test_registry_register_same_object_idempotent` — re-register same object is no-op
- `test_registry_register_different_object_same_id_keeps_existing` — warns, keeps existing
- `test_registry_replace_overwrites` — replace() unconditionally overwrites
- `test_registry_replace_returns_old` — returns previous source
- `test_registry_unregister_removes` — removes and returns source
- `test_registry_unregister_missing_raises` — KeyError
- `test_registry_get_missing_raises` — KeyError
- `test_registry_get_optional_missing_returns_none` — returns None
- `test_registry_contains` — __contains__ works
- `test_registry_len` — __len__ works
- `test_registry_iter` — __iter__ yields IDs
- `test_registry_clear` — removes all entries
- `test_registry_list_ids` — returns list of registered IDs

### 8. `test_packet_function.py` — PythonPacketFunction, CachedPacketFunction

**PythonPacketFunction:**
- `test_pf_from_simple_function` — wraps a function with explicit output_keys
- `test_pf_infers_input_schema_from_signature` — type annotations → input_packet_schema
- `test_pf_infers_output_schema` — output type annotations or output_keys → output_packet_schema
- `test_pf_rejects_variadic_parameters` — *args, **kwargs raise ValueError
- `test_pf_call_transforms_packet` — call() applies function to packet data
- `test_pf_call_returns_none_if_function_returns_none` — None propagates
- `test_pf_direct_call_bypasses_executor` — direct_call() ignores executor
- `test_pf_call_routes_through_executor` — call() uses executor when set
- `test_pf_version_parsing` — "v1.2" → major_version=1, minor_version_string="2"
- `test_pf_canonical_function_name` — uses function.__name__ or explicit name
- `test_pf_content_hash_deterministic` — same function → same hash
- `test_pf_content_hash_changes_with_function` — different function → different hash
- `test_pf_pipeline_hash_ignores_data` — pipeline_hash based on schema only

**CachedPacketFunction:**
- `test_cached_pf_cache_miss_computes_and_stores` — first call computes + records
- `test_cached_pf_cache_hit_returns_stored` — second call returns cached result
- `test_cached_pf_skip_cache_lookup_always_computes` — skip_cache_lookup=True forces compute
- `test_cached_pf_skip_cache_insert_doesnt_store` — skip_cache_insert=True skips recording
- `test_cached_pf_get_all_cached_outputs` — returns all stored records as table
- `test_cached_pf_record_path_based_on_function_hash` — record path includes function identity

### 9. `test_function_pod.py` — FunctionPod, FunctionPodStream

**FunctionPod:**
- `test_function_pod_process_returns_stream` — process() returns FunctionPodStream
- `test_function_pod_validate_inputs_single_stream` — accepts exactly one stream
- `test_function_pod_validate_inputs_multiple_raises` — rejects multiple streams
- `test_function_pod_output_schema_prediction` — output_schema() matches actual output
- `test_function_pod_callable_alias` — __call__ same as process()
- `test_function_pod_never_modifies_tags` — tags pass through unchanged
- `test_function_pod_transforms_packets` — packets are transformed by function

**FunctionPodStream:**
- `test_fps_lazy_evaluation` — iter_packets() triggers computation
- `test_fps_producer_is_function_pod` — producer property returns the pod
- `test_fps_upstreams_contains_input_stream`
- `test_fps_keys_matches_pod_output_schema` — keys() consistent with pod.output_schema()
- `test_fps_as_table_materialization` — as_table() returns correct table
- `test_fps_clear_cache_forces_recompute` — clear_cache() resets cached state

**Decorator:**
- `test_function_pod_decorator_creates_pod_attribute` — @function_pod adds .pod
- `test_function_pod_decorator_with_result_database` — wraps in CachedPacketFunction

### 10. `test_operators.py` — All Operators

**Join (N-ary, commutative):**
- `test_join_two_streams_on_common_tags` — inner join on shared tag columns
- `test_join_non_overlapping_packet_columns_required` — InputValidationError on collision
- `test_join_commutative` — join(A, B) == join(B, A) (same rows regardless of order)
- `test_join_three_or_more_streams` — N-ary join works
- `test_join_empty_result_when_no_matches` — disjoint tags → empty stream
- `test_join_system_tag_name_extending` — system tag columns get ::pipeline_hash:position suffix
- `test_join_system_tag_values_sorted_for_commutativity` — canonical ordering of tag values
- `test_join_output_schema_prediction` — output_schema() matches actual output

**MergeJoin (binary):**
- `test_merge_join_colliding_columns_become_sorted_lists` — same-name packet cols → list[T]
- `test_merge_join_requires_identical_types` — different types raise error
- `test_merge_join_non_colliding_columns_pass_through` — unmatched columns kept as-is
- `test_merge_join_system_tag_name_extending`
- `test_merge_join_output_schema_prediction` — predicts list[T] types correctly

**SemiJoin (binary, non-commutative):**
- `test_semijoin_filters_left_by_right_tags` — keeps left rows matching right tags
- `test_semijoin_non_commutative` — semijoin(A, B) != semijoin(B, A) in general
- `test_semijoin_preserves_left_packet_columns` — right packet columns dropped
- `test_semijoin_system_tag_name_extending`

**Batch:**
- `test_batch_groups_rows` — groups rows by tag, aggregates packets
- `test_batch_types_become_lists` — packet column types become list[T]
- `test_batch_system_tag_type_evolving` — system tag type becomes list[str]
- `test_batch_with_batch_size` — batch_size limits group size
- `test_batch_drop_partial_batch` — drop_partial_batch=True drops incomplete groups
- `test_batch_output_schema_prediction` — predicts list[T] types

**Column Selection (Select/Drop Tag/Packet):**
- `test_select_tag_columns` — keeps only specified tag columns
- `test_select_tag_columns_strict_missing_raises` — strict=True raises on missing column
- `test_select_packet_columns` — keeps only specified packet columns
- `test_drop_tag_columns` — removes specified tag columns
- `test_drop_packet_columns` — removes specified packet columns
- `test_column_selection_system_tag_name_preserving` — system tags unchanged

**MapTags/MapPackets:**
- `test_map_tags_renames_tag_columns` — renames specified tag columns
- `test_map_tags_drop_unmapped` — drop_unmapped=True removes unrenamed columns
- `test_map_packets_renames_packet_columns`
- `test_map_preserves_system_tags` — system tag columns unchanged (name-preserving)

**PolarsFilter:**
- `test_polars_filter_with_predicate` — filters rows matching predicate
- `test_polars_filter_with_constraints` — filters by column=value constraints
- `test_polars_filter_preserves_schema` — output schema same as input
- `test_polars_filter_system_tag_name_preserving`

**Operator Base Classes:**
- `test_unary_operator_rejects_multiple_inputs` — validate_inputs raises for >1 stream
- `test_binary_operator_rejects_wrong_count` — validate_inputs raises for !=2 streams
- `test_nonzero_input_operator_rejects_zero` — validate_inputs raises for 0 streams

### 11. `test_nodes.py` — FunctionNode, OperatorNode, Persistent variants

**FunctionNode:**
- `test_function_node_iter_packets` — iterates and transforms all packets
- `test_function_node_process_packet` — transforms single (tag, packet) pair
- `test_function_node_producer_is_function_pod`
- `test_function_node_upstreams`
- `test_function_node_clear_cache`

**PersistentFunctionNode:**
- `test_persistent_fn_two_phase_iteration` — Phase 1: cached records, Phase 2: compute missing
- `test_persistent_fn_pipeline_path_uses_pipeline_hash` — path includes pipeline_hash
- `test_persistent_fn_caches_computed_results` — computed results stored in DB
- `test_persistent_fn_skips_already_cached` — Phase 2 skips inputs with cached outputs
- `test_persistent_fn_run_eagerly_processes_all` — run() processes all packets
- `test_persistent_fn_as_source_returns_derived_source` — as_source() returns DerivedSource

**OperatorNode:**
- `test_operator_node_delegates_to_operator`
- `test_operator_node_clear_cache`
- `test_operator_node_run`

**PersistentOperatorNode:**
- `test_persistent_on_cache_mode_off` — always recomputes
- `test_persistent_on_cache_mode_log` — computes and stores
- `test_persistent_on_cache_mode_replay` — loads from DB, no recompute
- `test_persistent_on_as_source_returns_derived_source`

### 12. `test_hashing.py` — SemanticHasher, TypeHandlerRegistry

**BaseSemanticHasher:**
- `test_hasher_primitives` — int, str, float, bool, None hashed deterministically
- `test_hasher_structures` — list, dict, tuple, set expanded structurally
- `test_hasher_content_hash_terminal` — ContentHash inputs returned as-is
- `test_hasher_content_identifiable_uses_identity_structure` — resolves via identity_structure()
- `test_hasher_unknown_type_strict_raises` — TypeError in strict mode
- `test_hasher_deterministic` — same input → same hash always
- `test_hasher_different_inputs_different_hashes` — collision resistance
- `test_hasher_nested_structures` — deeply nested dicts/lists hashed correctly

**TypeHandlerRegistry:**
- `test_registry_register_and_lookup` — register handler, get_handler returns it
- `test_registry_mro_aware_lookup` — subclass falls back to parent handler
- `test_registry_unregister` — remove handler
- `test_registry_has_handler` — boolean check
- `test_registry_registered_types` — list all registered types
- `test_registry_thread_safety` — concurrent register/lookup doesn't crash

**Built-in Handlers:**
- `test_path_handler_hashes_file_content` — Path → file content hash
- `test_path_handler_missing_file_raises` — FileNotFoundError
- `test_uuid_handler` — UUID → canonical string
- `test_bytes_handler` — bytes → hex string
- `test_function_handler` — function → signature-based identity
- `test_type_object_handler` — type → "type:module.qualname"
- `test_arrow_table_handler` — pa.Table → content hash

### 13. `test_databases.py` — InMemory, DeltaLake, NoOp

**InMemoryArrowDatabase:**
- `test_inmemory_add_and_get_record` — add_record + get_record_by_id roundtrip
- `test_inmemory_add_records_batch` — add_records with multiple rows
- `test_inmemory_get_all_records` — returns all at path
- `test_inmemory_get_records_by_ids` — returns subset by IDs
- `test_inmemory_skip_duplicates` — skip_duplicates=True doesn't raise
- `test_inmemory_pending_batch_semantics` — records not visible before flush()
- `test_inmemory_flush_makes_visible` — flush() commits pending records
- `test_inmemory_invalid_path_raises` — ValueError for empty/invalid paths
- `test_inmemory_get_nonexistent_returns_none` — missing path → None

**NoOpArrowDatabase:**
- `test_noop_all_writes_silently_discarded` — add_record/add_records don't error
- `test_noop_all_reads_return_none` — get_* always returns None
- `test_noop_flush_noop` — flush() doesn't error

**DeltaTableDatabase (if available):**
- `test_delta_add_and_get_record` — persistence roundtrip
- `test_delta_flush_writes_to_disk` — data survives flush
- `test_delta_path_validation` — invalid paths rejected

### 14. `test_schema_utils.py` — Schema Utilities

- `test_extract_function_schemas_from_annotations` — infers schemas from type hints
- `test_extract_function_schemas_rejects_variadic` — ValueError for *args/**kwargs
- `test_verify_packet_schema_valid` — matching dict passes
- `test_verify_packet_schema_type_mismatch` — mismatched types fail
- `test_check_schema_compatibility` — compatible types pass
- `test_infer_schema_from_dict` — infers types from values
- `test_union_schemas_no_conflict` — merges cleanly
- `test_union_schemas_with_conflict_raises` — TypeError on conflicting types
- `test_intersection_schemas` — returns common fields
- `test_get_compatible_type_int_float` — numeric promotion
- `test_get_compatible_type_incompatible_raises` — TypeError

### 15. `test_arrow_utils.py` — Arrow Utilities

- `test_schema_select` — selects subset of arrow schema columns
- `test_schema_select_missing_raises` — KeyError for missing columns
- `test_schema_drop` — drops specified columns
- `test_normalize_to_large_types` — string → large_string, etc.
- `test_pylist_to_pydict` — row-oriented → column-oriented
- `test_pydict_to_pylist` — column-oriented → row-oriented
- `test_pydict_to_pylist_inconsistent_lengths_raises` — ValueError
- `test_hstack_tables` — horizontal concatenation
- `test_hstack_tables_different_row_counts_raises` — ValueError
- `test_hstack_tables_duplicate_columns_raises` — ValueError
- `test_check_arrow_schema_compatibility` — compatible schemas pass
- `test_split_by_column_groups` — splits table into multiple tables

### 16. `test_arrow_data_utils.py` — System Tags & Source Info

- `test_add_system_tag_columns` — adds _tag:: prefixed columns
- `test_add_system_tag_columns_empty_table_raises` — ValueError
- `test_add_system_tag_columns_length_mismatch_raises` — ValueError
- `test_append_to_system_tags` — extends existing system tag values
- `test_sort_system_tag_values` — canonical sorting for commutativity
- `test_add_source_info` — adds _source_ prefixed columns
- `test_drop_columns_with_prefix` — removes columns matching prefix
- `test_drop_system_columns` — removes __ and __ prefixed columns

### 17. `test_semantic_types.py` — UniversalTypeConverter

- `test_python_to_arrow_type_primitives` — int→int64, str→large_string, etc.
- `test_python_to_arrow_type_list` — list[int]→large_list(int64)
- `test_python_to_arrow_type_dict` — dict→struct
- `test_arrow_to_python_type_roundtrip` — python→arrow→python recovers original
- `test_python_dicts_to_arrow_table` — list of dicts → pa.Table
- `test_arrow_table_to_python_dicts` — pa.Table → list of dicts
- `test_schema_conversion_roundtrip` — Schema→pa.Schema→Schema preserves types

### 18. `test_contexts.py` — DataContext

- `test_resolve_context_none_returns_default` — None → default context
- `test_resolve_context_string_version` — "v0.1" → matching context
- `test_resolve_context_datacontext_passthrough` — DataContext returned as-is
- `test_resolve_context_invalid_raises` — ContextResolutionError
- `test_get_available_contexts` — returns sorted version list
- `test_default_context_has_all_components` — type_converter, arrow_hasher, semantic_hasher present

### 19. `test_tracker.py` — BasicTrackerManager, GraphTracker

- `test_tracker_manager_register_deregister` — add/remove trackers
- `test_tracker_manager_broadcasts_invocations` — records sent to all active trackers
- `test_tracker_manager_no_tracking_context` — no_tracking() suspends recording
- `test_graph_tracker_records_function_pod_invocation` — node added to graph
- `test_graph_tracker_records_operator_invocation` — node added to graph
- `test_graph_tracker_compile_builds_graph` — compile() produces nx.DiGraph
- `test_graph_tracker_reset_clears_state`

### 20. `test_lazy_module.py` — LazyModule

- `test_lazy_module_not_loaded_initially` — is_loaded is False
- `test_lazy_module_loads_on_attribute_access` — accessing attr triggers import
- `test_lazy_module_force_load` — force_load() triggers immediate import
- `test_lazy_module_invalid_module_raises` — ModuleNotFoundError

---

## Integration Test Cases

### `test_pipeline_flows.py` — End-to-End Pipeline Scenarios

- `test_source_to_stream_to_single_operator` — Source → Filter → Stream
- `test_source_to_function_pod` — Source → FunctionPod → Stream with transformed packets
- `test_multi_source_join` — Two sources → Join → Stream with combined data
- `test_chained_operators` — Source → Filter → Select → MapTags → Stream
- `test_function_pod_then_operator` — Source → FunctionPod → Filter → Stream
- `test_join_then_batch` — Two sources → Join → Batch → Stream
- `test_semijoin_filters_correctly` — Source A semi-joined with Source B
- `test_merge_join_combines_columns` — Two sources with overlapping columns → MergeJoin
- `test_diamond_pipeline` — Source → [branch A, branch B] → Join → Stream
- `test_pipeline_with_multiple_function_pods` — Source → FunctionPod1 → FunctionPod2

### `test_caching_flows.py` — DB-Backed Caching Scenarios

- `test_persistent_function_node_caches_and_replays` — first run computes, second replays
- `test_persistent_function_node_incremental_update` — new input rows only compute missing
- `test_persistent_operator_node_log_mode` — CacheMode.LOG stores results
- `test_persistent_operator_node_replay_mode` — CacheMode.REPLAY loads from DB
- `test_derived_source_reingestion` — PersistentFunctionNode → DerivedSource → further pipeline
- `test_cached_packet_function_with_inmemory_db` — end-to-end caching flow

### `test_hash_invariants.py` — Hash Stability & Merkle Chain Properties

- `test_content_hash_stability_same_data` — identical data → identical hash across runs
- `test_content_hash_changes_with_data` — different data → different hash
- `test_pipeline_hash_ignores_data_content` — same schema, different data → same pipeline_hash
- `test_pipeline_hash_changes_with_schema` — different schema → different pipeline_hash
- `test_pipeline_hash_merkle_chain` — downstream hash commits to upstream hashes
- `test_commutative_join_pipeline_hash_order_independent` — join(A,B) pipeline_hash == join(B,A)
- `test_non_commutative_semijoin_pipeline_hash_order_dependent` — semijoin(A,B) != semijoin(B,A)

### `test_provenance.py` — System Tag Lineage Tracking

- `test_source_creates_system_tag_column` — source adds _tag::source:hash column
- `test_unary_operator_preserves_system_tags` — filter/select/map: name+value unchanged
- `test_join_extends_system_tag_names` — multi-input: column names get ::hash:pos suffix
- `test_join_sorts_system_tag_values` — commutative ops sort tag values
- `test_batch_evolves_system_tag_type` — batch: str → list[str]
- `test_full_pipeline_provenance_chain` — source → join → filter → batch: all rules applied

### `test_column_config_filtering.py` — ColumnConfig Across All Components

- `test_datagram_column_config_meta` — meta=True includes __ columns
- `test_datagram_column_config_data_only` — all False = data columns only
- `test_tag_column_config_system_tags` — system_tags=True includes _tag:: columns
- `test_packet_column_config_source` — source=True includes _source_ columns
- `test_stream_column_config_all_info` — all_info=True on keys/output_schema/as_table
- `test_stream_column_config_consistency` — keys(), output_schema(), as_table() all respect same config

---

## Property-Based & Advanced Testing (test-objective/property/)

### `test_schema_properties.py` (using Hypothesis)
- `test_schema_merge_commutative` — merge(A,B) == merge(B,A) when compatible
- `test_schema_select_then_drop_complementary` — select(X) ∪ drop(X) == original
- `test_schema_is_compatible_reflexive` — A.is_compatible_with(A) always True
- `test_schema_optional_fields_subset_of_all_fields`

### `test_hash_properties.py` (using Hypothesis)
- `test_hash_deterministic` — hash(X) == hash(X) for any X
- `test_hash_changes_with_any_field_mutation` — mutate one value → different hash
- `test_content_hash_string_roundtrip` — from_string(to_string(h)) == h for any h

### `test_operator_algebra.py`
- `test_join_commutativity` — join(A,B) data == join(B,A) data
- `test_join_associativity` — join(join(A,B),C) data == join(A,join(B,C)) data
- `test_filter_idempotency` — filter(filter(S, P), P) == filter(S, P)
- `test_select_then_select_is_intersection` — select(select(S, X), Y) == select(S, X∩Y)
- `test_drop_then_drop_is_union` — drop(drop(S, X), Y) == drop(S, X∪Y)

---

## Suggestions for More Objective Testing

### Included in `test-objective/property/`:
1. **Property-based testing** (Hypothesis) — generate random schemas, data, operations and verify algebraic invariants hold
2. **Algebraic property testing** — verify mathematical properties (commutativity of join, idempotency of filter, etc.)

### Recommended additions (not implemented in this PR, but suggested):
3. **Mutation testing** with `mutmut` — run `uv run mutmut run --paths-to-mutate=src/orcapod/ --tests-dir=test-objective/` to verify tests catch code mutations. A surviving mutant indicates a test gap
4. **Metamorphic testing** — "if I add a row to source A that matches source B's tags, the join output should have one more row" — tests relationships between inputs/outputs without knowing exact expected values
5. **Protocol conformance automation** — use `runtime_checkable` protocols and `isinstance` checks to verify every concrete class satisfies its protocol at import time
6. **Specification oracle** — for each documented behavior in `orcapod-design.md`, create a test that constructs the exact scenario described and verifies the documented outcome
7. **Fuzz testing** — feed malformed inputs (wrong types, extreme sizes, Unicode edge cases) to constructors and verify graceful error handling

---

## Implementation Order

1. **`conftest.py`** — shared fixtures (reusable sources, streams, packet functions, databases)
2. **`unit/test_types.py`** — foundational types (Schema, ContentHash, ColumnConfig)
3. **`unit/test_datagram.py`**, **`test_tag.py`**, **`test_packet.py`** — data containers
4. **`unit/test_stream.py`** — stream construction and iteration
5. **`unit/test_sources.py`** + **`test_source_registry.py`** — all source types
6. **`unit/test_hashing.py`** — semantic hasher and handlers
7. **`unit/test_schema_utils.py`** + **`test_arrow_utils.py`** + **`test_arrow_data_utils.py`** — utilities
8. **`unit/test_semantic_types.py`** + **`test_contexts.py`** — type conversion and contexts
9. **`unit/test_databases.py`** — database implementations
10. **`unit/test_packet_function.py`** — packet function behavior
11. **`unit/test_function_pod.py`** — function pod and streams
12. **`unit/test_operators.py`** — all operators
13. **`unit/test_nodes.py`** — function/operator nodes
14. **`unit/test_tracker.py`** + **`test_lazy_module.py`** — remaining units
15. **`integration/`** — all integration test files
16. **`property/`** — property-based tests

## Dependencies

- **hypothesis** — added as a test dependency for property-based testing in `test-objective/property/`
- **pytest** — test runner (already present)
- DeltaTableDatabase tests marked with `@pytest.mark.slow` (skip with `-m "not slow"`)

## Verification

Run the full test suite with:
```bash
uv run pytest test-objective/ -v
```

Run only unit tests:
```bash
uv run pytest test-objective/unit/ -v
```

Run only integration tests:
```bash
uv run pytest test-objective/integration/ -v
```

Run only property tests:
```bash
uv run pytest test-objective/property/ -v
```

## Key Files to Modify/Create

- **New:** `TESTING_PLAN.md` (project root) — the test case catalog document (content mirrors this plan)
- **New:** `test-objective/` directory tree — all files listed in the structure above
- **No modifications** to any existing source code or tests
