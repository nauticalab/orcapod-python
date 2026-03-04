# TODO Analysis

Comprehensive audit of all `TODO`, `FIXME`, and `HACK` comments in `src/orcapod/`.
Each item includes location, context, relevance assessment, estimated effort, and suggested
priority.

**Priority scale:**
- **P0 — Critical:** Correctness bugs, silent data loss, or security issues.
- **P1 — High:** Broken or incomplete features that affect users or downstream consumers.
- **P2 — Medium:** Performance, error-handling, or code-quality issues worth fixing in the
  normal course of development.
- **P3 — Low:** Nice-to-haves, cosmetic improvements, or speculative refactors.

**Effort scale:**
- **XS:** < 30 min, isolated change.
- **S:** 1–2 hours, single file or small cross-cutting.
- **M:** Half-day to one day, multiple files or design work.
- **L:** Multi-day, requires design decisions or broad refactoring.

---

## Summary

| Priority | Count | Description |
|----------|-------|-------------|
| P0       | 2     | Correctness / data integrity |
| P1       | 10    | Incomplete features, broken error handling |
| P2       | 28    | Performance, code quality, deduplication |
| P3       | 32    | Cosmetic, speculative, minor improvements |
| **Total**| **72**| (62 inline TODOs + 10 open DESIGN_ISSUES.md items) |

---

## P0 — Critical

### 1. `FIXME` — `FunctionSignatureExtractor` ignores input/output types
**File:** `hashing/semantic_hashing/function_info_extractors.py:36`
**TODO text:** `FIXME: Fix this implementation!!` / `BUG: Currently this is not using the input_types and output_types parameters`
**Context:** `extract_function_info()` accepts `input_typespec` and `output_typespec` but never uses them. The extracted signature string is therefore type-agnostic — two functions with identical names but different type annotations produce the same hash.
**Relevance:** Still relevant. Affects hash correctness for type-overloaded functions.
**Effort:** S — wire the type specs into the signature string; update tests.
**Priority:** P0

### 2. `TODO` — Source-info column type is hard-coded to `large_string`
**File:** `utils/arrow_utils.py:604`
**TODO text:** `this won't work other data types!!!`
**Context:** In `add_source_info_to_table()`, when source info is a collection it is cast unconditionally to `pa.list_(pa.large_string())`. Any non-string collection values will fail or silently corrupt data.
**Relevance:** Still relevant. Will crash or produce wrong data as soon as non-string source info values are used.
**Effort:** S — inspect collection element types, select appropriate Arrow type.
**Priority:** P0

---

## P1 — High

### 3. `TODO` — Bare `except` in `get_git_info`
**File:** `utils/git_utils.py:55`
**TODO text:** `specify exception`
**Context:** Catches all exceptions including `KeyboardInterrupt` and `SystemExit`. Could silently swallow fatal errors during git info extraction.
**Relevance:** Still relevant.
**Effort:** XS — catch `(OSError, subprocess.SubprocessError, FileNotFoundError)`.
**Priority:** P1

### 4. `TODO` — `flush()` swallows individual batch errors
**File:** `databases/delta_lake_databases.py:817`
**TODO text:** `capture and re-raise exceptions at the end`
**Context:** `flush()` iterates over all pending batches and logs errors individually but never raises. Callers have no way to know that writes failed.
**Relevance:** Still relevant. Silent data loss on partial flush failure.
**Effort:** S — accumulate exceptions, raise an `ExceptionGroup` (or custom aggregate) at end.
**Priority:** P1

### 5. `TODO` — `overwrite` mode when creating Delta table
**File:** `databases/delta_lake_databases.py:856`
**TODO text:** `reconsider mode="overwrite" here`
**Context:** `flush_batch()` uses `mode="overwrite"` when creating a new Delta table. If a table already exists at that path (race condition or stale state), it silently destroys existing data.
**Relevance:** Still relevant. Risk of data loss under concurrent access.
**Effort:** S — switch to `mode="error"` or `mode="append"` with existence check.
**Priority:** P1

### 6. `TODO` — Schema compatibility check lacks strict mode
**File:** `utils/arrow_utils.py:433` and `utils/arrow_utils.py:462`
**TODO text:** `add strict comparison` / `if not strict, allow type coercion`
**Context:** `check_arrow_schema_compatibility()` documents strict vs. non-strict behavior but only implements the non-strict path (and even that raises on type mismatch instead of coercing).
**Relevance:** Still relevant. Users can't choose between strict and permissive checks.
**Effort:** M — implement strict field-order check, non-strict type coercion, and tests.
**Priority:** P1

### 7. `TODO` — Use custom exception for schema incompatibility
**File:** `core/function_pod.py:162`
**TODO text:** `use custom exception type for better error handling`
**Context:** `_validate_input_schema()` raises generic `ValueError` on schema mismatch. Callers can't catch schema errors specifically.
**Relevance:** Still relevant. Error types already exist (`InputValidationError`) — just not used here.
**Effort:** XS — change `ValueError` to `InputValidationError` (or new `SchemaIncompatibilityError`).
**Priority:** P1

### 8. `TODO` — Cache-matching policy not implemented
**File:** `core/packet_function.py:547` and `core/packet_function.py:549`
**TODO text:** `add match based on match_tier if specified` / `implement matching policy/strategy`
**Context:** `get_cached_output_for_packet()` has a `match_tier` parameter that is accepted but ignored. Cache lookups always use exact matching.
**Relevance:** Still relevant — feature is documented in the interface but unimplemented.
**Effort:** M — design matching strategy interface, implement at least exact and fuzzy tiers.
**Priority:** P1

### 9. `TODO` — `is_subhint` does not handle invariance properly
**File:** `utils/schema_utils.py:37`
**TODO text:** `is_subhint does not handle invariance properly`
**Context:** `check_schema_compatibility()` uses beartype's `is_subhint` which treats all generics as covariant. For mutable containers (`list[int]` vs `list[float]`), this can produce incorrect compatibility results.
**Relevance:** Still relevant. Can cause silent type mismatches in schema checks.
**Effort:** S — add invariance-aware wrapper or document the limitation prominently.
**Priority:** P1

### 10. `TODO` — Add system tag columns to cache entry ID
**File:** `core/function_pod.py:1077`
**TODO text:** `add system tag columns`
**Context:** `record_packet_for_cache()` builds a tag table for entry-ID computation but excludes system tags. This means two packets with identical user tags but different provenance (different system tags) get the same cache key — potential cache collisions.
**Relevance:** Still relevant. Affects cache correctness when same user-tags appear from different pipelines.
**Effort:** S — include system tag columns in the tag_with_hash table.
**Priority:** P1

### 11. `TODO` — Delta Lake loads full table to refresh ID cache
**File:** `databases/delta_lake_databases.py:252`
**TODO text:** `replace this with more targetted loading of only the target column and in batches`
**Context:** `_refresh_existing_ids_cache()` calls `to_pyarrow_table()` on the entire Delta table just to extract the ID column. For large tables this is a serious memory bottleneck.
**Relevance:** Still relevant.
**Effort:** S — use Delta Lake column projection (`columns=[id_col]`) and batch reading.
**Priority:** P1

### 12. `TODO` — Delta Lake schema check is reactive, not proactive
**File:** `databases/delta_lake_databases.py:257`
**TODO text:** `replace this with proper checking of the table schema first!`
**Context:** In the same method, if the ID column doesn't exist, a `KeyError` is caught as a fallback. Schema should be validated before loading.
**Relevance:** Still relevant.
**Effort:** XS — load schema metadata first, check for column existence.
**Priority:** P1

---

## P2 — Medium

### 13. `TODO` — Redundant validation in context registry
**File:** `contexts/registry.py:141`
**TODO text:** `clean this up -- sounds redundant to the validation performed by schema check`
**Context:** `_load_spec_file()` performs manual required-field checking followed by JSON Schema validation. The manual check is fully subsumed by the JSON Schema.
**Relevance:** Still relevant.
**Effort:** XS — remove manual field checks.
**Priority:** P2

### 14. `TODO` — Mutable data context setter
**File:** `core/base.py:92`
**TODO text:** `re-evaluate whether changing data context should be allowed`
**Context:** `DataContextMixin.data_context` has a property setter allowing runtime context changes, which could invalidate cached schemas and hashes.
**Relevance:** Still relevant — design decision needed.
**Effort:** XS (remove setter) or M (add cache invalidation).
**Priority:** P2

### 15. `TODO` — Simplify multi-stream handling
**File:** `core/function_pod.py:192`
**TODO text:** `simplify the multi-stream handling logic`
**Context:** `handle_input_streams()` has nested conditionals for single vs. multi-stream inputs.
**Relevance:** Still relevant. Code is functional but harder to follow than necessary.
**Effort:** S — extract into helper with clearer control flow.
**Priority:** P2

### 16. `TODO` — Output schema missing source columns
**File:** `core/function_pod.py:238`
**TODO text:** `handle and extend to include additional columns`
**Context:** `_FunctionPodBase.output_schema()` does not include source-info columns even when requested via `ColumnConfig`.
**Relevance:** Still relevant.
**Effort:** S — extend schema to include source columns conditioned on config.
**Priority:** P2

### 17. `TODO` — Verify dict-to-Arrow conversion correctness
**File:** `core/function_pod.py:503`
**TODO text:** `re-verify the implemetation of this conversion`
**Context:** `as_table()` converts Python dicts to Arrow struct dicts. Edge cases (None, nested optionals) may not be handled.
**Relevance:** Still relevant. Should be addressed with comprehensive tests.
**Effort:** S — add edge-case tests; fix any discovered issues.
**Priority:** P2

### 18. `TODO` — Inefficient system tag column lookup
**File:** `core/function_pod.py:528`
**TODO text:** `get system tags more effiicently`
**Context:** System tag columns are found by scanning all column names by prefix on every `as_table()` call.
**Relevance:** Still relevant.
**Effort:** XS — cache system tag column names during construction.
**Priority:** P2

### 19. `TODO` — Order preservation in content hash computation
**File:** `core/function_pod.py:549`
**TODO text:** `verify that order will be preserved`
**Context:** Content hashes are computed by iterating packets and assumed to align with table row order.
**Relevance:** Still relevant. Correctness depends on an invariant that is not asserted.
**Effort:** XS — add assertion or explicit index tracking.
**Priority:** P2

### 20. `TODO` — Polars detour for table sorting
**File:** `core/function_pod.py:568`
**TODO text:** `reimplement using polars natively`
**Context:** Converts Arrow → Polars → sort → Arrow. PyArrow's `.sort_by()` would be simpler.
**Relevance:** Still relevant. The comment text says "polars natively" but should really say "Arrow natively".
**Effort:** XS — replace with `table.sort_by(...)`.
**Priority:** P2

### 21. `TODO` — Return type of `FunctionPod.process()`
**File:** `core/function_pod.py:691`
**TODO text:** `reconsider whether to return FunctionPodStream here in the signature`
**Context:** Returns `StreamProtocol` but always produces a `FunctionPodStream`. Narrower type would help type checkers.
**Relevance:** Still relevant but low-impact.
**Effort:** XS — update return annotation.
**Priority:** P2

### 22. `TODO` — Consider bytes for cache hash representation
**File:** `core/function_pod.py:1078`
**TODO text:** `consider using bytes instead of string representation`
**Context:** Packet hashes stored as strings (`.to_string()`) rather than raw bytes, doubling storage cost.
**Relevance:** Still relevant for large-scale deployments.
**Effort:** M — change hash column type in DB schema, update all readers/writers.
**Priority:** P2

### 23. `TODO` — Git info extraction should be optional
**File:** `core/packet_function.py:324`
**TODO text:** `turn this into optional addition`
**Context:** `PythonPacketFunction.__init__()` unconditionally calls `get_git_info()`. Fails or slows init in non-git environments.
**Relevance:** Still relevant.
**Effort:** XS — add `include_git_info=True` parameter.
**Priority:** P2

### 24. `TODO` — Execution engine opts not recorded
**File:** `core/packet_function.py:593`
**TODO text:** `consider incorporating execution_engine_opts into the record`
**Context:** `record_packet()` stores execution metadata but omits executor configuration.
**Relevance:** Still relevant for audit trails.
**Effort:** XS — include opts in the record dict.
**Priority:** P2

### 25. `TODO` — `record_packet()` doesn't return stored table
**File:** `core/packet_function.py:639`
**TODO text:** `make store return retrieved table`
**Context:** Method writes to DB and returns nothing. Returning the stored table would enable verification.
**Relevance:** Still relevant.
**Effort:** XS — update DB interface and return value.
**Priority:** P2

### 26. `TODO` — `SourceNode.identity_structure()` assumes root source
**File:** `core/tracker.py:163`
**TODO text:** `revisit this logic for case where stream is not a root source`
**Context:** Delegates directly to stream's identity structure, which may not work for derived sources.
**Relevance:** Still relevant.
**Effort:** S — add isinstance check or protocol-based dispatch.
**Priority:** P2

### 27. `TODO` — `defaultdict` not serializable
**File:** `databases/delta_lake_databases.py:69`
**TODO text:** `reconsider this approach as this is NOT serializable`
**Context:** `_cache_dirty` initialized as `defaultdict(bool)`. Not pickle-serializable.
**Relevance:** Relevant if databases are ever serialized (e.g. multiprocessing).
**Effort:** XS — use regular dict with `.get()` fallback.
**Priority:** P2

### 28. `TODO` — Pre-validation may be unnecessary
**File:** `databases/delta_lake_databases.py:104`
**TODO text:** `consider removing this as path creation can be tried directly`
**Context:** `_validate_record_path()` checks paths before creation; EAFP pattern would be simpler.
**Relevance:** Still relevant.
**Effort:** XS — remove method, rely on try/except.
**Priority:** P2

### 29. `TODO` — Silent deduplication in Delta Lake
**File:** `databases/delta_lake_databases.py:383`
**TODO text:** `consider erroring out if duplicates are found`
**Context:** `_deduplicate_within_table()` silently drops duplicate rows.
**Relevance:** Still relevant. Users may want to be warned about duplicates.
**Effort:** XS — add logging or configurable behavior.
**Priority:** P2

### 30. `TODO` — Naive schema equality check
**File:** `databases/delta_lake_databases.py:467` and `databases/delta_lake_databases.py:485`
**TODO text:** `perform more careful check` / `perform more careful error check`
**Context:** `_handle_schema_compatibility()` uses simple equality and catches all exceptions.
**Relevance:** Still relevant.
**Effort:** S — implement nuanced schema comparison; catch specific exceptions.
**Priority:** P2

### 31. `TODO` — In-memory DB `_committed_ids()` efficiency
**File:** `databases/in_memory_databases.py:128`
**TODO text:** `evaluate the efficiency of this implementation`
**Context:** Converts full ID list to set on every lookup.
**Relevance:** Still relevant for large in-memory tables.
**Effort:** XS — cache the set, invalidate on write.
**Priority:** P2

### 32. `TODO` — Legacy exports in `hashing/__init__.py`
**File:** `hashing/__init__.py:141`
**TODO text:** `remove legacy section`
**Context:** Backwards-compatible re-exports of old API names.
**Relevance:** Still relevant. Should be removed in next breaking release.
**Effort:** S — audit usages, add deprecation warnings, remove in next major version.
**Priority:** P2

### 33. `TODO` — Arrow hasher processes full table at once
**File:** `hashing/arrow_hashers.py:104`
**TODO text:** `Process in batchwise/chunk-wise fashion for memory efficiency`
**Context:** `_process_table_columns()` calls `to_pylist()` on the entire table.
**Relevance:** Still relevant. Memory-intensive for large tables.
**Effort:** M — implement chunk-wise iteration using Arrow's `to_batches()`.
**Priority:** P2

### 34. `TODO` — Visitor pattern for map types incomplete
**File:** `hashing/visitors.py:225`
**TODO text:** `Implement proper map traversal if needed for semantic types in keys/values.`
**Context:** `visit_map()` is a pass-through. Semantic types inside map keys/values are not processed.
**Relevance:** Still relevant. Will break when maps with semantic types are hashed.
**Effort:** S — implement recursive key/value visitation.
**Priority:** P2

### 35. `TODO` — Redis cacher pattern cleanup
**File:** `hashing/string_cachers.py:607`
**TODO text:** `cleanup the redis use pattern`
**Context:** Redis connection initialization is verbose and lacks connection pooling.
**Relevance:** Still relevant.
**Effort:** S — refactor to use connection pool; extract helper.
**Priority:** P2

### 36. `TODO` — Remove redundant validation in column selection operators (×4)
**File:** `core/operators/column_selection.py:58`, `137`, `214`, `292`
**TODO text:** `remove redundant logic` (all four)
**Context:** `SelectTagColumns`, `SelectPacketColumns`, `DropTagColumns`, `DropPacketColumns` each have near-identical `validate_unary_input()` implementations. The only difference is which key set (tag vs. packet) and error message.
**Relevance:** Still relevant. Classic DRY violation.
**Effort:** S — extract shared validation helper, parameterize by key source and message.
**Priority:** P2

### 37. `TODO` — Redundant validation in `PolarsFilterByPacketColumns`
**File:** `core/operators/filters.py:135`
**TODO text:** `remove redundant logic`
**Context:** Same pattern as #36 — duplicated validation logic.
**Relevance:** Still relevant.
**Effort:** XS — reuse the shared helper from #36.
**Priority:** P2

### 38. `TODO` — `PolarsFilter` efficiency
**File:** `core/operators/filters.py:52`
**TODO text:** `improve efficiency here...`
**Context:** `unary_static_process()` materializes the full table, converts to Polars DataFrame, filters, and converts back. For simple predicates this is wasteful.
**Relevance:** Still relevant. Could use Arrow compute expressions directly for simple filters.
**Effort:** M — evaluate Arrow compute vs. Polars for common predicate types.
**Priority:** P2

### 39. `TODO` — Schema simplification in `schema_utils`
**File:** `utils/schema_utils.py:227`
**TODO text:** `simplify the handling here -- technically all keys should already be in return_types`
**Context:** `infer_output_schema()` iterates `output_keys` and checks `verified_output_types` with a fallback to `inferred_output_types`. If all keys are guaranteed present, the fallback logic is dead code.
**Relevance:** Still relevant. Simplification would clarify the contract.
**Effort:** XS — verify invariant with assertion, remove fallback.
**Priority:** P2

### 40. `TODO` — Source column drop verification
**File:** `protocols/core_protocols/streams.py:309`
**TODO text:** `check to make sure source columns are also dropped`
**Context:** `drop_packet_columns()` protocol method — unclear if source-info columns for the dropped packet column are also cleaned up.
**Relevance:** Still relevant. Could leave orphan source-info columns.
**Effort:** S — verify behavior in implementations; add source column cleanup if missing.
**Priority:** P2

---

## P3 — Low

### 41. `TODO` — Older `Union` type support in `DataType`
**File:** `types.py:39`
**TODO text:** `revisit and consider a way to incorporate older Union type`
**Context:** `DataType` supports `type | UnionType` (PEP 604) but not `typing.Union[X, Y]`.
**Relevance:** Low. Modern Python (3.10+) uses `|` syntax. Only matters for legacy code.
**Effort:** S — add `typing.Union` handling to type introspection utilities.
**Priority:** P3

### 42. `TODO` — Broader `PathLike` support
**File:** `types.py:44`
**TODO text:** `accomodate other Path-like objects`
**Context:** `PathLike = str | os.PathLike`. Already covers `pathlib.Path` (which implements `os.PathLike`).
**Relevance:** Low — effectively already handled. The TODO is misleading.
**Effort:** XS — remove or clarify the comment.
**Priority:** P3

### 43. `TODO` — `datetime` in `TagValue`
**File:** `types.py:49`
**TODO text:** `accomodate other common data types such as datetime`
**Context:** `TagValue` is `int | str | None | Collection[TagValue]`. Adding `datetime` has downstream implications for serialization, hashing, and Arrow conversion.
**Relevance:** Still relevant as a feature request but requires careful design.
**Effort:** M — add datetime to union; update serialization, hashing, and Arrow conversion paths.
**Priority:** P3

### 44. `TODO` — Rename `handle_config`
**File:** `types.py:384`
**TODO text:** `consider renaming this to something more intuitive`
**Context:** `ColumnConfig.handle_config()` normalizes config input. Name is vague.
**Relevance:** Still relevant.
**Effort:** S — rename to `normalize()` or `from_input()`; update ~20 call sites.
**Priority:** P3

### 45. `TODO` — `arrow_compat` dict usage
**File:** `core/function_pod.py:499`
**TODO text:** `make use of arrow_compat dict`
**Context:** In `as_table()`, an `arrow_compat` dict exists but is not used during conversion.
**Relevance:** Unclear. May be dead code or incomplete feature.
**Effort:** XS — investigate and either wire up or remove.
**Priority:** P3

### 46. `TODO` — `Batch` operator schema wrapping necessity
**File:** `core/operators/batch.py:91`
**TODO text:** `check if this is really necessary`
**Context:** `unary_output_schema()` wraps all types in `list[T]`. The TODO questions whether this is needed or if the schema could be inferred differently.
**Relevance:** Low — the wrapping is correct by definition (batching produces lists).
**Effort:** XS — verify correctness, remove the TODO.
**Priority:** P3

### 47. `TODO` — Join column reordering algorithm
**File:** `core/operators/join.py:157`
**TODO text:** `come up with a better algorithm`
**Context:** After join, tag columns are reordered to the front via list comprehension. Works but is O(n²) for many columns.
**Relevance:** Low — column counts are typically small.
**Effort:** XS — replace with set-based approach if desired.
**Priority:** P3

### 48. `TODO` — Better error message in `ArrowTableStream`
**File:** `core/streams/arrow_table_stream.py:56`
**TODO text:** `provide better error message`
**Context:** Raises `ValueError("Table must contain at least one column...")` without naming the problematic table/source.
**Relevance:** Still relevant.
**Effort:** XS — include table metadata in message.
**Priority:** P3

### 49. `TODO` — Standard column parsing in `keys()`
**File:** `core/streams/arrow_table_stream.py:171`
**TODO text:** `add standard parsing of columns`
**Context:** `keys()` method handles `ColumnConfig` manually instead of using a standard parser.
**Relevance:** Low.
**Effort:** XS — align with `as_table()` pattern.
**Priority:** P3

### 50. `TODO` — `MappingProxyType` for immutable schema dicts (×2)
**File:** `core/streams/arrow_table_stream.py:188` and `core/streams/base.py:29`
**TODO text:** `consider using MappingProxyType to avoid copying the dicts`
**Context:** Schema dicts are copied on every `output_schema()` call. `MappingProxyType` would provide read-only views without copies.
**Relevance:** Still relevant as minor optimization.
**Effort:** XS — wrap dicts in `MappingProxyType`.
**Priority:** P3

### 51. `TODO` — Sort tag selection logic cleanup
**File:** `core/streams/arrow_table_stream.py:235`
**TODO text:** `cleanup the sorting tag selection logic`
**Context:** `as_table()` selects sort-by tags with an inline conditional. Could be cleaner.
**Relevance:** Low.
**Effort:** XS — extract to helper property.
**Priority:** P3

### 52. `TODO` — Table batch stream support
**File:** `core/streams/arrow_table_stream.py:261`
**TODO text:** `make it work with table batch stream`
**Context:** `iter_packets()` only works with full Arrow tables, not RecordBatches streamed lazily.
**Relevance:** Relevant for future streaming support.
**Effort:** M — implement batch-aware iteration.
**Priority:** P3

### 53. `TODO` — Clean up `iter_packets()` logic
**File:** `core/streams/arrow_table_stream.py:271`
**TODO text:** `come back and clean up this logic`
**Context:** The tag/packet iteration logic has complex batch handling with zip and slicing.
**Relevance:** Still relevant.
**Effort:** S — refactor into clearer helper methods.
**Priority:** P3

### 54. `TODO` — Better `_repr_html_` for streams (×2)
**File:** `core/streams/base.py:329` and `core/streams/base.py:344`
**TODO text:** `construct repr html better`
**Context:** `_repr_html_()` and `view()` both produce basic HTML via Polars DataFrame rendering.
**Relevance:** Low — cosmetic.
**Effort:** S — design better HTML layout.
**Priority:** P3

### 55. `TODO` — `OperatorPodProtocol` source relationship method
**File:** `protocols/core_protocols/operator_pod.py:12`
**TODO text:** `add a method to map out source relationship`
**Context:** Protocol docstring mentions a future method for provenance/lineage mapping.
**Relevance:** Relevant as a feature request.
**Effort:** M — design the API and implement across all operators.
**Priority:** P3

### 56. `TODO` — Substream system
**File:** `protocols/core_protocols/streams.py:38`
**TODO text:** `add substream system`
**Context:** `StreamProtocol` has a placeholder for substream support (e.g., windowed or partitioned views).
**Relevance:** Relevant for future architecture.
**Effort:** L — requires design work.
**Priority:** P3

### 57. `TODO` — Null type default is hard-coded
**File:** `utils/arrow_utils.py:92`
**TODO text:** `make this configurable`
**Context:** `normalize_to_large_types()` maps null type → `large_string`. Should be parameterizable.
**Relevance:** Low.
**Effort:** XS — add parameter.
**Priority:** P3

### 58. `TODO` — Clean up source-info column logic
**File:** `utils/arrow_utils.py:602`
**TODO text:** `clean up the logic here`
**Context:** `add_source_info_to_table()` has nested isinstance checks for collection vs. scalar source info values.
**Relevance:** Related to P0 item #2. Should be addressed together.
**Effort:** S (combined with #2).
**Priority:** P3

### 59. `TODO` — `name.py` location
**File:** `utils/name.py:8`
**TODO text:** `move these functions to util`
**Context:** File is already in `utils/`. TODO is stale.
**Relevance:** Not relevant — already resolved.
**Effort:** XS — delete the comment.
**Priority:** P3

### 60. `TODO` — `pascal_to_snake()` robustness
**File:** `utils/name.py:104`
**TODO text:** `replace this crude check with a more robust one`
**Context:** Simple underscore check for detecting snake_case. Edge cases with acronyms/numbers.
**Relevance:** Low.
**Effort:** XS — use regex `r'^[a-z][a-z0-9_]*$'`.
**Priority:** P3

### 61. `TODO` — Serialization options for Arrow hasher
**File:** `hashing/arrow_hashers.py:64`
**TODO text:** `consider passing options for serialization method`
**Context:** Serialization method is hard-coded in `SemanticArrowHasher`.
**Relevance:** Low — current default works for all supported types.
**Effort:** XS — add parameter.
**Priority:** P3

### 62. `TODO` — Verify Arrow hasher visitor pattern
**File:** `hashing/arrow_hashers.py:115`
**TODO text:** `verify the functioning of the visitor pattern`
**Context:** Visitor pattern for column processing recently added; needs test coverage.
**Relevance:** Still relevant.
**Effort:** S — add targeted unit tests.
**Priority:** P3

### 63. `TODO` — Revisit Arrow array construction logic
**File:** `hashing/arrow_hashers.py:131`
**TODO text:** `revisit this logic`
**Context:** Array construction from processed data may have edge cases.
**Relevance:** Low.
**Effort:** XS — review and add assertions.
**Priority:** P3

### 64. `TODO` — Test None/missing values in precomputed converters
**File:** `semantic_types/precomputed_converters.py:86`
**TODO text:** `test the case of None/missing value`
**Context:** `python_dicts_to_struct_dicts()` may not handle None field values correctly.
**Relevance:** Still relevant.
**Effort:** XS — add test cases.
**Priority:** P3

### 65. `TODO` — Benchmark conversion approaches
**File:** `semantic_types/precomputed_converters.py:106`
**TODO text:** `benchmark which approach of conversion would be faster`
**Context:** Per-row vs. column-wise conversion in `struct_dicts_to_python_dicts()`.
**Relevance:** Low — performance optimization.
**Effort:** S — write benchmark.
**Priority:** P3

### 66. `TODO` — `Any` type handling in schema inference (×4)
**File:** `semantic_types/pydata_utils.py:189`, `semantic_types/type_inference.py:61`, `116`, `124`
**TODO text:** `consider the case of Any`
**Context:** Schema inference functions don't handle `Any` type gracefully when wrapping with `Optional`.
**Relevance:** Still relevant. `Any | None` has unclear semantics.
**Effort:** S — define policy for Any in type inference; apply consistently.
**Priority:** P3

### 67. `TODO` — `_infer_type_from_values()` return type includes `Any`
**File:** `semantic_types/pydata_utils.py:197`
**TODO text:** `reconsider this type hint -- use of Any effectively renders this type hint useless`
**Context:** Return type union includes `Any`, defeating type checking.
**Relevance:** Still relevant.
**Effort:** XS — narrow return type.
**Priority:** P3

### 68. `TODO` — `pydict` vs `pylist` schema inference efficiency
**File:** `semantic_types/semantic_registry.py:35`
**TODO text:** `consider which data type is more efficient and use that pylist or pydict`
**Context:** Converts pydict → pylist before inference. Direct pydict inference may be faster.
**Relevance:** Low.
**Effort:** S — benchmark and potentially add direct pydict path.
**Priority:** P3

### 69. `TODO` — Hardcoded semantic struct type check
**File:** `semantic_types/semantic_struct_converters.py:133`
**TODO text:** `infer this check based on identified struct type as defined in the __init__`
**Context:** `is_semantic_struct()` hardcodes check for `{"path"}` fields instead of using registry.
**Relevance:** Still relevant. Will break when new semantic struct types are added.
**Effort:** S — look up struct type from registry.
**Priority:** P3 (bumped to P2 if new struct types are imminent)

### 70. `TODO` — Better error message in universal converter
**File:** `semantic_types/universal_converter.py:273`
**TODO text:** `add more helpful message here`
**Context:** `python_dicts_to_arrow_table()` raises with minimal context on conversion failure.
**Relevance:** Still relevant.
**Effort:** XS — add input data context to error message.
**Priority:** P3

### 71. `TODO` — Heterogeneous tuple field validation
**File:** `semantic_types/universal_converter.py:477`
**TODO text:** `add check for heterogeneous tuple checking each field starts with f`
**Context:** `arrow_type_to_python_type()` detects tuples from struct fields but doesn't verify `f0, f1, ...` naming.
**Relevance:** Still relevant.
**Effort:** XS — add field name validation.
**Priority:** P3

### 72. `TODO` — `field_specs` type could be `Schema`
**File:** `semantic_types/universal_converter.py:566`
**TODO text:** `consider setting type of field_specs to Schema`
**Context:** Parameter accepts `Mapping[str, DataType]` but could use `Schema` for consistency.
**Relevance:** Low.
**Effort:** XS — update type annotation.
**Priority:** P3

### 73. `TODO` — Unnecessary type conversion step
**File:** `semantic_types/universal_converter.py:611`
**TODO text:** `check if this step is necessary`
**Context:** `_create_python_to_arrow_converter()` calls `python_type_to_arrow_type()` and discards the result. May be a side-effect-dependent validation step.
**Relevance:** Still relevant.
**Effort:** XS — verify if the call has side effects; remove if not.
**Priority:** P3

### 74. `TODO` — `PathSet` recursive structure
**File:** `databases/file_utils.py:392`
**TODO text:** `re-assess the structure of PathSet and consider making it recursive`
**Context:** Commented-out code for recursive path set handling. Appears to be dead code.
**Relevance:** Unclear — may be obsolete.
**Effort:** XS — delete the commented-out code.
**Priority:** P3

---

## Open Items from `DESIGN_ISSUES.md`

These are tracked separately but overlap with some inline TODOs:

| ID | Title | Severity | Relates to TODO # |
|----|-------|----------|-------------------|
| P3 | `PacketFunctionWrapper` missing version | medium | — |
| P4 | Duplicate output schema hash | low | — |
| F2 | Typo "A think wrapper" | trivial | — |
| F3 | Dual URI computation paths | low | — |
| F4 | `FunctionPodNode` not subclass of `TrackedPacketFunctionPod` | medium | — |
| F5 | `FunctionPodStream`/`FunctionPodNodeStream` duplication | medium | — |
| F6 | `WrappedFunctionPod.process` transparency | medium | — |
| F7 | TOCTOU race in `add_pipeline_record` | medium | — |
| F8 | `CallableWithPod` placement | low | — |
| O1 | Operators need streaming/incremental `async_execute` | medium | — |
| G1 | `AddResult` pod type | medium | — |
| G2 | Pod Group abstraction | low | — |

---

## Recommended Action Plan

### Immediate (next sprint)
1. Fix **P0 #1** (FIXME in function signature extractor) — hash correctness.
2. Fix **P0 #2** (source-info column type hardcoding) — data correctness.
3. Fix **P1 #3** (bare except) — XS effort, high value.
4. Fix **P1 #4** (flush error swallowing) — silent data loss.
5. Fix **P1 #7** (custom exception type) — XS effort.
6. Fix **P1 #10** (system tags in cache key) — cache correctness.
7. Fix **P1 #11–12** (Delta Lake ID cache loading) — performance.
8. Delete stale TODO **P3 #59** (`name.py` already in utils).

### Short-term (next 2–3 sprints)
- Address remaining P1 items (#5, #6, #8, #9).
- Tackle P2 cluster: column selection deduplication (#36–37), redundant validation (#13).
- P2 performance: Arrow hasher batching (#33), in-memory DB caching (#31).

### Medium-term
- P2 design decisions: mutable data context (#14), schema strict mode (#6).
- P3 feature requests: substream system (#56), datetime in TagValue (#43).
- DESIGN_ISSUES.md: F4/F5 (FunctionPodNode hierarchy deduplication), O1 (streaming operators).
