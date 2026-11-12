## System tag run stability (ITL-817)

### RS1 — Tag column order through `Join` / `MergeJoin` depended on `PYTHONHASHSEED`
**Status:** resolved
**Severity:** high
**Issue:** ITL-817

`Join.static_process` and `MergeJoin.binary_static_process` built `tag_columns` from a `set`,
so the order of user tag columns on every emitted `Tag` changed from process to process.
`FunctionJobNode._load_cached_entries` rebuilds cached rows with the compile-time tag order,
so a node output mixing cached and freshly computed rows could not be concatenated:
`SyncPipelineOrchestrator._materialize_as_stream` raised
`ArrowInvalid: Schema at index N was different`.

**Fix:** Column order follows the operator's argument symmetry. `Join` and `MergeJoin` are
commutative, so they list tag and data columns by first appearance across the inputs in
canonical `pipeline_hash` order: `Join(a, b)` and `Join(b, a)` give the same keys, schema,
table columns and per-row keys, in every process and on the async path. The order is defined
once, in `output_schema` / `binary_output_schema`; `static_process` reads it back and lays
the table out to match. `SemiJoin` is ordered and keeps the left operand's columns.
`arrow_utils.concat_tables_by_name` aligns per-row tables by column name in both buffer
materializers, so any operator that emits a different column order is tolerated. Column
order only: `arrow_hasher.hash_table` and the semantic hasher ignore column and key order,
so no record ID, system tag, `content_hash` or `pipeline_hash` changes.
