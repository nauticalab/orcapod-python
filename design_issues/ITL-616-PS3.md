## `src/orcapod/core/sources/polling_source.py`

### PS3 — `_combine` leaks `_content_hash` into the data schema on the second accumulating fetch
**Status:** resolved
**Severity:** high
**Issue:** ITL-616

`_combine` calls `as_table(all_info=True)` on both streams before concatenating them.
`all_info=True` resolves to `ColumnConfig.all()`, which includes `content_hash=True`.
In `ArrowTableStream.as_table()`, `content_hash=True` dynamically appends a `_content_hash`
column to the output table. This is a synthetic column — computed on demand, not stored in
`ArrowTableStream._table`.

`pa.concat_tables` then includes `_content_hash` in the combined table, which is passed
directly to `ArrowTableStream.__init__`. Since `_content_hash` has no recognized prefix
(`_tag::`, `_source_`, `_context_key`), it lands in `_data_columns` as if it were user data.

On the next `_combine` call, `_validate_combining_schemas` compares:
- `existing.keys()` → includes `_content_hash` in data keys (baked in from previous combine)
- `new_stream.keys()` → no `_content_hash` (freshly built from raw fetched data)

This raises `SchemaInconsistencyError`. A polling source emitting one new row per poll will
change its data schema on the second new-data poll and crash on the third.

**Fix:** Added `_STREAM_COMBINE_COLUMNS = ColumnConfig(system_tags=True, source=True, context=True)` constant and replaced `as_table(all_info=True)` with `as_table(columns=_STREAM_COMBINE_COLUMNS)` in `_combine`. `content_hash` is intentionally excluded — it is a synthetic output column, never a stored one.
