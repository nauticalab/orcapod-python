# ITL-616: Fix `PollingSource._combine` leaking `_content_hash` into the data schema

**Date:** 2026-08-25
**Issue:** [ITL-616](https://linear.app/metamorphic/issue/ITL-616)
**Branch:** `eywalker/itl-616-pollingsource_combine-leaks-_content_hash-into-the-data`

## Overview

`PollingSource._combine` passes `all_info=True` to `as_table()` on both streams before
concatenating them into the new accumulated stream. `all_info=True` resolves to
`ColumnConfig.all()`, which includes `content_hash=True`. In `ArrowTableStream.as_table()`,
`content_hash=True` dynamically appends a `_content_hash` column to the output table — a
synthetic, on-demand column that is not part of `ArrowTableStream._table` (internal storage).

The concatenated table (now containing `_content_hash`) is passed directly to
`ArrowTableStream.__init__`. Since `_content_hash` has no recognized prefix (`_tag::`,
`_source_`, `_context_key`), the constructor treats it as a user data column and stores it
in `_data_columns`.

This causes two problems:

1. **Silent schema mutation** — after the first accumulating combine, `output_schema()` and
   `keys()` report `_content_hash` as a data column, which is incorrect.
2. **Crash on the second combine** — `_validate_combining_schemas` compares the accumulated
   stream (which now has `_content_hash` in `keys()`) against a fresh batch (which does not),
   and raises `SchemaInconsistencyError`. A polling source emitting one new row per poll
   silently corrupts its schema on the second poll and crashes on the third.

## Audit scope

All other `as_table(all_info=True)` call sites were audited:

- `ArrowTableStream.identity_structure()` — result is hashed only, never fed into a new
  `ArrowTableStream`. Safe.
- `function_pod.py`, `function_node.py` — call `tag.as_dict(all_info=True)` /
  `data.as_dict(all_info=True)` on `Tag`/`Data` datagrams, not streams. Safe.
- All operators (`join.py`, `merge_join.py`, `static_output_pod.py`, etc.) — use explicit
  `ColumnConfig` dicts that never include `content_hash`. Safe.

The only affected site is `PollingSource._combine`.

## Fix

### Module-level constant

Add a named constant at module level in `polling_source.py` (below the existing imports,
above the `PollingSource` class):

```python
# ColumnConfig used when concatenating streams in _combine.
# Includes the provenance columns (system_tags, source, context) that
# ArrowTableStream.__init__ knows how to parse and split into their
# respective internal tables.
# content_hash is intentionally absent: it is a synthetic, on-demand
# column produced by as_table(); including it would bake it into stored
# data and corrupt the data schema on the next combine.
_STREAM_COMBINE_COLUMNS = ColumnConfig(system_tags=True, source=True, context=True)
```

### Change in `_combine`

Replace the two `as_table(all_info=True)` calls with `as_table(columns=_STREAM_COMBINE_COLUMNS)`:

```python
# Before
combined = pa.concat_tables(
    [
        existing.as_table(all_info=True),
        new_stream.as_table(all_info=True),
    ],
    promote_options="default",
)

# After
combined = pa.concat_tables(
    [
        existing.as_table(columns=_STREAM_COMBINE_COLUMNS),
        new_stream.as_table(columns=_STREAM_COMBINE_COLUMNS),
    ],
    promote_options="default",
)
```

All other code in `_combine` is unchanged: `_validate_combining_schemas` is still called
first, and the `ArrowTableStream` constructor call at the end is unchanged.

### Why this ColumnConfig is correct

`_STREAM_COMBINE_COLUMNS` includes exactly the columns that `ArrowTableStream.__init__`
knows how to parse and store:

| Flag | What it includes | Handled by |
|---|---|---|
| `system_tags=True` | `_tag::source:<hash>` columns | Detected by `_tag::` prefix in `__init__` |
| `source=True` | `_source_<col>` provenance columns | Extracted by `prepare_prefixed_columns` in `__init__` |
| `context=True` | `_context_key` column | Split off by `split_by_column_groups` in `__init__` |
| `content_hash` (absent) | `_content_hash` (synthetic) | Would land in `_data_columns` — excluded |

## Tests

Two new tests in `TestPollingSourceSchemaValidation` in
`tests/test_channels/test_polling_source.py`, covering the exact failure mode (existing tests
cover only a single combine; two combines are sufficient to trigger the crash):

### Sync test: `test_sync_three_fetches_no_content_hash_leak`

Three `iter_data()` calls against a `FakeDynamicSource` with 3 batches:
- Assert row counts: 1, 2, 3 (accumulation working correctly)
- Assert `"_content_hash"` is not in `src.keys()[1]` (data keys)
- Assert `"_content_hash"` is not in `src.output_schema()[1]` (data schema)

### Async test: `test_async_three_fetches_no_content_hash_leak`

Three-batch async drain using existing `PollingConfig` timing patterns:
- Assert 3 items emitted
- Assert `"_content_hash"` is not in `src._accumulated_stream.keys()[1]`
- Assert `"_content_hash"` is not in `src._accumulated_stream.output_schema()[1]`

## `DESIGN_ISSUES.md`

Add entry **PS3** to the `src/orcapod/core/sources/polling_source.py` section (after PS2),
with status `in progress` during development and `resolved` on merge. See the full entry
text in the design session above.

## What is not changed

- `_validate_combining_schemas` — unchanged; it correctly catches real schema drift between
  batches.
- `ArrowTableStream.__init__` — no defensive stripping added. The correct fix is at the call
  site: `_combine` should never request `content_hash` when building a storage table.
- No Polars intermediate step — the fix stays entirely in PyArrow.
