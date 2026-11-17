# Design: Empty Table Nullability Fix (ITL-563)

**Date:** 2026-07-22
**Issue:** [ITL-563](https://linear.app/metamorphic/issue/ITL-563/empty-failed-outputs-lose-tag-nullability-and-abort-error)
**Status:** Approved

## Overview

When a function fails under `error_policy="continue"`, its empty output table is
constructed by looping over schema fields with `python_type_to_arrow_type` and
passing the resulting dict to `pa.table()`. Because `pa.table(dict_of_arrays)`
marks every field `nullable=True` regardless of the input types, required fields
(`str`, `int`, …) lose their non-nullable annotation. A downstream `Join` then
sees incompatible schemas between its inputs and raises an `InputValidationError`
that aborts the entire pipeline — bypassing the `continue` policy.

The same pattern appears in five locations across the codebase.

## Root Cause

`pa.table(dict_of_arrays)` always produces `nullable=True` fields. The existing
`type_converter.python_schema_to_arrow_schema()` method already handles
nullability correctly (`T` → `nullable=False`, `T | None` → `nullable=True`)
and completes the bidirectional round-trip with `arrow_schema_to_python_schema`,
but it is not used when building empty tables.

## Goals & Success Criteria

- Materializing an empty buffer preserves the declared required/optional schema
  (verified by reading `ArrowTableStream.output_schema()` after construction).
- A failed function under `error_policy="continue"` feeding a `Join` does not
  abort orchestration — the pipeline completes and the join produces zero rows.
- The same nullability preservation holds for empty side-effect stream tables
  and empty `DerivedSource` cache tables.
- All five previously buggy construction sites are replaced by a single shared
  utility that cannot regress independently.

## Scope & Boundaries

**In scope:**
- Add `make_empty_table(python_schema, type_converter)` to `arrow_utils.py`.
- Replace all five buggy sites with calls to the new utility.
- Add unit and integration tests covering nullability preservation and the
  `error_policy="continue"` + `Join` pipeline scenario.
- Strengthen the existing `test_as_table_empty_schema_matches_non_empty_schema`
  with a nullability assertion.

**Out of scope:**
- Changing `Join` input-compatibility logic or loosening its validation.
- Making tags optional by default.
- The two `pa.table({})` fallbacks in `operator_node.py` (L1065) and
  `tag_data.py` (L346) — these are intentionally schema-free edge cases.
- `async_orchestrator.py` — it uses channels, not materialized buffers, and is
  not affected.

## Architecture

### New utility: `arrow_utils.make_empty_table`

```python
# src/orcapod/utils/arrow_utils.py

def make_empty_table(python_schema: dict, type_converter) -> pa.Table:
    """Return a zero-row PyArrow table whose field nullability matches python_schema.

    Uses python_schema_to_arrow_schema so that plain types (str, int, …) produce
    nullable=False fields and Optional types (str | None) produce nullable=True
    fields. This preserves the round-trip guarantee through
    ArrowTableStream.output_schema().

    Args:
        python_schema: Mapping of field name to Python type annotation.
        type_converter: A UniversalTypeConverter instance.

    Returns:
        A zero-row pa.Table with the correct Arrow schema.
    """
    arrow_schema = type_converter.python_schema_to_arrow_schema(python_schema)
    return pa.Table.from_batches([], schema=arrow_schema)
```

### Call-site changes (5 sites)

| File | Location | Change |
|---|---|---|
| `src/orcapod/pipeline/sync_orchestrator.py` | L181–198 `_materialize_as_stream()` | Replace 8-line loop + `pa.table()` with `make_empty_table({**tag_schema, **data_schema}, type_converter)` |
| `src/orcapod/core/nodes/operator_node.py` | L835–849 `_make_empty_table()` | Replace body with `make_empty_table({**tag_schema, **data_schema}, self.data_context.type_converter)` |
| `src/orcapod/side_effects.py` | L160–176 `SideEffectFunctionStream.as_table()` | Same one-liner replacement |
| `src/orcapod/side_effects.py` | L720–736 `SideEffectJobFunctionStream.as_table()` | Same one-liner replacement |
| `src/orcapod/core/sources/derived_source.py` | L75–95 DerivedSource cache | Build `python_schema = {k: tag_schema[k] for k in tag_keys}; python_schema.update(data_schema)`, then call `make_empty_table` |

`operator_node.py`'s `_make_empty_table()` is kept as a thin wrapper (it is
already called from three internal sites) so its callers need no changes.

### Error handling

No new error surface. `python_schema_to_arrow_schema` raises `TypeError` on
unsupported types — the same exception that `python_type_to_arrow_type` raises
today. `pa.Table.from_batches([], schema=...)` cannot fail on a valid schema.

## Testing Plan

### Unit tests — `tests/test_utils/test_arrow_utils.py` (new file)

1. `test_make_empty_table_preserves_required_fields` — schema with `str`, `int`
   produces `nullable=False` Arrow fields; table has zero rows.
2. `test_make_empty_table_preserves_optional_fields` — schema with `str | None`,
   `int | None` produces `nullable=True` fields.
3. `test_make_empty_table_mixed_nullability` — mixed schema (`str`, `int | None`)
   produces the correct `nullable` per field.
4. `test_make_empty_table_round_trips_through_arrow_table_stream` — pass the
   result to `ArrowTableStream.output_schema()` and assert the Python schema is
   identical to the input schema.

### Integration tests — `tests/test_pipeline/test_error_policy_continue.py` (new file)

5. `test_failed_function_with_join_does_not_abort_pipeline` — topology from the
   issue: `source → failing_function → Join ← source`, under
   `error_policy="continue"`. Assert: failing function is logged, Join produces
   zero rows, orchestration completes without raising.
6. `test_empty_buffer_schema_preserves_nullability` — run
   `_materialize_as_stream()` on a node with an empty buffer; assert the
   returned `ArrowTableStream.output_schema()` matches the declared schema
   exactly.

### Regression tests for side_effects and derived_source

7. `test_side_effect_empty_table_schema_preserves_nullability` — empty
   `SideEffectFunctionStream.as_table()` returns a table with correct
   nullable/non-nullable fields.
8. `test_derived_source_empty_cache_preserves_nullability` — empty
   `DerivedSource` cache table has correct field nullability.

### Existing test strengthened

- `test_as_table_empty_schema_matches_non_empty_schema` — add a nullability
  assertion:
  ```python
  assert all(
      empty_table.schema.field(n).nullable == full_table.schema.field(n).nullable
      for n in empty_table.column_names
  )
  ```
