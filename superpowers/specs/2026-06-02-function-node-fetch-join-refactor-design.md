# Design: Extract `_fetch_joined_records` to Eliminate Redundancy in `FunctionJobNode`

**Linear:** ENG-377
**Date:** 2026-06-02
**Status:** Approved

---

## Overview

`FunctionJobNode` has two methods — `get_all_records` and `_load_cached_entries` — that
independently implement the same ~10-line "fetch from both DBs, content-hash-filter,
inner-join, restore nullability" pattern. `get_cached_results` (the public cache façade)
already delegates to `_load_cached_entries` rather than duplicating logic, so partial
consolidation has happened — but the join core itself is still duplicated.

This design extracts that shared core into a single private helper, `_fetch_joined_records`,
and updates both callers to use it. No public API changes.

---

## Method roles and invocation chain

```
get_cached_results(entry_ids)
    ├─ checks _cached_output_datas (in-memory cache) for each id
    └─ calls _load_cached_entries(missing_ids) for cache misses
               └─ calls _fetch_joined_records(entry_ids=missing_ids)  ← shared core
               └─ converts joined table rows → dict[entry_id, (tag, data)]

get_all_records(columns, all_info)
    └─ calls _fetch_joined_records()                                   ← shared core
    └─ applies ColumnConfig column-drop logic → pa.Table | None
```

Each method has a distinct, non-overlapping responsibility:

| Method | Responsibility | Returns |
|---|---|---|
| `_fetch_joined_records(entry_ids)` | Primitive: fetch both DBs, content-hash-filter, inner-join | `pa.Table \| None` |
| `_load_cached_entries(entry_ids)` | DB loader: convert joined table rows to `(tag, data)` tuples keyed by entry ID | `dict[str, tuple[Tag, Data]]` |
| `get_cached_results(entry_ids)` | Public cache façade: serve from in-memory cache; delegate misses to `_load_cached_entries` | `dict[str, tuple[Tag, Data]]` |
| `get_all_records(columns, all_info)` | Public table view: fetch all joined records and apply user-facing column filtering | `pa.Table \| None` |

---

## `_fetch_joined_records` specification

### Signature

```python
def _fetch_joined_records(
    self,
    entry_ids: list[str] | None = None,
) -> pa.Table | None:
```

### Behaviour

1. **Guard:** return `None` if `self._cached_function_pod is None or self._pipeline_database is None`.
2. Fetch `taginfo` from `self._pipeline_database.get_all_records(self.node_identity_path, record_id_column=_PIPELINE_ENTRY_ID_COL)`.
3. Fetch `results` from `self._cached_function_pod._result_database.get_all_records(self._cached_function_pod.record_path, record_id_column=constants.DATA_RECORD_ID)`.
4. Return `None` if either fetch returns `None`.
5. Apply `self._filter_by_content_hash(taginfo)`.
6. Inner-join `taginfo` and `results` on `DATA_RECORD_ID` via polars.
7. If `entry_ids` is not `None`, filter the polars DataFrame to rows whose `_PIPELINE_ENTRY_ID_COL` value is in `entry_ids` — done in polars before `.to_arrow()` to avoid a round-trip.
8. Convert to Arrow and call `arrow_utils.restore_schema_nullability`.
9. Return the raw `pa.Table`. The table always includes the `_PIPELINE_ENTRY_ID_COL` column (`__pipeline_entry_id`).

### What it does NOT do

- No column config filtering (that is `get_all_records`'s job).
- No tuple conversion (that is `_load_cached_entries`'s job).
- No in-memory cache reads or writes (that is `get_cached_results`'s job).

### The `__pipeline_entry_id` column in `get_all_records`

`_fetch_joined_records` always includes `__pipeline_entry_id` in the returned table.
`get_all_records` does not need to handle this specially: `__pipeline_entry_id` starts
with `__` (the `META_PREFIX`), so it is automatically swept into the existing meta-column
drop when `column_config.meta` is False (the default). It is also dropped explicitly if
`column_config.all_info` is True, because `NODE_CONTENT_HASH_COL` and `__pipeline_entry_id`
are both internal discriminator columns, not user-facing data.

---

## Guard normalization

Currently, `get_all_records` guards only on `_cached_function_pod is None`, while
`_load_cached_entries` guards on `_cached_function_pod is None or _pipeline_database is None`.
In practice the two databases are always attached together, so the guards are equivalent —
but the inconsistency is confusing.

`_fetch_joined_records` uses the stricter, explicit form:

```python
if self._cached_function_pod is None or self._pipeline_database is None:
    return None
```

Both `get_all_records` and `_load_cached_entries` drop their individual guards and rely on
the helper returning `None` / `{}` for the no-DB case.

---

## Constant promotion

`PIPELINE_ENTRY_ID_COL = "__pipeline_entry_id"` is currently a local variable inside
`_load_cached_entries`. Promote it to a module-level private constant
`_PIPELINE_ENTRY_ID_COL` so both callers reference the same definition without a
`get_all_records`-visible name.

---

## Docstrings

All four methods get updated Google-style docstrings that:

- State the method's single responsibility in the first line.
- Explain what the method does NOT do (to prevent future scope creep).
- Reference the invocation chain where relevant (e.g., `get_cached_results` docstring
  notes it delegates misses to `_load_cached_entries`; `_load_cached_entries` notes it
  calls `_fetch_joined_records`).

---

## Tests

### Existing tests (must pass unchanged)

- `tests/test_core/nodes/test_function_node_get_cached.py` — all five `TestGetCachedResults` tests.
- All integration tests that call `get_all_records` indirectly via pipeline execution.

### New tests for `_fetch_joined_records`

Add `tests/test_core/nodes/test_function_node_fetch_joined.py`:

| Test | Asserts |
|---|---|
| `test_returns_none_when_no_db` | Returns `None` when `_cached_function_pod` is not set |
| `test_returns_none_when_db_fetch_returns_none` | Returns `None` when either `_pipeline_database.get_all_records` or `_result_database.get_all_records` returns `None` (e.g. no records written yet) |
| `test_returns_empty_table_when_join_produces_no_rows` | Returns a 0-row `pa.Table` (not `None`) when both DB fetches succeed but the inner join finds no matching `DATA_RECORD_ID`; callers check `num_rows` themselves |
| `test_returns_joined_table_with_entry_id_column` | After execution, returns a table that includes `__pipeline_entry_id` |
| `test_entry_ids_filter_narrows_rows` | Passing `entry_ids` returns only matching rows |
| `test_no_entry_ids_returns_all_rows` | Passing `entry_ids=None` returns all rows |

---

## Scope

**In scope:**
- Extract `_fetch_joined_records` as described.
- Update `get_all_records` and `_load_cached_entries` to call it.
- Promote the `__pipeline_entry_id` string to a module-level constant.
- Normalize the `None` guard.
- Update docstrings on all four methods.
- Add `test_function_node_fetch_joined.py`.

**Out of scope:**
- Changing the public signatures of `get_cached_results` or `get_all_records`.
- Any refactoring outside `FunctionJobNode` / its test file.
- Performance changes to the join strategy.
