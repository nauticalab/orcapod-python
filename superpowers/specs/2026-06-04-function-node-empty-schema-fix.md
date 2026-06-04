# Fix: `FunctionNode.as_table()` Returns Correct Schema When Empty

**Issue:** ENG-572  
**Date:** 2026-06-04  
**Status:** In progress

---

## Overview

`FunctionNodeBase.as_table()` returns a zero-row table with **no columns** when
`iter_data()` yields nothing (pipeline unrun, or upstream produced zero rows). The
function pod's declared output schema is known at construction time but is ignored
in the empty case. This spec describes the minimal fix: add `_make_empty_table()` to
`FunctionNodeBase`, and restructure the empty branch in `as_table()` to use it.

---

## Root Cause

In `FunctionNodeBase.as_table()` (lines 315–426), when `iter_data()` yields nothing:

1. `tag_schema` and `data_schema` remain `None`.
2. Line 333 sets `self._cached_output_table = pa.table({})` as a placeholder.
3. **The code falls through** — there is no early return or `else`. Lines 336–361
   run unconditionally with empty lists and `None` schemas:
   - `pa.Table.from_pylist([], schema=None)` → zero-row table, no columns
   - `hstack_tables(no-col-table, no-col-table)` → zero-row table, no columns
4. Line 359 **overwrites** the `pa.table({})` set in step 2 with the same broken result.

The final `_cached_output_table` has zero columns regardless of the pod's declared output.

---

## Design

### Pattern to follow: `OperatorJobNode._make_empty_table()`

`OperatorJobNode` already solves this identically (lines 816–830):

```python
def _make_empty_table(self) -> "pa.Table":
    tag_schema, data_schema = self.output_schema()
    type_converter = self.data_context.type_converter
    empty_fields: dict = {}
    for name, py_type in {**tag_schema, **data_schema}.items():
        arrow_type = type_converter.python_type_to_arrow_type(py_type)
        empty_fields[name] = pa.array([], type=arrow_type)
    return pa.table(empty_fields)
```

`FunctionNodeBase` already has both `output_schema()` (lines 230–244) and
`data_context` (lines 183–184), so the same pattern applies directly.

The only difference from `OperatorJobNode._make_empty_table()`: call
`output_schema(all_info=True)` so that system tag columns (e.g. `_tag::source:…`)
are included in the empty table. This mirrors what the non-empty path stores in
`_cached_output_table` (the non-empty path uses `tag.arrow_schema(all_info=True)`).

### Changes to `FunctionNodeBase`

**1. Add `_make_empty_table()` method** (new, in `FunctionNodeBase`):

```python
def _make_empty_table(self) -> "pa.Table":
    """Build a zero-row PyArrow table matching this node's full output schema.

    Uses ``output_schema(all_info=True)`` for column names/types and
    ``data_context.type_converter`` for the Python → Arrow type mapping.

    Returns:
        A zero-row ``pa.Table`` with the correct tag and data columns.
        Falls back to an empty table if the function pod is unavailable
        (read-only/deserialized nodes without a live pod).
    """
    if self._function_pod is None:
        return pa.table({})
    tag_schema, data_schema = self.output_schema(all_info=True)
    type_converter = self.data_context.type_converter
    empty_fields: dict = {}
    for name, py_type in {**tag_schema, **data_schema}.items():
        arrow_type = type_converter.python_type_to_arrow_type(py_type)
        empty_fields[name] = pa.array([], type=arrow_type)
    return pa.table(empty_fields)
```

**2. Restructure the empty branch in `as_table()`:**

Before (broken):
```python
if not all_tags:
    self._cached_output_table = pa.table({})  # placeholder, immediately overwritten

converter = ...                        # runs unconditionally
struct_data = ...                      # empty list, schema=None
all_tags_as_tables = pa.Table.from_pylist(all_tags, schema=tag_schema)   # schema=None
all_data_as_tables = pa.Table.from_pylist(struct_data, schema=data_schema)  # schema=None
self._cached_output_table = hstack_tables(...)  # overwrites with empty schema
```

After (fixed):
```python
if not all_tags:
    self._cached_output_table = self._make_empty_table()
else:
    converter = ...
    struct_data = ...
    all_tags_as_tables = pa.Table.from_pylist(all_tags, schema=tag_schema)
    if constants.CONTEXT_KEY in all_tags_as_tables.column_names:
        all_tags_as_tables = all_tags_as_tables.drop([constants.CONTEXT_KEY])
    all_data_as_tables = pa.Table.from_pylist(struct_data, schema=data_schema)
    self._cached_output_table = hstack_tables(all_tags_as_tables, all_data_as_tables)
```

**3. Remove the now-unreachable fallback** at the end of the `if self._cached_output_table is None` block:

```python
# Remove this — it can never be reached after the above restructuring:
if self._cached_output_table is None:
    self._cached_output_table = pa.table({})
```

### Behaviour after the fix

| Scenario | Before | After |
|---|---|---|
| `as_table()`, no data | `pa.table({})` — no columns | `pa.table({"id": [], "result": []})` — correct schema |
| `as_table()`, has data | Correct | Unchanged |
| `as_table(all_info=True)`, no data | `pa.table({})` | Tag + system-tag + data cols (meta cols absent — pre-existing limitation, out of scope) |
| Read-only node (`_function_pod is None`), no data | `pa.table({})` | Same fallback `pa.table({})` — unchanged |

### Column filtering remains unchanged

The existing column-filtering block (lines 365–396) operates on `_cached_output_table`
and drops system tags, meta, source, and context columns per `ColumnConfig`. Because
`_make_empty_table()` includes system tag columns in the cached table, the existing
filtering code handles them correctly for both empty and non-empty cases without
modification.

---

## Tests

File: `tests/test_core/nodes/test_function_node_iteration.py`

### New / updated assertions

**Test 1 — update existing test** `test_as_table_fresh_node_returns_empty_no_compute`:
Add schema assertions after the existing `len(table) == 0` check:
```python
assert "id" in table.column_names      # tag column
assert "result" in table.column_names  # declared output column
```

**Test 2 — new test** `test_as_table_empty_schema_matches_non_empty_schema`:
```python
def test_as_table_empty_schema_matches_non_empty_schema():
    """Empty-case schema matches non-empty-case schema for the same node."""
    node_before = _make_node()
    empty_table = node_before.as_table()

    db = InMemoryArrowDatabase()
    node_after = _make_node(db=db)
    node_after.run()
    full_table = node_after.as_table()

    assert empty_table.num_rows == 0
    assert full_table.num_rows > 0
    assert set(empty_table.column_names) == set(full_table.column_names)
```

---

## `DESIGN_ISSUES.md` update

Add entry under the active bugs section:

```
### FN3 — `FunctionNodeBase.as_table()` returns empty schema for zero-row output
Status: resolved (ENG-572)
Fix: Added `_make_empty_table()` to `FunctionNodeBase` (mirrors `OperatorJobNode`).
     Restructured the empty branch in `as_table()` with an `else` guard so the
     non-empty processing path is skipped when no data exists. The cached table
     is now built from the pod's declared output schema rather than inferred from
     empty lists with `schema=None`.
```

---

## Out of scope

- Meta columns (`__data_id`, `__pod_version` etc.) are not included in the empty table
  because `output_schema()` does not expose them. They are dropped by default column
  filtering anyway, so the default `as_table()` schema matches between empty and
  non-empty cases. `as_table(all_info=True)` still has a discrepancy — file a separate
  issue if needed.
- Schema behavior for `SourceNode`, `OperatorNode`, or other node types.
- Changes to the function pod schema-declaration API.
