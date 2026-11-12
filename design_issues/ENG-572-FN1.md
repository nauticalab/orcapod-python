## `src/orcapod/core/nodes/function_node.py`

### FN1 — `FunctionNodeBase.as_table()` returned empty schema when no data existed
**Status:** resolved
**Severity:** high
**Issue:** ENG-572

`as_table()` on an unrun node returned a zero-row table with no columns because
the `if not all_tags: self._cached_output_table = pa.table({})` assignment was
immediately overwritten by the fall-through `hstack_tables` call with
`schema=None`.

**Fix:** Restructured `as_table()` to derive schema from `self.output_schema()`
upfront and branch immediately on empty vs. non-empty, so the empty case creates
a properly-schemed zero-row table. Extracted column-filtering logic into
`arrow_utils.apply_column_config()` standalone function.  Also fixed
`DATACLASS_TYPE_FIELD` sentinel rename (`__type` → `__dataclass.`) and
`arrow_schema_to_python_schema` returning `Any` for dataclass structs
(now synthesizes a concrete dataclass type).
