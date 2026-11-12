## `src/orcapod/core/function_pod.py`

### F13 — `_FunctionPodBase.output_schema()` omits source-info columns
**Status:** open
**Severity:** medium

`output_schema()` (line ~238) does not include source-info columns even when `ColumnConfig`
requests them:
```python
# TODO: handle and extend to include additional columns
```

This means callers using `columns={"source": True}` on a FunctionPod's output schema get an
incomplete schema, inconsistent with `as_table()` which does include source columns.
