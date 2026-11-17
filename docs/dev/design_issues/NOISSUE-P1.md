## `src/orcapod/core/data_function.py`

### P1 — `parse_function_outputs` is dead code
**Status:** resolved
**Severity:** medium
`parse_function_outputs` is a module-level function with a `self` parameter, suggesting it was
originally a method. It is never called. `PythonDataFunction.call` duplicates its logic verbatim.
Should be deleted or wired up as a method on `DataFunctionBase` and used from `call`.

**Fix:** Converted to a proper standalone function `parse_function_outputs(output_keys, values)`.
Replaced the duplicated unpacking block in `PythonDataFunction.call` with a call to it.
Updated tests accordingly.
