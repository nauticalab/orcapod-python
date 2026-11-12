## `src/orcapod/core/data_function.py`

### P4 — `PythonDataFunction` computes the output schema hash twice
**Status:** resolved
**Severity:** low
`__init__` stores `self._output_schema_hash` (line ~289). `DataFunctionBase` also lazily
caches `self._output_data_schema_hash` (different attribute name) via
`output_data_schema_hash`. Two fields holding the same value. One is redundant.

**Fix:** Removed the eager `self._output_schema_hash` assignment from
`PythonDataFunction.__init__`. The canonical schema hash is now computed and cached exclusively
by `DataFunctionBase.output_data_schema_hash` (stored in `_output_data_schema_hash`).
