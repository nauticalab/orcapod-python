## `src/orcapod/core/nodes/function_node.py`

### FN1 — a function node's table rebuilt its tags from Python values with inferred types
**Status:** resolved
**Severity:** critical
**Issue:** ITL-928

`FunctionNodeBase.as_table` built its table from `tag.as_dict()` and `data.as_dict()` Python
values, typed by the node's Python output schema through the type converter. A function pod
never touches tags, yet the tag columns came back with types inferred from Python: every
integer width as `int64`, `float32` as `double`, every naive or zoned timestamp as
`timestamp[us, tz=UTC]`, `date64` as `date32`; a `time`, `duration`, `decimal`, `uuid` or
`Path` tag made it raise. It happened on every database, sync and async, while `iter_data()`,
`get_all_records()` and the stored pipeline rows kept the input types. Joining a node's output
with another source on such a tag raised "Join inputs disagree on the Arrow type". The same
build also dropped the `_source_<output>` and `_context_key` columns that the pod's stream
outside a job has. `FunctionPodStream.as_table`, the same pod outside a job, built its tag
table from Python values too (typed with the tag's own Arrow schema), so only a `uuid` or
`Path` tag broke it ("Expected bytes, got a 'UUID' object").

**Fix:** both build their tag table from each row's own Arrow table (`tag.as_table()`), and the
node its data table from `data.as_table()` (source info and context, no meta columns),
concatenated by name; rows that disagree on a column's Arrow type raise
`InputValidationError` naming the column and both types, never a widened type. No stored value, record id or hash changes. The zero-row table of a
node that has not run is still built from the Python output schema (the ITL-912 problem, for
function nodes).
The Delta database's own type changes are entry DT1 (`design_issues/ITL-928-DT1.md`).
