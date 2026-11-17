## `src/orcapod/core/operators/join.py` — async path

### TD1 — A name could be a tag in one input and a data column in another
**Status:** resolved
**Severity:** high
**Issue:** ITL-940

`Join` and `MergeJoin` accepted an input whose tag is `x` together with an input whose data
column is `x`: `keys()` and `output_schema()` returned a result. Reading it raised a raw
`KeyError: Field "x" exists 2 times`, and the async `Join` yielded rows in which tag `x` and
data `x` held different values (a wrong result with no error). `Index(out=<tag>)`,
`Pick(out=<tag>)` and a function pod whose output key equals an input tag were accepted the
same way and failed, or gave mismatched values, when read.

**Fix:** Each is rejected with `InputValidationError` naming the column, the roles and the
remedy. `Join` checks every pair of inputs in the schema prediction (so at graph build and in
`static_process`) and on the first row of each side of the async join; `MergeJoin` checks in
its schema prediction and at the start of `binary_static_process` (the async path);
`Index` and `Pick` in `validate_unary_input`; a function pod in `validate_inputs`,
`process`, `output_schema`, `FunctionNode` construction and the first row of
`async_execute`. `SemiJoin` is not affected: it matches on every common column, tag or data,
and returns the left stream's columns unchanged.
