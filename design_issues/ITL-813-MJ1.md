## `src/orcapod/core/operators/merge_join.py`

### MJ1 — `MergeJoin` raises when an input stream has zero rows
**Status:** resolved
**Severity:** high
**Issue:** ITL-813

`MergeJoin.binary_static_process` built two columns with `pa.array(...)` and no explicit
type. `pa.array([])` is `null`-typed, so with a zero-row input:

1. The `_common` helper join key was `null` on the empty side and `int64` on the other, and
   Polars raised `SchemaError: datatypes of join keys don't match`.
2. With both sides empty (or once 1 is fixed) the merged list column of a colliding data
   column, and its source-info column, were `null`-typed, and schema reconstruction raised
   `ValueError: A null type field may not be non-nullable`.

`Join` had the same helper-key bug in 0.1.4; it was fixed on `main` as a side effect of
ITL-563 but without tests.

**Fix:** The helper key is typed `pa.int8()` (as in `Join`). Merged columns and their
source-info columns are built with an explicit type taken from the inputs (see MJ2), never
inferred from the values, so a zero-row result has exactly the types of a non-empty one and
the predicted schema; the two concatenate. `tests/test_core/operators/test_empty_inputs.py`
covers zero-row inputs for every operator, comparing the materialized schema with the
predicted `output_schema()`; `test_merge_join_arrow_types.py` covers zero-row left, right
and both inputs for every element type.
