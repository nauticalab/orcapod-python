## `src/orcapod/core/data_function.py` — returned values not checked against the declared type

### FO1 — A pod declared `-> int` stored `1.5` as `1`
**Status:** resolved
**Severity:** high
**Issue:** ITL-895

`PythonDataFunction._build_output_data` built `Data(output, python_schema=declared)` without
looking at the values. The Arrow table was built later with
`pa.Table.from_pylist(rows, schema=...)`, and the type converter passes primitives through
unchanged. pyarrow's integer converter calls `__int__` on a non-`int`, so `1.5`,
`numpy.float64(1.5)`, `Decimal` and `Fraction` were truncated. Numbers were read as epoch ticks
for date and timestamp fields, `bytes` and `str` were decoded and encoded into each other, and
`None` became a NULL in a not-null field. The `Data` kept the raw dict, so `as_dict()` gave
`1.5` while `as_table()`, the store and the hash gave `1`. Downstream pods read `as_dict()`, so
the same graph gave different results standalone, on the sync orchestrator and on the async
orchestrator. Present in every release up to v0.2.0. Root-cause evidence:
`superpowers/specs/itl-895-float-to-int/FINDINGS.md`.

**Fix (function outputs):** The shared `semantic_types/output_conformance.py` check
runs in `PythonDataFunction._build_output_data` for both sync and async execution. It
checks primitive and nested types, incorporates the ndarray guard, and delegates exact or
structural class acceptance to the active logical type. Mismatches raise `OutputTypeError`
with one diagnostic on standalone and stored paths. Returned values and mapping keys are
absent from diagnostic text and chained converter tracebacks.

The November 12 decision supersedes the earlier warning conversions: only exactly represented
same-kind numeric values are accepted across Python/NumPy types. Unrelated container types, numeric narrowing, and non-subclass kind changes require
explicit conversion. Python subclasses are accepted; the declared logical type controls
storage. Base storage omits subclass-only fields (and their contribution to row hashes);
`bool` for `int` and `datetime` for `date` also follow this subclass rule. Storage is validated eagerly; numeric exceptions are normalized while compatible
user-class objects retain their identity in memory.
Regressions: `tests/test_core/function_pod/test_output_conformance.py`.

Rows stored wrongly before the fix are served unchanged. They are not repaired automatically.
