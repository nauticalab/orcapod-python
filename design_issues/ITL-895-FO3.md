## `src/orcapod/core/data_function.py` — returned values not checked against the declared type

### FO3 — A function that reads subclass-only fields gets path-dependent, reusable results
**Status:** open
**Severity:** medium
**Issue:** ITL-895

A subclass returned for a declared base type is passed on in memory as it is and stored as
the base: the declared type's logical type decides what is stored, and the row hash covers
the stored fields only. A downstream function that reads a field its declared parameter type
does not have (`getattr(y, "extra", ...)` on a `Reading` parameter that received an
`ExtendedReading`) therefore gets results that depend on the execution path, and results that
are reused across values:

- standalone and the async orchestrator hand the function the in-memory subclass object
  (within one async run a row may instead be served the result another row of the same
  row hash stored first; which happens depends on timing, seen on Python 3.13 in CI);
- the sync orchestrator hands it the stored base value, without the extra field;
- two values that differ only in subclass-only fields have the same row hash, so a later row
  is served the result computed for the earlier one, without a call.

The same holds for a `datetime` returned for a declared `date`. Also in scope: a `list` or
`dict` subclass returned for a declared `list[T]` / `dict[K, V]` is rebuilt as a plain
`list` / `dict` in memory.

Decision (developer, 2026-11-12): "this is a limitation of Python language by design and
... we should not try to fight it. How well this is handled is thus the job of the extension
logical type and we shouldn't over engineer a solution to restrict down things." No code
change before v0.3; follow-up ITL-978 (https://linear.app/metamorphic/issue/ITL-978).

Current behaviour is pinned by `TestSubclassOnlyFieldsDownstream` in
`tests/test_core/function_pod/test_output_conformance.py`.
