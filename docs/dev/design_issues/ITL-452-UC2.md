## `src/orcapod/semantic_types/universal_converter.py`

### UC2 — Premature rejection of union-typed function inputs
**Status:** resolved
**Severity:** high
**Issue:** ITL-452

`ensure_types_registered_for_schemas` forwarded each schema annotation directly
to `register_python_class`, which rejects complex union types (``str | Path``)
because Arrow has no native union storage type. This caused ``FunctionPod``
construction to fail with a ``ValueError`` whenever a function declared a
union-typed input argument, even though union inputs are semantically valid
(they express that the pod accepts either concrete type, with the concrete
branch resolved at stream-binding time).

**Fix:** Modified `ensure_types_registered_for_schemas` to detect union
annotations and register each non-``None`` branch individually, leaving
``register_python_class`` unchanged (it correctly rejects unions when invoked
directly for explicit type conversion).
