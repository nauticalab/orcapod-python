## `src/orcapod/hashing/semantic_hashing/`

### H1 — Semantic hasher does not support PEP 604 union types (`int | None`)
**Status:** open
**Severity:** medium

The `BaseSemanticHasher` raises `BeartypeDoorNonpepException` when hashing a
`PythonDataFunction` whose return type uses PEP 604 syntax (`int | None`).
The hasher's `_handle_unknown` path receives `types.UnionType` (the Python 3.10+ type for
`X | Y` expressions) and has no registered handler for it.

`typing.Optional[int]` also fails (different error path through beartype).

This means data functions cannot use union return types — a common pattern for functions
that may filter data by returning `None`.

**Workaround:** Use non-union return types and raise/return sentinel values instead.

**Fix needed:** Register a `TypeHandlerProtocol` for `types.UnionType` (and
`typing.Union`/`typing.Optional`) in the semantic hasher's type handler registry.
