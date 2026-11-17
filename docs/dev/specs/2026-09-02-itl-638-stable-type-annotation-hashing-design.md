# Stable Type Annotation Hashing Design (ITL-638)

## Overview

Function pod signature hashing currently serializes type objects using their fully-qualified
import path (e.g. `"type:orcapod.logical_types.file_type.File"`). Any internal reorganization
of orcapod's own type modules — even a pure move with no semantic change — silently invalidates
all cached function pod signatures that use those types. This design replaces path-based type
identity with stable canonical names drawn from `LogicalTypeRegistry`.

## Goals & Success Criteria

- Type annotations in function pod signatures are serialized using the type's `logical_type_name`
  from `LogicalTypeRegistry` (e.g. `"orcapod.file"`) rather than its `__module__.__qualname__`.
- Moving a type between internal modules (e.g. `extension_types` → `logical_types`) does **not**
  change the hash of any function pod signature, provided the type's `logical_type_name` is
  unchanged.
- Parameter annotations are canonicalized at extraction time via `canonical_annotation_str`;
  return annotations are stored as raw type objects and canonicalized by `TypeObjectHandler`
  at hash time.  The end result is stable across module relocations in both positions.
- Builtin types (`int`, `str`, `list[int]`, etc.) and user types not registered in the logical
  type registry are unaffected — their hashes remain identical to the pre-fix values.
- A pre-fix golden-value fixture captures current hash values. Post-fix tests assert that only
  orcapod logical types changed and everything else is stable.
- One-time cache invalidation for any function pod signature that contained `op.File`,
  `op.Directory`, `op.Path`, or `op.UUID` is accepted (pre-v0.1.0 project).

## Scope & Boundaries

In scope:

- `TypeObjectHandler` — resolves bare type objects to canonical names via registry.
- `FunctionSignatureExtractor` — parameter annotation substrings are replaced with canonical
  forms via `canonical_annotation_str` at extraction time.  Return annotations remain as raw
  type objects (`parts["returns"] = sig.return_annotation`); `TypeObjectHandler` (configured
  with the same `type_converter`) canonicalises them at hash time.  This avoids special-casing
  in the extractor and keeps the `"type:"` prefix consistent for all return types.
- `canonical_annotation_str` in `hash_utils.py` — extended with an optional `LogicalTypeRegistry`
  parameter and generic-alias recursion.
- `register_builtin_python_type_handlers` — threads `logical_type_registry` through to both
  handlers.
- Golden-value test fixture and diff-assertion regression tests.

Out of scope:

- Broader redesign of the typing system or `LogicalTypeRegistry`.
- Renaming public `op.*` names — those *should* invalidate caches.
- Other serialization surfaces (persistence, wire format, logging keys) — swept and noted but
  not changed here.
- Backward-compatibility shims or grace-period hash migration (pre-v0.1.0 policy).

## Architecture

### Canonical name source of truth

`LogicalTypeRegistry` (in `src/orcapod/logical_types/registry.py`) already maintains a
three-way binding: `(logical_type_name, arrow_extension_name, python_type) → LogicalType`.
Each registered type has a stable `logical_type_name` such as `"orcapod.file"`,
`"orcapod.directory"`, `"orcapod.path"`, `"orcapod.uuid"`. This name is already used as the
stable Arrow extension identifier and is decoupled from the Python module path. It is the
natural source of truth for hashing identity.

For types not in the registry (builtins, user types), the existing fallback
`"type:{module}.{qualname}"` is preserved unchanged.

### Registry access pattern

Both `TypeObjectHandler` and `FunctionSignatureExtractor` accept an optional
`logical_type_registry` constructor argument. When `None`, they resolve the default context's
registry lazily at call time via `get_default_context().logical_type_registry`. This is
identical to the pattern already used by `ArrowTableHandler` and avoids construction-time
circular dependencies.

```
register_builtin_python_type_handlers(registry, ..., logical_type_registry=lt_registry)
    ├── TypeObjectHandler(logical_type_registry=lt_registry)
    └── FunctionSignatureExtractor(logical_type_registry=lt_registry)
            └── canonical_annotation_str(annotation, lt_registry)
```

### `canonical_annotation_str` extension

The existing function in `hash_utils.py` only sorts union members for order-independence. It
is extended (backward-compatibly — new optional parameter defaults to `None`) to:

1. **Registered bare type** (`isinstance(annotation, type)` and found in registry) → return
   `lt.logical_type_name` (e.g. `"orcapod.file"`).
2. **Union** (`X | Y`, `Optional[X]`) → recurse each member with registry, sort, join with
   `" | "`. Existing behaviour preserved; nested orcapod types now also canonicalized.
3. **Generic alias** (`list[X]`, `dict[K, V]`) → recurse `__origin__` and each `__args__`
   member with registry, reconstruct `"origin[arg1, arg2]"` string.
4. **Anything else** → `inspect.formatannotation(annotation)` fallback (unchanged).

When `registry=None`, the function behaves identically to the pre-fix version.

### `TypeObjectHandler` change

```python
# Before
return f"type:{module}.{qualname}"

# After
lt = registry.get_by_python_type(obj)
if lt is not None:
    return f"type:{lt.logical_type_name}"   # e.g. "type:orcapod.file"
return f"type:{module}.{qualname}"          # unchanged fallback
```

### `FunctionSignatureExtractor` change

**Parameter annotations** — the existing union-only post-processing is generalized:

```python
# Before: only union types were post-processed
if is_union_annotation(annotation):
    old_ann = inspect.formatannotation(annotation)
    new_ann = canonical_annotation_str(annotation)
    param_str = param_str.replace(f": {old_ann}", f": {new_ann}", 1)

# After: all annotations are post-processed (no-op when old_ann == new_ann)
old_ann = inspect.formatannotation(annotation)
new_ann = canonical_annotation_str(annotation, registry)
if old_ann != new_ann:
    param_str = param_str.replace(f": {old_ann}", f": {new_ann}", 1)
```

**Return annotation** — stored as the raw type object, unchanged from before:

```python
parts["returns"] = sig.return_annotation   # raw type object (same as before)
```

`TypeObjectHandler` (configured with the same `type_converter`) canonicalises it at hash time
via `type_converter.get_logical_type(obj)`, producing `"type:orcapod.file"` instead of
`"type:orcapod.logical_types.file_type.File"`.  The `"type:"` prefix is therefore present
consistently for all return types, registered or not — there is no special-casing in the
extractor.

**Concrete example.** For `def fn(f: op.File, n: int) -> op.Directory`, `extract_function_info`
currently produces:

```python
# Before
{
    "module": "mymodule",
    "name": "fn",
    "params": "f: orcapod.logical_types.file_type.File, n: int",  # full module path baked in
    "returns": <class 'orcapod.logical_types.file_type.File'>,    # raw type object
}
```

After the fix:

```python
# After
{
    "module": "mymodule",
    "name": "fn",
    "params": "f: orcapod.file, n: int",                            # canonical name; int unchanged
    "returns": <class 'orcapod.logical_types.directory_type.Directory'>,  # still a raw type object
}
# TypeObjectHandler then hashes parts["returns"] to "type:orcapod.directory"
```

## Affected Hash Values

| Annotation form | Before fix | After fix |
|---|---|---|
| `int`, `str`, `float`, `bytes` | `type:builtins.int` etc. | **unchanged** |
| `list[int]`, `dict[str, int]` | `list[int]` etc. | **unchanged** |
| `int \| str` | `int \| str` (sorted) | **unchanged** |
| `op.File` | `type:orcapod.logical_types.file_type.File` | `type:orcapod.file` |
| `op.Directory` | `type:orcapod.logical_types.directory_type.Directory` | `type:orcapod.directory` |
| `op.Path` | `type:pathlib.Path` | `type:orcapod.path` |
| `op.UUID` | `type:uuid.UUID` | `type:orcapod.uuid` |
| `list[op.File]` | `list[orcapod.logical_types.file_type.File]` | `list[orcapod.file]` |
| `op.File \| None` | `NoneType \| orcapod.logical_types.file_type.File` | `NoneType \| orcapod.file` |

Any function pod signature containing any of the "After fix" rows will produce a different
hash, triggering a one-time recompute on next run. This is the intended and accepted outcome.

## Testing Strategy

**Pre-fix golden fixture** (`tests/test_hashing/hash_samples/type_annotation_golden.json`):
Generated by a standalone script before any code change. Committed as an immutable record of
the broken state. Captures annotation hashes, `FunctionSignatureExtractor` output hashes, and
full `hash_object(func)` hashes for the full matrix of annotation forms above.

**`TestGoldenStability`**: After the fix, every builtin and non-orcapod annotation must hash
identically to the golden. Any deviation is an unintended regression.

**`TestGoldenCanonical`**: After the fix, every orcapod logical type annotation must hash
*differently* from the golden. If any still matches, the fix did not apply correctly.

**Unit tests per component**:
- `TestCanonicalAnnotationStrWithRegistry` — covers all four branches of the extended function
- `TestTypeObjectHandlerWithRegistry` — includes a module-relocation simulation test (patches
  `op.File.__module__` at runtime and verifies hash stability)
- `TestFunctionSignatureExtractorWithRegistry` — verifies canonical form for params and returns,
  asserts consistency between the two paths, includes relocation simulation

## Dependencies & Risks

- **Circular imports**: `TypeObjectHandler` and `FunctionSignatureExtractor` are in
  `orcapod.hashing.*`, which must not import from `orcapod.contexts` at module load time.
  Mitigated by the lazy fallback pattern (deferred import inside `_get_registry()`).
- **Registry not yet populated at hash time**: In tests that construct a fresh hasher without
  the default context, the lazy fallback will return a fresh empty registry and registered types
  will fall back to module-path form. Tests that need canonical names must inject the registry
  explicitly. This is correct behavior and is tested.
- **`FunctionInfoExtractorProtocol`**: The protocol in `hashing_protocols.py` defines
  `extract_function_info(func, ...) -> dict`. The `"returns"` value remains a raw type
  object (or union/generic alias) for all annotations — only `"params"` changes for
  functions with orcapod type annotations (the annotation substring is replaced with the
  canonical name).  `TypeObjectHandler` canonicalises `"returns"` at hash time.
