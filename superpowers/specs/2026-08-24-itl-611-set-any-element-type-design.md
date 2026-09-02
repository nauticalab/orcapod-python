# ITL-611: Extend `set[T]` Round-Trip to Any Element Type — Design Spec

**Date:** 2026-08-24
**Linear issue:** ITL-611
**PR target:** `main`

---

## Overview

ITL-173 introduced `ListLogicalType` to preserve `list[T]` and `set[T]` semantics through
Arrow/Parquet round-trips when `T` is a registered logical type (an Arrow extension type like
`uuid.UUID`, `pathlib.Path`, or a dataclass). That spec explicitly scoped out the case where
`T` is a primitive: `set[int]`, `set[str]`, etc. currently fall through to
`pa.large_list(element_type)` with no extension metadata, so the "set" semantics are lost on
read-back (returns `list` instead of `set`).

This spec extends `set[T]` round-trip support to **any hashable element type**, including
primitive scalars, by adding a "native element" construction mode to `ListLogicalType`.

---

## Problem Statement

```python
converter = create_registry().get_context().type_converter

# set[int] loses set semantics on round-trip
arrow_type = converter.python_type_to_arrow_type(set[int])
# → pa.large_list(pa.int64())   ← no extension metadata, no "set" marker
# Read back: pa.large_list(int64) → list[int]  ✗ (should be set[int])
```

`list[int]` already round-trips correctly (`pa.large_list(int64)` → `list[int]`) because
the list semantics are preserved by the Arrow type itself. `set` has no Arrow-native
representation, so it requires an extension type marker.

---

## Design

### Principle of separation

Two concerns are kept strictly separate:

1. **`ListLogicalType` constructor** — dumb, always wraps. If called with a primitive Python
   type as element, it unconditionally creates an extension-wrapped set/list. It has no
   opinion about whether the caller *should* be wrapping.

2. **`UniversalTypeConverter` inference path** — the policy layer. Decides when to call
   `ListLogicalType` vs returning a plain `pa.large_list`. The rule:
   - `list[T]` for primitive `T` → `pa.large_list(T)` (unchanged — round-trips correctly as-is)
   - `list[T]` for extension `T` → `ListLogicalType(element_lt, is_set=False)` (unchanged)
   - `set[T]` for primitive `T` → `ListLogicalType(T, is_set=True)` (NEW)
   - `set[T]` for extension `T` → `ListLogicalType(element_lt, is_set=True)` (unchanged)

### Unified `ListLogicalType` constructor

The constructor is extended to accept either a `LogicalTypeProtocol` or a plain Python type:

```python
def __init__(
    self,
    element: "LogicalTypeProtocol | type",
    *,
    is_set: bool = False,
) -> None:
```

**Extension element mode** (existing, triggered when `element` has `get_arrow_extension_type`):
- Derives `element_ext_name` and `element_ext_metadata` from `element.get_arrow_extension_type()`
- Metadata: `{"category": "set"|"list", "element_ext_name": "orcapod.uuid", "element_ext_metadata": "..."}`
- Extension name: `set[orcapod.uuid]`, `list[orcapod.uuid]`, etc.

**Native element mode** (new, triggered when `element` is a plain Python type):
- Looks up the Arrow storage type from `_get_python_to_arrow_map()`
- Raises `ValueError` if the type is not in the map
- Metadata: `{"category": "set"|"list", "element_kind": "native", "element_python_type": "int"}`
- Extension name: `set[int]`, `list[int]`, etc.
- `python_type` property returns `set[int]` or `list[int]`

Backward compatibility: existing metadata without `element_kind` is treated as extension mode.

### Native type map

A module-level dict in `list_logical_type_factory.py` (separate from, but drawn from, the
existing `_get_python_to_arrow_map()` in `universal_converter.py`):

```python
_NATIVE_ELEMENT_TYPES: dict[str, type] = {
    "int":      int,
    "str":      str,
    "float":    float,
    "bool":     bool,
    "bytes":    bytes,
    "datetime": datetime,
    "date":     date,
}
```

The Python type name (the dict key) is stored in metadata for reconstruction. On the read
path, `_NATIVE_ELEMENT_TYPES["int"]` → `int`, then `_get_python_to_arrow_map()[int]` →
`pa.int64()`. No Arrow type is stored in metadata — it is always derived from the Python type.

### Metadata format

Extension mode (unchanged):
```json
{
  "category": "set",
  "element_ext_name": "orcapod.uuid",
  "element_ext_metadata": null
}
```

Native mode (new):
```json
{
  "category": "set",
  "element_kind": "native",
  "element_python_type": "int"
}
```

The `element_kind` key is the discriminator. Its absence (old data) means extension mode.

### Read path: `ListLogicalTypeFactory.reconstruct_from_arrow`

Gains a branch for `element_kind == "native"`:

1. Read `element_python_type` name from metadata (e.g. `"int"`)
2. Look up `python_type = _NATIVE_ELEMENT_TYPES[element_python_type_name]`
3. Construct `ListLogicalType(python_type, is_set=(category == "set"))`

The existing branch (no `element_kind` key, or `element_kind == "extension"`) is unchanged.

### `ListLogicalTypeFactory.create_for_python_type`

For `set[T]` where `T` has no `LogicalType` and is in `_NATIVE_ELEMENT_TYPES` (keyed by
`T.__name__`): construct `ListLogicalType(T, is_set=True)`.

For `list[T]` where `T` has no `LogicalType`: raise `ValueError` as before — the inference
path returns plain `pa.large_list` for this case and never calls `create_for_python_type`.

### `UniversalTypeConverter` changes

In both `_register_python_class_impl` and `_convert_python_to_arrow` for `origin is set`:

```python
# BEFORE (falls through to plain large_list for primitives)
if origin is set:
    inner = self.register_python_class(args[0])
    if self._logical_type_registry is not None and hasattr(inner, "extension_name"):
        element_lt = self._logical_type_registry.get_by_arrow_extension_name(inner.extension_name)
        if element_lt is not None:
            return self._make_or_get_list_logical_type(element_lt, is_set=True)
    return pa.large_list(inner)   # ← set semantics lost for primitives

# AFTER
if origin is set:
    inner = self.register_python_class(args[0])
    if self._logical_type_registry is not None and hasattr(inner, "extension_name"):
        element_lt = self._logical_type_registry.get_by_arrow_extension_name(inner.extension_name)
        if element_lt is not None:
            return self._make_or_get_list_logical_type(element_lt, is_set=True)
    # NEW: wrap primitive T in native-mode ListLogicalType to preserve set semantics
    if self._logical_type_registry is not None and args[0] in _get_python_to_arrow_map():
        return self._make_or_get_native_list_logical_type(args[0], is_set=True)
    return pa.large_list(inner)   # fallback (no registry, or unrecognised type)
```

A new helper `_make_or_get_native_list_logical_type(python_type, *, is_set)` mirrors the
existing `_make_or_get_list_logical_type` but constructs `ListLogicalType(python_type, ...)`.

The `origin is list` branch is **unchanged** — `list[int]` continues to produce plain
`pa.large_list(int64)`.

---

## Scope

### In scope

- `set[T]` where `T` is a primitive scalar in `_NATIVE_ELEMENT_TYPES`: `int`, `str`,
  `float`, `bool`, `bytes`, `datetime`, `date`
- `set[T]` where `T` is an existing logical type (UUID, Path, UPath, dataclass, Pydantic,
  NumPy, Pandas) — already works, unchanged
- Direct explicit construction of `ListLogicalType(int, is_set=False)` (extension-wrapped
  `list[int]`) — always functional even though the inference path never generates it
- Full schema round-trip: `python_schema_to_arrow_schema` → store → `load_logical_types` →
  `arrow_schema_to_python_schema` returns `set[int]` (not `list[int]`)
- Parquet and Delta back-ends (same as all other logical types)

### Out of scope

- `list[T]` for primitive `T` — no change; already round-trips correctly as `pa.large_list`
- `set[list[int]]` — `list` is not hashable; Python cannot construct a `set` from list
  elements, so this combination is unsupported by Python itself
- `set[dict[...]]` / `set[set[...]]` — same hashability constraint
- Extending the native type map beyond the 7 primitive types listed above (e.g. numpy scalars)
  — can be added incrementally in follow-up issues

---

## Files Changed

| File | Change |
|------|--------|
| `src/orcapod/logical_types/list_logical_type_factory.py` | Extend `ListLogicalType.__init__` to accept native element; add `_NATIVE_ELEMENT_TYPES`; update `ListLogicalTypeFactory.reconstruct_from_arrow` and `create_for_python_type` |
| `src/orcapod/semantic_types/universal_converter.py` | Update `set[T]` branch in `_register_python_class_impl` and `_convert_python_to_arrow`; add `_make_or_get_native_list_logical_type` helper |
| `tests/test_logical_types/test_roundtrips.py` | Add round-trip tests for `set[int]`, `set[str]`, `set[float]`, `set[bool]`, `set[bytes]`, `set[datetime]`; schema round-trip test |

---

## Test Plan

1. **`set[int]` Parquet + Delta round-trip** — write `{1, 2, 3}` in a `set[int]` column;
   read back with a fresh converter; assert `isinstance(result, set)` and value equality.
2. **`set[str]` round-trip** — same pattern for strings.
3. **`set[float]` round-trip** — includes `NaN` ordering edge case via `repr`-based sort.
4. **`set[bool]` round-trip** — `{True, False}`.
5. **`set[bytes]` round-trip** — byte string elements.
6. **`set[datetime]` round-trip** — timezone-aware datetimes.
7. **Extension name check** — assert `field.type.extension_name == "set[int]"` after read.
8. **Schema round-trip** — `python_schema_to_arrow_schema({"s": set[int]})` → store →
   `arrow_schema_to_python_schema` returns `{"s": set[int]}` (not `set[Any]` or `list[int]`).
9. **`list[int]` unchanged** — assert `converter.python_type_to_arrow_type(list[int])` is
   still `pa.large_list(pa.int64())` (no extension wrapping).
10. **Explicit native list construction** — `ListLogicalType(int, is_set=False)` produces a
    functional extension-wrapped `list[int]` that round-trips correctly.
11. **Fresh converter read** — write with converter A; read with converter B (no prior
    registration); `load_logical_types` correctly reconstructs `set[int]`.
