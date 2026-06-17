# PLT-1720: register_python_class storage-type cleanup + registration completeness fix

**Date:** 2026-06-17
**Issue:** PLT-1720 — Cleanup: register_python_class should return plain storage type, not extension type
**Branch:** `eywalker/plt-1720-cleanup-register_python_class-should-return-plain-storage`

---

## Problem

`register_python_class(annotation)` currently returns a `pa.ExtensionType` for annotations
that have a registered logical type. Callers that build Arrow struct fields must immediately
strip that extension type back to plain storage via `_strip_ext_to_storage`, because Arrow and
Polars cannot construct arrays with `pa.ExtensionType` nodes inside struct fields (ET1 in
`DESIGN_ISSUES.md`).

This creates an API impedance mismatch: the return value of `register_python_class` cannot
be used where struct fields are needed, which is its primary call site.

A second, related problem: `DataclassLogicalTypeFactory.reconstruct_from_arrow` (the Parquet
read path) does not call `converter.register_python_class` for its field annotations. This
means that in a fresh process, a nested dataclass (e.g. `Inner` inside `Outer`) is never
registered when reading `Outer` from Parquet. Value conversion for `Inner` then fails with
`ValueError("Unsupported Python type: Inner.")`.

---

## Design invariant

**Registration completeness**: when a logical type is registered by any path, all nested
logical types it depends on must also be registered as a consequence. This is a contract on
`LogicalTypeFactoryProtocol`: both `create_for_python_type` and `reconstruct_from_arrow`
must leave the converter in a state where every logical type the returned `LogicalTypeProtocol`
depends on is also registered before the method returns.

How a factory satisfies this invariant is an implementation detail and is not prescribed here.
A future factory could, for example, embed enough information in its Arrow extension metadata
to reconstruct and register all inner types directly from the metadata, without ever importing
the Python class. That would be equally valid.

For `DataclassLogicalTypeFactory` specifically, the current implementation satisfies the
invariant by calling `converter.register_python_class(annotation)` for each field annotation
in both `create_for_python_type` (which already did this to build struct fields) and the
newly updated `reconstruct_from_arrow` (which discards the return value and uses only the
registration side effect). This is the natural choice because the dataclass field annotations
are available via `typing.get_type_hints` and `register_python_class` already handles
recursive registration correctly.

---

## Contract changes

| Function | Before | After |
|---|---|---|
| `register_python_class(annotation)` | Returns `pa.ExtensionType` for registered classes | Returns plain `pa.DataType` (storage type) for all annotations |
| `register_storage_type(arrow_type)` | Returns resolved `pa.DataType` | Returns `None` (side-effect registration only) |
| `reconstruct_from_arrow(...)` | Does not register nested types | Must ensure all nested types are registered before returning (mechanism is factory-specific) |

`python_type_to_arrow_type(annotation)` is **unchanged** — it still returns `pa.ExtensionType`
for registered classes, used for top-level column schema via `python_schema_to_arrow_schema`.

---

## Changes

### 1. `extension_types/protocols.py` — TypeConverterProtocol

- `register_python_class`: update docstring — "return its plain Arrow storage type"
- `register_storage_type`: change return type annotation from `pa.DataType` to `None`;
  update docstring — "traverse an Arrow type bottom-up, registering extension types;
  return value is None (side-effect only)"

### 2. `semantic_types/universal_converter.py` — UniversalTypeConverter

**`_register_python_class_impl`**: two return sites change from extension type to storage type:

```python
# Registry hit
lt = self._logical_type_registry.get_by_python_type(annotation)
if lt is not None:
    return lt.get_arrow_extension_type().storage_type   # was .get_arrow_extension_type()

# After factory dispatch
lt = factory.create_for_python_type(annotation, converter=self)
self._logical_type_registry.register_logical_type(lt)
return lt.get_arrow_extension_type().storage_type       # was .get_arrow_extension_type()
```

All recursive calls within `_register_python_class_impl` (`list[T]`, `set[T]`, `dict[K,V]`,
`Optional[T]`) naturally propagate storage types because they recurse through
`self.register_python_class(...)`. For example:
- `list[UUID]` → `pa.large_list(pa.large_binary())`
- `dict[str, UUID]` → `pa.large_list(pa.struct([key: large_string, value: large_binary]))`
- `Optional[UUID]` → `pa.large_binary()`

`_convert_python_to_arrow` (used by `python_type_to_arrow_type`) is not touched.

**`register_storage_type`**: simplified from "traverse + rebuild" to "traverse + register only".
No longer rebuilds struct or list types (storage types are always plain after this change):

```python
def register_storage_type(self, arrow_type: "pa.DataType") -> None:
    if isinstance(arrow_type, pa.ExtensionType):
        ext_name = arrow_type.extension_name
        if self._logical_type_registry is not None:
            if self._logical_type_registry.get_by_arrow_extension_name(ext_name) is not None:
                return  # already registered
        self.register_storage_type(arrow_type.storage_type)   # bottom-up first
        raw_meta = arrow_type.__arrow_ext_serialize__()
        self.register_arrow_extension(ext_name, raw_meta or None, arrow_type.storage_type)
        return
    if pa.types.is_struct(arrow_type):
        for i in range(arrow_type.num_fields):
            self.register_storage_type(arrow_type.field(i).type)
        return
    if pa.types.is_large_list(arrow_type) or pa.types.is_list(arrow_type):
        self.register_storage_type(arrow_type.value_field.type)
        return
    # primitives: nothing to do
```

### 3. `extension_types/dataclass_logical_type_factory.py`

**`_strip_ext_to_storage`**: deleted entirely (private, not exported, no longer called).

**`create_for_python_type`**: remove the `_strip_ext_to_storage` call; use `arrow_type` directly:

```python
arrow_type = converter.register_python_class(annotation)
# stripped_type = _strip_ext_to_storage(arrow_type)  ← removed
arrow_fields.append(pa.field(field.name, arrow_type))   # arrow_type is already plain
```

**`reconstruct_from_arrow`** (`DataclassLogicalTypeFactory` implementation): satisfies the
registration completeness invariant by calling `converter.register_python_class(annotation)`
for each field annotation — the same mechanism the write path already uses. The return value
is discarded; only the registration side effect is needed here. This is the implementation
choice for the dataclass factory; other factories may satisfy the invariant differently.

```python
for field in dataclasses.fields(cls):
    if not field.init:
        continue
    annotation = hints.get(field.name, Any)
    converter.register_python_class(annotation)        # ← NEW: registers nested types
    field_annotations.append((field.name, annotation))
```

Trigger chain on read path (for the dataclass factory):
```
register_discovered_extensions
  → converter.register_arrow_extension("mymod.Outer", ...)
      → DataclassLogicalTypeFactory.reconstruct_from_arrow(...)
          → converter.register_python_class(Inner)     ← registers Inner
              → DataclassLogicalTypeFactory.create_for_python_type(Inner, ...)
```

### 4. `extension_types/database_hooks.py`

Drop the now-unused return value of `register_storage_type`; pass `info.storage_type`
directly to `register_arrow_extension` (it is always plain after this change):

```python
converter.register_storage_type(info.storage_type)   # side effects only
converter.register_arrow_extension(
    info.extension_name,
    info.extension_metadata,
    info.storage_type,                               # was: resolved_storage
)
```

### 5. `DESIGN_ISSUES.md`

Check whether the nested-dataclass read-path breakage is logged. If so, mark it resolved;
if not, it was an untracked bug — no new entry needed since the fix is delivered here.

---

## Test changes

### `tests/test_semantic_types/test_universal_converter.py`

**`register_python_class` tests** (4 updates): tests that assert `isinstance(result, pa.ExtensionType)` or check extension names are updated to assert the plain storage type instead:

| Test | Old assertion | New assertion |
|---|---|---|
| `test_register_python_class_registry_hit_path` | `isinstance(result, pa.ExtensionType)` | `result == pa.large_string()` (Path storage) |
| `test_register_python_class_uuid_registry_hit` | `isinstance(result, pa.ExtensionType)` | `result == pa.large_binary()` |
| `test_register_python_class_factory_dispatch` | `isinstance(result, pa.ExtensionType)` | storage type of the custom ext; side-effect (registry entry) verified separately |
| `test_register_python_class_factory_dispatch` second call | `result2 == result` | `result2 == result` (still holds — same storage type) |

**`register_storage_type` tests** (7 updates): all return-value assertions replaced with:
- `assert result is None`
- side-effect assertion: type is findable in the registry (for extension type tests)

Tests that currently verify struct/list return type shapes become side-effect-only (the
traversal still happens, just no rebuilt type is returned).

### `tests/test_extension_types/test_dataclass_logical_type_factory.py`

- `test_register_python_class_dispatches_to_dataclass_factory`: update assertion from
  `isinstance(result, pa.ExtensionType)` to checking the plain storage type
- New test `test_reconstruct_from_arrow_registers_nested_types`: creates a two-level
  dataclass hierarchy, calls `reconstruct_from_arrow` for the outer type only, then
  asserts that the inner type is also present in the registry

---

## What does not change

- `python_type_to_arrow_type` — still returns extension type
- `python_schema_to_arrow_schema` — already calls `python_type_to_arrow_type` (correct)
- `register_arrow_extension` — unchanged
- All write-path value conversion (`python_to_storage`, `get_python_to_arrow_converter`)
- All read-path value conversion (`storage_to_python`, `get_arrow_to_python_converter`)
- `DataclassLogicalType` itself
- `apply_extension_types` / `database_hooks.apply_extension_types`
- All existing round-trip tests (behavior is unchanged; they continue to pass)
