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
| `register_python_class(annotation)` | Returns `pa.ExtensionType` for registered classes; may return extension types nested inside struct/list fields | Returns storage-safe `pa.DataType`: may be extension type at the top level for registered types, but struct/list fields always contain plain (non-extension) types at every depth |
| `register_storage_type(arrow_type)` | Returns `pa.DataType`; may return extension types nested inside struct/list fields | Returns storage-safe `pa.DataType`: may be extension type at the top level, but struct/list fields always contain plain (non-extension) types at every depth |
| `reconstruct_from_arrow(...)` | Does not register nested types | Must ensure all nested types are registered before returning (mechanism is factory-specific) |

`python_type_to_arrow_type(annotation)` is **unchanged** — it still returns `pa.ExtensionType`
for registered classes, used for top-level column schema via `python_schema_to_arrow_schema`.

---

## Changes

### 1. `extension_types/protocols.py` — TypeConverterProtocol

- `register_python_class`: update docstring — "return the storage-safe Arrow type: may be
  extension type at the top level for registered types, but struct/list fields are always plain"
- `register_storage_type`: update docstring — "traverse an Arrow type bottom-up,
  registering any extension types encountered; return a storage-safe ``pa.DataType``
  (may be extension type at the top level, but struct/list fields contain only plain types)"

### 2. `semantic_types/universal_converter.py` — UniversalTypeConverter

**`_register_python_class_impl`**: the two return sites that previously returned the extension
type now return it unchanged (no `.storage_type` strip). The storage-safe guarantee is satisfied
at the top level because `DataclassLogicalType` and other factories always build their struct
storage with plain field types:

```python
# Registry hit — return ext type directly (already storage-safe by factory invariant)
lt = self._logical_type_registry.get_by_python_type(annotation)
if lt is not None:
    return lt.get_arrow_extension_type()   # unchanged from current behaviour

# After factory dispatch — same
lt = factory.create_for_python_type(annotation, converter=self)
self._logical_type_registry.register_logical_type(lt)
return lt.get_arrow_extension_type()       # unchanged from current behaviour
```

The container branches (`list[T]`, `set[T]`, `dict[K,V]`, `Optional[T]`) recurse through
`self.register_python_class(...)` and receive a potentially extension-typed result. They strip
it to `.storage_type` before embedding it in a list value or struct field — a trivial one-liner
that replaces the old recursive `_strip_ext_to_storage` helper:

```python
# list[T] branch (illustrative)
inner = self.register_python_class(inner_type)
if isinstance(inner, pa.ExtensionType):
    inner = inner.storage_type   # strip: cannot nest ext inside list value type
return pa.large_list(inner)
```

End-to-end examples (identical to current spec — stripping in container branches is unchanged):
- `list[UUID]` → `pa.large_list(pa.large_binary())`
- `dict[str, UUID]` → `pa.large_list(pa.struct([key: large_string, value: large_binary]))`
- `Optional[UUID]` → `orcapod.uuid` extension type (same as `UUID` directly; `Optional[T]` is a nullability wrapper that delegates to `register_python_class(T)` unchanged)
- `UUID` directly → `orcapod.uuid` extension type (top-level; storage is `pa.large_binary()`)

`_convert_python_to_arrow` (used by `python_type_to_arrow_type`) is not touched.

**`register_storage_type`**: updated from "traverse + rebuild (may preserve nested extension types)" to "traverse + rebuild with storage-safe guarantee (strip extension types from struct/list fields)":

```python
def register_storage_type(self, arrow_type: "pa.DataType") -> "pa.DataType":
    if isinstance(arrow_type, pa.ExtensionType):
        ext_name = arrow_type.extension_name
        if self._logical_type_registry is not None:
            if self._logical_type_registry.get_by_arrow_extension_name(ext_name) is not None:
                lt = self._logical_type_registry.get_by_arrow_extension_name(ext_name)
                return lt.get_arrow_extension_type()   # already registered, return ext type
        self.register_storage_type(arrow_type.storage_type)   # bottom-up first
        raw_meta = arrow_type.__arrow_ext_serialize__()
        return self.register_arrow_extension(ext_name, raw_meta or None, arrow_type.storage_type)
    if pa.types.is_struct(arrow_type):
        resolved_fields = []
        for i in range(arrow_type.num_fields):
            field = arrow_type.field(i)
            resolved = self.register_storage_type(field.type)
            if isinstance(resolved, pa.ExtensionType):
                resolved = resolved.storage_type   # strip: ET1 forbids ext inside struct fields
            resolved_fields.append(pa.field(field.name, resolved, nullable=field.nullable, metadata=field.metadata))
        return pa.struct(resolved_fields)
    if pa.types.is_large_list(arrow_type) or pa.types.is_list(arrow_type):
        vf = arrow_type.value_field
        resolved = self.register_storage_type(vf.type)
        if isinstance(resolved, pa.ExtensionType):
            resolved = resolved.storage_type   # strip: ET1 forbids ext inside list value type
        return pa.large_list(pa.field(vf.name, resolved, nullable=vf.nullable, metadata=vf.metadata))
    return arrow_type   # primitives: return unchanged
```

The storage-safe guarantee: a top-level extension type may be returned (the caller can use it as a column type), but any struct or list the returned type contains will never have extension type nodes in their fields/value types.

### 3. `extension_types/dataclass_logical_type_factory.py`

**`_strip_ext_to_storage`**: deleted entirely (private, not exported, no longer called).

**`create_for_python_type`**: replace the recursive `_strip_ext_to_storage` call with a trivial
one-liner that strips only the top-level extension type (the storage-safe guarantee from
`register_python_class` ensures `.storage_type` is always clean — no further recursion needed):

```python
arrow_type = converter.register_python_class(annotation)
if isinstance(arrow_type, pa.ExtensionType):
    arrow_type = arrow_type.storage_type   # strip top-level ext for struct field (ET1)
arrow_fields.append(pa.field(field.name, arrow_type))
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

**No change.** `register_storage_type` still returns a meaningful `pa.DataType`, and
`database_hooks.py` already passes that resolved value into `register_arrow_extension`:

```python
resolved_storage = converter.register_storage_type(info.storage_type)
converter.register_arrow_extension(
    info.extension_name,
    info.extension_metadata,
    resolved_storage,
)
```

The only behavioral difference is that `resolved_storage` is now guaranteed to be
storage-safe (no nested extension types in struct/list fields), which is precisely what
`register_arrow_extension` needs.

### 5. `DESIGN_ISSUES.md`

Check whether the nested-dataclass read-path breakage is logged. If so, mark it resolved;
if not, it was an untracked bug — no new entry needed since the fix is delivered here.

---

## Test changes

### `tests/test_semantic_types/test_universal_converter.py`

**`register_python_class` tests** (0 updates): the existing assertions check
`isinstance(result, pa.ExtensionType)` and `result.extension_name == "..."`. Under the new
storage-safe contract `register_python_class` still returns an extension type for registered
classes — these tests are already correct and need no changes.

**`register_storage_type` tests** (1 update): only the test that currently asserts an
extension type is *preserved* inside a struct field needs to change. Under the new
storage-safe contract, that extension type must be stripped to its storage type before
being placed into the rebuilt struct.

All other `register_storage_type` tests — including those that check the returned struct
or list shape — continue to pass with only the assertion on the inner field type updated.

### `tests/test_extension_types/test_dataclass_logical_type_factory.py`

- `test_register_python_class_dispatches_to_dataclass_factory`: **no change** — already
  asserts `isinstance(result, pa.ExtensionType)` and `result.extension_name == "orcapod.uuid"`,
  which is correct under the new storage-safe contract
- New test `test_reconstruct_from_arrow_registers_nested_types`: creates a two-level
  dataclass hierarchy, calls `reconstruct_from_arrow` for the outer type only, then
  asserts that the inner type is also present in the registry
- New test `test_nested_dataclass_parquet_roundtrip`: end-to-end Parquet round-trip for
  a two-level dataclass (`_Inner` nested inside `_Outer`). Write path: build a converter,
  register `_Outer`, write an Arrow table with an `_Outer` instance to a Parquet file.
  Read path: create a **fresh converter** (only built-in types + `DataclassLogicalTypeFactory`,
  neither `_Inner` nor `_Outer` pre-registered), read the Parquet file back, call
  `register_discovered_extensions` on the schema — this should trigger the chain that
  registers `_Outer` which in turn registers `_Inner`. Assert that converting the Arrow
  struct storage back to a Python `_Outer` value produces the original object.

---

## What does not change

- `python_type_to_arrow_type` — still returns extension type
- `python_schema_to_arrow_schema` — already calls `python_type_to_arrow_type` (correct)
- `register_arrow_extension` — unchanged
- `extension_types/database_hooks.py` — unchanged (continues to use `register_storage_type` return value as before)
- All write-path value conversion (`python_to_storage`, `get_python_to_arrow_converter`)
- All read-path value conversion (`storage_to_python`, `get_arrow_to_python_converter`)
- `DataclassLogicalType` itself
- `apply_extension_types` / `database_hooks.apply_extension_types`
- All existing round-trip tests (behavior is unchanged; they continue to pass)
