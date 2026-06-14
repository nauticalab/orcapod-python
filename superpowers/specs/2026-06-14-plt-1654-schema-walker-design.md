# PLT-1654: Recursive Arrow Schema Walker Design

**Date:** 2026-06-14
**Linear issue:** PLT-1654
**Status:** Approved

---

## Overview

Add `src/orcapod/extension_types/schema_walker.py` — a pure discovery utility that
walks an Arrow schema (or a single field) recursively and returns all extension-typed
fields found at any depth of nesting (struct, list, map, etc.).

This is the third piece of the `extension_types/` subpackage, sitting between
`registry.py` (PLT-1653) and the database peek-schema helper (PLT-1655). It produces the
`(extension_name, extension_metadata, storage_type)` information that PLT-1655 feeds into
`ExtensionTypeRegistry` at read time.

**Strictly additive.** No existing code is modified. This aligns with the project-wide
parallel-build strategy: old semantic type code is untouched until PLT-1660 (the hard cut).

---

## Goals & Success Criteria

- `walk_schema(schema)` returns all extension types found in a `pa.Schema` at any depth,
  deduplicated by `(extension_name, extension_metadata)`.
- `walk_field(field)` does the same for a single `pa.Field`.
- Both channels are handled: registered types (`pa.types.is_extension`) and unregistered
  types (raw `ARROW:extension:name` field metadata after a Parquet/IPC round-trip).
- All container nesting cases work: top-level column, list value, struct field, map
  key/value, and arbitrary combinations thereof.
- Empty bytes `b""` from `__arrow_ext_serialize__()` is normalised to `None` so callers
  never see an empty-bytes sentinel.
- No registration triggered — purely inspection.
- Works on `DeltaTable.schema().to_arrow()` output.

---

## Scope & Boundaries

In scope:
- New `src/orcapod/extension_types/schema_walker.py`
- Additive exports in `src/orcapod/extension_types/__init__.py`
- New `tests/test_extension_types/test_schema_walker.py`

Out of scope:
- Database read path changes (PLT-1655)
- Built-in converter registrations (PLT-1656)
- Any modification to existing `semantic_types/` code
- Thread safety (registration is import-time, before concurrent I/O)

---

## Architecture

### File map

| File | Change |
|---|---|
| `src/orcapod/extension_types/schema_walker.py` | **New** |
| `src/orcapod/extension_types/__init__.py` | Additive — new exports appended |
| `tests/test_extension_types/test_schema_walker.py` | **New** |

No other files are touched.

---

## `schema_walker.py`

### `ExtensionTypeInfo` data container

```python
@dataclasses.dataclass(frozen=True)
class ExtensionTypeInfo:
    extension_name: str
    extension_metadata: bytes | None
    storage_type: pa.DataType
```

A frozen dataclass (not a NamedTuple): immutable, hashable, attribute access only.
`b""` is normalised to `None` at construction time — no caller ever sees an
empty-bytes metadata value.

### Public API

```python
def walk_schema(schema: pa.Schema) -> list[ExtensionTypeInfo]: ...
def walk_field(field: pa.Field) -> list[ExtensionTypeInfo]: ...
```

Both return a deduplicated list in depth-first, first-seen order. The deduplication key
is `(extension_name, extension_metadata)`. When the same pair appears in multiple
columns, only the first occurrence (and its `storage_type`) is kept.

### Internal helpers

**`_collect(field, seen, results)`** — the recursive core. Mutates `seen` (a
`set[tuple[str, bytes | None]]`) and `results` (a `list[ExtensionTypeInfo]`) in place:

1. Call `_detect_extension(field)`. If it returns an `ExtensionTypeInfo`:
   - Add to `results` if `(extension_name, extension_metadata)` is not in `seen`.
   - Update `seen`.
   - **Return immediately** — do not descend into the storage type.
2. Otherwise inspect `field.type` and recurse:
   - `is_struct` → `t.field(i)` for each `i` in `range(t.num_fields)`
   - `is_list` / `is_large_list` / `is_fixed_size_list` / `is_list_view` /
     `is_large_list_view` → `t.value_field`
   - `is_map` → `t.key_field` and `t.item_field` (via `getattr`; available in
     PyArrow ≥ 14, project requires ≥ 20)
   - Primitives and unrecognised types → no-op

**`_detect_extension(field) -> ExtensionTypeInfo | None`** — detects whether a field
carries extension type information via either channel:

**Channel 1 — Registered** (`pa.types.is_extension(field.type)` is True):

The extension type is registered in this process; the type object carries everything:

```python
ext_type = field.type
raw_meta = ext_type.__arrow_ext_serialize__()
return ExtensionTypeInfo(
    extension_name=ext_type.extension_name,
    extension_metadata=raw_meta or None,
    storage_type=ext_type.storage_type,
)
```

**Channel 2 — Unregistered** (`field.metadata` contains `b"ARROW:extension:name"`):

The type was registered elsewhere and survived a Parquet/IPC round-trip. The raw
`field.type` is the storage type; name and metadata are in the field's Arrow metadata:

```python
name = field.metadata[b"ARROW:extension:name"].decode("utf-8")
raw_meta = field.metadata.get(b"ARROW:extension:metadata")
return ExtensionTypeInfo(
    extension_name=name,
    extension_metadata=raw_meta or None,
    storage_type=field.type,
)
```

Channel 1 is checked first. `None` is returned if neither applies.

---

## `__init__.py` additions

```python
from .schema_walker import ExtensionTypeInfo, walk_field, walk_schema

__all__ = [
    "ExtensionTypeConverter",
    "ExtensionTypeRegistry",
    "default_extension_type_registry",
    # PLT-1654
    "ExtensionTypeInfo",
    "walk_schema",
    "walk_field",
]
```

---

## Tests — `tests/test_extension_types/test_schema_walker.py`

Uses the same `_make_stub` / `_unique_name` helper pattern from `test_registry.py`.
Registered-channel tests use `ExtensionTypeRegistry` to put types into PyArrow's global
registry. Unregistered-channel tests construct `pa.Field` objects with explicit
`metadata={b"ARROW:extension:name": ..., b"ARROW:extension:metadata": ...}`.

| Test | What it covers |
|---|---|
| `test_empty_schema` | Empty schema → `[]` |
| `test_no_extension_types` | Schema with only primitives → `[]` |
| `test_top_level_registered` | Registered ext type as top-level column |
| `test_top_level_unregistered` | Unregistered ext type via raw field metadata |
| `test_list_of_registered` | Registered ext type as list value field |
| `test_list_of_unregistered` | Unregistered ext type as list value field |
| `test_struct_containing_registered` | Registered ext type inside a struct field |
| `test_struct_containing_unregistered` | Unregistered ext type inside a struct field |
| `test_nested_list_struct` | `list<struct<x: ext<…>>>` — arbitrary nesting |
| `test_deduplication` | Same `(name, metadata)` in two columns → one result |
| `test_empty_metadata_normalised_to_none` | `b""` from `__arrow_ext_serialize__` → `None` |
| `test_walk_field` | `walk_field` on a single field returns correct result |
| `test_map_type` | Extension type as map item value |

---

## PLT-1660 cleanup items (deferred)

- Remove `SemanticTypeRegistry.find_semantic_fields_in_schema` (shape-based — replaced by
  `walk_schema`).
- Remove `SemanticTypeRegistry.get_semantic_field_info` (shape-based — same fate).
