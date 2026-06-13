# PLT-1652: ExtensionTypeConverter Protocol Design

**Issue:** PLT-1652  
**Date:** 2026-06-13  
**Project:** Orcapod: Arrow/Polars Extension Type Semantic Type System

---

## Overview

Define `ExtensionTypeConverter` — the new protocol contract for all Arrow/Polars extension-type-backed converters in Orcapod.

This is the foundation of the new extension type system. It replaces `SemanticStructConverterProtocol` as the converter interface, but under the project's parallel-build strategy, it is added as new code without touching the existing protocol or any other existing file.

---

## Parallel-build strategy (applies to the full project)

All build-phase issues (PLT-1652 through PLT-1659) add **new code only**. The old semantic type system (`SemanticStructConverterProtocol`, `SemanticStructConverterBase`, `SemanticTypeRegistry`, `dataclass_encoding.py`, `semantic_struct_converters.py`) is completely untouched until PLT-1660 (the hard cut).

The new system lives in a new `src/orcapod/extension_types/` subpackage:

```
src/orcapod/extension_types/
├── __init__.py                  (import-time registration — PLT-1656)
├── protocols.py                 (ExtensionTypeConverter — PLT-1652, this issue)
├── registry.py                  (ExtensionTypeRegistry — PLT-1653)
├── schema_walker.py             (recursive extension type discovery — PLT-1654)
├── builtin_converters.py        (Path, UPath, UUID converters — PLT-1656)
├── dataclass_handler.py         (orcapod.dataclass category handler — PLT-1657)
└── picklable_handler.py         (orcapod.picklable category handler — PLT-1658)
```

Each build-phase issue tracks a "PLT-1660 cleanup items" list. PLT-1660 is the only issue that deletes old system code.

---

## ExtensionTypeConverter protocol

**File:** `src/orcapod/extension_types/protocols.py`

### Members

| Member | Type | Purpose |
|---|---|---|
| `extension_name` | `str` | Fully-qualified Python class name; stored as `ARROW:extension:name` |
| `extension_metadata` | `bytes \| None` | Category tag (e.g. `"orcapod.dataclass"`); stored as `ARROW:extension:metadata` |
| `storage_type` | `pa.DataType` | Underlying Arrow storage type (any Arrow type — not constrained to struct) |
| `python_type` | `type` | The Python class this converter handles |
| `python_to_storage(value)` | `Any` | Convert Python value to Arrow storage scalar/array |
| `storage_to_python(storage_value)` | `Any` | Convert Arrow storage scalar/array back to Python |

### What is explicitly excluded

The following members of `SemanticStructConverterProtocol` have no place in the new protocol:

| Removed member | Reason |
|---|---|
| `hash_struct_dict(struct_dict)` | Hashing is exclusively `PythonTypeHandlerRegistry` / `SemanticAwarePythonHasher` |
| `hasher_id` | Same — hashing responsibility removed |
| `can_handle_python_type(python_type)` | Identity is now explicit via `extension_name`, not dynamic dispatch |
| `can_handle_struct_type(struct_type)` | Same |
| `arrow_struct_type` | Replaced by `storage_type` (more general — any Arrow type, not just struct) |

### Design notes

- `extension_name` is by convention the fully-qualified class name but not enforced — user-defined converters may choose any unique string
- `storage_type` is any `pa.DataType`, enabling scalar-like types (Path → `pa.large_string()`, UUID → `pa.binary(16)`) without a struct wrapper
- `extension_metadata` is the category tag the registry uses at read time to find the right factory handler
- Method names are `python_to_storage` / `storage_to_python` (not `to_arrow` / `from_arrow`)
- Name `ExtensionTypeConverter` not `SemanticTypeConverter` — "Semantic" is reserved for the hashing layer

---

## Identity model comparison

| | Old (shape-based) | New (extension-name-based) |
|---|---|---|
| Identity key | Arrow struct shape | `extension_name` string |
| Storage constraint | Must be struct | Any Arrow type |
| Hashing | In the converter | In `PythonTypeHandlerRegistry` only |
| Type detection | `can_handle_python_type` / `can_handle_struct_type` | Explicit `extension_name` lookup |
| Survives Parquet round-trip | No (struct shape is stable, but collisions possible) | Yes (`ARROW:extension:name` metadata preserved) |

---

## PLT-1660 cleanup items (deferred)

- Remove `SemanticStructConverterProtocol` entirely from `protocols/semantic_types_protocols.py`
