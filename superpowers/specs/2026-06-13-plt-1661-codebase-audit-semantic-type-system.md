# PLT-1661: Codebase Audit — Arrow/Polars Extension Type Semantic Type System

**Date:** 2026-06-13
**Issue:** [PLT-1661](https://linear.app/enigma-metamorphic/issue/PLT-1661/codebase-audit-and-project-plan-adjustment)
**Scope:** Audit of `src/orcapod/semantic_types/`, `src/orcapod/protocols/semantic_types_protocols.py`, `src/orcapod/databases/`, and `tests/test_semantic_types/` against the project plan in PLT-1652 through PLT-1660.

---

## 1. Current Codebase State

### 1.1 `SemanticStructConverterProtocol` (the current converter contract)

File: `src/orcapod/protocols/semantic_types_protocols.py`

The current protocol is named **`SemanticStructConverterProtocol`** (not `SemanticTypeConverter`).
Its interface:

| Member | Type | Description |
|---|---|---|
| `python_type` | `DataType` | Python type this converter handles |
| `arrow_struct_type` | `pa.StructType` | Arrow struct type this converter produces |
| `python_to_struct_dict(value)` | method | Python value → struct dict |
| `struct_dict_to_python(struct_dict)` | method | Struct dict → Python value |
| `can_handle_python_type(python_type)` | method | Shape-based type check |
| `can_handle_struct_type(struct_type)` | method | Shape-based struct check |
| `hash_struct_dict(struct_dict)` | method | Content hash from struct dict |
| `hasher_id` | `str` | Identifier for this hasher |

There is **no** `extension_name`, `extension_metadata`, or storage-type concept.

There is also a separate `TypeConverterProtocol` (the `UniversalTypeConverter` contract) in the same file — that is distinct and not being changed by this project.

### 1.2 `SemanticTypeRegistry` (`semantic_types/semantic_registry.py`)

Internal data structures — all keyed by struct shape:

- `_python_to_struct: dict[DataType, pa.StructType]`
- `_struct_to_python: dict[pa.StructType, DataType]`
- `_struct_to_converter: dict[pa.StructType, SemanticStructConverterProtocol]`
- `_name_to_converter: dict[str, SemanticStructConverterProtocol]`
- `_struct_to_name: dict[pa.StructType, str]`

Key method: `find_semantic_fields_in_schema(schema)` — iterates schema fields, checks `field.type in self._struct_to_name` (pure struct shape match). No PyArrow or Polars extension type registration anywhere.

### 1.3 Built-in converters (actual storage representations)

File: `semantic_types/semantic_struct_converters.py`

| Converter | Python type | Current Arrow storage |
|---|---|---|
| `PythonPathStructConverter` | `pathlib.Path` | `struct{path: pa.large_string()}` |
| `UPathStructConverter` | `upath.UPath` | `struct{upath: pa.large_string()}` |
| `UUIDStructConverter` | `uuid.UUID` | `struct{uuid: pa.binary(16)}` |

All three use **struct wrappers** — none use a primitive storage type directly.

Note: PLT-1656 describes the old Path storage as `struct{path: str}`. The actual type is `struct{path: pa.large_string()}`. `pa.large_string()` is used throughout for Polars compatibility. The new scalar storage should use `pa.large_string()` (not `pa.string()`) for `Path`/`UPath`, and `pa.binary(16)` or `pa.large_binary()` for `UUID` (see PLT-1656 note below).

### 1.4 `UniversalTypeConverter` (`semantic_types/universal_converter.py`)

Struct dispatch in `_convert_arrow_to_python`:

```python
elif pa.types.is_struct(arrow_type):
    # Check semantic registry first
    if self.semantic_registry:
        python_type = self.semantic_registry.get_python_type_for_semantic_struct_signature(arrow_type)
        if python_type:
            return python_type
    # ... dataclass sentinel check, TypedDict creation ...
```

And in `_create_arrow_to_python_converter`:

```python
if self.semantic_registry and pa.types.is_struct(arrow_type):
    registered_python_type = self.semantic_registry.get_python_type_for_semantic_struct_signature(arrow_type)
    if registered_python_type:
        converter = self.semantic_registry.get_converter_for_python_type(registered_python_type)
        return converter.struct_dict_to_python
```

After PLT-1652/1653, semantic types will be Arrow extension types (not plain structs). The converter needs to dispatch on `pa.types.is_extension(arrow_type)` and look up by extension name instead of struct shape. **This is not covered by any existing issue.**

### 1.5 Dataclass encoding (`semantic_types/dataclass_encoding.py`)

A separate, pre-existing dataclass serialization system that stores dataclass FQCNs in a `__dataclass.` sentinel field inside an Arrow struct:

- `DATACLASS_TYPE_FIELD = "__dataclass."` — the sentinel field name
- `DATACLASS_TYPE_PREFIX = "dataclass:"` — value prefix in the sentinel
- `register_dataclass(cls)` — process-global registry for tier-2 reconstruction
- `_DATACLASS_REGISTRY: dict[str, type]` — the global registry dict
- 3-tier reconstruction: import → registry → synthesize

**This entire module is superseded by the new extension type system.** PLT-1657 replaces it completely. The sentinel field, `DATACLASS_TYPE_PREFIX`, `register_dataclass()`, `_DATACLASS_REGISTRY`, and all reconstruction logic will be removed. The FQCN will instead be stored as `ARROW:extension:name`; the `orcapod.dataclass` category tag goes into `ARROW:extension:metadata`.

### 1.6 Database layer

**There is no shared database base class.** The two persistence implementations are independent:

- `databases/connector_arrow_database.py` — `ConnectorArrowDatabase` (SQLite, PostgreSQL, SpiralDB)
- `databases/delta_lake_databases.py` — `DeltaTableDatabase` (Delta Lake / Parquet)

Read paths:
- `ConnectorArrowDatabase._get_committed_table()` → `connector.iter_batches(SQL)` → `pa.Table.from_batches()`
- `DeltaTableDatabase._read_delta_table()` → `delta_table.to_pyarrow_dataset(as_large_types=True).to_table()`

Neither read path calls any semantic type registration. There is no "peek schema → register → read" pattern.

Schema compatibility checks exist at **write** time only:
- `ConnectorArrowDatabase.flush()`: compares `{name: arrow_type}` dicts — errors on mismatch
- `DeltaTableDatabase._handle_schema_compatibility()`: pending-batch schema merge/coerce/error

Neither check understands extension types.

### 1.7 Existing test suite for the old system

`tests/test_semantic_types/` contains ~2,650 lines covering the shape-based system:

| File | Lines |
|---|---|
| `test_dataclass_encoding.py` | 804 |
| `test_universal_converter.py` | 630 |
| `test_semantic_registry.py` | 239 |
| `test_schema_arrow_equality.py` | 323 |
| `test_path_struct_converter.py` | 132 |
| `test_upath_struct_converter.py` | 148 |
| `test_uuid_struct_converter.py` | 134 |
| `test_semantic_struct_converters.py` | 107 |
| `test_pydata_utils.py` | 136 |

All of these test the old shape-based system and must be removed as part of the migration.

---

## 2. Gap Analysis — PLT-1652 through PLT-1660

### PLT-1652: Define `SemanticTypeConverter` protocol with extension name + metadata

**Gaps / nuance to add to description:**

1. The existing protocol is `SemanticStructConverterProtocol` in `protocols/semantic_types_protocols.py` — the issue description should name this file and the current class name so it's clear what's being replaced.
2. The `hash_struct_dict` / `hasher_id` methods exist on the current protocol. The new protocol drops them (hashing is handled externally in the semantic hasher layer, which uses the converter via `TypeHandlerRegistry`, not directly). The issue description should explicitly say these are removed.
3. `can_handle_python_type` / `can_handle_struct_type` are also removed — identity is now explicit via `extension_name`, not shape-checked.
4. The base class `SemanticStructConverterBase` in `semantic_struct_converters.py` (providing `_compute_content_hash`, `_format_semantic_hash`) will be removed or significantly reduced.

**Verdict:** Accurately scoped. Add the above nuances to the description.

---

### PLT-1653: Revamp `SemanticTypeRegistry`

**Gaps / nuance to add to description:**

1. The current registry's five internal dicts (all struct-shape-keyed) all disappear; the new primary key is `extension_name: str`.
2. `find_semantic_fields_in_schema(schema)` — this public method does struct shape matching. It must be removed or replaced by the schema walker (PLT-1654).
3. `get_semantic_field_info(schema)` — also struct-shape-based, same fate.
4. **Import-time registration vs. the context system**: The `SemanticTypeRegistry` is currently instantiated via the JSON context system (`contexts/registry.py` → `JSONDataContextRegistry` → `DataContext.type_converter`). The `UniversalTypeConverter` holds a `semantic_registry` reference. How does "auto-register at import time" interact with this? Recommendation: the built-in registrations (Path, UPath, UUID) happen when `orcapod` is imported by calling into a global or default registry; the context system either references the same global registry or is updated to wire it correctly. This detail should be noted in PLT-1653.
5. The `_name_to_converter` dict (keyed by semantic type name string) should map to `extension_name` in the new design since that IS the converter's unique key.

**Verdict:** Accurately scoped. Add the above nuances.

---

### PLT-1654: Implement recursive Arrow schema walker for extension type discovery

**Verdict:** Accurately scoped. No conflicts with existing code — this is new code. No changes needed to the description.

---

### PLT-1655: Add peek-schema → register → read pattern with per-process cache to database base class

**Critical gap:** **There is no shared database base class.** `DeltaTableDatabase` and `ConnectorArrowDatabase` are fully independent.

**Required description update:**

Replace "database base class" with the following approach:

- Implement `_ensure_extensions_registered(schema: pa.Schema)` as a **standalone module-level function** in a new file `databases/extension_utils.py`. (The schema walker from PLT-1654 lives in `semantic_types/` since it operates on Arrow types; this DB-layer helper is separate and calls into the walker.)
- The per-process cache (keyed on `(extension_name, metadata)`) lives in `databases/extension_utils.py` as a module-level dict.
- Each database class (`DeltaTableDatabase`, `ConnectorArrowDatabase`) calls this function at the start of their read methods.
- The read chokepoints to update:
  - `DeltaTableDatabase._read_delta_table()` — after `dataset.to_table()`, before returning
  - `ConnectorArrowDatabase._get_committed_table()` — after `pa.Table.from_batches()`, before returning

**Verdict:** Description needs significant revision. No new issue needed — scope stays with PLT-1655 but the approach must be clarified.

---

### PLT-1656: Migrate built-in semantic type converters (Path, UPath, UUID) to extension type protocol

**Gaps / nuance to add to description:**

1. Old storage types: `pa.large_string()` (not `pa.string()`). The issue says `struct{path: str}` — this should be `struct{path: pa.large_string()}`.
2. New storage types: The issue says `pa.string()` for Path/UPath. Use `pa.large_string()` to match the existing convention (Polars compatibility).
3. For UUID new storage (`pa.binary(16)`): `pa.binary(16)` is fine since `binary(16)` is a fixed-size binary and doesn't have the Polars string_view concern. Keep as `pa.binary(16)` (or explicitly document the choice).
4. `PythonPathStructConverter` / `UPathStructConverter` subclass `PathStructConverterBase` which subclasses `SemanticStructConverterBase`. These inheritance chains will be removed when the protocol changes in PLT-1652.
5. `UUIDStructConverter` similarly subclasses `SemanticStructConverterBase`. Both the base class and the UUID converter will be rewritten from scratch.
6. The `file_hasher` dependency on `PathStructConverterBase` (for `hash_struct_dict`) disappears when `hash_struct_dict` is removed from the protocol.

**Verdict:** Accurately scoped. Add storage type corrections and inheritance chain notes.

---

### PLT-1657: Implement dataclass category handler (`orcapod.dataclass`)

**Critical gap:** `dataclass_encoding.py` must be explicitly called out.

**Required description update:**

Add a section "What gets removed":
- `semantic_types/dataclass_encoding.py` — entire module removed
- `DATACLASS_TYPE_FIELD = "__dataclass."` sentinel field
- `DATACLASS_TYPE_PREFIX = "dataclass:"` prefix convention
- `register_dataclass()` decorator / `_DATACLASS_REGISTRY` global
- `has_dataclass_type_sentinel()`, `dataclass_to_struct_dict()`, `struct_dict_to_dataclass()`

The 3-tier reconstruction logic (import → registry → synthesize) is conceptually reused in the new category handler, but via the extension type dispatch path rather than per-row struct dict inspection:

- The extension name `IS` the FQCN — no per-row sentinel field needed
- Tier-1 (importlib) and tier-2 (registry) can be reused in the category handler's `construct_from(extension_name, storage_type)` method
- Tier-3 (synthesize) is only needed for the peek-register step, not per row

Also update `UniversalTypeConverter` references — the `has_dataclass_type_sentinel` / `DATACLASS_TYPE_FIELD` checks in `_convert_arrow_to_python` and `_create_arrow_to_python_converter` all go away.

**Verdict:** Accurately scoped in intent but missing explicit disposition of `dataclass_encoding.py`. Add the "What gets removed" section.

---

### PLT-1658: Implement picklable category handler (`orcapod.picklable`)

**Gaps / nuance to add:**

1. Storage type fields should use `pa.large_string()` (not `pa.string()`) for `class_name` and `pa.large_binary()` (not `pa.binary()`) for `pickle_bytes` — consistent with the Polars large-type convention used throughout the codebase.

**Verdict:** Accurately scoped. Minor storage type correction needed.

---

### PLT-1659: Integration tests: end-to-end semantic type round-trips through Parquet and Delta

**Gaps / nuance to add:**

1. Note that all existing `tests/test_semantic_types/` tests cover the OLD shape-based system. They will be removed as part of PLT-1660 (or as part of whichever issue touches each old module). PLT-1659 is the replacement test suite.
2. The test for `ConnectorArrowDatabase` (SQLite) read path should also be included, not just Parquet and Delta — the peek-register-read pattern applies to all three.

**Verdict:** Accurately scoped. Add notes about old test removal and SQLite read path coverage.

---

### PLT-1660: Remove shape-based type identity code (hard cut)

**Gaps / nuance to add:**

1. Explicit list of modules to be fully deleted:
   - `semantic_types/semantic_struct_converters.py`
   - `semantic_types/dataclass_encoding.py`
2. All of `tests/test_semantic_types/` (~2,650 lines across 9 files) — explicitly call out removal.
3. `SemanticStructConverterBase` in `semantic_struct_converters.py` — gone.
4. `SemanticStructConverterProtocol` in `protocols/semantic_types_protocols.py` — gone (replaced by the new `SemanticTypeConverter` protocol from PLT-1652).
5. `find_semantic_fields_in_schema` / `get_semantic_field_info` on `SemanticTypeRegistry` — gone.
6. The `UniversalTypeConverter` has_dataclass_type_sentinel checks and the struct-branch semantic registry lookup — gone (replaced by extension type dispatch, see new issue below).

**Verdict:** Accurately scoped. Add the explicit list of files/methods deleted.

---

## 3. Missing Issues

### NEW ISSUE: Update `UniversalTypeConverter` to use PyArrow extension type dispatch

**Why:** After PLT-1652 (new protocol) and PLT-1653 (registry revamp), semantic types are Arrow extension types. The `UniversalTypeConverter._convert_arrow_to_python` currently dispatches on struct shape via `self.semantic_registry.get_python_type_for_semantic_struct_signature(arrow_type)`. This needs to be rewritten to:

1. Detect `pa.types.is_extension(arrow_type)` and retrieve `arrow_type.extension_name`
2. Look up the converter in the revamped registry by `extension_name`
3. Remove the `has_dataclass_type_sentinel` branch (replaced by the dataclass category handler via the extension dispatch path)
4. Remove the `TypedDict` synthesis fallback for unregistered structs that were previously semantic types
5. Update `_create_arrow_to_python_converter` similarly

**Sequencing:** After PLT-1653, before PLT-1656.
**Estimate:** S (small — ~60 lines of targeted rewrite in `universal_converter.py`, plus corresponding test updates in `test_universal_converter.py`).

---

## 4. Execution Order (revised)

Proposed ordering after adjustments:

1. PLT-1652 — Define new `SemanticTypeConverter` protocol
2. PLT-1653 — Revamp `SemanticTypeRegistry`
3. PLT-1654 — Recursive Arrow schema walker
4. **NEW** — Update `UniversalTypeConverter` to extension type dispatch
5. PLT-1655 — Peek-schema → register → read pattern (standalone helper + both DB classes)
6. PLT-1656 — Migrate built-in converters (Path, UPath, UUID)
7. PLT-1657 — Dataclass category handler (replaces `dataclass_encoding.py`)
8. PLT-1658 — Picklable category handler
9. PLT-1659 — Integration tests
10. PLT-1660 — Remove shape-based type identity code (hard cut)
