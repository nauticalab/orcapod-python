# PLT-1661: Codebase Audit — Arrow/Polars Extension Type Semantic Type System

**Date:** 2026-06-13
**Issue:** [PLT-1661](https://linear.app/enigma-metamorphic/issue/PLT-1661/codebase-audit-and-project-plan-adjustment)
**Scope:** Audit of `src/orcapod/semantic_types/`, `src/orcapod/protocols/semantic_types_protocols.py`, `src/orcapod/databases/`, and `tests/test_semantic_types/` against the project plan in PLT-1652 through PLT-1660.

---

## 0. Naming and Architecture Conventions (agreed during audit)

The following naming and architectural decisions were made during this audit and apply to all
new and renamed code produced by PLT-1652 through PLT-1660.

### 0.1 Renames

| Old name | New name | Rationale |
|---|---|---|
| `SemanticTypeRegistry` | `ExtensionTypeRegistry` | Solely about Arrow extension type ↔ Python mapping; "semantic" is reserved for the hashing layer. |
| `SemanticTypeConverter` (new protocol per PLT-1652) | `ExtensionTypeConverter` | Same reasoning; this protocol is about Arrow I/O, not hashing. |
| `TypeHandlerRegistry` | `PythonTypeHandlerRegistry` | Handles *any* Python object, not just extension-backed ones. |
| `BuiltinTypeHandlerRegistry` | `BuiltinPythonTypeHandlerRegistry` | Consistency. |
| `BaseSemanticHasher` | `SemanticAwarePythonHasher` | Hashes any Python value; "semantic-aware" signals that it understands extension-backed types. |

Unchanged: `SemanticArrowHasher`, `SemanticHashingVisitor` — "semantic" here correctly means
"understands extension-backed types embedded in Arrow tables."

### 0.2 Canonical definitions

- **Extension-backed type** — a Python class registered with `ExtensionTypeRegistry`, meaning
  it has a defined Arrow extension type storage representation. Examples: `pathlib.Path`,
  `uuid.UUID`, `upath.UPath`. Functions, type objects, and generic aliases are *not*
  extension-backed types.
- **`ExtensionTypeRegistry`** — authoritative registry for extension-backed types: bidirectional
  Python class ↔ Arrow extension type mapping. Knows nothing about hashing.
- **`ExtensionTypeConverter`** — protocol for a single extension-backed type. Members:
  `extension_name`, `extension_metadata`, `storage_type`, `python_to_storage(value)`,
  `storage_to_python(storage_value)`. No hashing methods.
- **`PythonTypeHandlerRegistry`** — hash-dispatch registry for *any* Python object. Entirely
  Arrow-agnostic. Extension-backed types register here independently; so do functions, bytes,
  type objects, generics, and Arrow tables.
- **`SemanticAwarePythonHasher`** — Python-level recursive hasher that uses
  `PythonTypeHandlerRegistry` for type dispatch.

### 0.3 Separation of concerns and the bridge

The two registries are **fully independent** — neither holds a reference to the other.

| Registry | Question it answers | Arrow-aware? |
|---|---|---|
| `ExtensionTypeRegistry` | "Given this Arrow extension type / Python class, how do I convert?" | Yes |
| `PythonTypeHandlerRegistry` | "Given this Python value of any type, how do I hash it?" | No |

The **Python class** (e.g., `pathlib.Path`) is the only shared intermediary.

**`SemanticHashingVisitor` / `SemanticArrowHasher`** are the deliberate bridge. When they
encounter an Arrow extension column they:
1. Look up the converter in `ExtensionTypeRegistry` by `extension_name`
2. Convert storage value → Python object: `converter.storage_to_python(value)` → e.g., `Path("/foo")`
3. Pass the Python object to `SemanticAwarePythonHasher.hash_object(python_value)`
   which dispatches through `PythonTypeHandlerRegistry`
4. Return `content_hash.to_string()` as the hash string for that cell

This means `PathContentHandler`, `UPathContentHandler`, and `UUIDHandler` remain as
**first-class entries** in `builtin_handlers.py` — they are not derived from or replaced by
the extension type converters. There is no auto-registration coupling between the two
registries.

**Performance note**: The Arrow→Python roundtrip in step 2 is not a concern. For `Path`/`UPath`
the bottleneck is file I/O (reading and SHA-256'ing the file). For `UUID` the bottleneck is
hashing 16 bytes. The conversion overhead is negligible in both cases. If profiling ever
reveals a hot path, `ExtensionTypeConverter` can gain an optional
`hash_storage_value(storage_value) -> ContentHash | None` method that defaults to `None`
(triggering the roundtrip) and can be overridden for a direct-bytes shortcut.

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

### 1.7 Two parallel hashing systems for semantic types

There are **two fully independent codepaths** that hash semantic type values. Both exist today and contain logically redundant file-opening/hashing logic for `Path` and `UPath`.

**Path 1 — Arrow table hashing (data packet signature in FunctionNode)**

Call chain: `SemanticArrowHasher.hash_table(table)` → `_process_table_columns(table)` → creates `SemanticHashingVisitor(self.semantic_registry)` per column → `visitor.visit(field.type, value)` → `visit_struct(struct_type, data)` → `registry.get_converter_for_struct_signature(struct_type)` → `converter.hash_struct_dict(data)` → returns `(pa.large_string(), hash_string)`.

For `PythonPathStructConverter.hash_struct_dict({"path": "/foo"})`:
1. Extract path string from struct dict
2. Call `self._file_hasher.hash_file(Path("/foo"))` → `ContentHash`
3. Return `"path:sha256:<hex>"`

For `UUIDStructConverter.hash_struct_dict({"uuid": b'\x00...'})`:
1. Extract raw bytes from struct dict
2. Call `self._compute_content_hash(bytes(raw))` → `ContentHash`
3. Return `"uuid:sha256:<hex>"`

**Path 2 — Python-level hashing (pipeline identity / content_hash)**

Call chain: `BaseSemanticHasher.hash_object(Path("/foo"))` → `self._registry.get_handler(obj)` (MRO lookup) → `PathContentHandler.handle(path, hasher)` → `self.file_hasher.hash_file(path)` → returns `ContentHash`.

For `BaseSemanticHasher.hash_object(uuid.UUID(...))`:
`UUIDHandler.handle(uuid_val, hasher)` → returns `uuid_val.bytes` (raw 16 bytes) → `BaseSemanticHasher` hashes those bytes → `ContentHash`.

**The redundancy**: Both paths open the file and hash content for a `Path` value — but through completely separate implementations in `semantic_struct_converters.py` (Path 1) and `hashing/semantic_hashing/builtin_handlers.py` (Path 2). The `file_hasher` is injected separately in each (into `PathStructConverterBase.__init__` and `PathContentHandler.__init__`).

**Key files involved:**
- `hashing/visitors.py` — `SemanticHashingVisitor` (Path 1 visitor logic; will gain `visit_extension` bridge)
- `hashing/arrow_hashers.py` — `SemanticArrowHasher` (Path 1 entry point)
- `hashing/semantic_hashing/builtin_handlers.py` — `PathContentHandler`, `UPathContentHandler`, `UUIDHandler` (Path 2 handlers — **retained** as first-class entries)
- `hashing/semantic_hashing/semantic_hasher.py` — `BaseSemanticHasher` → renamed `SemanticAwarePythonHasher` (Path 2 entry point)
- `hashing/semantic_hashing/type_handler_registry.py` — `TypeHandlerRegistry` → renamed `PythonTypeHandlerRegistry`; `BuiltinTypeHandlerRegistry` → renamed `BuiltinPythonTypeHandlerRegistry`
- `semantic_types/semantic_struct_converters.py` — `PathStructConverterBase.hash_struct_dict`, `UUIDStructConverter.hash_struct_dict` (removed with the whole module in PLT-1660)

**Resolution** (see §0 for full design): The two paths are unified at `SemanticHashingVisitor` via the bridge pattern — the visitor converts Arrow storage → Python object via `ExtensionTypeRegistry`, then delegates to `SemanticAwarePythonHasher` which dispatches through `PythonTypeHandlerRegistry`. The two registries remain **fully independent**; the Python class is the only shared intermediary. `PathContentHandler`, `UPathContentHandler`, and `UUIDHandler` are **retained** as first-class entries — not removed or replaced.

### 1.8 Existing test suite for the old system

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

### PLT-1652: Define `ExtensionTypeConverter` protocol (renamed from `SemanticTypeConverter`)

**Gaps / nuance to add to description:**

1. The existing protocol is `SemanticStructConverterProtocol` in `protocols/semantic_types_protocols.py` — the issue description should name this file and the current class name so it's clear what's being replaced.
2. **Rename**: The new protocol is `ExtensionTypeConverter`, not `SemanticTypeConverter`. "Semantic" is reserved for the hashing layer; this protocol is purely about Arrow extension type I/O (see §0).
3. The `hash_struct_dict` / `hasher_id` methods exist on the current protocol. They are **completely removed** — hashing is the exclusive concern of `PythonTypeHandlerRegistry` and `SemanticAwarePythonHasher`. The new protocol has no hashing methods whatsoever.
4. `can_handle_python_type` / `can_handle_struct_type` are also removed — identity is now explicit via `extension_name`, not shape-checked.
5. The base class `SemanticStructConverterBase` in `semantic_struct_converters.py` (providing `_compute_content_hash`, `_format_semantic_hash`) is removed entirely.
6. The new protocol members are: `extension_name: str`, `extension_metadata: bytes | None`, `storage_type: pa.DataType`, `python_to_storage(value) -> storage_value`, `storage_to_python(storage_value) -> value`. That is the complete interface.

**Verdict:** Accurately scoped in intent. Update title and description to use `ExtensionTypeConverter` and document the removal of all hashing methods.

---

### PLT-1653: Revamp `SemanticTypeRegistry` → rename to `ExtensionTypeRegistry`

**Gaps / nuance to add to description:**

1. **Rename**: `SemanticTypeRegistry` → `ExtensionTypeRegistry`. The registry is purely about Arrow extension type ↔ Python class mapping. It holds no reference to `PythonTypeHandlerRegistry` and does no hashing (see §0).
2. The current registry's five internal dicts (all struct-shape-keyed) all disappear; the new primary key is `extension_name: str`.
3. `find_semantic_fields_in_schema(schema)` — this public method does struct shape matching. It must be removed or replaced by the schema walker (PLT-1654).
4. `get_semantic_field_info(schema)` — also struct-shape-based, same fate.
5. **Import-time registration vs. the context system**: The `SemanticTypeRegistry` is currently instantiated via the JSON context system (`contexts/registry.py` → `JSONDataContextRegistry` → `DataContext.type_converter`). The `UniversalTypeConverter` holds a `semantic_registry` reference. Recommendation: the built-in registrations (Path, UPath, UUID) happen when `orcapod` is imported; the context system references the same global registry or is updated to wire it correctly.
6. The `_name_to_converter` dict (keyed by semantic type name string) maps to `extension_name` in the new design since that IS the converter's unique key.
7. **Rename `TypeHandlerRegistry` → `PythonTypeHandlerRegistry`** (and `BuiltinTypeHandlerRegistry` → `BuiltinPythonTypeHandlerRegistry`) is also part of this issue's scope. Update `DataContext` field: `type_handler_registry: TypeHandlerRegistry` → `python_type_handler_registry: PythonTypeHandlerRegistry`. Update JSON context specs and all call sites. **No** coupling between `ExtensionTypeRegistry` and `PythonTypeHandlerRegistry` — they are fully independent.

**Verdict:** Accurately scoped. Update title and description to use `ExtensionTypeRegistry`, add the above nuances.

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

### PLT-1656: Migrate built-in extension-backed type converters (Path, UPath, UUID) to `ExtensionTypeConverter` protocol

**Gaps / nuance to add to description:**

1. Old storage types: `pa.large_string()` (not `pa.string()`). The issue says `struct{path: str}` — this should be `struct{path: pa.large_string()}`.
2. New storage types: The issue says `pa.string()` for Path/UPath. Use `pa.large_string()` to match the existing convention (Polars compatibility).
3. For UUID new storage (`pa.binary(16)`): fine as-is. `binary(16)` is fixed-size and doesn't have the Polars string_view concern.
4. `PythonPathStructConverter` / `UPathStructConverter` subclass `PathStructConverterBase` which subclasses `SemanticStructConverterBase`. These inheritance chains are removed entirely — the new converters implement `ExtensionTypeConverter` directly.
5. `UUIDStructConverter` similarly subclasses `SemanticStructConverterBase`. Both the base class and the UUID converter are rewritten from scratch.
6. The `file_hasher` dependency on `PathStructConverterBase` is **removed** from the converter. Hashing of `Path`/`UPath` remains exclusively in `PathContentHandler` / `UPathContentHandler` in `builtin_handlers.py`. The new `PathExtensionConverter` and `UPathExtensionConverter` only deal with Arrow I/O: `python_to_storage(path)` → path string, `storage_to_python(string)` → `Path`. No file_hasher needed.
7. **Update `SemanticHashingVisitor`** (`hashing/visitors.py`): The visitor currently recognises extension-backed types by struct shape (`visit_struct` checks `registry.get_converter_for_struct_signature`). After extension types land, these columns arrive as `pa.ExtensionType`, not structs. Add `visit_extension(extension_type, storage_value)` abstract method to `ArrowTypeDataVisitor`, and override in `SemanticHashingVisitor` to implement the bridge (see §0.3): look up converter by `extension_type.extension_name` in `ExtensionTypeRegistry` → `converter.storage_to_python(storage_value)` → pass Python value to `SemanticAwarePythonHasher.hash_object()` → return `(pa.large_string(), content_hash.to_string())`. The old `visit_struct` semantic-type branch in `SemanticHashingVisitor` is removed.

**Verdict:** Accurately scoped. Update title to use `ExtensionTypeConverter`, add the above nuances.

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
4. `SemanticStructConverterProtocol` in `protocols/semantic_types_protocols.py` — gone (replaced by `ExtensionTypeConverter` from PLT-1652).
5. `find_semantic_fields_in_schema` / `get_semantic_field_info` on `SemanticTypeRegistry` — gone.
6. The `UniversalTypeConverter` has_dataclass_type_sentinel checks and the struct-branch semantic registry lookup — gone (replaced by extension type dispatch, see new issue below).
7. **`PathContentHandler`, `UPathContentHandler`, `UUIDHandler` in `builtin_handlers.py` are kept** — they are first-class entries in `PythonTypeHandlerRegistry` and are not affected by the converter redesign (see §0.3).
8. **Rename `BaseSemanticHasher` → `SemanticAwarePythonHasher`** (`hashing/semantic_hashing/semantic_hasher.py`). Update all references: `DataContext.semantic_hasher` type annotation, JSON context specs, `SemanticHasherProtocol` (if the protocol name is also affected), and all call sites.

**Verdict:** Accurately scoped. Add the explicit list of files/methods deleted, and clarify that `PathContentHandler` etc. are retained.

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

1. PLT-1652 — Define `ExtensionTypeConverter` protocol (rename from `SemanticTypeConverter`)
2. PLT-1653 — Rename `SemanticTypeRegistry` → `ExtensionTypeRegistry`; rename `TypeHandlerRegistry` → `PythonTypeHandlerRegistry`; revamp internals
3. PLT-1654 — Recursive Arrow schema walker
4. **NEW** — Update `UniversalTypeConverter` to extension type dispatch
5. PLT-1655 — Peek-schema → register → read pattern (standalone `databases/extension_utils.py` + both DB classes)
6. PLT-1656 — Migrate built-in converters (Path, UPath, UUID) to `ExtensionTypeConverter`; update `SemanticHashingVisitor` bridge
7. PLT-1657 — Dataclass category handler (replaces `dataclass_encoding.py` entirely)
8. PLT-1658 — Picklable category handler
9. PLT-1659 — Integration tests (Path/UUID/UPath/dataclass/picklable round-trips; all three DB backends)
10. PLT-1660 — Remove shape-based code (hard cut); rename `BaseSemanticHasher` → `SemanticAwarePythonHasher`
