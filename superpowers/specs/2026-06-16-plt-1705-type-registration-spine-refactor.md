# PLT-1705: Type Registration Spine Refactor and DataclassHandlerFactory

**Issue:** PLT-1705
**Date:** 2026-06-16
**Project:** Orcapod: Arrow/Polars Extension Type Semantic Type System
**Branch:** `eywalker/plt-1705-refactor-type-registration-spine-and-implement`

---

## Overview

`UniversalTypeConverter` becomes the **single re-entry point** for all Python ↔ Arrow type
registration and value conversion. `LogicalTypeRegistry` becomes its private implementation
detail. Factories and logical types are thin leaf nodes with no upward dependencies beyond
the `TypeConverterProtocol`.

This supersedes PLT-1657 and closes PR #174 without merging. `DataclassHandlerFactory` is
implemented from scratch on the refined architecture.

---

## Core design principle

`UniversalTypeConverter` owns all traversal of Python annotations and Arrow types in both
directions. Everything that used to be split across `LogicalTypeRegistry.ensure_*` methods
moves into two symmetric public methods on the converter:

| Direction | Method | Input | Output |
|---|---|---|---|
| Write (register) | `register_python_class(annotation)` | Python type annotation | `pa.DataType` |
| Read (register) | `register_storage_type(arrow_type)` | `pa.DataType` | `pa.DataType` |

Both methods walk their input recursively, register any new logical types encountered as a
side effect, and return the normalised Arrow type with extension types embedded.

---

## Section 1: Protocol changes (`extension_types/protocols.py`)

### New: `TypeConverterProtocol`

Minimal protocol exposing what factories and logical types need from the converter.
Placed in `protocols.py` to avoid circular imports.

```python
class TypeConverterProtocol(Protocol):
    def register_python_class(self, annotation: Any) -> pa.DataType: ...
    def register_storage_type(self, arrow_type: pa.DataType) -> pa.DataType: ...
    def python_to_storage(self, value: Any, annotation: Any) -> Any: ...
    def storage_to_python(self, storage_value: Any, annotation: Any) -> Any: ...
```

### Updated: `LogicalTypeFactoryProtocol`

`supports_class` is added (write-side probe). Both factory methods receive `converter`
instead of `registry` and `ResolutionContext`.

```python
class LogicalTypeFactoryProtocol(Protocol):
    def supports_class(self, python_type: type) -> bool: ...

    def create_for_python_type(
        self,
        python_type: type,
        converter: TypeConverterProtocol,
    ) -> LogicalTypeProtocol: ...

    def reconstruct_from_arrow(
        self,
        arrow_extension_name: str,
        storage_type: pa.DataType,
        metadata: dict[str, Any],
        converter: TypeConverterProtocol,
    ) -> LogicalTypeProtocol: ...
```

### Updated: `LogicalTypeProtocol`

Value conversion methods receive `converter`. Built-in implementations accept and ignore it;
`DataclassLogicalType` uses it for per-field recursion.

```python
def python_to_storage(self, value: Any, converter: TypeConverterProtocol) -> Any: ...
def storage_to_python(self, storage_value: Any, converter: TypeConverterProtocol) -> Any: ...
```

---

## Section 2: Registry becomes a thin data store (`extension_types/registry.py`)

### Public surface retained

- `register_logical_type(lt)`
- `register_logical_type_factory(factory, *, category, python_bases)`
- `get_by_python_type`, `get_by_arrow_extension_name`, `get_by_logical_name`

### Removed

- `ensure_logical_type_for_python_class` — logic moves into `UniversalTypeConverter.register_python_class`
- `ensure_extension_type` — logic moves into `UniversalTypeConverter.register_storage_type`

The registry is never passed to factories. It is an internal data structure of the converter.

---

## Section 3: `UniversalTypeConverter` — single re-entry point (`semantic_types/universal_converter.py`)

### `register_python_class(annotation) -> pa.DataType`

Write-side re-entry point. Traverses Python annotations recursively.

- **Primitive** → return from type map directly (no side effects)
- **Registry hit** (concrete type already in `_registry`) → return `lt.get_arrow_extension_type()`
- **Generics** (recurse structurally):
  - `list[T]` → `pa.large_list(register_python_class(T))`
  - `dict[K, V]` → `pa.large_list(pa.struct([field("key", K), field("value", V)]))`
  - `Optional[T]` / `T | None` → `register_python_class(T)` (nullability at field level)
  - `set[T]` → `pa.large_list(register_python_class(T))`
- **Registry miss** on concrete type → MRO walk over `_python_class_factories`, call
  `factory.supports_class(type)` to find match, call
  `factory.create_for_python_type(type, converter=self)`, register result, return extension type
- **Cycle detection** via `_in_progress: set[type]` instance variable: if a type is already
  being synthesised, raise `TypeError`

### `register_storage_type(arrow_type: pa.DataType) -> pa.DataType`

Read-side re-entry point. Traverses Arrow types recursively, bottom-up.

- **Primitive** → return as-is
- **`pa.ExtensionType`**:
  - Registry hit → return immediately (no-op)
  - Registry miss → recurse into `storage_type` first (bottom-up resolution), then parse
    metadata, find factory by `"category"` key, call
    `factory.reconstruct_from_arrow(name, resolved_storage_type, metadata, converter=self)`,
    register result, return extension type
- **`pa.StructType`** → recurse into each field, reassemble with resolved field types
- **`pa.ListType` / `pa.LargeListType`** → recurse into value type, reassemble

The bottom-up order guarantees that when a factory receives `storage_type`, all nested
extension types within it are already registered and resolved.

**Example** — `my_data.Dataset` (dataclass) wrapping `struct{a: i32, b: list[orcapod.uuid]}`:

```
register_storage_type(my_data.Dataset ext → struct{a:i32, b:list[large_binary w/ orcapod.uuid]})
  recurse into storage:
    register_storage_type(struct{a:i32, b:list[orcapod.uuid ext]})
      field a: i32 → i32
      field b: register_storage_type(list[orcapod.uuid ext])
        register_storage_type(orcapod.uuid ext) → registry hit → orcapod.uuid ext
        → list[orcapod.uuid ext]
      → struct{a:i32, b:list[orcapod.uuid ext]}   ← resolved storage type
  my_data.Dataset: registry miss
    → factory.reconstruct_from_arrow("my_data.Dataset",
          struct{a:i32, b:list[orcapod.uuid ext]},    ← resolved, not raw
          {"category":"orcapod.dataclass"}, converter=self)
    → register → return my_data.Dataset ext type
```

### Value conversion methods

```python
def python_to_storage(self, value: Any, annotation: Any) -> Any: ...
def storage_to_python(self, storage_value: Any, annotation: Any) -> Any: ...
```

Thin wrappers over the existing `get_python_to_arrow_converter` /
`get_arrow_to_python_converter` machinery. For extension types, the generated converter
calls `lt.python_to_storage(value, converter=self)` / `lt.storage_to_python(value, converter=self)`.
These are needed by `DataclassLogicalType` for per-field delegation back to the converter.

### Registration pass-throughs

```python
def register_logical_type(self, lt: LogicalTypeProtocol) -> None:
    self._registry.register_logical_type(lt)

def register_logical_type_factory(
    self, factory: LogicalTypeFactoryProtocol,
    *, category: str | None = None,
    python_bases: Iterable[type] = (),
) -> None:
    self._registry.register_logical_type_factory(factory, category=category, python_bases=python_bases)
```

External code that previously used `context.logical_type_registry.register_*` now uses
`context.type_converter.register_*`.

### `ensure_types_registered_for_schemas` (simplified)

```python
def ensure_types_registered_for_schemas(self, *schemas: Schema) -> None:
    for schema in schemas:
        for annotation in schema.values():
            self.register_python_class(annotation)
```

### Removals

- `semantic_registry` constructor parameter and all its usage in `_convert_python_to_arrow`
  / `_convert_arrow_to_python` — removed
- All `dataclass_encoding` imports and the old sentinel-based dataclass struct path — removed;
  `dataclass_encoding.py` is deleted

---

## Section 4: `DataclassHandlerFactory` (`extension_types/dataclass_handler.py` — new file)

### `DataclassLogicalType`

Thin holder of identity, schema, and field annotations. No pre-baked converters.

```python
def python_to_storage(self, value, converter):
    return {
        name: converter.python_to_storage(getattr(value, name), annotation)
        for name, annotation in self._field_annotations
    }

def storage_to_python(self, storage_value, converter):
    return self._python_type(**{
        name: converter.storage_to_python(storage_value[name], annotation)
        for name, annotation in self._field_annotations
    })
```

`_field_annotations: list[tuple[str, type]]` stores `(field_name, python_annotation)` pairs.
No Arrow types stored in the logical type — the converter owns all Arrow-level reasoning.

### `DataclassHandlerFactory`

Stateless. Approximately 30 lines of logic.

**`supports_class(python_type)`**: `return dataclasses.is_dataclass(python_type)`

**`create_for_python_type(python_type, converter)`** (write path):
1. Reject local / unnamed classes (no stable FQCN) with hard `ValueError`
2. `get_type_hints(python_type)` to obtain field annotations
3. Iterate `dataclasses.fields(python_type)`; for each field:
   `arrow_type = converter.register_python_class(annotation)` — all traversal delegated to converter
4. Assemble `pa.struct([pa.field(name, arrow_type), ...])` and `field_annotations` list
5. Return `DataclassLogicalType(fqcn, python_type, storage_type, field_annotations)`

`dict[K, V]` fields encode as `list[struct{key:K, value:V}]` — owned entirely by
`converter.register_python_class`, no special handling in the factory.

**`reconstruct_from_arrow(name, storage_type, metadata, converter)`** (read path):
1. Import class from `name` (FQCN) using longest-prefix module walk — hard `ImportError` if not found
2. `get_type_hints(imported_cls)` → build `field_annotations` matched against `storage_type`'s fields
3. `storage_type` is already resolved (sub-extension types embedded, bottom-up by `register_storage_type`)
4. Factory does **not** call `converter.register_storage_type` for sub-fields — already done
5. Return `DataclassLogicalType(name, imported_cls, storage_type, field_annotations)`

---

## Section 5: `DataContext` and context wiring

**`contexts/core.py`**: `logical_type_registry: LogicalTypeRegistry` field removed.
`type_converter` is the sole public API for type operations.

**`contexts/__init__.py`**: `get_default_logical_type_registry()` removed.

**`contexts/registry.py`**: `_create_context_from_spec` no longer passes `logical_type_registry`
to `DataContext`. The `LogicalTypeRegistry` is constructed as a nested object inside
`type_converter`'s config — it never appears as a top-level `ref_lut` entry.

**`contexts/data/v0.1.json`**:
- Top-level `logical_type_registry` key removed
- Registry construction (with built-in `logical_types` list) moves into `type_converter`'s `_config`
- `semantic_registry` reference removed from `type_converter`'s `_config`

**`contexts/data/schemas/context_schema.json`**:
- Remove `logical_type_registry` from required fields and properties

---

## Section 6: `database_hooks.py` and `ExtensionAwareDatabase`

**`register_discovered_extensions`** simplifies to:

```python
def register_discovered_extensions(converter: TypeConverterProtocol, schema: pa.Schema) -> None:
    for field in schema:
        converter.register_storage_type(field.type)
```

The schema walker's depth-first extension-field extraction is no longer needed here —
`register_storage_type` owns that traversal. `schema_walker.py` itself is retained (other
callers may use it).

**`databases/extension_aware_database.py`**: takes `converter: TypeConverterProtocol`
(was `registry: LogicalTypeRegistry`). Internal call sites updated accordingly.

---

## Section 7: Deletions, built-in updates, and testing

### Deleted files

| File | Reason |
|---|---|
| `semantic_types/dataclass_encoding.py` | Superseded by `DataclassHandlerFactory` + converter |

### Files with removed usages

| File | What is removed |
|---|---|
| `semantic_types/universal_converter.py` | `semantic_registry` usage, `dataclass_encoding` imports |
| `extension_types/type_utils.py` | `extract_leaf_classes` made private (`_extract_leaf_classes`) or removed; traversal lives in converter |

### Built-in logical types (`builtin_logical_types.py`)

`LogicalPath`, `LogicalUUID`, `LogicalUPath` — add `converter` param (accepted, ignored) to
`python_to_storage` and `storage_to_python` on all three, for protocol conformance.

### Test files

| File | Change |
|---|---|
| `tests/test_extension_types/test_protocols.py` | Add `TypeConverterProtocol` conformance; update factory/logical-type stubs for new signatures |
| `tests/test_extension_types/test_registry.py` | Remove `ensure_*` tests; add converter pass-through tests |
| `tests/test_extension_types/test_builtin_logical_types.py` | Update `python_to_storage` / `storage_to_python` call sites to pass a converter stub |
| `tests/test_extension_types/test_dataclass_handler.py` | **New**: `DataclassLogicalType` unit tests; factory write path (flat, list, dict, nested); read path; local-class rejection; cycle detection; `supports_class`; Arrow round-trips |
| `tests/test_semantic_types/test_universal_converter.py` | Add `register_python_class` tests (primitives, generics, factory dispatch, cycle detection); `register_storage_type` tests (primitives, extension types, struct/list recursion); `python_to_storage` / `storage_to_python` for logical type dispatch |

---

## File-by-file change summary

| File | Change |
|---|---|
| `extension_types/protocols.py` | Add `TypeConverterProtocol`; update `LogicalTypeFactoryProtocol` (add `supports_class`, `converter` param); update `LogicalTypeProtocol` (`converter` param on conversion methods) |
| `extension_types/registry.py` | Remove `ensure_logical_type_for_python_class`, `ensure_extension_type` |
| `extension_types/builtin_logical_types.py` | Add `converter` param (ignored) to `python_to_storage` / `storage_to_python` |
| `extension_types/type_utils.py` | `extract_leaf_classes` made private or removed |
| `extension_types/dataclass_handler.py` | **New**: `DataclassLogicalType` + `DataclassHandlerFactory` |
| `semantic_types/universal_converter.py` | Add `register_python_class`, `register_storage_type`, `python_to_storage`, `storage_to_python`, `register_logical_type`, `register_logical_type_factory`; remove `semantic_registry` usage; remove `dataclass_encoding` usage; simplify `ensure_types_registered_for_schemas` |
| `semantic_types/dataclass_encoding.py` | **Deleted** |
| `extension_types/database_hooks.py` | `register_discovered_extensions` takes converter, calls `register_storage_type` per field |
| `databases/extension_aware_database.py` | Takes `converter` instead of `registry` |
| `contexts/core.py` | Remove `logical_type_registry` field |
| `contexts/__init__.py` | Remove `get_default_logical_type_registry` |
| `contexts/registry.py` | Stop passing `logical_type_registry` to `DataContext` |
| `contexts/data/v0.1.json` | Move registry construction inside `type_converter` config; remove `semantic_registry` from `type_converter` config |

---

## Out of scope

- Wiring `DataclassHandlerFactory` into the default context — PLT-1701
- Nested extension types inside struct sub-fields (self-describing nesting) — PLT-1700 (v0.2)
- `dict[K, V]` as `list[struct{key, value}]` — **in scope** (owned by converter, zero factory logic)

## Note: registered logical types as dataclass field types work naturally

Because `DataclassHandlerFactory` delegates all per-field type resolution to
`converter.register_python_class`, dataclass fields typed as registered logical types
(e.g. `pathlib.Path`, `uuid.UUID`, `upath.UPath`) work without any special handling.
`register_python_class` hits the registry immediately for pre-registered types and returns
their Arrow extension type. Value conversion dispatches through the logical type's
`python_to_storage` / `storage_to_python` methods. This was listed as a follow-up gap in
PLT-1657, but is resolved by the new architecture at no extra cost.
