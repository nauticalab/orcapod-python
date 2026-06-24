# PLT-1660: Hard cut — delete old semantic type system and wire in extension type system

**Date:** 2026-06-24
**Issue:** PLT-1660
**Branch:** `eywalker/plt-1660-hard-cut-delete-old-semantic-type-system-and-wire-in`
**Target:** `extension-type-system`

---

## Overview

The codebase currently has two parallel "semantic type" systems:

1. **Old system** (shape-based identity): `SemanticTypeRegistry` / `SemanticStructConverterProtocol` — identifies
   extension types by matching Arrow struct field signatures. Lives in `src/orcapod/semantic_types/`.
2. **New system** (extension type identity): `LogicalTypeRegistry` / `LogicalTypeProtocol` — identifies types by
   `ARROW:extension:name` metadata embedded in the Arrow field. Lives in `src/orcapod/extension_types/`.

`UniversalTypeConverter` already uses only the new system. This issue performs a "hard cut": delete the old
system entirely and wire the new system into the remaining production call sites — primarily the Arrow hashing
visitors.

This issue also folds in a protocol tightening: `TypeHandlerProtocol.handle()` currently has a mixed return
type (`Any`) — some handlers return `ContentHash` directly (Path, ArrowTable), while others return intermediate
values (UUID returns `bytes`, BytesHandler returns `str`, etc.). Since all handlers receive the full hasher
reference and the only purpose of a handler is to produce a hash, the protocol is tightened so every handler
returns `ContentHash` directly. This makes the naming accurate and the interface uniform.

---

## Scope

### In scope
- Rewrite `SemanticHashingVisitor` in `visitors.py` to dispatch on extension types instead of struct signatures
- Update `StarfixArrowHasher` (and delete `SemanticArrowHasher`) to accept `type_converter + semantic_hasher`
  instead of `semantic_registry`
- **Protocol tightening**: change `TypeHandlerProtocol.handle() -> Any` to
  `PythonTypeSemanticHasherProtocol.hash() -> ContentHash`; update all builtin handlers accordingly
- **Renames** (full list in §Design §5):
  - `BaseSemanticHasher` → `SemanticAwarePythonHasher`
  - `TypeHandlerRegistry` → `PythonTypeSemanticHasherRegistry`
  - `BuiltinTypeHandlerRegistry` → `BuiltinPythonTypeSemanticHasherRegistry`
  - `TypeHandlerProtocol` → `PythonTypeSemanticHasherProtocol`
  - All builtin handler classes renamed (e.g. `PathContentHandler` → `PathSemanticHasher`)
  - `register_builtin_handlers` → `register_builtin_python_type_semantic_hashers`
  - `get_default_type_handler_registry` → `get_default_python_type_semantic_hasher_registry`
- Update `v0.1.json` to remove `semantic_registry` component and update all class names / cross-refs
- Update `context_schema.json` to match
- Delete `semantic_struct_converters.py`, `semantic_registry.py`, `SemanticStructConverterProtocol`, and
  `tests/test_semantic_types/`
- Update all imports and references across the codebase

### Out of scope
- PLT-1798 (making `extension_name == logical_type_name` invariant explicit in code)
- Any changes to `UniversalTypeConverter` — already fully migrated

---

## Design

### 1. Extension-type dispatch in `ArrowTypeDataVisitor`

**File:** `src/orcapod/hashing/visitors.py`

Add `visit_extension` as a non-abstract method on the base class. Update `visit()` to check
`isinstance(arrow_type, pa.ExtensionType)` **before** the struct check — otherwise extension types with
struct storage would be swallowed by `visit_struct`.

```python
def visit_extension(
    self, extension_type: "pa.ExtensionType", storage_value: Any
) -> tuple["pa.DataType", Any]:
    """Handle an Arrow extension type.

    Default implementation: passthrough — preserves the extension type and its storage
    value unchanged so that the downstream StarfixArrowHasher / ArrowDigester sees the
    full extension metadata when it receives the pre-processed table.

    Subclasses may override to convert recognised extension types to a hashed
    pa.large_binary() value.
    """
    return extension_type, storage_value

def visit(self, arrow_type: "pa.DataType", data: Any) -> tuple["pa.DataType", Any]:
    # Extension types must be checked FIRST. A Path column has storage type
    # large_string, and its field type is an ExtensionType wrapping that storage.
    # Checking is_struct first would incorrectly route extension types with struct
    # storage into visit_struct.
    if isinstance(arrow_type, pa.ExtensionType):
        new_type, new_data = self.visit_extension(arrow_type, data)
        # Re-visit if visit_extension transformed to a non-extension type.
        # This enables composability (e.g. a list-of-extension-type handler returning
        # pa.large_list(pa.large_binary())) and avoids infinite recursion: we only
        # re-enter when the type changed AND is no longer an extension type.
        if new_type is not arrow_type and not isinstance(new_type, pa.ExtensionType):
            return self.visit(new_type, new_data)
        return new_type, new_data
    if pa.types.is_struct(arrow_type):
        return self.visit_struct(arrow_type, data)
    elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return self.visit_list(arrow_type, data)
    elif pa.types.is_fixed_size_list(arrow_type):
        return self.visit_list(arrow_type, data)
    elif pa.types.is_map(arrow_type):
        return self.visit_map(arrow_type, data)
    else:
        return self.visit_primitive(arrow_type, data)
```

### 2. `SemanticHashingVisitor` rewrite

**File:** `src/orcapod/hashing/visitors.py`

Constructor changes from `(semantic_registry: SemanticTypeRegistry)` to
`(type_converter: UniversalTypeConverter, python_hasher: SemanticAwarePythonHasher)`.

Core logic moves from `visit_struct` into `visit_extension`:

```python
class SemanticHashingVisitor(ArrowTypeDataVisitor):
    """Visitor that replaces extension-typed columns with their content hashes.

    For each Arrow column whose type is a ``pa.ExtensionType``:
    1. Look up the corresponding Python type via ``type_converter``.
    2. If the Python type has a semantic hasher registered in ``python_hasher``,
       convert the storage value to a Python object and hash it, replacing the
       column with a ``pa.large_binary()`` value of the form::

           extension_name_bytes + b":" + content_hash.to_prefixed_digest()

       where ``content_hash.to_prefixed_digest()`` = ``method_bytes + b":" + digest``.
    3. If no hasher is registered (or if ``type_converter`` does not know the
       extension type), return the extension type and storage value unchanged.
       The downstream ``StarfixArrowHasher`` / ``ArrowDigester`` will see the
       full extension metadata intact and hash it in a type-aware way.
    """

    def __init__(
        self,
        type_converter: "UniversalTypeConverter",
        python_hasher: "SemanticAwarePythonHasher",
    ) -> None:
        self._type_converter = type_converter
        self._python_hasher = python_hasher
        self._current_field_path: list[str] = []

    def visit_extension(
        self, extension_type: "pa.ExtensionType", storage_value: Any
    ) -> tuple["pa.DataType", Any]:
        if storage_value is None:
            return extension_type, None

        # Resolve extension type → Python type.
        python_type = self._type_converter.arrow_type_to_python_type(extension_type)

        # If the converter couldn't resolve to a concrete class, passthrough.
        if python_type is Any or not isinstance(python_type, type):
            return extension_type, storage_value

        # Only hash if the python hasher has a semantic hasher for this type.
        if not self._python_hasher.type_semantic_hasher_registry.has_semantic_hasher(python_type):
            return extension_type, storage_value

        # Convert storage value → Python object and hash it.
        python_obj = self._type_converter.storage_to_python(storage_value, python_type)
        content_hash = self._python_hasher.hash_object(python_obj)

        # Encode as binary: "<extension_name>:<method>:<digest>"
        hash_bytes = (
            extension_type.extension_name.encode("ascii")
            + b":"
            + content_hash.to_prefixed_digest()
        )
        return pa.large_binary(), hash_bytes

    def visit_struct(self, struct_type, data):
        """Regular struct (no extension identity) — recurse into fields."""
        if data is None:
            return struct_type, None
        return self._visit_struct_fields(struct_type, data)

    def visit_list(self, list_type, data):
        if data is None:
            return list_type, None
        self._current_field_path.append("[*]")
        try:
            return self._visit_list_elements(list_type, data)
        finally:
            self._current_field_path.pop()

    def visit_map(self, map_type, data):
        return map_type, data

    def visit_primitive(self, primitive_type, data):
        return primitive_type, data
```

**Passthrough invariant:** when `visit_extension` returns the original `(extension_type, storage_value)`,
the column's field type remains a `pa.ExtensionType`. `schema_cleaner.clean_schema_for_hashing` retains
all `ARROW:extension:*` metadata, so `ArrowDigester.hash_table(..., include_metadata=True)` sees the full
extension identity. Extension types without a registered Python semantic hasher are still hashed in a
type-aware way by the underlying starfix algorithm.

### 3. `StarfixArrowHasher` constructor update

**File:** `src/orcapod/hashing/arrow_hashers.py`

```python
# Before
def __init__(self, semantic_registry: SemanticTypeRegistry, hasher_id: str) -> None:

# After
def __init__(
    self,
    type_converter: "UniversalTypeConverter",
    semantic_hasher: "SemanticAwarePythonHasher",
    hasher_id: str,
) -> None:
    self._type_converter = type_converter
    self._semantic_hasher = semantic_hasher
    self._hasher_id = hasher_id
```

`_process_table_columns` constructs `SemanticHashingVisitor(self._type_converter, self._semantic_hasher)`
instead of `SemanticHashingVisitor(self.semantic_registry)`.

The short-circuit in `_process_table_columns` that skips non-struct/non-list columns must also allow
extension type columns through — otherwise Path columns (storage: `large_string`) would be silently skipped
before the visitor sees them:

```python
if not (
    isinstance(field.type, pa.ExtensionType)   # ← add this
    or pa.types.is_struct(field.type)
    or pa.types.is_list(field.type)
    or pa.types.is_large_list(field.type)
    or pa.types.is_fixed_size_list(field.type)
    or pa.types.is_map(field.type)
):
    new_columns.append(table.column(i))
    new_fields.append(field)
    continue
```

### 4. `SemanticArrowHasher` (legacy hasher)

**File:** `src/orcapod/hashing/arrow_hashers.py`

`SemanticArrowHasher` predates `StarfixArrowHasher` and is not referenced in `v0.1.json`. **Delete** it as
part of the hard cut. If any test depends on it directly, delete the test — these tests are superseded by the
extension type integration tests.

### 5. Renames

#### Classes and protocols

| Old name | New name | File |
|----------|----------|------|
| `BaseSemanticHasher` | `SemanticAwarePythonHasher` | `semantic_hashing/semantic_hasher.py` |
| `TypeHandlerRegistry` | `PythonTypeSemanticHasherRegistry` | `semantic_hashing/type_handler_registry.py` |
| `BuiltinTypeHandlerRegistry` | `BuiltinPythonTypeSemanticHasherRegistry` | `semantic_hashing/type_handler_registry.py` |
| `TypeHandlerProtocol` | `PythonTypeSemanticHasherProtocol` | `protocols/hashing_protocols.py` |

#### Builtin handler classes (in `semantic_hashing/builtin_handlers.py`)

| Old name | New name |
|----------|----------|
| `PathContentHandler` | `PathSemanticHasher` |
| `UPathContentHandler` | `UPathSemanticHasher` |
| `UUIDHandler` | `UUIDSemanticHasher` |
| `BytesHandler` | `BytesSemanticHasher` |
| `FunctionHandler` | `FunctionSemanticHasher` |
| `TypeObjectHandler` | `TypeObjectSemanticHasher` |
| `SpecialFormHandler` | `SpecialFormSemanticHasher` |
| `GenericAliasHandler` | `GenericAliasSemanticHasher` |
| `UnionTypeHandler` | `UnionTypeSemanticHasher` |
| `ArrowTableHandler` | `ArrowTableSemanticHasher` |
| `SchemaHandler` | `SchemaSemanticHasher` |

#### Functions and properties

| Old name | New name | Location |
|----------|----------|----------|
| `register_builtin_handlers(registry)` | `register_builtin_python_type_semantic_hashers(registry)` | `builtin_handlers.py` |
| `get_default_type_handler_registry()` | `get_default_python_type_semantic_hasher_registry()` | `type_handler_registry.py` and `defaults.py` |
| `BaseSemanticHasher.type_handler_registry` property | `SemanticAwarePythonHasher.type_semantic_hasher_registry` | `semantic_hasher.py` |

#### Registry methods

| Old name | New name |
|----------|----------|
| `get_handler(obj)` | `get_semantic_hasher(obj)` |
| `get_handler_for_type(target_type)` | `get_semantic_hasher_for_type(target_type)` |
| `has_handler(target_type)` | `has_semantic_hasher(target_type)` |

The `register(target_type, handler)` method name is unchanged — "register" is generic enough.

All references across the codebase (imports, JSON specs, tests, docs) must be updated in the same PR.
Per the project's no-backward-compatibility policy: no re-export aliases or deprecation wrappers.

### 6. Protocol tightening — `PythonTypeSemanticHasherProtocol`

**File:** `src/orcapod/protocols/hashing_protocols.py`

The `handle(obj, hasher) -> Any` method is replaced by `hash(obj, hasher) -> ContentHash`:

```python
class PythonTypeSemanticHasherProtocol(Protocol):
    """Protocol for type-specific semantic hashers used by SemanticAwarePythonHasher.

    A PythonTypeSemanticHasherProtocol hashes a specific Python type to a ContentHash.
    Implementations are registered with a PythonTypeSemanticHasherRegistry and looked
    up via MRO-aware resolution.

    Each implementation receives the full SemanticAwarePythonHasher so it can delegate
    hashing of sub-values (e.g. hashing a dict of function metadata) back to the outer
    hasher without coupling to a specific hasher instance.
    """

    def hash(self, obj: Any, hasher: "SemanticAwarePythonHasher") -> ContentHash:
        """Hash *obj* to a ContentHash.

        Args:
            obj:    The object to hash. Always matches the registered type.
            hasher: The active SemanticAwarePythonHasher. Use
                    ``hasher.hash_object(sub_value)`` to hash sub-values.

        Returns:
            ContentHash: The content-addressed hash of *obj*.
        """
        ...
```

#### `hash_object()` simplification

Because every semantic hasher now returns `ContentHash` directly, the dispatch in `hash_object()` simplifies
from a double call to a single call:

```python
# Before
semantic_hasher = self._registry.get_semantic_hasher(obj)
if semantic_hasher is not None:
    return self.hash_object(semantic_hasher.handle(obj, self), resolver=resolver)
    #        ^^^ recursive wrap ^^^

# After
semantic_hasher = self._registry.get_semantic_hasher(obj)
if semantic_hasher is not None:
    return semantic_hasher.hash(obj, self)   # always ContentHash — no wrap
```

#### Updated builtin implementations

Each builtin class returns `ContentHash` directly by delegating sub-values back to `hasher.hash_object()`:

```python
class PathSemanticHasher:
    def __init__(self, file_hasher: FileContentHasherProtocol) -> None:
        self.file_hasher = file_hasher

    def hash(self, obj: PathLike, hasher: SemanticAwarePythonHasher) -> ContentHash:
        path = Path(obj)
        # (existence / is_dir checks unchanged)
        return self.file_hasher.hash_file(path)   # already returns ContentHash


class UUIDSemanticHasher:
    def hash(self, obj: Any, hasher: SemanticAwarePythonHasher) -> ContentHash:
        return hasher.hash_object(obj.bytes)       # bytes → ContentHash via hasher


class BytesSemanticHasher:
    def hash(self, obj: Any, hasher: SemanticAwarePythonHasher) -> ContentHash:
        if isinstance(obj, (bytes, bytearray)):
            return hasher.hash_object(obj.hex())   # hex str → ContentHash via hasher
        raise TypeError(...)


class FunctionSemanticHasher:
    def __init__(self, function_info_extractor: Any) -> None:
        self.function_info_extractor = function_info_extractor

    def hash(self, obj: Any, hasher: SemanticAwarePythonHasher) -> ContentHash:
        info = self.function_info_extractor.extract_function_info(obj)
        return hasher.hash_object(info)            # dict → ContentHash via hasher


class TypeObjectSemanticHasher:
    def hash(self, obj: Any, hasher: SemanticAwarePythonHasher) -> ContentHash:
        module = obj.__module__ or "<unknown>"
        return hasher.hash_object(f"type:{module}.{obj.__qualname__}")


class ArrowTableSemanticHasher:
    def __init__(self, arrow_hasher: ArrowHasherProtocol) -> None:
        self.arrow_hasher = arrow_hasher

    def hash(self, obj: Any, hasher: SemanticAwarePythonHasher) -> ContentHash:
        if isinstance(obj, pa.RecordBatch):
            obj = pa.Table.from_batches([obj])
        return self.arrow_hasher.hash_table(obj)   # already returns ContentHash


class SpecialFormSemanticHasher:
    def hash(self, obj: Any, hasher: SemanticAwarePythonHasher) -> ContentHash:
        name = getattr(obj, "_name", None) or repr(obj)
        return hasher.hash_object(f"special_form:typing.{name}")


class GenericAliasSemanticHasher:
    def hash(self, obj: Any, hasher: SemanticAwarePythonHasher) -> ContentHash:
        import typing
        origin = getattr(obj, "__origin__", None)
        args = getattr(obj, "__args__", None) or ()
        if origin is None:
            return hasher.hash_object(f"generic_alias:{obj!r}")
        if origin is typing.Union:
            hashed_args = sorted(hasher.hash_object(arg).to_string() for arg in args)
            return hasher.hash_object({"__type__": "union", "args": hashed_args})
        return hasher.hash_object({
            "__type__": "generic_alias",
            "origin": hasher.hash_object(origin).to_string(),
            "args": [hasher.hash_object(arg).to_string() for arg in args],
        })


class UnionTypeSemanticHasher:
    def hash(self, obj: Any, hasher: SemanticAwarePythonHasher) -> ContentHash:
        args = getattr(obj, "__args__", None) or ()
        hashed_args = sorted(hasher.hash_object(arg).to_string() for arg in args)
        return hasher.hash_object({"__type__": "union", "args": hashed_args})
```

### 7. `v0.1.json` changes

**File:** `src/orcapod/contexts/data/v0.1.json`

- Remove the `semantic_registry` top-level component entirely.
- In `arrow_hasher._config`, replace:
  ```json
  "semantic_registry": {"_ref": "semantic_registry"}
  ```
  with:
  ```json
  "type_converter": {"_ref": "type_converter"},
  "semantic_hasher": {"_ref": "semantic_hasher"}
  ```
- Rename component key `type_handler_registry` → `python_type_semantic_hasher_registry`.
- Update `semantic_hasher._config` ref:
  ```json
  "type_handler_registry": {"_ref": "python_type_semantic_hasher_registry"}
  ```
- Update `semantic_hasher._class`:
  `orcapod.hashing.semantic_hashing.semantic_hasher.BaseSemanticHasher`
  → `orcapod.hashing.semantic_hashing.semantic_hasher.SemanticAwarePythonHasher`
- Update `python_type_semantic_hasher_registry._class`:
  `orcapod.hashing.semantic_hashing.type_handler_registry.TypeHandlerRegistry`
  → `orcapod.hashing.semantic_hashing.type_handler_registry.PythonTypeSemanticHasherRegistry`
- Update all handler `_class` entries in `python_type_semantic_hasher_registry._config.handlers`
  to use the new class names (e.g. `PathContentHandler` → `PathSemanticHasher`, etc.)

Full updated component list in file order:
```
file_hasher                         (unchanged)
semantic_registry                   ← DELETE
arrow_hasher                        (class unchanged; _config: + type_converter ref, + semantic_hasher ref, - semantic_registry ref)
type_converter                      (unchanged)
function_info_extractor             (unchanged)
python_type_semantic_hasher_registry ← renamed from type_handler_registry; class + handler entries updated
semantic_hasher                     (class → SemanticAwarePythonHasher; ref updated)
```

### 8. `context_schema.json` changes

**File:** `src/orcapod/contexts/data/schemas/context_schema.json`

- Remove the `semantic_registry` property from `properties`.
- Rename `type_handler_registry` property to `python_type_semantic_hasher_registry`.

### 9. `DataContext` core

**File:** `src/orcapod/contexts/core.py`

`DataContext` is a dataclass with `type_converter`, `arrow_hasher`, and `semantic_hasher` fields.
`type_handler_registry` is not a field on `DataContext` — it is an implementation detail of `semantic_hasher`.
No changes needed to `core.py`.

### 10. `versioned_hashers.py`

**File:** `src/orcapod/hashing/versioned_hashers.py`

Update `get_versioned_semantic_arrow_hasher()`:
- Remove inline `SemanticTypeRegistry` / `PythonPathStructConverter` / `UUIDStructConverter` construction.
- Source `type_converter` and `semantic_hasher` from the default `DataContext`:

```python
def get_versioned_semantic_arrow_hasher(
    hasher_id: str = _CURRENT_ARROW_HASHER_ID,
) -> hp.ArrowHasherProtocol:
    from orcapod.hashing.arrow_hashers import StarfixArrowHasher
    from orcapod.contexts import resolve_context

    ctx = resolve_context(None)   # default context
    return StarfixArrowHasher(
        hasher_id=hasher_id,
        type_converter=ctx.type_converter,
        semantic_hasher=ctx.semantic_hasher,
    )
```

Update `get_versioned_semantic_hasher()` to import `SemanticAwarePythonHasher` instead of `BaseSemanticHasher`.

---

## Files to delete

| File | Reason |
|------|--------|
| `src/orcapod/semantic_types/semantic_struct_converters.py` | Old shape-based converters |
| `src/orcapod/semantic_types/semantic_registry.py` | Old `SemanticTypeRegistry` |
| `SemanticStructConverterProtocol` class in `src/orcapod/protocols/semantic_types_protocols.py` | Protocol for old system |
| `tests/test_semantic_types/` (all 9 files) | Tests for old system |

After deletion, verify `src/orcapod/semantic_types/__init__.py` no longer re-exports deleted names.

---

## Files to update (beyond the core changes)

These files import from the deleted or renamed modules and must be updated:

- `src/orcapod/hashing/__init__.py` — re-exports `BaseSemanticHasher`, `TypeHandlerRegistry`, `TypeHandlerProtocol`
- `src/orcapod/hashing/semantic_hashing/__init__.py` — re-exports all renamed classes
- `src/orcapod/hashing/defaults.py` — `get_default_type_handler_registry` → `get_default_python_type_semantic_hasher_registry`
- `src/orcapod/hashing/semantic_hashing/content_identifiable_mixin.py` — references `BaseSemanticHasher`
- `src/orcapod/hashing/versioned_hashers.py` — inline registry construction, old class names
- `src/orcapod/protocols/hashing_protocols.py` — `TypeHandlerProtocol` docstring references
- `src/orcapod/contexts/core.py` — `TYPE_CHECKING` import of `BaseSemanticHasher` (if any)
- `tests/test_hashing/` — update imports and any direct registry/handler references

Run this sweep after implementation to catch any remaining references:

```bash
grep -rn "SemanticTypeRegistry\|semantic_registry\|SemanticStructConverter\
\|BaseSemanticHasher\|TypeHandlerRegistry\|BuiltinTypeHandlerRegistry\
\|TypeHandlerProtocol\|PathContentHandler\|UPathContentHandler\
\|UUIDHandler\|BytesHandler\|FunctionHandler\|TypeObjectHandler\
\|SpecialFormHandler\|GenericAliasHandler\|UnionTypeHandler\|ArrowTableHandler\
\|SchemaHandler\|register_builtin_handlers\|get_default_type_handler_registry\
\|type_handler_registry\|get_handler\|has_handler" src/ tests/
```

---

## Binary encoding format

Hash values produced by `visit_extension` are stored as `pa.large_binary()` with the layout:

```
<extension_name_ascii> ":" <content_hash.to_prefixed_digest()>
```

where `content_hash.to_prefixed_digest()` = `method.encode("ascii") + b":" + digest_bytes`.

Full example for a `pathlib.Path` column whose file is hashed with SHA-256 by the semantic hasher:
```
b"orcapod.path:semantic_v0.1:\xab\xcd\xef..."
              ^^^^^^^^^^^^^^  ^^^^^^^^^^^^^^
              hasher_id       raw SHA-256 digest
```

This is consistent with the existing pattern in `function_node.py`:
```python
self.data_context.arrow_hasher.hash_table(tag_with_hash).to_prefixed_digest()
```

---

## Test strategy

1. Existing tests in `tests/test_hashing/` must all pass after renames, protocol changes, and wiring.
2. `tests/test_extension_types/` round-trip tests verify the full conversion chain; these must pass.
3. The deleted `tests/test_semantic_types/` tests are superseded by the extension type integration tests.
4. Run: `uv run pytest tests/test_hashing/ tests/test_extension_types/ tests/test_core/ -x`

---

## Implementation order

1. **Rename `TypeHandlerProtocol` → `PythonTypeSemanticHasherProtocol`**, change `handle() -> Any` to
   `hash() -> ContentHash` in `protocols/hashing_protocols.py`. Update docstring.
2. **Rename `TypeHandlerRegistry` → `PythonTypeSemanticHasherRegistry`**, rename all registry methods
   (`get_handler` → `get_semantic_hasher`, `has_handler` → `has_semantic_hasher`, etc.),
   rename `BuiltinTypeHandlerRegistry` → `BuiltinPythonTypeSemanticHasherRegistry`.
3. **Update all builtin handler classes** in `builtin_handlers.py`: rename each class, change `handle()` →
   `hash()`, update return type from `Any` → `ContentHash`, update implementations to return `ContentHash`
   directly. Rename `register_builtin_handlers` → `register_builtin_python_type_semantic_hashers`.
4. **Rename `BaseSemanticHasher` → `SemanticAwarePythonHasher`** in `semantic_hasher.py`: simplify
   `hash_object()` dispatch (remove double-wrap), rename `type_handler_registry` property →
   `type_semantic_hasher_registry`, rename `get_default_type_handler_registry` → 
   `get_default_python_type_semantic_hasher_registry`.
5. **Update `__init__.py` exports** in `hashing/` and `hashing/semantic_hashing/` to use new names.
6. **Add `visit_extension` to `ArrowTypeDataVisitor`**; update `visit()` dispatch.
7. **Rewrite `SemanticHashingVisitor`** constructor and `visit_extension` implementation.
8. **Update `StarfixArrowHasher`**: new constructor signature, `_process_table_columns` short-circuit fix,
   delete `SemanticArrowHasher`.
9. **Update `v0.1.json`** and **`context_schema.json`**.
10. **Update `versioned_hashers.py`** to source from `DataContext`.
11. **Delete** old semantic type files and their tests.
12. **Run grep sweep** for stale references; fix any found.
13. **Run full test suite**: `uv run pytest tests/test_hashing/ tests/test_extension_types/ tests/test_core/ -x`
