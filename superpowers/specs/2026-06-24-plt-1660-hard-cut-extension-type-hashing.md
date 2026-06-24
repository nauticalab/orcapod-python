# PLT-1660: Hard cut — delete old semantic type system and wire in extension type system

**Date:** 2026-06-24
**Issue:** PLT-1660
**Branch:** `eywalker/plt-1660-hard-cut-delete-old-semantic-type-system-and-wire-in`
**Target:** `extension-type-system`

---

## Overview

The codebase currently has two parallel "semantic type" systems:

1. **Old system** (shape-based identity): `SemanticTypeRegistry` / `SemanticStructConverterProtocol` — identifies extension
   types by matching Arrow struct field signatures. Lives in `src/orcapod/semantic_types/`.
2. **New system** (extension type identity): `LogicalTypeRegistry` / `LogicalTypeProtocol` — identifies types by
   `ARROW:extension:name` metadata embedded in the Arrow field. Lives in `src/orcapod/extension_types/`.

The `UniversalTypeConverter` already uses only the new system. This issue performs a "hard cut": delete the old
system entirely and wire the new system into the remaining production call sites — primarily the Arrow hashing visitors.

---

## Scope

### In scope
- Rewrite `SemanticHashingVisitor` in `visitors.py` to dispatch on extension types instead of struct signatures
- Update `StarfixArrowHasher` (and `SemanticArrowHasher`) to accept `type_converter + semantic_hasher` instead of `semantic_registry`
- Rename `BaseSemanticHasher` → `SemanticAwarePythonHasher`
- Rename `TypeHandlerRegistry` → `PythonTypeHandlerRegistry`, `BuiltinTypeHandlerRegistry` → `BuiltinPythonTypeHandlerRegistry`
- Update `v0.1.json` to remove the `semantic_registry` component and update all cross-refs
- Update `context_schema.json` to match
- Delete `semantic_struct_converters.py`, `semantic_registry.py`, the `SemanticStructConverterProtocol` class, and the old semantic type test directory
- Update all imports and references across the codebase

### Out of scope
- PLT-1798 (making `extension_name == logical_type_name` invariant explicit in code)
- Any changes to `UniversalTypeConverter` — already fully migrated

---

## Design

### 1. Extension-type dispatch in `ArrowTypeDataVisitor`

**File:** `src/orcapod/hashing/visitors.py`

Add `visit_extension` as a non-abstract method on the base class. Update `visit()` to check
`isinstance(arrow_type, pa.ExtensionType)` **before** the struct check, since extension types with
struct storage are otherwise swallowed by `visit_struct`.

```python
def visit_extension(
    self, extension_type: "pa.ExtensionType", storage_value: Any
) -> tuple["pa.DataType", Any]:
    """Handle an Arrow extension type.

    Default implementation: passthrough (preserves extension name and storage value
    unchanged so that the underlying StarfixArrowHasher / ArrowDigester sees the full
    extension metadata when it receives the pre-processed table).

    Subclasses may override to convert recognised extension types to a hashed
    binary value (pa.large_binary()).
    """
    return extension_type, storage_value

def visit(self, arrow_type: "pa.DataType", data: Any) -> tuple["pa.DataType", Any]:
    # Extension types must be checked FIRST; a Path column has storage type
    # large_string, and its field type is an ExtensionType wrapping that storage.
    # If we checked is_struct first, extension types with struct storage would be
    # incorrectly routed to visit_struct.
    if isinstance(arrow_type, pa.ExtensionType):
        new_type, new_data = self.visit_extension(arrow_type, data)
        # Re-visit the result if visit_extension transformed it into a non-extension type.
        # This allows future composability (e.g. a "list of extension type" handler that
        # returns a pa.large_list(pa.large_binary()) from visit_extension) and avoids
        # infinite recursion since we only re-enter when the type changed AND is no
        # longer an extension type.
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

The constructor changes from `(semantic_registry: SemanticTypeRegistry)` to
`(type_converter: UniversalTypeConverter, python_hasher: SemanticAwarePythonHasher)`.

The core logic moves from `visit_struct` into `visit_extension`:

```python
class SemanticHashingVisitor(ArrowTypeDataVisitor):
    """Visitor that replaces extension-typed columns with their content hashes.

    For each Arrow column whose type is a ``pa.ExtensionType``:
    1. Look up the corresponding Python type via ``type_converter``.
    2. If the Python type has a handler registered in ``python_hasher``, convert
       the storage value to a Python object and hash it, replacing the column
       with a ``pa.large_binary()`` value of the form::

           extension_name_bytes + b":" + content_hash.to_prefixed_digest()

       where ``content_hash.to_prefixed_digest()`` = ``method_bytes + b":" + digest``.
    3. If no handler is registered (or if ``type_converter`` does not know the
       extension type), return the extension type and storage value unchanged.
       The downstream ``StarfixArrowHasher`` / ``ArrowDigester`` will see the
       full extension metadata intact and include it in the cross-language hash.
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

        # Only hash if the python hasher has a handler for this type.
        if not self._python_hasher.type_handler_registry.has_handler(python_type):
            return extension_type, storage_value

        # Convert storage value → Python object and hash it.
        python_obj = self._type_converter.storage_to_python(storage_value, python_type)
        content_hash = self._python_hasher.hash_object(python_obj)

        # Encode as binary: "<extension_name>:<method>:<digest>"
        # extension_name identifies the logical type; the content_hash.to_prefixed_digest()
        # encodes the method name + raw digest bytes (compatible with pa.large_binary()
        # columns elsewhere in the codebase that use h.to_prefixed_digest()).
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
all `ARROW:extension:*` metadata, so `ArrowDigester.hash_table(..., include_metadata=True)` will see the
full extension identity. This ensures that extension types without a registered Python handler are still
hashed in a type-aware way by the underlying starfix algorithm.

### 3. `StarfixArrowHasher` constructor update

**File:** `src/orcapod/hashing/arrow_hashers.py`

```python
# Before
def __init__(self, semantic_registry: SemanticTypeRegistry, hasher_id: str) -> None:
    self.semantic_registry = semantic_registry

# After
def __init__(
    self,
    type_converter: "UniversalTypeConverter",
    semantic_hasher: "SemanticAwarePythonHasher",
    hasher_id: str,
) -> None:
    self._type_converter = type_converter
    self._semantic_hasher = semantic_hasher
```

`_process_table_columns` creates `SemanticHashingVisitor(self._type_converter, self._semantic_hasher)` instead of
`SemanticHashingVisitor(self.semantic_registry)`.

The short-circuit in `_process_table_columns` that skips non-struct/non-list columns should be updated: extension
types at the top level of a column CAN need processing, so the check should also pass through when
`isinstance(field.type, pa.ExtensionType)` is True (skip the short-circuit, so the visitor can dispatch
`visit_extension`).

### 4. `SemanticArrowHasher` (legacy hasher)

**File:** `src/orcapod/hashing/arrow_hashers.py`

`SemanticArrowHasher` predates `StarfixArrowHasher` and is not referenced in `v0.1.json`. Apply the same
constructor change (`semantic_registry` → `type_converter + semantic_hasher`) for consistency, or delete it
entirely if no tests depend on it. Preference: **delete** as part of the hard cut.

### 5. Renames

| Old name | New name | File |
|----------|----------|------|
| `BaseSemanticHasher` | `SemanticAwarePythonHasher` | `src/orcapod/hashing/semantic_hashing/semantic_hasher.py` |
| `TypeHandlerRegistry` | `PythonTypeHandlerRegistry` | `src/orcapod/hashing/semantic_hashing/type_handler_registry.py` |
| `BuiltinTypeHandlerRegistry` | `BuiltinPythonTypeHandlerRegistry` | `src/orcapod/hashing/semantic_hashing/type_handler_registry.py` |

All references across the codebase (imports, JSON specs, tests, docs) must be updated in the same PR.

Per the project's no-backward-compatibility policy: no re-export aliases or deprecation wrappers.

### 6. `v0.1.json` changes

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
- Rename the `type_handler_registry` component key → `python_type_handler_registry`.
  Update the `semantic_hasher._config` ref accordingly:
  ```json
  "type_handler_registry": {"_ref": "python_type_handler_registry"}
  ```
- Update `arrow_hasher._class` from `StarfixArrowHasher` (already correct) and verify `semantic_hasher._class` is updated to `SemanticAwarePythonHasher`.
- Update `type_handler_registry` (inside `_config`) class references:
  `TypeHandlerRegistry` → `PythonTypeHandlerRegistry`

Full updated component list in file order:
```
file_hasher            (unchanged)
semantic_registry      ← DELETE
arrow_hasher           (updated refs: type_converter + semantic_hasher)
type_converter         (unchanged)
function_info_extractor(unchanged)
python_type_handler_registry  ← renamed from type_handler_registry
semantic_hasher        (class → SemanticAwarePythonHasher, ref updated)
```

### 7. `context_schema.json` changes

**File:** `src/orcapod/contexts/data/schemas/context_schema.json`

- Remove the `semantic_registry` property from `properties`.
- Rename `type_handler_registry` property to `python_type_handler_registry`.

### 8. `DataContext` core

**File:** `src/orcapod/contexts/core.py`

`DataContext` is a dataclass with `type_converter`, `arrow_hasher`, and `semantic_hasher` fields.
The `type_handler_registry` is not a field on `DataContext` — it is an implementation detail of the
`semantic_hasher`. No changes needed to `core.py` for this issue.

### 9. `versioned_hashers.py`

**File:** `src/orcapod/hashing/versioned_hashers.py`

Update `get_versioned_semantic_arrow_hasher()` to use the new constructor signature:
```python
from orcapod.hashing.semantic_hashing.semantic_hasher import SemanticAwarePythonHasher
# ...
hasher = StarfixArrowHasher(
    hasher_id=hasher_id,
    type_converter=type_converter,   # UniversalTypeConverter from DataContext
    semantic_hasher=semantic_hasher, # SemanticAwarePythonHasher from DataContext
)
```

Since `versioned_hashers.py` currently constructs its own `SemanticTypeRegistry` inline, this module
needs to source `type_converter` and `semantic_hasher` from the active `DataContext` instead. If no context
is available at call time, wire it from the default context.

---

## Files to delete

| File | Reason |
|------|--------|
| `src/orcapod/semantic_types/semantic_struct_converters.py` | Old shape-based converters (PythonPathStructConverter, UUIDStructConverter, UPathStructConverter) |
| `src/orcapod/semantic_types/semantic_registry.py` | Old SemanticTypeRegistry |
| `SemanticStructConverterProtocol` class in `src/orcapod/protocols/semantic_types_protocols.py` | Protocol for old converters |
| `tests/test_semantic_types/` (all 9 files) | Tests for the old system |

After deletion, verify `src/orcapod/semantic_types/__init__.py` no longer re-exports deleted names.

---

## Files to update (beyond the core changes)

These files import from the deleted / renamed modules and must be updated:

- `src/orcapod/hashing/__init__.py` — re-exports `SemanticArrowHasher` (if deleted) and `TypeHandlerRegistry` (renamed)
- `src/orcapod/hashing/versioned_hashers.py` — inline `SemanticTypeRegistry` construction, renamed hasher class
- `src/orcapod/contexts/registry.py` — constructs contexts from JSON; will pick up new class names automatically via `parse_objectspec` as long as the JSON is updated
- `src/orcapod/__init__.py` — any top-level re-exports
- `tests/test_hashing/` — update imports and any `SemanticTypeRegistry` references

Run `grep -r "SemanticTypeRegistry\|semantic_registry\|SemanticStructConverter\|BaseSemanticHasher\|TypeHandlerRegistry\|BuiltinTypeHandlerRegistry" src/ tests/` after implementation to catch any remaining references.

---

## Binary encoding format

Hash values produced by `visit_extension` are stored as `pa.large_binary()` with the layout:

```
<extension_name_ascii> ":" <content_hash.to_prefixed_digest()>
```

where `content_hash.to_prefixed_digest()` = `method.encode("ascii") + b":" + digest_bytes`.

Full example for a `pathlib.Path` column hashed with SHA-256:
```
b"orcapod.path:semantic_v0.1:\xab\xcd\xef..."
```

This is consistent with the pattern already used in `function_node.py`:
```python
self.data_context.arrow_hasher.hash_table(tag_with_hash).to_prefixed_digest()
```

---

## Extension type short-circuit fix

In `StarfixArrowHasher._process_table_columns`, the current short-circuit bypasses the visitor for
non-struct/non-list columns:

```python
if not (
    pa.types.is_struct(field.type)
    or pa.types.is_list(field.type)
    or ...
):
    new_columns.append(table.column(i))  # skipped — no visitor call
    ...
    continue
```

Extension type columns whose storage type is `pa.large_string()` (e.g. `orcapod.path`) would be
short-circuited here. The fix: also skip the short-circuit when the field type is an extension type:

```python
if not (
    isinstance(field.type, pa.ExtensionType)   # ← add this
    or pa.types.is_struct(field.type)
    or pa.types.is_list(field.type)
    or pa.types.is_large_list(field.type)
    or pa.types.is_fixed_size_list(field.type)
    or pa.types.is_map(field.type)
):
    ...
    continue
```

---

## Test strategy

1. Existing tests in `tests/test_hashing/` must all pass after the rename and wiring changes.
2. `tests/test_extension_types/` round-trip tests verify the conversion chain; these should continue to pass.
3. The deleted `tests/test_semantic_types/` tests are replaced implicitly by the extension type integration
   tests — no new test file is required unless a specific gap is identified.
4. Run: `uv run pytest tests/test_hashing/ tests/test_extension_types/ tests/test_core/ -x`

---

## Implementation order

1. Rename `BaseSemanticHasher` → `SemanticAwarePythonHasher` and `TypeHandlerRegistry` → `PythonTypeHandlerRegistry` (update all references).
2. Add `visit_extension` to `ArrowTypeDataVisitor`; update `visit()` dispatch.
3. Rewrite `SemanticHashingVisitor` constructor and `visit_extension` implementation.
4. Update `StarfixArrowHasher` constructor; update `_process_table_columns` short-circuit.
5. Update `v0.1.json` and `context_schema.json`.
6. Update `versioned_hashers.py`.
7. Delete old semantic type files and their tests.
8. Run grep sweep for stale references; fix any found.
9. Run full test suite.
