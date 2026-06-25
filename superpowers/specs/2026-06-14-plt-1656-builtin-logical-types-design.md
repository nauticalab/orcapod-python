# PLT-1656: Built-in LogicalType Implementations (Path, UPath, UUID)

**Date:** 2026-06-14
**Issue:** PLT-1656
**Depends on:** PLT-1668 (LogicalType protocol + LogicalTypeRegistry — completed)

---

## Overview

Implement the three built-in `LogicalType` instances (`LogicalPath`,
`LogicalUPath`, `LogicalUUID`) in a new module
`src/orcapod/extension_types/builtin_logical_types.py`.

Wire the default registry into `DataContext` via `v0.1.json` using the existing
`parse_objectspec()` JSON object spec mechanism — exactly as `semantic_registry`,
`type_converter`, and the other default objects are built. The primary access path
for the default registry is `get_default_context().logical_type_registry`, with a
`get_default_logical_type_registry()` convenience function added to `contexts`.

These are the first concrete implementations of the `LogicalType` protocol
introduced by PLT-1668. The naming convention is `LogicalXXX` (no "Type" suffix):
`LogicalType` is the abstract protocol; `LogicalPath`, `LogicalUPath`, `LogicalUUID`
are the concrete descriptors. The old `PythonPathStructConverter`,
`UPathStructConverter`, and `UUIDStructConverter` in
`semantic_types/semantic_struct_converters.py` remain untouched until PLT-1660
(hard cut).

---

## New file: `src/orcapod/extension_types/builtin_logical_types.py`

### `LogicalPath`

| Property / Method | Value |
|---|---|
| `logical_type_name` | `"pathlib.Path"` |
| `python_type` | `pathlib.Path` |
| Arrow extension name | `"pathlib.Path"` (custom — created via `make_arrow_extension_type`) |
| Arrow storage type | `pa.large_string()` |
| Arrow extension metadata | `b"orcapod.builtin"` |
| `python_to_storage(path)` | `str(path)` |
| `storage_to_python(s)` | `Path(s)` |

`get_arrow_extension_type()` uses
`make_arrow_extension_type("pathlib.Path", pa.large_string(), b"orcapod.builtin")`
to obtain the class (called once), then returns a cached instance.

`get_polars_extension_type()` uses
`make_polars_extension_type("pathlib.Path", pa.large_string(), "orcapod.builtin")`
similarly.

### `LogicalUPath`

Identical structure to `LogicalPath` with:

| Property / Method | Value |
|---|---|
| `logical_type_name` | `"upath.UPath"` |
| `python_type` | `upath.UPath` |
| Arrow extension name | `"upath.UPath"` |
| `python_to_storage(upath)` | `str(upath)` |
| `storage_to_python(s)` | `UPath(s)` |

### `LogicalUUID`

| Property / Method | Value |
|---|---|
| `logical_type_name` | `"uuid.UUID"` |
| `python_type` | `uuid.UUID` |
| Arrow extension name | `"uuid.UUID"` (custom — created via `make_arrow_extension_type`) |
| Arrow storage type | `pa.large_binary()` |
| Arrow extension metadata | `None` (empty bytes) |
| `python_to_storage(uuid_val)` | `uuid_val.bytes` |
| `storage_to_python(bytes_val)` | `uuid.UUID(bytes=bytes(bytes_val))` |

`get_arrow_extension_type()` uses
`make_arrow_extension_type("uuid.UUID", pa.large_binary())`, following the
same pattern as `LogicalPath` and `LogicalUPath`. `logical_type_name` and the
Arrow extension name are both `"uuid.UUID"`.

`pa.large_binary()` is used rather than `pa.binary(16)` (fixed-size) because
Polars maps fixed-size binary to variable-length on the round-trip, which
would conflict with the deserializer's storage-type check.

PyArrow's built-in `pa.uuid()` (`"arrow.uuid"`) is intentionally **not** used:
it is a C++ built-in type (`UuidType(BaseExtensionType)`) that Polars has
hardcoded in its Rust layer at startup and cannot be overridden from Python,
causing Arrow → Polars → Arrow round-trips to silently strip the extension.

`get_polars_extension_type()` uses
`make_polars_extension_type("uuid.UUID", pa.large_binary())`.

### Caching strategy

Each class caches its Arrow and Polars extension type instances as class-level
attributes to avoid re-creating dynamic subclasses on every `get_*` call:

```python
class LogicalPath:
    _arrow_ext_class = make_arrow_extension_type("pathlib.Path", pa.large_string())
    _arrow_ext: pa.ExtensionType | None = None

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        if LogicalPath._arrow_ext is None:
            LogicalPath._arrow_ext = LogicalPath._arrow_ext_class()
        return LogicalPath._arrow_ext
```

Imports inside `builtin_logical_types.py` must use direct submodule paths
(e.g. `from orcapod.extension_types.registry import make_arrow_extension_type`),
not the package `__init__` (`from orcapod.extension_types import ...`), to avoid
a circular import when the context system loads this module.

---

## New helper: `make_polars_extension_type` in `registry.py`

Add alongside the existing `make_arrow_extension_type`:

```python
def make_polars_extension_type(
    extension_name: str,
    arrow_storage_type: pa.DataType,
    metadata: str | None = None,
) -> type[pl.BaseExtension]:
    """Synthesise and return a ``pl.BaseExtension`` subclass.

    Derives the Polars storage dtype from *arrow_storage_type* via
    ``pl.from_arrow``. Returns the *class*; callers instantiate it inside
    ``get_polars_extension_type()``.
    """
```

Polars dtype is computed once via
`pl.from_arrow(pa.array([], type=arrow_storage_type)).dtype` and captured
in the closure, mirroring the `make_arrow_extension_type` pattern.

Export `make_polars_extension_type` from `__init__.py` alongside
`make_arrow_extension_type`.

---

## `LogicalTypeRegistry.__init__` — add `logical_types` parameter

Small backward-compatible addition to `registry.py` so that
`parse_objectspec()` can populate the registry via `_config`:

```python
def __init__(self, logical_types: list[LogicalType] | None = None) -> None:
    self._by_logical_name: dict[str, LogicalType] = {}
    self._by_arrow_name: dict[str, LogicalType] = {}
    self._by_python_type: dict[type, LogicalType] = {}
    for lt in (logical_types or []):
        self.register(lt)
```

Same pattern as `SemanticTypeRegistry`'s `converters` constructor argument.

---

## `DataContext` — add `logical_type_registry` field

In `src/orcapod/contexts/core.py`, add field to the `DataContext` dataclass:

```python
from orcapod.extension_types.registry import LogicalTypeRegistry

@dataclass
class DataContext:
    ...
    logical_type_registry: LogicalTypeRegistry
```

---

## `v0.1.json` — add `logical_type_registry` entry

Add before the `"metadata"` key:

```json
"logical_type_registry": {
    "_class": "orcapod.extension_types.registry.LogicalTypeRegistry",
    "_config": {
        "logical_types": [
            {
                "_class": "orcapod.extension_types.builtin_logical_types.LogicalPath",
                "_config": {}
            },
            {
                "_class": "orcapod.extension_types.builtin_logical_types.LogicalUPath",
                "_config": {}
            },
            {
                "_class": "orcapod.extension_types.builtin_logical_types.LogicalUUID",
                "_config": {}
            }
        ]
    }
}
```

---

## `contexts/__init__.py` — add convenience accessor

Add alongside `get_default_type_converter()`:

```python
def get_default_logical_type_registry() -> LogicalTypeRegistry:
    """Get the default logical type registry.

    Returns:
        ``LogicalTypeRegistry`` instance from the default context.
    """
    return get_default_context().logical_type_registry
```

Add to `__all__`.

---

## `context_schema.json` — add `logical_type_registry`

Add `"logical_type_registry"` to the required/allowed fields in
`src/orcapod/contexts/data/schemas/context_schema.json`.

---

## `extension_types/__init__.py` — remove standalone default registry

**Remove** the line `default_logical_type_registry = LogicalTypeRegistry()`.

The standard access paths are now:
- `get_default_context().logical_type_registry`
- `get_default_logical_type_registry()` (from `orcapod.contexts`)

Removing the module-level variable avoids a circular import: if `__init__.py`
called `get_default_context()` at import time, it would force-eager-load all
context components (file hasher, semantic registry, arrow hasher, etc.) whenever
`orcapod.extension_types` is imported.

Update `__all__` accordingly.

---

## Tests: `tests/test_extension_types/test_builtin_logical_types.py`

### Protocol conformance
- `isinstance(LogicalPath(), LogicalType)` → `True` (and `LogicalUPath`, `LogicalUUID`)

### Property values
- `logical_type_name`, `python_type` correct for each class
- `get_arrow_extension_type().extension_name` returns expected Arrow ext name
- UUID: `get_arrow_extension_type().extension_name == "arrow.uuid"` (not `"uuid.UUID"`)

### Conversion round-trips
- `Path`: `storage_to_python(python_to_storage(Path("/tmp/foo"))) == Path("/tmp/foo")`
- `UPath`: `storage_to_python(python_to_storage(UPath("s3://bucket/key"))) == UPath("s3://bucket/key")`
- `UUID`: `storage_to_python(python_to_storage(some_uuid)) == some_uuid`

### Default context registration
After `from orcapod.contexts import get_default_context`:
- `get_default_context().logical_type_registry.get_by_logical_name("pathlib.Path")` → `LogicalPath`
- `get_default_context().logical_type_registry.get_by_python_type(Path)` → `LogicalPath`
- `get_default_context().logical_type_registry.get_by_arrow_extension_name("pathlib.Path")` → `LogicalPath`
- Same pattern for UPath
- `get_default_context().logical_type_registry.get_by_logical_name("uuid.UUID")` → `LogicalUUID`
- `get_default_context().logical_type_registry.get_by_arrow_extension_name("arrow.uuid")` → `LogicalUUID`

### Pre-existing Arrow type tolerance
- Registering `LogicalUUID` succeeds even though `pa.uuid()` (`"arrow.uuid"`) is already registered in PyArrow

### Idempotence
- Calling `get_default_context()` twice returns the same `LogicalTypeRegistry` instance (context caching)

---

## Summary of files changed

| File | Change |
|---|---|
| `src/orcapod/extension_types/builtin_logical_types.py` | **New** — three `LogicalType` implementations |
| `src/orcapod/extension_types/registry.py` | Add `make_polars_extension_type` helper; add `logical_types` param to `LogicalTypeRegistry.__init__` |
| `src/orcapod/extension_types/__init__.py` | Remove `default_logical_type_registry`; export `make_polars_extension_type` |
| `src/orcapod/contexts/core.py` | Add `logical_type_registry: LogicalTypeRegistry` to `DataContext` |
| `src/orcapod/contexts/data/v0.1.json` | Add `logical_type_registry` entry |
| `src/orcapod/contexts/data/schemas/context_schema.json` | Add `logical_type_registry` to schema |
| `src/orcapod/contexts/__init__.py` | Add `get_default_logical_type_registry()` |
| `tests/test_extension_types/test_builtin_logical_types.py` | **New** — tests |

---

## Scope boundaries

**In scope (this issue):**
- `builtin_logical_types.py` with three `LogicalType` implementations
- `make_polars_extension_type` helper in `registry.py`
- `logical_types` constructor param in `LogicalTypeRegistry`
- `DataContext.logical_type_registry` field + `v0.1.json` entry + schema update
- `get_default_logical_type_registry()` in `contexts`
- Tests in `test_builtin_logical_types.py`

**Out of scope (deferred to PLT-1660):**
- Deleting `PythonPathStructConverter`, `UPathStructConverter`, `UUIDStructConverter`
- Using `logical_type_registry` inside `DataContext`'s other components
  (e.g. replacing `UniversalTypeConverter`'s semantic registry lookup)
- File hashing — remains exclusively in `PathContentHandler` / `UPathContentHandler`
