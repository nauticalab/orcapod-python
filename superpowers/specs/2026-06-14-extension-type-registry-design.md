# ExtensionTypeRegistry Design

**Date:** 2026-06-14
**Linear issue:** PLT-1653
**Status:** Approved

---

## Overview

The `extension_types/` subpackage has a protocol (`ExtensionTypeConverter`) but no registry.
This spec adds `ExtensionTypeRegistry` — a class that maps `extension_name` strings to converter
instances and, as a side effect of each `register()` call, populates both PyArrow's and Polars'
process-global extension type registries so that columns using these types round-trip correctly
through Arrow IPC, Parquet, and Polars DataFrames.

---

## Goals & Success Criteria

- `ExtensionTypeRegistry.register(converter)` stores the converter and registers the extension
  type in both PyArrow and Polars global registries in a single call.
- Registering a converter with a duplicate `extension_name` raises a clear `ValueError`.
- Converters are retrievable by `extension_name` (primary lookup) or `python_type` (secondary,
  for the write path). Subclass relationships are honoured in the python-type lookup.
- A module-level `extension_type_registry` instance is created when
  `orcapod.extension_types` is imported. It starts empty; PLT-1656 adds the built-in
  registrations (`Path`, `UPath`, `UUID`).
- `pyproject.toml` is updated from `polars>=1.31.0` to `polars>=1.36.0`, the minimum version
  that ships `pl.BaseExtension` and `pl.register_extension_type`.

---

## Architecture

### File map

| File | Change |
|---|---|
| `pyproject.toml` | Update `polars>=1.31.0` → `polars>=1.36.0` |
| `src/orcapod/extension_types/registry.py` | **New** — `ExtensionTypeRegistry` class + private helpers |
| `src/orcapod/extension_types/__init__.py` | Export `ExtensionTypeRegistry`; create `extension_type_registry` |
| `tests/test_extension_types/test_registry.py` | **New** — unit and integration tests |

---

## `registry.py` Module

### Internal storage

```python
self._by_name: dict[str, ExtensionTypeConverter]
self._by_python_type: dict[type, ExtensionTypeConverter]
```

Both dicts are populated together on every `register()` call. Neither has a reverse mapping
(no need to look up `extension_name` from `python_type` — that path is not required by this
issue).

### Public API

```python
class ExtensionTypeRegistry:
    def register(self, converter: ExtensionTypeConverter) -> None
    def get_converter_for_name(self, name: str) -> ExtensionTypeConverter | None
    def get_converter_for_python_type(self, python_type: type) -> ExtensionTypeConverter | None
    def has_extension_name(self, name: str) -> bool
    def has_python_type(self, python_type: type) -> bool
    def list_extension_names(self) -> list[str]
    def list_python_types(self) -> list[type]
```

**`register(converter)`** — the only mutating method:

1. Look up `converter.extension_name` in `_by_name`. If found, raise:
   `ValueError: Extension type '{name}' is already registered.`
2. Store `_by_name[name] = converter` and `_by_python_type[converter.python_type] = converter`.
3. Call `_register_arrow_ext_type(converter)`.
4. Call `_register_polars_ext_type(converter)`.

**`get_converter_for_python_type(python_type)`** — exact match first, then `issubclass` scan.
Returns the first registered type for which `issubclass(python_type, registered_type)` is true.
If multiple registered types are superclasses of `python_type`, the one encountered first in
insertion order wins (Python 3.7+ dict ordering). Returns `None` if nothing matches.

All other public methods are straightforward dict lookups or list returns.

### Private helpers

**`_register_arrow_ext_type(converter)`**

Creates a `pa.ExtensionType` subclass dynamically using `type()` and calls
`pa.register_extension_type(instance)`. If PyArrow raises (type already registered globally),
the exception is silently suppressed — our registry-level duplicate check already ensures we
never re-register the same name intentionally. Suppressing the PyArrow error makes the registry
safe to use in test suites where the process-global PyArrow state persists across test cases.

```python
# Pseudocode for the dynamically created class
class _ArrowExt_<sanitized_name>(pa.ExtensionType):
    def __init__(self):
        pa.ExtensionType.__init__(self, converter.storage_type, converter.extension_name)

    def __arrow_ext_serialize__(self) -> bytes:
        return converter.extension_metadata or b""

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return cls()
```

Name sanitization: replace all non-alphanumeric characters with `_` (e.g.
`pathlib.Path` → `_ArrowExt_pathlib_Path`). This is cosmetic only (for `repr` and debugging);
PyArrow identifies the type by `extension_name`, not the class name.

**`_register_polars_ext_type(converter)`**

Derives the Polars storage dtype by creating an empty PyArrow array of the storage type and
converting it via `pl.from_arrow`:

```python
pl_storage = pl.from_arrow(pa.array([], type=converter.storage_type)).dtype
```

This is reliable, handles all Arrow → Polars type mappings (including `pa.large_utf8()` →
`pl.String`, `pa.binary(16)` → `pl.Binary`, `pa.struct(...)` → `pl.Struct({...})`), and
requires no manually-maintained mapping table.

Then creates a `pl.BaseExtension` subclass dynamically:

```python
# Pseudocode for the dynamically created class
class _PolarsExt_<sanitized_name>(pl.BaseExtension):
    def __init__(self):
        metadata_str = converter.extension_metadata.decode("utf-8") if converter.extension_metadata else None
        super().__init__(converter.extension_name, pl_storage, metadata_str)

    @classmethod
    def ext_from_params(cls, name, storage, metadata):
        return cls()
```

Calls `pl.register_extension_type(converter.extension_name, _PolarsExtType)`. If Polars raises
(already registered), silently suppressed for the same reason as PyArrow.

Note: `pl.BaseExtension` is marked unstable in Polars. The `polars>=1.36.0` constraint is a
forward commitment; if a future Polars release changes this API, the helpers in `registry.py`
are the only place to update.

---

## `__init__.py`

```python
from .registry import ExtensionTypeRegistry

extension_type_registry = ExtensionTypeRegistry()
# PLT-1656 adds: extension_type_registry.register(<PathConverter>), etc.

__all__ = ["ExtensionTypeRegistry", "extension_type_registry"]
```

The module-level `extension_type_registry` is the process default. It is not yet referenced by
`DataContext` (that wiring is PLT-1660).

---

## `pyproject.toml`

```toml
# Before
"polars>=1.31.0",

# After
"polars>=1.36.0",
```

Polars 1.36.0 is the first release that exports `pl.BaseExtension` and
`pl.register_extension_type`. The currently installed version in CI is 1.41.2.

---

## Error Handling

| Situation | Behaviour |
|---|---|
| Duplicate `extension_name` in `register()` | `ValueError` with the offending name |
| PyArrow already has the type globally | Silently suppressed (safe for re-import and test isolation) |
| Polars already has the type globally | Silently suppressed (same reason) |
| `get_converter_for_name` / `get_converter_for_python_type` miss | Returns `None` |
| Non-`ExtensionTypeConverter` passed to `register()` | `beartype` raises `BeartypeCallHintParamViolation` at the call site |

---

## Tests

File: `tests/test_extension_types/test_registry.py`

A `_StubConverter` factory (similar to the one in `test_protocols.py`) creates minimal
conforming `ExtensionTypeConverter` instances with `pa.large_utf8()` as `storage_type`. Each
test that touches the process-global PA/Polars registries uses a unique `extension_name` to
avoid cross-test interference (since those globals persist for the process lifetime).

| Test | What it verifies |
|---|---|
| `test_register_stores_converter` | `get_converter_for_name` returns the converter after `register()` |
| `test_register_populates_arrow_registry` | After `register()`, attempting to re-register the same name with PyArrow raises `pa.lib.ArrowKeyError` (proving it is registered) |
| `test_register_populates_polars_registry` | After `register()`, `pl.from_arrow(pa.array([...], type=ext_type_instance)).dtype` is a `pl.BaseExtension` instance |
| `test_register_duplicate_raises` | Second `register()` with same `extension_name` → `ValueError` |
| `test_get_converter_for_name_miss` | Unknown name returns `None` |
| `test_get_converter_for_python_type_exact` | Exact type lookup returns converter |
| `test_get_converter_for_python_type_subclass` | Subclass of registered type returns converter |
| `test_get_converter_for_python_type_miss` | Unrelated type returns `None` |
| `test_has_extension_name` | Returns `True` after register, `False` before |
| `test_has_python_type` | Returns `True` after register, `False` before |
| `test_list_extension_names` | Returns correct list of registered names |
| `test_list_python_types` | Returns correct list of registered types |
| `test_arrow_polars_round_trip` | PA ext array → `pl.from_arrow` → `to_arrow()` preserves extension type and values |
| `test_extension_type_registry_module_instance` | `extension_types.extension_type_registry` is an `ExtensionTypeRegistry` instance and starts empty |

---

## Out of Scope

- Registering built-in converters (`Path`, `UPath`, `UUID`) — that is PLT-1656.
- Wiring `extension_type_registry` into `DataContext` — that is PLT-1660.
- Schema analysis helpers (finding extension-type columns in a schema) — not needed until PLT-1660.
- Thread safety — registration is expected to happen at import time before any concurrent I/O.
