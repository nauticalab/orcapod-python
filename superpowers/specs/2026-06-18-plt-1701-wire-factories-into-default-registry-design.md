# PLT-1701: Wire DataclassHandlerFactory and PydanticLogicalTypeFactory into the Default LogicalTypeRegistry

**Date:** 2026-06-18
**Issue:** PLT-1701
**Branch:** eywalker/plt-1701-wire-dataclasshandlerfactory-into-the-default

---

## Overview

`DataclassLogicalTypeFactory` and `PydanticLogicalTypeFactory` are fully implemented but must
be manually registered by users on their `LogicalTypeRegistry` instances. Until they are
wired into the default context, dataclass- and pydantic-annotated pod fields are not
auto-handled out of the box.

Wiring them in requires one registry change: `LogicalTypeRegistry.__init__` must accept a
`factories` parameter so the JSON object-spec config can specify factory registrations
alongside the existing `logical_types` list.

Pydantic is promoted to an explicit (non-optional) orcapod dependency. This removes all
graceful-import logic and makes missing pydantic a hard failure — both at context-load time
and inside the factory itself.

---

## Goals & Success Criteria

- `LogicalTypeRegistry.__init__` accepts an optional `factories` parameter: a list of dicts,
  each with keys `factory` (instance), `category` (string), `python_bases` (list of `type`).
- `v0.1.json` wires in both factories under `logical_type_registry._config.factories`, using
  `{"_type": "..."}` object-specs for `python_bases` — resolved by `parse_objectspec` at
  context-load time exactly as other types are today.
- Pydantic is listed as a required dependency in `pyproject.toml`.
- `PydanticLogicalTypeFactory.supports_class` drops its `try/except ImportError` guard and
  imports pydantic directly.
- The default context automatically handles dataclass- and pydantic-annotated pod fields
  (write path) and reconstructs such columns from Parquet/Delta (read path) with zero
  user-side setup.
- Existing tests pass. New tests explicitly verify factory registration and end-to-end use.

---

## Scope & Boundaries

**In scope:**
- `pyproject.toml` — pydantic added as explicit required dependency
- `LogicalTypeRegistry.__init__` `factories` parameter
- `PydanticLogicalTypeFactory.supports_class` — remove try/except, direct pydantic import
- `v0.1.json` update (both factories)
- Unit tests: registry `factories` param, pydantic import directness
- Integration tests: default context factory registration + converter end-to-end with
  dataclass and pydantic types, Parquet round-trip

**Out of scope:**
- String-FQCN support in `register_logical_type_factory` — not needed; `parse_objectspec`
  resolves `{"_type": "pydantic.BaseModel"}` directly
- Changes to `DataclassLogicalTypeFactory` logic
- Changes to `parse_objectspec`, `contexts/core.py`, or `contexts/registry.py`
- Picklable or other factory types — those wire in separately
- `context_schema.json` — `_config` already uses `"additionalProperties": true`

---

## Design

### 1. `pyproject.toml`

Add `pydantic>=2.0` (or the currently pinned version) to the `[project.dependencies]` list.
Remove any `[project.optional-dependencies]` entry for pydantic if one exists.

### 2. `extension_types/pydantic_logical_type_factory.py`

`supports_class` currently wraps its `from pydantic import BaseModel` in a `try/except
ImportError` that returns `False` when pydantic is absent. Drop the guard:

```python
def supports_class(self, python_type: type) -> bool:
    from pydantic import BaseModel
    return isinstance(python_type, type) and issubclass(python_type, BaseModel)
```

No other changes to this file.

### 3. `extension_types/registry.py`

#### `__init__` — add `factories` parameter

```python
def __init__(
    self,
    logical_types: list[LogicalTypeProtocol] | None = None,
    factories: list[dict] | None = None,
) -> None:
```

After registering `logical_types`, iterate `factories` (if any). Each dict has:

| Key | Type | Required | Description |
|---|---|---|---|
| `factory` | `LogicalTypeFactoryProtocol` | yes | Factory instance |
| `category` | `str` | no | Category key for read-path dispatch |
| `python_bases` | `list[type]` | no | Base classes for write-path dispatch |

Call `self.register_logical_type_factory(factory, category=..., python_bases=...)` for each.

No changes to `register_logical_type_factory` itself — it already accepts `Iterable[type]`.

### 4. `contexts/data/v0.1.json`

Add a `"factories"` list to `type_converter → _config → logical_type_registry → _config`:

```json
"factories": [
  {
    "factory": {
      "_class": "orcapod.extension_types.dataclass_logical_type_factory.DataclassLogicalTypeFactory",
      "_config": {}
    },
    "category": "orcapod.dataclass",
    "python_bases": [{"_type": "builtins.object"}]
  },
  {
    "factory": {
      "_class": "orcapod.extension_types.pydantic_logical_type_factory.PydanticLogicalTypeFactory",
      "_config": {}
    },
    "category": "orcapod.pydantic",
    "python_bases": [{"_type": "pydantic.BaseModel"}]
  }
]
```

`parse_objectspec` resolves `{"_type": "builtins.object"}` → `object` and
`{"_type": "pydantic.BaseModel"}` → `BaseModel`. Both arrive in `__init__` as actual
`type` objects — no special handling needed in the registry.

---

## Test Plan

### `tests/test_extension_types/test_default_context_factories.py` (new file)

**Registry constructor unit tests:**

- `test_registry_factories_param_registers_category_factory` — construct
  `LogicalTypeRegistry(factories=[{"factory": DataclassLogicalTypeFactory(), "category": "orcapod.dataclass", "python_bases": [object]}])`
  and assert the category factory is accessible via `_category_factories`.
- `test_registry_factories_param_registers_python_base_factory` — same shape, verify
  `_python_class_factories[object]` is set.
- `test_registry_factories_param_empty_list_is_noop` — `LogicalTypeRegistry(factories=[])`
  succeeds without error.

**Default context integration tests:**

- `test_default_context_has_dataclass_factory_registered` — create a fresh registry via
  `create_registry()` and verify `_category_factories["orcapod.dataclass"]` is an instance
  of `DataclassLogicalTypeFactory`.
- `test_default_context_has_pydantic_factory_registered` — same for `"orcapod.pydantic"` /
  `PydanticLogicalTypeFactory`.
- `test_default_context_dataclass_auto_registered_on_use` — call
  `create_registry().get_context().type_converter.register_python_class(SomeModuleLevelDataclass)`
  with no prior manual setup; verify the returned Arrow type is an extension type with the
  correct extension name (the dataclass FQCN).
- `test_default_context_pydantic_model_auto_registered_on_use` — same for a pydantic
  `BaseModel` subclass.
- `test_default_context_dataclass_parquet_roundtrip` — full end-to-end: write a dataclass
  column via a fresh default-context converter to Parquet; read it back with another fresh
  default-context converter using `register_discovered_extensions` + `apply_extension_types`;
  verify the reconstructed Python object matches the original, with no manual factory
  registration calls anywhere in the test.

**Note on context freshness:** All integration tests use `create_registry().get_context()`
rather than `get_default_context()` to avoid cross-test contamination via the global
singleton cache.

---

## Dependencies

- `DataclassLogicalTypeFactory` (PLT-1705, already on `extension-type-system`)
- `PydanticLogicalTypeFactory` (PLT-1731, already on `extension-type-system`)
- `parse_objectspec` already handles `{"_type": "..."}` → no changes needed there
