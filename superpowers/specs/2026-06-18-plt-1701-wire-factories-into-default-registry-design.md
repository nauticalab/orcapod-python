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

Wiring them in requires two things:

1. `LogicalTypeRegistry.__init__` must accept a `factories` parameter so the JSON object-spec
   config can specify factory registrations alongside the existing `logical_types` list.
2. `register_logical_type_factory` must accept string FQCNs in `python_bases` (resolved lazily
   via `_walk_fqcn`) so that optional dependencies like pydantic do not get imported at
   context-load time when they are not installed.

---

## Goals & Success Criteria

- `LogicalTypeRegistry.__init__` accepts an optional `factories` parameter: a list of dicts,
  each with keys `factory` (instance), `category` (string), `python_bases` (list of
  `type | str`).
- `register_logical_type_factory` resolves string FQCNs in `python_bases` using
  `type_utils._walk_fqcn`. On `ImportError`, logs a `WARNING` and skips that base — category
  registration still proceeds, preserving the read path.
- `v0.1.json` wires in both factories under `logical_type_registry._config.factories`.
- The default context automatically handles dataclass-annotated pod fields (write path) and
  reconstructs dataclass columns from Parquet/Delta (read path) with zero user-side setup.
- Pydantic factory's category is always registered (read path works regardless of whether
  pydantic is installed). Write-path base registration is skipped gracefully if pydantic is
  absent.
- Instantiating `PydanticLogicalTypeFactory()` does NOT import pydantic.
- Existing tests pass. New tests explicitly verify all of the above.

---

## Scope & Boundaries

**In scope:**
- `LogicalTypeRegistry.__init__` `factories` parameter
- `register_logical_type_factory` string-FQCN support via `_walk_fqcn`
- `v0.1.json` update (both factories)
- Unit tests: registry `factories` param, mock-based graceful failure, pydantic import safety
- Integration tests: default context factory registration + FunctionPod/converter end-to-end
  with dataclass and pydantic types

**Out of scope:**
- Changes to `DataclassLogicalTypeFactory` or `PydanticLogicalTypeFactory` logic
- Changes to `parse_objectspec`, `contexts/core.py`, or `contexts/registry.py`
- Picklable or other factory types — those wire in separately
- `context_schema.json` — `_config` already uses `"additionalProperties": true`

---

## Design

### 1. `extension_types/registry.py`

#### `register_logical_type_factory` — accept `type | str` in `python_bases`

Change the signature's accepted types from `Iterable[type]` to `Iterable[type | str]`.
At the top of the method, iterate `python_bases` and resolve each entry:

- If the entry is already a `type`, use it directly.
- If the entry is a `str`, call `type_utils._walk_fqcn(fqcn)` inside a `try/except ImportError`.
  On success, use the resolved type. On `ImportError`, emit `logger.warning(...)` and skip
  that base entry. The `category` registration still proceeds — the read path works even
  without the optional dep.

This reuses the existing `_walk_fqcn` helper, which already handles dotted attribute walks
for nested classes. No raw `importlib` wrangling needed.

#### `__init__` — add `factories` parameter

```python
def __init__(
    self,
    logical_types: list[LogicalTypeProtocol] | None = None,
    factories: list[dict] | None = None,
) -> None:
```

After registering `logical_types`, iterate `factories` (if any). Each dict must have:

| Key | Type | Description |
|---|---|---|
| `factory` | `LogicalTypeFactoryProtocol` | Factory instance |
| `category` | `str \| None` | Category key for read-path dispatch (optional) |
| `python_bases` | `list[type \| str]` | Base classes for write-path dispatch (optional) |

Call `self.register_logical_type_factory(factory, category=category, python_bases=python_bases)`
for each entry.

### 2. `contexts/data/v0.1.json`

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
    "python_bases": ["pydantic.BaseModel"]
  }
]
```

**Dataclass entry:** `python_bases` uses `{"_type": "builtins.object"}` — `parse_objectspec`
resolves this to `object` (always importable), so the registry receives an actual `type`.

**Pydantic entry:** `python_bases` uses the plain string `"pydantic.BaseModel"` — `parse_objectspec`
passes strings through unchanged (they are primitives). The registry then resolves it via
`_walk_fqcn` at registration time, catching `ImportError` if pydantic is absent. This avoids
a hard failure at context-load time even when pydantic is not installed.

### 3. `PydanticLogicalTypeFactory` — no code changes needed

The current `PydanticLogicalTypeFactory` has no explicit `__init__` and no module-level
pydantic import. Instantiation is already pydantic-free. A test will lock this invariant.

---

## Test Plan

### New test file: `tests/test_extension_types/test_default_context_factories.py`

**Registry constructor tests (unit):**

- `test_registry_factories_param_registers_dataclass_factory` — construct a `LogicalTypeRegistry`
  with `factories=[{"factory": DataclassLogicalTypeFactory(), "category": "orcapod.dataclass", "python_bases": [object]}]`
  and verify the category factory is accessible.
- `test_registry_factories_param_registers_pydantic_factory` — same for pydantic with
  `python_bases=["pydantic.BaseModel"]`.
- `test_registry_string_python_base_graceful_on_missing_dep` — use
  `unittest.mock.patch.dict(sys.modules, {"pydantic": None})` to simulate pydantic being
  absent; register with `python_bases=["pydantic.BaseModel"]`; verify no exception is raised
  and that the category is still registered, but the write-path base is not.

**Pydantic import-safety test (unit):**

- `test_pydantic_factory_instantiation_does_not_import_pydantic` — record `sys.modules` keys
  before and after `PydanticLogicalTypeFactory()`; assert no pydantic-related module was
  imported.

**Default context integration tests:**

- `test_default_context_has_dataclass_factory_registered` — `resolve_context()` creates a
  fresh context; verify `_category_factories["orcapod.dataclass"]` is a
  `DataclassLogicalTypeFactory`.
- `test_default_context_has_pydantic_factory_registered` — same for `"orcapod.pydantic"`.
- `test_default_context_dataclass_type_is_auto_registered_on_use` — use
  `get_default_type_converter().register_python_class(SomeModuleLevelDataclass)` with no
  prior manual setup; verify the returned Arrow type is an extension type with the correct
  extension name.
- `test_default_context_pydantic_model_type_is_auto_registered_on_use` — same for a pydantic
  `BaseModel` subclass (skipped if pydantic not installed).
- `test_default_context_dataclass_parquet_roundtrip` — full end-to-end: write a dataclass
  column via a fresh default-context converter to Parquet; read it back with another fresh
  default-context converter using `register_discovered_extensions` + `apply_extension_types`;
  verify the reconstructed Python object matches the original.

**Note on context freshness:** Tests that mutate registry state (registering new types) must
use `create_registry()` to get a fresh `JSONDataContextRegistry` instance rather than the
global singleton, to avoid cross-test contamination.

---

## Dependencies

- `_walk_fqcn` from `orcapod.extension_types.type_utils` (existing, private helper)
- `DataclassLogicalTypeFactory` (PLT-1657/PLT-1705, already on `extension-type-system`)
- `PydanticLogicalTypeFactory` (PLT-1731, already on `extension-type-system`)
