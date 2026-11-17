# Pydantic Logical Type Factory Design

**Issue:** PLT-1731  
**Date:** 2026-06-17  
**Branch:** `eywalker/plt-1731-implement-pydantic-logical-type-factory-on-refined`

---

## Overview

Implement `PydanticLogicalType` and `PydanticLogicalTypeFactory` for pydantic v2 `BaseModel`
subclasses. The factory follows the same thin-leaf pattern established by
`DataclassLogicalTypeFactory` (PLT-1705): it synthesises one logical type per supported class,
delegates all field-type resolution to the converter via `register_python_class`, and stores
field annotations so that value conversion flows back through the converter at runtime.

The two factories are fully independent — `PydanticLogicalTypeFactory` has no dependency on
`dataclass_logical_type_factory.py` and vice versa.

---

## Goals & Success Criteria

- `PydanticLogicalTypeFactory` implements `LogicalTypeFactoryProtocol` (write path +
  read path).
- `PydanticLogicalType` implements `LogicalTypeProtocol`.
- For each model field, schema derivation and value conversion flow through the converter
  re-entry points — no annotation traversal inside the factory.
- No coupling to `LogicalTypeRegistry` or cycle-detection internals.
- Pydantic is an optional dependency; the factory is importable and gracefully returns
  `False` from `supports_class` when pydantic is not installed.
- All tests pass; a full Parquet round-trip test demonstrates end-to-end correctness.

---

## Scope & Boundaries

In scope:
- `PydanticLogicalType` and `PydanticLogicalTypeFactory` in a new
  `src/orcapod/extension_types/pydantic_logical_type_factory.py`.
- Refactoring the FQCN walk loop into `type_utils._walk_fqcn` to avoid duplication.
- `pyproject.toml`: add `pydantic = ["pydantic>=2.0"]` optional extra; add to `all`.
- `extension_types/__init__.py`: export the new symbols.
- Test file `tests/test_extension_types/test_pydantic_logical_type_factory.py`.

Out of scope:
- Wiring `PydanticLogicalTypeFactory` into the default `DataContext` / context JSON
  (separate issue).
- Pydantic v1 support.
- Pydantic computed fields (`model_computed_fields`) — these are derived and not stored.
- Pydantic private attributes (`PrivateAttr`) — always have defaults; not stored.
- Pydantic model validators or field validators affecting storage values.
- Nested extension types inside list value fields (ET2 gap, tracked separately).

---

## Architecture

### New file: `pydantic_logical_type_factory.py`

```
src/orcapod/extension_types/pydantic_logical_type_factory.py
```

Contains:

- `PYDANTIC_CATEGORY = "orcapod.pydantic"` — category tag embedded in Arrow extension
  metadata; used as the factory dispatch key on the read path.
- `PydanticLogicalType` — logical type binding a pydantic `BaseModel` subclass to its
  Arrow extension type representation.
- `PydanticLogicalTypeFactory` — stateless factory that synthesises and reconstructs
  `PydanticLogicalType` instances.
- `_import_pydantic_model_from_fqcn(fqcn)` — private import helper; calls
  `type_utils._walk_fqcn` then validates the resolved object is a `BaseModel` subclass.

### `PydanticLogicalType`

Constructor arguments:

| Parameter | Type | Description |
|---|---|---|
| `logical_name` | `str` | FQCN; used as logical type name and Arrow extension name |
| `python_type` | `type` | The `BaseModel` subclass |
| `storage_type` | `pa.StructType` | Arrow struct of model fields |
| `field_annotations` | `list[tuple[str, Any]]` | Ordered `(field_name, annotation)` pairs |

**`python_to_storage(value, converter)`**

```python
{name: converter.python_to_storage(getattr(value, name), annotation)
 for name, annotation in self._field_annotations}
```

**`storage_to_python(storage_value, converter)`**

```python
kwargs = {name: converter.storage_to_python(storage_value[name], annotation)
          for name, annotation in self._field_annotations}
return self._python_type(**kwargs)
```

Calling `python_type(**kwargs)` triggers full pydantic validation on reconstruction,
ensuring the model is always in a valid state.

Arrow/Polars extension types are created via `make_arrow_extension_type` /
`make_polars_extension_type` with
`metadata = json.dumps({"category": PYDANTIC_CATEGORY}).encode("utf-8")`.

Both conversion methods raise `ValueError` when `converter is None`.

### `PydanticLogicalTypeFactory`

**`supports_class(python_type)`**

```python
try:
    from pydantic import BaseModel
except ImportError:
    return False
return isinstance(python_type, type) and issubclass(python_type, BaseModel)
```

Gracefully returns `False` if pydantic is not installed. The `try/except` is inside the
method rather than at module level so the factory module is importable regardless.

**`create_for_python_type(python_type, converter)` — write path**

1. Derive FQCN as `f"{python_type.__module__}.{python_type.__qualname__}"`.
2. Reject local classes (`"<locals>"` in FQCN) with `ValueError`.
3. Call `typing.get_type_hints(python_type)` to resolve annotations (handles forward refs).
4. Iterate `python_type.model_fields` (pydantic v2 API) — this is the authoritative set of
   stored fields. Computed fields and private attributes are automatically excluded.
5. For each field: `arrow_type = converter.register_python_class(annotation)`. Strip any
   top-level `pa.ExtensionType` before inserting into the struct (ET1 constraint: struct
   fields must never contain nested extension types).
6. Return `PydanticLogicalType(fqcn, python_type, pa.struct(arrow_fields), field_annotations)`.

**`reconstruct_from_arrow(arrow_extension_name, storage_type, metadata, converter)` — read path**

1. Validate `storage_type` is a struct; raise `ValueError` otherwise.
2. Import class from FQCN via `_import_pydantic_model_from_fqcn`; raises `ImportError` if
   not found or not a `BaseModel` subclass.
3. Call `typing.get_type_hints(cls)` and iterate `cls.model_fields` to recover
   `field_annotations`.
4. Call `converter.register_python_class(annotation)` per field — registration completeness
   invariant: all nested logical types must be registered when the outer type is registered.
5. Return `PydanticLogicalType(arrow_extension_name, cls, storage_type, field_annotations)`.

### FQCN import refactoring

`type_utils._walk_fqcn(fqcn: str) -> Any` performs the module-prefix walk and attribute
chain traversal, returning the raw resolved object without type validation. Both
`dataclass_logical_type_factory._import_from_fqcn` and
`pydantic_logical_type_factory._import_pydantic_model_from_fqcn` call `_walk_fqcn` and
apply their own type validation on top. The ~25-line walk loop is written once.

### Registration

```python
from pydantic import BaseModel
from orcapod.extension_types.pydantic_logical_type_factory import (
    PydanticLogicalTypeFactory, PYDANTIC_CATEGORY
)
converter.register_logical_type_factory(
    PydanticLogicalTypeFactory(),
    category=PYDANTIC_CATEGORY,
    python_bases=[BaseModel],
)
```

`python_bases=[BaseModel]` ensures MRO dispatch only probes this factory for classes that
actually inherit from `BaseModel`, rather than every class in the system.

---

## Dependency changes

**`pyproject.toml`:**

```toml
[project.optional-dependencies]
# existing entries ...
pydantic = ["pydantic>=2.0"]
all = ["orcapod[redis]", "orcapod[ray]", "orcapod[postgresql]", "orcapod[spiraldb]", "orcapod[pydantic]"]
```

---

## Files changed

| File | Change |
|---|---|
| `src/orcapod/extension_types/pydantic_logical_type_factory.py` | New — `PYDANTIC_CATEGORY`, `PydanticLogicalType`, `PydanticLogicalTypeFactory`, `_import_pydantic_model_from_fqcn` |
| `src/orcapod/extension_types/__init__.py` | Export `PYDANTIC_CATEGORY`, `PydanticLogicalType`, `PydanticLogicalTypeFactory` |
| `src/orcapod/extension_types/type_utils.py` | Add `_walk_fqcn` shared helper |
| `src/orcapod/extension_types/dataclass_logical_type_factory.py` | `_import_from_fqcn` delegates to `type_utils._walk_fqcn` |
| `pyproject.toml` | Add `pydantic` optional extra; add to `all` |
| `tests/test_extension_types/test_pydantic_logical_type_factory.py` | New — full test suite |

---

## Test plan

All module-level pydantic models used in tests that require FQCN reconstruction are
defined at module scope (not inside test functions), consistent with
`test_dataclass_logical_type_factory.py`.

| Test | What it checks |
|---|---|
| `test_pydantic_logical_type_is_importable` | Module-level smoke test |
| `test_pydantic_logical_type_protocol_conformance` | `isinstance(lt, LogicalTypeProtocol)` |
| `test_pydantic_logical_type_python_to_storage` | `getattr`-based dict output |
| `test_pydantic_logical_type_storage_to_python` | `python_type(**kwargs)` reconstruction |
| `test_pydantic_logical_type_logical_type_name` | FQCN stored correctly |
| `test_pydantic_logical_type_python_type` | `.python_type` property |
| `test_factory_supports_class_pydantic_model` | Returns `True` for `BaseModel` subclass |
| `test_factory_supports_class_non_pydantic` | Returns `False` for `str`, `int`, plain dataclass |
| `test_factory_create_flat_model` | Arrow struct with correct field types |
| `test_factory_create_model_with_uuid_field` | UUID field stripped to `large_binary` in struct (ET1) |
| `test_factory_create_model_with_list_field` | `list[str]` → `pa.large_list(pa.large_string())` |
| `test_factory_create_model_with_dict_field` | `dict[str, int]` → `list[struct{key, value}]` |
| `test_factory_rejects_local_class` | `ValueError` with `"local"` in message |
| `test_factory_reconstruct_from_arrow` | Read path rebuilds correct `PydanticLogicalType` |
| `test_factory_reconstruct_from_arrow_invalid_fqcn` | `ImportError` on bad FQCN |
| `test_reconstruct_from_arrow_registers_nested_types` | Nested model registered as side effect |
| `test_pydantic_python_to_storage_round_trip` | `python_to_storage` → `storage_to_python` → equivalent model |
| `test_pydantic_with_uuid_round_trip` | UUID field survives round-trip |
| `test_python_to_storage_raises_when_converter_none` | `ValueError` guard |
| `test_storage_to_python_raises_when_converter_none` | `ValueError` guard |
| `test_nested_pydantic_model_parquet_roundtrip` | Full Parquet write → fresh-converter read |
| `test_private_fields_not_stored` | Model with `PrivateAttr` — private field absent from Arrow struct |
