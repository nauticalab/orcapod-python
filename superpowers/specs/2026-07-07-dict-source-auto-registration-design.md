# DictSource Auto-Registration of Pydantic / Dataclass Types

**Issue:** ITL-446
**Date:** 2026-07-07
**Status:** approved

---

## Overview

When a `DictSource` is created with Pydantic model or `@dataclass` instances as column
values, the universal converter raises `ValueError("Unsupported Python type: ...")` because
the type has never been registered in the `LogicalTypeRegistry`. Function pods avoid this by
calling `ensure_types_registered_for_schemas` eagerly at construction time; `DictSource` has
no equivalent hook, so unregistered types fall through to the fatal error path in
`_convert_python_to_arrow`.

The fix is to add the same pre-flight registration call inside
`python_dicts_to_arrow_table`, immediately after the Python schema is known and before
`python_schema_to_arrow_schema` converts it to Arrow.

---

## Design

### Single code change

**File:** `src/orcapod/semantic_types/universal_converter.py`
**Method:** `python_dicts_to_arrow_table`

Inside the `if arrow_schema is None:` branch, add one call to
`ensure_types_registered_for_schemas` before the schema is converted:

```python
if arrow_schema is None:
    assert python_schema is not None, "Python schema should not be None here"
    self.ensure_types_registered_for_schemas(python_schema)  # auto-register Pydantic/dataclass
    arrow_schema = self.python_schema_to_arrow_schema(python_schema)
```

This is the only source file that changes.

### Why this is the right place

`python_dicts_to_arrow_table` is the single entry point for converting Python dict
collections into Arrow tables. It is called by:

- `DictSource.__init__`
- Any other code that builds an Arrow table from Python dicts via the type converter

Inserting the registration call here covers all callers automatically, mirrors the
function-pod pattern exactly, and keeps `DictSource` itself free of type-system concerns.

### Behaviour of `ensure_types_registered_for_schemas`

`ensure_types_registered_for_schemas` already handles every case that `python_schema` can
contain:

| Type in schema | Behaviour |
|---|---|
| Primitive (`int`, `str`, `float`, …) | No-op — primitive map hit, no factory needed |
| Already-registered logical type | No-op — idempotent registry check |
| `Optional[T]` / `T \| None` | Unwraps to `T`, then registers `T` |
| Pydantic `BaseModel` subclass | Dispatches to `PydanticLogicalTypeFactory`, registers |
| `@dataclass` class | Dispatches to `DataclassLogicalTypeFactory`, registers |

Registration is:
- **Idempotent** — calling it twice with the same type is safe.
- **Thread-safe** — cycle detection uses a `ContextVar` (`_register_in_progress`), not
  instance state, so concurrent or async callers do not interfere.
- **No-op without a registry** — if `self._logical_type_registry is None`,
  `ensure_types_registered_for_schemas` returns immediately.

### What does not change

- The `if isinstance(data_schema, pa.Schema):` branch in `DictSource.__init__` is
  unaffected — when an Arrow schema is provided directly, schema inference and Python-type
  registration are not needed.
- Function-pod eager registration (`_FunctionPodBase.__init__` calling
  `ensure_types_registered_for_schemas`) is not modified.
- `_convert_python_to_arrow` is not modified — it remains a pure read-from-registry
  function.
- `LogicalTypeRegistry` and its factory dispatch are not modified.

---

## Tests

**New file:** `tests/test_core/sources/test_dict_source_auto_registration.py`

### Required test cases

| # | Name | Description |
|---|---|---|
| 1 | `test_dict_source_pydantic_model_no_prior_registration` | Build a `DictSource` with a fresh `BaseModel` subclass (never used in a pod). Construct succeeds; `iter_data()` or `as_table()` returns the correct rows. |
| 2 | `test_dict_source_dataclass_no_prior_registration` | Same as #1 for a `@dataclass` class. |
| 3 | `test_dict_source_pydantic_no_double_registration_error` | Create two `DictSource`s with the same `BaseModel` type in sequence. Second construction does not raise. |
| 4 | `test_dict_source_pod_registration_not_broken` | Create a function pod with a type, then create a `DictSource` with the same type. No error; type converter state is consistent. |

Tests use the default `DataContext` (via `get_default_context()`) so they exercise the full
factory chain.
