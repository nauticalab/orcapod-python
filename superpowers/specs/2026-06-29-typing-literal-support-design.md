# Design: `typing.Literal` support in `UniversalTypeConverter` (ITL-442)

**Date:** 2026-06-29
**Issue:** [ITL-442](https://linear.app/enigma-metamorphic/issue/ITL-442/)
**Status:** Approved

---

## Overview

Pydantic models whose fields use `typing.Literal[...]` (e.g. `method: Literal["a", "b"]`)
fail to register as pipeline columns with:

```
ValueError: Unsupported annotation: typing.Literal['a', 'b']
```

`Literal` is the standard pydantic pattern for enumerated string/int choices. The fix
maps `Literal[v1, v2, ...]` to the Arrow type of the values' Python type — identical to
how a plain `str`, `int`, etc. field would be stored.

---

## Root Cause

`_register_python_class_impl` in `universal_converter.py` has branches for `Optional`,
`list`, `set`, `dict`, and concrete classes, but no branch for `typing.Literal`. Any
`Literal[...]` annotation falls through to `raise ValueError("Unsupported annotation: …")`.

A second site, `_convert_python_to_arrow`, has the same gap. This matters because
`_create_python_to_arrow_converter` calls `python_type_to_arrow_type` internally, so value
serialization of `Literal` fields inside pydantic models also fails at runtime once type
registration would otherwise succeed.

---

## Approach

Option B: fix both affected sites with inline branches — no helper extraction.

The logic is identical at both sites (4–5 lines), simple enough that a shared helper
would be over-engineering.

---

## Changes

### `src/orcapod/semantic_types/universal_converter.py`

**1. `_register_python_class_impl`**

Insert a `Literal` branch after the `Union/Optional` branch and before the `list` branch:

```python
# typing.Literal[v1, v2, ...] → Arrow type of the literal values' type.
# None members are stripped (treat as optional/nullable); mixed non-None types raise.
if origin is typing.Literal:
    value_types = {type(a) for a in args if a is not None}
    if not value_types:
        raise ValueError(
            f"Literal[None] is not supported as an Arrow type. "
            f"Use Optional[T] to express nullability instead."
        )
    if len(value_types) != 1:
        raise ValueError(
            f"Mixed-type Literal is not supported: {annotation!r}. "
            f"All members must share one type (e.g. Literal['a', 'b'])."
        )
    return self.register_python_class(next(iter(value_types)))
```

**2. `_convert_python_to_arrow`**

Insert the same `Literal` branch after the `Union/Optional` `elif` and before the `set`
branch:

```python
elif origin is typing.Literal:
    value_types = {type(a) for a in args if a is not None}
    if not value_types:
        raise ValueError(
            f"Literal[None] is not supported as an Arrow type. "
            f"Use Optional[T] to express nullability instead."
        )
    if len(value_types) != 1:
        raise ValueError(
            f"Mixed-type Literal is not supported: {python_type!r}. "
            f"All members must share one type (e.g. Literal['a', 'b'])."
        )
    return self.python_type_to_arrow_type(next(iter(value_types)))
```

No changes to `_create_python_to_arrow_converter` — the existing `else: return lambda
value: value` passthrough handles `Literal` scalars correctly once `_convert_python_to_arrow`
no longer raises.

---

## Data Flow

`Literal` values are plain scalars of the underlying type. No new conversion logic is
needed. The full round-trip for `method: Literal["a", "b"]`:

```
register_python_class(Literal["a", "b"])
  → new branch: value_types = {str}
  → recurse: register_python_class(str) → pa.large_string()

python_to_storage(instance, Literal["a", "b"])
  → python_type_to_arrow_type(Literal["a", "b"]) → pa.large_string()  [fixed]
  → passthrough: "a" → "a"

storage_to_python("a", Literal["a", "b"])
  → arrow_type = large_string → str passthrough → "a"
```

---

## Error Handling

| Input | Result |
|---|---|
| `Literal["a", "b"]` | `pa.large_string()` |
| `Literal[1, 2, 3]` | `pa.int64()` |
| `Literal[True, False]` | `pa.bool_()` |
| `Literal["a", None]` | Strip `None` → `pa.large_string()` |
| `Literal[None]` | `ValueError`: use `Optional[T]` instead |
| `Literal["a", 1]` | `ValueError`: mixed types not supported |

---

## Tests

All new tests in `tests/test_extension_types/test_pydantic_logical_type_factory.py`.
Module-level model definitions are required (per existing pattern — local classes are
rejected by the factory).

| Test | What it verifies |
|---|---|
| `test_factory_create_model_with_literal_str_field` | `Literal["a", "b"]` field → `large_string` in struct |
| `test_factory_create_model_with_literal_int_field` | `Literal[1, 2]` → `int64` |
| `test_factory_create_model_with_literal_none_field` | `Literal["a", None]` → `large_string` (None stripped) |
| `test_factory_rejects_literal_none_only` | `Literal[None]` → raises `ValueError` |
| `test_factory_rejects_mixed_literal` | `Literal["a", 1]` → raises `ValueError` |
| `test_literal_model_round_trip` | `python_to_storage` → `storage_to_python` round-trip, value unchanged |
| `test_literal_model_as_dictsource_column` | End-to-end: `DictSource` with a `Literal`-field model as a schema column succeeds |

---

## Out of Scope

- Struct-field nullability for `Optional` vs plain fields — tracked in ITL-445.
- `DataclassLogicalTypeFactory` — not affected (dataclass annotations flow through the
  same `register_python_class` path, so the fix in `_register_python_class_impl` covers
  dataclasses automatically).
- Preserving the allowed-value set as Arrow metadata — not needed to unblock the use case.
