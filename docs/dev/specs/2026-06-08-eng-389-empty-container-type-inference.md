# ENG-389: Empty Container Type Inference Fix

**Date:** 2026-06-08
**Issue:** [ENG-389](https://linear.app/enigma-metamorphic/issue/ENG-389/type-inference-empty-containers-infer-as-dictany-any-listany)
**Status:** Approved

---

## Problem

`_infer_list_type`, `_infer_dict_type`, and `_infer_set_type` in
`src/orcapod/semantic_types/pydata_utils.py` return `list[Any]`, `dict[Any, Any]`,
and `set[Any]` respectively when all input containers are empty (no elements to
inspect for type). These types then fail in `python_type_to_arrow_type` because
`typing.Any` is not mapped to any Arrow type — the converter raises:

```
ValueError: Unsupported Python type: typing.Any.
```

**Concrete example:** `extra_info: {}` (an empty `dict[str, str]`) infers as
`dict[Any, Any]`, which cannot be converted to an Arrow column.

---

## Approach

Map `typing.Any` → `pa.null()` in the `UniversalTypeConverter`. `pa.null()` is
Arrow's canonical "no type information" type. Empty containers have no elements
to validate, so `pa.null()` is semantically correct — it accurately represents
"we don't know the element type." The mapping is bidirectional: `pa.null()` →
`typing.Any` on the reverse path.

This is a converter-only fix. The inference functions correctly return `Any` to
signal unknown element type; the fix is in how the converter handles that signal.

### Why `pa.null()` over `pa.large_string()`

`pa.null()` is semantically correct: it makes no assumptions about what the
element type will be. `pa.large_string()` would silently coerce unknown types to
string, which could hide bugs. `pa.null()` fails loudly if non-null elements are
written to the column, which is the right behavior when the schema has been
inferred rather than specified.

---

## Round-trip analysis

All paths verified:

| Python type | Arrow type | Back to Python |
|---|---|---|
| `Any` | `pa.null()` | `Any` |
| `list[Any]` | `pa.large_list(pa.null())` | `list[Any]` |
| `dict[Any, Any]` | `pa.large_list(pa.struct([("key", pa.null()), ("value", pa.null())]))` | `dict[Any, Any]` |
| `set[Any]` | `pa.large_list(pa.null())` | `list[Any]` (pre-existing set→list identity loss, out of scope) |

**Nullable handling:** `arrow_schema_to_python_schema` already guards
`if field.nullable and python_type is not Any`, so `Any | None` is never
attempted. ✓

**Value converters:** `_create_python_to_arrow_converter(Any)` falls into the
`origin is None` passthrough branch — correct for `Any` as a standalone type.
For `list[Any]`, the element converter is a passthrough; `[]` converts to `[]`. ✓

**Hashing:** `arrow_utils.py` line 172 already handles `pa.null()` → `"NoneType"`
explicitly. No changes needed. ✓

**`pa.null()` warning:** Without an explicit case, `pa.null()` falls through to
the warning branch in `_convert_arrow_to_python` (returns `Any` but logs a
spurious warning). The explicit `pa.null()` case is required to suppress this.

---

## Changes

### 1. `src/orcapod/semantic_types/universal_converter.py`

**`_get_python_to_arrow_map()`** — add one entry to `_PYTHON_TO_ARROW_MAP`:

```python
Any: pa.null(),
```

Since the type map is checked first in `_convert_python_to_arrow`
(`if python_type in type_map`), this intercepts `Any` before the
`if origin is None` error branch. The error branch is retained for all other
non-origin types.

**`_convert_arrow_to_python()`** — add explicit `pa.null()` handling near the
top of the method, before the struct/list dispatch:

```python
if pa.types.is_null(arrow_type):
    return Any
```

This produces a clean round-trip (`pa.null()` → `Any`) and avoids the spurious
"no mapping for Arrow type" warning that would otherwise fire on every
`pa.null()` encounter.

**`_convert_python_to_arrow()` error hint** — the `if python_type is Any:` hint
block (lines 424–432) becomes unreachable since `Any` is now caught by the type
map. Remove the dead code block; the generic `raise ValueError` remains for
other unsupported types.

### 2. `tests/test_semantic_types/test_pydata_utils.py`

Two new tests documenting inference behaviour for empty containers:

- **`test_infer_empty_list_schema`** — `[{"items": []}]` infers `"items": list[Any]`
- **`test_infer_empty_dict_schema`** — `[{"meta": {}}]` infers `"meta": dict[Any, Any]`

### 3. `tests/test_semantic_types/test_universal_converter.py`

Six new tests covering conversion and round-trips:

- **`test_any_to_arrow_type`** — `python_type_to_arrow_type(Any)` == `pa.null()`
- **`test_list_any_to_arrow_type`** — `python_type_to_arrow_type(list[Any])` == `pa.large_list(pa.null())`
- **`test_dict_any_any_to_arrow_type`** — `python_type_to_arrow_type(dict[Any, Any])` produces the expected `pa.large_list(pa.struct([...]))` with `pa.null()` key/value types
- **`test_null_arrow_to_any_python_type`** — `arrow_type_to_python_type(pa.null())` == `Any`
- **`test_empty_container_inference_to_arrow_no_error`** — end-to-end: infer schema from `[{"items": [], "meta": {}}]`, convert to Arrow schema via `python_schema_to_arrow_schema`, no exception raised
- **`test_pyarrow_empty_list_with_null_type`** — directly verifies `pa.Table.from_pylist([{"items": []}], schema=pa.schema([pa.field("items", pa.large_list(pa.null()))]))` succeeds; confirms the PyArrow assumption holds

### 4. `DESIGN_ISSUES.md`

Add a new entry (resolved) documenting this fix for discoverability.

---

## Out of scope

- Changing inference behaviour for non-empty containers.
- Removing the existing workarounds in `datagram.py` and `function_node.py` —
  they remain correct (using the Arrow schema when available is always more
  precise than re-inference) and are worth keeping as defensive code.
- The `set[Any]` → `list[Any]` round-trip identity loss — pre-existing behaviour
  unrelated to this issue.
