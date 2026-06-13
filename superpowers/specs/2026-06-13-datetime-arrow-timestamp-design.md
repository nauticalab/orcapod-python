# Design: Native datetime ↔ Arrow timestamp support in UniversalTypeConverter

**Linear issue:** ENG-387  
**Date:** 2026-06-13  
**Status:** Approved

---

## Overview

`UniversalTypeConverter` does not recognise `datetime.datetime` as a Python type:
`python_type_to_arrow_type(datetime)` raises `ValueError`, and
`arrow_type_to_python_type(pa.timestamp(...))` logs a warning and falls back to `Any`.
This forces callers to use workarounds (e.g. the ISO 8601 string workaround in
`ResultCache.store()`). The fix adds first-class support for the `datetime ↔
pa.timestamp("us", tz="UTC")` pair throughout the converter.

---

## Goals & Success Criteria

- `python_type_to_arrow_type(datetime)` returns `pa.timestamp("us", tz="UTC")`
- `arrow_type_to_python_type(pa.timestamp("us", tz="UTC"))` returns `datetime`
- `python_dicts_to_arrow_table` / `arrow_table_to_python_dicts` round-trip
  timezone-aware `datetime` values correctly
- Naive datetimes (no `tzinfo`) raise `ValueError` immediately in the Python→Arrow
  value converter — no silent coercion
- The ISO 8601 string workaround in `ResultCache.store()` is reverted to native
  `pa.timestamp("us", tz="UTC")`

---

## Design

### Precision choice

Python `datetime.datetime` has microsecond precision (`microsecond` field, 0–999999).
`pa.timestamp("us", tz="UTC")` is the exact lossless fit:
- `"ms"` would silently truncate microseconds
- `"ns"` stores zeros in sub-microsecond slots (wasted space, potential confusion)
- `"us"` is a round-trip identity

### Timezone policy

All datetimes stored via the universal converter must be timezone-aware. If a caller
passes a naive `datetime` (no `tzinfo`), the Python→Arrow value converter raises
`ValueError` immediately — before PyArrow ever sees the value. No silent UTC assumption.

---

## Changes

### 1. `src/orcapod/semantic_types/universal_converter.py`

#### Module-level import

Add `from datetime import datetime, timezone` (stdlib, no lazy-import concern).

#### `_get_python_to_arrow_map()`

Add one entry to the type map dict:

```python
datetime: pa.timestamp("us", tz="UTC"),
```

This makes `python_type_to_arrow_type(datetime)` work, and also enables schema
inference through `infer_python_schema_from_pylist_data` (which calls `type(v)` on
sample values).

#### `_convert_arrow_to_python()`

Add before the `else` fallback:

```python
elif pa.types.is_timestamp(arrow_type):
    return datetime
```

Covers all timestamp variants (`pa.timestamp("us", tz="UTC")`, `pa.timestamp("ms")`,
etc.) — the Python type is always `datetime` regardless of Arrow precision or timezone.

#### `_create_python_to_arrow_converter()`

Add before the existing `if python_type in {int, float, str, bool, bytes} or origin is None`
catch-all (which would otherwise silently pass `datetime` through without validation):

```python
if python_type is datetime:
    def _convert_datetime(dt):
        if dt.tzinfo is None:
            raise ValueError(
                f"Naive datetime (no timezone info) is not supported. "
                f"Use a timezone-aware datetime, e.g. datetime.now(timezone.utc). "
                f"Got: {dt!r}"
            )
        return dt
    return _convert_datetime
```

The validated `datetime` is returned as-is — PyArrow's `pa.Table.from_pylist()` knows
how to convert a timezone-aware `datetime` into a `pa.timestamp("us", tz="UTC")` scalar.

#### `_create_arrow_to_python_converter()`

Add an explicit branch before the `else` passthrough:

```python
elif pa.types.is_timestamp(arrow_type):
    return lambda value: value
```

`arrow_table.to_pylist()` already calls `.as_py()` on each scalar, which returns a
timezone-aware `datetime` for UTC-typed columns. The passthrough is correct; the
explicit branch exists to avoid the silent `else` fallthrough and its absence of
intent.

---

### 2. `src/orcapod/core/result_cache.py`

Revert the ISO 8601 string workaround in `store()` (lines 194–202).

**Before:**
```python
# Append timestamp as ISO 8601 string.
# TODO: switch to pa.timestamp("us", tz="UTC") once the universal
# converter supports native Arrow timestamp ↔ Python datetime round-trip.
# ISO 8601 strings sort lexicographically in time order, so the
# conflict-resolution sort in lookup() still works correctly.
timestamp = datetime.now(timezone.utc).isoformat()
data_table = data_table.append_column(
    constants.POD_TIMESTAMP,
    pa.array([timestamp], type=pa.large_string()),
)
```

**After:**
```python
data_table = data_table.append_column(
    constants.POD_TIMESTAMP,
    pa.array([datetime.now(timezone.utc)], type=pa.timestamp("us", tz="UTC")),
)
```

The `lookup()` sort on `POD_TIMESTAMP` continues to work correctly — Arrow timestamps
sort chronologically, matching the previous ISO string lexicographic order.

---

### 3. Tests (`tests/test_semantic_types/test_universal_converter.py`)

**Type mapping:**
- `test_python_type_to_arrow_type_datetime` — `python_type_to_arrow_type(datetime)` returns `pa.timestamp("us", tz="UTC")`
- `test_arrow_type_to_python_type_timestamp` — `arrow_type_to_python_type(pa.timestamp("us", tz="UTC"))` returns `datetime`
- `test_arrow_type_to_python_type_timestamp_no_tz` — `pa.timestamp("us")` (no tz) also maps to `datetime`

**Value converter:**
- `test_datetime_converter_rejects_naive` — naive `datetime` raises `ValueError`
- `test_datetime_converter_accepts_aware` — timezone-aware `datetime` passes through without error

**Round-trip:**
- `test_datetime_round_trip` — `python_dicts_to_arrow_table` / `arrow_table_to_python_dicts` with a `datetime` field round-trips a UTC-aware `datetime` losslessly
- `test_optional_datetime_round_trip` — same for `Optional[datetime]`, including a `None` value

---

## Out of Scope

- `datetime.date`, `datetime.time`, and other Arrow temporal types (`date32`, `time64`,
  `duration`) — explicitly deferred per ENG-387
- Configurable precision or timezone — `"us"` + `"UTC"` are fixed defaults; no API
  knob is added
