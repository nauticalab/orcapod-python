# Design: Add date and datetime support for Tag values (ITL-45)

**Linear issue:** ITL-45
**Date:** 2026-06-29
**Status:** Approved

---

## Overview

Datetime values are common tag metadata — experiment date, date of birth, acquisition
timestamp. OrcaPod's type aliases (`TagValue`, `SupportedNativePythonData`) do not
include `datetime.date` or `datetime.datetime`, making them technically unsupported
even though the underlying Arrow conversion infrastructure already handles them
partially. The prior ENG-387 work added `datetime.datetime ↔ pa.timestamp("us",
tz="UTC")` to `UniversalTypeConverter`, but left `datetime.date` out of scope and did
not update the public type aliases. This design closes both gaps.

---

## Goals & Success Criteria

* `TagValue` includes `datetime.date | datetime.datetime` as valid scalar types.
* `SupportedNativePythonData` includes `datetime.date | datetime.datetime`.
* `python_type_to_arrow_type(date)` returns `pa.date32()` via direct map lookup
  (not the `__name__` string fallback that currently silently works).
* `arrow_type_to_python_type(pa.date32())` returns `datetime.date` (currently falls
  back to `typing.Any` with a warning).
* A `Tag` constructed with `date` or `datetime` values round-trips correctly:
  Python dict → Arrow table → Python dict, with correct types and values.
* `content_hash()` on a `Tag` with `date` or `datetime` values succeeds (enabled by
  upgrading `starfix` to `~=0.4.0` which adds timestamp hashing support).
* Naive `datetime` (no `tzinfo`) raises `ValueError` at construction time — no silent
  coercion (strict timezone policy, already enforced by ENG-387).

---

## Scope & Boundaries

In scope:
* `datetime.date` ↔ `pa.date32()` bidirectional support in `UniversalTypeConverter`.
* `datetime.date | datetime.datetime` added to `TagValue` and `SupportedNativePythonData`.
* `starfix` dependency bumped to `~=0.4.0` to enable `pa.timestamp` hashing.
* Tests covering round-trip, content hashing, and type conversion for both types.

Out of scope:
* `datetime.time` and Arrow `time32`/`time64` — not a common tag value type.
* `datetime.timedelta` and Arrow `duration` — deferred.
* Configurable Arrow precision or timezone for `date` — `pa.date32()` is the canonical
  choice (matches the prior `datetime` decision of using the lossless format).

---

## Design

### `datetime.date` ↔ Arrow mapping

`datetime.date` maps to `pa.date32()`:
- Arrow `date32` is a 32-bit integer representing days since Unix epoch — the
  lossless, compact format for calendar dates.
- `date64` (milliseconds since epoch) has no advantage for calendar-only values and
  is less common in the ecosystem.
- PyArrow's `from_pylist` already accepts `datetime.date` objects for `date32` columns
  and `to_pylist` returns `datetime.date` objects — no custom value converter needed.

### Timezone policy for `datetime.datetime`

Already enforced by ENG-387: naive datetimes raise `ValueError` in the Python→Arrow
value converter. This design does not change that behaviour.

### No value converter needed for `date`

Unlike `datetime`, `date` values have no timezone concern. PyArrow handles
`datetime.date → date32` natively in `from_pylist`, and `date32 → datetime.date` in
`to_pylist`. The value converter for `date` is a passthrough (same as the catch-all
for non-generic types in `_create_python_to_arrow_converter`), which already applies
because `origin is None` for `date`.

---

## Changes

### 1. `pyproject.toml`

Bump starfix:

```
"starfix~=0.4.0"
```

starfix 0.4.0 adds `pa.timestamp` support in `_element_size_for_type`
(see nauticalab/starfix#18), enabling `content_hash()` on datagrams with datetime
columns. Without this bump, datetime-valued tags would crash during hashing.

### 2. `src/orcapod/types.py`

#### Imports

Add `date` to the existing `datetime` import:

```python
from datetime import date, datetime
```

#### `TagValue`

```python
TagValue: TypeAlias = int | str | date | datetime | None | Collection["TagValue"]
```

Remove the `# TODO: accomodate other common data types such as datetime` comment above it.

#### `SupportedNativePythonData`

```python
SupportedNativePythonData: TypeAlias = str | int | float | bool | bytes | date | datetime
```

Update the docstring to mention the two new types.

### 3. `src/orcapod/semantic_types/universal_converter.py`

#### Import

The existing import is:

```python
from datetime import datetime, timezone
```

Add `date`:

```python
from datetime import date, datetime, timezone
```

#### `_get_python_to_arrow_map()`

Add one entry after the existing `datetime` entry:

```python
date: pa.date32(),
```

This upgrades the mapping from the silent `__name__` fallback (which resolves
`"date"` string → `pa.date32()`) to a direct key lookup — more explicit, faster, and
consistent with how all other types are mapped.

#### `_convert_arrow_to_python()`

Add a branch for date types before the timestamp branch:

```python
elif pa.types.is_date(arrow_type):
    return date
```

This fixes `arrow_type_to_python_type(pa.date32())` returning `typing.Any` with a
warning. `pa.types.is_date` covers both `date32` and `date64`.

### 4. Tests

#### `test-objective/unit/test_tag.py` — new class `TestTagDatetimeValues`

Tests covering:
- `Tag({"exp_date": date(2024, 1, 15)})` — construction succeeds
- Round-trip via `as_dict()` returns `datetime.date` with the original value
- `as_table()` produces a `date32` column
- `content_hash()` succeeds (requires starfix 0.4.0)
- `Tag({"ts": datetime(2024, 1, 15, tzinfo=timezone.utc)})` — construction succeeds
- Round-trip via `as_dict()` returns timezone-aware `datetime`
- `as_table()` produces a `timestamp[us, tz=UTC]` column
- `content_hash()` succeeds
- `Tag({"ts": datetime(2024, 1, 15)})` — naive datetime raises `ValueError`

#### `test-objective/unit/test_semantic_types.py` — additions to existing classes

- `TestPythonToArrowType`: `python_type_to_arrow_type(date)` returns `pa.date32()`
- `TestArrowToPythonType`: `arrow_type_to_python_type(pa.date32())` returns `date`
- `TestSchemaConversionRoundtrip`: schema with `date` and `datetime` fields round-trips

---

## Out of Scope

- `datetime.time` and Arrow `time32`/`time64` — deferred to ITL-443 (v0.2).
- `datetime.timedelta` and Arrow `duration` — deferred to ITL-444 (v0.2).
- Configurable precision or timezone for dates.
