# datetime ↔ Arrow timestamp support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add native `datetime.datetime ↔ pa.timestamp("us", tz="UTC")` support to `UniversalTypeConverter` and revert the ISO string workaround in `ResultCache`.

**Architecture:** Four surgical additions to `universal_converter.py` (type map entry, reverse type branch, Python→Arrow value validator, Arrow→Python explicit passthrough) plus a one-line revert in `result_cache.py`. All changes are tested with TDD before implementation.

**Tech Stack:** Python 3.11+, PyArrow ≥ 20, pytest, `uv run pytest`

---

## File map

| File | Change |
|---|---|
| `src/orcapod/semantic_types/universal_converter.py` | Add `datetime` import; 4 targeted additions |
| `src/orcapod/core/result_cache.py` | Revert ISO string workaround to native `pa.timestamp` |
| `tests/test_semantic_types/test_universal_converter.py` | Add 7 new tests |

---

## Task 1: Type mapping — Python → Arrow and Arrow → Python

**Files:**
- Modify: `src/orcapod/semantic_types/universal_converter.py:12-32` (imports), `:61-92` (`_get_python_to_arrow_map`), `:637-650` (`_convert_arrow_to_python` else-fallback)
- Test: `tests/test_semantic_types/test_universal_converter.py`

- [ ] **Step 1.1: Write three failing type-mapping tests**

Open `tests/test_semantic_types/test_universal_converter.py`. Add the following import at the top of the file alongside the existing imports:

```python
from datetime import datetime, timezone
```

Then add these three test functions after `test_python_type_to_arrow_type_basic`:

```python
def test_python_type_to_arrow_type_datetime():
    assert universal_converter.python_type_to_arrow_type(datetime) == pa.timestamp(
        "us", tz="UTC"
    )


def test_arrow_type_to_python_type_timestamp_with_tz():
    assert (
        universal_converter.arrow_type_to_python_type(pa.timestamp("us", tz="UTC"))
        is datetime
    )


def test_arrow_type_to_python_type_timestamp_no_tz():
    assert universal_converter.arrow_type_to_python_type(pa.timestamp("us")) is datetime
```

- [ ] **Step 1.2: Run the tests to confirm they fail**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py::test_python_type_to_arrow_type_datetime tests/test_semantic_types/test_universal_converter.py::test_arrow_type_to_python_type_timestamp_with_tz tests/test_semantic_types/test_universal_converter.py::test_arrow_type_to_python_type_timestamp_no_tz -v
```

Expected: all three FAIL — `ValueError: Unsupported Python type` for the first, and the last two return `Any` instead of `datetime`.

- [ ] **Step 1.3: Add the `datetime` import to `universal_converter.py`**

In `src/orcapod/semantic_types/universal_converter.py`, the current imports block starts at line 12 with `from __future__ import annotations`. Add the stdlib import on line 14 (after the `from __future__` line and the blank line):

Find this block:
```python
from __future__ import annotations

import hashlib
import logging
import types
import typing
```

Replace with:
```python
from __future__ import annotations

import hashlib
import logging
import types
import typing
from datetime import datetime, timezone
```

- [ ] **Step 1.4: Add `datetime` to `_get_python_to_arrow_map()`**

In `_get_python_to_arrow_map()`, find the date/time string entries (around line 88):

```python
        # Date/time types
        "date": pa.date32(),
        "datetime": pa.timestamp("us"),
        "timestamp": pa.timestamp("us"),
```

Replace with:

```python
        # Date/time types
        "date": pa.date32(),
        "datetime": pa.timestamp("us"),
        "timestamp": pa.timestamp("us"),
        datetime: pa.timestamp("us", tz="UTC"),
```

- [ ] **Step 1.5: Add `is_timestamp` branch to `_convert_arrow_to_python()`**

Find the `else` fallback at line 637:

```python
        else:
            # Default case for unsupported types.
            # NOTE: this silent fallback to Any can cause cryptic errors
            # downstream when code tries to convert Any back to Arrow
            # (e.g. "Unsupported Python type: typing.Any"). If you hit that,
            # the root cause is likely an unmapped Arrow type here.
            # (pa.null() is intentionally excluded — it is handled above.)
            logger.warning(
```

Insert the new branch immediately before it:

```python
        elif pa.types.is_timestamp(arrow_type):
            return datetime

        else:
            # Default case for unsupported types.
```

- [ ] **Step 1.6: Run the three tests to confirm they pass**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py::test_python_type_to_arrow_type_datetime tests/test_semantic_types/test_universal_converter.py::test_arrow_type_to_python_type_timestamp_with_tz tests/test_semantic_types/test_universal_converter.py::test_arrow_type_to_python_type_timestamp_no_tz -v
```

Expected: all three PASS.

- [ ] **Step 1.7: Run the full converter test suite to check for regressions**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py -v
```

Expected: all existing tests still pass.

- [ ] **Step 1.8: Commit**

```bash
git add src/orcapod/semantic_types/universal_converter.py tests/test_semantic_types/test_universal_converter.py
git commit -m "feat(semantic-types): map datetime ↔ pa.timestamp(us, UTC) in type converter"
```

---

## Task 2: Value converters — naive rejection and explicit Arrow → Python passthrough

**Files:**
- Modify: `src/orcapod/semantic_types/universal_converter.py:748-753` (`_create_python_to_arrow_converter`), `:929-931` (`_create_arrow_to_python_converter`)
- Test: `tests/test_semantic_types/test_universal_converter.py`

- [ ] **Step 2.1: Write two failing value-converter tests**

Add these two tests after `test_arrow_type_to_python_type_timestamp_no_tz`:

```python
def test_datetime_converter_rejects_naive():
    to_arrow, _ = universal_converter.get_conversion_functions(datetime)
    naive = datetime(2024, 1, 15, 12, 30, 45, 123456)  # no tzinfo
    with pytest.raises(ValueError, match="Naive datetime"):
        to_arrow(naive)


def test_datetime_converter_accepts_aware():
    to_arrow, _ = universal_converter.get_conversion_functions(datetime)
    aware = datetime(2024, 1, 15, 12, 30, 45, 123456, tzinfo=timezone.utc)
    result = to_arrow(aware)
    assert result == aware
```

- [ ] **Step 2.2: Run the tests to confirm they fail**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py::test_datetime_converter_rejects_naive tests/test_semantic_types/test_universal_converter.py::test_datetime_converter_accepts_aware -v
```

Expected: `test_datetime_converter_rejects_naive` FAILS (no `ValueError` raised — naive datetime silently passes through), `test_datetime_converter_accepts_aware` MAY pass or fail.

- [ ] **Step 2.3: Add the datetime validator in `_create_python_to_arrow_converter()`**

In `_create_python_to_arrow_converter()`, find the block starting at line 746:

```python
        # Create conversion function based on type

        origin = get_origin(python_type)
        args = get_args(python_type)

        if python_type in {int, float, str, bool, bytes} or origin is None:
            # Basic types - no conversion needed
            return lambda value: value
```

Replace with:

```python
        # Create conversion function based on type

        # datetime must be intercepted before the `origin is None` catch-all
        # below, which would silently pass through naive datetimes.
        if python_type is datetime:

            def _convert_datetime(dt: datetime) -> datetime:
                if dt.tzinfo is None:
                    raise ValueError(
                        f"Naive datetime (no timezone info) is not supported. "
                        f"Use a timezone-aware datetime, "
                        f"e.g. datetime.now(timezone.utc). Got: {dt!r}"
                    )
                return dt

            return _convert_datetime

        origin = get_origin(python_type)
        args = get_args(python_type)

        if python_type in {int, float, str, bool, bytes} or origin is None:
            # Basic types - no conversion needed
            return lambda value: value
```

- [ ] **Step 2.4: Add explicit timestamp passthrough in `_create_arrow_to_python_converter()`**

Find the `else` passthrough at line 929:

```python
        else:
            # Default passthrough
            return lambda value: value
```

Insert the new branch immediately before it:

```python
        elif pa.types.is_timestamp(arrow_type):
            # PyArrow's to_pylist() already calls .as_py() on each scalar,
            # returning a timezone-aware datetime for UTC columns.
            return lambda value: value

        else:
            # Default passthrough
            return lambda value: value
```

- [ ] **Step 2.5: Run the two value-converter tests to confirm they pass**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py::test_datetime_converter_rejects_naive tests/test_semantic_types/test_universal_converter.py::test_datetime_converter_accepts_aware -v
```

Expected: both PASS.

- [ ] **Step 2.6: Run the full converter test suite to check for regressions**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py -v
```

Expected: all tests pass.

- [ ] **Step 2.7: Commit**

```bash
git add src/orcapod/semantic_types/universal_converter.py tests/test_semantic_types/test_universal_converter.py
git commit -m "feat(semantic-types): add datetime value converter with naive-datetime guard"
```

---

## Task 3: Round-trip tests — full table conversion

**Files:**
- Test: `tests/test_semantic_types/test_universal_converter.py`

These tests exercise `python_dicts_to_arrow_table` and `arrow_table_to_python_dicts` on the
global converter retrieved via `get_default_context().type_converter`.

- [ ] **Step 3.1: Write two round-trip tests**

`get_default_context` is already imported at the top of the test file — no new import needed.

Add these two tests after `test_datetime_converter_accepts_aware`:

```python
def test_datetime_round_trip():
    converter = get_default_context().type_converter
    ts = datetime(2024, 3, 15, 10, 30, 45, 123456, tzinfo=timezone.utc)
    rows_in = [{"event": "launch", "ts": ts}]

    table = converter.python_dicts_to_arrow_table(rows_in)

    # Arrow schema must use timestamp(us, UTC)
    assert table.schema.field("ts").type == pa.timestamp("us", tz="UTC")

    rows_out = converter.arrow_table_to_python_dicts(table)
    assert len(rows_out) == 1
    assert rows_out[0]["event"] == "launch"
    assert rows_out[0]["ts"] == ts


def test_optional_datetime_round_trip():
    converter = get_default_context().type_converter
    ts = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
    rows_in = [
        {"label": "a", "ts": ts},
        {"label": "b", "ts": None},
    ]
    python_schema = {"label": str, "ts": datetime | None}

    table = converter.python_dicts_to_arrow_table(rows_in, python_schema=python_schema)

    assert table.schema.field("ts").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("ts").nullable is True

    rows_out = converter.arrow_table_to_python_dicts(table)
    assert rows_out[0]["ts"] == ts
    assert rows_out[1]["ts"] is None
```

- [ ] **Step 3.2: Run the tests — they should pass immediately**

Tasks 1 and 2 already provide all the implementation needed for these tests. Run them to
confirm end-to-end correctness:

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py::test_datetime_round_trip tests/test_semantic_types/test_universal_converter.py::test_optional_datetime_round_trip -v
```

Expected: both PASS.

If either test fails, debug using the failing assertion message — the type mapping or value
converter from Tasks 1/2 may need a correction.

- [ ] **Step 3.4: Run the full converter test suite**

```bash
uv run pytest tests/test_semantic_types/test_universal_converter.py -v
```

Expected: all tests pass.

- [ ] **Step 3.5: Commit**

```bash
git add tests/test_semantic_types/test_universal_converter.py
git commit -m "test(semantic-types): add datetime ↔ Arrow timestamp round-trip tests"
```

---

## Task 4: Revert `ResultCache` ISO string workaround

**Files:**
- Modify: `src/orcapod/core/result_cache.py:194-203`

- [ ] **Step 4.1: Run the existing result_cache tests to establish baseline**

```bash
uv run pytest tests/test_core/test_result_cache.py -v
```

Note the number of passing tests as your baseline.

- [ ] **Step 4.2: Revert the ISO string workaround**

In `src/orcapod/core/result_cache.py`, find lines 194–203:

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

Replace with:

```python
        data_table = data_table.append_column(
            constants.POD_TIMESTAMP,
            pa.array([datetime.now(timezone.utc)], type=pa.timestamp("us", tz="UTC")),
        )
```

- [ ] **Step 4.3: Run the result_cache tests to confirm no regressions**

```bash
uv run pytest tests/test_core/test_result_cache.py -v
```

Expected: same number of passing tests as the baseline from Step 4.1.

- [ ] **Step 4.4: Run the full test suite**

```bash
uv run pytest -v
```

Expected: all tests pass. No new failures introduced by the timestamp type change.

- [ ] **Step 4.5: Commit**

```bash
git add src/orcapod/core/result_cache.py
git commit -m "fix(core): revert ResultCache timestamp to native pa.timestamp(us, UTC) — ENG-387"
```

---

## Final check

- [ ] **Run the complete test suite one last time**

```bash
uv run pytest -v
```

Expected: all tests pass, zero failures.
