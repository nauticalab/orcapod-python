# DictSource Auto-Registration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `DictSource` automatically register Pydantic `BaseModel` and `@dataclass` column types so users never need to pre-register them via a function pod.

**Architecture:** Insert one call to `ensure_types_registered_for_schemas(python_schema)` in `UniversalTypeConverter.python_dicts_to_arrow_table` immediately before the Python schema is converted to an Arrow schema. This mirrors the existing function-pod eager-registration pattern and is idempotent, thread-safe, and a no-op for already-registered or primitive types.

**Tech Stack:** Python, PyArrow, Pydantic v2, `dataclasses` stdlib, pytest (run via `uv run pytest`)

---

## File Map

| Action | Path | What changes |
|--------|------|--------------|
| Modify | `src/orcapod/semantic_types/universal_converter.py` | Add 1 line in `python_dicts_to_arrow_table` |
| Create | `tests/test_core/sources/test_dict_source_auto_registration.py` | 4 new tests |

---

## Task 1: Write the Failing Tests

**Files:**
- Create: `tests/test_core/sources/test_dict_source_auto_registration.py`

- [ ] **Step 1.1 — Write the test file**

```python
"""Tests for DictSource auto-registration of Pydantic / dataclass column types.

Verifies that DictSource construction and iteration succeed for Pydantic BaseModel
and @dataclass column types that have never been registered via a function pod.
"""
from __future__ import annotations

import dataclasses

import pytest
from pydantic import BaseModel

from orcapod.core.sources import DictSource


# ---------------------------------------------------------------------------
# Fixtures — fresh model classes scoped per-test to avoid cross-test registry
# state. Using classes defined at module level is fine because registration is
# idempotent; the classes just need to be new relative to any prior test run.
# ---------------------------------------------------------------------------


class _Point(BaseModel):
    x: float
    y: float


@dataclasses.dataclass
class _Measurement:
    value: float
    unit: str


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDictSourceAutoRegistration:
    def test_pydantic_model_no_prior_registration(self):
        """DictSource with a fresh Pydantic model column succeeds end-to-end."""
        src = DictSource(
            data=[
                {"id": 1, "pt": _Point(x=1.0, y=2.0)},
                {"id": 2, "pt": _Point(x=3.0, y=4.0)},
            ],
            tag_columns=["id"],
        )
        rows = list(src.iter_data())
        assert len(rows) == 2

    def test_dataclass_no_prior_registration(self):
        """DictSource with a fresh @dataclass column succeeds end-to-end."""
        src = DictSource(
            data=[
                {"id": 1, "m": _Measurement(value=1.5, unit="mm")},
                {"id": 2, "m": _Measurement(value=2.5, unit="cm")},
            ],
            tag_columns=["id"],
        )
        rows = list(src.iter_data())
        assert len(rows) == 2

    def test_pydantic_no_double_registration_error(self):
        """Creating a second DictSource with the same Pydantic model type does not raise."""
        data = [{"id": 1, "pt": _Point(x=0.0, y=0.0)}]
        DictSource(data=data, tag_columns=["id"])
        # Second construction — type already in registry; must be idempotent.
        src2 = DictSource(data=data, tag_columns=["id"])
        rows = list(src2.iter_data())
        assert len(rows) == 1

    def test_pod_registration_not_broken(self):
        """Function-pod eager registration still works after DictSource auto-registration."""
        from orcapod.core.data_function import PythonDataFunction
        from orcapod.core.function_pod import FunctionPod

        class _Sensor(BaseModel):
            reading: float

        def identity(sensor: _Sensor) -> _Sensor:
            return sensor

        # Eager registration via pod construction — must not raise.
        pod = FunctionPod(
            data_function=PythonDataFunction(identity, output_keys="sensor")
        )
        assert pod is not None

        # DictSource with the same type — registration already done; must not raise.
        src = DictSource(
            data=[{"id": 1, "sensor": _Sensor(reading=42.0)}],
            tag_columns=["id"],
        )
        rows = list(src.iter_data())
        assert len(rows) == 1
```

- [ ] **Step 1.2 — Run the tests to confirm they fail**

```bash
cd /path/to/orcapod-python
uv run pytest tests/test_core/sources/test_dict_source_auto_registration.py -v
```

Expected: All four tests **FAIL** with `ValueError: Unsupported Python type: ...` (for the first three) and the pod test may pass or fail depending on whether the fix is in. All failing with that error confirms the tests exercise the right path.

---

## Task 2: Implement the Fix

**Files:**
- Modify: `src/orcapod/semantic_types/universal_converter.py` (around line 956–959)

- [ ] **Step 2.1 — Apply the one-line change**

Open `src/orcapod/semantic_types/universal_converter.py`. Find `python_dicts_to_arrow_table`. The current `if arrow_schema is None:` block (starting around line 956) reads:

```python
        if arrow_schema is None:
            # Convert to Arrow schema
            assert python_schema is not None, "Python schema should not be None here"
            arrow_schema = self.python_schema_to_arrow_schema(python_schema)
```

Replace it with:

```python
        if arrow_schema is None:
            # Convert to Arrow schema — auto-register any Pydantic / dataclass types
            # encountered here so DictSource users do not need to pre-register via a
            # function pod. ensure_types_registered_for_schemas is idempotent and
            # thread-safe; it is a no-op for primitives and already-registered types.
            assert python_schema is not None, "Python schema should not be None here"
            self.ensure_types_registered_for_schemas(python_schema)
            arrow_schema = self.python_schema_to_arrow_schema(python_schema)
```

- [ ] **Step 2.2 — Run the new tests to confirm they pass**

```bash
uv run pytest tests/test_core/sources/test_dict_source_auto_registration.py -v
```

Expected output (all green):

```
PASSED tests/test_core/sources/test_dict_source_auto_registration.py::TestDictSourceAutoRegistration::test_pydantic_model_no_prior_registration
PASSED tests/test_core/sources/test_dict_source_auto_registration.py::TestDictSourceAutoRegistration::test_dataclass_no_prior_registration
PASSED tests/test_core/sources/test_dict_source_auto_registration.py::TestDictSourceAutoRegistration::test_pydantic_no_double_registration_error
PASSED tests/test_core/sources/test_dict_source_auto_registration.py::TestDictSourceAutoRegistration::test_pod_registration_not_broken
```

- [ ] **Step 2.3 — Run the full test suite to confirm no regressions**

```bash
uv run pytest tests/ -x -q
```

Expected: all tests pass (exit code 0). If any pre-existing failures appear, check that they existed on `main` before this branch — do not count them as regressions introduced by this change.

- [ ] **Step 2.4 — Commit**

```bash
git add src/orcapod/semantic_types/universal_converter.py \
        tests/test_core/sources/test_dict_source_auto_registration.py
git commit -m "fix(universal_converter): auto-register Pydantic/dataclass types in python_dicts_to_arrow_table (ITL-446)"
```

---

## Task 3: Commit the Plan

**Files:**
- Commit: `superpowers/plans/2026-07-07-dict-source-auto-registration.md` (this file)

- [ ] **Step 3.1 — Commit the plan doc**

```bash
git add superpowers/plans/2026-07-07-dict-source-auto-registration.md
git commit -m "docs(plans): add implementation plan for DictSource auto-registration (ITL-446)"
```

---

## Self-Review Checklist

**Spec coverage:**
- [x] `python_dicts_to_arrow_table` patched — Task 2.
- [x] Pydantic test, no prior registration — Task 1, `test_pydantic_model_no_prior_registration`.
- [x] Dataclass test, no prior registration — Task 1, `test_dataclass_no_prior_registration`.
- [x] Idempotency (no double-registration error) — Task 1, `test_pydantic_no_double_registration_error`.
- [x] No pod regression — Task 1, `test_pod_registration_not_broken`.

**Placeholder scan:** No TBDs, no vague "add error handling" steps. ✓

**Type consistency:** `ensure_types_registered_for_schemas` used in Task 2 is the same method called in Task 1's test (indirectly via DictSource). ✓
