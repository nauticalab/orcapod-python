# ITL-10: Dataclass → DataFrame Conversion — Test Coverage Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 6 tests (two new files) confirming that dataclass-output `FunctionNode`s produce correct Polars DataFrames, and that plain Arrow structs round-trip safely through the converter as dynamic TypedDicts.

**Architecture:** Two independent test files — one end-to-end pipeline layer (`test_core/nodes/`), one converter unit layer (`test_extension_types/`). No production code changes. All dataclass and function-pod fixtures live at module level (not inside test functions) to satisfy `get_type_hints` and the FQCN registration requirement.

**Tech Stack:** `pytest`, `pyarrow`, `polars`, `orcapod`, `dataclasses`

---

## File Map

| File | Status | Responsibility |
|---|---|---|
| `tests/test_core/nodes/test_function_node_dataclass.py` | **Create** | End-to-end: `FunctionNode` with dataclass return type → `as_df()` |
| `tests/test_extension_types/test_universal_converter_struct.py` | **Create** | Unit: plain struct → dynamic TypedDict → write-back round-trip |

---

## Task 1: End-to-end pipeline tests — populated node

**Files:**
- Create: `tests/test_core/nodes/test_function_node_dataclass.py`

- [ ] **Step 1: Write the failing test (populated node)**

Create `tests/test_core/nodes/test_function_node_dataclass.py` with the following content:

```python
"""End-to-end tests: FunctionNode with a dataclass return type → as_df().

Fixtures are defined at module level so that get_type_hints() can resolve
annotations (local-scope classes are not reachable via __globals__) and so
DataclassLogicalTypeFactory can build a stable FQCN for registration.
"""
from __future__ import annotations

import dataclasses

import polars as pl
import pytest

import orcapod as op
from orcapod.core.sources import DictSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import PipelineJob


# ---------------------------------------------------------------------------
# Module-level fixtures
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class _SumResult:
    total: int
    delta: int


@op.function_pod("result")
def take_sum(a: int, b: int) -> _SumResult:
    return _SumResult(a + b, a - b)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_as_df_dataclass_column_populated():
    """FunctionNode with a dataclass return type produces a correct Polars DataFrame."""
    store = InMemoryArrowDatabase()
    job = PipelineJob(store=store)
    source = DictSource([{"id": 0, "a": 5, "b": 3}], tag_columns=["id"])

    with job:
        take_sum.pod(source)

    job.run()
    df = job.nodes["take_sum"].as_df()

    # Shape: one row, two visible columns (tag + data)
    assert df.shape[0] == 1
    assert "id" in df.columns
    assert "result" in df.columns

    # result column must NOT be Null/Any — it should be a struct or extension type
    assert df["result"].dtype != pl.Null

    # Values are correct
    row = df["result"][0]
    assert row["total"] == 8
    assert row["delta"] == 2
```

- [ ] **Step 2: Run to confirm it fails before the test file exists**

```bash
uv run pytest tests/test_core/nodes/test_function_node_dataclass.py -v
```

Expected: `ERROR — file not found` or import error (file doesn't exist yet). This confirms the test file needs to be created (it was just created in step 1, so this step verifies the test runs and passes — see next step).

- [ ] **Step 3: Run the test to confirm it passes**

```bash
uv run pytest tests/test_core/nodes/test_function_node_dataclass.py::test_as_df_dataclass_column_populated -v
```

Expected: `PASSED`

- [ ] **Step 4: Commit**

```bash
git add tests/test_core/nodes/test_function_node_dataclass.py
git commit -m "test(function_node): add end-to-end dataclass as_df populated test (ITL-10)"
```

---

## Task 2: End-to-end pipeline tests — empty node schema

**Files:**
- Modify: `tests/test_core/nodes/test_function_node_dataclass.py`

- [ ] **Step 1: Add the empty-node test**

Append the following two test functions to `tests/test_core/nodes/test_function_node_dataclass.py`:

```python
def test_as_df_dataclass_column_empty_node():
    """Unrun FunctionNode returns a zero-row DataFrame with the dataclass column present."""
    job = PipelineJob(store=InMemoryArrowDatabase())
    source = DictSource([{"id": 0, "a": 5, "b": 3}], tag_columns=["id"])

    with job:
        take_sum.pod(source)

    # Deliberately do NOT call job.run() — node has no computed results yet.
    df = job.nodes["take_sum"].as_df()

    assert df.shape[0] == 0          # zero rows
    assert "id" in df.columns        # tag column present
    assert "result" in df.columns    # data column present even with no data
    assert df["result"].dtype != pl.Null


def test_empty_schema_matches_nonempty_schema():
    """Empty and populated as_df() for the same node type share identical schemas."""
    source = DictSource([{"id": 0, "a": 5, "b": 3}], tag_columns=["id"])

    # Populated node
    job_full = PipelineJob(store=InMemoryArrowDatabase())
    with job_full:
        take_sum.pod(source)
    job_full.run()
    full_df = job_full.nodes["take_sum"].as_df()

    # Unrun node — same pod, fresh job
    job_empty = PipelineJob(store=InMemoryArrowDatabase())
    with job_empty:
        take_sum.pod(source)
    empty_df = job_empty.nodes["take_sum"].as_df()

    assert full_df.shape[0] > 0
    assert empty_df.shape[0] == 0
    assert set(empty_df.columns) == set(full_df.columns)
    # Column names and types must match exactly
    assert empty_df.schema == full_df.schema
```

- [ ] **Step 2: Run the new tests**

```bash
uv run pytest tests/test_core/nodes/test_function_node_dataclass.py -v
```

Expected: all 3 tests `PASSED`

- [ ] **Step 3: Commit**

```bash
git add tests/test_core/nodes/test_function_node_dataclass.py
git commit -m "test(function_node): add empty-node schema tests for dataclass output (ITL-10)"
```

---

## Task 3: Converter unit tests — plain struct → TypedDict

**Files:**
- Create: `tests/test_extension_types/test_universal_converter_struct.py`

- [ ] **Step 1: Write the first two converter tests**

Create `tests/test_extension_types/test_universal_converter_struct.py` with the following content:

```python
"""Unit tests for UniversalTypeConverter behaviour with plain Arrow struct types.

A plain struct (no ARROW:extension:* field metadata) should be inferred as a
dynamic TypedDict by arrow_schema_to_python_schema, and that TypedDict should
round-trip back to the identical Arrow struct type via python_type_to_arrow_type.
"""
from __future__ import annotations

import typing

import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context


# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

PLAIN_STRUCT = pa.struct([
    pa.field("total", pa.int64()),
    pa.field("delta", pa.int64()),
])


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def converter():
    """Fresh UniversalTypeConverter from the default DataContext."""
    return get_default_context().type_converter


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_plain_struct_infers_as_dynamic_typeddict(converter):
    """arrow_schema_to_python_schema on a plain struct returns a dynamic TypedDict, not Any."""
    schema = pa.schema([pa.field("result", PLAIN_STRUCT, nullable=False)])
    python_schema = converter.arrow_schema_to_python_schema(schema)

    result_type = python_schema["result"]

    assert typing.is_typeddict(result_type), (
        f"Expected a TypedDict, got {result_type!r}"
    )
    assert converter.is_dynamic_typeddict(result_type), (
        "Expected converter.is_dynamic_typeddict() to return True"
    )
    assert not hasattr(result_type, "__dataclass_fields__"), (
        "Plain struct must not be mistaken for a dataclass"
    )
    assert result_type is not typing.Any, (
        "arrow_schema_to_python_schema must not return Any for a plain struct"
    )


def test_dynamic_typeddict_roundtrips_to_struct(converter):
    """The TypedDict returned for a plain struct maps back to the identical Arrow struct."""
    schema = pa.schema([pa.field("result", PLAIN_STRUCT, nullable=False)])
    python_schema = converter.arrow_schema_to_python_schema(schema)

    result_type = python_schema["result"]
    arrow_type_back = converter.python_type_to_arrow_type(result_type)

    assert arrow_type_back == PLAIN_STRUCT, (
        f"Round-trip failed: expected {PLAIN_STRUCT!r}, got {arrow_type_back!r}"
    )
```

- [ ] **Step 2: Run the tests**

```bash
uv run pytest tests/test_extension_types/test_universal_converter_struct.py -v
```

Expected: both tests `PASSED`

- [ ] **Step 3: Commit**

```bash
git add tests/test_extension_types/test_universal_converter_struct.py
git commit -m "test(converter): add plain struct → dynamic TypedDict inference tests (ITL-10)"
```

---

## Task 4: Converter unit test — TypedDict write-back

**Files:**
- Modify: `tests/test_extension_types/test_universal_converter_struct.py`

- [ ] **Step 1: Add the write-back test**

Append the following test to `tests/test_extension_types/test_universal_converter_struct.py`:

```python
def test_dynamic_typeddict_write_back(converter):
    """python_dicts_to_struct_dicts with a TypedDict schema correctly writes struct data."""
    schema = pa.schema([pa.field("result", PLAIN_STRUCT, nullable=False)])
    python_schema = converter.arrow_schema_to_python_schema(schema)

    data = [
        {"result": {"total": 8, "delta": 2}},
        {"result": {"total": 17, "delta": 3}},
    ]
    struct_dicts = converter.python_dicts_to_struct_dicts(data, python_schema=python_schema)
    table = pa.Table.from_pylist(struct_dicts, schema=schema)

    assert table.num_rows == 2
    assert table.schema == schema

    rows = table.column("result").to_pylist()
    assert rows[0] == {"total": 8, "delta": 2}
    assert rows[1] == {"total": 17, "delta": 3}
```

- [ ] **Step 2: Run the full converter test file**

```bash
uv run pytest tests/test_extension_types/test_universal_converter_struct.py -v
```

Expected: all 3 tests `PASSED`

- [ ] **Step 3: Commit**

```bash
git add tests/test_extension_types/test_universal_converter_struct.py
git commit -m "test(converter): add dynamic TypedDict write-back round-trip test (ITL-10)"
```

---

## Task 5: Full test suite verification

- [ ] **Step 1: Run all new tests together**

```bash
uv run pytest tests/test_core/nodes/test_function_node_dataclass.py tests/test_extension_types/test_universal_converter_struct.py -v
```

Expected: all 6 tests `PASSED`, no warnings about unregistered types.

- [ ] **Step 2: Run the broader suites for regression**

```bash
uv run pytest tests/test_core/nodes/ tests/test_extension_types/ -q
```

Expected: all existing tests continue to pass; no new failures.

- [ ] **Step 3: Create the PR**

Use `sensei:create-pr` to open a PR against `main`.

The PR description must include:
- `Fixes ITL-10`
- A summary of the staleness finding (core bug was already fixed by FN1; this PR adds the required test coverage)
- The two test files added

---

## Self-Review Checklist

**Spec coverage:**
- ✅ `FunctionNode` with dataclass → `as_df()` → correct DataFrame → Task 1
- ✅ Unrun node → `as_df()` → zero-row DataFrame with correct schema → Task 2 (first test)
- ✅ Empty schema matches non-empty schema → Task 2 (second test)
- ✅ Plain struct → dynamic TypedDict (not `Any`) → Task 3
- ✅ TypedDict → `python_type_to_arrow_type` → original struct → Task 3
- ✅ TypedDict as schema in `python_dicts_to_struct_dicts` → correct Arrow table → Task 4

**Placeholder scan:** None found.

**Type consistency:**
- `take_sum` (function pod) used consistently across Task 1 and Task 2
- `converter` fixture used consistently across Tasks 3 and 4
- `PLAIN_STRUCT` constant defined once in Task 3, reused in Task 4
- `python_schema`, `result_type`, `arrow_type_back` names consistent across tasks
