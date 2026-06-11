# ENG-389: Empty Container Type Inference Fix — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `list[Any]` and `dict[Any, Any]` (inferred from empty containers) convertible to Arrow types by mapping `typing.Any` ↔ `pa.null()` in `UniversalTypeConverter`.

**Architecture:** Two targeted edits to `universal_converter.py`: add `Any: pa.null()` to the type map (forward path) and add an explicit `pa.null()` check in `_convert_arrow_to_python` (reverse path). Remove one dead-code block. No inference changes.

**Tech Stack:** Python, PyArrow, `uv run pytest`.

**Spec:** `superpowers/specs/2026-06-08-eng-389-empty-container-type-inference.md`

---

## File Map

| File | Action | What changes |
|---|---|---|
| `src/orcapod/semantic_types/universal_converter.py` | Modify | Add `Any: pa.null()` to type map; add `pa.null()` case in `_convert_arrow_to_python`; remove dead `Any` hint block |
| `tests/test_semantic_types/test_universal_converter.py` | Modify | Add 6 converter/round-trip tests |
| `tests/test_semantic_types/test_pydata_utils.py` | Modify | Add 2 inference documentation tests |
| `DESIGN_ISSUES.md` | Modify | Add resolved entry for ENG-389 |

---

## Task 1: Write failing converter tests

**Files:**
- Modify: `tests/test_semantic_types/test_universal_converter.py`

- [ ] **Step 1: Append the six new tests to the end of the file**

Open `tests/test_semantic_types/test_universal_converter.py` and append:

```python
# ---------------------------------------------------------------------------
# ENG-389: Any <-> pa.null() round-trip
# ---------------------------------------------------------------------------

from typing import Any


def test_any_to_arrow_type():
    """typing.Any maps to pa.null()."""
    assert universal_converter.python_type_to_arrow_type(Any) == pa.null()


def test_list_any_to_arrow_type():
    """list[Any] maps to pa.large_list(pa.null())."""
    assert (
        universal_converter.python_type_to_arrow_type(list[Any])
        == pa.large_list(pa.null())
    )


def test_dict_any_any_to_arrow_type():
    """dict[Any, Any] maps to pa.large_list(pa.struct([("key", pa.null()), ("value", pa.null())]))."""
    expected = pa.large_list(
        pa.struct([("key", pa.null()), ("value", pa.null())])
    )
    assert universal_converter.python_type_to_arrow_type(dict[Any, Any]) == expected


def test_null_arrow_to_any_python_type():
    """pa.null() maps back to typing.Any."""
    assert universal_converter.arrow_type_to_python_type(pa.null()) is Any


def test_empty_container_inference_to_arrow_no_error():
    """Inferring schema from empty containers and converting to Arrow does not raise."""
    from orcapod.semantic_types.pydata_utils import infer_python_schema_from_pylist_data
    from orcapod.semantic_types.universal_converter import UniversalTypeConverter

    schema = infer_python_schema_from_pylist_data([{"items": [], "meta": {}}])
    converter = UniversalTypeConverter()
    # Must not raise ValueError: Unsupported Python type: typing.Any
    arrow_schema = converter.python_schema_to_arrow_schema(schema)
    assert "items" in [f.name for f in arrow_schema]
    assert "meta" in [f.name for f in arrow_schema]


def test_pyarrow_empty_list_with_null_type():
    """PyArrow accepts empty-list values for pa.large_list(pa.null()) columns."""
    schema = pa.schema([
        pa.field("items", pa.large_list(pa.null())),
        pa.field("meta", pa.large_list(pa.struct([("key", pa.null()), ("value", pa.null())]))),
    ])
    table = pa.Table.from_pylist([{"items": [], "meta": []}], schema=schema)
    assert table.num_rows == 1
    assert table.schema.field("items").type == pa.large_list(pa.null())
```

- [ ] **Step 2: Run the new tests to confirm they fail**

```
uv run pytest tests/test_semantic_types/test_universal_converter.py::test_any_to_arrow_type tests/test_semantic_types/test_universal_converter.py::test_list_any_to_arrow_type tests/test_semantic_types/test_universal_converter.py::test_dict_any_any_to_arrow_type tests/test_semantic_types/test_universal_converter.py::test_null_arrow_to_any_python_type tests/test_semantic_types/test_universal_converter.py::test_empty_container_inference_to_arrow_no_error tests/test_semantic_types/test_universal_converter.py::test_pyarrow_empty_list_with_null_type -v
```

Expected: all 6 FAIL. The first four fail with `AssertionError` or `ValueError: Unsupported Python type: typing.Any`. The fifth fails with `ValueError`. The sixth may pass (PyArrow assumption check) — if it does, that is acceptable.

---

## Task 2: Implement the fix in `universal_converter.py`

**Files:**
- Modify: `src/orcapod/semantic_types/universal_converter.py`

### Change A — add `Any: pa.null()` to the type map

- [ ] **Step 1: Open `_get_python_to_arrow_map()` (around line 61) and add the `Any` entry**

Find this block:

```python
    _PYTHON_TO_ARROW_MAP = {
        # Python built-ins
        int: pa.int64(),
        float: pa.float64(),
        str: pa.large_string(),  # Use large_string by default for Polars compatibility
        bool: pa.bool_(),
        bytes: pa.large_binary(),  # Use large_binary by default for Polars compatibility
```

Replace with:

```python
    _PYTHON_TO_ARROW_MAP = {
        # Python built-ins
        int: pa.int64(),
        float: pa.float64(),
        str: pa.large_string(),  # Use large_string by default for Polars compatibility
        bool: pa.bool_(),
        bytes: pa.large_binary(),  # Use large_binary by default for Polars compatibility
        # typing.Any — used when element type is unknown (e.g. inferred from empty containers)
        Any: pa.null(),
```

### Change B — add `pa.null()` case to `_convert_arrow_to_python()`

- [ ] **Step 2: Open `_convert_arrow_to_python()` (around line 496) and add the null check**

Find this block at the start of `_convert_arrow_to_python`:

```python
        # Handle basic types
        if pa.types.is_integer(arrow_type):
            return int
        elif pa.types.is_floating(arrow_type):
            return float
```

Replace with:

```python
        # Handle null type — maps to Any (unknown element type, e.g. from empty containers)
        if pa.types.is_null(arrow_type):
            return Any

        # Handle basic types
        if pa.types.is_integer(arrow_type):
            return int
        elif pa.types.is_floating(arrow_type):
            return float
```

### Change C — remove dead `Any` hint block from `_convert_python_to_arrow()`

- [ ] **Step 3: Remove the now-unreachable `Any` hint in `_convert_python_to_arrow()` (around lines 423–432)**

Find and remove this block entirely:

```python
            hint = ""
            if python_type is Any:
                hint = (
                    " Hint: typing.Any usually appears when an Arrow type had "
                    "no mapping in arrow_type_to_python_type (check warnings). "
                    "It can also come from schema inference on empty containers "
                    "(e.g. {} infers as dict[Any, Any])."
                )
            raise ValueError(
                f"Unsupported Python type: {python_type}.{hint}"
            )
```

Replace with:

```python
            raise ValueError(f"Unsupported Python type: {python_type}.")
```

- [ ] **Step 4: Run all six new tests to confirm they now pass**

```
uv run pytest tests/test_semantic_types/test_universal_converter.py::test_any_to_arrow_type tests/test_semantic_types/test_universal_converter.py::test_list_any_to_arrow_type tests/test_semantic_types/test_universal_converter.py::test_dict_any_any_to_arrow_type tests/test_semantic_types/test_universal_converter.py::test_null_arrow_to_any_python_type tests/test_semantic_types/test_universal_converter.py::test_empty_container_inference_to_arrow_no_error tests/test_semantic_types/test_universal_converter.py::test_pyarrow_empty_list_with_null_type -v
```

Expected: all 6 PASS.

- [ ] **Step 5: Run the full `test_universal_converter.py` suite to check for regressions**

```
uv run pytest tests/test_semantic_types/test_universal_converter.py -v
```

Expected: all existing tests still PASS.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/semantic_types/universal_converter.py \
        tests/test_semantic_types/test_universal_converter.py
git commit -m "fix(type-inference): map Any to pa.null() in UniversalTypeConverter

Empty containers ([] / {}) infer element type as typing.Any, which previously
raised ValueError in python_type_to_arrow_type. Now Any maps to pa.null() (Arrow's
canonical unknown-type), and pa.null() maps back to Any on the reverse path.

Closes ENG-389"
```

---

## Task 3: Add inference documentation tests to `test_pydata_utils.py`

These tests document that inference of empty containers already yields `list[Any]` /
`dict[Any, Any]` (the inference is correct; the fix was in the converter). They should
pass immediately.

**Files:**
- Modify: `tests/test_semantic_types/test_pydata_utils.py`

- [ ] **Step 1: Append the two new tests to the end of the file**

```python
# ---------------------------------------------------------------------------
# ENG-389: empty container inference produces list[Any] / dict[Any, Any]
# ---------------------------------------------------------------------------

from typing import Any


def test_infer_empty_list_schema():
    """A field whose only value is [] infers as list[Any]."""
    schema = pydata_utils.infer_python_schema_from_pylist_data([{"items": []}])
    assert schema["items"] == list[Any]


def test_infer_empty_dict_schema():
    """A field whose only value is {} infers as dict[Any, Any]."""
    schema = pydata_utils.infer_python_schema_from_pylist_data([{"meta": {}}])
    assert schema["meta"] == dict[Any, Any]
```

- [ ] **Step 2: Run the two new tests to confirm they pass**

```
uv run pytest tests/test_semantic_types/test_pydata_utils.py::test_infer_empty_list_schema tests/test_semantic_types/test_pydata_utils.py::test_infer_empty_dict_schema -v
```

Expected: both PASS (inference already behaves correctly).

- [ ] **Step 3: Run the full `test_pydata_utils.py` suite to check for regressions**

```
uv run pytest tests/test_semantic_types/test_pydata_utils.py -v
```

Expected: all tests PASS.

- [ ] **Step 4: Commit**

```bash
git add tests/test_semantic_types/test_pydata_utils.py
git commit -m "test(type-inference): document empty container inference as list[Any]/dict[Any, Any]

ENG-389"
```

---

## Task 4: Update `DESIGN_ISSUES.md`

**Files:**
- Modify: `DESIGN_ISSUES.md`

- [ ] **Step 1: Add a new resolved entry**

Append the following section to `DESIGN_ISSUES.md` (after the last existing entry):

```markdown
---

## `src/orcapod/semantic_types/universal_converter.py`

### UC1 — `python_type_to_arrow_type` raised on `typing.Any` from empty-container inference
**Status:** resolved
**Severity:** medium
**Issue:** ENG-389

`_infer_list_type` and `_infer_dict_type` in `pydata_utils.py` return `list[Any]` /
`dict[Any, Any]` when all sampled containers are empty (no elements to inspect). Passing
these inferred types to `python_type_to_arrow_type` raised `ValueError: Unsupported
Python type: typing.Any`.

**Fix:** Added `Any: pa.null()` to `_PYTHON_TO_ARROW_MAP` (forward path) and an explicit
`pa.types.is_null → Any` check to `_convert_arrow_to_python` (reverse path). `pa.null()`
is Arrow's canonical "unknown/no-type" marker; empty containers have no elements to
validate, so the encoding is semantically correct. The now-unreachable `Any`-specific hint
in the error branch was removed.
```

- [ ] **Step 2: Run the full semantic-types test suite to confirm nothing regressed**

```
uv run pytest tests/test_semantic_types/ -v
```

Expected: all tests PASS.

- [ ] **Step 3: Commit**

```bash
git add DESIGN_ISSUES.md
git commit -m "docs(design-issues): log ENG-389 Any→pa.null() fix as resolved"
```

---

## Final verification

- [ ] **Run the full test suite**

```
uv run pytest tests/ -x -q
```

Expected: all tests pass, no errors.
