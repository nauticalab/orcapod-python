# Union-Typed Function Inputs Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix `FunctionPod` construction to accept union-typed input arguments (e.g. `x: str | Path`) and add tests confirming construction, correct stream binding, and mismatch rejection.

**Architecture:** `_FunctionPodBase.__init__` calls `ensure_types_registered_for_schemas` on both input and output schemas. The fix modifies that method to walk union branches and register each non-`None` branch individually rather than forwarding the whole union type to `register_python_class` (which rejects it). Stream-binding validation via `check_schema_compatibility` / `beartype.door.is_subhint` already works correctly and requires no changes.

**Tech Stack:** Python 3.12, PyArrow, beartype, uv (test runner: `uv run pytest`)

---

## File Map

| File | Action | Purpose |
|---|---|---|
| `src/orcapod/semantic_types/universal_converter.py` | Modify | Walk union branches in `ensure_types_registered_for_schemas`; add clarifying comment in `_register_python_class_impl` |
| `src/orcapod/core/function_pod.py` | Modify | Add inline comments explaining union-input acceptance contract |
| `tests/test_core/function_pod/test_union_typed_inputs.py` | Create | All tests for this feature |
| `DESIGN_ISSUES.md` | Modify | Add resolved bug entry |

---

### Task 1: Check out the feature branch

**Files:** none (git only)

- [ ] **Step 1: Create and check out the branch**

```bash
git checkout -b eywalker/itl-452-verify-union-typed-function-inputs-are-accepted-concrete-pod
```

- [ ] **Step 2: Verify you are on the correct branch**

```bash
git branch --show-current
```

Expected output:
```
eywalker/itl-452-verify-union-typed-function-inputs-are-accepted-concrete-pod
```

---

### Task 2: Write failing tests

**Files:**
- Create: `tests/test_core/function_pod/test_union_typed_inputs.py`

- [ ] **Step 1: Create the test file**

`tests/test_core/function_pod/test_union_typed_inputs.py`:

```python
"""
Tests for union-typed function inputs (ITL-452).

Verifies:
- FunctionPod construction succeeds when an input arg is declared with a union type.
- Binding a concrete stream of a matching type succeeds and the input schema
  reflects the concrete branch (accessible via pod_stream.upstreams[0].output_schema()).
- Binding a stream whose type is not a member of the union raises ValueError.
- Data processing works correctly for each concrete branch.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.streams import ArrowTableStream


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_str_stream(n: int = 2) -> ArrowTableStream:
    """Stream with tag=id (int64), data=x (large_string / str)."""
    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("x", pa.large_string(), nullable=False),
    ])
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array([f"value_{i}" for i in range(n)], type=pa.large_string()),
        },
        schema=schema,
    )
    return ArrowTableStream(table, tag_columns=["id"])


def make_path_stream(n: int = 2) -> ArrowTableStream:
    """Stream with tag=id (int64), data=x (orcapod.path / Path)."""
    ctx = get_default_context()
    path_arrow_type = ctx.type_converter.python_type_to_arrow_type(Path)
    storage = pa.array([f"/tmp/test_{i}" for i in range(n)], type=pa.large_string())
    path_array = pa.ExtensionArray.from_storage(path_arrow_type, storage)
    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("x", path_arrow_type, nullable=False),
    ])
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": path_array,
        },
        schema=schema,
    )
    return ArrowTableStream(table, tag_columns=["id"])


def make_int_stream(n: int = 2) -> ArrowTableStream:
    """Stream with tag=id (int64), data=x (int64) — incompatible with str | Path."""
    schema = pa.schema([
        pa.field("id", pa.int64(), nullable=False),
        pa.field("x", pa.int64(), nullable=False),
    ])
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
        },
        schema=schema,
    )
    return ArrowTableStream(table, tag_columns=["id"])


def make_union_pod() -> FunctionPod:
    """FunctionPod whose input x accepts str | Path."""
    def union_fn(x: str | Path) -> str:
        return str(x)

    return FunctionPod(PythonDataFunction(union_fn, output_keys="result"))


# ---------------------------------------------------------------------------
# Construction tests
# ---------------------------------------------------------------------------


class TestUnionInputConstruction:
    def test_pod_construction_succeeds(self):
        """Creating a FunctionPod with a union-typed input must not raise."""
        def union_fn(x: str | Path) -> str:
            return str(x)

        # Must not raise ValueError about complex unions
        pod = FunctionPod(PythonDataFunction(union_fn, output_keys="result"))
        assert pod is not None

    def test_input_schema_preserves_union_type(self):
        """The data function's input_data_schema must record the full union type."""
        def union_fn(x: str | Path) -> str:
            return str(x)

        pod = FunctionPod(PythonDataFunction(union_fn, output_keys="result"))
        assert pod.data_function.input_data_schema["x"] == str | Path


# ---------------------------------------------------------------------------
# Stream binding tests
# ---------------------------------------------------------------------------


class TestUnionInputStreamBinding:
    def test_bind_str_stream_succeeds(self):
        """Binding a str-typed stream to a str | Path pod must succeed."""
        pod = make_union_pod()
        pod_stream = pod.process(make_str_stream())
        # The upstream stream's data schema must report the concrete str type
        _, input_data_schema = pod_stream.upstreams[0].output_schema()
        assert input_data_schema["x"] == str

    def test_bind_path_stream_succeeds(self):
        """Binding a Path-typed stream to a str | Path pod must succeed."""
        pod = make_union_pod()
        pod_stream = pod.process(make_path_stream())
        # The upstream stream's data schema must report the concrete Path type
        _, input_data_schema = pod_stream.upstreams[0].output_schema()
        assert input_data_schema["x"] == Path

    def test_bind_incompatible_type_raises(self):
        """Binding a stream whose type is not in the union must raise ValueError."""
        pod = make_union_pod()
        with pytest.raises(ValueError, match="not compatible"):
            pod.process(make_int_stream())


# ---------------------------------------------------------------------------
# Data processing tests
# ---------------------------------------------------------------------------


class TestUnionInputDataProcessing:
    def test_process_str_input_yields_correct_output(self):
        """Processing a str-typed stream through a str | Path pod gives correct results."""
        def union_fn(x: str | Path) -> str:
            return str(x).upper()

        pod = FunctionPod(PythonDataFunction(union_fn, output_keys="result"))
        pod_stream = pod.process(make_str_stream(n=2))
        results = list(pod_stream.iter_data())
        assert len(results) == 2
        _, out_data = results[0]
        assert out_data.as_dict()["result"] == "VALUE_0"

    def test_process_path_input_yields_correct_output(self):
        """Processing a Path-typed stream through a str | Path pod gives correct results."""
        def union_fn(x: str | Path) -> str:
            return str(x)

        pod = FunctionPod(PythonDataFunction(union_fn, output_keys="result"))
        pod_stream = pod.process(make_path_stream(n=2))
        results = list(pod_stream.iter_data())
        assert len(results) == 2
        _, out_data = results[0]
        assert out_data.as_dict()["result"] == "/tmp/test_0"
```

- [ ] **Step 2: Run the tests to confirm they fail**

```bash
uv run pytest tests/test_core/function_pod/test_union_typed_inputs.py -v
```

Expected: ALL tests FAIL — construction tests fail with:
```
ValueError: Complex unions with multiple non-None types are not supported: str | pathlib.Path
```

---

### Task 3: Fix `ensure_types_registered_for_schemas`

**Files:**
- Modify: `src/orcapod/semantic_types/universal_converter.py`

- [ ] **Step 1: Replace the body of `ensure_types_registered_for_schemas`**

In `src/orcapod/semantic_types/universal_converter.py`, replace the existing method body (lines ~227–232):

```python
    def ensure_types_registered_for_schemas(self, *schemas: Schema) -> None:
        """Ensure a LogicalType is registered for every annotation in schemas.

        Calls ``register_python_class`` for each annotation, which recursively
        resolves nested types and synthesises via factory if needed.
        When no ``LogicalTypeRegistry`` is configured, this is a no-op.

        Union-typed annotations (e.g. ``str | Path``) are handled by registering
        each non-``None`` branch individually. Arrow has no union storage type,
        so the union itself is never registered; instead each concrete branch's
        ``LogicalType`` is made available so it is ready when a stream of that
        type is bound.

        Args:
            *schemas: One or more ``Schema`` mappings (column name → Python type).

        Raises:
            TypeError: If a leaf class has no registered ``LogicalType`` and
                no registered factory covers it.
        """
        if self._logical_type_registry is None:
            return
        for schema in schemas:
            for annotation in schema.values():
                origin = get_origin(annotation)
                if origin is typing.Union or origin is types.UnionType:
                    # Union types (e.g. str | Path) are valid in function input
                    # schemas — they express that the pod accepts either concrete
                    # type. Register each non-None branch so its LogicalType is
                    # available when a stream is bound; the union itself has no
                    # Arrow representation.
                    for branch in get_args(annotation):
                        if branch is not type(None):
                            self.register_python_class(branch)
                else:
                    self.register_python_class(annotation)
```

- [ ] **Step 2: Add a note at the complex-union raise in `_register_python_class_impl`**

Locate the `raise ValueError` for complex unions in `_register_python_class_impl` (~line 295) and add a preceding comment:

```python
        # Optional[T] / T | None → strip None arm
        if origin is typing.Union or origin is _types_mod.UnionType:
            non_none = [a for a in args if a is not type(None)]
            if len(non_none) == 1:
                return self.register_python_class(non_none[0])
            # Direct callers must not pass complex unions here — there is no
            # Arrow type for str | Path.  For schema-level registration
            # (where union-typed input args are valid), use
            # ensure_types_registered_for_schemas(), which registers each
            # non-None branch individually.
            raise ValueError(
                f"Complex unions with multiple non-None types are not supported: "
                f"{annotation!r}. Only Optional[T] (T | None) is allowed."
            )
```

- [ ] **Step 3: Run the tests to confirm they now pass**

```bash
uv run pytest tests/test_core/function_pod/test_union_typed_inputs.py -v
```

Expected: ALL 7 tests PASS.

- [ ] **Step 4: Run the full test suite to check for regressions**

```bash
uv run pytest tests/ -x -q
```

Expected: all existing tests continue to pass.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/semantic_types/universal_converter.py \
        tests/test_core/function_pod/test_union_typed_inputs.py
git commit -m "fix(types): accept union-typed function inputs in ensure_types_registered_for_schemas (ITL-452)"
```

---

### Task 4: Add documentation comments to `function_pod.py`

**Files:**
- Modify: `src/orcapod/core/function_pod.py`

- [ ] **Step 1: Add inline comment to `_FunctionPodBase.__init__`**

Locate the `ensure_types_registered_for_schemas` call in `_FunctionPodBase.__init__` (~line 78) and add a comment immediately above it:

```python
        # Union-typed input args (e.g. x: str | Path) are deliberately accepted
        # at construction time. ensure_types_registered_for_schemas registers
        # each non-None branch individually; the union is only resolved to a
        # concrete branch when a stream is bound — see _validate_input_schema.
        self.data_context.type_converter.ensure_types_registered_for_schemas(
            data_function.input_data_schema,
            data_function.output_data_schema,
        )
```

- [ ] **Step 2: Add inline comment to `_validate_input_schema`**

Locate `_validate_input_schema` (~line 129) and add a comment inside the method:

```python
    def _validate_input_schema(self, input_schema: Schema) -> None:
        expected_data_schema = self.data_function.input_data_schema
        # When expected_data_schema contains a union type (e.g. str | Path),
        # check_schema_compatibility uses beartype.door.is_subhint which accepts
        # any concrete branch: is_subhint(str, str | Path) → True,
        # is_subhint(int, str | Path) → False.
        if not schema_utils.check_schema_compatibility(
            input_schema, expected_data_schema
        ):
            raise ValueError(
                f"Incoming data data type {input_schema} is not compatible with expected input schema {expected_data_schema}"
            )
```

- [ ] **Step 3: Run tests to verify no regression from comment-only edits**

```bash
uv run pytest tests/test_core/function_pod/ -q
```

Expected: all function_pod tests pass.

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/core/function_pod.py
git commit -m "docs(function_pod): document union-typed input acceptance and bind-time resolution (ITL-452)"
```

---

### Task 5: Update DESIGN_ISSUES.md

**Files:**
- Modify: `DESIGN_ISSUES.md`

- [ ] **Step 1: Add a resolved entry**

Add the following new section to `DESIGN_ISSUES.md` (insert after the last resolved entry, before any open entries, or at the bottom of the file — follow the existing ordering convention):

```markdown
## `src/orcapod/semantic_types/universal_converter.py`

### UC1 — Premature rejection of union-typed function inputs
**Status:** resolved
**Severity:** high
**Issue:** ITL-452

`ensure_types_registered_for_schemas` forwarded each schema annotation directly
to `register_python_class`, which rejects complex union types (``str | Path``)
because Arrow has no native union storage type. This caused ``FunctionPod``
construction to fail with a ``ValueError`` whenever a function declared a
union-typed input argument, even though union inputs are semantically valid
(they express that the pod accepts either concrete type, with the concrete
branch resolved at stream-binding time).

**Fix:** Modified `ensure_types_registered_for_schemas` to detect union
annotations and register each non-``None`` branch individually, leaving
``register_python_class`` unchanged (it correctly rejects unions when invoked
directly for explicit type conversion).
```

- [ ] **Step 2: Commit**

```bash
git add DESIGN_ISSUES.md
git commit -m "docs(design-issues): log UC1 union-typed input premature rejection as resolved (ITL-452)"
```

---

### Task 6: Final verification and push

- [ ] **Step 1: Run the full test suite one final time**

```bash
uv run pytest tests/ -q
```

Expected: all tests pass, no failures.

- [ ] **Step 2: Push the branch**

```bash
git push -u origin eywalker/itl-452-verify-union-typed-function-inputs-are-accepted-concrete-pod
```
