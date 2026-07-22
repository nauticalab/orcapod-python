# Empty Table Nullability Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix five empty-table construction sites that silently drop field nullability, causing `error_policy="continue"` pipelines to abort when a failing function feeds a `Join`.

**Architecture:** Add a single `make_empty_table(python_schema, type_converter)` utility to `arrow_utils.py` that uses the existing `python_schema_to_arrow_schema` method (which correctly preserves `nullable=False` for required fields). Replace all five buggy call sites with this utility. Write tests first at every step.

**Tech Stack:** Python, PyArrow, pytest, `uv run`

---

## File Map

| Action | Path | Purpose |
|---|---|---|
| Modify | `src/orcapod/utils/arrow_utils.py` | Add `make_empty_table()` |
| Modify | `src/orcapod/core/nodes/operator_node.py` | Fix `_make_empty_table()` body |
| Modify | `src/orcapod/pipeline/sync_orchestrator.py` | Fix `_materialize_as_stream()` empty branch |
| Modify | `src/orcapod/side_effects.py` | Fix `SideEffectFunctionStream.as_table()` and `SideEffectJobFunctionStream.as_table()` |
| Modify | `src/orcapod/core/sources/derived_source.py` | Fix `DerivedSource._get_stream()` empty branch |
| Modify | `tests/test_utils/test_arrow_utils.py` | Add `make_empty_table` unit tests |
| Create | `tests/test_pipeline/test_error_policy_continue.py` | Integration test for the bug scenario |
| Modify | `tests/test_core/nodes/test_function_node_iteration.py` | Strengthen nullability assertion |
| Modify | `tests/test_core/sources/test_derived_source.py` | Add nullability regression test |
| Modify | `tests/test_core/side_effect_function/test_side_effect_function_pod.py` | Add nullability regression test |
| Modify | `DESIGN_ISSUES.md` | Mark CC1 resolved |

---

## Task 1: Create the feature branch

**Files:** none (git only)

- [ ] **Step 1: Check out the feature branch**

```bash
cd /path/to/orcapod-python   # your actual working directory
git checkout -b eywalker/itl-563-empty-failed-outputs-lose-tag-nullability-and-abort
git branch --show-current
```

Expected output: `eywalker/itl-563-empty-failed-outputs-lose-tag-nullability-and-abort`

---

## Task 2: Unit tests for `make_empty_table` (write tests first, implementation not yet added)

**Files:**
- Modify: `tests/test_utils/test_arrow_utils.py`

- [ ] **Step 1: Write four failing tests at the bottom of the existing test file**

Append to `tests/test_utils/test_arrow_utils.py`:

```python
# ---------------------------------------------------------------------------
# make_empty_table — ITL-563
# ---------------------------------------------------------------------------


class TestMakeEmptyTable:
    """make_empty_table preserves field nullability from python_schema."""

    def _converter(self):
        from orcapod.contexts import DataContext
        return DataContext().type_converter

    def test_required_fields_are_non_nullable(self):
        """Plain types produce nullable=False Arrow fields."""
        from orcapod.utils.arrow_utils import make_empty_table

        schema = {"name": str, "count": int}
        table = make_empty_table(schema, self._converter())

        assert table.num_rows == 0
        assert table.schema.field("name").nullable is False
        assert table.schema.field("count").nullable is False

    def test_optional_fields_are_nullable(self):
        """Optional types produce nullable=True Arrow fields."""
        from orcapod.utils.arrow_utils import make_empty_table

        schema = {"name": str | None, "count": int | None}
        table = make_empty_table(schema, self._converter())

        assert table.num_rows == 0
        assert table.schema.field("name").nullable is True
        assert table.schema.field("count").nullable is True

    def test_mixed_nullability_per_field(self):
        """Mixed schema: required field non-nullable, optional field nullable."""
        from orcapod.utils.arrow_utils import make_empty_table

        schema = {"subject": str, "score": int | None}
        table = make_empty_table(schema, self._converter())

        assert table.num_rows == 0
        assert table.schema.field("subject").nullable is False
        assert table.schema.field("score").nullable is True

    def test_round_trips_through_arrow_table_stream(self):
        """Python schema → make_empty_table → ArrowTableStream.output_schema() is identity."""
        from orcapod.utils.arrow_utils import make_empty_table
        from orcapod.core.streams import ArrowTableStream

        converter = self._converter()
        python_schema = {"subject": str, "score": int | None}
        table = make_empty_table(python_schema, converter)

        # ArrowTableStream requires at least one data column; tag=subject, data=score
        stream = ArrowTableStream(table, tag_columns=["subject"])
        _, data_schema = stream.output_schema()

        assert data_schema["score"] == (int | None)
```

- [ ] **Step 2: Run tests to verify they fail (function not yet defined)**

```bash
uv run pytest tests/test_utils/test_arrow_utils.py::TestMakeEmptyTable -v
```

Expected: 4 failures with `ImportError` or `cannot import name 'make_empty_table'`.

---

## Task 3: Implement `make_empty_table` in `arrow_utils.py`

**Files:**
- Modify: `src/orcapod/utils/arrow_utils.py`

- [ ] **Step 1: Add the function after the last import block, before `schema_select`**

In `src/orcapod/utils/arrow_utils.py`, insert after line 14 (the `pa = LazyModule("pyarrow")` line) and before `def schema_select`:

```python

def make_empty_table(python_schema: "Mapping[str, Any]", type_converter: Any) -> "pa.Table":
    """Return a zero-row PyArrow table whose field nullability matches ``python_schema``.

    Uses ``python_schema_to_arrow_schema`` so that plain types (``str``, ``int``, …)
    produce ``nullable=False`` fields and Optional types (``str | None``) produce
    ``nullable=True`` fields. This preserves the bidirectional round-trip through
    ``ArrowTableStream.output_schema()``.

    Args:
        python_schema: Mapping of field name to Python type annotation.
        type_converter: A ``UniversalTypeConverter`` instance.

    Returns:
        A zero-row ``pa.Table`` with the correct Arrow schema.
    """
    arrow_schema = type_converter.python_schema_to_arrow_schema(python_schema)
    return pa.Table.from_batches([], schema=arrow_schema)
```

Also update the `Mapping` import at the top — it is already imported from `collections.abc`.

- [ ] **Step 2: Run unit tests to verify they pass**

```bash
uv run pytest tests/test_utils/test_arrow_utils.py::TestMakeEmptyTable -v
```

Expected: 4 tests PASS.

- [ ] **Step 3: Commit**

```bash
git add src/orcapod/utils/arrow_utils.py tests/test_utils/test_arrow_utils.py
git commit -m "feat(arrow_utils): add make_empty_table preserving field nullability (ITL-563)"
```

---

## Task 4: Fix `operator_node.py` — `_make_empty_table()`

**Files:**
- Modify: `src/orcapod/core/nodes/operator_node.py`
- Modify: `tests/test_core/nodes/test_function_node_iteration.py`

`operator_node.py` already has a `_make_empty_table()` method (lines 835–849) called from
three internal sites. We replace its body with a call to `arrow_utils.make_empty_table()`.

- [ ] **Step 1: Strengthen the existing test to assert nullability (it currently only checks column names)**

In `tests/test_core/nodes/test_function_node_iteration.py`, update `test_as_table_empty_schema_matches_non_empty_schema`:

```python
def test_as_table_empty_schema_matches_non_empty_schema():
    """as_table() empty table has the same columns and nullability as the populated table."""
    db = InMemoryArrowDatabase()
    node_after = _make_node(db=db)
    node_after.run()
    full_table = node_after.as_table()

    node_before = _make_node()
    empty_table = node_before.as_table()

    assert empty_table.num_rows == 0
    assert full_table.num_rows > 0
    assert set(empty_table.column_names) == set(full_table.column_names)
    # Nullability must match — this failed before the fix
    assert all(
        empty_table.schema.field(n).nullable == full_table.schema.field(n).nullable
        for n in empty_table.column_names
    )
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
uv run pytest tests/test_core/nodes/test_function_node_iteration.py::test_as_table_empty_schema_matches_non_empty_schema -v
```

Expected: FAIL — `nullable` flags differ for required fields.

- [ ] **Step 3: Fix the body of `_make_empty_table()` in `operator_node.py`**

Replace the current body of `_make_empty_table()` (lines 835–849) with:

```python
    def _make_empty_table(self) -> "pa.Table":
        """Build a zero-row PyArrow table matching this node's full output schema.

        Uses ``arrow_utils.make_empty_table`` so field nullability is preserved
        (``nullable=False`` for required fields, ``nullable=True`` for optional).
        Requires ``self._operator is not None`` (pre-existing limitation shared
        with ``_replay_from_cache``).
        """
        from orcapod.utils.arrow_utils import make_empty_table

        tag_schema, data_schema = self.output_schema()
        return make_empty_table(
            {**tag_schema, **data_schema},
            self.data_context.type_converter,
        )
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
uv run pytest tests/test_core/nodes/test_function_node_iteration.py::test_as_table_empty_schema_matches_non_empty_schema -v
```

Expected: PASS.

- [ ] **Step 5: Run the full `test_core/nodes` suite to check for regressions**

```bash
uv run pytest tests/test_core/nodes/ -v
```

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/core/nodes/operator_node.py tests/test_core/nodes/test_function_node_iteration.py
git commit -m "fix(operator_node): use make_empty_table to preserve field nullability (ITL-563)"
```

---

## Task 5: Fix `sync_orchestrator.py` — `_materialize_as_stream()` + integration test

**Files:**
- Create: `tests/test_pipeline/test_error_policy_continue.py`
- Modify: `src/orcapod/pipeline/sync_orchestrator.py`

- [ ] **Step 1: Write the failing integration tests**

Create `tests/test_pipeline/test_error_policy_continue.py`:

```python
"""Integration tests for error_policy='continue' + Join schema compatibility.

Regression for ITL-563: failed functions produced empty tables with wrong
nullable flags, causing Join to raise InputValidationError and abort the pipeline.
"""
from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators.join import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline.job import PipelineJob


def _make_source_with_required_field(subjects: list[str], values: list[int]) -> ArrowTableSource:
    """Source with tag=subject (str, non-nullable) and data=value (int, non-nullable)."""
    schema = pa.schema([
        pa.field("subject", pa.large_string(), nullable=False),
        pa.field("value", pa.int64(), nullable=False),
    ])
    table = pa.table(
        {"subject": subjects, "value": values},
        schema=schema,
    )
    return ArrowTableSource(table, tag_columns=["subject"])


def test_failed_function_with_join_does_not_abort_pipeline():
    """Topology: source → failing_function → Join ← source.

    Under error_policy='continue', the failing function should be logged,
    the Join should produce zero rows (empty × non-empty = empty), and
    orchestration should complete without raising.
    """
    src = _make_source_with_required_field(["a", "b"], [1, 2])

    def always_fails(value: int) -> int:
        raise RuntimeError("intentional failure")

    pf = PythonDataFunction(always_fails, output_keys="transformed")
    failing_pod = FunctionPod(pf)

    job = PipelineJob(name="test_join_continue", store=InMemoryArrowDatabase())
    with job:
        failing_out = failing_pod(src, label="failing")
        Join()(failing_out, src, label="joined")

    # Should complete without raising despite the failing function
    job.run(error_policy="continue")

    joined_records = job.nodes["joined"].get_all_records()
    # Join of empty × non-empty = empty (zero rows), not an error
    assert joined_records is None or joined_records.num_rows == 0


def test_empty_buffer_schema_preserves_nullability():
    """_materialize_as_stream on an empty buffer preserves required field nullability."""
    from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
    from orcapod.core.nodes.function_node import FunctionJobNode

    src = _make_source_with_required_field(["x"], [10])

    def identity(value: int) -> int:
        return value

    pf = PythonDataFunction(identity, output_keys="result")
    pod = FunctionPod(pf)

    db = InMemoryArrowDatabase()
    job = PipelineJob(name="test_empty_schema", store=db)
    with job:
        pod(src, label="fn")

    # Don't run — buffer will be empty
    node = job.nodes["fn"]
    stream = SyncPipelineOrchestrator._materialize_as_stream([], node)
    tag_schema, data_schema = stream.output_schema()

    # "result" is declared as int (required), must NOT become int | None
    assert data_schema["result"] == int
```

- [ ] **Step 2: Run the tests to verify they fail**

```bash
uv run pytest tests/test_pipeline/test_error_policy_continue.py -v
```

Expected: `test_failed_function_with_join_does_not_abort_pipeline` raises `InputValidationError`; `test_empty_buffer_schema_preserves_nullability` asserts `int | None` but expects `int`.

- [ ] **Step 3: Fix `_materialize_as_stream()` in `sync_orchestrator.py`**

In `src/orcapod/pipeline/sync_orchestrator.py`, replace the empty-branch of
`_materialize_as_stream()` (lines 181–198). The current code is:

```python
        if not buf:
            # Build an empty stream with the correct schema from the upstream node
            tag_schema, data_schema = upstream_node.output_schema(
                columns={"system_tags": True, "source": True}
            )
            type_converter = upstream_node.data_context.type_converter
            empty_fields = {}
            for name, py_type in {**tag_schema, **data_schema}.items():
                arrow_type = type_converter.python_type_to_arrow_type(py_type)
                empty_fields[name] = pa.array([], type=arrow_type)
            empty_table = pa.table(empty_fields)
            tag_keys = upstream_node.keys()[0]
            return ArrowTableStream(
                empty_table,
                tag_columns=tag_keys,
                producer=upstream_node.producer,
                upstreams=upstream_node.upstreams,
            )
```

Replace with:

```python
        if not buf:
            # Build an empty stream preserving declared field nullability.
            tag_schema, data_schema = upstream_node.output_schema(
                columns={"system_tags": True, "source": True}
            )
            type_converter = upstream_node.data_context.type_converter
            empty_table = arrow_utils.make_empty_table(
                {**tag_schema, **data_schema}, type_converter
            )
            tag_keys = upstream_node.keys()[0]
            return ArrowTableStream(
                empty_table,
                tag_columns=tag_keys,
                producer=upstream_node.producer,
                upstreams=upstream_node.upstreams,
            )
```

Note: `arrow_utils` is already imported at line 176 (`from orcapod.utils import arrow_utils`).

- [ ] **Step 4: Run the integration tests to verify they pass**

```bash
uv run pytest tests/test_pipeline/test_error_policy_continue.py -v
```

Expected: both PASS.

- [ ] **Step 5: Run the full pipeline test suite to check for regressions**

```bash
uv run pytest tests/test_pipeline/ -v
```

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/pipeline/sync_orchestrator.py tests/test_pipeline/test_error_policy_continue.py
git commit -m "fix(sync_orchestrator): use make_empty_table to preserve field nullability (ITL-563)"
```

---

## Task 6: Fix `side_effects.py` — two `as_table()` methods

**Files:**
- Modify: `src/orcapod/side_effects.py`
- Modify: `tests/test_core/side_effect_function/test_side_effect_function_pod.py`

- [ ] **Step 1: Write a failing test for `SideEffectFunctionStream.as_table()`**

Append to `tests/test_core/side_effect_function/test_side_effect_function_pod.py`:

```python
class TestSideEffectEmptyTableNullability:
    """Regression for ITL-563: empty as_table() must preserve field nullability."""

    def test_empty_stream_schema_preserves_required_fields(self):
        """SideEffectFunctionStream.as_table() with no rows preserves nullable=False."""
        from orcapod.core.function_pod import FunctionPod
        from orcapod.side_effects import InvocationContext

        def my_fn(value: int, ctx: InvocationContext) -> str:
            return str(value)

        pod = FunctionPod.from_fn(my_fn, output_keys=["result"], ctx_arg_name="ctx")
        stream = _make_stream(n=0)  # empty input → empty output

        # Build an empty stream (iter_data yields nothing)
        from orcapod.side_effects import SideEffectFunctionStream
        se_stream = SideEffectFunctionStream(pod=pod, input_stream=stream)

        table = se_stream.as_table()
        assert table.num_rows == 0
        # "result" is declared as str (required) — must not become str | None
        assert table.schema.field("result").nullable is False
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
uv run pytest "tests/test_core/side_effect_function/test_side_effect_function_pod.py::TestSideEffectEmptyTableNullability" -v
```

Expected: FAIL — `nullable` is `True` but should be `False`.

- [ ] **Step 3: Fix `SideEffectFunctionStream.as_table()` in `side_effects.py`**

Locate the empty-branch inside `SideEffectFunctionStream.as_table()` (around lines 165–176).
Current code:

```python
        if not tag_tables:
            # Return an empty table with the correct schema
            tag_schema, data_schema = self.output_schema(
                columns=column_config
            )
            tc = self._pod.data_context.type_converter
            fields = {}
            for name, py_type in {**tag_schema, **data_schema}.items():
                fields[name] = pa.array(
                    [], type=tc.python_type_to_arrow_type(py_type)
                )
            return pa.table(fields)
```

Replace with:

```python
        if not tag_tables:
            # Return an empty table preserving declared field nullability.
            tag_schema, data_schema = self.output_schema(
                columns=column_config
            )
            tc = self._pod.data_context.type_converter
            return arrow_utils.make_empty_table(
                {**tag_schema, **data_schema}, tc
            )
```

Note: `arrow_utils` is already imported in the `as_table()` body (`from orcapod.utils import arrow_utils`).

- [ ] **Step 4: Fix `SideEffectJobFunctionStream.as_table()` in `side_effects.py`**

Locate the empty-branch inside `SideEffectJobFunctionStream.as_table()` (around lines 728–736).
Current code:

```python
        if not tag_tables:
            tag_schema, data_schema = self.output_schema(columns=column_config)
            tc = self._pod.data_context.type_converter
            fields = {}
            for name, py_type in {**tag_schema, **data_schema}.items():
                fields[name] = pa.array(
                    [], type=tc.python_type_to_arrow_type(py_type)
                )
            return pa.table(fields)
```

Replace with:

```python
        if not tag_tables:
            tag_schema, data_schema = self.output_schema(columns=column_config)
            tc = self._pod.data_context.type_converter
            return arrow_utils.make_empty_table(
                {**tag_schema, **data_schema}, tc
            )
```

Note: `arrow_utils` is already imported in this method's body too.

- [ ] **Step 5: Run the test to verify it passes**

```bash
uv run pytest "tests/test_core/side_effect_function/test_side_effect_function_pod.py::TestSideEffectEmptyTableNullability" -v
```

Expected: PASS.

- [ ] **Step 6: Run the full side-effect test suite to check for regressions**

```bash
uv run pytest tests/test_core/side_effect_function/ tests/test_core/side_effect_pod/ -v
```

Expected: all PASS.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/side_effects.py tests/test_core/side_effect_function/test_side_effect_function_pod.py
git commit -m "fix(side_effects): use make_empty_table to preserve field nullability (ITL-563)"
```

---

## Task 7: Fix `derived_source.py` — `DerivedSource._get_stream()`

**Files:**
- Modify: `src/orcapod/core/sources/derived_source.py`
- Modify: `tests/test_core/sources/test_derived_source.py`

- [ ] **Step 1: Write a failing test for `DerivedSource` empty cache nullability**

Append to `tests/test_core/sources/test_derived_source.py`:

```python
def test_derived_source_empty_cache_preserves_nullability():
    """DerivedSource._get_stream() empty table preserves nullable=False for required fields.

    Regression for ITL-563: pa.field() without nullable=False defaulted to True.
    """
    # _make_node() builds a FunctionJobNode with data output "result: int" (required).
    node = _make_node()
    source = node.as_source()  # DerivedSource backed by unrun node → empty cache

    # as_table() triggers _get_stream() which hits the empty-cache branch
    table = source.as_table()

    assert table.num_rows == 0
    # "result" is int (required), must not become int | None
    result_field = table.schema.field("result")
    assert result_field.nullable is False
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
uv run pytest tests/test_core/sources/test_derived_source.py::test_derived_source_empty_cache_preserves_nullability -v
```

Expected: FAIL — `nullable` is `True` but should be `False`.

- [ ] **Step 3: Fix `DerivedSource._get_stream()` in `derived_source.py`**

Locate the `records is None` branch in `_get_stream()` (lines 75–95). Current code:

```python
            if records is None:
                # Build empty table with correct schema
                tag_schema, data_schema = self._origin.output_schema()
                tag_keys = self._origin.keys()[0]
                tc = self.data_context.type_converter
                fields = [
                    pa.field(k, tc.python_type_to_arrow_type(tag_schema[k]))
                    for k in tag_keys
                ]
                fields += [
                    pa.field(k, tc.python_type_to_arrow_type(v))
                    for k, v in data_schema.items()
                ]
                arrow_schema = pa.schema(fields)
                self._cached_table = pa.table(
                    {f.name: pa.array([], type=f.type) for f in arrow_schema},
                    schema=arrow_schema,
                )
```

Replace with:

```python
            if records is None:
                # Build empty table preserving declared field nullability.
                from orcapod.utils.arrow_utils import make_empty_table

                tag_schema, data_schema = self._origin.output_schema()
                tag_keys = self._origin.keys()[0]
                tc = self.data_context.type_converter
                python_schema = {k: tag_schema[k] for k in tag_keys}
                python_schema.update(data_schema)
                self._cached_table = make_empty_table(python_schema, tc)
```

- [ ] **Step 4: Run the test to verify it passes**

```bash
uv run pytest tests/test_core/sources/test_derived_source.py::test_derived_source_empty_cache_preserves_nullability -v
```

Expected: PASS.

- [ ] **Step 5: Run the full sources test suite to check for regressions**

```bash
uv run pytest tests/test_core/sources/ -v
```

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/core/sources/derived_source.py tests/test_core/sources/test_derived_source.py
git commit -m "fix(derived_source): use make_empty_table to preserve field nullability (ITL-563)"
```

---

## Task 8: Mark DESIGN_ISSUES.md resolved + run full test suite

**Files:**
- Modify: `DESIGN_ISSUES.md`

- [ ] **Step 1: Mark CC1 resolved in `DESIGN_ISSUES.md`**

Update the CC1 entry: change `**Status:** in progress` to `**Status:** resolved` and append a **Fix:** line:

```
**Fix:** Extracted ``arrow_utils.make_empty_table(python_schema, type_converter)`` using
``python_schema_to_arrow_schema`` + ``pa.Table.from_batches([], schema=...)``. Replaced all
five buggy sites. Added unit tests in ``test_arrow_utils.py``, integration test in
``test_error_policy_continue.py``, and per-site regression tests. PR: ITL-563.
```

- [ ] **Step 2: Run the full test suite**

```bash
uv run pytest tests/ -v --tb=short
```

Expected: all PASS (or pre-existing failures only — check against `git stash` baseline if any fail).

- [ ] **Step 3: Final commit**

```bash
git add DESIGN_ISSUES.md
git commit -m "docs(design-issues): mark CC1 resolved — empty table nullability fix (ITL-563)"
```

---

## Self-Review Checklist (completed inline)

| Spec requirement | Covered by task |
|---|---|
| `make_empty_table` in `arrow_utils.py` | Task 3 |
| Fix `sync_orchestrator._materialize_as_stream` | Task 5 |
| Fix `operator_node._make_empty_table` | Task 4 |
| Fix `SideEffectFunctionStream.as_table` | Task 6 |
| Fix `SideEffectJobFunctionStream.as_table` | Task 6 |
| Fix `DerivedSource._get_stream` | Task 7 |
| Unit tests: required / optional / mixed / round-trip | Task 2 |
| Integration test: failing fn + Join + continue policy | Task 5 |
| Integration test: empty buffer schema preserves nullability | Task 5 |
| Regression: side-effect empty table nullability | Task 6 |
| Regression: DerivedSource empty cache nullability | Task 7 |
| Strengthen `test_as_table_empty_schema_matches_non_empty_schema` | Task 4 |
| DESIGN_ISSUES.md updated | Tasks 1 (in progress), 8 (resolved) |
