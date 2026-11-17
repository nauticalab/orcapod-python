# Type-Specific Pod Access Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `source_pods`, `function_pods`, and `operator_pods` read-only properties to `AbstractPipelineBase` and `PipelineProtocol`, each returning a filtered `dict[str, Any]` subset of `.nodes`.

**Architecture:** Each property is a one-liner dict comprehension filtering `self._nodes` by `node_type`. The properties live on `AbstractPipelineBase` so both `Pipeline` and `PipelineJob` inherit them for free. `PipelineProtocol` gets matching stubs so protocol-typed callers can use the new accessors without downcasting.

**Tech Stack:** Python, pytest, uv (all tests run via `uv run pytest`)

---

## File Map

| File | Change |
|---|---|
| `tests/test_pipeline/test_pipeline.py` | Add `TestTypePodAccess` class with 6 tests |
| `src/orcapod/pipeline/base.py` | Add 3 properties to `AbstractPipelineBase` after `.nodes` |
| `src/orcapod/protocols/pipeline_protocols.py` | Add 3 property stubs to `PipelineProtocol` after `.nodes` |

---

## Task 1: Write failing tests

**Files:**
- Modify: `tests/test_pipeline/test_pipeline.py` (append new class at end of file)

- [ ] **Step 1: Append `TestTypePodAccess` to `tests/test_pipeline/test_pipeline.py`**

Add the following class at the very end of the file (after the last existing class):

```python
# ---------------------------------------------------------------------------
# Tests: Type-specific pod access (PLT-420)
# ---------------------------------------------------------------------------


class TestTypePodAccess:
    """Tests for source_pods, function_pods, and operator_pods properties."""

    def test_function_pods_returns_only_function_nodes(self):
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        pod = FunctionPod(PythonDataFunction(double_value, output_keys="result"))
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(src, label="doubled")
        fp = pipeline.function_pods
        assert len(fp) == 1
        assert "doubled" in fp
        assert isinstance(fp["doubled"], FunctionNode)

    def test_source_pods_returns_only_source_nodes(self):
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        pod = FunctionPod(PythonDataFunction(double_value, output_keys="result"))
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(src, label="doubled")
        sp = pipeline.source_pods
        assert len(sp) == 1
        assert isinstance(list(sp.values())[0], SourceNode)

    def test_operator_pods_returns_only_operator_nodes(self):
        src_a, src_b = _make_two_sources()
        pipeline = Pipeline(name="test")
        with pipeline:
            Join()(src_a, src_b, label="joined")
        op = pipeline.operator_pods
        assert len(op) == 1
        assert "joined" in op
        assert isinstance(op["joined"], OperatorNode)

    def test_type_pods_union_covers_all_nodes(self):
        src_a, src_b = _make_two_sources()
        pod = FunctionPod(PythonDataFunction(double_value, output_keys="result"))
        pipeline = Pipeline(name="test")
        with pipeline:
            joined = Join()(src_a, src_b, label="joined")
            pod(joined, label="doubled")
        all_nodes = pipeline.nodes
        combined = {
            **pipeline.source_pods,
            **pipeline.function_pods,
            **pipeline.operator_pods,
        }
        assert combined == all_nodes

    def test_function_pods_empty_when_no_function_nodes(self):
        src_a, src_b = _make_two_sources()
        pipeline = Pipeline(name="test")
        with pipeline:
            Join()(src_a, src_b, label="joined")
        assert pipeline.function_pods == {}

    def test_operator_pods_empty_when_no_operator_nodes(self):
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        pod = FunctionPod(PythonDataFunction(double_value, output_keys="result"))
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(src, label="doubled")
        assert pipeline.operator_pods == {}
```

- [ ] **Step 2: Run the new tests — confirm they all fail with `AttributeError`**

```bash
uv run pytest tests/test_pipeline/test_pipeline.py::TestTypePodAccess -v
```

Expected: 6 FAILED — each with:
```
AttributeError: 'Pipeline' object has no attribute 'function_pods'
```
(or `source_pods` / `operator_pods` depending on which test runs first)

- [ ] **Step 3: Commit the failing tests**

```bash
git add tests/test_pipeline/test_pipeline.py
git commit -m "test(pipeline): add failing tests for source_pods, function_pods, operator_pods"
```

---

## Task 2: Implement properties on `AbstractPipelineBase`

**Files:**
- Modify: `src/orcapod/pipeline/base.py:118-121` (after the `.nodes` property)

- [ ] **Step 1: Add the three properties immediately after the `nodes` property in `base.py`**

Locate this block (lines ~118–121):

```python
    @property
    def nodes(self) -> dict[str, Any]:
        """Copy of the compiled nodes dict (label → node)."""
        return self._nodes.copy()
```

Insert the following three properties directly after it (before the `dag` property):

```python
    @property
    def source_pods(self) -> dict[str, Any]:
        """Copy of compiled nodes that are source nodes (label → node)."""
        return {k: v for k, v in self._nodes.items() if v.node_type == "source"}

    @property
    def function_pods(self) -> dict[str, Any]:
        """Copy of compiled nodes that are function-pod nodes (label → node)."""
        return {k: v for k, v in self._nodes.items() if v.node_type == "function"}

    @property
    def operator_pods(self) -> dict[str, Any]:
        """Copy of compiled nodes that are operator-pod nodes (label → node)."""
        return {k: v for k, v in self._nodes.items() if v.node_type == "operator"}
```

- [ ] **Step 2: Run the new tests — confirm they all pass**

```bash
uv run pytest tests/test_pipeline/test_pipeline.py::TestTypePodAccess -v
```

Expected: 6 PASSED

- [ ] **Step 3: Run the full pipeline test suite — confirm no regressions**

```bash
uv run pytest tests/test_pipeline/ -v
```

Expected: all tests PASSED (no failures introduced)

- [ ] **Step 4: Commit the implementation**

```bash
git add src/orcapod/pipeline/base.py
git commit -m "feat(pipeline): add source_pods, function_pods, operator_pods properties to AbstractPipelineBase"
```

---

## Task 3: Update `PipelineProtocol`

**Files:**
- Modify: `src/orcapod/protocols/pipeline_protocols.py:38-45` (after the `.nodes` property stub)

- [ ] **Step 1: Add three property stubs to `PipelineProtocol` after the `nodes` property**

Locate this block in `PipelineProtocol`:

```python
    @property
    def nodes(self) -> dict[str, NodeT]:
        """Copy of the compiled label -> node mapping."""
        ...
```

Insert the following three stubs directly after it (before the `dag` property):

```python
    @property
    def source_pods(self) -> dict[str, NodeT]:
        """Copy of compiled nodes that are source nodes (label → node)."""
        ...

    @property
    def function_pods(self) -> dict[str, NodeT]:
        """Copy of compiled nodes that are function-pod nodes (label → node)."""
        ...

    @property
    def operator_pods(self) -> dict[str, NodeT]:
        """Copy of compiled nodes that are operator-pod nodes (label → node)."""
        ...
```

- [ ] **Step 2: Run the full test suite — confirm everything still passes**

```bash
uv run pytest tests/ -v
```

Expected: all tests PASSED

- [ ] **Step 3: Commit the protocol update**

```bash
git add src/orcapod/protocols/pipeline_protocols.py
git commit -m "feat(protocols): add source_pods, function_pods, operator_pods to PipelineProtocol"
```
