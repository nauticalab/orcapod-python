# ENG-493 Task 6: PipelineJob Recording Uses JobNode Types

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** After a `with job:` block, `job._persistent_node_map` contains only `JobNode` variants (`SourceJobNode`, `FunctionJobNode`, `OperatorJobNode`), not blueprint types.

**Architecture:** `record_function_pod_invocation` and `record_operator_pod_invocation` now create `FunctionJobNode`/`OperatorJobNode` in `_rec_node_lut`. `_compile_from_recording` converts these to lightweight `FunctionNode`/`OperatorNode` (via `.as_node()`) for the blueprint `Pipeline._node_lut`, then builds `job._persistent_node_map` with `SourceJobNode` leaves by walking the compiled pipeline topologically. `_build_execution_graph` already uses `FunctionNode`/`OperatorNode` from the blueprint pipeline and is unchanged.

**Tech Stack:** Python, uv, pytest, networkx, orcapod nodes hierarchy.

---

## File Map

| File | Change |
|---|---|
| `src/orcapod/pipeline/job.py` | Modify `record_function_pod_invocation`, `record_operator_pod_invocation`, and `_compile_from_recording` |
| `tests/test_pipeline/test_pipeline_job.py` | Add `TestPipelineJobUsesJobNodes` class with fixture |

---

### Task 1: Write failing tests for job node types in `_persistent_node_map`

**Files:**
- Modify: `tests/test_pipeline/test_pipeline_job.py`

- [ ] **Step 1: Add the `pipeline_job_with_sources` fixture and `TestPipelineJobUsesJobNodes` class**

Open `tests/test_pipeline/test_pipeline_job.py` and add the following immediately before the final `class TestFromPipeline:` block (after the `compiled_pipeline`/`db`/`source_a`/`source_b`/`pipeline_job`/`pipeline_job_complete` fixtures, around line 720):

```python
@pytest.fixture
def pipeline_job_with_sources(store):
    """A PipelineJob created via with-block using concrete sources + a FunctionPod."""
    src_a, src_b = _make_two_sources()
    pf = PythonDataFunction(add_values, output_keys="total")
    pod = FunctionPod(data_function=pf)

    job = PipelineJob(store=store)
    with job:
        joined = Join()(src_a, src_b, label="joiner")
        pod(joined, label="adder")
    return job


class TestPipelineJobUsesJobNodes:
    """PipelineJob._persistent_node_map must contain only JobNode variants after recording."""

    def test_persistent_map_has_source_job_nodes(self, pipeline_job_with_sources):
        """Source entries in PipelineJob._persistent_node_map must be SourceJobNode."""
        from orcapod.core.nodes.source_node import SourceJobNode, SourceNodeBase

        assert pipeline_job_with_sources._persistent_node_map is not None, (
            "_persistent_node_map must be set after with-block"
        )
        for node in pipeline_job_with_sources._persistent_node_map.values():
            if isinstance(node, SourceNodeBase):
                assert isinstance(node, SourceJobNode), (
                    f"Expected SourceJobNode but got {type(node).__name__}"
                )

    def test_persistent_map_has_function_job_nodes(self, pipeline_job_with_sources):
        """Function entries in PipelineJob._persistent_node_map must be FunctionJobNode."""
        from orcapod.core.nodes.function_node import FunctionJobNode, FunctionNodeBase

        assert pipeline_job_with_sources._persistent_node_map is not None
        fn_nodes = [
            n for n in pipeline_job_with_sources._persistent_node_map.values()
            if isinstance(n, FunctionNodeBase)
        ]
        assert len(fn_nodes) >= 1, "Expected at least one FunctionJobNode"
        for node in fn_nodes:
            assert isinstance(node, FunctionJobNode), (
                f"Expected FunctionJobNode but got {type(node).__name__}"
            )

    def test_persistent_map_has_operator_job_nodes(self, pipeline_job_with_sources):
        """Operator entries in PipelineJob._persistent_node_map must be OperatorJobNode."""
        from orcapod.core.nodes.operator_node import OperatorJobNode, OperatorNodeBase

        assert pipeline_job_with_sources._persistent_node_map is not None
        op_nodes = [
            n for n in pipeline_job_with_sources._persistent_node_map.values()
            if isinstance(n, OperatorNodeBase)
        ]
        assert len(op_nodes) >= 1, "Expected at least one OperatorJobNode"
        for node in op_nodes:
            assert isinstance(node, OperatorJobNode), (
                f"Expected OperatorJobNode but got {type(node).__name__}"
            )

    def test_blueprint_pipeline_still_has_lightweight_nodes(self, pipeline_job_with_sources):
        """The compiled pipeline's _persistent_node_map still has lightweight nodes."""
        from orcapod.core.nodes.function_node import FunctionJobNode
        from orcapod.core.nodes.operator_node import OperatorJobNode

        for node in pipeline_job_with_sources.pipeline._persistent_node_map.values():
            assert not isinstance(node, FunctionJobNode), (
                "Blueprint pipeline must not contain FunctionJobNode"
            )
            assert not isinstance(node, OperatorJobNode), (
                "Blueprint pipeline must not contain OperatorJobNode"
            )
```

- [ ] **Step 2: Run the new tests to verify they fail**

```bash
cd /home/kurouto/kurouto-jobs/5bda6bb8-f5e1-4b33-b256-7eef168aa769/orcapod-python && \
uv run pytest tests/test_pipeline/test_pipeline_job.py::TestPipelineJobUsesJobNodes -v 2>&1 | tail -20
```

Expected: FAIL — `_persistent_node_map` is `None` after with-block (returns `AssertionError: _persistent_node_map must be set after with-block`), and/or `FunctionNode`/`OperatorNode` found instead of job variants.

---

### Task 2: Update `record_function_pod_invocation` to create `FunctionJobNode`

**Files:**
- Modify: `src/orcapod/pipeline/job.py` (lines ~197–221)

- [ ] **Step 1: Replace the body of `record_function_pod_invocation`**

Find this block in `src/orcapod/pipeline/job.py`:

```python
    def record_function_pod_invocation(
        self,
        pod: cp.FunctionPodProtocol,
        input_stream: cp.StreamProtocol,
        label: str | None = None,
    ) -> None:
        """Record a function pod invocation, promoting concrete sources to specs.

        Args:
            pod: The function pod being invoked.
            input_stream: The upstream stream (concrete source or spec).
            label: Optional label for the resulting node.
        """
        from orcapod.core.nodes import FunctionNode

        input_stream = self._to_node_stream(input_stream)

        input_hash = input_stream.content_hash().to_string()
        function_node = FunctionNode(function_pod=pod, input_stream=input_stream, label=label)
        fn_hash = function_node.content_hash().to_string()

        self._rec_node_lut[fn_hash] = function_node
        self._rec_upstreams[input_hash] = input_stream
        self._rec_graph_edges.append((input_hash, fn_hash))
```

Replace with:

```python
    def record_function_pod_invocation(
        self,
        pod: cp.FunctionPodProtocol,
        input_stream: cp.StreamProtocol,
        label: str | None = None,
    ) -> None:
        """Record a function pod invocation, promoting concrete sources to specs.

        Args:
            pod: The function pod being invoked.
            input_stream: The upstream stream (concrete source or spec).
            label: Optional label for the resulting node.
        """
        from orcapod.core.nodes.function_node import FunctionJobNode

        input_stream = self._to_node_stream(input_stream)

        input_hash = input_stream.content_hash().to_string()
        function_node = FunctionJobNode(function_pod=pod, input_stream=input_stream, label=label)
        fn_hash = function_node.content_hash().to_string()

        self._rec_node_lut[fn_hash] = function_node
        self._rec_upstreams[input_hash] = input_stream
        self._rec_graph_edges.append((input_hash, fn_hash))
```

The only change is `FunctionNode` → `FunctionJobNode` in the import and construction. The hash is identical because `FunctionJobNode` inherits `content_hash()` from `FunctionNodeBase` with the same identity structure.

---

### Task 3: Update `record_operator_pod_invocation` to create `OperatorJobNode`

**Files:**
- Modify: `src/orcapod/pipeline/job.py` (lines ~222–246)

- [ ] **Step 1: Replace the body of `record_operator_pod_invocation`**

Find this block in `src/orcapod/pipeline/job.py`:

```python
    def record_operator_pod_invocation(
        self,
        pod: cp.OperatorPodProtocol,
        upstreams: tuple[cp.StreamProtocol, ...] = (),
        label: str | None = None,
    ) -> None:
        """Record an operator pod invocation, promoting concrete sources to specs.

        Args:
            pod: The operator pod being invoked.
            upstreams: Upstream streams (concrete sources or specs).
            label: Optional label for the resulting node.
        """
        from orcapod.core.nodes import OperatorNode

        processed = tuple(self._to_node_stream(s) for s in upstreams)

        operator_node = OperatorNode(operator=pod, input_streams=processed, label=label)
        op_hash = operator_node.content_hash().to_string()

        self._rec_node_lut[op_hash] = operator_node
        for upstream in processed:
            up_hash = upstream.content_hash().to_string()
            self._rec_upstreams[up_hash] = upstream
            self._rec_graph_edges.append((up_hash, op_hash))
```

Replace with:

```python
    def record_operator_pod_invocation(
        self,
        pod: cp.OperatorPodProtocol,
        upstreams: tuple[cp.StreamProtocol, ...] = (),
        label: str | None = None,
    ) -> None:
        """Record an operator pod invocation, promoting concrete sources to specs.

        Args:
            pod: The operator pod being invoked.
            upstreams: Upstream streams (concrete sources or specs).
            label: Optional label for the resulting node.
        """
        from orcapod.core.nodes.operator_node import OperatorJobNode

        processed = tuple(self._to_node_stream(s) for s in upstreams)

        operator_node = OperatorJobNode(operator=pod, input_streams=processed, label=label)
        op_hash = operator_node.content_hash().to_string()

        self._rec_node_lut[op_hash] = operator_node
        for upstream in processed:
            up_hash = upstream.content_hash().to_string()
            self._rec_upstreams[up_hash] = upstream
            self._rec_graph_edges.append((up_hash, op_hash))
```

The only change is `OperatorNode` → `OperatorJobNode` in the import and construction.

---

### Task 4: Update `_compile_from_recording` to convert JobNodes for blueprint and build job `_persistent_node_map`

**Files:**
- Modify: `src/orcapod/pipeline/job.py` (lines ~103–130)

**Key insight:** `Pipeline.compile()` expects `FunctionNode`/`OperatorNode` in `pipeline._node_lut` — it raises `TypeError` for any other type. So `_compile_from_recording` must convert `FunctionJobNode`→`FunctionNode` and `OperatorJobNode`→`OperatorNode` via `.as_node()` before calling `pipeline.compile()`. Then it builds `job._persistent_node_map` by walking the compiled pipeline topologically, creating `SourceJobNode` leaves from the concrete sources captured in `_sources`.

- [ ] **Step 1: Replace `_compile_from_recording`**

Find this entire method in `src/orcapod/pipeline/job.py`:

```python
    def _compile_from_recording(self) -> None:
        """Compile the recorded edges into a pure Pipeline."""
        from orcapod.pipeline.graph import Pipeline

        pipeline = Pipeline(name=self._name, auto_compile=False)
        # Inject the recording state into the pipeline
        pipeline._graph_edges = list(self._rec_graph_edges)
        pipeline._upstreams = dict(self._rec_upstreams)
        pipeline._node_lut = dict(self._rec_node_lut)
        # Rebuild hash graph from edges
        for edge in self._rec_graph_edges:
            pipeline._hash_graph.add_edge(*edge)

        # Annotate node_type on each recorded node (function/operator).
        for node_hash, node in self._rec_node_lut.items():
            if node_hash in pipeline._hash_graph.nodes:
                pipeline._hash_graph.nodes[node_hash]["node_type"] = node.node_type
                if node.label:
                    pipeline._hash_graph.nodes[node_hash]["label"] = node.label

        # Annotate upstream (source) nodes that are not in _rec_node_lut.
        for node_hash, stream in self._rec_upstreams.items():
            if node_hash in pipeline._hash_graph.nodes:
                if not pipeline._hash_graph.nodes[node_hash].get("node_type"):
                    pipeline._hash_graph.nodes[node_hash]["node_type"] = "source"

        pipeline.compile()
        self._compiled_pipeline = pipeline
```

Replace with:

```python
    def _compile_from_recording(self) -> None:
        """Compile the recorded edges into a pure Pipeline and build the job node map.

        ``_rec_node_lut`` now contains ``FunctionJobNode`` / ``OperatorJobNode`` objects
        (set by ``record_function_pod_invocation`` / ``record_operator_pod_invocation``).
        This method:

        1. Converts each recorded job node to its lightweight blueprint counterpart via
           ``.as_node()`` and injects the result into ``pipeline._node_lut`` so that
           ``Pipeline.compile()`` sees only ``FunctionNode`` / ``OperatorNode`` objects.
        2. After compiling the blueprint pipeline, walks it topologically to build
           ``self._persistent_node_map`` using ``SourceJobNode`` for leaf nodes
           (concrete sources are taken from ``self._sources``) and the original recorded
           ``FunctionJobNode`` / ``OperatorJobNode`` objects for non-leaf nodes.
        """
        import networkx as _nx
        from orcapod.pipeline.graph import Pipeline
        from orcapod.core.nodes.source_node import SourceJobNode, SourceNodeBase
        from orcapod.core.nodes.function_node import FunctionJobNode, FunctionNodeBase
        from orcapod.core.nodes.operator_node import OperatorJobNode, OperatorNodeBase

        pipeline = Pipeline(name=self._name, auto_compile=False)
        # Inject the recording state into the pipeline, converting job nodes → blueprint nodes
        pipeline._graph_edges = list(self._rec_graph_edges)
        pipeline._upstreams = dict(self._rec_upstreams)
        pipeline._node_lut = {
            h: node.as_node() for h, node in self._rec_node_lut.items()
        }
        # Rebuild hash graph from edges
        for edge in self._rec_graph_edges:
            pipeline._hash_graph.add_edge(*edge)

        # Annotate node_type on each recorded node (function/operator).
        for node_hash, node in self._rec_node_lut.items():
            if node_hash in pipeline._hash_graph.nodes:
                pipeline._hash_graph.nodes[node_hash]["node_type"] = node.node_type
                if node.label:
                    pipeline._hash_graph.nodes[node_hash]["label"] = node.label

        # Annotate upstream (source) nodes that are not in _rec_node_lut.
        for node_hash, stream in self._rec_upstreams.items():
            if node_hash in pipeline._hash_graph.nodes:
                if not pipeline._hash_graph.nodes[node_hash].get("node_type"):
                    pipeline._hash_graph.nodes[node_hash]["node_type"] = "source"

        pipeline.compile()
        self._compiled_pipeline = pipeline

        # Build PipelineJob's own job node map walking compiled pipeline topologically.
        # Leaf nodes (SourceNode) become SourceJobNode with concrete binding from _sources.
        # Non-leaf nodes reuse the FunctionJobNode/OperatorJobNode from _rec_node_lut.
        G = pipeline._hash_graph
        job_node_map: dict[str, object] = {}

        for node_hash in _nx.topological_sort(G):
            if node_hash not in pipeline._persistent_node_map:
                continue

            bp_node = pipeline._persistent_node_map[node_hash]

            if isinstance(bp_node, SourceNodeBase):
                concrete = self._sources.get(bp_node.name)
                job_node: object = SourceJobNode(
                    name=bp_node.name,
                    tag_schema=bp_node.tag_schema,
                    data_schema=bp_node.data_schema,
                    concrete=concrete,
                )
            elif isinstance(bp_node, FunctionNodeBase):
                # Reuse the FunctionJobNode from _rec_node_lut, rewired to upstream job node.
                # The original rec node has the correct table_scope and tracker_manager.
                rec_node = self._rec_node_lut[node_hash]
                original_input_hash = bp_node._input_stream.content_hash().to_string()
                upstream_job_node = job_node_map[original_input_hash]
                job_node = FunctionJobNode(
                    function_pod=rec_node._function_pod,
                    input_stream=upstream_job_node,
                    label=rec_node._label,
                    table_scope=rec_node._table_scope,
                    tracker_manager=rec_node.tracker_manager,
                )
            elif isinstance(bp_node, OperatorNodeBase):
                rec_node = self._rec_node_lut[node_hash]
                upstream_job_nodes = tuple(
                    job_node_map[s.content_hash().to_string()]
                    for s in bp_node._input_streams
                )
                job_node = OperatorJobNode(
                    operator=rec_node._operator,
                    input_streams=upstream_job_nodes,
                    label=rec_node._label,
                    table_scope=rec_node._table_scope,
                    tracker_manager=rec_node.tracker_manager,
                )
            else:
                raise TypeError(
                    f"Unknown blueprint node type in compiled pipeline: {type(bp_node)}"
                )

            job_node_map[node_hash] = job_node

        self._persistent_node_map = job_node_map

        # Build label → job node map from pipeline._nodes
        self._nodes = {
            label: job_node_map[node.content_hash().to_string()]
            for label, node in pipeline._nodes.items()
            if node.content_hash().to_string() in job_node_map
        }

        # Wire databases if store is already set
        if self._store is not None:
            self._distribute_databases()
```

---

### Task 5: Run the new tests — verify they pass

- [ ] **Step 1: Run `TestPipelineJobUsesJobNodes`**

```bash
cd /home/kurouto/kurouto-jobs/5bda6bb8-f5e1-4b33-b256-7eef168aa769/orcapod-python && \
uv run pytest tests/test_pipeline/test_pipeline_job.py::TestPipelineJobUsesJobNodes -v 2>&1 | tail -20
```

Expected: All 4 tests PASS.

- [ ] **Step 2: Run the full pipeline test suite**

```bash
cd /home/kurouto/kurouto-jobs/5bda6bb8-f5e1-4b33-b256-7eef168aa769/orcapod-python && \
uv run pytest tests/test_pipeline/ -v --tb=short 2>&1 | tail -50
```

Expected: All tests pass. Watch for:
- `TestPipelineJobEndToEnd::test_end_to_end_source_join_function` — checks `compiled_nodes["joiner"]` is `OperatorNode` and `compiled_nodes["adder"]` is `FunctionNode`. These look at `job.pipeline.compiled_nodes` (the blueprint pipeline), not `job._persistent_node_map`, so they should still pass.
- `TestPipelineJobRun` — exercises `job.run()` via `_build_execution_graph` which reads from `pipeline._node_lut` (blueprint); should be unaffected.

If any test fails due to type checks on `_persistent_node_map` expecting `FunctionNode`/`OperatorNode`, update those assertions to check for `FunctionJobNode`/`OperatorJobNode` respectively.

- [ ] **Step 3: Run the full test suite**

```bash
cd /home/kurouto/kurouto-jobs/5bda6bb8-f5e1-4b33-b256-7eef168aa769/orcapod-python && \
uv run pytest tests/ -v --tb=short 2>&1 | tail -60
```

Expected: All tests pass.

---

### Task 6: Commit

- [ ] **Step 1: Commit the changes**

```bash
cd /home/kurouto/kurouto-jobs/5bda6bb8-f5e1-4b33-b256-7eef168aa769/orcapod-python && \
git add src/orcapod/pipeline/job.py tests/test_pipeline/test_pipeline_job.py && \
git commit -m "refactor(pipeline): PipelineJob recording creates FunctionJobNode/OperatorJobNode; compile builds SourceJobNode leaves"
```

---

## Self-Review

**Spec coverage:**
- `record_function_pod_invocation` → `FunctionJobNode`: Task 2. ✓
- `record_operator_pod_invocation` → `OperatorJobNode`: Task 3. ✓
- `_compile_from_recording` converts via `.as_node()` for blueprint: Task 4. ✓
- `_compile_from_recording` builds `_persistent_node_map` with `SourceJobNode`: Task 4. ✓
- `job._nodes` label map updated: Task 4. ✓
- `_distribute_databases()` called when store set: Task 4. ✓
- Blueprint pipeline still has lightweight nodes: tested in Task 1, step 1 (4th test). ✓
- Existing tests not broken: Task 5 full suite run. ✓

**Placeholder scan:** No TBDs, TODOs, or vague steps found.

**Type consistency:**
- `FunctionJobNode` referenced in Tasks 2, 4 — both use `from orcapod.core.nodes.function_node import FunctionJobNode`. ✓
- `OperatorJobNode` referenced in Tasks 3, 4 — both use `from orcapod.core.nodes.operator_node import OperatorJobNode`. ✓
- `SourceJobNode` referenced in Task 4 — uses `from orcapod.core.nodes.source_node import SourceJobNode`. ✓
- `bp_node._input_stream` (Task 4, FunctionNodeBase branch) — `FunctionNodeBase` has `_input_stream` attribute. ✓
- `bp_node._input_streams` (Task 4, OperatorNodeBase branch) — `OperatorNodeBase` has `_input_streams` attribute. ✓
- `rec_node._function_pod`, `rec_node._table_scope`, `rec_node.tracker_manager` (Task 4) — all present on `FunctionJobNode` via `FunctionNodeBase`. ✓
- `rec_node._operator`, `rec_node._input_streams`, `rec_node._table_scope`, `rec_node.tracker_manager` (Task 4) — all present on `OperatorJobNode` via `OperatorNodeBase`. ✓
