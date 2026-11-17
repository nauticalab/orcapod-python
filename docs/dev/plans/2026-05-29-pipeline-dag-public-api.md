# Pipeline DAG Public API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the private `_node_graph: nx.DiGraph` with `OrcaDAG[NodeT]`, expose it as a public `dag` property, make `AbstractPipelineBase` generic over its node type, and add a structural `PipelineProtocol[NodeT]`.

**Architecture:** `AbstractPipelineBase[NodeT]` gains a `Generic[NodeT]` parent, `_node_graph` becomes `OrcaDAG[NodeT] | None`, and a `dag` property exposes it publicly. `PipelineProtocol[NodeT]` formalises the public surface. `Pipeline` binds `NodeT=GraphNode`; `PipelineJob` binds `NodeT=JobNode`. All four OrcaDAG-construction sites (compile, Pipeline.load, as_pipeline, PipelineJob.load) are updated to build `OrcaDAG` instead of `nx.DiGraph`.

**Tech Stack:** Python generics (`typing.Generic`, `TypeVar`), `OrcaDAG` / `GraphProtocol` from `orcapod.pipeline.dag`, `pytest` via `uv run pytest`.

---

## File Map

| Action | File | What changes |
| ------ | ---- | ------------ |
| Modify | `src/orcapod/pipeline/base.py` | `Generic[NodeT]` parent, `_node_graph` type, `dag` property, compile() step 5 |
| Modify | `src/orcapod/protocols/pipeline_protocols.py` | Add `PipelineProtocol[NodeT]` |
| Modify | `src/orcapod/pipeline/graph.py` | `Pipeline(AbstractPipelineBase[GraphNode])`, load() OrcaDAG build, render_graph annotation |
| Modify | `src/orcapod/pipeline/job.py` | `PipelineJob(AbstractPipelineBase[JobNode])`, as_pipeline() OrcaDAG build, load() OrcaDAG build |
| Modify | `tests/test_pipeline/test_dag.py` | New tests: `dag` property, pre-compile error, `PipelineProtocol` conformance |
| Modify | `tests/test_pipeline/test_pipeline.py` | `_node_graph` → `dag` (4 sites) |
| Modify | `tests/test_pipeline/test_pipeline_job.py` | `_node_graph` → `dag` (1 site) |
| Modify | `tests/test_pipeline/test_serialization.py` | `_node_graph` → `dag` (2 sites) |
| Modify | `tests/test_pipeline/test_logging_observer_integration.py` | Helper `_get_function_node` |
| Modify | `tests/test_pipeline/test_graph_rendering.py` | Fixture return type + 2 sites |
| Modify | `tests/test_pipeline/test_status_observer_integration.py` | Helper + direct call |
| Modify | `tests/test_pipeline/test_orchestrator.py` | All `_node_graph` calls (~13 sites) |
| Modify | `tests/test_pipeline/test_orchestrator_executor_matrix.py` | 2 sites |
| Modify | `tests/test_pipeline/test_sync_orchestrator.py` | All `_node_graph` calls (~15 sites) |
| Modify | `tests/test_channels/test_pipeline_async_integration.py` | Delete `_build_exec_dag`, use `job.dag` |

---

## Task 1: AbstractPipelineBase[NodeT] — generic base + dag property + compile() fix

**Files:**
- Modify: `src/orcapod/pipeline/base.py:1-25` (imports), `base.py:46` (class decl), `base.py:88-91` (field types), `base.py:104-117` (properties), `base.py:357-366` (compile step 5)
- Test: `tests/test_pipeline/test_dag.py`

- [ ] **Step 1: Write failing tests for `dag` property**

Add a new test class to the bottom of `tests/test_pipeline/test_dag.py`:

```python
# ---------------------------------------------------------------------------
# dag property on Pipeline / PipelineJob
# ---------------------------------------------------------------------------


class TestPipelineDagProperty:
    """Pipeline.dag and PipelineJob.dag expose an OrcaDAG of node objects."""

    def _make_simple_pipeline(self):
        """Return a compiled single-function Pipeline."""
        import pyarrow as pa
        from orcapod.core.sources.arrow_table_source import ArrowTableSource
        from orcapod.core.function_pod import FunctionPod
        from orcapod.core.data_function import PythonDataFunction
        from orcapod.pipeline.graph import Pipeline
        from orcapod.types import Schema

        src = ArrowTableSource(
            pa.table({"id": pa.array(["a"], type=pa.large_string()), "v": pa.array([1], type=pa.int64())}),
            tag_columns=["id"],
            source_id="src",
        )
        fn = PythonDataFunction(
            func=lambda v: {"out": v},
            input_schema=Schema({"v": int}),
            output_schema=Schema({"out": int}),
        )
        pod = FunctionPod(fn)
        pipeline = Pipeline(name="test_dag")
        with pipeline:
            pod(src)
        return pipeline

    def test_dag_returns_orca_dag_after_compile(self):
        """pipeline.dag returns an OrcaDAG instance after compilation."""
        pipeline = self._make_simple_pipeline()
        assert isinstance(pipeline.dag, OrcaDAG)

    def test_dag_has_correct_node_count(self):
        """dag contains source node + function node = 2 nodes."""
        pipeline = self._make_simple_pipeline()
        assert len(list(pipeline.dag.nodes())) == 2

    def test_dag_has_correct_edge(self):
        """dag has exactly one edge (source → function)."""
        pipeline = self._make_simple_pipeline()
        assert len(list(pipeline.dag.edges())) == 1

    def test_dag_topological_sort_works(self):
        """dag.topological_sort() returns a 2-element list."""
        pipeline = self._make_simple_pipeline()
        order = pipeline.dag.topological_sort()
        assert len(order) == 2

    def test_dag_predecessors_and_successors(self):
        """Source node has no predecessors; function node is its successor."""
        from orcapod.core.nodes import SourceNode, FunctionNode
        pipeline = self._make_simple_pipeline()
        source = next(n for n in pipeline.dag.nodes() if isinstance(n, SourceNode))
        fn_node = next(n for n in pipeline.dag.nodes() if isinstance(n, FunctionNode))
        assert pipeline.dag.successors(source) == {fn_node}
        assert pipeline.dag.predecessors(fn_node) == {source}

    def test_dag_raises_before_compile(self):
        """Accessing dag before compile() raises RuntimeError."""
        from orcapod.pipeline.graph import Pipeline
        pipeline = Pipeline(name="uncompiled", auto_compile=False)
        with pytest.raises(RuntimeError, match="not been compiled"):
            _ = pipeline.dag
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_pipeline/test_dag.py::TestPipelineDagProperty -v
```

Expected: FAIL with `AttributeError: 'Pipeline' object has no attribute 'dag'`

- [ ] **Step 3: Add `Generic[NodeT]` to `AbstractPipelineBase`, add `dag` property, fix `compile()` step 5**

In `src/orcapod/pipeline/base.py`, update the imports at the top:

```python
"""AbstractPipelineBase — shared recording mechanism for Pipeline and PipelineJob."""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from orcapod.core.tracker import AutoRegisteringContextBasedTracker
from orcapod.pipeline.dag import OrcaDAG
from orcapod.pipeline.pod_invocation import (
    FunctionInvocation,
    OperatorInvocation,
    PodInvocation,
)
from orcapod.protocols import core_protocols as cp
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import networkx as nx
else:
    nx = LazyModule("networkx")

logger = logging.getLogger(__name__)

NodeT = TypeVar("NodeT")
```

Update the class declaration:

```python
class AbstractPipelineBase(Generic[NodeT], AutoRegisteringContextBasedTracker, ABC):
```

Update the `__init__` field declarations (lines 88-91):

```python
        # --- Compiled state (populated / replaced by compile()) --------
        self._persistent_node_map: dict[str, NodeT] = {}  # type: ignore[assignment]
        self._nodes: dict[str, NodeT] = {}  # type: ignore[assignment]
        self._node_graph: OrcaDAG[NodeT] | None = None
        self._compiled: bool = False
```

Add the `dag` property immediately after the existing `nodes` property (after line 117):

```python
    @property
    def dag(self) -> OrcaDAG[NodeT]:
        """Node-object DAG for this pipeline.

        Returns an ``OrcaDAG`` whose vertices are the compiled node objects
        (``GraphNode`` for ``Pipeline``, ``JobNode`` for ``PipelineJob``) and
        whose edges follow the data-flow topology.

        Raises:
            RuntimeError: If the pipeline has not been compiled yet.
        """
        if self._node_graph is None:
            raise RuntimeError(
                "Pipeline has not been compiled. "
                "Use 'with pipeline:' or call compile() first."
            )
        return self._node_graph
```

Replace compile() step 5 (lines 357-366) — replace the `_nx.DiGraph()` block:

```python
        # 5. Build node_graph (OrcaDAG with node objects as vertices).
        node_dag: OrcaDAG[Any] = OrcaDAG()
        for up_hash, down_hash in self._hash_graph.edges():
            up_node = node_map.get(up_hash)
            down_node = node_map.get(down_hash)
            if up_node is not None and down_node is not None:
                node_dag.add_edge(up_node, down_node)
        for node in node_map.values():
            if node not in node_dag:
                node_dag.add_node(node)
        self._node_graph = node_dag  # type: ignore[assignment]
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_pipeline/test_dag.py::TestPipelineDagProperty -v
```

Expected: PASS (all 6 tests green)

- [ ] **Step 5: Run full test suite to check regressions**

```bash
uv run pytest tests/ -x -q 2>&1 | tail -20
```

Expected: Some failures in test files that call `nx.topological_sort(pipeline._node_graph)` — those will be fixed in Task 6. All other tests should still pass.

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/pipeline/base.py tests/test_pipeline/test_dag.py
git commit -m "feat(pipeline): make AbstractPipelineBase generic and add dag property

NodeT TypeVar, OrcaDAG[NodeT] _node_graph, dag property with RuntimeError guard,
compile() step 5 now builds OrcaDAG instead of nx.DiGraph.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 2: PipelineProtocol[NodeT]

**Files:**
- Modify: `src/orcapod/protocols/pipeline_protocols.py`
- Modify: `tests/test_pipeline/test_dag.py`

- [ ] **Step 1: Write failing test for structural conformance**

Add to `tests/test_pipeline/test_dag.py`:

```python
# ---------------------------------------------------------------------------
# PipelineProtocol structural conformance
# ---------------------------------------------------------------------------


class TestPipelineProtocolConformance:
    """Pipeline and PipelineJob satisfy PipelineProtocol structurally."""

    def test_pipeline_satisfies_protocol(self):
        """Pipeline is runtime-checkable as PipelineProtocol."""
        import pyarrow as pa
        from orcapod.core.sources.arrow_table_source import ArrowTableSource
        from orcapod.pipeline.graph import Pipeline
        from orcapod.protocols.pipeline_protocols import PipelineProtocol

        src = ArrowTableSource(
            pa.table({"id": pa.array(["a"], type=pa.large_string()), "v": pa.array([1], type=pa.int64())}),
            tag_columns=["id"],
            source_id="src",
        )
        pipeline = Pipeline(name="proto_test")
        with pipeline:
            pass  # no ops, just source wrapping

        assert isinstance(pipeline, PipelineProtocol)

    def test_pipeline_job_satisfies_protocol(self):
        """PipelineJob is runtime-checkable as PipelineProtocol."""
        import pyarrow as pa
        from orcapod.core.sources.arrow_table_source import ArrowTableSource
        from orcapod.databases.in_memory_arrow_database import InMemoryArrowDatabase
        from orcapod.pipeline.job import PipelineJob
        from orcapod.protocols.pipeline_protocols import PipelineProtocol

        src = ArrowTableSource(
            pa.table({"id": pa.array(["a"], type=pa.large_string()), "v": pa.array([1], type=pa.int64())}),
            tag_columns=["id"],
            source_id="src",
        )
        job = PipelineJob(store=InMemoryArrowDatabase())
        with job:
            pass

        assert isinstance(job, PipelineProtocol)

    def test_protocol_dag_returns_graph_protocol(self):
        """dag attribute declared on PipelineProtocol returns GraphProtocol."""
        import pyarrow as pa
        from orcapod.core.sources.arrow_table_source import ArrowTableSource
        from orcapod.pipeline.graph import Pipeline
        from orcapod.pipeline.dag import GraphProtocol

        src = ArrowTableSource(
            pa.table({"id": pa.array(["a"], type=pa.large_string()), "v": pa.array([1], type=pa.int64())}),
            tag_columns=["id"],
            source_id="src",
        )
        pipeline = Pipeline(name="proto_dag_test")
        with pipeline:
            pass

        assert isinstance(pipeline.dag, GraphProtocol)
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_pipeline/test_dag.py::TestPipelineProtocolConformance -v
```

Expected: FAIL with `ImportError: cannot import name 'PipelineProtocol'` (or `AttributeError`)

- [ ] **Step 3: Add `PipelineProtocol[NodeT]` to `pipeline_protocols.py`**

Replace the full contents of `src/orcapod/protocols/pipeline_protocols.py`:

```python
# Protocols for pipeline and nodes
from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, TypeVar, runtime_checkable

from orcapod.protocols import core_protocols as cp

if TYPE_CHECKING:
    import pyarrow as pa
    from orcapod.pipeline.dag import GraphProtocol

NodeT = TypeVar("NodeT")


@runtime_checkable
class PipelineProtocol(Protocol[NodeT]):
    """Structural protocol for ``Pipeline`` and ``PipelineJob``.

    Both ``Pipeline`` (``NodeT=GraphNode``) and ``PipelineJob``
    (``NodeT=JobNode``) satisfy this protocol.  Callers that only need
    DAG introspection can accept ``PipelineProtocol[Any]`` rather than
    importing the concrete classes.

    Note:
        The ``dag`` return type is ``GraphProtocol[NodeT]`` here (the abstract
        protocol).  Callers using the concrete classes receive the more
        specific ``OrcaDAG[NodeT]`` type.
    """

    @property
    def name(self) -> tuple[str, ...]:
        """Pipeline name tuple."""
        ...

    @property
    def nodes(self) -> dict[str, NodeT]:
        """Copy of the compiled label → node mapping."""
        ...

    @property
    def dag(self) -> "GraphProtocol[NodeT]":
        """Node-object DAG for topology traversal and introspection."""
        ...


class NodeProtocol(cp.Source, Protocol):
    # def record_pipeline_outputs(self):
    #     pass
    ...


@runtime_checkable
class PodNodeProtocol(cp.CachedPod, Protocol):
    def get_all_records(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        """
        Retrieve all tag and data processed by this PodProtocol.

        This method returns a table containing all data processed by the PodProtocol,
        including metadata and system columns if requested. It is useful for:
        - Debugging and analysis
        - Auditing and data lineage tracking
        - Performance monitoring

        Args:
            include_system_columns: Whether to include system columns in the output

        Returns:
            pa.Table | None: A table containing all processed records, or None if no records are available
        """
        ...

    def flush(self):
        """
        Flush any in-memory data to persistent storage.

        This method ensures that all buffered data is written to the underlying
        storage system, making it durable and consistent. It is useful for:
        - Ensuring data integrity before shutdown or restart
        - Committing changes after a batch of operations
        - Reducing memory usage by clearing buffers

        """
        ...

    def add_pipeline_record(
        self,
        tag: cp.TagProtocol,
        input_data: cp.DataProtocol,
        data_record_id: str,
        retrieved: bool | None = None,
        skip_cache_lookup: bool = False,
    ) -> None: ...
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
uv run pytest tests/test_pipeline/test_dag.py::TestPipelineProtocolConformance -v
```

Expected: PASS (all 3 tests green)

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/protocols/pipeline_protocols.py tests/test_pipeline/test_dag.py
git commit -m "feat(protocols): add PipelineProtocol[NodeT]

Generic structural protocol covering name, nodes, and dag.
Runtime-checkable so isinstance checks work.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 3: Pipeline(AbstractPipelineBase[GraphNode]) + Pipeline.load() + render_graph annotation

**Files:**
- Modify: `src/orcapod/pipeline/graph.py:34` (class decl), `graph.py:369-377` (load OrcaDAG), `graph.py:726-741` (render_graph annotation)

- [ ] **Step 1: Update `Pipeline` class declaration**

In `src/orcapod/pipeline/graph.py`, update the import at the top to include `GraphNode`:

```python
from orcapod.core.nodes import (
    FunctionNode,
    GraphNode,
    OperatorNode,
    SourceNode,
)
```

(This import already exists — `GraphNode` is already imported. No change needed.)

Change the class declaration at line 34:

```python
class Pipeline(AbstractPipelineBase[GraphNode]):
```

- [ ] **Step 2: Update `Pipeline.load()` to build OrcaDAG**

In `src/orcapod/pipeline/graph.py`, add `OrcaDAG` to the imports at the top of the file:

```python
from orcapod.pipeline.dag import OrcaDAG
```

Replace the `_node_graph` construction block in `Pipeline.load()` (lines 369-377):

```python
        node_dag: OrcaDAG[GraphNode] = OrcaDAG()
        for up_hash, down_hash in edges:
            up_node = reconstructed.get(up_hash)
            down_node = reconstructed.get(down_hash)
            if up_node is not None and down_node is not None:
                node_dag.add_edge(up_node, down_node)
        for node in reconstructed.values():
            if node not in node_dag:
                node_dag.add_node(node)
        pipeline._node_graph = node_dag
```

- [ ] **Step 3: Update `render_graph` type annotations**

In `src/orcapod/pipeline/graph.py`, update the `GraphRenderer.generate_dot` signature (line 601-606), `GraphRenderer.render_graph` (line 645-648), the module-level `render_graph` (line 726-729), and `render_graph_dark_theme` (line 744-746) to accept `GraphProtocol[Any]` instead of `nx.DiGraph`:

Add to the `TYPE_CHECKING` block at the top of `graph.py`:

```python
if TYPE_CHECKING:
    import networkx as nx
    from orcapod.pipeline.execution_context import ExecutionContext
```

becomes:

```python
if TYPE_CHECKING:
    import networkx as nx
    from orcapod.pipeline.dag import GraphProtocol
    from orcapod.pipeline.execution_context import ExecutionContext
```

Then update the four signatures:

```python
    def generate_dot(
        self,
        graph: "GraphProtocol[GraphNode]",
        ...
    ) -> str:
```

```python
    def render_graph(
        self,
        graph: "GraphProtocol[GraphNode]",
        ...
    ) -> str | None:
```

```python
def render_graph(
    graph: "GraphProtocol[GraphNode]",
    ...
) -> str | None:
```

```python
def render_graph_dark_theme(
    graph: "GraphProtocol[GraphNode]",
    ...
) -> str | None:
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/test_pipeline/test_serialization.py -v -k "save_load"
```

Expected: The `test_save_load_preserves_graph_shape` test now uses `loaded._node_graph` directly — it still fails because the test calls `_node_graph` (will be fixed in Task 6). Other serialization tests should pass.

```bash
uv run pytest tests/test_pipeline/test_dag.py -v
```

Expected: PASS (all dag tests still green)

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/pipeline/graph.py
git commit -m "feat(pipeline): Pipeline[GraphNode] + load() OrcaDAG + render_graph annotation

Pipeline class now explicitly parameterised as AbstractPipelineBase[GraphNode].
Pipeline.load() builds OrcaDAG[GraphNode] instead of nx.DiGraph.
render_graph/generate_dot accept GraphProtocol[GraphNode].

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 4: PipelineJob(AbstractPipelineBase[JobNode]) + as_pipeline() OrcaDAG fix

**Files:**
- Modify: `src/orcapod/pipeline/job.py:28` (class decl), `job.py:530-540` (as_pipeline extracted pipeline._node_graph)

- [ ] **Step 1: Update `PipelineJob` class declaration**

In `src/orcapod/pipeline/job.py`, add `JobNode` to the top-level imports:

```python
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.nodes.operator_node import OperatorJobNode
from orcapod.core.nodes.source_node import SourceJobNode
```

becomes:

```python
from orcapod.core.nodes import JobNode
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.core.nodes.operator_node import OperatorJobNode
from orcapod.core.nodes.source_node import SourceJobNode
```

Change the class declaration at line 28:

```python
class PipelineJob(AbstractPipelineBase[JobNode]):
```

- [ ] **Step 2: Fix the extracted `pipeline._node_graph` build in `as_pipeline()`**

In `src/orcapod/pipeline/job.py`, find the block starting with `# Build _node_graph (DiGraph with node objects as vertices).` around line 530. Replace it:

```python
        # Build _node_graph (OrcaDAG with node objects as vertices).
        # Mirrors AbstractPipelineBase.compile() step 5.
        # node_map here contains GraphNode objects (blueprint nodes).
        from orcapod.pipeline.dag import OrcaDAG as _OrcaDAG
        bp_dag: _OrcaDAG = _OrcaDAG()
        for up_h, down_h in self._hash_graph.edges():
            up_node = node_map.get(up_h)
            down_node = node_map.get(down_h)
            if up_node is not None and down_node is not None:
                bp_dag.add_edge(up_node, down_node)
        for node in node_map.values():
            if node not in bp_dag:
                bp_dag.add_node(node)
        pipeline._node_graph = bp_dag
```

Note: `OrcaDAG` is already imported at the top of `job.py` (`from orcapod.pipeline.dag import OrcaDAG`). Use it directly without the local alias:

```python
        # Build _node_graph (OrcaDAG with node objects as vertices).
        # Mirrors AbstractPipelineBase.compile() step 5.
        # node_map here contains GraphNode objects (blueprint nodes).
        bp_dag: OrcaDAG = OrcaDAG()
        for up_h, down_h in self._hash_graph.edges():
            up_node = node_map.get(up_h)
            down_node = node_map.get(down_h)
            if up_node is not None and down_node is not None:
                bp_dag.add_edge(up_node, down_node)
        for node in node_map.values():
            if node not in bp_dag:
                bp_dag.add_node(node)
        pipeline._node_graph = bp_dag
```

Also remove the now-unused `_nx` local import in `as_pipeline()`. Find the line:
```python
        import networkx as _nx
```
and remove it (it was used only for `_nx.DiGraph()`).

- [ ] **Step 3: Run tests**

```bash
uv run pytest tests/test_pipeline/test_dag.py tests/test_pipeline/test_pipeline.py -v -q 2>&1 | tail -20
```

Expected: `TestPipelineDagProperty` and `TestPipelineProtocolConformance` still pass. `test_pipeline.py` failures at `_node_graph` sites are expected (fixed in Task 6).

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/pipeline/job.py
git commit -m "feat(pipeline): PipelineJob[JobNode] + as_pipeline() OrcaDAG fix

PipelineJob explicitly parameterised as AbstractPipelineBase[JobNode].
as_pipeline() now builds OrcaDAG for the extracted Pipeline blueprint.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 5: PipelineJob.load() — fix job._node_graph to OrcaDAG[JobNode]

**Files:**
- Modify: `src/orcapod/pipeline/job.py:1080-1083`
- Test: `tests/test_pipeline/test_dag.py`

- [ ] **Step 1: Write failing test for `job.dag` on a loaded job**

Add to `tests/test_pipeline/test_dag.py`:

```python
class TestPipelineJobDagOnLoadedJob:
    """job.dag returns OrcaDAG[JobNode] even for loaded (not compiled) jobs."""

    def test_loaded_job_dag_returns_job_nodes(self, tmp_path):
        """PipelineJob.load() must produce OrcaDAG with JobNode objects."""
        import pyarrow as pa
        from orcapod.core.sources.arrow_table_source import ArrowTableSource
        from orcapod.core.function_pod import FunctionPod
        from orcapod.core.data_function import PythonDataFunction
        from orcapod.core.nodes import JobNode
        from orcapod.databases.in_memory_arrow_database import InMemoryArrowDatabase
        from orcapod.pipeline.graph import Pipeline
        from orcapod.pipeline.job import PipelineJob
        from orcapod.types import Schema

        src = ArrowTableSource(
            pa.table({"id": pa.array(["a"], type=pa.large_string()), "v": pa.array([1], type=pa.int64())}),
            tag_columns=["id"],
            source_id="src",
        )
        fn = PythonDataFunction(
            func=lambda v: {"out": v},
            input_schema=Schema({"v": int}),
            output_schema=Schema({"out": int}),
        )
        pod = FunctionPod(fn)

        # Build, run, and save a pipeline
        pipeline = Pipeline(name="load_dag_test")
        with pipeline:
            pod(src)
        job = PipelineJob.from_pipeline(
            pipeline, sources={"src": src}, store=InMemoryArrowDatabase()
        )
        job.run()
        save_path = tmp_path / "job.json"
        job.save(str(save_path))

        # Load and check dag
        loaded_job = PipelineJob.load(str(save_path))
        assert isinstance(loaded_job.dag, OrcaDAG)
        # All nodes in the loaded job's dag must be JobNode instances
        for node in loaded_job.dag.nodes():
            assert isinstance(node, JobNode), (
                f"Expected JobNode, got {type(node).__name__}"
            )
```

- [ ] **Step 2: Run test to verify it fails**

```bash
uv run pytest tests/test_pipeline/test_dag.py::TestPipelineJobDagOnLoadedJob -v
```

Expected: FAIL — the loaded job's dag currently contains `GraphNode` (blueprint) objects instead of `JobNode`.

- [ ] **Step 3: Fix `PipelineJob.load()` to build `OrcaDAG[JobNode]`**

In `src/orcapod/pipeline/job.py`, find the block around line 1080:

```python
        # Copy pipeline._node_graph for graph-introspection use (renderers, etc.).
        # Nodes here are SourceNode / FunctionNode / OperatorNode from the blueprint;
        # is_runnable() does not use this graph and is unaffected.
        job._node_graph = pipeline._node_graph
```

Replace it with:

```python
        # Build job._node_graph as OrcaDAG[JobNode] from the job's own persistent
        # node map.  This makes job.dag consistent with the compiled-job path
        # (where compile() builds _node_graph from job nodes).
        # Callers needing blueprint nodes for rendering use job.pipeline.dag.
        job_dag: OrcaDAG = OrcaDAG()
        for node in job._persistent_node_map.values():
            job_dag.add_node(node)
        for u_hash, v_hash in job._graph_edges:
            u_node = job._persistent_node_map.get(u_hash)
            v_node = job._persistent_node_map.get(v_hash)
            if u_node is not None and v_node is not None:
                job_dag.add_edge(u_node, v_node)
        job._node_graph = job_dag
```

- [ ] **Step 4: Run test to verify it passes**

```bash
uv run pytest tests/test_pipeline/test_dag.py::TestPipelineJobDagOnLoadedJob -v
```

Expected: PASS

- [ ] **Step 5: Run full dag test suite**

```bash
uv run pytest tests/test_pipeline/test_dag.py -v
```

Expected: All tests pass (including `TestPipelineDagProperty`, `TestPipelineProtocolConformance`, `TestPipelineJobDagOnLoadedJob`).

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/pipeline/job.py tests/test_pipeline/test_dag.py
git commit -m "fix(pipeline): PipelineJob.load() builds OrcaDAG[JobNode] for job.dag

Previously copied blueprint (GraphNode) objects from pipeline._node_graph.
Now builds OrcaDAG from _persistent_node_map so job.dag is consistent
with the compiled-job path. Callers needing blueprint nodes use job.pipeline.dag.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Task 6: Migrate all test call sites from `_node_graph` to `dag`

**Files:**
- Modify: `tests/test_pipeline/test_pipeline.py`
- Modify: `tests/test_pipeline/test_pipeline_job.py`
- Modify: `tests/test_pipeline/test_serialization.py`
- Modify: `tests/test_pipeline/test_logging_observer_integration.py`
- Modify: `tests/test_pipeline/test_graph_rendering.py`
- Modify: `tests/test_pipeline/test_status_observer_integration.py`
- Modify: `tests/test_pipeline/test_orchestrator.py`
- Modify: `tests/test_pipeline/test_orchestrator_executor_matrix.py`
- Modify: `tests/test_pipeline/test_sync_orchestrator.py`
- Modify: `tests/test_channels/test_pipeline_async_integration.py`

- [ ] **Step 1: Migrate `test_pipeline.py` (4 sites)**

Replace all 4 occurrences of `pipeline._node_graph.nodes()` with `pipeline.dag.nodes()`:

```python
# Line 102: was
source_nodes = [
    n for n in pipeline._node_graph.nodes() if isinstance(n, SourceNode)
]
# becomes
source_nodes = [
    n for n in pipeline.dag.nodes() if isinstance(n, SourceNode)
]

# Line 115: same pattern
source_nodes = [n for n in pipeline.dag.nodes() if isinstance(n, SourceNode)]

# Line 156: same pattern
source_nodes = [n for n in pipeline.dag.nodes() if isinstance(n, SourceNode)]

# Line 678: was job.pipeline._node_graph
source_nodes = [
    n for n in job.pipeline.dag.nodes() if isinstance(n, SourceNode)
]
```

- [ ] **Step 2: Migrate `test_pipeline_job.py` (1 site)**

```python
# Line 73: was job.pipeline._node_graph
source_nodes = [
    n for n in job.pipeline.dag.nodes() if isinstance(n, SourceNode)
]
```

- [ ] **Step 3: Migrate `test_serialization.py` (2 sites)**

```python
# Line 83: was
assert len(list(loaded._node_graph.edges())) == len(list(pipeline._node_graph.edges()))
# becomes
assert len(list(loaded.dag.edges())) == len(list(pipeline.dag.edges()))
```

- [ ] **Step 4: Migrate `test_logging_observer_integration.py`**

Replace the `_get_function_node` helper (lines 39-46):

```python
def _get_function_node(pipeline: Pipeline):
    """Return the first function node from the pipeline graph."""
    for node in pipeline.dag.topological_sort():
        if node.node_type == "function":
            return node
    raise RuntimeError("No function node found")
```

Remove the `import networkx as nx` inside the function (it's no longer needed).

- [ ] **Step 5: Migrate `test_graph_rendering.py` (fixture + 2 sites)**

Update the `node_graph` fixture (lines 80-83):

```python
@pytest.fixture
def node_graph(compiled_pipeline: Pipeline) -> OrcaDAG:
    assert compiled_pipeline._compiled
    return compiled_pipeline.dag
```

Add `from orcapod.pipeline.dag import OrcaDAG` to the imports at the top of the test file, and remove `import networkx as nx` if it's only used for this fixture.

Update lines 319-320:

```python
# was: assert compiled_pipeline._node_graph is not None
assert compiled_pipeline._compiled
# was: first_node = next(iter(compiled_pipeline._node_graph.nodes()))
first_node = next(iter(compiled_pipeline.dag.nodes()))
```

- [ ] **Step 6: Migrate `test_status_observer_integration.py`**

Replace the `_get_function_node` helper (same pattern as logging):

```python
def _get_function_node(pipeline: Pipeline):
    """Return the first function node from the pipeline graph."""
    for node in pipeline.dag.topological_sort():
        if node.node_type == "function":
            return node
    raise RuntimeError("No function node found")
```

Replace line 454:
```python
# was
orch.run(pipeline._node_graph, observer=obs, run_id="my-custom-run-id")
# becomes
orch.run(pipeline.dag, observer=obs, run_id="my-custom-run-id")
```

- [ ] **Step 7: Migrate `test_orchestrator.py` (~13 sites)**

Replace every `pipeline._node_graph` with `pipeline.dag`:

```python
# All occurrences of the pattern:
AsyncPipelineOrchestrator().run(pipeline._node_graph)
# become:
AsyncPipelineOrchestrator().run(pipeline.dag)

# And:
await orchestrator.run_async(pipeline._node_graph)
# becomes:
await orchestrator.run_async(pipeline.dag)

# And:
orch.run(pipeline._node_graph, ...)
# becomes:
orch.run(pipeline.dag, ...)

# And:
result = orch.run(pipeline._node_graph, materialize_results=True)
# becomes:
result = orch.run(pipeline.dag, materialize_results=True)
```

Run a quick verify after this file:

```bash
uv run pytest tests/test_pipeline/test_orchestrator.py -v -q 2>&1 | tail -10
```

- [ ] **Step 8: Migrate `test_orchestrator_executor_matrix.py` (2 sites)**

```python
# Line 177:
result = orch.run(pipeline.dag)
# Line 511:
await AsyncPipelineOrchestrator().run_async(pipeline.dag)
```

- [ ] **Step 9: Migrate `test_sync_orchestrator.py` (~15 sites)**

Replace every `pipeline._node_graph` and `async_pipeline._node_graph` with `.dag`:

```python
# All occurrences:
orch.run(pipeline._node_graph)          → orch.run(pipeline.dag)
orch.run(pipeline._node_graph, ...)     → orch.run(pipeline.dag, ...)
AsyncPipelineOrchestrator().run(async_pipeline._node_graph)
    → AsyncPipelineOrchestrator().run(async_pipeline.dag)
```

- [ ] **Step 10: Migrate `test_pipeline_async_integration.py` — delete `_build_exec_dag`**

Delete the `_build_exec_dag` helper function (lines 103-120).

Replace every call `exec_dag = _build_exec_dag(job)` with `exec_dag = job.dag`:

```python
# was:
exec_dag = _build_exec_dag(job)
AsyncPipelineOrchestrator().run(exec_dag)
# becomes:
AsyncPipelineOrchestrator().run(job.dag)

# was:
exec_dag = _build_exec_dag(async_job)
AsyncPipelineOrchestrator().run(exec_dag)
# becomes:
AsyncPipelineOrchestrator().run(async_job.dag)

# And for run_async:
exec_dag = _build_exec_dag(job)
await orchestrator.run_async(exec_dag)
# becomes:
await orchestrator.run_async(job.dag)
```

- [ ] **Step 11: Run the full test suite**

```bash
uv run pytest tests/ -q 2>&1 | tail -20
```

Expected: All tests pass. Zero failures.

- [ ] **Step 12: Commit**

```bash
git add \
  tests/test_pipeline/test_pipeline.py \
  tests/test_pipeline/test_pipeline_job.py \
  tests/test_pipeline/test_serialization.py \
  tests/test_pipeline/test_logging_observer_integration.py \
  tests/test_pipeline/test_graph_rendering.py \
  tests/test_pipeline/test_status_observer_integration.py \
  tests/test_pipeline/test_orchestrator.py \
  tests/test_pipeline/test_orchestrator_executor_matrix.py \
  tests/test_pipeline/test_sync_orchestrator.py \
  tests/test_channels/test_pipeline_async_integration.py
git commit -m "refactor(tests): migrate _node_graph → dag across all test files

All test call sites updated to use the public dag property.
_build_exec_dag helper in async integration tests deleted (job.dag replaces it).
import networkx as nx removed where no longer needed.

Co-Authored-By: Claude Sonnet 4.6 <noreply@anthropic.com>"
```

---

## Self-Review Checklist

- [x] **Spec coverage:** `PipelineProtocol[NodeT]` ✓ Task 2. `AbstractPipelineBase[NodeT]` ✓ Task 1. `_node_graph` → `OrcaDAG` in all four sites ✓ Tasks 1, 3, 4, 5. `dag` property with guard ✓ Task 1. Call-site migration ✓ Task 6. New tests ✓ Tasks 1, 2, 5.
- [x] **Placeholder scan:** No TBDs or "add appropriate handling" patterns found.
- [x] **Type consistency:** `OrcaDAG` (not `OrcaDAG[Any]`) used consistently in non-generic contexts. `dag` property returns `OrcaDAG[NodeT]` in base, `GraphProtocol[NodeT]` in protocol.
