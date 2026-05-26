# ENG-517: PipelineJob Execution Graph Cleanup — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the copy-on-run pattern from `PipelineJob.run()`, wire `GraphProtocol`/`OrcaDAG` into the orchestrator boundary, and consolidate the `unbound_sources` API.

**Architecture:** `run()` builds a filtered `OrcaDAG[JobNode]` directly from `_persistent_node_map` (no node cloning), hands it to the orchestrator, then mutates `self._has_run`/`_run_id` in place and returns `self`. Orchestrators are updated to accept `GraphProtocol[JobNode]` instead of `nx.DiGraph`. `unbound_source_nodes()` and `_unresolved_specs`/`unresolved_specs` are removed; a single live-computed `unbound_sources` property replaces both.

**Tech Stack:** Python 3.11+, `uv run pytest` for all test runs, `graphlib.TopologicalSorter` (stdlib) inside `OrcaDAG`.

---

## File Map

| File | Change |
|---|---|
| `src/orcapod/pipeline/dag.py` | Rename `GraphBackend` → `GraphProtocol`; add `ancestors()` to protocol + `OrcaDAG` |
| `src/orcapod/pipeline/networkx_backend.py` | Update docstring reference; add `ancestors()` |
| `src/orcapod/pipeline/job.py` | Add `unbound_sources`; remove `unbound_source_nodes`, `_unresolved_specs`, `unresolved_specs`; rename `spec_names`→`source_names`; update `save`/`load`; rewrite `run()`; delete `_build_execution_graph()` / `build_execution_graph()` |
| `src/orcapod/pipeline/sync_orchestrator.py` | Accept `GraphProtocol`, drop `nx.topological_sort` |
| `src/orcapod/pipeline/async_orchestrator.py` | Accept `GraphProtocol`, drop `nx.topological_sort` |
| `tests/test_pipeline/test_dag.py` | Add `ancestors()` tests |
| `tests/test_pipeline/test_networkx_backend.py` | Update `GraphBackend` → `GraphProtocol`; add `ancestors()` test |
| `tests/test_pipeline/test_pipeline_job.py` | Migrate `unbound_source_nodes` → `unbound_sources`; `unresolved_specs` → `unbound_sources`; `result.pipeline.compiled_nodes` → `result.compiled_nodes`; rewrite `test_run_is_non_mutating` |
| `tests/test_pipeline/test_serialization.py` | `unresolved_specs` → `unbound_sources` |

---

## Task 1: Create the feature branch

**Files:** none

- [ ] **Step 1: Check out the branch**

```bash
cd /home/kurouto/kurouto-jobs/71dc4f00-85e9-4767-a435-8d1f5e163103/orcapod-python
git checkout -b eywalker/eng-517-review-cleanup-of-pipelinejob-execution-graph-remove
git branch --show-current
```

Expected output: `eywalker/eng-517-review-cleanup-of-pipelinejob-execution-graph-remove`

---

## Task 2: Rename `GraphBackend` → `GraphProtocol` and add `ancestors()` to `OrcaDAG`

**Files:**
- Modify: `src/orcapod/pipeline/dag.py` (lines 33, 59–95, 348)
- Modify: `tests/test_pipeline/test_dag.py`
- Modify: `tests/test_pipeline/test_networkx_backend.py` (lines 4–5, 14, 23, 26, 30, 33, 35)

- [ ] **Step 1: Write failing tests for `ancestors()`**

Add to the bottom of `tests/test_pipeline/test_dag.py`:

```python
class TestOrcaDAGAncestors:
    def test_ancestors_of_source_node_is_empty(self):
        """A source node (no predecessors) has no ancestors."""
        dag = OrcaDAG()
        dag.add_node("a")
        assert dag.ancestors("a") == frozenset()

    def test_ancestors_returns_direct_predecessor(self):
        dag = OrcaDAG()
        dag.add_edge("a", "b")
        assert dag.ancestors("b") == frozenset({"a"})

    def test_ancestors_returns_transitive_predecessors(self):
        """ancestors() walks all the way back to source nodes."""
        dag = OrcaDAG()
        dag.add_edge("a", "b")
        dag.add_edge("b", "c")
        assert dag.ancestors("c") == frozenset({"a", "b"})
        assert dag.ancestors("b") == frozenset({"a"})

    def test_ancestors_handles_diamond(self):
        """Two paths to same ancestor — no duplicates."""
        dag = OrcaDAG()
        dag.add_edge("a", "b")
        dag.add_edge("a", "c")
        dag.add_edge("b", "d")
        dag.add_edge("c", "d")
        assert dag.ancestors("d") == frozenset({"a", "b", "c"})
```

- [ ] **Step 2: Run tests to verify they fail**

```bash
uv run pytest tests/test_pipeline/test_dag.py::TestOrcaDAGAncestors -v
```

Expected: `AttributeError: 'OrcaDAG' object has no attribute 'ancestors'`

- [ ] **Step 3: Rename `GraphBackend` → `GraphProtocol` and add `ancestors()` in `dag.py`**

In `src/orcapod/pipeline/dag.py`:

**Line 33** — update `__all__`:
```python
__all__ = ["Comparable", "GraphProtocol", "OrcaDAG", "CycleError"]
```

**Lines 58–95** — rename class and add `ancestors()` stub at end of protocol:
```python
@runtime_checkable
class GraphProtocol(Protocol[NodeT]):
    """Structural protocol for DAG backend implementations.

    Both ``OrcaDAG`` and ``NetworkxBackend`` satisfy this protocol, enabling
    callers to switch graph implementations via a config flag (ENG-494).

    NodeT must be ``Hashable``.  The ``topological_sort_deterministic`` method
    is not part of this protocol because it additionally requires NodeT to
    satisfy ``Comparable``; callers that need deterministic ordering should
    use ``OrcaDAG`` or ``NetworkxBackend`` directly with a comparable node type
    (e.g. ``OrcaDAG[str]``).
    """

    def add_node(self, node: NodeT, **attrs: Any) -> None: ...

    def add_edge(self, u: NodeT, v: NodeT) -> None: ...

    def node_attrs(self, node: NodeT) -> dict[str, Any]: ...

    def __contains__(self, node: object) -> bool: ...

    def __len__(self) -> int: ...

    def __iter__(self) -> Iterator[NodeT]: ...

    def nodes(self) -> Iterable[NodeT]: ...

    def edges(self) -> Iterable[tuple[NodeT, NodeT]]: ...

    def successors(self, node: NodeT) -> frozenset[NodeT]: ...

    def predecessors(self, node: NodeT) -> frozenset[NodeT]: ...

    def in_degree(self, node: NodeT) -> int: ...

    def topological_sort(self) -> list[NodeT]: ...

    def ancestors(self, node: NodeT) -> frozenset[NodeT]: ...
```

**After line 348** (end of `topological_sort_deterministic`) — add `ancestors()` to `OrcaDAG`:
```python
    def ancestors(self, node: NodeT) -> frozenset[NodeT]:
        """Return all transitive predecessors of *node* via BFS.

        Args:
            node: The node whose ancestors to find.

        Returns:
            Frozen set of all nodes from which there is a directed path to
            *node*. Empty if *node* is a source (no incoming edges).

        Raises:
            KeyError: If *node* is not in the graph.
        """
        _ = self._predecessors[node]  # raise KeyError if absent
        visited: set[NodeT] = set()
        queue: list[NodeT] = list(self._predecessors[node])
        while queue:
            n = queue.pop()
            if n not in visited:
                visited.add(n)
                queue.extend(self._predecessors.get(n, set()))
        return frozenset(visited)
```

- [ ] **Step 4: Update `test_networkx_backend.py` to use `GraphProtocol`**

In `tests/test_pipeline/test_networkx_backend.py`, make these replacements:
- Line 14: `from orcapod.pipeline.dag import GraphBackend, OrcaDAG` → `from orcapod.pipeline.dag import GraphProtocol, OrcaDAG`
- Line 26: `assert isinstance(dag, GraphBackend)` → `assert isinstance(dag, GraphProtocol)`
- Line 30: `assert isinstance(backend, GraphBackend)` → `assert isinstance(backend, GraphProtocol)`
- Line 33: docstring `"""Verify both backends can be used interchangeably via GraphBackend."""` → `"""Verify both backends can be used interchangeably via GraphProtocol."""`
- Line 35: `def populate(g: GraphBackend[str]) -> None:` → `def populate(g: GraphProtocol[str]) -> None:`
- Lines 4–5: Update module docstring references from `GraphBackend` to `GraphProtocol`

- [ ] **Step 5: Run all dag and networkx_backend tests**

```bash
uv run pytest tests/test_pipeline/test_dag.py tests/test_pipeline/test_networkx_backend.py -v
```

Expected: all pass (new `ancestors()` tests now green; `GraphProtocol` rename tests still green).

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/pipeline/dag.py tests/test_pipeline/test_dag.py tests/test_pipeline/test_networkx_backend.py
git commit -m "refactor(pipeline): rename GraphBackend → GraphProtocol; add ancestors() to OrcaDAG"
```

---

## Task 3: Add `ancestors()` to `NetworkxBackend`

**Files:**
- Modify: `src/orcapod/pipeline/networkx_backend.py` (docstring line ~46, after last method ~line 282)
- Modify: `tests/test_pipeline/test_networkx_backend.py`

- [ ] **Step 1: Write failing test for `NetworkxBackend.ancestors()`**

Add to `tests/test_pipeline/test_networkx_backend.py` (after existing tests):

```python
class TestNetworkxBackendAncestors:
    def test_ancestors_returns_transitive_predecessors(self):
        from orcapod.pipeline.networkx_backend import NetworkxBackend

        backend = NetworkxBackend()
        backend.add_edge("a", "b")
        backend.add_edge("b", "c")
        assert backend.ancestors("c") == frozenset({"a", "b"})
        assert backend.ancestors("b") == frozenset({"a"})
        assert backend.ancestors("a") == frozenset()
```

- [ ] **Step 2: Run to confirm failure**

```bash
uv run pytest tests/test_pipeline/test_networkx_backend.py::TestNetworkxBackendAncestors -v
```

Expected: `AttributeError: 'NetworkxBackend' object has no attribute 'ancestors'`

- [ ] **Step 3: Add `ancestors()` to `NetworkxBackend` and update docstring**

In `src/orcapod/pipeline/networkx_backend.py`:

**Docstring** (line ~46): change `satisfying the \`GraphBackend\` protocol` → `satisfying the \`GraphProtocol\` protocol`

**After the last method** (after `topological_sort_deterministic`, ~line 282), add:

```python
    def ancestors(self, node: NodeT) -> frozenset[NodeT]:
        """Return all transitive predecessors of *node*.

        Args:
            node: The node whose ancestors to find.

        Returns:
            Frozen set of all nodes from which there is a directed path to
            *node*.
        """
        import networkx as nx

        return frozenset(nx.ancestors(self._g, node))
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/test_pipeline/test_networkx_backend.py -v
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/pipeline/networkx_backend.py tests/test_pipeline/test_networkx_backend.py
git commit -m "refactor(pipeline): add ancestors() to NetworkxBackend; update GraphProtocol reference"
```

---

## Task 4: Add `unbound_sources` property to `PipelineJob`

**Files:**
- Modify: `src/orcapod/pipeline/job.py` (after `is_complete()` at line ~597)
- Modify: `tests/test_pipeline/test_pipeline_job.py`

- [ ] **Step 1: Write failing tests for `unbound_sources`**

Add a new test class to `tests/test_pipeline/test_pipeline_job.py` immediately after `TestPipelineJobCompleteness` (around line 294):

```python
class TestUnboundSources:
    def test_unbound_sources_returns_names_of_unbound_source_nodes(self, store):
        """unbound_sources lists the name of each unbound SourceJobNode slot."""
        src_a, src_b = _make_two_sources()
        tag_b, data_b = src_b.output_schema()
        node_b = SourceNode(name="spec_b", tag_schema=tag_b, data_schema=data_b)

        job = PipelineJob(store=store)
        with job:
            Join()(src_a, node_b)

        assert job.unbound_sources == ["spec_b"]

    def test_unbound_sources_empty_when_all_bound(self, store):
        """unbound_sources is empty when all sources are bound."""
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b)

        assert job.unbound_sources == []

    def test_unbound_sources_empty_before_compile(self):
        """unbound_sources returns [] when job is not yet compiled."""
        job = PipelineJob()
        assert job.unbound_sources == []

    def test_unbound_sources_reflects_bind_call(self, store):
        """After binding a source, it no longer appears in unbound_sources."""
        src_a, src_b = _make_two_sources()
        tag_b, data_b = src_b.output_schema()
        node_b = SourceNode(name="spec_b", tag_schema=tag_b, data_schema=data_b)

        job = PipelineJob(store=store)
        with job:
            Join()(src_a, node_b)

        assert "spec_b" in job.unbound_sources
        job.bind(sources={"spec_b": src_b})
        assert job.unbound_sources == []
```

- [ ] **Step 2: Run to confirm failure**

```bash
uv run pytest tests/test_pipeline/test_pipeline_job.py::TestUnboundSources -v
```

Expected: `AttributeError: 'PipelineJob' object has no attribute 'unbound_sources'`

- [ ] **Step 3: Add `unbound_sources` property to `job.py`**

In `src/orcapod/pipeline/job.py`, add the following property immediately after `is_complete()` (after line 597), inside the `# Completeness introspection` section:

```python
    @property
    def unbound_sources(self) -> list[str]:
        """Names of source slots not yet bound in this job.

        Computed live from ``_persistent_node_map`` — reflects the current
        binding state at all times (before and after run).

        Returns:
            List of unbound source slot names, in order of appearance in
            the pipeline graph. Empty list if the job is not yet compiled.
        """
        from orcapod.core.nodes.source_node import SourceJobNode

        if not self._compiled:
            return []
        seen: set[str] = set()
        result: list[str] = []
        for node in (self._persistent_node_map or {}).values():
            if (
                isinstance(node, SourceJobNode)
                and node.bound_source is None
                and node.name not in seen
            ):
                result.append(node.name)
                seen.add(node.name)
        return result
```

- [ ] **Step 4: Run tests**

```bash
uv run pytest tests/test_pipeline/test_pipeline_job.py::TestUnboundSources -v
```

Expected: all 4 pass.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/pipeline/job.py tests/test_pipeline/test_pipeline_job.py
git commit -m "feat(pipeline): add unbound_sources live-computed property to PipelineJob"
```

---

## Task 5: Remove old API, update `is_complete()`, `save()`, `load()`, `bind()`

**Files:**
- Modify: `src/orcapod/pipeline/job.py`

This task removes the old surface that `unbound_sources` now replaces, and updates related code.

- [ ] **Step 1: Update `is_complete()` (line 591–597)**

Replace:
```python
    def is_complete(self) -> bool:
        """Return ``True`` when all source nodes are bound and a store is set.

        Returns:
            ``True`` if all SourceNode slots are bound and a store is set.
        """
        return self._store is not None and len(self.unbound_source_nodes()) == 0
```

With:
```python
    def is_complete(self) -> bool:
        """Return ``True`` when all source nodes are bound and a store is set.

        Returns:
            ``True`` if all SourceNode slots are bound and a store is set.
        """
        return self._store is not None and not self.unbound_sources
```

- [ ] **Step 2: Delete `unbound_source_nodes()` (lines 567–589)**

Remove the entire `unbound_source_nodes()` method:
```python
    def unbound_source_nodes(self) -> "list[Any]":
        ...
        return unbound
```

- [ ] **Step 3: Delete `unresolved_specs` property (lines 648–659)**

Remove the entire `unresolved_specs` property block:
```python
    # unresolved_specs property
    # ...
    @property
    def unresolved_specs(self) -> list[str]:
        ...
        return list(self._unresolved_specs)
```

- [ ] **Step 4: Remove `_unresolved_specs` from `__init__` (line 84)**

Remove this line from `__init__`:
```python
        self._unresolved_specs: list[str] = []
```

- [ ] **Step 5: Rename `spec_names` → `source_names` in `bind()` — first occurrence (lines 326–336)**

Replace:
```python
            spec_names = {
                node.name
                for node in (self._persistent_node_map or {}).values()
                if isinstance(node, SourceNodeBase)
            }
            unknown = set(sources.keys()) - spec_names
            if unknown:
                raise ValueError(
                    f"bind() received source keys with no matching source slot "
                    f"(SourceJobNode): {sorted(unknown)}. "
                    f"Known slot names: {sorted(spec_names)}"
                )
```

With:
```python
            source_names = {
                node.name
                for node in (self._persistent_node_map or {}).values()
                if isinstance(node, SourceNodeBase)
            }
            unknown = set(sources.keys()) - source_names
            if unknown:
                raise ValueError(
                    f"bind() received source keys with no matching source slot "
                    f"(SourceJobNode): {sorted(unknown)}. "
                    f"Known slot names: {sorted(source_names)}"
                )
```

- [ ] **Step 6: Rename `spec_names` → `source_names` in `from_pipeline()` — second occurrence (lines 252–261)**

Replace:
```python
            spec_names = {
                node.name
                for node in pipeline._persistent_node_map.values()
                if isinstance(node, SourceNodeBase)
            }
            unknown = set(bound_sources.keys()) - spec_names
            if unknown:
                raise ValueError(
                    f"from_pipeline() received source keys with no matching SourceNode: "
                    f"{sorted(unknown)}. Known names: {sorted(spec_names)}"
                )
```

With:
```python
            source_names = {
                node.name
                for node in pipeline._persistent_node_map.values()
                if isinstance(node, SourceNodeBase)
            }
            unknown = set(bound_sources.keys()) - source_names
            if unknown:
                raise ValueError(
                    f"from_pipeline() received source keys with no matching SourceNode: "
                    f"{sorted(unknown)}. Known names: {sorted(source_names)}"
                )
```

- [ ] **Step 7: Update `save()` JSON key (line 987–997)**

Find the block that writes `"unresolved_specs"` to the run block and update the key:

Replace:
```python
        elif self._unresolved_specs:
```
With:
```python
        elif self.unbound_sources:
```

And replace:
```python
                "unresolved_specs": list(self._unresolved_specs),
```
With:
```python
                "unbound_sources": list(self.unbound_sources),
```

- [ ] **Step 8: Update `load()` to remove `_unresolved_specs` restoration (line 1149)**

Remove this line from `load()`:
```python
        job._unresolved_specs = run_block.get("unresolved_specs", [])
```

(`unbound_sources` is live-computed from loaded `SourceJobNode` stubs — no restore needed.)

- [ ] **Step 9: Run the completeness-related tests to confirm they still pass**

```bash
uv run pytest tests/test_pipeline/test_pipeline_job.py::TestPipelineJobCompleteness tests/test_pipeline/test_pipeline_job.py::TestUnboundSources -v
```

Expected: all pass.

- [ ] **Step 10: Commit**

```bash
git add src/orcapod/pipeline/job.py
git commit -m "refactor(pipeline): remove unbound_source_nodes/unresolved_specs; rename spec_names→source_names; update save/load"
```

---

## Task 6: Migrate all tests to new API

**Files:**
- Modify: `tests/test_pipeline/test_pipeline_job.py`
- Modify: `tests/test_pipeline/test_serialization.py`

- [ ] **Step 1: Update `TestPipelineJobCompleteness` to use `unbound_sources`**

In `tests/test_pipeline/test_pipeline_job.py`, replace the two `unbound_source_nodes()` calls:

**`test_unbound_specs_lists_unbound` (lines 246–258):**
```python
    def test_unbound_specs_lists_unbound(self, store):
        """unbound_sources lists names of unbound SourceJobNode slots."""
        src_a, src_b = _make_two_sources()
        tag_b, data_b = src_b.output_schema()
        node_b = SourceNode(name="spec_b", tag_schema=tag_b, data_schema=data_b)

        job = PipelineJob(store=store)
        with job:
            Join()(src_a, node_b)

        assert job.unbound_sources == ["spec_b"]

    def test_unbound_specs_empty_when_all_bound(self, store):
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b)

        assert job.unbound_sources == []
```

- [ ] **Step 2: Update `test_run_executes_all_nodes` — `result.pipeline.compiled_nodes` → `result.compiled_nodes` (line 363)**

Replace:
```python
        node = result.pipeline.compiled_nodes["adder"]
```
With:
```python
        node = result.compiled_nodes["adder"]
```

- [ ] **Step 3: Update `test_run_produces_correct_values` (line 380)**

Replace:
```python
        table = result.pipeline.compiled_nodes["adder"].as_table()
```
With:
```python
        table = result.compiled_nodes["adder"].as_table()
```

- [ ] **Step 4: Update `test_run_partial_execution_skips_unbound_subgraph` (line 399)**

Replace:
```python
        assert "spec_b" in result.unresolved_specs
```
With:
```python
        assert "spec_b" in result.unbound_sources
```

- [ ] **Step 5: Rewrite `test_run_is_non_mutating` (lines 401–414)**

Replace the entire method:
```python
    def test_run_returns_self_and_mutates_in_place(self, store):
        """run() returns self and sets _has_run / _run_id in place."""
        src_a, src_b = _make_two_sources()

        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b, label="joiner")

        result = job.run()
        assert result is job
        assert job._has_run is True
        assert job._run_id is not None
```

- [ ] **Step 6: Update `test_run_does_not_mutate_blueprint_nodes` docstring (lines 416–447)**

Replace the docstring only (keep the test body — it still verifies blueprint._nodes is untouched):
```python
    def test_run_does_not_mutate_blueprint_nodes(self, store):
        """run() must not replace blueprint _nodes with execution nodes.

        The pipeline blueprint is a shared, reusable object. Running a job must
        not replace blueprint template nodes with live exec nodes — that would
        break subsequent jobs (and ``PipelineJob.from_pipeline()`` callers) that
        share the same pipeline.
        """
```

- [ ] **Step 7: Update `test_run_twice_is_safe` (line 474)**

Replace:
```python
        table = result.pipeline.compiled_nodes["adder"].as_table()
```
With:
```python
        table = result.compiled_nodes["adder"].as_table()
```

- [ ] **Step 8: Update `test_load_after_partial_run_restores_unresolved_specs` (lines 663–679)**

Replace the entire method:
```python
    def test_load_after_partial_run_preserves_unbound_sources(self, store, tmp_path):
        """Loaded job reports unbound sources for slots not yet bound."""
        src_a, src_b = _make_two_sources()
        tag_b, data_b = src_b.output_schema()
        node_b = SourceNode(name="unbound_b", tag_schema=tag_b, data_schema=data_b)
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)
        job = PipelineJob(store=store)
        with job:
            joined = Join()(src_a, node_b)
            pod(joined, label="adder")
        result = job.run()
        assert "unbound_b" in result.unbound_sources
        path = tmp_path / "partial.json"
        result.save(str(path))
        loaded = PipelineJob.load(str(path), store=store)
        assert "unbound_b" in loaded.unbound_sources
```

- [ ] **Step 9: Update `test_end_to_end_*` tests in `TestPipelineJobEndToEnd` — scan for `pipeline.compiled_nodes`**

Search for any remaining `result.pipeline.compiled_nodes` or `.unresolved_specs` in the test file and replace:

```bash
grep -n "pipeline\.compiled_nodes\|\.unresolved_specs\|unbound_source_nodes" tests/test_pipeline/test_pipeline_job.py
```

Fix any remaining occurrences using the same pattern:
- `result.pipeline.compiled_nodes["x"]` → `result.compiled_nodes["x"]`
- `result.unresolved_specs` → `result.unbound_sources`
- `job.unbound_source_nodes()` → `job.unbound_sources`

- [ ] **Step 10: Update `test_serialization.py` (line 164)**

Replace:
```python
        assert completed.unresolved_specs == []
```
With:
```python
        assert completed.unbound_sources == []
```

- [ ] **Step 11: Run all migrated tests to see what passes and what still needs `run()` rewrite**

```bash
uv run pytest tests/test_pipeline/test_pipeline_job.py tests/test_pipeline/test_serialization.py -v 2>&1 | tail -30
```

Expected: completeness tests pass; `TestPipelineJobRun` tests may fail (they still call `run()` which still returns a new object — those will be fixed in Task 8).

- [ ] **Step 12: Commit migrated tests**

```bash
git add tests/test_pipeline/test_pipeline_job.py tests/test_pipeline/test_serialization.py
git commit -m "test(pipeline): migrate tests to unbound_sources; update run() return-value expectations"
```

---

## Task 7: Update orchestrators to accept `GraphProtocol`

**Files:**
- Modify: `src/orcapod/pipeline/sync_orchestrator.py`
- Modify: `src/orcapod/pipeline/async_orchestrator.py`

- [ ] **Step 1: Update `sync_orchestrator.py`**

**Import section** — replace the `TYPE_CHECKING` block:
```python
if TYPE_CHECKING:
    from orcapod.pipeline.dag import GraphProtocol
    from orcapod.protocols.observability_protocols import ExecutionObserverProtocol
    from orcapod.protocols.core_protocols import DataProtocol, TagProtocol
```

(Remove `import networkx as nx` from the `TYPE_CHECKING` block.)

**`run()` signature** (line 48) — replace:
```python
    def run(
        self,
        graph: "nx.DiGraph",
```
With:
```python
    def run(
        self,
        graph: "GraphProtocol[Any]",
```

**Inside `run()` body** (line 73–80) — remove the local `import networkx as nx` and replace:
```python
        import networkx as nx

        run_id = run_id or str(uuid.uuid4())
        ...
        topo_order = list(nx.topological_sort(graph))
```
With:
```python
        run_id = run_id or str(uuid.uuid4())
        ...
        topo_order = list(graph.topological_sort())
```

**`run()` docstring** — update `graph` argument description:
```python
            graph: A ``GraphProtocol`` DAG with GraphNode objects as vertices.
```

**`_gather_upstream()` signature** (line 122) — replace `graph: nx.DiGraph` with `graph: "GraphProtocol[Any]"`:
```python
    def _gather_upstream(
        node: Any, graph: "GraphProtocol[Any]", buffers: dict[Any, list[tuple[Any, Any]]]
    ) -> list[tuple[Any, Any]]:
```

**`_gather_upstream_multi()` signature** (line 133) — same replacement:
```python
    def _gather_upstream_multi(
        node: Any, graph: "GraphProtocol[Any]", buffers: dict[Any, list[tuple[Any, Any]]]
    ) -> list[tuple[list[tuple[Any, Any]], Any]]:
```

**`_gc_buffers()` signature** (line 217) — same replacement:
```python
    def _gc_buffers(
        current_node: Any,
        graph: "GraphProtocol[Any]",
        buffers: dict[Any, list[tuple[Any, Any]]],
        processed: set[Any],
    ) -> None:
```

- [ ] **Step 2: Update `async_orchestrator.py`**

**Import section** — replace `import networkx as nx` in the `TYPE_CHECKING` block with:
```python
if TYPE_CHECKING:
    from orcapod.pipeline.dag import GraphProtocol
    ...
```

**`_run_async()` signature** (find `graph: nx.DiGraph` in the async method) — replace with `graph: "GraphProtocol[Any]"`.

**Inside `_run_async()` body** (line 134–141) — remove `import networkx as nx` and replace:
```python
        import networkx as nx

        run_id = run_id or str(uuid.uuid4())
        ...
        topo_order = list(nx.topological_sort(graph))
```
With:
```python
        run_id = run_id or str(uuid.uuid4())
        ...
        topo_order = list(graph.topological_sort())
```

**`graph.edges()` call** (line 147) — no change needed (already matches `GraphProtocol`):
```python
        for upstream_node, downstream_node in graph.edges():
```

- [ ] **Step 3: Run existing pipeline tests (they still pass since `run()` still passes nx.DiGraph)**

```bash
uv run pytest tests/test_pipeline/ -v --ignore=tests/test_pipeline/test_pipeline_job.py -v 2>&1 | tail -20
```

Expected: all pass (orchestrator tests not directly testing the graph type).

- [ ] **Step 4: Commit**

```bash
git add src/orcapod/pipeline/sync_orchestrator.py src/orcapod/pipeline/async_orchestrator.py
git commit -m "refactor(pipeline): orchestrators accept GraphProtocol instead of nx.DiGraph"
```

---

## Task 8: Rewrite `run()`, remove `_build_execution_graph()` and `build_execution_graph()`

**Files:**
- Modify: `src/orcapod/pipeline/job.py`

This is the core task. After this, all migrated tests should pass.

- [ ] **Step 1: Add `OrcaDAG` import to `job.py`**

At the top of `src/orcapod/pipeline/job.py`, in the `TYPE_CHECKING` block (or directly if not already lazy), add:

```python
from orcapod.pipeline.dag import OrcaDAG
```

Place this with the other non-conditional imports (it's a lightweight stdlib-only module — no lazy-loading needed).

- [ ] **Step 2: Delete `build_execution_graph()` (lines 665–679)**

Remove the entire public method:
```python
    def build_execution_graph(
        self,
    ) -> "tuple[Any, list[str], Pipeline]":
        ...
        return self._build_execution_graph()
```

- [ ] **Step 3: Delete `_build_execution_graph()` (lines 681–839)**

Remove the entire private method (lines 681–839). This is ~160 lines.

- [ ] **Step 4: Rewrite `run()` (lines 845–916)**

Replace the entire `run()` method with:

```python
    def run(
        self,
        observer: "ExecutionObserverProtocol | None" = None,
    ) -> "PipelineJob":
        """Execute the resolvable subgraph of this job in place.

        Nodes whose upstream includes an unbound source (and all their
        dependents) are excluded from execution. Partial execution is a
        first-class outcome — ``unbound_sources`` reports which source
        slots were excluded.

        After a successful run, ``job.compiled_nodes`` returns the
        execution-ready nodes with populated database caches.

        Args:
            observer: Optional execution observer.

        Returns:
            ``self`` — the same ``PipelineJob`` instance, with
            ``_has_run`` and ``_run_id`` set.

        Raises:
            ValueError: If no store is set.
            RuntimeError: If no pipeline has been compiled.
        """
        import hashlib
        import uuid

        from orcapod.core.nodes.source_node import SourceJobNode
        from orcapod.pipeline.observer import NoOpObserver
        from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

        if not self._compiled:
            raise RuntimeError(
                "PipelineJob has no compiled pipeline. "
                "Record invocations inside a 'with job:' block first."
            )
        if self._store is None:
            raise ValueError(
                "PipelineJob.run() requires a store. "
                "Call job.bind(store=db) before run()."
            )

        # --- Build hash-keyed OrcaDAG for topological ordering + exclusion ---
        hash_dag: OrcaDAG[str] = OrcaDAG()
        for u, v in self._graph_edges:
            hash_dag.add_edge(u, v)
        for node_hash in self._persistent_node_map:
            if node_hash not in hash_dag:
                hash_dag.add_node(node_hash)

        # Walk topologically; exclude unbound sources and all their dependents.
        excluded_hashes: set[str] = set()
        for node_hash in hash_dag.topological_sort():
            if node_hash not in self._persistent_node_map:
                continue
            node = self._persistent_node_map[node_hash]
            if isinstance(node, SourceJobNode) and node.bound_source is None:
                excluded_hashes.add(node_hash)
            elif any(p in excluded_hashes for p in hash_dag.predecessors(node_hash)):
                excluded_hashes.add(node_hash)

        # --- Build execution OrcaDAG from existing node objects (no cloning) ---
        exec_dag: OrcaDAG = OrcaDAG()
        for node_hash, node in self._persistent_node_map.items():
            if node_hash not in excluded_hashes:
                exec_dag.add_node(node)
        for u_hash, v_hash in self._graph_edges:
            if (
                u_hash not in excluded_hashes
                and v_hash not in excluded_hashes
                and u_hash in self._persistent_node_map
                and v_hash in self._persistent_node_map
            ):
                exec_dag.add_edge(
                    self._persistent_node_map[u_hash],
                    self._persistent_node_map[v_hash],
                )

        # --- Execute ---
        effective_observer = observer or NoOpObserver()
        run_id = uuid.uuid4().hex[:16]

        # Snapshot hash: SHA-256 of sink-node content hashes for the run URI.
        leaf_hashes = sorted(
            node.content_hash().to_string()
            for node in exec_dag.nodes()
            if not exec_dag.successors(node) and hasattr(node, "content_hash")
        )
        snapshot_hash = hashlib.sha256("\n".join(leaf_hashes).encode()).hexdigest()[:16]
        pipeline_uri = "/".join(self._name) + "@" + snapshot_hash

        SyncPipelineOrchestrator().run(
            exec_dag,
            observer=effective_observer,
            pipeline_uri=pipeline_uri,
        )

        # Flush databases.
        pipeline_db = self._store.at(*self._name)
        result_db = pipeline_db.at("_result")
        pipeline_db.flush()
        result_db.flush()

        # Mutate self in place.
        self._has_run = True
        self._run_id = run_id

        return self
```

- [ ] **Step 5: Run the full pipeline test suite**

```bash
uv run pytest tests/test_pipeline/ -v 2>&1 | tail -40
```

Expected: all pass. If any fail, read the error output carefully — the most likely issues are:
- A `result.pipeline.compiled_nodes` reference that was missed in Task 6 — fix it to `result.compiled_nodes`
- A `result.unresolved_specs` reference that was missed — fix it to `result.unbound_sources`
- A `result is not job` assertion — fix it to `result is job`

- [ ] **Step 6: Run the broader test suite to check for regressions**

```bash
uv run pytest tests/ -v 2>&1 | tail -50
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add src/orcapod/pipeline/job.py
git commit -m "refactor(pipeline): remove copy-on-run; inline exec graph in run(); drop _build_execution_graph()"
```

---

## Task 9: Final verification and push

- [ ] **Step 1: Full clean test run**

```bash
uv run pytest tests/ -v 2>&1 | tail -20
```

Expected: all pass, no warnings about deprecated APIs.

- [ ] **Step 2: Check DESIGN_ISSUES.md**

```bash
grep -i "build_execution_graph\|unresolved_spec\|copy.on.run\|exec_pipeline" DESIGN_ISSUES.md
```

If any matching open issues exist, update their status to `resolved` and add a `Fix:` note referencing this PR.

- [ ] **Step 3: Push branch**

```bash
git push -u origin eywalker/eng-517-review-cleanup-of-pipelinejob-execution-graph-remove
```

- [ ] **Step 4: Create PR**

```bash
gh pr create \
  --title "refactor(pipeline): remove copy-on-run pattern; adopt GraphProtocol; consolidate unbound_sources" \
  --base main \
  --body "$(cat <<'EOF'
## Summary

- Removes the copy-on-run pattern from `PipelineJob.run()`: no more cloning of nodes, no `exec_pipeline`, no fresh `FunctionJobNode`/`OperatorJobNode` instances per run.
- `run()` now mutates `self` in place and returns `self`; databases were already wired at `bind(store=...)` time.
- `_build_execution_graph()` and `build_execution_graph()` deleted; execution graph construction inlined into `run()` as an `OrcaDAG[JobNode]`.
- Orchestrators updated to accept `GraphProtocol` instead of `nx.DiGraph`; three call-site substitutions cover all usage.
- `GraphBackend` renamed to `GraphProtocol` in `dag.py`/`networkx_backend.py`; `ancestors()` added to both.
- `unbound_source_nodes()` and `_unresolved_specs`/`unresolved_specs` removed; single live-computed `unbound_sources` property replaces both.
- `spec_names` local variable renamed to `source_names` in `bind()` and `from_pipeline()`.

Closes ENG-517

## Test plan

- [ ] `uv run pytest tests/test_pipeline/test_dag.py` — new `ancestors()` tests pass
- [ ] `uv run pytest tests/test_pipeline/test_networkx_backend.py` — `GraphProtocol` conformance + `ancestors()` pass
- [ ] `uv run pytest tests/test_pipeline/test_pipeline_job.py` — all run/completeness/load tests pass with new API
- [ ] `uv run pytest tests/test_pipeline/test_serialization.py` — `unbound_sources` key in saved JSON
- [ ] `uv run pytest tests/` — full suite green, no regressions

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review Notes

- **Spec coverage:** All 8 goals from the spec are addressed: `run()` mutates in place (Task 8), `_build_execution_graph()`/`build_execution_graph()` removed (Task 8), `exec_pipeline` gone (Task 8), orchestrators accept `GraphProtocol` (Task 7), `GraphProtocol` rename + `ancestors()` added (Tasks 2–3), `unbound_sources` replaces both old APIs (Tasks 4–5), `spec_names` → `source_names` (Task 5), `"unresolved_specs"` key → `"unbound_sources"` (Task 5), no regressions (Task 9).
- **No placeholders:** All steps include exact code.
- **Type consistency:** `GraphProtocol` used throughout Tasks 2–7; `OrcaDAG` introduced in Task 8; `unbound_sources` introduced in Task 4 and referenced consistently in Tasks 5–6.
- **`predecessors()` return type:** `OrcaDAG.predecessors()` returns `frozenset[NodeT]`. The `any(p in excluded_hashes ...)` call in Task 8 iterates a `frozenset` — correct.
- **`_graph_edges` availability:** Confirmed on `AbstractPipelineBase` (line 98 of `base.py`) — populated by `compile()`. Accessible as `self._graph_edges` in `run()`.
- **Backward compat:** No shims added per CLAUDE.md (`pre-v0.1.0 greenfield`). `load()` simply drops the `_unresolved_specs` restoration.
