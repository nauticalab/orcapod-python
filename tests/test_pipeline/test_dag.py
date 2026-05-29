"""Tests for OrcaDAG — the lean in-house DAG replacing networkx.DiGraph.

Covers all nine API shapes used by Orcapod:
- add_node / add_edge (construction)
- node_attrs (attribute dict access)
- nodes() / edges() (traversal)
- in_degree() / successors() (local graph queries)
- topological_sort() / topological_sort_deterministic() (ordering)
- __contains__ / __len__ / __iter__ (membership and sizing)
"""

from __future__ import annotations

import pytest
from graphlib import CycleError

from orcapod.pipeline.dag import OrcaDAG


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestAddNode:
    def test_adds_node(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_node("a")
        assert "a" in dag

    def test_add_node_idempotent(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_node("a")
        dag.add_node("a")  # second call is a no-op
        assert len(dag) == 1

    def test_add_node_with_attrs(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_node("a", label="source", node_type="source")
        assert dag.node_attrs("a")["label"] == "source"
        assert dag.node_attrs("a")["node_type"] == "source"

    def test_subsequent_add_node_does_not_clear_existing_attrs(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_node("a", label="original")
        dag.add_node("a")  # no attrs — must not clear existing
        assert dag.node_attrs("a")["label"] == "original"

    def test_subsequent_add_node_merges_new_attrs(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_node("a", label="original")
        dag.add_node("a", node_type="source")
        assert dag.node_attrs("a")["label"] == "original"
        assert dag.node_attrs("a")["node_type"] == "source"

    def test_subsequent_add_node_overwrites_same_key(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_node("a", label="original")
        dag.add_node("a", label="updated")  # same key, different value
        assert dag.node_attrs("a")["label"] == "updated"


class TestAddEdge:
    def test_adds_both_nodes_implicitly(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_edge("a", "b")
        assert "a" in dag
        assert "b" in dag

    def test_adds_edge(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_edge("a", "b")
        assert ("a", "b") in list(dag.edges())

    def test_duplicate_edge_is_idempotent(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_edge("a", "b")
        dag.add_edge("a", "b")
        assert list(dag.edges()).count(("a", "b")) == 1

    def test_multiple_edges_from_same_source(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_edge("a", "b")
        dag.add_edge("a", "c")
        edges = set(dag.edges())
        assert ("a", "b") in edges
        assert ("a", "c") in edges


# ---------------------------------------------------------------------------
# Node attribute access
# ---------------------------------------------------------------------------


class TestNodeAttrs:
    def test_returns_mutable_dict(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_node("a")
        attrs = dag.node_attrs("a")
        attrs["x"] = 42
        assert dag.node_attrs("a")["x"] == 42

    def test_dict_access_pattern(self) -> None:
        """Mirrors nx.DiGraph.nodes[key] access pattern used in graph.py."""
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_node("a")
        dag.node_attrs("a")["node_type"] = "source"
        assert dag.node_attrs("a").get("node_type") == "source"
        assert dag.node_attrs("a").get("missing") is None

    def test_raises_key_error_for_missing_node(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        with pytest.raises(KeyError):
            dag.node_attrs("nonexistent")


# ---------------------------------------------------------------------------
# Membership and sizing
# ---------------------------------------------------------------------------


class TestMembership:
    def test_contains_after_add_node(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_node("a")
        assert "a" in dag
        assert "b" not in dag

    def test_not_in_empty_dag(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        assert "a" not in dag

    def test_len_empty(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        assert len(dag) == 0

    def test_len_after_add_node(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_node("a")
        dag.add_node("b")
        assert len(dag) == 2

    def test_len_edge_does_not_double_count(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_edge("a", "b")  # implicitly adds 2 nodes
        assert len(dag) == 2

    def test_bool_empty_is_false(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        assert not dag

    def test_bool_nonempty_is_true(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_node("a")
        assert dag

    def test_iter_yields_nodes(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_node("a")
        dag.add_node("b")
        assert set(dag) == {"a", "b"}

    def test_in_degree_comprehension_pattern(self) -> None:
        """Mirrors the dict-comprehension pattern in graph.py line 560."""
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_edge("a", "b")
        dag.add_edge("a", "c")
        in_degrees = {n: dag.in_degree(n) for n in dag}
        assert in_degrees == {"a": 0, "b": 1, "c": 1}


# ---------------------------------------------------------------------------
# Traversal
# ---------------------------------------------------------------------------


class TestNodes:
    def test_returns_all_nodes(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_node("a")
        dag.add_node("b")
        assert set(dag.nodes()) == {"a", "b"}

    def test_empty_dag(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        assert list(dag.nodes()) == []


class TestEdges:
    def test_returns_all_edges(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_edge("a", "b")
        dag.add_edge("b", "c")
        assert set(dag.edges()) == {("a", "b"), ("b", "c")}

    def test_empty_dag(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        assert list(dag.edges()) == []

    def test_isolated_node_has_no_edges(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_node("a")
        assert list(dag.edges()) == []

    def test_sorted_edges_pattern(self) -> None:
        """Mirrors sorted(g.edges()) used in graph.py line 576."""
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_edge("b", "c")
        dag.add_edge("a", "b")
        assert sorted(dag.edges()) == [("a", "b"), ("b", "c")]


class TestSuccessors:
    def test_returns_direct_successors(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_edge("a", "b")
        dag.add_edge("a", "c")
        assert set(dag.successors("a")) == {"b", "c"}

    def test_leaf_node_has_no_successors(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_edge("a", "b")
        assert set(dag.successors("b")) == set()

    def test_raises_key_error_for_missing_node(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        with pytest.raises(KeyError):
            list(dag.successors("nonexistent"))


class TestPredecessors:
    def test_returns_direct_predecessors(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_edge("a", "c")
        dag.add_edge("b", "c")
        assert dag.predecessors("c") == {"a", "b"}

    def test_root_node_has_no_predecessors(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_edge("a", "b")
        assert dag.predecessors("a") == frozenset()

    def test_returns_frozenset_snapshot(self) -> None:
        """Returned frozenset must not expose internal state to mutation."""
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_edge("a", "b")
        snap = dag.predecessors("b")
        assert isinstance(snap, frozenset)

    def test_raises_key_error_for_missing_node(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        with pytest.raises(KeyError):
            dag.predecessors("nonexistent")


class TestInDegree:
    def test_source_node_has_in_degree_zero(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_edge("a", "b")
        assert dag.in_degree("a") == 0

    def test_single_incoming_edge(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_edge("a", "b")
        assert dag.in_degree("b") == 1

    def test_multiple_incoming_edges(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_edge("a", "c")
        dag.add_edge("b", "c")
        assert dag.in_degree("c") == 2

    def test_raises_key_error_for_missing_node(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        with pytest.raises(KeyError):
            dag.in_degree("nonexistent")


# ---------------------------------------------------------------------------
# Topological sort
# ---------------------------------------------------------------------------


class TestTopologicalSort:
    def _is_valid_topo_order(self, dag: OrcaDAG[str], order: list[str]) -> bool:
        """Return True if *order* is a valid topological ordering of *dag*."""
        position = {node: idx for idx, node in enumerate(order)}
        for u, v in dag.edges():
            if position[u] >= position[v]:
                return False
        return True

    def test_linear_chain(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_edge("a", "b")
        dag.add_edge("b", "c")
        order = dag.topological_sort()
        assert self._is_valid_topo_order(dag, order)
        assert set(order) == {"a", "b", "c"}

    def test_diamond_dag(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_edge("a", "b")
        dag.add_edge("a", "c")
        dag.add_edge("b", "d")
        dag.add_edge("c", "d")
        order = dag.topological_sort()
        assert self._is_valid_topo_order(dag, order)
        assert set(order) == {"a", "b", "c", "d"}

    def test_isolated_nodes_included(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_node("isolated")
        dag.add_edge("a", "b")
        order = dag.topological_sort()
        assert set(order) == {"isolated", "a", "b"}

    def test_single_node(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_node("only")
        assert dag.topological_sort() == ["only"]

    def test_empty_dag(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        assert dag.topological_sort() == []

    def test_raises_cycle_error(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_edge("a", "b")
        dag.add_edge("b", "c")
        dag.add_edge("c", "a")  # cycle
        with pytest.raises(CycleError):
            dag.topological_sort()


class TestTopologicalSortDeterministic:
    def _is_valid_topo_order(self, dag: OrcaDAG[str], order: list[str]) -> bool:
        position = {node: idx for idx, node in enumerate(order)}
        for u, v in dag.edges():
            if position[u] >= position[v]:
                return False
        return True

    def test_linear_chain(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_edge("a", "b")
        dag.add_edge("b", "c")
        order = dag.topological_sort_deterministic()
        assert order == ["a", "b", "c"]

    def test_diamond_dag_deterministic(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_edge("a", "b")
        dag.add_edge("a", "c")
        dag.add_edge("b", "d")
        dag.add_edge("c", "d")
        order = dag.topological_sort_deterministic()
        assert self._is_valid_topo_order(dag, order)
        # With Kahn's + min-heap: "b" < "c" so b before c
        assert order.index("b") < order.index("c")

    def test_stable_across_repeated_calls(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_edge("z", "m")
        dag.add_edge("z", "a")
        dag.add_edge("m", "b")
        dag.add_edge("a", "b")
        first = dag.topological_sort_deterministic()
        second = dag.topological_sort_deterministic()
        assert first == second

    def test_insertion_order_independent(self) -> None:
        """Same graph, different insertion order → same deterministic output."""
        dag1: OrcaDAG[str] = OrcaDAG()
        dag1.add_edge("a", "c")
        dag1.add_edge("b", "c")

        dag2: OrcaDAG[str] = OrcaDAG()
        dag2.add_edge("b", "c")
        dag2.add_edge("a", "c")

        assert dag1.topological_sort_deterministic() == (
            dag2.topological_sort_deterministic()
        )

    def test_raises_cycle_error(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_edge("a", "b")
        dag.add_edge("b", "a")
        with pytest.raises(CycleError):
            dag.topological_sort_deterministic()

    def test_empty_dag(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        assert dag.topological_sort_deterministic() == []


# ---------------------------------------------------------------------------
# Generic type support
# ---------------------------------------------------------------------------


class TestGenericNodeTypes:
    def test_string_nodes(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        dag.add_edge("hash_a", "hash_b")
        assert "hash_a" in dag

    def test_object_nodes(self) -> None:
        """OrcaPod uses GraphNode objects as nodes in the dag."""

        class FakeNode:
            def __init__(self, name: str) -> None:
                self.name = name

            def __lt__(self, other: object) -> bool:
                assert isinstance(other, FakeNode)
                return self.name < other.name

        a, b = FakeNode("a"), FakeNode("b")
        dag: OrcaDAG[FakeNode] = OrcaDAG()
        dag.add_edge(a, b)
        assert a in dag
        assert b in dag
        assert dag.successors(a) == {b}

    def test_integer_nodes(self) -> None:
        dag: OrcaDAG[int] = OrcaDAG()
        dag.add_edge(1, 2)
        dag.add_edge(2, 3)
        order = dag.topological_sort()
        assert set(order) == {1, 2, 3}


# ---------------------------------------------------------------------------
# ancestors()
# ---------------------------------------------------------------------------


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

    def test_ancestors_raises_for_unknown_node(self):
        dag = OrcaDAG()
        dag.add_node("a")
        with pytest.raises(KeyError):
            dag.ancestors("z")


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

        src = ArrowTableSource(
            pa.table({"id": pa.array(["a"], type=pa.large_string()), "v": pa.array([1], type=pa.int64())}),
            tag_columns=["id"],
            source_id="src",
            infer_nullable=True,
        )

        def double_v(v: int) -> int:
            return v * 2

        fn = PythonDataFunction(
            function=double_v,
            output_keys="out",
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


# ---------------------------------------------------------------------------
# PipelineProtocol structural conformance
# ---------------------------------------------------------------------------


class TestPipelineProtocolConformance:
    """Pipeline and PipelineJob satisfy PipelineProtocol structurally."""

    def test_pipeline_satisfies_protocol(self):
        """Pipeline is runtime-checkable as PipelineProtocol."""
        from orcapod.pipeline.graph import Pipeline
        from orcapod.protocols.pipeline_protocols import PipelineProtocol

        pipeline = Pipeline(name="proto_test")
        with pipeline:
            pass  # no ops, just compile

        assert isinstance(pipeline, PipelineProtocol)

    def test_pipeline_job_satisfies_protocol(self):
        """PipelineJob is runtime-checkable as PipelineProtocol."""
        from orcapod.databases import InMemoryArrowDatabase
        from orcapod.pipeline.job import PipelineJob
        from orcapod.protocols.pipeline_protocols import PipelineProtocol

        job = PipelineJob(store=InMemoryArrowDatabase())
        with job:
            pass

        assert isinstance(job, PipelineProtocol)

    def test_protocol_dag_returns_graph_protocol(self):
        """dag attribute on PipelineProtocol returns GraphProtocol."""
        from orcapod.pipeline.graph import Pipeline
        from orcapod.pipeline.dag import GraphProtocol

        pipeline = Pipeline(name="proto_dag_test")
        with pipeline:
            pass

        assert isinstance(pipeline.dag, GraphProtocol)


# ---------------------------------------------------------------------------
# PipelineJob.load() dag property returns OrcaDAG[JobNode]
# ---------------------------------------------------------------------------


class TestPipelineJobDagOnLoadedJob:
    """job.dag returns OrcaDAG[JobNode] even for loaded (not compiled) jobs."""

    def test_loaded_job_dag_returns_job_nodes(self, tmp_path):
        """PipelineJob.load() must produce OrcaDAG with JobNode objects."""
        import pyarrow as pa
        from orcapod.core.data_function import PythonDataFunction
        from orcapod.core.function_pod import FunctionPod
        from orcapod.core.nodes import JobNode
        from orcapod.core.sources.arrow_table_source import ArrowTableSource
        from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
        from orcapod.pipeline.graph import Pipeline
        from orcapod.pipeline.job import PipelineJob

        src = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array(["a"], type=pa.large_string()),
                    "v": pa.array([1], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            source_id="src",
            infer_nullable=True,
        )
        def double_v(v: int) -> int:
            return v * 2

        fn = PythonDataFunction(double_v, output_keys="out")
        pod = FunctionPod(fn)

        # Build, run, and save a pipeline job
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
        # Topology should match the original: 2 nodes, 1 edge (source → function)
        assert len(list(loaded_job.dag.nodes())) == 2
        assert len(list(loaded_job.dag.edges())) == 1
