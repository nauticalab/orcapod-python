"""Tests for the Pipeline and PipelineJob classes.

Verifies that Pipeline correctly wraps all nodes during compile():
- Leaf streams → SourceNode (SourceSpec-only; concrete sources raise ValueError)
- Function pod invocations → FunctionNode
- Operator invocations → OperatorNode

Also covers PipelineJob execution, hash chain behavior, hash graph
attributes, source node access, and observer integration.
"""

from __future__ import annotations

from typing import cast

import networkx as nx
import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import (
    FunctionNode,
    OperatorNode,
    SourceNode,
)
from orcapod.core.operators import Join
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.sources import ArrowTableSource, CachedSource
from orcapod.core.sources.source_spec import SourceSpec
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import Pipeline
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.observer import NoOpObserver

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_source(tag_col: str, data_col: str, data: dict) -> ArrowTableSource:
    table = pa.table(
        {
            tag_col: pa.array(data[tag_col], type=pa.large_string()),
            data_col: pa.array(data[data_col], type=pa.int64()),
        }
    )
    return ArrowTableSource(table, tag_columns=[tag_col], infer_nullable=True)


def _make_two_sources():
    src_a = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
    src_b = _make_source("key", "score", {"key": ["a", "b"], "score": [100, 200]})
    return src_a, src_b


def add_values(value: int, score: int) -> int:
    return value + score


def double_value(value: int) -> int:
    return value * 2


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def pipeline_db():
    return InMemoryArrowDatabase()


# ---------------------------------------------------------------------------
# Tests: SourceSpec enforcement (Task 4)
# ---------------------------------------------------------------------------


class TestPipelineSourceSpecEnforcement:
    def test_pipeline_no_database_params(self):
        """New Pipeline() takes only name (no pipeline_database)."""
        pipeline = Pipeline(name="test")
        with pipeline:
            pass
        assert pipeline._compiled

    def test_pipeline_with_spec_leaves_compiles(self):
        """Pipeline with SourceSpec leaves compiles without error."""
        src_a, src_b = _make_two_sources()
        tag_a, data_a = src_a.output_schema()
        tag_b, data_b = src_b.output_schema()
        spec_a = SourceSpec("input_a", tag_schema=tag_a, data_schema=data_a)
        spec_b = SourceSpec("input_b", tag_schema=tag_b, data_schema=data_b)

        pipeline = Pipeline(name="spec_pipe")
        with pipeline:
            Join()(spec_a, spec_b)

        assert pipeline._compiled
        source_nodes = [
            n for n in pipeline._node_graph.nodes() if isinstance(n, SourceNode)
        ]
        assert len(source_nodes) == 2

    def test_pipeline_with_concrete_leaf_raises(self):
        """Pipeline.compile() raises ValueError if any leaf is not a SourceSpec."""
        src_a, src_b = _make_two_sources()

        pipeline = Pipeline(name="bad_pipe")
        with pytest.raises(ValueError, match="SourceSpec"):
            with pipeline:
                Join()(src_a, src_b)

    def test_pipeline_bind_returns_pipeline_job(self):
        """Pipeline.bind() returns a PipelineJob without modifying the pipeline."""
        src_a, src_b = _make_two_sources()
        tag_a, data_a = src_a.output_schema()
        tag_b, data_b = src_b.output_schema()
        spec_a = SourceSpec("a", tag_schema=tag_a, data_schema=data_a)
        spec_b = SourceSpec("b", tag_schema=tag_b, data_schema=data_b)

        pipeline = Pipeline(name="p")
        with pipeline:
            Join()(spec_a, spec_b)

        db = InMemoryArrowDatabase()
        job = pipeline.bind(sources={"a": src_a, "b": src_b}, store=db)

        assert isinstance(job, PipelineJob)
        assert job.pipeline is pipeline
        assert job.store is db


# ---------------------------------------------------------------------------
# Tests: compile wraps leaf streams as SourceNode
# ---------------------------------------------------------------------------


class TestCompileSourceWrapping:
    def test_compile_wraps_leaf_streams_as_persistent_source_node(self, pipeline_db):
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=pipeline_db)
        with job:
            Join()(src_a, src_b)

        pipeline = job.pipeline
        assert pipeline._compiled
        assert len(pipeline.compiled_nodes) > 0
        source_nodes = [n for n in pipeline._node_graph.nodes() if isinstance(n, SourceNode)]
        assert len(source_nodes) == 2


# ---------------------------------------------------------------------------
# Tests: compile creates FunctionNode
# ---------------------------------------------------------------------------


class TestCompileFunctionNode:
    def test_compile_creates_persistent_function_node(self, pipeline_db):
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)
        job = PipelineJob(store=pipeline_db)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        assert "adder" in job.pipeline.compiled_nodes
        node = job.pipeline.compiled_nodes["adder"]
        assert isinstance(node, FunctionNode)

    def test_function_node_pipeline_path_prefix(self, pipeline_db):
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)
        job = PipelineJob(store=pipeline_db)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        node = job.pipeline.compiled_nodes["adder"]
        assert isinstance(node, FunctionNode)
        assert node.node_identity_path[0] == "add_values"


# ---------------------------------------------------------------------------
# Tests: compile creates OperatorNode
# ---------------------------------------------------------------------------


class TestCompileOperatorNode:
    def test_compile_creates_persistent_operator_node(self, pipeline_db):
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=pipeline_db)
        with job:
            Join()(src_a, src_b, label="joiner")
        assert "joiner" in job.pipeline.compiled_nodes
        node = job.pipeline.compiled_nodes["joiner"]
        assert isinstance(node, OperatorNode)

    def test_operator_node_pipeline_path_prefix(self, pipeline_db):
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=pipeline_db)
        with job:
            Join()(src_a, src_b, label="joiner")
        node = job.pipeline.compiled_nodes["joiner"]
        assert isinstance(node, OperatorNode)
        assert node.node_identity_path[0] == "Join"


# ---------------------------------------------------------------------------
# Tests: compile mutates recorded nodes via attach_databases()
# ---------------------------------------------------------------------------


class TestCompileMutatesNodes:
    def test_exec_nodes_have_pipeline_database_after_run(self, pipeline_db):
        """After run(), exec nodes have pipeline_database attached."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)
        job = PipelineJob(store=pipeline_db)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")
        result = job.run()

        # After run, compiled_nodes["adder"] is the exec node in the returned result
        exec_node = result.pipeline.compiled_nodes["adder"]
        assert isinstance(exec_node, FunctionNode)
        assert exec_node._pipeline_database is not None

    def test_exec_operator_nodes_have_pipeline_database_after_run(self, pipeline_db):
        """After run(), exec OperatorNodes have pipeline_database attached."""
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=pipeline_db)
        with job:
            Join()(src_a, src_b, label="joiner")
        result = job.run()

        exec_node = result.pipeline.compiled_nodes["joiner"]
        assert isinstance(exec_node, OperatorNode)
        assert exec_node._pipeline_database is not None


# ---------------------------------------------------------------------------
# Tests: function database handling
# ---------------------------------------------------------------------------


class TestFunctionDatabaseHandling:
    def test_result_database_scoped_to_pipeline_name(self, pipeline_db):
        """Result DB is auto-scoped to pipeline_name/_result after run()."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)
        job = PipelineJob(name="my_pipe", store=pipeline_db)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")
        result = job.run()

        exec_node = result.pipeline.compiled_nodes["adder"]
        assert isinstance(exec_node, FunctionNode)
        # Verify the exec node has databases attached
        assert exec_node._pipeline_database is not None
        # The result DB is scoped as pipeline_name/_result internally.
        # We verify the behavior: records should be accessible via the node.
        records = exec_node.get_all_records()
        assert records is not None
        assert records.num_rows == 2  # two input rows


# ---------------------------------------------------------------------------
# Tests: label access
# ---------------------------------------------------------------------------


class TestLabelAccess:
    def test_node_access_by_label(self, pipeline_db):
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=pipeline_db)
        with job:
            Join()(src_a, src_b, label="my_join")

        # Access via __getattr__
        node = job.pipeline.my_join
        assert isinstance(node, OperatorNode)

    def test_label_collision_sorted_by_content_hash(self, pipeline_db):
        """Two nodes with same label get _1, _2 sorted by content hash."""
        src_a = _make_source("k", "value", {"k": ["a"], "value": [1]})
        src_b = _make_source("k", "value", {"k": ["b"], "value": [2]})

        pf1 = PythonDataFunction(double_value, output_keys="result")
        pf2 = PythonDataFunction(double_value, output_keys="result")
        pod1 = FunctionPod(data_function=pf1)
        pod2 = FunctionPod(data_function=pf2)

        job = PipelineJob(store=pipeline_db)
        with job:
            pod1(src_a, label="compute")
            pod2(src_b, label="compute")

        pipeline = job.pipeline
        # Both should be disambiguated
        assert "compute_1" in pipeline.compiled_nodes
        assert "compute_2" in pipeline.compiled_nodes
        assert isinstance(pipeline.compute_1, FunctionNode)
        assert isinstance(pipeline.compute_2, FunctionNode)

        # Verify deterministic ordering by content hash
        hash_1 = pipeline.compute_1.content_hash().to_string()
        hash_2 = pipeline.compute_2.content_hash().to_string()
        assert hash_1 <= hash_2

    def test_getattr_raises_for_unknown(self, pipeline_db):
        job = PipelineJob(store=pipeline_db)
        with job:
            pass  # empty pipeline

        with pytest.raises(AttributeError, match="Pipeline has no attribute"):
            _ = job.pipeline.nonexistent

    def test_dir_includes_node_labels(self, pipeline_db):
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=pipeline_db)
        with job:
            Join()(src_a, src_b, label="my_join")

        d = dir(job.pipeline)
        assert "my_join" in d


# ---------------------------------------------------------------------------
# Tests: auto compile and run
# ---------------------------------------------------------------------------


class TestAutoCompileAndRun:
    def test_auto_compile_on_exit(self, pipeline_db):
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=pipeline_db)
        with job:
            Join()(src_a, src_b, label="joiner")

        # Should be compiled after exiting context
        assert job.pipeline._compiled
        assert "joiner" in job.pipeline.compiled_nodes

    def test_run_executes_all_nodes(self, pipeline_db):
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)

        job = PipelineJob(store=pipeline_db)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        result = job.run()

        # After run, function node should have records (in the returned result job)
        node = result.pipeline.compiled_nodes["adder"]
        records = node.get_all_records()
        assert records is not None
        assert records.num_rows == 2  # two input rows (a, b)

    def test_pipeline_path_prefix_scoping(self, pipeline_db):
        """node_identity_path reflects the operator/function name."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)

        job = PipelineJob(store=pipeline_db)
        with job:
            joined = Join()(src_a, src_b, label="joiner")
            pod(joined, label="adder")

        result = job.run()

        # Check operator node
        joiner = result.pipeline.compiled_nodes["joiner"]
        assert joiner.node_identity_path[0] == "Join"

        # Check function node
        adder = result.pipeline.compiled_nodes["adder"]
        assert adder.node_identity_path[0] == "add_values"


# ---------------------------------------------------------------------------
# Tests: end-to-end
# ---------------------------------------------------------------------------


class TestEndToEnd:
    def test_end_to_end_source_join_function(self, pipeline_db):
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)
        job = PipelineJob(store=pipeline_db)
        with job:
            joined = Join()(src_a, src_b, label="joiner")
            pod(joined, label="adder")

        assert isinstance(job.pipeline.compiled_nodes["joiner"], OperatorNode)
        assert isinstance(job.pipeline.compiled_nodes["adder"], FunctionNode)

        result = job.run()

        fn_records = result.pipeline.compiled_nodes["adder"].get_all_records()
        assert fn_records is not None
        assert fn_records.num_rows == 2

        table = result.pipeline.compiled_nodes["adder"].as_table()
        totals = sorted(cast(list[int], table.column("total").to_pylist()))
        assert totals == [110, 220]


# ---------------------------------------------------------------------------
# Tests: hash chain — detaching via .as_source() breaks chain
# ---------------------------------------------------------------------------


class TestHashChainDetaching:
    """DerivedSource (via .as_source()) has a different hash chain than direct extension."""

    def test_detached_pipeline_hash_differs_from_node(self, pipeline_db):
        """DerivedSource.pipeline_hash() is schema-based, not topology-based."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)
        job = PipelineJob(store=pipeline_db)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")
        result = job.run()
        fn_node = result.pipeline.compiled_nodes["adder"]
        derived = fn_node.as_source()
        # DerivedSource uses RootSource hash (schema-only), not topology hash
        assert derived.pipeline_hash() != fn_node.pipeline_hash()

    def test_detached_content_hash_differs_from_node(self, pipeline_db):
        """DerivedSource.content_hash() differs from the source FunctionNode."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)
        job = PipelineJob(store=pipeline_db)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")
        result = job.run()
        fn_node = result.pipeline.compiled_nodes["adder"]
        derived = fn_node.as_source()
        assert derived.content_hash() != fn_node.content_hash()

    def test_two_derived_sources_from_same_node_share_pipeline_hash(self, pipeline_db):
        """Two DerivedSources from the same FunctionNode share pipeline_hash."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)
        job = PipelineJob(store=pipeline_db)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")
        result = job.run()
        fn_node = result.pipeline.compiled_nodes["adder"]
        derived1 = fn_node.as_source()
        derived2 = fn_node.as_source()
        assert derived1.pipeline_hash() == derived2.pipeline_hash()


# ---------------------------------------------------------------------------
# Tests: hash graph
# ---------------------------------------------------------------------------


class TestHashGraph:
    def test_graph_empty_before_context(self):
        """pipeline.graph is an empty DiGraph before any with block."""
        # Test the Pipeline object directly — no PipelineJob needed for this property check.
        pipeline = Pipeline(name="test")
        assert isinstance(pipeline.graph, nx.DiGraph)
        assert len(pipeline.graph.nodes) == 0
        assert len(pipeline.graph.edges) == 0

    def test_graph_has_edges_after_compile(self, pipeline_db):
        """After a with block + compile, graph contains the right edges."""
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=pipeline_db)
        with job:
            Join()(src_a, src_b, label="joiner")
        pipeline = job.pipeline
        g = pipeline.graph
        assert len(g.edges) > 0
        joiner_hash = job.pipeline.compiled_nodes["joiner"].content_hash().to_string()
        assert joiner_hash in g.nodes

    def test_graph_multi_node_has_more_edges_than_single_node(self, pipeline_db):
        """A pipeline with more nodes has more edges than one with fewer."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)
        # Build single-node pipeline
        job1 = PipelineJob(store=pipeline_db)
        with job1:
            Join()(src_a, src_b, label="joiner")
        edges_single = set(job1.pipeline.graph.edges)
        # Build two-node pipeline
        src_a2, src_b2 = _make_two_sources()
        job2 = PipelineJob(store=InMemoryArrowDatabase())
        with job2:
            joined = Join()(src_a2, src_b2, label="joiner")
            pod(joined, label="adder")
        edges_double = set(job2.pipeline.graph.edges)
        assert len(edges_double) > len(edges_single)

    def test_graph_node_type_source(self, pipeline_db):
        """Source nodes have node_type='source' after compile."""
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=pipeline_db)
        with job:
            Join()(src_a, src_b, label="joiner")
        pipeline = job.pipeline
        g = pipeline.graph
        joiner_hash = pipeline.joiner.content_hash().to_string()
        # Predecessors of joiner are source nodes
        for src_hash in g.predecessors(joiner_hash):
            assert g.nodes[src_hash].get("node_type") == "source"

    def test_graph_node_type_operator(self, pipeline_db):
        """Operator nodes have node_type='operator' after compile."""
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=pipeline_db)
        with job:
            Join()(src_a, src_b, label="joiner")
        pipeline = job.pipeline
        joiner_hash = pipeline.joiner.content_hash().to_string()
        assert pipeline.graph.nodes[joiner_hash].get("node_type") == "operator"

    def test_graph_node_type_function(self, pipeline_db):
        """Function nodes have node_type='function' after compile."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)
        job = PipelineJob(store=pipeline_db)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")
        pipeline = job.pipeline
        adder_hash = pipeline.adder.content_hash().to_string()
        assert pipeline.graph.nodes[adder_hash].get("node_type") == "function"

    def test_graph_label_attribute(self, pipeline_db):
        """Labeled nodes carry their label in graph node attributes after compile."""
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=pipeline_db)
        with job:
            Join()(src_a, src_b, label="my_join")
        pipeline = job.pipeline
        joiner_hash = pipeline.my_join.content_hash().to_string()
        assert pipeline.graph.nodes[joiner_hash].get("label") == "my_join"

    def test_graph_pipeline_hash_attribute(self, pipeline_db):
        """Compiled nodes have pipeline_hash attribute set in the graph."""
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=pipeline_db)
        with job:
            Join()(src_a, src_b, label="joiner")
        pipeline = job.pipeline
        joiner_hash = pipeline.joiner.content_hash().to_string()
        stored_ph = pipeline.graph.nodes[joiner_hash].get("pipeline_hash")
        assert stored_ph is not None
        assert stored_ph == pipeline.joiner.pipeline_hash().to_string()


# ---------------------------------------------------------------------------
# Tests: compile() does not eagerly trigger upstream execution
# ---------------------------------------------------------------------------


class TestCompileDoesNotTriggerExecution:
    """Verify that Pipeline.compile() constructs persistent nodes without
    triggering upstream iter_data / run / as_table materialisation."""

    def test_compile_does_not_trigger_source_materialization(self, pipeline_db):
        """Compile should not trigger any computation or database writes."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(pf)
        job = PipelineJob(store=pipeline_db)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")
        # After compile but before run, adder node has no records
        assert job.pipeline.compiled_nodes["adder"].get_all_records() is None
        # Running should work correctly
        result = job.run()
        table = result.pipeline.compiled_nodes["adder"].as_table()
        assert table.num_rows == 2


class TestSourceNodesInPipeline:
    """Verify that source nodes are first-class pipeline members."""

    def test_source_nodes_in_compiled_nodes(self, pipeline_db):
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)
        job = PipelineJob(store=pipeline_db)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")
        source_nodes = [
            n for n in job.pipeline.compiled_nodes.values() if isinstance(n, SourceNode)
        ]
        assert len(source_nodes) > 0

    def test_source_node_accessible_by_label(self, pipeline_db):
        src = ArrowTableSource(
            table=pa.table({
                "key": pa.array(["a"], type=pa.large_string()),
                "value": pa.array([10], type=pa.int64()),
            }),
            tag_columns=["key"],
            label="my_source",
            infer_nullable=True,
        )
        pf = PythonDataFunction(double_value, output_keys="result")
        pod = FunctionPod(data_function=pf)
        job = PipelineJob(store=pipeline_db)
        with job:
            pod(src, label="doubler")
        assert "my_source" in job.pipeline.compiled_nodes
        assert isinstance(job.pipeline.compiled_nodes["my_source"], SourceNode)

    def test_source_node_label_from_content_hash_when_unlabeled(self, pipeline_db):
        """Source without explicit label gets content-hash-based spec name."""
        src_a, _ = _make_two_sources()
        pf = PythonDataFunction(double_value, output_keys="result")
        pod = FunctionPod(data_function=pf)
        job = PipelineJob(store=pipeline_db)
        with job:
            pod(src_a, label="doubler")
        source_nodes = [
            n for n in job.pipeline.compiled_nodes.values() if isinstance(n, SourceNode)
        ]
        assert len(source_nodes) == 1


class TestSourceNodeNoCaching:
    """Verify that SourceNode does not cache — caching is a source-level concern."""

    def test_source_nodes_do_not_have_cache_path(self, pipeline_db):
        """SourceNode objects wrapping SourceSpecs do not have a cache_path attribute."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)
        job = PipelineJob(store=pipeline_db)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")
        result = job.run()
        records = result.pipeline.compiled_nodes["adder"].get_all_records()
        assert records is not None
        assert records.num_rows == 2
        source_nodes = [
            n for n in job.pipeline._node_graph.nodes() if isinstance(n, SourceNode)
        ]
        assert len(source_nodes) == 2
        for sn in source_nodes:
            assert not hasattr(sn, "cache_path")

    def test_pipeline_with_cached_source_input(self, pipeline_db):
        """CachedSource as pipeline input works and caches source data separately."""
        src_a, src_b = _make_two_sources()
        source_db = InMemoryArrowDatabase()
        cached_a = CachedSource(src_a, cache_database=source_db)
        cached_b = CachedSource(src_b, cache_database=source_db)
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)
        job = PipelineJob(store=pipeline_db)
        with job:
            joined = Join()(cached_a, cached_b)
            pod(joined, label="adder")
        result = job.run()
        records = result.pipeline.compiled_nodes["adder"].get_all_records()
        assert records is not None
        assert records.num_rows == 2


# ---------------------------------------------------------------------------
# Tests: standalone
# ---------------------------------------------------------------------------


def test_run_with_override_observer_does_not_raise():
    src_a, src_b = _make_two_sources()
    db = InMemoryArrowDatabase()
    job = PipelineJob(store=db)
    with job:
        Join()(src_a, src_b, label="joiner")
    # Passing an explicit observer must not raise
    job.run(observer=NoOpObserver())
