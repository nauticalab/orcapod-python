"""
Tests for the Pipeline class.

Verifies that Pipeline correctly wraps all nodes
during compile():
- Leaf streams → SourceNode
- Function pod invocations → FunctionNode
- Operator invocations → OperatorNode
"""

from __future__ import annotations

from typing import Any, cast

import pyarrow as pa
import pytest

from orcapod.core.executors import PythonFunctionExecutorBase
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import (
    FunctionNode,
    OperatorNode,
    SourceNode,
)
from orcapod.core.operators import Join, MapPackets
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import Pipeline
from orcapod.protocols.core_protocols import PacketFunctionProtocol, PacketProtocol

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_source(tag_col: str, packet_col: str, data: dict) -> ArrowTableSource:
    table = pa.table(
        {
            tag_col: pa.array(data[tag_col], type=pa.large_string()),
            packet_col: pa.array(data[packet_col], type=pa.int64()),
        }
    )
    return ArrowTableSource(table, tag_columns=[tag_col])


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


@pytest.fixture
def function_db():
    return InMemoryArrowDatabase()


# ---------------------------------------------------------------------------
# Tests: compile wraps leaf streams as SourceNode
# ---------------------------------------------------------------------------


class TestCompileSourceWrapping:
    def test_compile_wraps_leaf_streams_as_persistent_source_node(self, pipeline_db):
        src_a, src_b = _make_two_sources()
        pipeline = Pipeline(name="test_pipe", pipeline_database=pipeline_db)

        with pipeline:
            _ = Join()(src_a, src_b)

        # The join node should be accessible by label
        assert pipeline._compiled
        # Check that there are nodes in the compiled graph
        assert len(pipeline.compiled_nodes) > 0

        assert pipeline._node_graph is not None
        # The node graph should contain SourceNode instances
        source_nodes = [
            n for n in pipeline._node_graph.nodes() if isinstance(n, SourceNode)
        ]
        assert len(source_nodes) == 2


# ---------------------------------------------------------------------------
# Tests: compile creates FunctionNode
# ---------------------------------------------------------------------------


class TestCompileFunctionNode:
    def test_compile_creates_persistent_function_node(self, pipeline_db):
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(name="fn_pipe", pipeline_database=pipeline_db)

        with pipeline:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        assert "adder" in pipeline.compiled_nodes
        node = pipeline.compiled_nodes["adder"]
        assert isinstance(node, FunctionNode)

    def test_function_node_pipeline_path_prefix(self, pipeline_db):
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(name="fn_pipe", pipeline_database=pipeline_db)

        with pipeline:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        node = pipeline.compiled_nodes["adder"]
        assert isinstance(node, FunctionNode)
        # pipeline_path should start with the pipeline name
        assert node.pipeline_path[0] == "fn_pipe"


# ---------------------------------------------------------------------------
# Tests: compile creates OperatorNode
# ---------------------------------------------------------------------------


class TestCompileOperatorNode:
    def test_compile_creates_persistent_operator_node(self, pipeline_db):
        src_a, src_b = _make_two_sources()

        pipeline = Pipeline(name="op_pipe", pipeline_database=pipeline_db)

        with pipeline:
            Join()(src_a, src_b, label="joiner")

        assert "joiner" in pipeline.compiled_nodes
        node = pipeline.compiled_nodes["joiner"]
        assert isinstance(node, OperatorNode)

    def test_operator_node_pipeline_path_prefix(self, pipeline_db):
        src_a, src_b = _make_two_sources()

        pipeline = Pipeline(name="op_pipe", pipeline_database=pipeline_db)

        with pipeline:
            Join()(src_a, src_b, label="joiner")

        node = pipeline.compiled_nodes["joiner"]
        assert isinstance(node, OperatorNode)
        assert node.pipeline_path[0] == "op_pipe"


# ---------------------------------------------------------------------------
# Tests: compile mutates recorded nodes via attach_databases()
# ---------------------------------------------------------------------------


class TestCompileMutatesNodes:
    def test_compile_reuses_recorded_function_node_objects(self, pipeline_db):
        """compile() should mutate recorded FunctionNodes in place, not create new ones."""
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(name="test", pipeline_database=pipeline_db)

        with pipeline:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        # The FunctionNode in compiled_nodes should be the same object
        # that was recorded in _node_lut during the with block
        fn_nodes = [
            n
            for n in pipeline._persistent_node_map.values()
            if isinstance(n, FunctionNode)
        ]
        assert len(fn_nodes) > 0
        for fn_node in fn_nodes:
            assert fn_node._pipeline_database is not None  # DB was attached

    def test_compile_reuses_recorded_operator_node_objects(self, pipeline_db):
        """compile() should mutate recorded OperatorNodes in place, not create new ones."""
        src_a, src_b = _make_two_sources()

        pipeline = Pipeline(name="test", pipeline_database=pipeline_db)

        with pipeline:
            Join()(src_a, src_b, label="joiner")

        op_nodes = [
            n
            for n in pipeline._persistent_node_map.values()
            if isinstance(n, OperatorNode)
        ]
        assert len(op_nodes) > 0
        for op_node in op_nodes:
            assert op_node._pipeline_database is not None

    def test_recorded_node_identity_preserved_after_compile(self, pipeline_db):
        """The node object from recording should be the same object after compile."""
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(
            name="test", pipeline_database=pipeline_db, auto_compile=False
        )

        with pipeline:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        # Capture node objects before compile
        recorded_nodes = dict(pipeline._node_lut)

        pipeline.compile()

        # Verify that at least the function/operator nodes in persistent_node_map
        # are the SAME objects as were recorded
        for node_hash, recorded_node in recorded_nodes.items():
            if node_hash in pipeline._persistent_node_map:
                assert pipeline._persistent_node_map[node_hash] is recorded_node


# ---------------------------------------------------------------------------
# Tests: function database handling
# ---------------------------------------------------------------------------


class TestFunctionDatabaseHandling:
    def test_function_database_none_uses_results_subfolder(self, pipeline_db):
        """When function_database=None, result path should be pipeline_name/_results."""
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(
            name="my_pipe", pipeline_database=pipeline_db, function_database=None
        )

        with pipeline:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        node = pipeline.compiled_nodes["adder"]
        assert isinstance(node, FunctionNode)

        # The CachedFunctionPod's record_path should start with
        # (pipeline_name, "_results", ...)
        record_path = node._cached_function_pod.record_path
        assert record_path[0] == "my_pipe"
        assert record_path[1] == "_results"

    def test_separate_function_database(self, pipeline_db, function_db):
        """When function_database is provided, it's used as result_database."""
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(
            name="my_pipe",
            pipeline_database=pipeline_db,
            function_database=function_db,
        )

        with pipeline:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        node = pipeline.compiled_nodes["adder"]
        assert isinstance(node, FunctionNode)

        # The CachedFunctionPod should use function_db
        assert node._cached_function_pod._result_database is function_db


# ---------------------------------------------------------------------------
# Tests: label access
# ---------------------------------------------------------------------------


class TestLabelAccess:
    def test_node_access_by_label(self, pipeline_db):
        src_a, src_b = _make_two_sources()

        pipeline = Pipeline(name="test", pipeline_database=pipeline_db)

        with pipeline:
            Join()(src_a, src_b, label="my_join")

        # Access via __getattr__
        node = pipeline.my_join
        assert isinstance(node, OperatorNode)

    def test_label_collision_sorted_by_content_hash(self, pipeline_db):
        """Two nodes with same label get _1, _2 sorted by content hash."""
        src_a = _make_source("k", "value", {"k": ["a"], "value": [1]})
        src_b = _make_source("k", "value", {"k": ["b"], "value": [2]})

        pf1 = PythonPacketFunction(double_value, output_keys="result")
        pf2 = PythonPacketFunction(double_value, output_keys="result")
        pod1 = FunctionPod(packet_function=pf1)
        pod2 = FunctionPod(packet_function=pf2)

        pipeline = Pipeline(name="collision", pipeline_database=pipeline_db)

        with pipeline:
            pod1(src_a, label="compute")
            pod2(src_b, label="compute")

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
        pipeline = Pipeline(name="test", pipeline_database=pipeline_db)
        with pipeline:
            pass  # empty pipeline

        with pytest.raises(AttributeError, match="Pipeline has no attribute"):
            _ = pipeline.nonexistent

    def test_dir_includes_node_labels(self, pipeline_db):
        src_a, src_b = _make_two_sources()
        pipeline = Pipeline(name="test", pipeline_database=pipeline_db)

        with pipeline:
            Join()(src_a, src_b, label="my_join")

        d = dir(pipeline)
        assert "my_join" in d


# ---------------------------------------------------------------------------
# Tests: auto compile and run
# ---------------------------------------------------------------------------


class TestAutoCompileAndRun:
    def test_auto_compile_on_exit(self, pipeline_db):
        src_a, src_b = _make_two_sources()
        pipeline = Pipeline(name="test", pipeline_database=pipeline_db)

        with pipeline:
            Join()(src_a, src_b, label="joiner")

        # Should be compiled after exiting context
        assert pipeline._compiled
        assert "joiner" in pipeline.compiled_nodes

    def test_run_executes_all_nodes(self, pipeline_db):
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(name="run_test", pipeline_database=pipeline_db)

        with pipeline:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        pipeline.run()

        # After run, function node should have records
        node = pipeline.adder
        records = node.get_all_records()
        assert records is not None
        assert records.num_rows == 2  # two input rows (a, b)

    def test_run_auto_saves_when_path_set(self, pipeline_db, tmp_path):
        """Pipeline.run() writes a save file when auto_save_path is set."""
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)
        save_path = tmp_path / "auto_saved.json"

        pipeline = Pipeline(
            name="auto_save_test",
            pipeline_database=pipeline_db,
            auto_save_path=save_path,
        )

        with pipeline:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        pipeline.run()

        assert save_path.exists()
        import json

        data = json.loads(save_path.read_text())
        assert "nodes" in data
        assert "edges" in data

    def test_run_does_not_save_when_path_not_set(self, pipeline_db, tmp_path):
        """Pipeline.run() does not write any file when auto_save_path is None."""
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(name="no_save_test", pipeline_database=pipeline_db)

        with pipeline:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        pipeline.run()

        # No JSON files should have been created
        assert list(tmp_path.glob("*.json")) == []

    def test_pipeline_path_prefix_scoping(self, pipeline_db):
        """All persistent nodes' paths start with pipeline name prefix."""
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(name="scoped", pipeline_database=pipeline_db)

        with pipeline:
            joined = Join()(src_a, src_b, label="joiner")
            pod(joined, label="adder")

        # Check operator node
        joiner = pipeline.joiner
        assert joiner.pipeline_path[0] == "scoped"

        # Check function node
        adder = pipeline.adder
        assert adder.pipeline_path[0] == "scoped"


# ---------------------------------------------------------------------------
# Tests: flush
# ---------------------------------------------------------------------------


class TestFlush:
    def test_flush_flushes_databases(self, pipeline_db, function_db):
        pipeline = Pipeline(
            name="test",
            pipeline_database=pipeline_db,
            function_database=function_db,
        )
        # Just verify it doesn't raise
        pipeline.flush()


# ---------------------------------------------------------------------------
# Tests: end-to-end
# ---------------------------------------------------------------------------


class TestEndToEnd:
    def test_end_to_end_source_join_function(self, pipeline_db):
        """Full pipeline: two sources → Join → FunctionPod.

        Verifies all nodes are persistent and DB records exist after run().
        """
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(name="e2e", pipeline_database=pipeline_db)

        with pipeline:
            joined = Join()(src_a, src_b, label="joiner")
            pod(joined, label="adder")

        # Verify node types
        assert isinstance(pipeline.joiner, OperatorNode)
        assert isinstance(pipeline.adder, FunctionNode)

        # Run the pipeline
        pipeline.run()

        # Function node should have results
        fn_records = pipeline.adder.get_all_records()
        assert fn_records is not None
        assert fn_records.num_rows == 2

        # Verify output values
        table = pipeline.adder.as_table()
        totals = sorted(cast(list[int], table.column("total").to_pylist()))
        # a: 10 + 100 = 110, b: 20 + 200 = 220
        assert totals == [110, 220]


# ---------------------------------------------------------------------------
# Tests: pipeline extension
# ---------------------------------------------------------------------------


class TestPipelineExtension:
    def test_extend_pipeline_with_new_sources(self, pipeline_db):
        """Re-enter pipeline context to add more operations from new sources."""
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(
            name="extend", pipeline_database=pipeline_db, auto_compile=False
        )

        # First context: build the initial graph
        with pipeline:
            joined = src_a.join(src_b, label="joiner")

        # Second context: extend the graph with a new source and function pod
        src_c = _make_source("key", "extra", {"key": ["a", "b"], "extra": [1000, 2000]})
        with pipeline:
            wider = joined.join(src_c, label="wider_join")
            # select only value+score so add_values can process it
            selected = wider.select_packet_columns(["value", "score"], label="selector")
            pod(selected, label="adder")

        pipeline.compile()

        assert "joiner" in pipeline.compiled_nodes
        assert "wider_join" in pipeline.compiled_nodes
        assert "selector" in pipeline.compiled_nodes
        assert "adder" in pipeline.compiled_nodes
        assert isinstance(pipeline.joiner, OperatorNode)
        assert isinstance(pipeline.wider_join, OperatorNode)
        assert isinstance(pipeline.adder, FunctionNode)

        pipeline.run()

        table = pipeline.wider_join.as_table()
        assert table.num_rows == 2
        assert "extra" in table.column_names

        totals = sorted(
            cast(list[int], pipeline.adder.as_table().column("total").to_pylist())
        )
        assert totals == [110, 220]

    def test_extend_pipeline_from_compiled_node(self, pipeline_db):
        """Second context uses an already-compiled persistent node as input."""
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(name="extend_compiled", pipeline_database=pipeline_db)

        # First context: build and auto-compile
        with pipeline:
            joined = src_a.join(src_b, label="joiner")
            pod(joined, label="adder")

        # pipeline.adder is now a FunctionNode
        assert isinstance(pipeline.adder, FunctionNode)

        # Second context: extend from the compiled node
        with pipeline:
            pipeline.adder.map_packets({"total": "final_total"}, label="renamer")

        # Re-compile picks up the extension
        assert "renamer" in pipeline.compiled_nodes
        assert isinstance(pipeline.renamer, OperatorNode)

        pipeline.run()

        table = pipeline.renamer.as_table()
        assert "final_total" in table.column_names
        assert sorted(cast(list[int], table.column("final_total").to_pylist())) == [
            110,
            220,
        ]

    def test_second_pipeline_from_first_pipeline_node(self, pipeline_db):
        """Pipeline B starts from Pipeline A's final compiled node."""
        src_a, src_b = _make_two_sources()
        pf_add = PythonPacketFunction(add_values, output_keys="total")
        pod_add = FunctionPod(packet_function=pf_add)

        pipe_a = Pipeline(name="pipe_a", pipeline_database=pipeline_db)
        with pipe_a:
            joined = src_a.join(src_b, label="joiner")
            pod_add(joined, label="adder")

        pipe_a.run()

        # Pipeline B uses pipe_a.adder as its input source
        db_b = InMemoryArrowDatabase()
        pipe_b = Pipeline(name="pipe_b", pipeline_database=db_b)

        with pipe_b:
            pipe_a.adder.map_packets({"total": "renamed_total"}, label="renamer")

        assert "renamer" in pipe_b.compiled_nodes
        assert isinstance(pipe_b.renamer, OperatorNode)

        # pipe_b should scope everything under "pipe_b"
        assert pipe_b.renamer.pipeline_path[0] == "pipe_b"

        pipe_b.run()

        table = pipe_b.renamer.as_table()
        assert "renamed_total" in table.column_names
        assert sorted(cast(list[int], table.column("renamed_total").to_pylist())) == [
            110,
            220,
        ]

        # pipe_b's source nodes wrap pipe_a.adder as a SourceNode
        assert pipe_b._node_graph is not None
        source_nodes = [
            n for n in pipe_b._node_graph.nodes() if isinstance(n, SourceNode)
        ]
        assert len(source_nodes) == 1


# ---------------------------------------------------------------------------
# Tests: hash chain — extending preserves hashes
# ---------------------------------------------------------------------------


class TestHashChainExtending:
    def test_extending_content_hash_matches_single_pipeline(self, pipeline_db):
        """An operator downstream of pipe_a.adder in pipe_b has the same
        content_hash as if it were defined in a single pipeline."""
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        # Single pipeline baseline
        db_single = InMemoryArrowDatabase()
        single = Pipeline(name="single", pipeline_database=db_single)
        with single:
            joined = src_a.join(src_b, label="joiner")
            pod(joined, label="adder")
        # Get content_hash of adder in single pipeline
        with single:
            single._nodes["adder"].map_packets(
                {"total": "final_total"}, label="renamer"
            )
        single_renamer_content = single.renamer.content_hash()
        single_renamer_pipeline = single.renamer.pipeline_hash()

        # Two-pipeline version: pipe_a has adder, pipe_b uses pipe_a.adder → renamer
        db_a = InMemoryArrowDatabase()
        pipe_a = Pipeline(name="a", pipeline_database=db_a)
        with pipe_a:
            joined = src_a.join(src_b, label="joiner")
            pod(joined, label="adder")

        db_b = InMemoryArrowDatabase()
        pipe_b = Pipeline(name="b", pipeline_database=db_b)
        with pipe_b:
            pipe_a.adder.map_packets({"total": "final_total"}, label="renamer")

        two_renamer_content = pipe_b.renamer.content_hash()
        two_renamer_pipeline = pipe_b.renamer.pipeline_hash()

        # Extending should produce identical hashes
        assert single_renamer_content == two_renamer_content
        assert single_renamer_pipeline == two_renamer_pipeline

    def test_extending_pipeline_hash_matches_single_pipeline(self, pipeline_db):
        """pipeline_hash is identical whether nodes defined in one or two pipelines."""
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        # Single pipeline
        db_single = InMemoryArrowDatabase()
        single = Pipeline(name="single", pipeline_database=db_single)
        with single:
            joined = src_a.join(src_b, label="joiner")
            pod(joined, label="adder")
        with single:
            single.adder.map_packets({"total": "final_total"}, label="renamer")
        single_pipeline_hash = single.renamer.pipeline_hash()

        # Two pipelines
        db_a = InMemoryArrowDatabase()
        pipe_a = Pipeline(name="a", pipeline_database=db_a)
        with pipe_a:
            joined = src_a.join(src_b, label="joiner")
            pod(joined, label="adder")

        db_b = InMemoryArrowDatabase()
        pipe_b = Pipeline(name="b", pipeline_database=db_b)
        with pipe_b:
            pipe_a.adder.map_packets({"total": "final_total"}, label="renamer")

        assert single_pipeline_hash == pipe_b.renamer.pipeline_hash()

    def test_extending_same_pipeline_hashes_match_single_context(self, pipeline_db):
        """Re-entering the same pipeline context preserves hash chain."""
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        # Single context baseline
        db1 = InMemoryArrowDatabase()
        single = Pipeline(name="s", pipeline_database=db1, auto_compile=False)
        with single:
            joined = src_a.join(src_b, label="joiner")
            pod(joined, label="adder")
            MapPackets({"total": "final_total"})(joined, label="renamer")
        single.compile()

        # Two contexts
        db2 = InMemoryArrowDatabase()
        multi = Pipeline(name="m", pipeline_database=db2, auto_compile=False)
        with multi:
            joined = src_a.join(src_b, label="joiner")
            pod(joined, label="adder")
        with multi:
            MapPackets({"total": "final_total"})(joined, label="renamer")
        multi.compile()

        # adder hashes match (same upstream structure)
        assert single.adder.content_hash() == multi.adder.content_hash()
        assert single.adder.pipeline_hash() == multi.adder.pipeline_hash()


# ---------------------------------------------------------------------------
# Tests: hash chain — detaching via .as_source() breaks chain
# ---------------------------------------------------------------------------


class TestHashChainDetaching:
    def test_detached_content_hash_differs_from_extending(self, pipeline_db):
        """DerivedSource (via .as_source()) has different content_hash than
        using the node directly for extending."""
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        db_a = InMemoryArrowDatabase()
        pipe_a = Pipeline(name="pipe_a", pipeline_database=db_a)
        with pipe_a:
            joined = src_a.join(src_b, label="joiner")
            pod(joined, label="adder")
        pipe_a.run()

        # Extending: use pipe_a.adder directly as input
        db_ext = InMemoryArrowDatabase()
        pipe_ext = Pipeline(name="ext", pipeline_database=db_ext)
        with pipe_ext:
            pipe_a.adder.map_packets({"total": "final_total"}, label="renamer")
        ext_hash = pipe_ext.renamer.content_hash()

        # Detaching: use pipe_a.adder.as_source() as input
        derived_src = pipe_a.adder.as_source()
        db_det = InMemoryArrowDatabase()
        pipe_det = Pipeline(name="det", pipeline_database=db_det)
        with pipe_det:
            derived_src.map_packets({"total": "final_total"}, label="renamer")
        det_hash = pipe_det.renamer.content_hash()

        # Hashes should differ — detaching breaks the chain
        assert ext_hash != det_hash

    def test_detached_pipeline_hash_is_schema_only(self, pipeline_db):
        """DerivedSource inherits RootSource.pipeline_identity_structure()
        = (tag_schema, packet_schema), breaking the upstream Merkle chain."""
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        db = InMemoryArrowDatabase()
        pipe = Pipeline(name="pipe", pipeline_database=db)
        with pipe:
            joined = src_a.join(src_b, label="joiner")
            pod(joined, label="adder")
        pipe.run()

        derived_src = pipe.adder.as_source()
        # DerivedSource pipeline_hash should be the RootSource base case
        # (schema-only, no upstream topology)
        tag_schema, packet_schema = derived_src.output_schema()
        # Pipeline hash should NOT equal the origin node's pipeline hash
        assert derived_src.pipeline_hash() != pipe.adder.pipeline_hash()
        # But two DerivedSources with same schema should share pipeline_hash
        derived_src2 = pipe.adder.as_source()
        assert derived_src.pipeline_hash() == derived_src2.pipeline_hash()

    def test_detached_pipeline_downstream_hash_differs_from_extending(
        self, pipeline_db
    ):
        """A full pipeline built from .as_source() produces different hashes
        at every downstream node compared to extending directly."""
        src_a, src_b = _make_two_sources()
        pf_add = PythonPacketFunction(add_values, output_keys="total")
        pod_add = FunctionPod(packet_function=pf_add)
        pf_double = PythonPacketFunction(double_value, output_keys="doubled")

        # Pipeline A: sources → join → adder
        db_a = InMemoryArrowDatabase()
        pipe_a = Pipeline(name="pipe_a", pipeline_database=db_a)
        with pipe_a:
            joined = src_a.join(src_b, label="joiner")
            pod_add(joined, label="adder")
        pipe_a.run()

        # Extending: pipe_b uses pipe_a.adder directly → renamer → doubler
        db_ext = InMemoryArrowDatabase()
        pipe_ext = Pipeline(name="ext", pipeline_database=db_ext)
        with pipe_ext:
            renamed = pipe_a.adder.map_packets({"total": "value"}, label="renamer")
            FunctionPod(packet_function=pf_double)(renamed, label="doubler")

        # Detaching: pipe_c uses pipe_a.adder.as_source() → renamer → doubler
        derived = pipe_a.adder.as_source()
        db_det = InMemoryArrowDatabase()
        pipe_det = Pipeline(name="det", pipeline_database=db_det)
        with pipe_det:
            renamed = derived.map_packets({"total": "value"}, label="renamer")
            FunctionPod(packet_function=pf_double)(renamed, label="doubler")

        # Both content_hash and pipeline_hash should differ at every level
        assert pipe_ext.renamer.content_hash() != pipe_det.renamer.content_hash()
        assert pipe_ext.renamer.pipeline_hash() != pipe_det.renamer.pipeline_hash()
        assert pipe_ext.doubler.content_hash() != pipe_det.doubler.content_hash()
        assert pipe_ext.doubler.pipeline_hash() != pipe_det.doubler.pipeline_hash()

        # But the detached pipeline should still be runnable and correct
        pipe_det.run()
        table = pipe_det.doubler.as_table()
        # 110*2=220, 220*2=440
        assert sorted(table.column("doubled").to_pylist()) == [220, 440]

    def test_detached_pipeline_hash_matches_equivalent_fresh_source(self, pipeline_db):
        """A DerivedSource and a fresh ArrowTableSource with the same schema
        produce identical pipeline_hash downstream, but different content_hash
        (because source_id differs → different identity_structure)."""
        src_a, src_b = _make_two_sources()
        pf_add = PythonPacketFunction(add_values, output_keys="total")
        pod_add = FunctionPod(packet_function=pf_add)
        pf_double = PythonPacketFunction(double_value, output_keys="doubled")

        # Pipeline A: sources → join → adder (schema: tag=key, packet=total)
        db_a = InMemoryArrowDatabase()
        pipe_a = Pipeline(name="pipe_a", pipeline_database=db_a)
        with pipe_a:
            joined = src_a.join(src_b, label="joiner")
            pod_add(joined, label="adder")
        pipe_a.run()

        # Branch 1: pipeline from DerivedSource
        derived = pipe_a.adder.as_source()
        db_derived = InMemoryArrowDatabase()
        pipe_derived = Pipeline(name="derived_pipe", pipeline_database=db_derived)
        with pipe_derived:
            renamed = derived.map_packets({"total": "value"}, label="renamer")
            FunctionPod(packet_function=pf_double)(renamed, label="doubler")

        # Branch 2: pipeline from a fresh ArrowTableSource with identical schema
        # Same schema as DerivedSource: tag=key (large_string), packet=total (int64)
        fresh_table = pa.table(
            {
                "key": pa.array(["x", "y"], type=pa.large_string()),
                "total": pa.array([999, 888], type=pa.int64()),
            }
        )
        fresh_src = ArrowTableSource(fresh_table, tag_columns=["key"])
        db_fresh = InMemoryArrowDatabase()
        pipe_fresh = Pipeline(name="fresh_pipe", pipeline_database=db_fresh)
        with pipe_fresh:
            renamed = fresh_src.map_packets({"total": "value"}, label="renamer")
            FunctionPod(packet_function=pf_double)(renamed, label="doubler")

        # pipeline_hash should be IDENTICAL at every level
        # (both start from RootSource with same schema → same pipeline identity base case)
        assert (
            pipe_derived.renamer.pipeline_hash() == pipe_fresh.renamer.pipeline_hash()
        )
        assert (
            pipe_derived.doubler.pipeline_hash() == pipe_fresh.doubler.pipeline_hash()
        )

        # content_hash should DIFFER at every level
        # (different source_id → different identity_structure → different content_hash)
        assert pipe_derived.renamer.content_hash() != pipe_fresh.renamer.content_hash()
        assert pipe_derived.doubler.content_hash() != pipe_fresh.doubler.content_hash()

        # Both pipelines should run correctly with their own data
        pipe_derived.run()
        pipe_fresh.run()
        derived_results = sorted(
            pipe_derived.doubler.as_table().column("doubled").to_pylist()
        )
        fresh_results = sorted(
            pipe_fresh.doubler.as_table().column("doubled").to_pylist()
        )
        # 110*2=220, 220*2=440 for derived; 999*2=1998, 888*2=1776 for fresh
        assert derived_results == [220, 440]
        assert fresh_results == [1776, 1998]

    def test_detached_source_has_source_id(self, pipeline_db):
        """DerivedSource.source_id contains pipeline path info."""
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        db = InMemoryArrowDatabase()
        pipe = Pipeline(name="my_pipe", pipeline_database=db)
        with pipe:
            joined = src_a.join(src_b, label="joiner")
            pod(joined, label="adder")
        pipe.run()

        derived_src = pipe.adder.as_source()
        sid = derived_src.source_id
        assert isinstance(sid, str)
        # Should contain the pipeline name
        assert "my_pipe" in sid
        # Should contain a content hash fragment
        content_frag = pipe.adder.content_hash().to_string()[:16]
        assert content_frag in sid


# ---------------------------------------------------------------------------
# Tests: hash graph
# ---------------------------------------------------------------------------


class TestHashGraph:
    def test_graph_empty_before_context(self, pipeline_db):
        """pipeline.graph is an empty DiGraph before any with block."""
        import networkx as nx

        pipeline = Pipeline(name="test", pipeline_database=pipeline_db)
        assert isinstance(pipeline.graph, nx.DiGraph)
        assert len(pipeline.graph.nodes) == 0
        assert len(pipeline.graph.edges) == 0

    def test_graph_has_edges_after_compile(self, pipeline_db):
        """After a with block + compile, graph contains the right edges."""
        src_a, src_b = _make_two_sources()
        pipeline = Pipeline(name="test", pipeline_database=pipeline_db)

        with pipeline:
            Join()(src_a, src_b, label="joiner")

        g = pipeline.graph
        assert len(g.edges) > 0
        # joiner node hash should be in the graph
        joiner_hash = pipeline.joiner.content_hash().to_string()
        assert joiner_hash in g.nodes

    def test_graph_accumulates_across_with_blocks(self, pipeline_db):
        """Edges from a second with block are added to those from the first."""
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(name="acc", pipeline_database=pipeline_db)

        with pipeline:
            joined = src_a.join(src_b, label="joiner")

        edges_after_first = set(pipeline.graph.edges)
        assert len(edges_after_first) > 0

        with pipeline:
            pod(joined, label="adder")

        edges_after_second = set(pipeline.graph.edges)
        # Second block adds more edges; first block's edges are preserved
        assert edges_after_first.issubset(edges_after_second)
        assert len(edges_after_second) > len(edges_after_first)

    def test_graph_node_type_source(self, pipeline_db):
        """Source nodes have node_type='source' after compile."""
        src_a, src_b = _make_two_sources()
        pipeline = Pipeline(name="types", pipeline_database=pipeline_db)

        with pipeline:
            Join()(src_a, src_b, label="joiner")

        g = pipeline.graph
        joiner_hash = pipeline.joiner.content_hash().to_string()
        # Predecessors of joiner are source nodes
        for src_hash in g.predecessors(joiner_hash):
            assert g.nodes[src_hash].get("node_type") == "source"

    def test_graph_node_type_operator(self, pipeline_db):
        """Operator nodes have node_type='operator' after compile."""
        src_a, src_b = _make_two_sources()
        pipeline = Pipeline(name="types", pipeline_database=pipeline_db)

        with pipeline:
            Join()(src_a, src_b, label="joiner")

        joiner_hash = pipeline.joiner.content_hash().to_string()
        assert pipeline.graph.nodes[joiner_hash].get("node_type") == "operator"

    def test_graph_node_type_function(self, pipeline_db):
        """Function nodes have node_type='function' after compile."""
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(name="types_fn", pipeline_database=pipeline_db)

        with pipeline:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        adder_hash = pipeline.adder.content_hash().to_string()
        assert pipeline.graph.nodes[adder_hash].get("node_type") == "function"

    def test_graph_label_attribute(self, pipeline_db):
        """Labeled nodes carry their label in graph node attributes after compile."""
        src_a, src_b = _make_two_sources()
        pipeline = Pipeline(name="labels", pipeline_database=pipeline_db)

        with pipeline:
            Join()(src_a, src_b, label="my_join")

        joiner_hash = pipeline.my_join.content_hash().to_string()
        assert pipeline.graph.nodes[joiner_hash].get("label") == "my_join"

    def test_graph_pipeline_hash_attribute(self, pipeline_db):
        """Compiled nodes have pipeline_hash attribute set in the graph."""
        src_a, src_b = _make_two_sources()
        pipeline = Pipeline(name="phash", pipeline_database=pipeline_db)

        with pipeline:
            Join()(src_a, src_b, label="joiner")

        joiner_hash = pipeline.joiner.content_hash().to_string()
        stored_ph = pipeline.graph.nodes[joiner_hash].get("pipeline_hash")
        assert stored_ph is not None
        assert stored_ph == pipeline.joiner.pipeline_hash().to_string()


# ---------------------------------------------------------------------------
# Tests: _compute_pipeline_snapshot_hash
# ---------------------------------------------------------------------------


class TestPipelineSnapshotHash:
    """Tests for Pipeline._compute_pipeline_snapshot_hash().

    Verifies determinism, sensitivity to node changes, and sensitivity to
    edge changes (since edges are included in the canonical hash input).
    """

    def test_hash_is_deterministic_for_same_graph(self, pipeline_db):
        """Compiling the same pipeline twice yields the same snapshot hash."""
        src_a, src_b = _make_two_sources()
        pipeline = Pipeline(name="snap", pipeline_database=pipeline_db)
        with pipeline:
            Join()(src_a, src_b, label="joiner")

        hash1 = pipeline._compute_pipeline_snapshot_hash()
        hash2 = pipeline._compute_pipeline_snapshot_hash()
        assert hash1 == hash2
        assert len(hash1) == 16  # 16-char truncated SHA-256 prefix

    def test_hash_changes_when_node_added(self, pipeline_db):
        """Adding a new node to the pipeline changes the snapshot hash."""
        src_a, src_b = _make_two_sources()
        pipeline = Pipeline(name="snap2", pipeline_database=pipeline_db)
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        with pipeline:
            joined = Join()(src_a, src_b, label="joiner")

        hash_before = pipeline._compute_pipeline_snapshot_hash()

        with pipeline:
            pod(joined, label="adder")

        hash_after = pipeline._compute_pipeline_snapshot_hash()
        assert hash_before != hash_after

    def test_hash_changes_when_topology_differs(self, pipeline_db):
        """Two pipelines that apply the same transformation but with different
        DAG topologies (one extra hop vs direct) produce different hashes,
        because edges are included in the canonical SHA-256 input alongside
        the node ordering."""
        src_a, src_b = _make_two_sources()
        pf_add = PythonPacketFunction(add_values, output_keys="total")
        pf_double = PythonPacketFunction(double_value, output_keys="value")
        pod_add = FunctionPod(packet_function=pf_add)
        pod_double = FunctionPod(packet_function=pf_double)

        # Pipeline 1: join → add (two nodes, one edge between them)
        db1 = InMemoryArrowDatabase()
        pipeline1 = Pipeline(name="topo_test", pipeline_database=db1)
        with pipeline1:
            joined = Join()(src_a, src_b, label="joiner")
            pod_add(joined, label="adder")
        hash1 = pipeline1._compute_pipeline_snapshot_hash()

        # Pipeline 2: source → double → (different terminal node, different edge)
        src_c = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        db2 = InMemoryArrowDatabase()
        pipeline2 = Pipeline(name="topo_test", pipeline_database=db2)
        with pipeline2:
            pod_double(src_c, label="doubler")
        hash2 = pipeline2._compute_pipeline_snapshot_hash()

        assert hash1 != hash2

    def test_empty_graph_returns_empty_string(self, pipeline_db):
        """A freshly-constructed (uncompiled) pipeline returns '' because
        the hash graph has no nodes."""
        pipeline = Pipeline(name="empty", pipeline_database=pipeline_db)
        assert pipeline._compute_pipeline_snapshot_hash() == ""


# ---------------------------------------------------------------------------
# Tests: incremental compile preserves existing nodes
# ---------------------------------------------------------------------------


class TestIncrementalCompile:
    def test_recompile_preserves_existing_node_objects(self, pipeline_db):
        """After re-entering context and compiling, existing persistent nodes
        are the same Python objects (identity via `is`)."""
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(name="incr", pipeline_database=pipeline_db)

        # First compile
        with pipeline:
            joined = src_a.join(src_b, label="joiner")
            pod(joined, label="adder")

        first_joiner = pipeline.joiner
        first_adder = pipeline.adder

        # Second context: extend
        with pipeline:
            pipeline.adder.map_packets({"total": "final_total"}, label="renamer")

        # Original nodes should be the exact same objects
        assert pipeline.joiner is first_joiner
        assert pipeline.adder is first_adder
        # New node should exist
        assert "renamer" in pipeline.compiled_nodes

    def test_recompile_preserves_existing_source_nodes(self, pipeline_db):
        """SourceNode objects from first compile survive second compile."""
        src_a, src_b = _make_two_sources()

        pipeline = Pipeline(name="incr_src", pipeline_database=pipeline_db)

        with pipeline:
            src_a.join(src_b, label="joiner")

        assert pipeline._node_graph is not None
        first_source_nodes = {
            id(n) for n in pipeline._node_graph.nodes() if isinstance(n, SourceNode)
        }

        # Extend with another operation
        with pipeline:
            pipeline.joiner.map_packets({"value": "val"}, label="renamer")

        second_source_nodes = {
            id(n) for n in pipeline._node_graph.nodes() if isinstance(n, SourceNode)
        }

        # All original source nodes should be preserved (same object ids)
        assert first_source_nodes.issubset(second_source_nodes)

    def test_recompile_adds_new_nodes_without_replacing_old(self, pipeline_db):
        """New operations appear in compiled_nodes alongside preserved old ones."""
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(name="incr_add", pipeline_database=pipeline_db)

        with pipeline:
            joined = src_a.join(src_b, label="joiner")
            pod(joined, label="adder")

        assert len(pipeline.compiled_nodes) == 4  # 2 sources + joiner + adder

        with pipeline:
            pipeline.adder.map_packets({"total": "final_total"}, label="renamer")

        assert len(pipeline.compiled_nodes) == 5  # 2 sources + joiner + adder + renamer
        assert isinstance(pipeline.renamer, OperatorNode)

        # Run to verify everything works end-to-end
        pipeline.run()
        table = pipeline.renamer.as_table()
        assert sorted(cast(list[int], table.column("final_total").to_pylist())) == [
            110,
            220,
        ]


# ---------------------------------------------------------------------------
# Tests: compile() does not eagerly trigger upstream execution
# ---------------------------------------------------------------------------


class TestCompileDoesNotTriggerExecution:
    """Verify that Pipeline.compile() constructs persistent nodes without
    triggering upstream iter_packets / run / as_table materialisation."""

    def test_compile_does_not_trigger_source_materialization(self, pipeline_db):
        """Compile should not trigger any computation or database writes."""
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(pf)

        pipeline = Pipeline(name="lazy_test", pipeline_database=pipeline_db)
        with pipeline:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        # After compile, the pipeline database should be empty — compile()
        # only builds the graph, it doesn't execute any nodes.
        assert pipeline.adder.get_all_records() is None

        # Running the pipeline should still work correctly after lazy compile.
        pipeline.run()
        table = pipeline.adder.as_table()
        assert table.num_rows == 2


# ---------------------------------------------------------------------------
# Tests: Pipeline.run() execution engine and async default
# ---------------------------------------------------------------------------


class _MockExecutor(PythonFunctionExecutorBase):
    """In-process executor that records sync vs async calls.

    Supports concurrent execution so the async pipeline orchestrator
    routes through async_execute.  with_options() returns a new instance
    so per-node opts can be inspected via .opts.
    """

    def __init__(self, opts: dict[str, Any] | None = None) -> None:
        self.opts: dict[str, Any] = opts or {}
        self.sync_calls: list[Any] = []
        self.async_calls: list[Any] = []

    @property
    def executor_type_id(self) -> str:
        return "mock"

    def supported_function_type_ids(self) -> frozenset[str]:
        return frozenset()  # empty frozenset = supports all types

    @property
    def supports_concurrent_execution(self) -> bool:
        return True

    def execute(
        self,
        packet_function: PacketFunctionProtocol,
        packet: PacketProtocol,
        *,
        logger=None,
    ) -> "PacketProtocol | None":
        self.sync_calls.append(packet)
        return packet_function.direct_call(packet)

    async def async_execute(
        self,
        packet_function: PacketFunctionProtocol,
        packet: PacketProtocol,
        *,
        logger=None,
    ) -> "PacketProtocol | None":
        self.async_calls.append(packet)
        return packet_function.direct_call(packet)

    def execute_callable(self, fn, kwargs, executor_options=None, *, logger=None):
        self.sync_calls.append(kwargs)
        return fn(**kwargs)

    async def async_execute_callable(self, fn, kwargs, executor_options=None, *, logger=None):
        self.async_calls.append(kwargs)
        return fn(**kwargs)

    def with_options(self, **opts: Any) -> "_MockExecutor":
        return _MockExecutor(opts={**self.opts, **opts})

    def get_execution_data(self) -> dict[str, Any]:
        return {"executor_type": self.executor_type_id, **self.opts}


class TestRunExecutionEngine:
    def test_engine_is_applied_to_all_function_nodes(self, pipeline_db):
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        mock = _MockExecutor()

        pipeline = Pipeline(name="test_engine", pipeline_database=pipeline_db)
        with pipeline:
            pod(src, label="doubler")

        pipeline.run(execution_engine=mock)

        assert isinstance(pipeline.doubler.executor, _MockExecutor)

    def test_engine_without_config_triggers_async_mode(self, pipeline_db):
        """No config + execution_engine → async channels mode by default."""
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        mock = _MockExecutor()

        pipeline = Pipeline(name="test_async_default", pipeline_database=pipeline_db)
        with pipeline:
            pod(src, label="doubler")

        pipeline.run(execution_engine=mock)

        # Each node gets its own copy; check the node's executor
        node_executor = pipeline.doubler.executor
        assert len(node_executor.async_calls) > 0
        assert len(node_executor.sync_calls) == 0

    def test_explicit_sync_config_overrides_async_default(self, pipeline_db):
        """Explicit config=PipelineConfig(executor=SYNCHRONOUS) takes priority
        over the async default, even when an execution_engine is provided."""
        from orcapod.types import ExecutorType, PipelineConfig

        src = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        mock = _MockExecutor()

        pipeline = Pipeline(name="test_sync_override", pipeline_database=pipeline_db)
        with pipeline:
            pod(src, label="doubler")

        pipeline.run(
            execution_engine=mock,
            config=PipelineConfig(executor=ExecutorType.SYNCHRONOUS),
        )

        node_executor = pipeline.doubler.executor
        assert len(node_executor.sync_calls) > 0
        assert len(node_executor.async_calls) == 0

    def test_pipeline_opts_applied_via_with_options(self, pipeline_db):
        """Pipeline-level execution_engine_opts are applied via with_options."""
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        mock = _MockExecutor()

        pipeline = Pipeline(name="test_pipeline_opts", pipeline_database=pipeline_db)
        with pipeline:
            pod(src, label="doubler")

        pipeline.run(
            execution_engine=mock,
            execution_engine_opts={"num_cpus": 4},
        )

        # Executor should have been created with the pipeline opts
        assert pipeline.doubler.executor.opts.get("num_cpus") == 4

    def test_no_opts_produces_per_node_copy(self, pipeline_db):
        """Without execution_engine_opts, each node gets its own executor copy."""
        src = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        mock = _MockExecutor()

        pipeline = Pipeline(name="test_no_opts", pipeline_database=pipeline_db)
        with pipeline:
            pod(src, label="doubler")

        pipeline.run(execution_engine=mock)

        # Each node gets a copy via with_options(), not the original
        assert pipeline.doubler.executor is not mock
        assert isinstance(pipeline.doubler.executor, _MockExecutor)
        assert pipeline.doubler.executor.opts == {}


class TestSourceNodesInPipeline:
    """Verify that source nodes are first-class pipeline members."""

    def test_source_nodes_in_compiled_nodes(self, pipeline_db):
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(name="test", pipeline_database=pipeline_db)
        with pipeline:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        source_nodes = [
            n for n in pipeline.compiled_nodes.values() if isinstance(n, SourceNode)
        ]
        assert len(source_nodes) > 0

    def test_source_node_accessible_by_label(self, pipeline_db):
        src = ArrowTableSource(
            table=pa.table(
                {
                    "key": pa.array(["a"], type=pa.large_string()),
                    "value": pa.array([10], type=pa.int64()),
                }
            ),
            tag_columns=["key"],
            label="my_source",
        )
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(name="test", pipeline_database=pipeline_db)
        with pipeline:
            pod(src, label="doubler")

        assert "my_source" in pipeline.compiled_nodes
        assert isinstance(pipeline.my_source, SourceNode)

    def test_source_node_label_defaults_to_class_name(self, pipeline_db):
        src_a, _ = _make_two_sources()
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(name="test", pipeline_database=pipeline_db)
        with pipeline:
            pod(src_a, label="doubler")

        # Source without explicit label gets class name as label
        source_nodes = [
            n for n in pipeline.compiled_nodes.values() if isinstance(n, SourceNode)
        ]
        assert len(source_nodes) == 1


class TestSourceNodeNoCaching:
    """Verify that SourceNode does not cache — caching is a source-level concern."""

    def test_source_nodes_do_not_write_to_db(self, pipeline_db):
        """Source nodes should not write anything to the pipeline DB."""
        src_a, src_b = _make_two_sources()
        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(name="test_pipe", pipeline_database=pipeline_db)
        with pipeline:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        pipeline.run()

        # Function node should have computed results (pipeline works)
        records = pipeline.adder.get_all_records()
        assert records is not None
        assert records.num_rows == 2

        # Source nodes are plain SourceNode — no caching, no DB writes
        source_nodes = [
            n for n in pipeline._node_graph.nodes() if isinstance(n, SourceNode)
        ]
        assert len(source_nodes) == 2
        for sn in source_nodes:
            assert not hasattr(sn, "cache_path")
            assert not hasattr(sn, "get_all_records")

    def test_pipeline_with_cached_source_input(self, pipeline_db):
        """CachedSource as pipeline input: source caching + pipeline execution."""
        from orcapod.core.sources import CachedSource

        src_a, src_b = _make_two_sources()
        source_db = InMemoryArrowDatabase()
        cached_a = CachedSource(src_a, cache_database=source_db)
        cached_b = CachedSource(src_b, cache_database=source_db)

        pf = PythonPacketFunction(add_values, output_keys="total")
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(name="cached_src", pipeline_database=pipeline_db)
        with pipeline:
            joined = Join()(cached_a, cached_b)
            pod(joined, label="adder")

        pipeline.run()

        # Function node computed results
        records = pipeline.adder.get_all_records()
        assert records is not None
        assert records.num_rows == 2

        # Source data was cached in source_db (not pipeline_db)
        assert cached_a.get_all_records() is not None
        assert cached_b.get_all_records() is not None
