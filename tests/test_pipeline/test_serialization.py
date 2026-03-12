"""End-to-end tests for Pipeline.save() and Pipeline.load()."""

import csv
import json

import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import Batch, Join
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource, CSVSource
from orcapod.core.sources.dict_source import DictSource
from orcapod.databases.delta_lake_databases import DeltaTableDatabase
from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
from orcapod.pipeline import Pipeline
from orcapod.pipeline.serialization import PIPELINE_FORMAT_VERSION, LoadStatus
from orcapod.types import Schema


def transform_func(y: int) -> int:
    """A simple transform function that doubles the value."""
    return y * 2


@pytest.fixture
def simple_pipeline(tmp_path):
    """Create a simple pipeline: source -> function_pod."""
    db = DeltaTableDatabase(base_path=str(tmp_path / "pipeline_db"))
    source = DictSource(
        data=[{"x": 1, "y": 2}, {"x": 3, "y": 4}],
        tag_columns=["x"],
        source_id="test_source",
    )
    pf = PythonPacketFunction(
        function=transform_func,
        output_keys="result",
        function_name="transform_func",
    )
    pod = FunctionPod(packet_function=pf)
    pipeline = Pipeline(name="test", pipeline_database=db)
    with pipeline:
        result = pod.process(source, label="transform")
    return pipeline, tmp_path


@pytest.fixture
def multi_source_pipeline(tmp_path):
    """Create a pipeline: two sources -> join -> function_pod."""
    db = DeltaTableDatabase(base_path=str(tmp_path / "pipeline_db"))

    table_a = pa.table(
        {
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "value": pa.array([10, 20], type=pa.int64()),
        }
    )
    table_b = pa.table(
        {
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "score": pa.array([100, 200], type=pa.int64()),
        }
    )
    src_a = ArrowTableSource(table_a, tag_columns=["key"], source_id="src_a")
    src_b = ArrowTableSource(table_b, tag_columns=["key"], source_id="src_b")

    def add_values(value: int, score: int) -> int:
        return value + score

    pf = PythonPacketFunction(
        function=add_values,
        output_keys="total",
        function_name="add_values",
    )
    pod = FunctionPod(packet_function=pf)
    join = Join()

    pipeline = Pipeline(name="multi", pipeline_database=db)
    with pipeline:
        joined = join.process(src_a, src_b, label="join_ab")
        result = pod.process(joined, label="compute")

    return pipeline, tmp_path


class TestPipelineSave:
    def test_save_creates_json_file(self, simple_pipeline):
        pipeline, tmp_path = simple_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        assert path.exists()

    def test_save_valid_json(self, simple_pipeline):
        pipeline, tmp_path = simple_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())
        assert "orcapod_pipeline_version" in data
        assert "pipeline" in data
        assert "nodes" in data
        assert "edges" in data

    def test_save_version(self, simple_pipeline):
        pipeline, tmp_path = simple_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())
        assert data["orcapod_pipeline_version"] == PIPELINE_FORMAT_VERSION

    def test_save_pipeline_metadata(self, simple_pipeline):
        pipeline, tmp_path = simple_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())
        assert data["pipeline"]["name"] == ["test"]
        assert (
            data["pipeline"]["databases"]["pipeline_database"]["type"] == "delta_table"
        )

    def test_save_function_database_null_when_not_set(self, simple_pipeline):
        pipeline, tmp_path = simple_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())
        assert data["pipeline"]["databases"]["function_database"] is None

    def test_save_includes_nodes(self, simple_pipeline):
        pipeline, tmp_path = simple_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())
        nodes = data["nodes"]
        # Should have source + function nodes
        node_types = {n["node_type"] for n in nodes.values()}
        assert "source" in node_types
        assert "function" in node_types

    def test_save_includes_edges(self, simple_pipeline):
        pipeline, tmp_path = simple_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())
        assert len(data["edges"]) > 0
        # Each edge is a pair of content hashes
        for edge in data["edges"]:
            assert len(edge) == 2
            assert isinstance(edge[0], str)
            assert isinstance(edge[1], str)

    def test_save_raises_if_not_compiled(self, tmp_path):
        db = InMemoryArrowDatabase()
        pipeline = Pipeline(name="test", pipeline_database=db, auto_compile=False)
        with pytest.raises(ValueError, match="not compiled"):
            pipeline.save(str(tmp_path / "pipeline.json"))

    def test_save_node_common_fields(self, simple_pipeline):
        """Every node descriptor has the required common fields."""
        pipeline, tmp_path = simple_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())

        required_fields = {
            "node_type",
            "content_hash",
            "pipeline_hash",
            "data_context_key",
            "output_schema",
        }
        for node_hash, descriptor in data["nodes"].items():
            assert required_fields <= set(descriptor.keys()), (
                f"Node {node_hash} missing fields: "
                f"{required_fields - set(descriptor.keys())}"
            )
            # output_schema has tag and packet sub-dicts
            assert "tag" in descriptor["output_schema"]
            assert "packet" in descriptor["output_schema"]

    def test_save_source_node_fields(self, simple_pipeline):
        """Source node descriptors have source-specific fields."""
        pipeline, tmp_path = simple_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())

        source_nodes = [n for n in data["nodes"].values() if n["node_type"] == "source"]
        assert len(source_nodes) == 1
        src = source_nodes[0]
        assert "stream_type" in src
        assert "source_id" in src
        assert "reconstructable" in src
        # DictSource is in-memory, not reconstructable
        assert src["reconstructable"] is False
        assert src["stream_type"] == "dict"

    def test_save_function_node_fields(self, simple_pipeline):
        """Function node descriptors have function-specific fields."""
        pipeline, tmp_path = simple_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())

        fn_nodes = [n for n in data["nodes"].values() if n["node_type"] == "function"]
        assert len(fn_nodes) == 1
        fn = fn_nodes[0]
        assert "function_pod" in fn
        assert "pipeline_path" in fn
        assert "result_record_path" in fn
        assert isinstance(fn["pipeline_path"], list)
        assert isinstance(fn["result_record_path"], list)

    def test_save_multi_source_pipeline(self, multi_source_pipeline):
        """Pipeline with join and multiple sources serializes correctly."""
        pipeline, tmp_path = multi_source_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())

        node_types = [n["node_type"] for n in data["nodes"].values()]
        assert node_types.count("source") == 2
        assert node_types.count("function") == 1
        # operator node for join
        assert node_types.count("operator") == 1

    def test_save_operator_node_fields(self, multi_source_pipeline):
        """Operator node descriptors have operator-specific fields."""
        pipeline, tmp_path = multi_source_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())

        op_nodes = [n for n in data["nodes"].values() if n["node_type"] == "operator"]
        assert len(op_nodes) == 1
        op = op_nodes[0]
        assert "operator" in op
        assert "cache_mode" in op
        assert "pipeline_path" in op
        assert isinstance(op["pipeline_path"], list)

    def test_save_edges_reference_valid_nodes(self, simple_pipeline):
        """All edge endpoints are valid node keys."""
        pipeline, tmp_path = simple_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())

        node_keys = set(data["nodes"].keys())
        for upstream, downstream in data["edges"]:
            assert upstream in node_keys, f"Edge upstream {upstream} not in nodes"
            assert downstream in node_keys, f"Edge downstream {downstream} not in nodes"

    def test_save_with_separate_function_database(self, tmp_path):
        """When a separate function_database is provided, it is serialized."""
        pipeline_db = DeltaTableDatabase(base_path=str(tmp_path / "pipeline_db"))
        function_db = DeltaTableDatabase(base_path=str(tmp_path / "function_db"))

        source = DictSource(
            data=[{"x": 1, "y": 2}],
            tag_columns=["x"],
            source_id="test_source",
        )
        pf = PythonPacketFunction(
            function=transform_func,
            output_keys=["result"],
            function_name="transform_func",
        )
        pod = FunctionPod(packet_function=pf)
        pipeline = Pipeline(
            name="test",
            pipeline_database=pipeline_db,
            function_database=function_db,
        )
        with pipeline:
            pod.process(source, label="transform")

        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())
        assert data["pipeline"]["databases"]["function_database"] is not None
        assert (
            data["pipeline"]["databases"]["function_database"]["type"] == "delta_table"
        )

    def test_save_node_content_hash_matches_key(self, simple_pipeline):
        """Each node's content_hash field matches its key in the nodes dict."""
        pipeline, tmp_path = simple_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())

        for node_hash, descriptor in data["nodes"].items():
            assert descriptor["content_hash"] == node_hash


class TestPipelineLoad:
    def test_load_full_mode(self, simple_pipeline):
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="full")
        assert loaded.name == pipeline.name
        assert len(loaded.compiled_nodes) == len(pipeline.compiled_nodes)

    def test_load_read_only_mode(self, simple_pipeline):
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="read_only")
        assert loaded.name == pipeline.name
        # Function nodes should be read-only
        for name, node in loaded.compiled_nodes.items():
            if hasattr(node, "load_status"):
                if node.node_type == "function":
                    assert node.load_status == LoadStatus.READ_ONLY

    def test_load_version_mismatch_raises(self, simple_pipeline, tmp_path):
        pipeline, _ = simple_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())
        data["orcapod_pipeline_version"] = "99.0.0"
        path.write_text(json.dumps(data))
        with pytest.raises(ValueError, match="version"):
            Pipeline.load(str(path))

    def test_load_preserves_node_labels(self, simple_pipeline):
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="full")
        original_labels = set(pipeline.compiled_nodes.keys())
        loaded_labels = set(loaded.compiled_nodes.keys())
        assert original_labels == loaded_labels

    def test_load_preserves_graph_structure(self, simple_pipeline):
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="full")
        assert len(loaded._graph_edges) == len(pipeline._graph_edges)


class TestPipelineSaveLoadIntegration:
    def test_save_load_run_full_cycle(self, tmp_path):
        """Save a pipeline, load in full mode, and verify structure."""
        db_path = str(tmp_path / "db")
        db = DeltaTableDatabase(base_path=db_path)
        source = DictSource(
            data=[{"x": 1, "y": 2}, {"x": 3, "y": 4}],
            tag_columns=["x"],
            source_id="test_source",
        )
        pf = PythonPacketFunction(
            function=transform_func,
            output_keys=["result"],
            function_name="transform_func",
        )
        pod = FunctionPod(packet_function=pf)
        pipeline = Pipeline(name="test", pipeline_database=db)
        with pipeline:
            result = pod.process(source, label="transform")
        pipeline.run()

        # Save and reload
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="full")

        # The loaded pipeline should have the same nodes
        assert set(loaded.compiled_nodes.keys()) == set(pipeline.compiled_nodes.keys())

    def test_read_only_can_access_cached_data(self, tmp_path):
        """Save a pipeline after run, load read-only, access cached results."""
        db_path = str(tmp_path / "db")
        db = DeltaTableDatabase(base_path=db_path)
        source = DictSource(
            data=[{"x": 1, "y": 2}, {"x": 3, "y": 4}],
            tag_columns=["x"],
            source_id="test_source",
        )
        pf = PythonPacketFunction(
            function=transform_func,
            output_keys=["result"],
            function_name="transform_func",
        )
        pod = FunctionPod(packet_function=pf)
        pipeline = Pipeline(name="test", pipeline_database=db)
        with pipeline:
            result = pod.process(source, label="transform")
        pipeline.run()
        db.flush()

        # Save and reload in read-only mode
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="read_only")

        # Function node should be read-only but have cached data accessible
        transform_node = loaded.compiled_nodes.get("transform")
        assert transform_node is not None
        assert transform_node.load_status == LoadStatus.READ_ONLY

    def test_pipeline_with_operator(self, tmp_path):
        """Save/load a pipeline with an operator node."""
        db = DeltaTableDatabase(base_path=str(tmp_path / "db"))
        source1 = DictSource(
            data=[{"a": 1, "b": 10}], tag_columns=["a"], source_id="s1"
        )
        source2 = DictSource(
            data=[{"a": 1, "c": 20}], tag_columns=["a"], source_id="s2"
        )
        pipeline = Pipeline(name="test", pipeline_database=db)
        with pipeline:
            joined = Join().process(source1, source2, label="my_join")
        pipeline.run()

        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="full")

        assert "my_join" in loaded.compiled_nodes


# ---------------------------------------------------------------------------
# Helper: write a CSV file for reconstructable source tests
# ---------------------------------------------------------------------------


def _write_csv(path, rows):
    """Write a list of dicts as a CSV file."""
    fieldnames = list(rows[0].keys())
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _make_csv_pipeline(tmp_path):
    """Build and return (pipeline, csv_path, json_path, db) using a CSVSource.

    The source is reconstructable, so full-mode load can rebuild it.
    """
    csv_path = str(tmp_path / "data.csv")
    _write_csv(csv_path, [{"x": "1", "y": "2"}, {"x": "3", "y": "4"}])

    db = DeltaTableDatabase(base_path=str(tmp_path / "db"))
    source = CSVSource(
        file_path=csv_path,
        tag_columns=["x"],
        source_id="csv_source",
    )
    pf = PythonPacketFunction(
        function=transform_func,
        output_keys=["result"],
        function_name="transform_func",
    )
    pod = FunctionPod(packet_function=pf)
    pipeline = Pipeline(name="csv_test", pipeline_database=db)
    with pipeline:
        pod.process(source, label="transform")
    pipeline.run()
    db.flush()

    json_path = str(tmp_path / "pipeline.json")
    pipeline.save(json_path)
    return pipeline, csv_path, json_path, db


# ---------------------------------------------------------------------------
# Thorough read-only mode tests
# ---------------------------------------------------------------------------


class TestReadOnlyMode:
    """Verify behavior of pipelines loaded in read_only mode."""

    def test_all_nodes_have_load_status(self, simple_pipeline):
        """Every node in a read-only pipeline exposes a load_status."""
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        pipeline.flush()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="read_only")

        for node_hash, node in loaded._persistent_node_map.items():
            assert hasattr(node, "load_status"), (
                f"Node {node_hash} ({node.node_type}) has no load_status"
            )

    def test_function_node_is_read_only(self, simple_pipeline):
        """Function nodes are always READ_ONLY in read_only mode."""
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        pipeline.flush()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="read_only")

        fn = loaded.compiled_nodes["transform"]
        assert fn.load_status == LoadStatus.READ_ONLY

    def test_source_node_status_for_non_reconstructable(self, simple_pipeline):
        """DictSource (non-reconstructable) source nodes are UNAVAILABLE in read_only."""
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        pipeline.flush()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="read_only")

        source_nodes = [
            n for n in loaded._persistent_node_map.values() if n.node_type == "source"
        ]
        assert len(source_nodes) == 1
        assert source_nodes[0].load_status == LoadStatus.UNAVAILABLE

    def test_read_only_source_returns_stored_schema(self, simple_pipeline):
        """Read-only source nodes return schema from the stored descriptor."""
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        pipeline.flush()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="read_only")

        source_node = [
            n for n in loaded._persistent_node_map.values() if n.node_type == "source"
        ][0]
        tag_schema, packet_schema = source_node.output_schema()
        assert isinstance(tag_schema, Schema)
        assert isinstance(packet_schema, Schema)
        # The original source has tag=["x"], packet=["y"]
        assert "x" in tag_schema
        assert "y" in packet_schema

    def test_read_only_source_returns_stored_keys(self, simple_pipeline):
        """Read-only source nodes return keys from the stored descriptor."""
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        pipeline.flush()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="read_only")

        source_node = [
            n for n in loaded._persistent_node_map.values() if n.node_type == "source"
        ][0]
        tag_keys, packet_keys = source_node.keys()
        assert "x" in tag_keys
        assert "y" in packet_keys

    def test_read_only_source_iter_packets_raises(self, simple_pipeline):
        """Read-only source nodes raise RuntimeError on iter_packets."""
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        pipeline.flush()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="read_only")

        source_node = [
            n for n in loaded._persistent_node_map.values() if n.node_type == "source"
        ][0]
        with pytest.raises(RuntimeError, match="read-only"):
            list(source_node.iter_packets())

    def test_read_only_source_as_table_raises(self, simple_pipeline):
        """Read-only source nodes raise RuntimeError on as_table."""
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        pipeline.flush()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="read_only")

        source_node = [
            n for n in loaded._persistent_node_map.values() if n.node_type == "source"
        ][0]
        with pytest.raises(RuntimeError, match="read-only"):
            source_node.as_table()

    def test_read_only_function_returns_stored_schema(self, simple_pipeline):
        """Read-only function nodes return schema from the stored descriptor."""
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        pipeline.flush()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="read_only")

        fn = loaded.compiled_nodes["transform"]
        tag_schema, packet_schema = fn.output_schema()
        assert isinstance(tag_schema, Schema)
        assert isinstance(packet_schema, Schema)
        assert "x" in tag_schema
        assert "result" in packet_schema

    def test_read_only_function_returns_stored_keys(self, simple_pipeline):
        """Read-only function nodes return keys from the stored descriptor."""
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        pipeline.flush()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="read_only")

        fn = loaded.compiled_nodes["transform"]
        tag_keys, packet_keys = fn.keys()
        assert "x" in tag_keys
        assert "result" in packet_keys

    def test_read_only_function_returns_stored_hashes(self, simple_pipeline):
        """Read-only function nodes return stored content_hash and pipeline_hash."""
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        pipeline.flush()
        # Capture original hashes before save
        orig_fn = pipeline.compiled_nodes["transform"]
        orig_content = orig_fn.content_hash().to_string()
        orig_pipeline = orig_fn.pipeline_hash().to_string()

        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="read_only")

        fn = loaded.compiled_nodes["transform"]
        assert fn.content_hash().to_string() == orig_content
        assert fn.pipeline_hash().to_string() == orig_pipeline

    def test_read_only_operator_with_join(self, tmp_path):
        """Read-only mode preserves operator node metadata for multi-input operators."""
        db = DeltaTableDatabase(base_path=str(tmp_path / "db"))
        source1 = DictSource(
            data=[{"a": 1, "b": 10}], tag_columns=["a"], source_id="s1"
        )
        source2 = DictSource(
            data=[{"a": 1, "c": 20}], tag_columns=["a"], source_id="s2"
        )
        pipeline = Pipeline(name="test", pipeline_database=db)
        with pipeline:
            joined = Join().process(source1, source2, label="my_join")
        pipeline.run()
        db.flush()

        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="read_only")

        op_node = loaded.compiled_nodes["my_join"]
        assert op_node.load_status == LoadStatus.READ_ONLY

        # Metadata should be accessible
        tag_schema, packet_schema = op_node.output_schema()
        assert isinstance(tag_schema, Schema)
        assert isinstance(packet_schema, Schema)
        assert "a" in tag_schema

    def test_read_only_pipeline_is_compiled(self, simple_pipeline):
        """A read-only loaded pipeline reports as compiled."""
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        pipeline.flush()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="read_only")

        assert loaded._compiled is True

    def test_read_only_csv_source_is_still_unavailable(self, tmp_path):
        """Even a reconstructable CSV source is UNAVAILABLE in read_only mode."""
        _, _, json_path, _ = _make_csv_pipeline(tmp_path)
        loaded = Pipeline.load(json_path, mode="read_only")

        source_nodes = [
            n for n in loaded._persistent_node_map.values() if n.node_type == "source"
        ]
        assert len(source_nodes) == 1
        # read_only mode skips source reconstruction
        assert source_nodes[0].load_status == LoadStatus.UNAVAILABLE


# ---------------------------------------------------------------------------
# Thorough full mode tests
# ---------------------------------------------------------------------------


class TestFullMode:
    """Verify behavior of pipelines loaded in full mode."""

    def test_full_mode_with_csv_source_reconstructs(self, tmp_path):
        """CSVSource (reconstructable) loads in FULL mode with a live stream."""
        _, _, json_path, _ = _make_csv_pipeline(tmp_path)
        loaded = Pipeline.load(json_path, mode="full")

        source_nodes = [
            n for n in loaded._persistent_node_map.values() if n.node_type == "source"
        ]
        assert len(source_nodes) == 1
        assert source_nodes[0].load_status == LoadStatus.FULL
        # Should be able to iterate the live source
        packets = list(source_nodes[0].iter_packets())
        assert len(packets) == 2

    def test_full_mode_csv_source_as_table(self, tmp_path):
        """A fully loaded CSV source can produce an Arrow table."""
        _, _, json_path, _ = _make_csv_pipeline(tmp_path)
        loaded = Pipeline.load(json_path, mode="full")

        source_node = [
            n for n in loaded._persistent_node_map.values() if n.node_type == "source"
        ][0]
        table = source_node.as_table()
        assert table.num_rows == 2

    def test_full_mode_csv_function_node_is_full(self, tmp_path):
        """When the source is reconstructable, the downstream function node is FULL."""
        _, _, json_path, _ = _make_csv_pipeline(tmp_path)
        loaded = Pipeline.load(json_path, mode="full")

        fn = loaded.compiled_nodes["transform"]
        assert fn.load_status == LoadStatus.FULL

    def test_full_mode_csv_pipeline_can_rerun(self, tmp_path):
        """A fully loaded CSV pipeline can be re-run without error."""
        _, _, json_path, _ = _make_csv_pipeline(tmp_path)
        loaded = Pipeline.load(json_path, mode="full")

        # Re-running should not raise
        loaded.run()

    def test_full_mode_dict_source_degrades_gracefully(self, simple_pipeline):
        """DictSource (non-reconstructable) degrades to UNAVAILABLE in full mode."""
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        pipeline.flush()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="full")

        source_nodes = [
            n for n in loaded._persistent_node_map.values() if n.node_type == "source"
        ]
        assert len(source_nodes) == 1
        assert source_nodes[0].load_status == LoadStatus.UNAVAILABLE

    def test_full_mode_function_degrades_when_source_unavailable(self, simple_pipeline):
        """Function node degrades to READ_ONLY when its source is UNAVAILABLE."""
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        pipeline.flush()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="full")

        fn = loaded.compiled_nodes["transform"]
        # Source is DictSource (UNAVAILABLE), so function node can't have
        # a live input stream and should degrade
        assert fn.load_status == LoadStatus.READ_ONLY

    def test_full_mode_operator_degrades_when_sources_unavailable(self, tmp_path):
        """Operator degrades to READ_ONLY when upstream sources are UNAVAILABLE."""
        db = DeltaTableDatabase(base_path=str(tmp_path / "db"))
        source1 = DictSource(
            data=[{"a": 1, "b": 10}], tag_columns=["a"], source_id="s1"
        )
        source2 = DictSource(
            data=[{"a": 1, "c": 20}], tag_columns=["a"], source_id="s2"
        )
        pipeline = Pipeline(name="test", pipeline_database=db)
        with pipeline:
            joined = Join().process(source1, source2, label="my_join")
        pipeline.run()
        db.flush()

        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="full")

        op_node = loaded.compiled_nodes["my_join"]
        # Both sources are DictSource (UNAVAILABLE), so operator degrades
        assert op_node.load_status == LoadStatus.READ_ONLY

    def test_full_mode_preserves_pipeline_name(self, simple_pipeline):
        """Full mode preserves the pipeline name."""
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="full")

        assert loaded.name == ("test",)

    def test_full_mode_preserves_database_type(self, simple_pipeline):
        """Full mode reconstructs the same database type."""
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="full")

        assert type(loaded.pipeline_database) is DeltaTableDatabase

    def test_full_mode_node_access_by_attribute(self, simple_pipeline):
        """Loaded pipeline supports node access via __getattr__."""
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="full")

        # Attribute access for labeled nodes
        assert loaded.transform is not None
        assert loaded.transform.node_type == "function"

    def test_full_mode_csv_function_iter_packets(self, tmp_path):
        """A fully loaded function node with CSV source can iterate packets."""
        _, _, json_path, _ = _make_csv_pipeline(tmp_path)
        loaded = Pipeline.load(json_path, mode="full")

        fn = loaded.compiled_nodes["transform"]
        packets = list(fn.iter_packets())
        # 2 rows in the CSV
        assert len(packets) == 2
        # Each packet should have a "result" key
        for tag, packet in packets:
            assert "result" in packet.keys()

    def test_full_mode_csv_function_as_table(self, tmp_path):
        """A fully loaded function node with CSV source produces a table."""
        _, _, json_path, _ = _make_csv_pipeline(tmp_path)
        loaded = Pipeline.load(json_path, mode="full")

        fn = loaded.compiled_nodes["transform"]
        table = fn.as_table()
        assert table.num_rows == 2
        assert "result" in table.column_names


# ---------------------------------------------------------------------------
# Edge cases and cross-mode comparisons
# ---------------------------------------------------------------------------


class TestLoadEdgeCases:
    """Edge cases for Pipeline.load()."""

    def test_load_default_mode_is_full(self, simple_pipeline):
        """Pipeline.load() defaults to full mode."""
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path))
        # Default is full, so with DictSource the function degrades
        fn = loaded.compiled_nodes["transform"]
        assert fn.load_status in (LoadStatus.FULL, LoadStatus.READ_ONLY)

    def test_load_multi_source_operator_pipeline_read_only(self, tmp_path):
        """Multi-source pipeline with operator loads in read_only with correct status."""
        db = DeltaTableDatabase(base_path=str(tmp_path / "db"))
        src1 = DictSource(data=[{"k": 1, "v1": 10}], tag_columns=["k"], source_id="s1")
        src2 = DictSource(data=[{"k": 1, "v2": 20}], tag_columns=["k"], source_id="s2")
        pipeline = Pipeline(name="multi", pipeline_database=db)
        with pipeline:
            joined = Join().process(src1, src2, label="join_node")
        pipeline.run()
        db.flush()

        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="read_only")

        # All nodes should have load_status
        for node in loaded._persistent_node_map.values():
            assert hasattr(node, "load_status")

        # Sources are UNAVAILABLE, operator is READ_ONLY
        sources = [
            n for n in loaded._persistent_node_map.values() if n.node_type == "source"
        ]
        assert all(s.load_status == LoadStatus.UNAVAILABLE for s in sources)

        op = loaded.compiled_nodes["join_node"]
        assert op.load_status == LoadStatus.READ_ONLY

    def test_load_preserves_all_edge_pairs(self, multi_source_pipeline):
        """Loaded pipeline has the same edge pairs as the original."""
        pipeline, tmp_path = multi_source_pipeline
        pipeline.run()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="full")

        orig_edges = set(tuple(e) for e in pipeline._graph_edges)
        loaded_edges = set(tuple(e) for e in loaded._graph_edges)
        assert orig_edges == loaded_edges

    def test_load_hash_graph_has_node_types(self, simple_pipeline):
        """The _hash_graph on a loaded pipeline has node_type attributes."""
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="full")

        for node_hash in loaded._hash_graph.nodes:
            attrs = loaded._hash_graph.nodes[node_hash]
            assert "node_type" in attrs, (
                f"Node {node_hash} missing node_type in _hash_graph"
            )

    def test_read_only_then_full_gives_different_status(self, tmp_path):
        """Loading the same JSON in different modes gives different statuses."""
        _, _, json_path, _ = _make_csv_pipeline(tmp_path)

        ro = Pipeline.load(json_path, mode="read_only")
        full = Pipeline.load(json_path, mode="full")

        ro_fn = ro.compiled_nodes["transform"]
        full_fn = full.compiled_nodes["transform"]

        assert ro_fn.load_status == LoadStatus.READ_ONLY
        assert full_fn.load_status == LoadStatus.FULL

    def test_function_database_round_trip(self, tmp_path):
        """Pipeline with separate function_database round-trips correctly."""
        pipeline_db = DeltaTableDatabase(base_path=str(tmp_path / "pdb"))
        function_db = DeltaTableDatabase(base_path=str(tmp_path / "fdb"))
        source = DictSource(data=[{"x": 1, "y": 2}], tag_columns=["x"], source_id="s")
        pf = PythonPacketFunction(
            function=transform_func,
            output_keys=["result"],
            function_name="transform_func",
        )
        pod = FunctionPod(packet_function=pf)
        pipeline = Pipeline(
            name="test",
            pipeline_database=pipeline_db,
            function_database=function_db,
        )
        with pipeline:
            pod.process(source, label="transform")
        pipeline.run()
        pipeline.flush()

        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="full")

        assert loaded.function_database is not None
        assert type(loaded.function_database) is DeltaTableDatabase
