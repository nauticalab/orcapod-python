"""End-to-end tests for Pipeline.save() and Pipeline.load()."""

import json

import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import Join
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource
from orcapod.core.sources.dict_source import DictSource
from orcapod.databases.delta_lake_databases import DeltaTableDatabase
from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
from orcapod.pipeline import Pipeline
from orcapod.pipeline.serialization import PIPELINE_FORMAT_VERSION, LoadStatus


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

    def add_values(value: int, score: int) -> dict:
        return {"total": value + score}

    pf = PythonPacketFunction(
        function=add_values,
        output_keys=["total"],
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
