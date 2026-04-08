"""End-to-end tests for Pipeline.save() and Pipeline.load()."""

import csv
import json

import pyarrow as pa
import pytest

from orcapod.channels import Channel
from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import Join
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
    src_a = ArrowTableSource(table_a, tag_columns=["key"], source_id="src_a", infer_nullable=True)
    src_b = ArrowTableSource(table_b, tag_columns=["key"], source_id="src_b", infer_nullable=True)

    def add_values(value: int, score: int) -> dict[str, int]:
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
        # New format: databases at top level, pipeline_database is a registry key string
        pipeline_db_key = data["pipeline"]["pipeline_database"]
        assert isinstance(pipeline_db_key, str)
        assert pipeline_db_key in data["databases"]
        assert data["databases"][pipeline_db_key]["type"] == "delta_table"

    def test_save_result_database_null_when_not_set(self, simple_pipeline):
        pipeline, tmp_path = simple_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())
        # New format: result_database is None in pipeline block when not set
        assert data["pipeline"]["result_database"] is None

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

        # node_uri is present at all levels; data_context_key at definition+ (standard default)
        required_fields = {
            "node_type",
            "content_hash",
            "pipeline_hash",
            "node_uri",
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
        # New format: stream_type and source_id are in node_uri, not top-level
        assert "node_uri" in src
        assert src["node_uri"][0] == "dict"  # stream_type
        assert "reconstructable" in src
        # DictSource is in-memory, not reconstructable
        assert src["reconstructable"] is False
        assert "source_config" in src

    def test_save_function_node_fields(self, simple_pipeline):
        """Function node descriptors have function-specific fields."""
        pipeline, tmp_path = simple_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())

        fn_nodes = [n for n in data["nodes"].values() if n["node_type"] == "function"]
        assert len(fn_nodes) == 1
        fn = fn_nodes[0]
        # New format: function_config key (not function_pod); no pipeline_path/result_record_path
        assert "function_config" in fn
        assert "function_pod" not in fn
        assert "pipeline_path" not in fn
        assert "result_record_path" not in fn
        # node_uri present at all levels
        assert "node_uri" in fn

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
        # New format: operator_config (not operator); no pipeline_path
        assert "operator_config" in op
        assert "operator" not in op
        assert "cache_mode" in op
        assert "pipeline_path" not in op
        # node_uri present at all levels
        assert "node_uri" in op

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

    def test_save_with_separate_result_database(self, tmp_path):
        """When a separate result_database is provided, it is serialized."""
        pipeline_db = DeltaTableDatabase(base_path=str(tmp_path / "pipeline_db"))
        result_db = DeltaTableDatabase(base_path=str(tmp_path / "result_db"))

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
            result_database=result_db,
        )
        with pipeline:
            pod.process(source, label="transform")

        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())
        # New format: result_database is a registry key string in pipeline block
        result_db_key = data["pipeline"]["result_database"]
        assert result_db_key is not None
        assert isinstance(result_db_key, str)
        assert result_db_key in data["databases"]
        assert data["databases"][result_db_key]["type"] == "delta_table"

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
        # Loaded pipeline exposes source nodes in compiled_nodes (unlike
        # compile() which excludes them), so compare against persistent
        # node count instead.
        assert len(loaded.compiled_nodes) == len(loaded._persistent_node_map)

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
        # Loaded pipeline also includes source nodes in compiled_nodes
        original_labels = set(pipeline.compiled_nodes.keys())
        loaded_labels = set(loaded.compiled_nodes.keys())
        assert original_labels.issubset(loaded_labels)

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

        # The loaded pipeline includes source nodes; original non-source
        # nodes should all be present.
        assert set(pipeline.compiled_nodes.keys()).issubset(
            set(loaded.compiled_nodes.keys())
        )

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
        """Read-only mode preserves operator node metadata for multi-input operators.

        An uncached Join (cache_mode=OFF) has no records in the database, so it
        resolves to UNAVAILABLE regardless of load mode — stored schema metadata
        is still accessible via output_schema().  (PLT-1158)
        """
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
        # Uncached Join (cache_mode=OFF) → UNAVAILABLE even in read_only mode
        assert op_node.load_status == LoadStatus.UNAVAILABLE

        # Stored metadata is still accessible
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
        """Function node degrades to CACHE_ONLY when its source is UNAVAILABLE.

        The node still has a wired-up proxy pod and DB access so it can
        serve all previously cached results (CACHE_ONLY), rather than being
        completely inert (READ_ONLY was the pre-PLT-1156 incorrect behaviour).
        """
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        pipeline.flush()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="full")

        fn = loaded.compiled_nodes["transform"]
        # Source is DictSource (UNAVAILABLE), function node should be CACHE_ONLY
        # so it can still serve cached results from persistent storage.
        assert fn.load_status == LoadStatus.CACHE_ONLY

    def test_full_mode_operator_degrades_when_sources_unavailable(self, tmp_path):
        """Uncached operator degrades to UNAVAILABLE when upstream sources are UNAVAILABLE.

        An operator without explicit DB caching (cache_mode=OFF) never writes
        records to the database, so even when a pipeline_db exists there is
        nothing to read back.  The correct status is therefore UNAVAILABLE,
        not READ_ONLY.  (PLT-1158)
        """
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
        # Uncached Join (cache_mode=OFF) with UNAVAILABLE sources → UNAVAILABLE
        assert op_node.load_status == LoadStatus.UNAVAILABLE

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


class TestCachedSourceWithSourceProxyRoundTrip:
    """End-to-end: build pipeline with cached DictSource, run, save, load,
    and verify data is recovered from the cache database."""

    def test_cached_dict_source_recovers_data_after_load(self, tmp_path):
        """DictSource wrapped in CachedSource → pipeline run → save → load.

        On load the DictSource becomes a SourceProxy (not reconstructable),
        but the CachedSource should still serve the cached data.
        """
        from orcapod.core.sources import CachedSource, DictSource
        from orcapod.core.sources.source_proxy import SourceProxy

        db = DeltaTableDatabase(base_path=str(tmp_path / "pipeline_db"))
        cache_db = DeltaTableDatabase(base_path=str(tmp_path / "cache_db"))

        source = DictSource(
            data=[{"x": 1, "y": 10}, {"x": 2, "y": 20}],
            tag_columns=["x"],
            source_id="dict_src",
        )
        cached_source = CachedSource(source, cache_database=cache_db)

        pf = PythonPacketFunction(
            function=transform_func,
            output_keys="result",
            function_name="transform_func",
        )
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(name="cached_test", pipeline_database=db)
        with pipeline:
            result = pod.process(cached_source, label="transform")

        pipeline.run()
        pipeline.flush()
        cache_db.flush()

        # Capture original output
        original_table = result.as_table()
        original_rows = original_table.num_rows

        # Save and reload
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="full")

        # The source node should wrap a CachedSource whose inner source is
        # a SourceProxy (DictSource is not reconstructable)
        source_nodes = [
            n for n in loaded._persistent_node_map.values() if n.node_type == "source"
        ]
        assert len(source_nodes) == 1
        loaded_stream = source_nodes[0].stream
        assert isinstance(loaded_stream, CachedSource)
        assert isinstance(loaded_stream._source, SourceProxy)
        assert loaded_stream._source.expected_class_name == "DictSource"

        # The cached source should serve data from the cache
        cached_table = loaded_stream.as_table()
        assert cached_table.num_rows == 2

    def test_cached_dict_source_function_output_matches(self, tmp_path):
        """After load, iterating the function node produces the same results
        as the original pipeline run."""
        from orcapod.core.sources import CachedSource, DictSource

        db = DeltaTableDatabase(base_path=str(tmp_path / "pipeline_db"))
        cache_db = DeltaTableDatabase(base_path=str(tmp_path / "cache_db"))

        source = DictSource(
            data=[{"x": 1, "y": 5}, {"x": 2, "y": 15}],
            tag_columns=["x"],
            source_id="dict_src2",
        )
        cached_source = CachedSource(source, cache_database=cache_db)

        pf = PythonPacketFunction(
            function=transform_func,
            output_keys="result",
            function_name="transform_func",
        )
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(name="cached_e2e", pipeline_database=db)
        with pipeline:
            result = pod.process(cached_source, label="transform")

        pipeline.run()
        pipeline.flush()
        cache_db.flush()

        # Capture original results
        original_packets = [p.as_dict()["result"] for _, p in result.iter_packets()]

        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="full")

        fn_node = loaded.compiled_nodes["transform"]
        assert fn_node.load_status == LoadStatus.FULL

        loaded_packets = [p.as_dict()["result"] for _, p in fn_node.iter_packets()]
        assert sorted(loaded_packets) == sorted(original_packets)


class TestLoadEdgeCases:
    """Edge cases for Pipeline.load()."""

    def test_load_default_mode_is_full(self, simple_pipeline):
        """Pipeline.load() defaults to full mode."""
        pipeline, tmp_path = simple_pipeline
        pipeline.run()
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path))
        # Default is full mode; with DictSource (UNAVAILABLE) the function node
        # degrades to CACHE_ONLY (has DB access but no live stream).
        fn = loaded.compiled_nodes["transform"]
        assert fn.load_status in (LoadStatus.FULL, LoadStatus.READ_ONLY, LoadStatus.CACHE_ONLY)

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

        # Sources are UNAVAILABLE, uncached operator is also UNAVAILABLE (PLT-1158)
        sources = [
            n for n in loaded._persistent_node_map.values() if n.node_type == "source"
        ]
        assert all(s.load_status == LoadStatus.UNAVAILABLE for s in sources)

        op = loaded.compiled_nodes["join_node"]
        assert op.load_status == LoadStatus.UNAVAILABLE

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

    def test_result_database_round_trip(self, tmp_path):
        """Pipeline with separate result_database round-trips correctly."""
        pipeline_db = DeltaTableDatabase(base_path=str(tmp_path / "pdb"))
        result_db = DeltaTableDatabase(base_path=str(tmp_path / "fdb"))
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
            result_database=result_db,
        )
        with pipeline:
            pod.process(source, label="transform")
        pipeline.run()
        pipeline.flush()

        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path), mode="full")

        assert loaded._result_database is not None
        assert type(loaded._result_database) is DeltaTableDatabase


# ---------------------------------------------------------------------------
# Pipeline load with unavailable function (Tasks 9-10)
# ---------------------------------------------------------------------------


def _double_age(age: int) -> tuple[int, int]:
    """A simple function that doubles age and preserves original."""
    return age * 2, age


def _corrupt_function_module_path(save_path):
    """Edit saved pipeline JSON to make function module unimportable."""
    with open(save_path) as f:
        data = json.load(f)
    for node in data["nodes"].values():
        if node.get("node_type") == "function":
            # Support both new key 'function_config' and old key 'function_pod'
            fn_cfg = node.get("function_config") or node.get("function_pod")
            if fn_cfg:
                pf_config = fn_cfg["packet_function"]["config"]
                pf_config["module_path"] = "nonexistent.module.that.does.not.exist"
    with open(save_path, "w") as f:
        json.dump(data, f)


class TestPipelineLoadWithUnavailableFunction:
    """Tests for loading a pipeline when the function cannot be imported."""

    @staticmethod
    def _write_csv(path, rows):
        """Write a list of dicts as a CSV file."""
        fieldnames = list(rows[0].keys())
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    def _build_and_save_pipeline(self, tmp_path):
        """Build pipeline with CSV source and function pod, run it, save to JSON.

        Uses CSVSource (reconstructable) so the source survives load and the
        function node enters the proxy path rather than the pure read-only path.

        Returns:
            Tuple of (save_path, db_path).
        """
        csv_path = str(tmp_path / "data.csv")
        self._write_csv(
            csv_path,
            [{"name": "alice", "age": "30"}, {"name": "bob", "age": "25"}],
        )

        db_path = str(tmp_path / "db")
        db = DeltaTableDatabase(base_path=db_path)
        source = CSVSource(
            file_path=csv_path,
            tag_columns=["name"],
            source_id="people",
        )
        pf = PythonPacketFunction(
            function=_double_age,
            output_keys=["doubled_age", "original_age"],
            function_name="_double_age",
        )
        pod = FunctionPod(packet_function=pf)
        pipeline = Pipeline(name="proxy_test", pipeline_database=db)
        with pipeline:
            pod.process(source, label="transform")
        pipeline.run()
        db.flush()

        save_path = str(tmp_path / "pipeline.json")
        pipeline.save(save_path)
        return save_path, db_path

    def _build_and_save_pipeline_with_operator(self, tmp_path):
        """Build pipeline: CSV source -> function_pod -> SelectPacketColumns.

        Returns:
            Tuple of (save_path, db_path).
        """
        from orcapod.core.operators import SelectPacketColumns

        csv_path = str(tmp_path / "data.csv")
        self._write_csv(
            csv_path,
            [{"name": "alice", "age": "30"}, {"name": "bob", "age": "25"}],
        )

        db_path = str(tmp_path / "db")
        db = DeltaTableDatabase(base_path=db_path)
        source = CSVSource(
            file_path=csv_path,
            tag_columns=["name"],
            source_id="people",
        )
        pf = PythonPacketFunction(
            function=_double_age,
            output_keys=["doubled_age", "original_age"],
            function_name="_double_age",
        )
        pod = FunctionPod(packet_function=pf)
        select_op = SelectPacketColumns(columns=["doubled_age"])

        pipeline = Pipeline(name="proxy_op_test", pipeline_database=db)
        with pipeline:
            fn_result = pod.process(source, label="transform")
            select_op.process(fn_result, label="select_col")
        pipeline.run()
        db.flush()

        save_path = str(tmp_path / "pipeline.json")
        pipeline.save(save_path)
        return save_path, db_path

    # -- Task 9 tests --

    def test_load_with_unavailable_function_has_read_only_status(self, tmp_path):
        """Loading a pipeline with corrupted module_path yields READ_ONLY function node."""
        save_path, _ = self._build_and_save_pipeline(tmp_path)
        _corrupt_function_module_path(save_path)

        loaded = Pipeline.load(save_path, mode="full")

        fn_nodes = [
            n
            for n in loaded.compiled_nodes.values()
            if n.node_type == "function"
        ]
        assert len(fn_nodes) == 1
        assert fn_nodes[0].load_status == LoadStatus.READ_ONLY

    def test_load_with_unavailable_function_can_get_all_records(self, tmp_path):
        """A READ_ONLY function node with proxy can retrieve all cached records."""
        save_path, _ = self._build_and_save_pipeline(tmp_path)
        _corrupt_function_module_path(save_path)

        loaded = Pipeline.load(save_path, mode="full")
        fn_node = loaded.compiled_nodes["transform"]

        records = fn_node.get_all_records()
        assert records is not None
        assert records.num_rows == 2

    def test_load_with_unavailable_function_iter_packets_yields_cached(self, tmp_path):
        """A READ_ONLY function node with proxy yields cached packets via iter_packets."""
        save_path, _ = self._build_and_save_pipeline(tmp_path)
        _corrupt_function_module_path(save_path)

        loaded = Pipeline.load(save_path, mode="full")
        fn_node = loaded.compiled_nodes["transform"]

        packets = list(fn_node.iter_packets())
        assert len(packets) == 2

        # Verify actual data values
        doubled_ages = sorted(p.as_dict()["doubled_age"] for _, p in packets)
        assert doubled_ages == [50, 60]

        original_ages = sorted(p.as_dict()["original_age"] for _, p in packets)
        assert original_ages == [25, 30]

    # -- Task 10 test --

    def test_downstream_operator_computes_from_cached(self, tmp_path):
        """Operator downstream of READ_ONLY function node can compute from cached data."""
        save_path, _ = self._build_and_save_pipeline_with_operator(tmp_path)
        _corrupt_function_module_path(save_path)

        loaded = Pipeline.load(save_path, mode="full")

        # Function node should be READ_ONLY (proxy)
        fn_node = loaded.compiled_nodes["transform"]
        assert fn_node.load_status == LoadStatus.READ_ONLY

        # Operator node should be FULL (it can reconstruct from cached upstream)
        op_node = loaded.compiled_nodes["select_col"]
        assert op_node.load_status == LoadStatus.FULL

        # Operator should produce correct results; must call run() explicitly
        # (as_table() is now read-only per PLT-1182 — it never triggers computation)
        op_node.run()
        table = op_node.as_table()
        assert table.num_rows == 2
        assert "doubled_age" in table.column_names
        # original_age should have been dropped by SelectPacketColumns
        assert "original_age" not in table.column_names

        doubled_ages = sorted(table.column("doubled_age").to_pylist())
        assert doubled_ages == [50, 60]


# ---------------------------------------------------------------------------
# PLT-1156: CACHE_ONLY mode — function node with unavailable upstream source
# ---------------------------------------------------------------------------


class TestCacheOnlyMode:
    """FunctionNode with an UNAVAILABLE upstream serves all cached results."""

    def _build_run_save(self, tmp_path, function, source_data=None):
        """Helper: build pipeline, run it, flush DB, save descriptor.

        Returns (json_path, original_results) where original_results is a
        sorted list of output dicts from the original run.
        """
        if source_data is None:
            source_data = [{"x": 1, "y": 2}, {"x": 3, "y": 4}]

        db = DeltaTableDatabase(base_path=str(tmp_path / "db"))
        source = DictSource(
            data=source_data,
            tag_columns=["x"],
            source_id="test_source",
        )
        pf = PythonPacketFunction(
            function=function,
            output_keys=["result"],
            function_name=function.__name__,
        )
        pod = FunctionPod(packet_function=pf)
        pipeline = Pipeline(name="test", pipeline_database=db)
        with pipeline:
            pod.process(source, label="transform")
        pipeline.run()
        db.flush()

        # Capture original results for later comparison
        fn_node = pipeline.compiled_nodes["transform"]
        original = sorted(
            [{"x": t.as_dict()["x"], "result": p.as_dict()["result"]}
             for t, p in fn_node.iter_packets()],
            key=lambda r: r["x"],
        )

        json_path = str(tmp_path / "pipeline.json")
        pipeline.save(json_path)
        return json_path, original

    def test_function_node_gets_cache_only_status_when_source_unavailable(
        self, tmp_path
    ):
        """Loading a pipeline whose source is UNAVAILABLE yields CACHE_ONLY status."""
        json_path, _ = self._build_run_save(tmp_path, transform_func)
        loaded = Pipeline.load(json_path, mode="full")

        fn = loaded.compiled_nodes["transform"]
        assert fn.load_status == LoadStatus.CACHE_ONLY

    def test_cache_only_mode_iter_packets_returns_all_cached_data(self, tmp_path):
        """iter_packets() on a CACHE_ONLY node returns all previously cached results."""
        json_path, original = self._build_run_save(tmp_path, transform_func)
        loaded = Pipeline.load(json_path, mode="full")

        fn = loaded.compiled_nodes["transform"]
        assert fn.load_status == LoadStatus.CACHE_ONLY

        packets = list(fn.iter_packets())
        assert len(packets) == len(original)

        recovered = sorted(
            [{"x": t.as_dict()["x"], "result": p.as_dict()["result"]}
             for t, p in packets],
            key=lambda r: r["x"],
        )
        assert recovered == original

    def test_cache_only_mode_with_transient_function_yields_cached_data(
        self, tmp_path
    ):
        """Transient (locally-scoped) function + UNAVAILABLE source → CACHE_ONLY,
        and iter_packets() still returns all cached results via the DB.
        """
        # Define the function locally so it cannot be resolved by module path
        # on reload (simulating a transient function → PacketFunctionProxy).
        def local_double(y: int) -> int:
            return y * 2

        json_path, original = self._build_run_save(tmp_path, local_double)
        loaded = Pipeline.load(json_path, mode="full")

        fn = loaded.compiled_nodes["transform"]
        assert fn.load_status == LoadStatus.CACHE_ONLY

        packets = list(fn.iter_packets())
        assert len(packets) == len(original)

        recovered = sorted(
            [{"x": t.as_dict()["x"], "result": p.as_dict()["result"]}
             for t, p in packets],
            key=lambda r: r["x"],
        )
        assert recovered == original

    def test_cache_only_data_context_does_not_raise(self, tmp_path):
        """data_context is accessible on a CACHE_ONLY function node."""
        json_path, _ = self._build_run_save(tmp_path, transform_func)
        loaded = Pipeline.load(json_path, mode="full")

        fn = loaded.compiled_nodes["transform"]
        assert fn.load_status == LoadStatus.CACHE_ONLY
        # Should not raise AttributeError
        _ = fn.data_context

    def test_live_stream_does_not_trigger_cache_only_mode(self, tmp_path):
        """A reconstructable live source produces FULL status, not CACHE_ONLY.

        Guards against CACHE_ONLY being incorrectly triggered for sources that
        CAN be reconstructed (i.e. are not UNAVAILABLE).
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
        pipeline = Pipeline(name="test", pipeline_database=db)
        with pipeline:
            pod.process(source, label="transform")
        pipeline.run()
        db.flush()
        pipeline.save(str(tmp_path / "pipeline.json"))

        loaded = Pipeline.load(str(tmp_path / "pipeline.json"), mode="full")
        fn = loaded.compiled_nodes["transform"]

        # Source is reconstructable (CSV), upstream is FULL → function node FULL
        assert fn.load_status == LoadStatus.FULL
        # CACHE_ONLY must NOT be set for a live stream
        assert fn.load_status != LoadStatus.CACHE_ONLY

    @pytest.mark.asyncio
    async def test_cache_only_mode_async_execute_yields_cached_data(self, tmp_path):
        """async_execute on a CACHE_ONLY node streams all cached results to output."""
        json_path, original = self._build_run_save(tmp_path, transform_func)
        loaded = Pipeline.load(json_path, mode="full")

        fn = loaded.compiled_nodes["transform"]
        assert fn.load_status == LoadStatus.CACHE_ONLY

        # In CACHE_ONLY mode the input channel is empty (source is unavailable)
        input_ch = Channel(buffer_size=16)
        await input_ch.writer.close()
        output_ch = Channel(buffer_size=16)

        await fn.async_execute(input_ch.reader, output_ch.writer)
        rows = await output_ch.reader.collect()

        assert len(rows) == len(original)
        recovered = sorted(
            [{"x": t.as_dict()["x"], "result": p.as_dict()["result"]}
             for t, p in rows],
            key=lambda r: r["x"],
        )
        assert recovered == original


# ---------------------------------------------------------------------------
# PLT-1158: Uncached operator with UNAVAILABLE sources must not be READ_ONLY
# ---------------------------------------------------------------------------


def _adder(x: int, y: int, z: int) -> int:
    """Simple adder used in the PLT-1158 reproduction pipeline."""
    return x + y + z


class TestPLT1158UncachedOperatorStatus:
    """Regression tests for PLT-1158.

    An implicit operator node (e.g. the join between two source nodes) that
    has no explicit database caching (cache_mode=OFF) must resolve to
    LoadStatus.UNAVAILABLE when its parent sources cannot be reconstructed.
    Previously it incorrectly resolved to READ_ONLY, which caused downstream
    function nodes to skip CACHE_ONLY mode and attempt computation — failing
    because the operator had no live data stream.
    """

    def _build_pipeline(self, tmp_path):
        """Build and save the pipeline from the PLT-1158 issue example.

        Two DictSources are joined (implicit, uncached Join) and passed to
        a function pod that sums three fields.  The pipeline is run, flushed,
        and saved to disk before returning the saved path and the expected
        results.
        """
        db = DeltaTableDatabase(base_path=str(tmp_path / "db"))

        data1 = DictSource(
            [
                {"id": 1, "x": 10, "y": 20},
                {"id": 2, "x": 30, "y": 40},
                {"id": 3, "x": 60, "y": 10},
            ],
            tag_columns=["id"],
            source_id="data1",
        )
        data2 = DictSource(
            [
                {"id": 2, "z": 30},
                {"id": 3, "z": 50},
            ],
            tag_columns=["id"],
            source_id="data2",
        )

        pf = PythonPacketFunction(
            function=_adder,
            output_keys=["result"],
            function_name="_adder",
        )
        pod = FunctionPod(packet_function=pf)

        pipeline = Pipeline(name="my_sample_pipeline", pipeline_database=db)
        with pipeline:
            pod.process(data1, data2, label="adder")

        pipeline.run()
        db.flush()

        json_path = str(tmp_path / "pipeline.json")
        pipeline.save(json_path)

        # Collect expected results for later comparison
        adder_node = pipeline.compiled_nodes["adder"]
        expected = sorted(
            [p.as_dict()["result"] for _, p in adder_node.iter_packets()]
        )
        return json_path, expected

    # ------------------------------------------------------------------
    # Status propagation
    # ------------------------------------------------------------------

    def test_uncached_operator_is_unavailable_not_read_only(self, tmp_path):
        """Uncached join resolves to UNAVAILABLE (not READ_ONLY) after load.

        This is the core regression guard for PLT-1158: previously the join
        got READ_ONLY because a pipeline_db existed, even though no records
        were ever written to it (cache_mode=OFF).
        """
        json_path, _ = self._build_pipeline(tmp_path)
        loaded = Pipeline.load(json_path, mode="full")

        # The implicit join operator has no DB caching → UNAVAILABLE.
        # Find operator nodes via node type (the join is upstream of the function node).
        op_nodes = [
            n for n in loaded._persistent_node_map.values()
            if n.node_type == "operator"
        ]
        assert len(op_nodes) >= 1
        for op in op_nodes:
            assert op.load_status == LoadStatus.UNAVAILABLE, (
                f"Expected UNAVAILABLE for uncached operator, got {op.load_status}"
            )

    def test_source_nodes_are_unavailable(self, tmp_path):
        """Source nodes (DictSource) are UNAVAILABLE after load — pre-condition."""
        json_path, _ = self._build_pipeline(tmp_path)
        loaded = Pipeline.load(json_path, mode="full")

        source_nodes = [
            n for n in loaded._persistent_node_map.values()
            if n.node_type == "source"
        ]
        assert len(source_nodes) >= 1
        for src in source_nodes:
            assert src.load_status == LoadStatus.UNAVAILABLE

    def test_function_node_gets_cache_only_when_operator_is_unavailable(
        self, tmp_path
    ):
        """Function node downstream of an UNAVAILABLE operator gets CACHE_ONLY.

        When the operator is correctly UNAVAILABLE, _load_function_node enters
        the UNAVAILABLE branch and wires up a proxy pod in CACHE_ONLY mode,
        allowing the function node to serve all previously cached results.
        """
        json_path, _ = self._build_pipeline(tmp_path)
        loaded = Pipeline.load(json_path, mode="full")

        fn = loaded.compiled_nodes["adder"]
        assert fn.load_status == LoadStatus.CACHE_ONLY

    def test_cache_only_function_node_serves_cached_results(self, tmp_path):
        """CACHE_ONLY function node returns the same results as the original run.

        This is the end-to-end regression guard from the PLT-1158 issue: in a
        fresh session the pipeline is loaded, the adder function node (backed
        by a DB cache) serves all previously computed results without touching
        the unavailable operator or sources.
        """
        json_path, expected = self._build_pipeline(tmp_path)
        loaded = Pipeline.load(json_path, mode="full")

        fn = loaded.compiled_nodes["adder"]
        assert fn.load_status == LoadStatus.CACHE_ONLY

        # Must not raise; must return same values as the original run
        recovered = sorted(
            [p.as_dict()["result"] for _, p in fn.iter_packets()]
        )
        assert recovered == expected

    def test_read_only_mode_uncached_operator_is_also_unavailable(self, tmp_path):
        """In read_only mode, an uncached operator is still UNAVAILABLE.

        The UNAVAILABLE status for cache_mode=OFF operators is not mode-specific:
        the operator never wrote records to the DB, so READ_ONLY would be
        misleading regardless of the load mode.
        """
        json_path, _ = self._build_pipeline(tmp_path)
        loaded = Pipeline.load(json_path, mode="read_only")

        op_nodes = [
            n for n in loaded._persistent_node_map.values()
            if n.node_type == "operator"
        ]
        assert len(op_nodes) >= 1
        for op in op_nodes:
            assert op.load_status == LoadStatus.UNAVAILABLE


def test_function_node_pipeline_path_two_level(tmp_path):
    """node_identity_path must end with schema:... component (pipeline_hash scope)."""
    db = InMemoryArrowDatabase()

    def add_one(y: int) -> int:
        return y + 1

    pf = PythonPacketFunction(
        function=add_one,
        output_keys="result",
        function_name="add_one",
    )
    pod = FunctionPod(packet_function=pf)
    source = DictSource(
        data=[{"x": 1, "y": 2}, {"x": 3, "y": 4}],
        tag_columns=["x"],
        source_id="test_src",
    )

    pipeline = Pipeline(name="test", pipeline_database=db)
    with pipeline:
        pod.process(source, label="fn")
    pipeline.compile()

    fn_node = pipeline._nodes["fn"]
    path = fn_node.node_identity_path

    assert path[-1].startswith("schema:"), f"Expected schema:... got {path[-1]!r}"
    assert fn_node.pipeline_hash().to_string() in path[-1]
    assert not any(seg.startswith("instance:") for seg in path)


def test_operator_node_pipeline_path_two_level(tmp_path):
    """OperatorNode node_identity_path must end with schema:... component (pipeline_hash scope)."""
    db = InMemoryArrowDatabase()

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
    src_a = ArrowTableSource(table_a, tag_columns=["key"], source_id="src_a", infer_nullable=True)
    src_b = ArrowTableSource(table_b, tag_columns=["key"], source_id="src_b", infer_nullable=True)
    join = Join()

    pipeline = Pipeline(name="test", pipeline_database=db)
    with pipeline:
        join.process(src_a, src_b, label="joined")
    pipeline.compile()

    joined_node = pipeline._nodes["joined"]
    path = joined_node.node_identity_path

    assert path[-1].startswith("schema:"), f"Expected schema:... got {path[-1]!r}"
    assert joined_node.pipeline_hash().to_string() in path[-1]
    assert not any(seg.startswith("instance:") for seg in path)


# ---------------------------------------------------------------------------
# Task 6: New save(level=) format tests
# ---------------------------------------------------------------------------


def test_save_level_field_present(simple_pipeline, tmp_path):
    """Saved file must include a 'level' field."""
    pipeline, _ = simple_pipeline
    path = tmp_path / "p.json"
    pipeline.save(str(path))
    with open(path) as f:
        data = json.load(f)
    assert "level" in data
    assert data["level"] == "standard"


def _make_simple_pipeline_for_level_tests(tmp_path):
    """Build a simple pipeline (DictSource -> FunctionPod) for level tests."""
    db = InMemoryArrowDatabase()

    pf = PythonPacketFunction(
        function=transform_func,
        output_keys="result",
        function_name="transform_func",
    )
    pod = FunctionPod(packet_function=pf)
    source = DictSource(data=[{"x": 1, "y": 2}], tag_columns=["x"], source_id="s")
    pipeline = Pipeline(name="p", pipeline_database=db)
    with pipeline:
        pod.process(source, label="fn")
    return pipeline


def test_save_minimal_level_structure(tmp_path):
    """minimal level: no databases block, node_uri present, no configs."""
    pipeline = _make_simple_pipeline_for_level_tests(tmp_path)

    path = tmp_path / "minimal.json"
    pipeline.save(str(path), level="minimal")
    with open(path) as f:
        data = json.load(f)

    assert data["level"] == "minimal"
    assert "databases" not in data
    assert "pipeline_database" not in data.get("pipeline", {})
    for node in data["nodes"].values():
        assert "node_uri" in node
        assert "content_hash" in node
        assert "pipeline_hash" in node
        assert "output_schema" in node
        assert "data_context_key" not in node
        assert "function_config" not in node
        assert "source_config" not in node
        assert "pipeline_path" not in node
        assert "result_record_path" not in node


def test_save_standard_level_top_level_databases(tmp_path):
    """standard level: databases at top level, pipeline has string keys."""
    pipeline = _make_simple_pipeline_for_level_tests(tmp_path)

    path = tmp_path / "standard.json"
    pipeline.save(str(path), level="standard")
    with open(path) as f:
        data = json.load(f)

    assert "databases" in data
    assert isinstance(data["databases"], dict)
    pipeline_db_key = data["pipeline"]["pipeline_database"]
    assert isinstance(pipeline_db_key, str)
    assert pipeline_db_key in data["databases"]


def test_save_never_stores_pipeline_path(tmp_path):
    """pipeline_path and result_record_path must not appear in any level."""
    pipeline = _make_simple_pipeline_for_level_tests(tmp_path)

    for level in ("minimal", "definition", "standard"):
        path = tmp_path / f"{level}.json"
        pipeline.save(str(path), level=level)
        raw = path.read_text()
        assert "pipeline_path" not in raw, f"pipeline_path leaked at {level}"
        assert "result_record_path" not in raw, f"result_record_path leaked at {level}"


def test_save_definition_level_has_configs_no_pipeline_db(tmp_path):
    """definition level: function_config present, no pipeline_database key."""
    pipeline = _make_simple_pipeline_for_level_tests(tmp_path)

    path = tmp_path / "definition.json"
    pipeline.save(str(path), level="definition")
    with open(path) as f:
        data = json.load(f)

    assert data["level"] == "definition"
    assert "pipeline_database" not in data.get("pipeline", {})
    fn_node = next(v for v in data["nodes"].values() if v["node_type"] == "function")
    assert "function_config" in fn_node
    assert "cache_mode" not in fn_node  # cache_mode only at standard+


def test_save_uses_function_config_not_function_pod(tmp_path):
    """Key must be 'function_config', not 'function_pod'."""
    pipeline = _make_simple_pipeline_for_level_tests(tmp_path)

    path = tmp_path / "p.json"
    pipeline.save(str(path))
    with open(path) as f:
        data = json.load(f)

    fn_node = next(v for v in data["nodes"].values() if v["node_type"] == "function")
    assert "function_config" in fn_node
    assert "function_pod" not in fn_node


def test_save_full_level_equivalent_to_standard(tmp_path):
    """full level saves without error and includes level field."""
    pipeline = _make_simple_pipeline_for_level_tests(tmp_path)

    path = tmp_path / "full.json"
    pipeline.save(str(path), level="full")
    with open(path) as f:
        data = json.load(f)
    assert data["level"] == "full"
    assert "databases" in data


# ---------------------------------------------------------------------------
# Copilot review fixes (PR #120)
# ---------------------------------------------------------------------------


def test_load_minimal_level_raises_clear_error(tmp_path):
    """Loading a 'minimal'-level save raises a clear ValueError mentioning 'minimal'.

    Minimal saves contain topology and identity only — not enough to reconstruct.
    """
    pipeline = _make_simple_pipeline_for_level_tests(tmp_path)
    path = tmp_path / "minimal.json"
    pipeline.save(str(path), level="minimal")

    with pytest.raises(ValueError, match="minimal"):
        Pipeline.load(str(path))


def test_load_operator_node_identity_path_has_schema_instance_components(tmp_path):
    """Operator loaded from a new-format save must have a valid node_identity_path.

    In the new format, pipeline_path is never stored in node descriptors.
    The pipeline name prefix lives in the database path, not in node_identity_path.
    With pipeline_hash scope (default), the node_identity_path ends with schema:... only.
    """
    from orcapod.core.operators import SelectPacketColumns

    csv_path = str(tmp_path / "data.csv")
    _write_csv(csv_path, [{"name": "alice", "y": 2}, {"name": "bob", "y": 4}])

    db = DeltaTableDatabase(base_path=str(tmp_path / "db"))
    source = CSVSource(file_path=csv_path, tag_columns=["name"], source_id="people")
    pf = PythonPacketFunction(
        function=transform_func,
        output_keys="result",
        function_name="transform_func",
    )
    pod = FunctionPod(packet_function=pf)
    select_op = SelectPacketColumns(columns=["result"])

    pipeline = Pipeline(name="prefixtest", pipeline_database=db)
    with pipeline:
        fn_result = pod.process(source, label="fn")
        select_op.process(fn_result, label="sel")
    pipeline.run()

    path = tmp_path / "pipeline.json"
    pipeline.save(str(path))
    loaded = Pipeline.load(str(path), mode="full")

    sel_node = loaded.compiled_nodes["sel"]
    pp = sel_node.node_identity_path
    # The node_identity_path must be non-empty ending with schema:... (pipeline_hash scope).
    # Pipeline name prefix is now encoded in the database path, not the identity path.
    assert len(pp) >= 1, f"Expected non-empty node_identity_path, got {pp!r}"
    assert pp[-1].startswith("schema:"), f"Expected schema:... got {pp[-1]!r}"
    assert not any(seg.startswith("instance:") for seg in pp)


def test_load_raises_on_missing_result_database_registry_key(tmp_path):
    """Loading raises ValueError when result_database key is not in the databases registry.

    A non-null result_database key that cannot be resolved from the registry must
    raise a clear ValueError rather than silently using None and mis-scoping function
    records (or crashing later with a confusing error).
    """
    pipeline_db = DeltaTableDatabase(base_path=str(tmp_path / "pdb"))
    result_db = DeltaTableDatabase(base_path=str(tmp_path / "fdb"))
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
        result_database=result_db,
    )
    with pipeline:
        pod.process(source, label="transform")

    path = tmp_path / "pipeline.json"
    pipeline.save(str(path))

    # Corrupt: replace the result_database key with one that doesn't exist
    with open(path) as f:
        data = json.load(f)
    data["pipeline"]["result_database"] = "db_nonexistent"
    with open(path, "w") as f:
        json.dump(data, f)

    with pytest.raises(ValueError, match="db_nonexistent"):
        Pipeline.load(str(path))


def test_load_function_node_identity_path_has_schema_instance_components(tmp_path):
    """FunctionNode loaded in full mode must have a valid node_identity_path.

    The node_identity_path is computed from the live pod (schema + content hash),
    not from any stored pipeline_path in the descriptor. Even when a legacy
    pipeline_path key is present in a descriptor (old-format saves), full-mode
    loading computes node_identity_path from the pod directly.
    """
    csv_path = str(tmp_path / "data.csv")
    _write_csv(csv_path, [{"name": "alice", "y": 2}, {"name": "bob", "y": 4}])

    db = DeltaTableDatabase(base_path=str(tmp_path / "db"))
    source = CSVSource(file_path=csv_path, tag_columns=["name"], source_id="people")
    pf = PythonPacketFunction(
        function=transform_func,
        output_keys="result",
        function_name="transform_func",
    )
    pod = FunctionPod(packet_function=pf)

    pipeline = Pipeline(name="prefixtest", pipeline_database=db)
    with pipeline:
        pod.process(source, label="fn")
    pipeline.run()

    path = tmp_path / "pipeline.json"
    pipeline.save(str(path))

    # Inject a legacy old-format pipeline_path into the descriptor to simulate
    # an old-format save. In full mode, this stored value must be ignored in
    # favour of the pod-derived identity path.
    with open(path) as f:
        data = json.load(f)
    fn_descriptor = next(v for v in data["nodes"].values() if v["node_type"] == "function")
    node_uri = fn_descriptor.get("node_uri", ["transform_func"])
    fn_descriptor["pipeline_path"] = ["prefixtest"] + list(node_uri) + ["node:oldhash"]
    with open(path, "w") as f:
        json.dump(data, f)

    loaded = Pipeline.load(str(path), mode="full")
    fn_node = loaded.compiled_nodes["fn"]
    pp = fn_node.node_identity_path
    # In full mode, node_identity_path is computed from the pod (schema only, pipeline_hash scope)
    assert len(pp) >= 1, f"Expected non-empty node_identity_path, got {pp!r}"
    assert pp[-1].startswith("schema:"), f"Expected schema:... got {pp[-1]!r}"
    assert not any(seg.startswith("instance:") for seg in pp)


# ---------------------------------------------------------------------------
# eywalker review fixes (PR #120)
# ---------------------------------------------------------------------------


def test_load_definition_level_without_db_succeeds(tmp_path):
    """Definition-level save can be loaded without providing a database.

    Previously raised ValueError unconditionally for definition level.
    The node ends up in a non-FULL status (UNAVAILABLE or CACHE_ONLY) since
    no live data is accessible without a database.
    """
    pipeline = _make_simple_pipeline_for_level_tests(tmp_path)
    path = tmp_path / "definition.json"
    pipeline.save(str(path), level="definition")

    loaded = Pipeline.load(str(path))  # no pipeline_database provided
    fn_node = loaded.compiled_nodes["fn"]
    assert fn_node.load_status != LoadStatus.FULL


def test_load_definition_level_with_pipeline_database_arg(tmp_path):
    """Definition-level save loaded with pipeline_database arg attaches DB.

    Nodes should get CACHE_ONLY or similar status when DB is provided
    but no data has been run yet (no cached records).
    """
    pipeline = _make_simple_pipeline_for_level_tests(tmp_path)
    path = tmp_path / "definition.json"
    pipeline.save(str(path), level="definition")

    db = InMemoryArrowDatabase()
    loaded = Pipeline.load(str(path), pipeline_database=db)
    fn_node = loaded.compiled_nodes["fn"]
    # With DB provided the node should not be UNAVAILABLE
    assert fn_node.load_status != LoadStatus.UNAVAILABLE


def test_load_old_format_with_pipeline_databases_in_pipeline_key_raises(tmp_path):
    """Old format (pipeline.databases dict) raises ValueError — not supported.

    New format uses a top-level 'databases' registry + pipeline.pipeline_database key.
    Loading an old-format file must raise rather than silently mis-parse.
    """
    pipeline = _make_simple_pipeline_for_level_tests(tmp_path)
    path = tmp_path / "standard.json"
    pipeline.save(str(path))

    with open(path) as f:
        data = json.load(f)

    # Rewrite to old format: move databases into pipeline.databases dict
    db_registry = data.pop("databases", {})
    pl = data["pipeline"]
    pipeline_db_key = pl.pop("pipeline_database", None)
    if pipeline_db_key and pipeline_db_key in db_registry:
        pl["databases"] = {"pipeline_database": db_registry[pipeline_db_key]}
    with open(path, "w") as f:
        json.dump(data, f)

    with pytest.raises(ValueError, match="databases"):
        Pipeline.load(str(path))


def test_load_function_node_with_old_function_pod_key_is_not_reconstructed(tmp_path):
    """function_pod key in node descriptor is no longer recognized as function config.

    A descriptor with only the old 'function_pod' key (not 'function_config') should
    result in a node that cannot reconstruct its function pod, falling to unavailable.
    Uses a CSVSource (reconstructable) so the upstream reaches FULL status, isolating
    the function-pod reconstruction as the only failure point.
    """
    csv_path = str(tmp_path / "data.csv")
    _write_csv(csv_path, [{"name": "alice", "y": 2}, {"name": "bob", "y": 4}])
    db = DeltaTableDatabase(base_path=str(tmp_path / "db"))
    source = CSVSource(file_path=csv_path, tag_columns=["name"], source_id="people")
    pf = PythonPacketFunction(
        function=transform_func, output_keys="result", function_name="transform_func"
    )
    pod = FunctionPod(packet_function=pf)
    pipeline = Pipeline(name="fntest", pipeline_database=db)
    with pipeline:
        pod.process(source, label="fn")
    pipeline.run()

    path = tmp_path / "p.json"
    pipeline.save(str(path))

    with open(path) as f:
        data = json.load(f)

    # Rename function_config → function_pod (old key)
    for descriptor in data["nodes"].values():
        if "function_config" in descriptor:
            descriptor["function_pod"] = descriptor.pop("function_config")

    with open(path, "w") as f:
        json.dump(data, f)

    loaded = Pipeline.load(str(path), mode="full")
    fn_node = loaded.compiled_nodes["fn"]
    # Cannot reconstruct function pod → should NOT be FULL (no live function)
    assert fn_node.load_status != LoadStatus.FULL


def test_load_operator_node_with_old_operator_key_is_not_reconstructed(tmp_path):
    """operator key in node descriptor is no longer recognized as operator config.

    A descriptor with only the old 'operator' key (not 'operator_config') should
    result in an operator node that cannot be reconstructed, falling to read-only.
    """
    from orcapod.core.operators import SelectPacketColumns

    csv_path = str(tmp_path / "data.csv")
    _write_csv(csv_path, [{"name": "alice", "y": 2}, {"name": "bob", "y": 4}])
    db = DeltaTableDatabase(base_path=str(tmp_path / "db"))
    source = CSVSource(file_path=csv_path, tag_columns=["name"], source_id="people")
    pf = PythonPacketFunction(
        function=transform_func, output_keys="result", function_name="transform_func"
    )
    pod = FunctionPod(packet_function=pf)
    select_op = SelectPacketColumns(columns=["result"])
    pipeline = Pipeline(name="q", pipeline_database=db)
    with pipeline:
        fn_result = pod.process(source, label="fn")
        select_op.process(fn_result, label="sel")
    pipeline.run()

    path = tmp_path / "p.json"
    pipeline.save(str(path))

    with open(path) as f:
        data = json.load(f)

    # Rename operator_config → operator (old key)
    for descriptor in data["nodes"].values():
        if "operator_config" in descriptor:
            descriptor["operator"] = descriptor.pop("operator_config")

    with open(path, "w") as f:
        json.dump(data, f)

    loaded = Pipeline.load(str(path), mode="full")
    sel_node = loaded.compiled_nodes["sel"]
    # Cannot reconstruct operator → should NOT be FULL
    assert sel_node.load_status != LoadStatus.FULL


def test_source_proxy_from_config_raises_without_any_identity_fields(tmp_path):
    """_source_proxy_from_config raises when neither node_descriptor nor
    config provides identity fields.

    Top-level nodes supply identity via node_descriptor (new format).
    Inner sources (e.g. inside CachedSource) embed identity via _identity_config().
    A bare config with no identity anywhere must raise a clear ValueError.
    """
    from orcapod.pipeline.serialization import _source_proxy_from_config

    # Bare config: no identity fields anywhere
    bare_config = {
        "source_type": "dict",
        "source_id": "s1",
        # No content_hash, no pipeline_hash, no schema fields
    }

    with pytest.raises(ValueError, match="identity fields"):
        _source_proxy_from_config(bare_config, node_descriptor=None)


# ---------------------------------------------------------------------------
# brian-arnold review: save→load→run round-trip integration tests
# ---------------------------------------------------------------------------


class TestSaveLoadRunRoundtrip:
    """Golden-path end-to-end tests: build, run, save, load, run again, compare.

    These tests verify that the full cycle from pipeline construction through
    serialization and reconstruction produces identical results.
    """

    def test_standard_save_load_run_roundtrip(self, tmp_path):
        """Build+run a pipeline, save at standard level, load, run loaded pipeline,
        verify results exactly match the original run.

        Uses CSVSource (reconstructable) so the full pipeline can be reconstructed
        from the saved JSON without needing live in-memory data.
        """
        csv_path = str(tmp_path / "data.csv")
        _write_csv(csv_path, [{"x": "1", "y": "2"}, {"x": "3", "y": "4"}])

        db = DeltaTableDatabase(base_path=str(tmp_path / "db"))
        source = CSVSource(file_path=csv_path, tag_columns=["x"], source_id="src")
        pf = PythonPacketFunction(
            function=transform_func,
            output_keys=["result"],
            function_name="transform_func",
        )
        pod = FunctionPod(packet_function=pf)
        pipeline = Pipeline(name="roundtrip", pipeline_database=db)
        with pipeline:
            result_stream = pod.process(source, label="transform")

        # Run original pipeline and capture results
        pipeline.run()
        db.flush()
        original_results = sorted(
            p.as_dict()["result"] for _, p in result_stream.iter_packets()
        )
        assert len(original_results) == 2

        # Save and load
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))  # default: standard level
        loaded = Pipeline.load(str(path), mode="full")

        # Run the loaded pipeline and compare
        fn_node = loaded.compiled_nodes["transform"]
        assert fn_node.load_status == LoadStatus.FULL
        loaded_results = sorted(
            p.as_dict()["result"] for _, p in fn_node.iter_packets()
        )
        assert loaded_results == original_results

    def test_definition_save_load_run_roundtrip(self, tmp_path):
        """Save at definition level, load with a caller-supplied database,
        run the loaded pipeline, verify results match the original.

        Definition-level saves carry graph structure and function config but
        no embedded DB config.  Supplying a fresh database at load time
        should allow the pipeline to execute as normal.
        """
        csv_path = str(tmp_path / "data.csv")
        _write_csv(csv_path, [{"x": "1", "y": "10"}, {"x": "3", "y": "20"}])

        original_db = DeltaTableDatabase(base_path=str(tmp_path / "original_db"))
        source = CSVSource(file_path=csv_path, tag_columns=["x"], source_id="src2")
        pf = PythonPacketFunction(
            function=transform_func,
            output_keys=["result"],
            function_name="transform_func",
        )
        pod = FunctionPod(packet_function=pf)
        pipeline = Pipeline(name="def_roundtrip", pipeline_database=original_db)
        with pipeline:
            result_stream = pod.process(source, label="transform")

        # Run original pipeline to capture expected results
        pipeline.run()
        original_db.flush()
        original_results = sorted(
            p.as_dict()["result"] for _, p in result_stream.iter_packets()
        )
        assert len(original_results) == 2

        # Save at definition level (no DB config embedded)
        path = tmp_path / "pipeline_definition.json"
        pipeline.save(str(path), level="definition")

        # Load with a fresh database supplied by the caller
        fresh_db = DeltaTableDatabase(base_path=str(tmp_path / "fresh_db"))
        loaded = Pipeline.load(str(path), pipeline_database=fresh_db)

        # Source is reconstructable → function node should reach FULL status
        fn_node = loaded.compiled_nodes["transform"]
        assert fn_node.load_status == LoadStatus.FULL

        # Run the loaded pipeline and compare results
        loaded.run()
        loaded_results = sorted(
            p.as_dict()["result"] for _, p in fn_node.iter_packets()
        )
        assert loaded_results == original_results

    def test_definition_save_load_with_unloadable_function_uses_proxy(self, tmp_path):
        """Save at definition level; on load the function module is unavailable.

        When the Python function cannot be imported in the new context (e.g. it
        was defined in a module that no longer exists, or was a lambda), the
        pipeline should still load successfully using ``PacketFunctionProxy``
        to stand in for the missing function.  The function node should reach
        ``READ_ONLY`` status (source reconstructable, function proxied) and be
        able to serve previously-cached results from the supplied database.
        """
        from orcapod.core.packet_function_proxy import PacketFunctionProxy

        csv_path = str(tmp_path / "data.csv")
        _write_csv(csv_path, [{"x": "1", "y": "5"}, {"x": "3", "y": "15"}])

        db = DeltaTableDatabase(base_path=str(tmp_path / "db"))
        source = CSVSource(file_path=csv_path, tag_columns=["x"], source_id="src3")
        pf = PythonPacketFunction(
            function=transform_func,
            output_keys=["result"],
            function_name="transform_func",
        )
        pod = FunctionPod(packet_function=pf)
        pipeline = Pipeline(name="proxy_def", pipeline_database=db)
        with pipeline:
            result_stream = pod.process(source, label="transform")

        # Run original pipeline and flush so cached results exist in the DB
        pipeline.run()
        db.flush()
        original_results = sorted(
            p.as_dict()["result"] for _, p in result_stream.iter_packets()
        )
        assert len(original_results) == 2

        # Save at definition level — no DB config embedded in the JSON
        path = tmp_path / "pipeline_def_proxy.json"
        pipeline.save(str(path), level="definition")

        # Corrupt the function's module path so it cannot be imported in this context
        _corrupt_function_module_path(str(path))

        # Load with the original DB (which has the cached results) supplied by caller.
        # The unloadable function should be replaced by PacketFunctionProxy.
        loaded = Pipeline.load(str(path), pipeline_database=db)

        fn_node = loaded.compiled_nodes["transform"]

        # Source is CSVSource (reconstructable → FULL); function is PacketFunctionProxy
        # → READ_ONLY (not FULL because live computation is unavailable).
        assert fn_node.load_status == LoadStatus.READ_ONLY

        # The packet function should be a proxy standing in for the missing function
        assert isinstance(fn_node._packet_function, PacketFunctionProxy)
