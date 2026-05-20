"""End-to-end tests for Pipeline.save() and Pipeline.load()."""

from __future__ import annotations

import json

import pyarrow as pa
import pytest

from orcapod.core.nodes import SourceNode
from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.core.sources.source_spec import SourceSpec
from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
from orcapod.pipeline import Pipeline
from orcapod.pipeline.serialization import PIPELINE_FORMAT_VERSION


@pytest.fixture
def spec_pipeline(tmp_path):
    """A compiled Pipeline using SourceSpec leaves."""
    def _src(tag, data):
        tbl = pa.table({tag: pa.array(["a"], type=pa.large_string()), data: pa.array([1], type=pa.int64())})
        return ArrowTableSource(tbl, tag_columns=[tag], infer_nullable=True)

    src_a = _src("key", "value")
    src_b = _src("key", "score")
    tag_a, data_a = src_a.output_schema()
    tag_b, data_b = src_b.output_schema()

    spec_a = SourceSpec("source_a", tag_schema=tag_a, data_schema=data_a)
    spec_b = SourceSpec("source_b", tag_schema=tag_b, data_schema=data_b)

    pipeline = Pipeline(name="spec_pipe")
    with pipeline:
        Join()(spec_a, spec_b, label="joiner")

    return pipeline, tmp_path


class TestPipelineBlueprintSave:
    def test_save_creates_file(self, spec_pipeline):
        pipeline, tmp_path = spec_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        assert path.exists()

    def test_save_has_pipeline_version(self, spec_pipeline):
        pipeline, tmp_path = spec_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())
        assert data["orcapod_pipeline_version"] == PIPELINE_FORMAT_VERSION

    def test_save_no_databases_block(self, spec_pipeline):
        """Pure blueprint save must not contain a 'databases' block."""
        pipeline, tmp_path = spec_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())
        assert "databases" not in data

    def test_save_source_spec_nodes(self, spec_pipeline):
        """SourceSpec nodes must serialize with source_type='spec'."""
        pipeline, tmp_path = spec_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())
        spec_nodes = [
            n for n in data["nodes"].values()
            if n.get("node_type") == "source"
            and n.get("source_config", {}).get("source_type") == "spec"
        ]
        assert len(spec_nodes) == 2

    def test_save_load_roundtrip_preserves_topology(self, spec_pipeline):
        """load() reconstructs the same number of nodes and edges."""
        pipeline, tmp_path = spec_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path))
        assert len(loaded._persistent_node_map) == len(pipeline._persistent_node_map)
        assert len(list(loaded._node_graph.edges())) == len(list(pipeline._node_graph.edges()))

    def test_save_load_restores_spec_names(self, spec_pipeline):
        """SourceSpec names must survive save/load."""
        pipeline, tmp_path = spec_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path))
        spec_names = {
            node.stream.name
            for node in loaded._persistent_node_map.values()
            if isinstance(node, SourceNode) and isinstance(node.stream, SourceSpec)
        }
        assert spec_names == {"source_a", "source_b"}


class TestPipelineBlueprintLoad:
    def test_load_creates_compiled_pipeline(self, spec_pipeline):
        """Pipeline.load() returns a compiled Pipeline instance."""
        pipeline, tmp_path = spec_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path))
        assert loaded._compiled is True

    def test_load_restores_pipeline_name(self, spec_pipeline):
        """Pipeline name tuple is preserved across save/load."""
        pipeline, tmp_path = spec_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path))
        assert loaded.name == ("spec_pipe",)

    def test_load_restores_node_labels(self, spec_pipeline):
        """Labeled nodes are accessible by label after load."""
        pipeline, tmp_path = spec_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path))
        assert "joiner" in loaded.compiled_nodes

    def test_load_version_mismatch_raises(self, spec_pipeline, tmp_path):
        """Loading a file with an unsupported version raises ValueError."""
        pipeline, _ = spec_pipeline
        path = tmp_path / "pipeline_bad.json"
        pipeline.save(str(path))
        data = json.loads(path.read_text())
        data["orcapod_pipeline_version"] = "99.0.0"
        path.write_text(json.dumps(data))
        with pytest.raises(ValueError, match="version"):
            Pipeline.load(str(path))

    def test_load_bindable_and_runnable(self, spec_pipeline):
        """Loaded pipeline can be bound to concrete sources and run."""
        pipeline, tmp_path = spec_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path))

        # Build concrete sources matching the SourceSpec schemas declared in spec_pipeline
        src_a = ArrowTableSource(
            pa.table({
                "key": pa.array(["a", "b"], type=pa.large_string()),
                "value": pa.array([1, 2], type=pa.int64()),
            }),
            tag_columns=["key"],
            infer_nullable=True,
        )
        src_b = ArrowTableSource(
            pa.table({
                "key": pa.array(["a", "b"], type=pa.large_string()),
                "score": pa.array([10, 20], type=pa.int64()),
            }),
            tag_columns=["key"],
            infer_nullable=True,
        )
        store = InMemoryArrowDatabase()
        job = loaded.bind(sources={"source_a": src_a, "source_b": src_b}, store=store)
        completed = job.run()
        assert completed._has_run is True

    def test_load_hash_graph_has_node_types(self, spec_pipeline):
        """_hash_graph on a loaded pipeline has node_type attributes set."""
        pipeline, tmp_path = spec_pipeline
        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))
        loaded = Pipeline.load(str(path))
        for node_hash in loaded._hash_graph.nodes:
            attrs = loaded._hash_graph.nodes[node_hash]
            assert "node_type" in attrs, (
                f"Node {node_hash} missing node_type in _hash_graph"
            )
