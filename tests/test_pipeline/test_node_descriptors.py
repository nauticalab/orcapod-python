"""Tests for node from_descriptor() classmethods."""

import pytest

from orcapod.core.nodes.source_node import SourceNode
from orcapod.core.sources.dict_source import DictSource
from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
from orcapod.pipeline.serialization import LoadStatus


class TestSourceNodeFromDescriptor:
    def _make_source_and_descriptor(self):
        source = DictSource(
            data=[{"a": 1, "b": 2}, {"a": 3, "b": 4}],
            tag_columns=["a"],
            source_id="test",
        )
        node = SourceNode(stream=source, label="my_source")
        tag_schema, packet_schema = node.output_schema()
        descriptor = {
            "node_type": "source",
            "label": "my_source",
            "content_hash": node.content_hash().to_string(),
            "pipeline_hash": node.pipeline_hash().to_string(),
            "data_context_key": node.data_context_key,
            "output_schema": {
                "tag": {k: str(v) for k, v in tag_schema.items()},
                "packet": {k: str(v) for k, v in packet_schema.items()},
            },
            "stream_type": "dict",
            "source_id": "test",
            "reconstructable": False,
        }
        return source, node, descriptor

    def test_from_descriptor_with_stream(self):
        source, original, descriptor = self._make_source_and_descriptor()
        loaded = SourceNode.from_descriptor(
            descriptor=descriptor,
            stream=source,
            databases={},
        )
        assert loaded.load_status == LoadStatus.FULL
        assert loaded.label == "my_source"

    def test_from_descriptor_without_stream_read_only(self):
        _, original, descriptor = self._make_source_and_descriptor()
        db = InMemoryArrowDatabase()
        loaded = SourceNode.from_descriptor(
            descriptor=descriptor,
            stream=None,
            databases={"pipeline": db},
        )
        assert loaded.load_status in (LoadStatus.READ_ONLY, LoadStatus.UNAVAILABLE)

    def test_from_descriptor_output_schema_from_metadata(self):
        _, original, descriptor = self._make_source_and_descriptor()
        loaded = SourceNode.from_descriptor(
            descriptor=descriptor,
            stream=None,
            databases={},
        )
        tag_schema, packet_schema = loaded.output_schema()
        assert set(tag_schema.keys()) == set(descriptor["output_schema"]["tag"].keys())
        assert set(packet_schema.keys()) == set(
            descriptor["output_schema"]["packet"].keys()
        )

    def test_from_descriptor_full_mode_delegates_to_stream(self):
        """Full-mode node should delegate output_schema, iter_packets, as_table to stream."""
        source, _, descriptor = self._make_source_and_descriptor()
        loaded = SourceNode.from_descriptor(
            descriptor=descriptor,
            stream=source,
            databases={},
        )
        tag_schema, packet_schema = loaded.output_schema()
        assert "a" in tag_schema
        assert "b" in packet_schema
        # iter_packets should work
        packets = list(loaded.iter_packets())
        assert len(packets) == 2

    def test_from_descriptor_read_only_iter_packets_raises(self):
        """Read-only node should raise when iter_packets is called."""
        _, _, descriptor = self._make_source_and_descriptor()
        loaded = SourceNode.from_descriptor(
            descriptor=descriptor,
            stream=None,
            databases={},
        )
        with pytest.raises(RuntimeError, match="read-only mode"):
            list(loaded.iter_packets())

    def test_from_descriptor_read_only_as_table_raises(self):
        """Read-only node should raise when as_table is called."""
        _, _, descriptor = self._make_source_and_descriptor()
        loaded = SourceNode.from_descriptor(
            descriptor=descriptor,
            stream=None,
            databases={},
        )
        with pytest.raises(RuntimeError, match="read-only mode"):
            loaded.as_table()

    def test_from_descriptor_stored_hashes(self):
        """Read-only node should return stored content_hash and pipeline_hash."""
        _, original, descriptor = self._make_source_and_descriptor()
        loaded = SourceNode.from_descriptor(
            descriptor=descriptor,
            stream=None,
            databases={},
        )
        assert loaded.content_hash().to_string() == descriptor["content_hash"]
        assert loaded.pipeline_hash().to_string() == descriptor["pipeline_hash"]


from orcapod.core.nodes.function_node import FunctionNode
from orcapod.core.function_pod import FunctionPod
from orcapod.core.packet_function import PythonPacketFunction


def _sample_func(b: int) -> dict[str, int]:
    return {"result": b * 2}


class TestFunctionNodeFromDescriptor:
    def _make_function_node_descriptor(self):
        source = DictSource(
            data=[{"a": 1, "b": 2}],
            tag_columns=["a"],
            source_id="test",
        )
        pf = PythonPacketFunction(function=_sample_func, output_keys=["result"])
        pod = FunctionPod(packet_function=pf)
        db = InMemoryArrowDatabase()
        scoped_db = db.at("test_pipeline")
        node = FunctionNode(
            function_pod=pod,
            input_stream=source,
            pipeline_database=scoped_db,
        )
        tag_schema, packet_schema = node.output_schema()
        descriptor = {
            "node_type": "function",
            "label": None,
            "content_hash": node.content_hash().to_string(),
            "pipeline_hash": node.pipeline_hash().to_string(),
            "data_context_key": node.data_context_key,
            "output_schema": {
                "tag": {k: str(v) for k, v in tag_schema.items()},
                "packet": {k: str(v) for k, v in packet_schema.items()},
            },
            "function_pod": pod.to_config(),
            "pipeline_path": list(node.node_identity_path),
            "result_record_path": list(node._cached_function_pod.record_path),
        }
        return node, descriptor, scoped_db

    def test_from_descriptor_full_mode(self):
        original, descriptor, db = self._make_function_node_descriptor()
        source = DictSource(
            data=[{"a": 1, "b": 2}],
            tag_columns=["a"],
            source_id="test",
        )
        pf = PythonPacketFunction(function=_sample_func, output_keys=["result"])
        pod = FunctionPod(packet_function=pf)
        loaded = FunctionNode.from_descriptor(
            descriptor=descriptor,
            function_pod=pod,
            input_stream=source,
            databases={"pipeline": db, "result": db},
        )
        assert loaded.load_status == LoadStatus.FULL

    def test_from_descriptor_read_only(self):
        original, descriptor, db = self._make_function_node_descriptor()
        loaded = FunctionNode.from_descriptor(
            descriptor=descriptor,
            function_pod=None,
            input_stream=None,
            databases={"pipeline": db, "result": db},
        )
        assert loaded.load_status in (LoadStatus.READ_ONLY, LoadStatus.UNAVAILABLE)


from orcapod.core.nodes.operator_node import OperatorNode
from orcapod.core.operators import Join


class TestOperatorNodeFromDescriptor:
    def test_from_descriptor_read_only(self):
        db = InMemoryArrowDatabase()
        descriptor = {
            "node_type": "operator",
            "label": "my_join",
            "content_hash": "fake_hash",
            "pipeline_hash": "fake_pipeline_hash",
            "data_context_key": "std:v0.1:default",
            "output_schema": {
                "tag": {"a": "int64"},
                "packet": {"b": "int64", "c": "int64"},
            },
            "operator": {
                "class_name": "Join",
                "module_path": "orcapod.core.operators.join",
                "config": {},
            },
            "cache_mode": "OFF",
            "pipeline_path": ["test", "Join", "hash", "schema:fake_pipeline_hash", "instance:fake_content_hash"],
        }
        loaded = OperatorNode.from_descriptor(
            descriptor=descriptor,
            operator=None,
            input_streams=(),
            databases={"pipeline": db},
        )
        assert loaded.load_status in (LoadStatus.READ_ONLY, LoadStatus.UNAVAILABLE)
        assert loaded.label == "my_join"

    def test_from_descriptor_full_mode(self):
        db = InMemoryArrowDatabase()
        scoped_db = db.at("test")
        source1 = DictSource(data=[{"a": 1, "b": 2}], tag_columns=["a"], source_id="s1")
        source2 = DictSource(data=[{"a": 1, "c": 3}], tag_columns=["a"], source_id="s2")
        op = Join()
        node = OperatorNode(
            operator=op,
            input_streams=(source1, source2),
            pipeline_database=scoped_db,
        )
        descriptor = {
            "node_type": "operator",
            "label": None,
            "content_hash": node.content_hash().to_string(),
            "pipeline_hash": node.pipeline_hash().to_string(),
            "data_context_key": node.data_context_key,
            "output_schema": {
                "tag": {"a": "int64"},
                "packet": {"b": "int64", "c": "int64"},
            },
            "operator": op.to_config(),
            "cache_mode": "OFF",
            "pipeline_path": list(node.node_identity_path),
        }
        loaded = OperatorNode.from_descriptor(
            descriptor=descriptor,
            operator=op,
            input_streams=(source1, source2),
            databases={"pipeline": scoped_db},
        )
        assert loaded.load_status == LoadStatus.FULL
