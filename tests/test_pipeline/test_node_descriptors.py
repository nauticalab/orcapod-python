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
