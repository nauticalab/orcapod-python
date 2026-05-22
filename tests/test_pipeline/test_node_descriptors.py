"""Tests for node from_descriptor() classmethods."""

import pytest

from orcapod.core.nodes.source_node import SourceNode
from orcapod.core.sources.dict_source import DictSource
from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
from orcapod.errors import UnboundSourceError
from orcapod.pipeline.serialization import LoadStatus
from orcapod.types import Schema


class TestSourceNodeFromDescriptor:
    """Tests for SourceNode construction — the new schema-only design.

    SourceNode no longer wraps a stream; instead it stores name + schemas
    and raises UnboundSourceError on data access.
    """

    def _make_source_node(self):
        tag_schema = Schema({"a": int})
        data_schema = Schema({"b": int})
        node = SourceNode(name="my_source", tag_schema=tag_schema, data_schema=data_schema)
        return node

    def test_from_descriptor_with_stream(self):
        """SourceNode can be constructed with name and schemas (new API)."""
        node = self._make_source_node()
        assert node.label == "my_source"
        assert node.name == "my_source"

    def test_from_descriptor_without_stream_read_only(self):
        """Unbound SourceNode has no concrete data."""
        node = self._make_source_node()
        with pytest.raises(UnboundSourceError):
            list(node.iter_data())

    def test_from_descriptor_output_schema_from_metadata(self):
        """SourceNode output_schema returns the declared tag and data schemas."""
        tag_schema = Schema({"a": int})
        data_schema = Schema({"b": int})
        node = SourceNode(name="test_node", tag_schema=tag_schema, data_schema=data_schema)
        t, d = node.output_schema()
        assert set(t.keys()) == {"a"}
        assert set(d.keys()) == {"b"}

    def test_from_descriptor_full_mode_delegates_to_stream(self):
        """SourceNode with bound concrete (via SourceJobNode) delegates iter_data."""
        from orcapod.core.nodes.source_node import SourceJobNode
        tag_schema = Schema({"a": int})
        data_schema = Schema({"b": int})
        source = DictSource(
            data=[{"a": 1, "b": 2}, {"a": 3, "b": 4}],
            tag_columns=["a"],
            source_id="test",
        )
        job_node = SourceJobNode(
            name="my_source",
            tag_schema=tag_schema,
            data_schema=data_schema,
            concrete=source,
        )
        t, d = job_node.output_schema()
        assert "a" in t
        assert "b" in d
        data = list(job_node.iter_data())
        assert len(data) == 2

    def test_from_descriptor_read_only_iter_data_raises(self):
        """Unbound SourceNode should raise UnboundSourceError on iter_data."""
        node = self._make_source_node()
        with pytest.raises(UnboundSourceError):
            list(node.iter_data())

    def test_from_descriptor_read_only_as_table_raises(self):
        """Unbound SourceNode should raise UnboundSourceError on as_table."""
        node = self._make_source_node()
        with pytest.raises(UnboundSourceError):
            node.as_table()

    def test_from_descriptor_stored_hashes(self):
        """SourceNode produces stable hashes based on name and schemas."""
        node = self._make_source_node()
        ch = node.content_hash()
        ph = node.pipeline_hash()
        # Same args → same hashes
        node2 = SourceNode(
            name="my_source",
            tag_schema=Schema({"a": int}),
            data_schema=Schema({"b": int}),
        )
        assert node2.content_hash() == ch
        assert node2.pipeline_hash() == ph


from orcapod.core.nodes.function_node import FunctionNode
from orcapod.core.function_pod import FunctionPod
from orcapod.core.data_function import PythonDataFunction


def _sample_func(b: int) -> dict[str, int]:
    return {"result": b * 2}


class TestFunctionNodeFromDescriptor:
    def _make_function_node_descriptor(self):
        source = DictSource(
            data=[{"a": 1, "b": 2}],
            tag_columns=["a"],
            source_id="test",
        )
        pf = PythonDataFunction(function=_sample_func, output_keys=["result"])
        pod = FunctionPod(data_function=pf)
        db = InMemoryArrowDatabase()
        scoped_db = db.at("test_pipeline")
        node = FunctionNode(
            function_pod=pod,
            input_stream=source,
            pipeline_database=scoped_db,
        )
        tag_schema, data_schema = node.output_schema()
        descriptor = {
            "node_type": "function",
            "label": None,
            "content_hash": node.content_hash().to_string(),
            "pipeline_hash": node.pipeline_hash().to_string(),
            "data_context_key": node.data_context_key,
            "table_scope": node._table_scope,
            "output_schema": {
                "tag": {k: str(v) for k, v in tag_schema.items()},
                "data": {k: str(v) for k, v in data_schema.items()},
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
        pf = PythonDataFunction(function=_sample_func, output_keys=["result"])
        pod = FunctionPod(data_function=pf)
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
            "table_scope": "pipeline_hash",
            "output_schema": {
                "tag": {"a": "int64"},
                "data": {"b": "int64", "c": "int64"},
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
            "table_scope": node._table_scope,
            "output_schema": {
                "tag": {"a": "int64"},
                "data": {"b": "int64", "c": "int64"},
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
