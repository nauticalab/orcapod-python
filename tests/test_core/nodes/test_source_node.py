"""Tests for SourceNode (schema-only slot) and SourceJobNode (execution variant)."""
from __future__ import annotations

import pytest

from orcapod.errors import SourceSpecMismatchError, UnboundSourceError
from orcapod.types import Schema


@pytest.fixture
def tag_schema():
    return Schema({"id": int})


@pytest.fixture
def data_schema():
    return Schema({"value": float})


class TestSourceNodeHashStability:
    """SourceNode must produce bit-identical hashes to SourceSpec with the same args."""

    def test_content_hash_matches_source_spec(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceNode
        from orcapod.core.sources.source_spec import SourceSpec

        spec = SourceSpec(name="slot_a", tag_schema=tag_schema, data_schema=data_schema)
        node = SourceNode(name="slot_a", tag_schema=tag_schema, data_schema=data_schema)
        assert node.content_hash() == spec.content_hash()

    def test_pipeline_hash_matches_source_spec(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceNode
        from orcapod.core.sources.source_spec import SourceSpec

        spec = SourceSpec(name="slot_a", tag_schema=tag_schema, data_schema=data_schema)
        node = SourceNode(name="slot_a", tag_schema=tag_schema, data_schema=data_schema)
        assert node.pipeline_hash() == spec.pipeline_hash()

    def test_different_names_different_content_hash(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceNode

        a = SourceNode(name="slot_a", tag_schema=tag_schema, data_schema=data_schema)
        b = SourceNode(name="slot_b", tag_schema=tag_schema, data_schema=data_schema)
        assert a.content_hash() != b.content_hash()

    def test_different_names_same_pipeline_hash(self, tag_schema, data_schema):
        """pipeline_hash is schema-only, name-independent."""
        from orcapod.core.nodes.source_node import SourceNode

        a = SourceNode(name="slot_a", tag_schema=tag_schema, data_schema=data_schema)
        b = SourceNode(name="slot_b", tag_schema=tag_schema, data_schema=data_schema)
        assert a.pipeline_hash() == b.pipeline_hash()


class TestSourceNodeInterface:
    def test_iter_data_raises_unbound_error(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceNode

        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        with pytest.raises(UnboundSourceError):
            list(node.iter_data())

    def test_output_schema(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceNode

        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        t, d = node.output_schema()
        assert t == tag_schema
        assert d == data_schema

    def test_label_resolves_to_name(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceNode

        node = SourceNode(name="my_slot", tag_schema=tag_schema, data_schema=data_schema)
        assert node.label == "my_slot"

    def test_node_type(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceNode

        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        assert node.node_type == "source"

    def test_name_property(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceNode

        node = SourceNode(name="my_slot", tag_schema=tag_schema, data_schema=data_schema)
        assert node.name == "my_slot"

    def test_validate_compatible_source(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceNode
        from orcapod.core.sources.dict_source import DictSource

        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        src = DictSource(data=[{"id": 1, "value": 1.0}], tag_columns=["id"])
        node.validate(src)  # must not raise

    def test_validate_incompatible_raises(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceNode
        from orcapod.core.sources.dict_source import DictSource

        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        src = DictSource(data=[{"id": 1, "wrong": 1.0}], tag_columns=["id"])
        with pytest.raises(SourceSpecMismatchError):
            node.validate(src)


class TestSourceJobNode:
    def test_unbound_iter_data_raises(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceJobNode

        job_node = SourceJobNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        with pytest.raises(UnboundSourceError):
            list(job_node.iter_data())

    def test_unbound_content_hash_matches_source_node(self, tag_schema, data_schema):
        """Unbound SourceJobNode has same content_hash as SourceNode."""
        from orcapod.core.nodes.source_node import SourceJobNode, SourceNode

        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        job_node = SourceJobNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        assert job_node.content_hash() == node.content_hash()

    def test_pipeline_hash_matches_source_node(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceJobNode, SourceNode

        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        job_node = SourceJobNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        assert job_node.pipeline_hash() == node.pipeline_hash()

    def test_bound_content_hash_is_concrete_hash(self, tag_schema, data_schema):
        """Bound SourceJobNode content_hash() == concrete.content_hash()."""
        from orcapod.core.nodes.source_node import SourceJobNode
        from orcapod.core.sources.dict_source import DictSource

        src = DictSource(data=[{"id": 1, "value": 1.0}], tag_columns=["id"])
        job_node = SourceJobNode(
            name="x", tag_schema=tag_schema, data_schema=data_schema, concrete=src
        )
        assert job_node.content_hash() == src.content_hash()

    def test_bound_pipeline_hash_still_schema_based(self, tag_schema, data_schema):
        """pipeline_hash stays schema-based even when concrete is bound."""
        from orcapod.core.nodes.source_node import SourceJobNode, SourceNode
        from orcapod.core.sources.dict_source import DictSource

        src = DictSource(data=[{"id": 1, "value": 1.0}], tag_columns=["id"])
        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        job_node = SourceJobNode(
            name="x", tag_schema=tag_schema, data_schema=data_schema, concrete=src
        )
        assert job_node.pipeline_hash() == node.pipeline_hash()

    def test_as_node_returns_source_node(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceJobNode, SourceNode

        job_node = SourceJobNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        node = job_node.as_node()
        assert isinstance(node, SourceNode)
        assert node.content_hash() == job_node.content_hash()

    def test_mutable_concrete_updates_in_place(self, tag_schema, data_schema):
        """Binding concrete mutates _concrete in-place."""
        from orcapod.core.nodes.source_node import SourceJobNode
        from orcapod.core.sources.dict_source import DictSource

        job_node = SourceJobNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        assert job_node._concrete is None
        src = DictSource(data=[{"id": 1, "value": 1.0}], tag_columns=["id"])
        job_node._concrete = src
        assert job_node._concrete is src

    def test_concrete_mutation_clears_content_hash_cache(self, tag_schema, data_schema):
        """Setting _concrete clears the content_hash cache so stale values are not returned."""
        from orcapod.core.nodes.source_node import SourceJobNode, SourceNode
        from orcapod.core.sources.dict_source import DictSource

        job_node = SourceJobNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        schema_hash = job_node.content_hash()  # populates cache with schema-based hash

        src = DictSource(data=[{"id": 1, "value": 1.0}], tag_columns=["id"])
        job_node._concrete = src  # should clear the cache

        assert job_node._content_hash_cache == {}  # cache cleared
        bound_hash = job_node.content_hash()
        assert bound_hash != schema_hash  # now returns concrete-based hash
        assert bound_hash == src.content_hash()


class TestSourceNodeAsTable:
    def test_source_node_as_table_raises_unbound_error(self, tag_schema, data_schema):
        """SourceNode.as_table() raises UnboundSourceError (no concrete data)."""
        from orcapod.core.nodes.source_node import SourceNode

        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        with pytest.raises(UnboundSourceError):
            node.as_table()

    def test_source_job_node_as_table_delegates_to_concrete(self, tag_schema, data_schema):
        """Bound SourceJobNode.as_table() delegates to concrete and does not raise."""
        import pyarrow as pa

        from orcapod.core.nodes.source_node import SourceJobNode
        from orcapod.core.sources.dict_source import DictSource

        src = DictSource(data=[{"id": 1, "value": 1.0}], tag_columns=["id"])
        job_node = SourceJobNode(
            name="x", tag_schema=tag_schema, data_schema=data_schema, concrete=src
        )
        table = job_node.as_table()
        assert isinstance(table, pa.Table)
