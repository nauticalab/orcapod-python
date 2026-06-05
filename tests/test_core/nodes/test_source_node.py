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
    """SourceNode must produce stable, deterministic hashes.

    Hash values below were verified to be bit-identical to SourceSpec
    with the same arguments during the SourceSpec→SourceNode migration (ENG-493).
    SourceSpec has since been deleted; these values serve as the stability anchor.

    For tag_schema=Schema({"id": int}), data_schema=Schema({"value": float}),
    name="slot_a":
      content_hash  = semantic_v0.1:df0cba56fd880f86584ef89b35ef850bd813c95c114ac3bc84818e195b2175cb
      pipeline_hash = semantic_v0.1:3e32b07447e313318744ce498086c21ad136a40596f833c05162088e840ad16e
    """

    def test_content_hash_is_deterministic(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceNode

        node_a = SourceNode(name="slot_a", tag_schema=tag_schema, data_schema=data_schema)
        node_b = SourceNode(name="slot_a", tag_schema=tag_schema, data_schema=data_schema)
        assert node_a.content_hash() == node_b.content_hash()

    def test_content_hash_stable_value(self, tag_schema, data_schema):
        """content_hash must match the value anchored at migration time.

        Digest (hex): b2779d890c22b601f0ed71eb2817138205cc509581b9d7e23186c2f0ec815695

        Note: hash changed in ENG-493 when identity_structure() prefix was updated
        from ``"SourceSpec"`` to ``"source_node"`` to better reflect the node type.
        """
        from orcapod.core.nodes.source_node import SourceNode
        from orcapod.types import ContentHash

        node = SourceNode(name="slot_a", tag_schema=tag_schema, data_schema=data_schema)
        expected = ContentHash(
            method="semantic_v0.1",
            digest=bytes.fromhex(
                "b2779d890c22b601f0ed71eb2817138205cc509581b9d7e23186c2f0ec815695"
            ),
        )
        assert node.content_hash() == expected

    def test_pipeline_hash_is_deterministic(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceNode

        node_a = SourceNode(name="slot_a", tag_schema=tag_schema, data_schema=data_schema)
        node_b = SourceNode(name="slot_a", tag_schema=tag_schema, data_schema=data_schema)
        assert node_a.pipeline_hash() == node_b.pipeline_hash()

    def test_pipeline_hash_stable_value(self, tag_schema, data_schema):
        """pipeline_hash must match the value anchored at migration time.

        Digest (hex): 3e32b07447e313318744ce498086c21ad136a40596f833c05162088e840ad16e
        """
        from orcapod.core.nodes.source_node import SourceNode
        from orcapod.types import ContentHash

        node = SourceNode(name="slot_a", tag_schema=tag_schema, data_schema=data_schema)
        expected = ContentHash(
            method="semantic_v0.1",
            digest=bytes.fromhex(
                "3e32b07447e313318744ce498086c21ad136a40596f833c05162088e840ad16e"
            ),
        )
        assert node.pipeline_hash() == expected

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
            name="x", tag_schema=tag_schema, data_schema=data_schema, bound_source=src
        )
        assert job_node.content_hash() == src.content_hash()

    def test_bound_pipeline_hash_still_schema_based(self, tag_schema, data_schema):
        """pipeline_hash stays schema-based even when concrete is bound."""
        from orcapod.core.nodes.source_node import SourceJobNode, SourceNode
        from orcapod.core.sources.dict_source import DictSource

        src = DictSource(data=[{"id": 1, "value": 1.0}], tag_columns=["id"])
        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        job_node = SourceJobNode(
            name="x", tag_schema=tag_schema, data_schema=data_schema, bound_source=src
        )
        assert job_node.pipeline_hash() == node.pipeline_hash()

    def test_as_node_returns_source_node(self, tag_schema, data_schema):
        from orcapod.core.nodes.source_node import SourceJobNode, SourceNode

        job_node = SourceJobNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        node = job_node.as_node()
        assert isinstance(node, SourceNode)
        assert node.content_hash() == job_node.content_hash()

    def test_bound_source_property_updates_in_place(self, tag_schema, data_schema):
        """Setting bound_source mutates the node in-place."""
        from orcapod.core.nodes.source_node import SourceJobNode
        from orcapod.core.sources.dict_source import DictSource

        job_node = SourceJobNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        assert job_node.bound_source is None
        src = DictSource(data=[{"id": 1, "value": 1.0}], tag_columns=["id"])
        job_node.bound_source = src
        assert job_node.bound_source is src

    def test_bound_source_setter_clears_content_hash_cache(self, tag_schema, data_schema):
        """Setting bound_source clears the content_hash cache so stale values are not returned."""
        from orcapod.core.nodes.source_node import SourceJobNode
        from orcapod.core.sources.dict_source import DictSource

        job_node = SourceJobNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        schema_hash = job_node.content_hash()  # populates cache with schema-based hash

        src = DictSource(data=[{"id": 1, "value": 1.0}], tag_columns=["id"])
        job_node.bound_source = src  # should clear the cache

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
            name="x", tag_schema=tag_schema, data_schema=data_schema, bound_source=src
        )
        table = job_node.as_table()
        assert isinstance(table, pa.Table)


class TestSourceNodeColumnConfig:
    """SourceNodeBase must honour column_config / all_info in keys() and output_schema()."""

    def test_unbound_keys_default_unchanged(self, tag_schema, data_schema):
        """Regression: keys() with no args still returns plain schema keys."""
        from orcapod.core.nodes.source_node import SourceNode

        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        tag_keys, data_keys = node.keys()
        assert tag_keys == ("id",)
        assert data_keys == ("value",)

    def test_unbound_keys_system_tags_includes_both_system_cols(self, tag_schema, data_schema):
        """keys(system_tags=True) adds both system-tag column names to tag_keys."""
        from orcapod.core.nodes.source_node import SourceNode
        from orcapod.system_constants import constants
        from orcapod.types import ColumnConfig

        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        tag_keys, data_keys = node.keys(columns=ColumnConfig(system_tags=True))

        assert data_keys == ("value",)
        assert len(tag_keys) == 3  # id + source_id_col + record_id_col
        assert tag_keys[0] == "id"
        # Both extra columns start with the system-tag prefix
        assert all(k.startswith(constants.SYSTEM_TAG_PREFIX) for k in tag_keys[1:])

    def test_unbound_keys_all_info_same_as_system_tags(self, tag_schema, data_schema):
        """keys(all_info=True) produces the same result as keys(system_tags=True)."""
        from orcapod.core.nodes.source_node import SourceNode
        from orcapod.types import ColumnConfig

        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        via_system_tags = node.keys(columns=ColumnConfig(system_tags=True))
        via_all_info = node.keys(all_info=True)
        assert via_system_tags == via_all_info

    def test_unbound_output_schema_system_tags_adds_str_entries(self, tag_schema, data_schema):
        """output_schema(system_tags=True) tag schema includes two str-typed system-tag entries."""
        from orcapod.core.nodes.source_node import SourceNode
        from orcapod.system_constants import constants
        from orcapod.types import ColumnConfig

        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        extended_tag_schema, data_schema_out = node.output_schema(
            columns=ColumnConfig(system_tags=True)
        )

        assert data_schema_out == data_schema
        assert "id" in extended_tag_schema
        system_tag_entries = {
            k: v for k, v in extended_tag_schema.items()
            if k.startswith(constants.SYSTEM_TAG_PREFIX)
        }
        assert len(system_tag_entries) == 2
        assert all(v is str for v in system_tag_entries.values())

    def test_unbound_output_schema_default_unchanged(self, tag_schema, data_schema):
        """Regression: output_schema() with no args still returns plain schemas."""
        from orcapod.core.nodes.source_node import SourceNode

        node = SourceNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        t, d = node.output_schema()
        assert t == tag_schema
        assert d == data_schema

    def test_unbound_job_node_keys_default_unchanged(self, tag_schema, data_schema):
        """Regression: unbound SourceJobNode.keys() still returns plain schema keys."""
        from orcapod.core.nodes.source_node import SourceJobNode

        job_node = SourceJobNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        tag_keys, data_keys = job_node.keys()
        assert tag_keys == ("id",)
        assert data_keys == ("value",)

    def test_unbound_system_tag_names_match_bound(self, tag_schema, data_schema):
        """Unbound SJN.keys(system_tags=True) returns the same names as the equivalent bound source."""
        from orcapod.core.nodes.source_node import SourceJobNode
        from orcapod.core.sources.dict_source import DictSource
        from orcapod.types import ColumnConfig

        src = DictSource(data=[{"id": 1, "value": 1.0}], tag_columns=["id"])
        unbound = SourceJobNode(name="x", tag_schema=tag_schema, data_schema=data_schema)
        bound = SourceJobNode(
            name="x", tag_schema=tag_schema, data_schema=data_schema, bound_source=src
        )

        unbound_keys = unbound.keys(columns=ColumnConfig(system_tags=True))
        bound_keys = bound.keys(columns=ColumnConfig(system_tags=True))
        assert unbound_keys == bound_keys

    def test_bound_keys_delegates_to_source(self, tag_schema, data_schema):
        """Bound SourceJobNode.keys() delegates to bound_source.keys()."""
        from unittest.mock import MagicMock
        from orcapod.core.nodes.source_node import SourceJobNode
        from orcapod.types import ColumnConfig

        mock_source = MagicMock()
        mock_source.output_schema.return_value = (tag_schema, data_schema)
        mock_source.keys.return_value = (("id",), ("value",))

        job_node = SourceJobNode(name="x", bound_source=mock_source)
        cfg = ColumnConfig(system_tags=True)
        job_node.keys(columns=cfg, all_info=False)

        mock_source.keys.assert_called_once_with(columns=cfg, all_info=False)

    def test_bound_keys_all_info_matches_source_directly(self, tag_schema, data_schema):
        """Bound SJN.keys(all_info=True) == bound_source.keys(all_info=True)."""
        from orcapod.core.nodes.source_node import SourceJobNode
        from orcapod.core.sources.dict_source import DictSource

        src = DictSource(data=[{"id": 1, "value": 1.0}], tag_columns=["id"])
        job_node = SourceJobNode(
            name="x", tag_schema=tag_schema, data_schema=data_schema, bound_source=src
        )

        assert job_node.keys(all_info=True) == src.keys(all_info=True)

    def test_bound_output_schema_delegates_to_source(self, tag_schema, data_schema):
        """Bound SourceJobNode.output_schema() delegates to bound_source.output_schema()."""
        from unittest.mock import MagicMock
        from orcapod.core.nodes.source_node import SourceJobNode
        from orcapod.types import ColumnConfig

        mock_source = MagicMock()
        mock_source.output_schema.return_value = (tag_schema, data_schema)

        job_node = SourceJobNode(name="x", bound_source=mock_source)
        # Reset call history accumulated during __init__ (schema derivation).
        mock_source.output_schema.reset_mock()
        cfg = ColumnConfig(system_tags=True)
        job_node.output_schema(columns=cfg, all_info=False)

        mock_source.output_schema.assert_called_once_with(columns=cfg, all_info=False)

    def test_bound_output_schema_all_info_matches_source_directly(self, tag_schema, data_schema):
        """Bound SJN.output_schema(all_info=True) == bound_source.output_schema(all_info=True)."""
        from orcapod.core.nodes.source_node import SourceJobNode
        from orcapod.core.sources.dict_source import DictSource

        src = DictSource(data=[{"id": 1, "value": 1.0}], tag_columns=["id"])
        job_node = SourceJobNode(
            name="x", tag_schema=tag_schema, data_schema=data_schema, bound_source=src
        )

        assert job_node.output_schema(all_info=True) == src.output_schema(all_info=True)
