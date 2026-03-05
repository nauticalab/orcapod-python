"""Specification-derived integration tests for ColumnConfig filtering across components.

Tests that ColumnConfig consistently controls column visibility across
Datagram, Tag, Packet, Stream, and Source components.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.datagrams.datagram import Datagram
from orcapod.core.datagrams.tag_packet import Packet, Tag
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig


# ===================================================================
# Datagram ColumnConfig
# ===================================================================


class TestDatagramColumnConfig:
    """Per design, ColumnConfig controls which column groups are visible."""

    def test_data_only_excludes_meta(self):
        d = Datagram(
            {"name": "alice", "age": 30},
            meta_info={"pipeline": "test"},
        )
        keys = d.keys()
        assert "name" in keys
        assert "age" in keys
        # Meta columns should not be visible
        for k in keys:
            assert not k.startswith(constants.META_PREFIX)

    def test_meta_true_includes_meta(self):
        d = Datagram(
            {"name": "alice"},
            meta_info={"pipeline": "test"},
        )
        keys_default = d.keys()
        keys_with_meta = d.keys(columns=ColumnConfig(meta=True))
        # With meta=True, there should be more keys than default
        assert len(keys_with_meta) > len(keys_default)
        assert "pipeline" in keys_with_meta

    def test_all_info_includes_everything(self):
        d = Datagram(
            {"name": "alice"},
            meta_info={"pipeline": "test"},
        )
        keys_all = d.keys(all_info=True)
        keys_default = d.keys()
        assert len(keys_all) >= len(keys_default)


# ===================================================================
# Tag ColumnConfig
# ===================================================================


class TestTagColumnConfig:
    """Per design, system_tags=True includes _tag:: columns in Tag."""

    def test_system_tags_excluded_by_default(self):
        t = Tag(
            {"id": 1},
            system_tags={"_tag::source:abc": "rec1"},
        )
        keys = t.keys()
        assert "_tag::source:abc" not in keys

    def test_system_tags_included_with_config(self):
        t = Tag(
            {"id": 1},
            system_tags={"_tag::source:abc": "rec1"},
        )
        keys_default = t.keys()
        keys_with_tags = t.keys(columns=ColumnConfig(system_tags=True))
        assert len(keys_with_tags) > len(keys_default)
        assert "_tag::source:abc" in keys_with_tags

    def test_all_info_includes_system_tags(self):
        t = Tag(
            {"id": 1},
            system_tags={"_tag::source:abc": "rec1"},
        )
        keys = t.keys(all_info=True)
        assert "_tag::source:abc" in keys


# ===================================================================
# Packet ColumnConfig
# ===================================================================


class TestPacketColumnConfig:
    """Per design, source=True includes _source_ columns in Packet."""

    def test_source_excluded_by_default(self):
        p = Packet(
            {"value": 42},
            source_info={"value": "src1:rec1"},
        )
        keys = p.keys()
        for k in keys:
            assert not k.startswith(constants.SOURCE_PREFIX)

    def test_source_included_with_config(self):
        p = Packet(
            {"value": 42},
            source_info={"value": "src1:rec1"},
        )
        keys = p.keys(columns=ColumnConfig(source=True))
        source_keys = [k for k in keys if k.startswith(constants.SOURCE_PREFIX)]
        assert len(source_keys) > 0

    def test_all_info_includes_source(self):
        p = Packet(
            {"value": 42},
            source_info={"value": "src1:rec1"},
        )
        keys = p.keys(all_info=True)
        source_keys = [k for k in keys if k.startswith(constants.SOURCE_PREFIX)]
        assert len(source_keys) > 0


# ===================================================================
# Stream ColumnConfig consistency
# ===================================================================


class TestStreamColumnConfigConsistency:
    """Per design, keys(), output_schema(), and as_table() should all
    respect the same ColumnConfig consistently."""

    def test_keys_schema_table_consistency_default(self):
        source = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value": pa.array([10, 20], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )
        tag_keys, packet_keys = source.keys()
        tag_schema, packet_schema = source.output_schema()
        table = source.as_table()

        # keys and schema should have same field names
        assert set(tag_keys) == set(tag_schema.keys())
        assert set(packet_keys) == set(packet_schema.keys())

        # Table should have all key columns
        all_keys = set(tag_keys) | set(packet_keys)
        assert all_keys.issubset(set(table.column_names))

    def test_keys_schema_table_consistency_all_info(self):
        source = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value": pa.array([10, 20], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )
        tag_keys, packet_keys = source.keys(all_info=True)
        tag_schema, packet_schema = source.output_schema(all_info=True)
        table = source.as_table(all_info=True)

        assert set(tag_keys) == set(tag_schema.keys())
        assert set(packet_keys) == set(packet_schema.keys())

        all_keys = set(tag_keys) | set(packet_keys)
        assert all_keys.issubset(set(table.column_names))

    def test_all_info_has_more_columns_than_default(self):
        source = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value": pa.array([10, 20], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )
        default_table = source.as_table()
        all_info_table = source.as_table(all_info=True)
        assert all_info_table.num_columns >= default_table.num_columns
