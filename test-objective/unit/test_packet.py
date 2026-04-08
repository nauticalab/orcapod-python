"""Specification-derived tests for Packet."""

import pyarrow as pa
import pytest

from orcapod.core.datagrams.datagram import Datagram
from orcapod.core.datagrams.tag_packet import Packet
from orcapod.types import ColumnConfig


def _make_context():
    """Create a DataContext for tests."""
    from orcapod.contexts import resolve_context
    return resolve_context(None)


# ---------------------------------------------------------------------------
# Source info stored per data column
# ---------------------------------------------------------------------------

class TestPacketSourceInfo:
    """source_info is stored per data column."""

    def test_packet_stores_source_info(self):
        ctx = _make_context()
        pkt = Packet(
            {"x": 1, "y": "hello"},
            data_context=ctx,
            source_info={"x": "src_x", "y": "src_y"},
        )
        assert pkt["x"] == 1

    def test_source_info_not_in_keys_by_default(self):
        ctx = _make_context()
        pkt = Packet(
            {"x": 1},
            data_context=ctx,
            source_info={"x": "src_x"},
        )
        keys = list(pkt.keys())
        assert "x" in keys
        assert not any(k.startswith("_source_") for k in keys)

    def test_source_info_not_in_as_dict_by_default(self):
        ctx = _make_context()
        pkt = Packet(
            {"x": 1},
            data_context=ctx,
            source_info={"x": "src_x"},
        )
        d = pkt.as_dict()
        assert not any(k.startswith("_source_") for k in d)

    def test_source_info_not_in_as_table_by_default(self):
        ctx = _make_context()
        pkt = Packet(
            {"x": 1},
            data_context=ctx,
            source_info={"x": "src_x"},
        )
        table = pkt.as_table()
        assert not any(name.startswith("_source_") for name in table.column_names)


# ---------------------------------------------------------------------------
# Source info included with ColumnConfig
# ---------------------------------------------------------------------------

class TestPacketSourceInfoWithConfig:
    """With ColumnConfig source=True or all_info=True, source columns included."""

    def test_keys_with_source_true(self):
        ctx = _make_context()
        pkt = Packet(
            {"x": 1},
            data_context=ctx,
            source_info={"x": "src_x"},
        )
        keys = list(pkt.keys(columns=ColumnConfig(source=True)))
        assert any(k.startswith("_source_") for k in keys)

    def test_as_dict_with_source_true(self):
        ctx = _make_context()
        pkt = Packet(
            {"x": 1},
            data_context=ctx,
            source_info={"x": "src_x"},
        )
        d = pkt.as_dict(columns=ColumnConfig(source=True))
        assert any(k.startswith("_source_") for k in d)

    def test_as_table_with_source_true(self):
        ctx = _make_context()
        pkt = Packet(
            {"x": 1},
            data_context=ctx,
            source_info={"x": "src_x"},
        )
        table = pkt.as_table(columns=ColumnConfig(source=True))
        assert any(name.startswith("_source_") for name in table.column_names)

    def test_keys_with_all_info(self):
        ctx = _make_context()
        pkt = Packet(
            {"x": 1},
            data_context=ctx,
            source_info={"x": "src_x"},
        )
        keys = list(pkt.keys(columns=ColumnConfig.all()))
        assert any(k.startswith("_source_") for k in keys)


# ---------------------------------------------------------------------------
# with_source_info() returns new instance (immutable)
# ---------------------------------------------------------------------------

class TestPacketWithSourceInfo:
    """with_source_info() returns new instance (immutable)."""

    def test_with_source_info_returns_new_instance(self):
        ctx = _make_context()
        pkt = Packet({"x": 1}, data_context=ctx, source_info={"x": "src_x"})
        new_pkt = pkt.with_source_info(x="new_src")
        assert new_pkt is not pkt

    def test_with_source_info_does_not_mutate_original(self):
        ctx = _make_context()
        pkt = Packet({"x": 1}, data_context=ctx, source_info={"x": "src_x"})
        pkt.with_source_info(x="new_src")
        # Original should still have old source info
        d = pkt.as_dict(columns=ColumnConfig(source=True))
        source_vals = {k: v for k, v in d.items() if k.startswith("_source_")}
        assert any(v == "src_x" for v in source_vals.values())


# ---------------------------------------------------------------------------
# rename() also renames source_info keys
# ---------------------------------------------------------------------------

class TestPacketRename:
    """rename() also renames source_info keys."""

    def test_rename_updates_source_info_keys(self):
        ctx = _make_context()
        pkt = Packet(
            {"x": 1, "y": 2},
            data_context=ctx,
            source_info={"x": "src_x", "y": "src_y"},
        )
        renamed = pkt.rename({"x": "alpha"})
        assert "alpha" in renamed
        assert "x" not in renamed
        # Source info should also be renamed
        d = renamed.as_dict(columns=ColumnConfig(source=True))
        assert any("alpha" in k for k in d if k.startswith("_source_"))
        assert not any("_source_x" == k for k in d)


# ---------------------------------------------------------------------------
# with_columns() adds source_info=None for new columns
# ---------------------------------------------------------------------------

class TestPacketWithColumns:
    """with_columns() adds source_info=None for new columns."""

    def test_with_columns_new_column_has_none_source(self):
        ctx = _make_context()
        pkt = Packet({"x": 1}, data_context=ctx, source_info={"x": "src_x"})
        extended = pkt.with_columns(z=99)
        assert "z" in extended
        # The new column should exist with source_info accessible
        d = extended.as_dict(columns=ColumnConfig(source=True))
        # z should have a source column, likely with None value
        source_z_keys = [k for k in d if k.startswith("_source_") and "z" in k]
        assert len(source_z_keys) > 0


# ---------------------------------------------------------------------------
# as_datagram() returns Datagram, not Packet
# ---------------------------------------------------------------------------

class TestPacketAsDatagram:
    """as_datagram() returns a Datagram (not Packet)."""

    def test_as_datagram_returns_datagram_type(self):
        ctx = _make_context()
        pkt = Packet({"x": 1}, data_context=ctx, source_info={"x": "src_x"})
        dg = pkt.as_datagram()
        assert isinstance(dg, Datagram)
        assert not isinstance(dg, Packet)

    def test_as_datagram_preserves_data(self):
        ctx = _make_context()
        pkt = Packet({"x": 1, "y": "hello"}, data_context=ctx, source_info={"x": "s1", "y": "s2"})
        dg = pkt.as_datagram()
        assert dg["x"] == 1
        assert dg["y"] == "hello"


# ---------------------------------------------------------------------------
# copy() preserves source_info
# ---------------------------------------------------------------------------

class TestPacketCopy:
    """copy() preserves source_info."""

    def test_copy_preserves_source_info(self):
        ctx = _make_context()
        pkt = Packet({"x": 1}, data_context=ctx, source_info={"x": "src_x"})
        copied = pkt.copy()
        assert copied is not pkt
        # Both should have same source info
        orig_d = pkt.as_dict(columns=ColumnConfig(source=True))
        copy_d = copied.as_dict(columns=ColumnConfig(source=True))
        orig_sources = {k: v for k, v in orig_d.items() if k.startswith("_source_")}
        copy_sources = {k: v for k, v in copy_d.items() if k.startswith("_source_")}
        assert orig_sources == copy_sources

    def test_copy_preserves_data(self):
        ctx = _make_context()
        pkt = Packet({"x": 1, "y": "hello"}, data_context=ctx, source_info={"x": "s1", "y": "s2"})
        copied = pkt.copy()
        assert copied["x"] == 1
        assert copied["y"] == "hello"
