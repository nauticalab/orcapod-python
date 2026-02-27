"""
Tests for core stream implementations.

Verifies that StreamBase and TableStream correctly implement the Stream protocol,
and tests the core behaviour of TableStream.
"""

import pyarrow as pa
import pytest

from orcapod.core.streams import TableStream
from orcapod.protocols.core_protocols.streams import Stream

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_table_stream(
    tag_columns: list[str] | None = None,
    n_rows: int = 3,
) -> TableStream:
    """Create a minimal TableStream for testing."""
    tag_columns = tag_columns or ["id"]
    table = pa.table(
        {
            "id": pa.array(list(range(n_rows)), type=pa.int64()),
            "value": pa.array([f"v{i}" for i in range(n_rows)], type=pa.large_string()),
        }
    )
    return TableStream(table, tag_columns=tag_columns)


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


class TestStreamProtocolConformance:
    """Verify that StreamBase (via TableStream) satisfies the Stream protocol."""

    def test_stream_base_is_subclass_of_stream_protocol(self):
        """StreamBase must be a structural subtype of Stream (runtime check)."""
        # isinstance on a Protocol checks structural conformance at method-name level
        stream = make_table_stream()
        assert isinstance(stream, Stream), (
            "TableStream instance does not satisfy the Stream protocol"
        )

    def test_stream_has_source_property(self):
        stream = make_table_stream()
        # attribute must exist and be accessible
        _ = stream.source

    def test_stream_has_upstreams_property(self):
        stream = make_table_stream()
        upstreams = stream.upstreams
        assert isinstance(upstreams, tuple)

    def test_stream_has_keys_method(self):
        stream = make_table_stream()
        tag_keys, packet_keys = stream.keys()
        assert isinstance(tag_keys, tuple)
        assert isinstance(packet_keys, tuple)

    def test_stream_has_output_schema_method(self):
        from orcapod.types import Schema

        stream = make_table_stream()
        tag_schema, packet_schema = stream.output_schema()
        assert isinstance(tag_schema, Schema)
        assert isinstance(packet_schema, Schema)

    def test_stream_has_iter_packets_method(self):
        stream = make_table_stream()
        it = stream.iter_packets()
        # must be iterable
        pair = next(it)
        assert len(pair) == 2  # (Tag, Packet)

    def test_stream_has_as_table_method(self):
        stream = make_table_stream()
        table = stream.as_table()
        assert isinstance(table, pa.Table)


# ---------------------------------------------------------------------------
# TableStream construction
# ---------------------------------------------------------------------------


class TestTableStreamConstruction:
    def test_basic_construction(self):
        stream = make_table_stream()
        assert stream is not None

    def test_tag_and_packet_columns_are_separated(self):
        stream = make_table_stream(tag_columns=["id"])
        tag_keys, packet_keys = stream.keys()
        assert "id" in tag_keys
        assert "value" in packet_keys
        assert "id" not in packet_keys

    def test_missing_tag_column_raises(self):
        table = pa.table({"value": pa.array([1, 2])})
        with pytest.raises(ValueError):
            TableStream(table, tag_columns=["nonexistent"])

    def test_no_packet_column_raises(self):
        # A table where all columns are tags → no packet columns → should raise
        table = pa.table({"id": pa.array([1, 2])})
        with pytest.raises(ValueError):
            TableStream(table, tag_columns=["id"])

    def test_source_defaults_to_none(self):
        stream = make_table_stream()
        assert stream.source is None

    def test_upstreams_defaults_to_empty(self):
        stream = make_table_stream()
        assert stream.upstreams == ()


# ---------------------------------------------------------------------------
# TableStream.keys()
# ---------------------------------------------------------------------------


class TestTableStreamKeys:
    def test_returns_correct_tag_keys(self):
        stream = make_table_stream(tag_columns=["id"])
        tag_keys, _ = stream.keys()
        assert tag_keys == ("id",)

    def test_returns_correct_packet_keys(self):
        stream = make_table_stream(tag_columns=["id"])
        _, packet_keys = stream.keys()
        assert packet_keys == ("value",)

    def test_no_tag_columns(self):
        table = pa.table({"a": pa.array([1]), "b": pa.array([2])})
        stream = TableStream(table, tag_columns=[])
        tag_keys, packet_keys = stream.keys()
        assert tag_keys == ()
        assert set(packet_keys) == {"a", "b"}


# ---------------------------------------------------------------------------
# TableStream.output_schema()
# ---------------------------------------------------------------------------


class TestTableStreamOutputSchema:
    def test_schema_keys_match_column_keys(self):
        stream = make_table_stream(tag_columns=["id"])
        tag_schema, packet_schema = stream.output_schema()
        tag_keys, packet_keys = stream.keys()
        assert set(tag_schema.keys()) == set(tag_keys)
        assert set(packet_schema.keys()) == set(packet_keys)

    def test_schema_values_are_types(self):
        stream = make_table_stream(tag_columns=["id"])
        tag_schema, packet_schema = stream.output_schema()
        for v in (*tag_schema.values(), *packet_schema.values()):
            assert isinstance(v, type), f"Expected a type, got {v!r}"


# ---------------------------------------------------------------------------
# TableStream.iter_packets()
# ---------------------------------------------------------------------------


class TestTableStreamIterPackets:
    def test_yields_correct_number_of_pairs(self):
        n = 5
        stream = make_table_stream(n_rows=n)
        pairs = list(stream.iter_packets())
        assert len(pairs) == n

    def test_each_pair_has_tag_and_packet(self):
        from orcapod.protocols.core_protocols.datagrams import Packet, Tag

        stream = make_table_stream()
        for tag, packet in stream.iter_packets():
            assert isinstance(tag, Tag)
            assert isinstance(packet, Packet)

    def test_tag_contains_tag_column(self):
        stream = make_table_stream(tag_columns=["id"])
        for tag, _ in stream.iter_packets():
            assert "id" in tag.keys()

    def test_packet_contains_packet_column(self):
        stream = make_table_stream(tag_columns=["id"])
        for _, packet in stream.iter_packets():
            assert "value" in packet.keys()

    def test_values_are_correct(self):
        stream = make_table_stream(tag_columns=["id"], n_rows=3)
        pairs = list(stream.iter_packets())
        for i, (tag, packet) in enumerate(pairs):
            assert tag["id"] == i
            assert packet["value"] == f"v{i}"

    def test_iteration_is_repeatable(self):
        stream = make_table_stream(n_rows=3)
        first = list(stream.iter_packets())
        second = list(stream.iter_packets())
        assert len(first) == len(second)
        for (t1, p1), (t2, p2) in zip(first, second):
            assert t1["id"] == t2["id"]
            assert p1["value"] == p2["value"]


# ---------------------------------------------------------------------------
# TableStream.as_table()
# ---------------------------------------------------------------------------


class TestTableStreamAsTable:
    def test_returns_pyarrow_table(self):
        stream = make_table_stream()
        assert isinstance(stream.as_table(), pa.Table)

    def test_table_has_correct_row_count(self):
        n = 4
        stream = make_table_stream(n_rows=n)
        assert len(stream.as_table()) == n

    def test_table_contains_all_columns(self):
        stream = make_table_stream(tag_columns=["id"])
        table = stream.as_table()
        assert "id" in table.column_names
        assert "value" in table.column_names

    def test_all_info_adds_extra_columns(self):
        stream = make_table_stream()
        default_table = stream.as_table()
        all_info_table = stream.as_table(all_info=True)
        # all_info includes context and source columns; must be at least as wide
        assert len(all_info_table.column_names) >= len(default_table.column_names)


# ---------------------------------------------------------------------------
# TableStream.__iter__ (convenience)
# ---------------------------------------------------------------------------


class TestTableStreamIter:
    def test_iter_delegates_to_iter_packets(self):
        stream = make_table_stream(n_rows=3)
        via_iter = list(stream)
        assert len(via_iter) == len(via_iter)
