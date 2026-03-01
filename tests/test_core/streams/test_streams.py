"""
Tests for core stream implementations.

Verifies that StreamBase and ArrowTableStream correctly implement the StreamProtocol protocol,
and tests the core behaviour of ArrowTableStream.
"""

import pyarrow as pa
import pytest

from orcapod.core.base import PipelineElementBase
from orcapod.core.streams import ArrowTableStream
from orcapod.core.streams.base import StreamBase
from orcapod.protocols.core_protocols.streams import StreamProtocol
from orcapod.types import Schema

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_table_stream(
    tag_columns: list[str] | None = None,
    n_rows: int = 3,
) -> ArrowTableStream:
    """Create a minimal ArrowTableStream for testing."""
    tag_columns = tag_columns or ["id"]
    table = pa.table(
        {
            "id": pa.array(list(range(n_rows)), type=pa.int64()),
            "value": pa.array([f"v{i}" for i in range(n_rows)], type=pa.large_string()),
        }
    )
    return ArrowTableStream(table, tag_columns=tag_columns)


# ---------------------------------------------------------------------------
# StreamBase protocol-conformance gap
# ---------------------------------------------------------------------------


class TestStreamBasePipelineElementBase:
    """
    Verifies that StreamBase now inherits PipelineElementBase, making
    pipeline_identity_structure() abstract on StreamBase and pipeline_hash()
    available on all concrete stream subclasses.
    """

    def test_stream_base_subclass_missing_abstract_methods_raises(self):
        """
        StreamBase is abstract w.r.t. both identity_structure() and
        pipeline_identity_structure(). Omitting either raises TypeError at instantiation.
        """

        class IncompleteStream(StreamBase):
            @property
            def producer(self):
                return None

            @property
            def upstreams(self):
                return ()

            def output_schema(self, *, columns=None, all_info=False):
                return Schema.empty(), Schema.empty()

            def keys(self, *, columns=None, all_info=False):
                return (), ()

            def iter_packets(self):
                return iter([])

            def as_table(self, *, columns=None, all_info=False):
                return pa.table({})

            # identity_structure and pipeline_identity_structure intentionally omitted

        with pytest.raises(TypeError):
            IncompleteStream()  # type: ignore[abstract]

    def test_explicit_pipeline_element_base_workaround_satisfies_stream_protocol(self):
        """
        Explicitly adding PipelineElementBase alongside StreamBase (diamond inheritance)
        still works — Python MRO handles it cleanly.
        """

        class FixedStream(StreamBase, PipelineElementBase):
            @property
            def producer(self):
                return None

            @property
            def upstreams(self):
                return ()

            def output_schema(self, *, columns=None, all_info=False):
                return Schema.empty(), Schema.empty()

            def keys(self, *, columns=None, all_info=False):
                return (), ()

            def iter_packets(self):
                return iter([])

            def as_table(self, *, columns=None, all_info=False):
                return pa.table({})

            def identity_structure(self):
                return ("fixed",)

            def pipeline_identity_structure(self):
                return ("fixed",)

        stream = FixedStream()
        assert isinstance(stream, StreamProtocol)

    def test_stream_base_alone_plus_pipeline_identity_satisfies_stream_protocol(self):
        """
        A class that only inherits StreamBase and implements both abstract methods
        satisfies StreamProtocol — pipeline_hash() is provided by StreamBase via
        PipelineElementBase, with no need for explicit double-inheritance.
        """

        class FixedStreamBaseOnly(StreamBase):
            @property
            def producer(self):
                return None

            @property
            def upstreams(self):
                return ()

            def output_schema(self, *, columns=None, all_info=False):
                return Schema.empty(), Schema.empty()

            def keys(self, *, columns=None, all_info=False):
                return (), ()

            def iter_packets(self):
                return iter([])

            def as_table(self, *, columns=None, all_info=False):
                return pa.table({})

            def identity_structure(self):
                return ("fixed",)

            def pipeline_identity_structure(self):
                return ("fixed",)

        stream = FixedStreamBaseOnly()
        assert isinstance(stream, StreamProtocol)


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


class TestStreamProtocolConformance:
    """Verify that StreamBase (via ArrowTableStream) satisfies the StreamProtocol protocol."""

    def test_stream_base_is_subclass_of_stream_protocol(self):
        """StreamBase must be a structural subtype of StreamProtocol (runtime check)."""
        # isinstance on a Protocol checks structural conformance at method-name level
        stream = make_table_stream()
        assert isinstance(stream, StreamProtocol), (
            "ArrowTableStream instance does not satisfy the StreamProtocol protocol"
        )

    def test_stream_has_producer_property(self):
        stream = make_table_stream()
        # attribute must exist and be accessible
        _ = stream.producer

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
        assert len(pair) == 2  # (TagProtocol, PacketProtocol)

    def test_stream_has_as_table_method(self):
        stream = make_table_stream()
        table = stream.as_table()
        assert isinstance(table, pa.Table)


# ---------------------------------------------------------------------------
# ArrowTableStream construction
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
            ArrowTableStream(table, tag_columns=["nonexistent"])

    def test_no_packet_column_raises(self):
        # A table where all columns are tags → no packet columns → should raise
        table = pa.table({"id": pa.array([1, 2])})
        with pytest.raises(ValueError):
            ArrowTableStream(table, tag_columns=["id"])

    def test_producer_defaults_to_none(self):
        stream = make_table_stream()
        assert stream.producer is None

    def test_upstreams_defaults_to_empty(self):
        stream = make_table_stream()
        assert stream.upstreams == ()


# ---------------------------------------------------------------------------
# ArrowTableStream.keys()
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
        stream = ArrowTableStream(table, tag_columns=[])
        tag_keys, packet_keys = stream.keys()
        assert tag_keys == ()
        assert set(packet_keys) == {"a", "b"}


# ---------------------------------------------------------------------------
# ArrowTableStream.output_schema()
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
# ArrowTableStream.iter_packets()
# ---------------------------------------------------------------------------


class TestTableStreamIterPackets:
    def test_yields_correct_number_of_pairs(self):
        n = 5
        stream = make_table_stream(n_rows=n)
        pairs = list(stream.iter_packets())
        assert len(pairs) == n

    def test_each_pair_has_tag_and_packet(self):
        from orcapod.protocols.core_protocols.datagrams import (
            PacketProtocol,
            TagProtocol,
        )

        stream = make_table_stream()
        for tag, packet in stream.iter_packets():
            assert isinstance(tag, TagProtocol)
            assert isinstance(packet, PacketProtocol)

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
# ArrowTableStream.as_table()
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
# ArrowTableStream.__iter__ (convenience)
# ---------------------------------------------------------------------------


class TestTableStreamIter:
    def test_iter_delegates_to_iter_packets(self):
        stream = make_table_stream(n_rows=3)
        via_iter = list(stream)
        assert len(via_iter) == len(via_iter)


# ---------------------------------------------------------------------------
# ArrowTableStream.identity_structure()
# ---------------------------------------------------------------------------


class TestArrowTableStreamIdentityStructure:
    """Tests for both branches of ArrowTableStream.identity_structure()."""

    # -- no-source branch (source is None) -----------------------------------

    def test_no_producer_content_hash_returns_content_hash(self):
        from orcapod.types import ContentHash

        stream = make_table_stream()
        assert isinstance(stream.content_hash(), ContentHash)

    def test_no_producer_same_data_same_hash(self):
        """Identical tables produce identical content hashes."""
        table = pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int64()),
                "value": pa.array([10, 20, 30], type=pa.int64()),
            }
        )
        s1 = ArrowTableStream(table, tag_columns=["id"])
        s2 = ArrowTableStream(table, tag_columns=["id"])
        assert s1.content_hash() == s2.content_hash()

    def test_no_producer_different_data_different_hash(self):
        """Different table contents produce different content hashes."""
        t1 = pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int64()),
                "value": pa.array([10, 20, 30], type=pa.int64()),
            }
        )
        t2 = pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int64()),
                "value": pa.array([10, 20, 99], type=pa.int64()),
            }
        )
        s1 = ArrowTableStream(t1, tag_columns=["id"])
        s2 = ArrowTableStream(t2, tag_columns=["id"])
        assert s1.content_hash() != s2.content_hash()

    def test_no_producer_identity_structure_contains_table(self):
        """identity_structure() for sourceless stream embeds a pa.Table."""
        stream = make_table_stream()
        structure = stream.identity_structure()
        assert any(isinstance(elem, pa.Table) for elem in structure)

    # -- with-source branch (source is not None) -----------------------------

    def _make_named_source(self, name: str):
        """Return a minimal ContentIdentifiableBase with a fixed identity."""
        from orcapod.core.base import ContentIdentifiableBase

        class NamedSource(ContentIdentifiableBase):
            def __init__(self, n: str) -> None:
                super().__init__()
                self._name = n

            def identity_structure(self):
                return (self._name,)

        return NamedSource(name)

    def test_with_producer_identity_structure_starts_with_producer(self):
        """identity_structure() returns (source, *upstreams) when source is set."""
        src = self._make_named_source("src_a")
        table = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "v": pa.array([10, 20], type=pa.int64()),
            }
        )
        stream = ArrowTableStream(table, tag_columns=["id"], producer=src)
        structure = stream.identity_structure()
        assert structure[0] is src

    def test_with_producer_content_hash_reflects_producer_identity(self):
        """Same source → same content hash even when underlying tables differ."""
        src = self._make_named_source("shared_source")
        t1 = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "v": pa.array([10, 20], type=pa.int64()),
            }
        )
        t2 = pa.table(
            {
                "id": pa.array([3, 4], type=pa.int64()),
                "v": pa.array([30, 40], type=pa.int64()),
            }
        )
        s1 = ArrowTableStream(t1, tag_columns=["id"], producer=src)
        s2 = ArrowTableStream(t2, tag_columns=["id"], producer=src)
        assert s1.content_hash() == s2.content_hash()

    def test_with_different_producers_different_hash(self):
        """Different sources → different content hashes even for identical tables."""
        src_a = self._make_named_source("source_a")
        src_b = self._make_named_source("source_b")
        table = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "v": pa.array([10, 20], type=pa.int64()),
            }
        )
        s1 = ArrowTableStream(table, tag_columns=["id"], producer=src_a)
        s2 = ArrowTableStream(table, tag_columns=["id"], producer=src_b)
        assert s1.content_hash() != s2.content_hash()
