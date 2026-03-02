"""
Tests for DerivedSource — Phase 6 of the redesign.

DerivedSource is returned by PersistentFunctionNode.as_source() and presents the
DB-computed results of a PersistentFunctionNode as a static, reusable stream.

Coverage:
- Construction via PersistentFunctionNode.as_source()
- Protocol conformance: RootSource, StreamProtocol, PipelineElementProtocol
- source == None, upstreams == () (pure stream, no upstream pod)
- iter_packets() and as_table() raise ValueError before run()
- Correct data after PersistentFunctionNode.run()
- output_schema() and keys() delegate to origin PersistentFunctionNode
- content_hash() tied to origin PersistentFunctionNode's content hash
- Same-origin DerivedSources share content_hash
- pipeline_hash() is schema-only (RootSource base case)
- Different-data same-schema DerivedSources share pipeline_hash but differ in content_hash
- Round-trip: PersistentFunctionNode → DerivedSource → iter_packets / as_table
"""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa
import pytest

from orcapod.core.function_pod import PersistentFunctionNode
from orcapod.core.sources import DerivedSource, RootSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.protocols.hashing_protocols import PipelineElementProtocol

from ..conftest import double, make_int_stream


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_node(
    n: int = 3, db: InMemoryArrowDatabase | None = None
) -> PersistentFunctionNode:
    from orcapod.core.packet_function import PythonPacketFunction

    if db is None:
        db = InMemoryArrowDatabase()
    pf = PythonPacketFunction(double, output_keys="result")
    return PersistentFunctionNode(
        packet_function=pf,
        input_stream=make_int_stream(n=n),
        pipeline_database=db,
    )


# ---------------------------------------------------------------------------
# 1. Construction and protocol conformance
# ---------------------------------------------------------------------------


class TestDerivedSourceConstruction:
    def test_as_source_returns_derived_source(self):
        node = _make_node(n=3)
        src = node.as_source()
        assert isinstance(src, DerivedSource)

    def test_derived_source_is_root_source(self):
        src = _make_node(n=3).as_source()
        assert isinstance(src, RootSource)

    def test_derived_source_is_stream_protocol(self):
        src = _make_node(n=3).as_source()
        assert isinstance(src, StreamProtocol)

    def test_derived_source_is_pipeline_element_protocol(self):
        src = _make_node(n=3).as_source()
        assert isinstance(src, PipelineElementProtocol)

    def test_producer_is_none(self):
        """DerivedSource is a root stream — source returns None."""
        src = _make_node(n=3).as_source()
        assert src.producer is None

    def test_upstreams_is_empty(self):
        src = _make_node(n=3).as_source()
        assert src.upstreams == ()


# ---------------------------------------------------------------------------
# 2. Access before run() raises
# ---------------------------------------------------------------------------


class TestDerivedSourceBeforeRun:
    def test_iter_packets_raises_before_run(self):
        src = _make_node(n=3).as_source()
        with pytest.raises(ValueError, match="run"):
            list(src.iter_packets())

    def test_as_table_raises_before_run(self):
        src = _make_node(n=3).as_source()
        with pytest.raises(ValueError, match="run"):
            src.as_table()


# ---------------------------------------------------------------------------
# 3. Correct data after run()
# ---------------------------------------------------------------------------


class TestDerivedSourceAfterRun:
    @pytest.fixture
    def src(self) -> DerivedSource:
        node = _make_node(n=4)
        node.run()
        return node.as_source()

    def test_iter_packets_yields_correct_count(self, src):
        assert len(list(src.iter_packets())) == 4

    def test_iter_packets_yields_correct_values(self, src):
        results = sorted(p["result"] for _, p in src.iter_packets())
        assert results == [0, 2, 4, 6]

    def test_iter_packets_yields_correct_tags(self, src):
        ids = sorted(t["id"] for t, _ in src.iter_packets())
        assert ids == [0, 1, 2, 3]

    def test_as_table_returns_pyarrow_table(self, src):
        assert isinstance(src.as_table(), pa.Table)

    def test_as_table_correct_row_count(self, src):
        assert src.as_table().num_rows == 4

    def test_as_table_has_tag_column(self, src):
        assert "id" in src.as_table().column_names

    def test_as_table_has_packet_column(self, src):
        assert "result" in src.as_table().column_names

    def test_as_table_values_match_iter_packets(self, src):
        table_results = sorted(src.as_table().column("result").to_pylist())
        iter_results = sorted(p["result"] for _, p in src.iter_packets())
        assert table_results == iter_results

    def test_iter_packets_is_repeatable(self, src):
        first = [(t["id"], p["result"]) for t, p in src.iter_packets()]
        second = [(t["id"], p["result"]) for t, p in src.iter_packets()]
        assert first == second


# ---------------------------------------------------------------------------
# 4. Round-trip: node results match DerivedSource
# ---------------------------------------------------------------------------


class TestDerivedSourceRoundTrip:
    def test_derived_source_matches_node_output(self):
        """Data from DerivedSource must exactly match data from PersistentFunctionNode."""
        node = _make_node(n=5)
        # Collect from node directly
        node_results = sorted(p["result"] for _, p in node.iter_packets())

        # Now get via DerivedSource
        src = node.as_source()
        src_results = sorted(p["result"] for _, p in src.iter_packets())

        assert node_results == src_results

    def test_derived_source_tag_schema_matches_node(self):
        node = _make_node(n=3)
        node.run()
        src = node.as_source()
        node_tag_schema, _ = node.output_schema()
        src_tag_schema, _ = src.output_schema()
        assert node_tag_schema == src_tag_schema

    def test_derived_source_packet_schema_matches_node(self):
        node = _make_node(n=3)
        node.run()
        src = node.as_source()
        _, node_packet_schema = node.output_schema()
        _, src_packet_schema = src.output_schema()
        assert node_packet_schema == src_packet_schema

    def test_derived_source_can_feed_downstream_node(self):
        """DerivedSource can be used as input to another PersistentFunctionNode."""
        from orcapod.core.packet_function import PythonPacketFunction

        node1 = _make_node(n=3)
        node1.run()
        src = node1.as_source()

        # Build a second node that reads the result column as input.
        # Use a fresh DB so node2 computes cleanly without inheriting node1's records.
        result_table = pa.table(
            {
                "id": src.as_table().column("id"),
                "x": src.as_table().column("result"),
            }
        )
        result_stream = ArrowTableStream(result_table, tag_columns=["id"])

        double_result = PythonPacketFunction(double, output_keys="result")
        node2 = PersistentFunctionNode(
            packet_function=double_result,
            input_stream=result_stream,
            pipeline_database=InMemoryArrowDatabase(),  # fresh DB
        )

        # node2 doubles the already-doubled values: 0*2*2=0, 1*2*2=4, 2*2*2=8
        results = sorted(p["result"] for _, p in node2.iter_packets())
        assert results == [0, 4, 8]


# ---------------------------------------------------------------------------
# 5. output_schema and keys delegation
# ---------------------------------------------------------------------------


class TestDerivedSourceSchema:
    def test_output_schema_returns_two_mappings(self):
        node = _make_node(n=3)
        node.run()
        src = node.as_source()
        tag_schema, packet_schema = src.output_schema()
        assert isinstance(tag_schema, Mapping)
        assert isinstance(packet_schema, Mapping)

    def test_output_schema_tag_has_id(self):
        node = _make_node(n=3)
        node.run()
        src = node.as_source()
        tag_schema, _ = src.output_schema()
        assert "id" in tag_schema

    def test_output_schema_packet_has_result(self):
        node = _make_node(n=3)
        node.run()
        src = node.as_source()
        _, packet_schema = src.output_schema()
        assert "result" in packet_schema

    def test_keys_tag_has_id(self):
        node = _make_node(n=3)
        node.run()
        src = node.as_source()
        tag_keys, _ = src.keys()
        assert "id" in tag_keys

    def test_keys_packet_has_result(self):
        node = _make_node(n=3)
        node.run()
        src = node.as_source()
        _, packet_keys = src.keys()
        assert "result" in packet_keys

    def test_keys_consistent_with_output_schema(self):
        node = _make_node(n=3)
        node.run()
        src = node.as_source()
        tag_keys, packet_keys = src.keys()
        tag_schema, packet_schema = src.output_schema()
        assert set(tag_keys) == set(tag_schema.keys())
        assert set(packet_keys) == set(packet_schema.keys())


# ---------------------------------------------------------------------------
# 6. Identity — content_hash and pipeline_hash
# ---------------------------------------------------------------------------


class TestDerivedSourceIdentity:
    def test_content_hash_is_stable(self):
        node = _make_node(n=3)
        node.run()
        src = node.as_source()
        assert src.content_hash() == src.content_hash()

    def test_same_origin_same_content_hash(self):
        """Two DerivedSources from the same node have the same content_hash."""
        node = _make_node(n=3)
        node.run()
        src1 = node.as_source()
        src2 = node.as_source()
        assert src1.content_hash() == src2.content_hash()

    def test_content_hash_tied_to_origin(self):
        """DerivedSource content_hash is derived from origin PersistentFunctionNode's content_hash."""
        db = InMemoryArrowDatabase()
        node = _make_node(n=3, db=db)
        node.run()
        src = node.as_source()
        # The DerivedSource identity_structure wraps origin.content_hash()
        # so its content_hash depends on the node's content_hash
        # (changing data → different node content_hash → different src content_hash)
        node_different_data = _make_node(n=5, db=InMemoryArrowDatabase())
        node_different_data.run()
        src_different = node_different_data.as_source()
        assert src.content_hash() != src_different.content_hash()

    def test_pipeline_hash_is_stable(self):
        node = _make_node(n=3)
        node.run()
        src = node.as_source()
        assert src.pipeline_hash() == src.pipeline_hash()

    def test_pipeline_hash_is_schema_only(self):
        """
        DerivedSource inherits RootSource.pipeline_identity_structure() = (tag_schema, packet_schema).
        Two DerivedSources with identical schemas share the same pipeline_hash even if
        the underlying PersistentFunctionNode processed different data.
        """
        node_a = _make_node(n=3)
        node_a.run()
        node_b = _make_node(n=7)  # same schema, different data count
        node_b.run()
        src_a = node_a.as_source()
        src_b = node_b.as_source()
        # Same output schema → same pipeline_hash
        assert src_a.pipeline_hash() == src_b.pipeline_hash()

    def test_pipeline_hash_differs_for_different_schema(self):
        """DerivedSources with different output schemas have different pipeline_hashes."""
        from orcapod.core.packet_function import PythonPacketFunction

        def triple(x: int) -> tuple[int, int]:
            return x * 3, x + 1

        triple_pf = PythonPacketFunction(triple, output_keys=["tripled", "incremented"])
        db = InMemoryArrowDatabase()
        node_double = _make_node(n=3, db=db)
        node_double.run()
        src_double = node_double.as_source()

        node_triple = PersistentFunctionNode(
            packet_function=triple_pf,
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )
        node_triple.run()
        src_triple = node_triple.as_source()

        assert src_double.pipeline_hash() != src_triple.pipeline_hash()

    def test_same_data_different_origin_content_hash_differs(self):
        """
        Two FunctionNodes processing the same data but using different DB instances
        should still produce DerivedSources with the same content_hash (since
        content_hash depends on function + input stream, not the DB).
        """
        from orcapod.core.packet_function import PythonPacketFunction

        pf = PythonPacketFunction(double, output_keys="result")
        stream = make_int_stream(n=3)

        node_a = PersistentFunctionNode(
            packet_function=pf,
            input_stream=stream,
            pipeline_database=InMemoryArrowDatabase(),
        )
        node_b = PersistentFunctionNode(
            packet_function=pf,
            input_stream=stream,
            pipeline_database=InMemoryArrowDatabase(),
        )
        node_a.run()
        node_b.run()
        # Same function + same input stream → same content_hash → same DerivedSource content_hash
        assert node_a.as_source().content_hash() == node_b.as_source().content_hash()
