"""
Tests for PersistentOperatorNode covering:
- Construction, producer, upstreams
- pipeline_path structure
- output_schema and keys
- identity_structure, content_hash, pipeline_identity_structure, pipeline_hash
- run() + get_all_records: DB storage and retrieval
- iter_packets / as_table stream interface
- Staleness detection
- as_source (DerivedSource round-trip)
- Argument symmetry: commutative operators produce same pipeline_hash regardless of input order
- StreamProtocol conformance
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.nodes import PersistentOperatorNode
from orcapod.core.operators import (
    DropPacketColumns,
    Join,
    MapPackets,
    SelectTagColumns,
)
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.protocols.hashing_protocols import PipelineElementProtocol
from orcapod.types import CacheMode


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def simple_stream() -> ArrowTableStream:
    """Stream with 1 tag (id) and 1 packet column (x)."""
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "x": pa.array([10, 20, 30], type=pa.int64()),
        }
    )
    return ArrowTableStream(table, tag_columns=["id"])


@pytest.fixture
def two_packet_stream() -> ArrowTableStream:
    """Stream with 1 tag (id) and 2 packet columns (x, y)."""
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "x": pa.array([10, 20, 30], type=pa.int64()),
            "y": pa.array([100, 200, 300], type=pa.int64()),
        }
    )
    return ArrowTableStream(table, tag_columns=["id"])


@pytest.fixture
def left_stream() -> ArrowTableStream:
    """Left stream for binary operator tests."""
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "value_a": pa.array([10, 20, 30], type=pa.int64()),
        }
    )
    return ArrowTableStream(table, tag_columns=["id"])


@pytest.fixture
def right_stream() -> ArrowTableStream:
    """Right stream for binary operator tests."""
    table = pa.table(
        {
            "id": pa.array([2, 3, 4], type=pa.int64()),
            "value_b": pa.array([200, 300, 400], type=pa.int64()),
        }
    )
    return ArrowTableStream(table, tag_columns=["id"])


@pytest.fixture
def db() -> InMemoryArrowDatabase:
    return InMemoryArrowDatabase()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_node(
    operator,
    streams: tuple[ArrowTableStream, ...],
    db: InMemoryArrowDatabase | None = None,
    prefix: tuple[str, ...] = (),
    cache_mode: CacheMode = CacheMode.OFF,
) -> PersistentOperatorNode:
    if db is None:
        db = InMemoryArrowDatabase()
    return PersistentOperatorNode(
        operator=operator,
        input_streams=streams,
        pipeline_database=db,
        cache_mode=cache_mode,
        pipeline_path_prefix=prefix,
    )


# ---------------------------------------------------------------------------
# Construction and basic properties
# ---------------------------------------------------------------------------


class TestOperatorNodeConstruction:
    def test_producer_is_operator(self, simple_stream):
        op = MapPackets({"x": "renamed_x"})
        node = _make_node(op, (simple_stream,))
        assert node.producer is op

    def test_upstreams_match_input(self, simple_stream):
        op = MapPackets({"x": "renamed_x"})
        node = _make_node(op, (simple_stream,))
        assert node.upstreams == (simple_stream,)

    def test_output_schema(self, simple_stream):
        op = MapPackets({"x": "renamed_x"})
        node = _make_node(op, (simple_stream,))
        tag_schema, packet_schema = node.output_schema()
        assert "id" in tag_schema
        assert "renamed_x" in packet_schema
        assert "x" not in packet_schema

    def test_keys(self, simple_stream):
        op = MapPackets({"x": "renamed_x"})
        node = _make_node(op, (simple_stream,))
        tag_keys, packet_keys = node.keys()
        assert "id" in tag_keys
        assert "renamed_x" in packet_keys

    def test_stream_protocol_conformance(self, simple_stream):
        op = MapPackets({"x": "renamed_x"})
        node = _make_node(op, (simple_stream,))
        assert isinstance(node, StreamProtocol)

    def test_pipeline_element_conformance(self, simple_stream):
        op = MapPackets({"x": "renamed_x"})
        node = _make_node(op, (simple_stream,))
        assert isinstance(node, PipelineElementProtocol)


# ---------------------------------------------------------------------------
# Pipeline path
# ---------------------------------------------------------------------------


class TestOperatorNodePipelinePath:
    def test_pipeline_path_contains_operator_uri(self, simple_stream):
        op = MapPackets({"x": "renamed_x"})
        node = _make_node(op, (simple_stream,))
        # pipeline_path = prefix + operator.uri + (f"node:{pipeline_hash}",)
        path = node.pipeline_path
        # operator.uri is a tuple starting with the class name
        assert any("MapPackets" in segment for segment in path)

    def test_pipeline_path_ends_with_node_hash(self, simple_stream):
        op = MapPackets({"x": "renamed_x"})
        node = _make_node(op, (simple_stream,))
        path = node.pipeline_path
        assert path[-1].startswith("node:")

    def test_pipeline_path_prefix(self, simple_stream):
        op = MapPackets({"x": "renamed_x"})
        prefix = ("my_pipeline", "v1")
        node = _make_node(op, (simple_stream,), prefix=prefix)
        path = node.pipeline_path
        assert path[:2] == prefix

    def test_no_tag_schema_hash_in_path(self, simple_stream):
        op = MapPackets({"x": "renamed_x"})
        node = _make_node(op, (simple_stream,))
        path = node.pipeline_path
        assert not any(segment.startswith("tag:") for segment in path)


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------


class TestOperatorNodeIdentity:
    def test_identity_structure_contains_operator(self, simple_stream):
        op = MapPackets({"x": "renamed_x"})
        node = _make_node(op, (simple_stream,))
        identity = node.identity_structure()
        assert op in identity

    def test_content_hash_is_stable(self, simple_stream):
        op = MapPackets({"x": "renamed_x"})
        node1 = _make_node(op, (simple_stream,))
        node2 = _make_node(op, (simple_stream,))
        assert node1.content_hash() == node2.content_hash()

    def test_pipeline_hash_is_stable(self, simple_stream):
        op = MapPackets({"x": "renamed_x"})
        node1 = _make_node(op, (simple_stream,))
        node2 = _make_node(op, (simple_stream,))
        assert node1.pipeline_hash() == node2.pipeline_hash()

    def test_different_operator_different_hash(self, simple_stream):
        op1 = MapPackets({"x": "renamed_x"})
        op2 = MapPackets({"x": "other_name"})
        node1 = _make_node(op1, (simple_stream,))
        node2 = _make_node(op2, (simple_stream,))
        assert node1.content_hash() != node2.content_hash()
        assert node1.pipeline_hash() != node2.pipeline_hash()

    def test_different_input_different_content_hash(self):
        table1 = pa.table({"id": [1, 2], "x": [10, 20]})
        table2 = pa.table({"id": [3, 4], "x": [30, 40]})
        s1 = ArrowTableStream(table1, tag_columns=["id"])
        s2 = ArrowTableStream(table2, tag_columns=["id"])
        op = MapPackets({"x": "y"})
        node1 = _make_node(op, (s1,))
        node2 = _make_node(op, (s2,))
        assert node1.content_hash() != node2.content_hash()

    def test_same_schema_same_pipeline_hash(self):
        """Different data but same schema → same pipeline_hash."""
        table1 = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "x": pa.array([10, 20], type=pa.int64()),
            }
        )
        table2 = pa.table(
            {
                "id": pa.array([3, 4], type=pa.int64()),
                "x": pa.array([30, 40], type=pa.int64()),
            }
        )
        s1 = ArrowTableStream(table1, tag_columns=["id"])
        s2 = ArrowTableStream(table2, tag_columns=["id"])
        op = MapPackets({"x": "y"})
        node1 = _make_node(op, (s1,))
        node2 = _make_node(op, (s2,))
        assert node1.pipeline_hash() == node2.pipeline_hash()


# ---------------------------------------------------------------------------
# Argument symmetry
# ---------------------------------------------------------------------------


class TestOperatorNodeArgumentSymmetry:
    def test_join_swapped_inputs_same_pipeline_hash(self, left_stream, right_stream):
        """Join is commutative — swapped inputs produce same pipeline_hash."""
        op = Join()
        node1 = _make_node(op, (left_stream, right_stream))
        node2 = _make_node(op, (right_stream, left_stream))
        assert node1.pipeline_hash() == node2.pipeline_hash()

    def test_join_swapped_inputs_same_content_hash(self, left_stream, right_stream):
        op = Join()
        node1 = _make_node(op, (left_stream, right_stream))
        node2 = _make_node(op, (right_stream, left_stream))
        assert node1.content_hash() == node2.content_hash()


# ---------------------------------------------------------------------------
# Run, DB storage, and retrieval
# ---------------------------------------------------------------------------


class TestOperatorNodeRunAndStorage:
    def test_run_off_does_not_write_db(self, simple_stream, db):
        """CacheMode.OFF: compute but do not write to DB."""
        op = MapPackets({"x": "renamed_x"})
        node = _make_node(op, (simple_stream,), db=db, cache_mode=CacheMode.OFF)
        node.run()
        records = node.get_all_records()
        assert records is None  # OFF never writes

    def test_run_log_populates_db(self, simple_stream, db):
        """CacheMode.LOG: compute and write to DB."""
        op = MapPackets({"x": "renamed_x"})
        node = _make_node(op, (simple_stream,), db=db, cache_mode=CacheMode.LOG)
        node.run()
        records = node.get_all_records()
        assert records is not None
        assert records.num_rows == 3

    def test_get_all_records_before_run_returns_none(self, simple_stream, db):
        op = MapPackets({"x": "renamed_x"})
        node = _make_node(op, (simple_stream,), db=db, cache_mode=CacheMode.LOG)
        records = node.get_all_records()
        assert records is None

    def test_get_all_records_has_correct_columns(self, simple_stream, db):
        op = MapPackets({"x": "renamed_x"})
        node = _make_node(op, (simple_stream,), db=db, cache_mode=CacheMode.LOG)
        node.run()
        records = node.get_all_records()
        assert records is not None
        assert "id" in records.column_names
        assert "renamed_x" in records.column_names

    def test_get_all_records_column_config_source(self, simple_stream, db):
        op = MapPackets({"x": "renamed_x"})
        node = _make_node(op, (simple_stream,), db=db, cache_mode=CacheMode.LOG)
        node.run()
        records = node.get_all_records(columns={"source": True})
        assert records is not None
        source_cols = [c for c in records.column_names if c.startswith("_source_")]
        assert len(source_cols) > 0

    def test_run_idempotent(self, simple_stream, db):
        op = MapPackets({"x": "renamed_x"})
        node = _make_node(op, (simple_stream,), db=db, cache_mode=CacheMode.LOG)
        node.run()
        records1 = node.get_all_records()
        node.run()  # second run should be no-op (cached)
        records2 = node.get_all_records()
        assert records1 is not None and records2 is not None
        assert records1.num_rows == records2.num_rows

    def test_iter_packets(self, simple_stream, db):
        op = MapPackets({"x": "renamed_x"})
        node = _make_node(op, (simple_stream,), db=db)
        packets = list(node.iter_packets())
        assert len(packets) == 3
        for tag, packet in packets:
            assert "renamed_x" in packet.keys()

    def test_as_table(self, simple_stream, db):
        op = MapPackets({"x": "renamed_x"})
        node = _make_node(op, (simple_stream,), db=db)
        table = node.as_table()
        assert table.num_rows == 3
        assert "renamed_x" in table.column_names

    def test_join_run_and_retrieve(self, left_stream, right_stream, db):
        op = Join()
        node = _make_node(
            op, (left_stream, right_stream), db=db, cache_mode=CacheMode.LOG
        )
        node.run()
        records = node.get_all_records()
        assert records is not None
        # Join on id=[2,3] common keys
        assert records.num_rows == 2
        assert "value_a" in records.column_names
        assert "value_b" in records.column_names

    def test_drop_columns_run_and_retrieve(self, two_packet_stream, db):
        op = DropPacketColumns("y")
        node = _make_node(op, (two_packet_stream,), db=db, cache_mode=CacheMode.LOG)
        node.run()
        records = node.get_all_records()
        assert records is not None
        assert records.num_rows == 3
        assert "x" in records.column_names
        assert "y" not in records.column_names

    def test_replay_from_cache(self, simple_stream, db):
        """CacheMode.REPLAY: skip computation, load from DB."""
        op = MapPackets({"x": "renamed_x"})
        # First, populate cache with LOG mode
        node_log = _make_node(op, (simple_stream,), db=db, cache_mode=CacheMode.LOG)
        node_log.run()

        # Then replay from cache
        node_replay = _make_node(
            op, (simple_stream,), db=db, cache_mode=CacheMode.REPLAY
        )
        table = node_replay.as_table()
        assert table.num_rows == 3
        assert "renamed_x" in table.column_names

    def test_replay_no_cache_returns_empty_stream(self, simple_stream, db):
        """CacheMode.REPLAY with no cached data yields an empty stream."""
        op = MapPackets({"x": "renamed_x"})
        node = _make_node(op, (simple_stream,), db=db, cache_mode=CacheMode.REPLAY)
        node.run()
        table = node.as_table()
        assert table.num_rows == 0
        # Schema is still correct
        tag_keys, packet_keys = node.keys()
        assert set(tag_keys).issubset(set(table.column_names))
        assert set(packet_keys).issubset(set(table.column_names))


# ---------------------------------------------------------------------------
# DerivedSource
# ---------------------------------------------------------------------------


class TestOperatorNodeDerivedSource:
    def test_as_source_returns_derived_source(self, simple_stream, db):
        from orcapod.core.sources.derived_source import DerivedSource

        op = MapPackets({"x": "renamed_x"})
        node = _make_node(op, (simple_stream,), db=db, cache_mode=CacheMode.LOG)
        node.run()
        source = node.as_source()
        assert isinstance(source, DerivedSource)

    def test_as_source_round_trip(self, simple_stream, db):
        op = MapPackets({"x": "renamed_x"})
        node = _make_node(op, (simple_stream,), db=db, cache_mode=CacheMode.LOG)
        node.run()
        source = node.as_source()
        # iter_packets should yield the same data
        packets = list(source.iter_packets())
        assert len(packets) == 3

    def test_as_source_schema_matches(self, simple_stream, db):
        op = MapPackets({"x": "renamed_x"})
        node = _make_node(op, (simple_stream,), db=db, cache_mode=CacheMode.LOG)
        node.run()
        source = node.as_source()
        assert source.output_schema() == node.output_schema()

    def test_as_source_before_run_returns_empty(self, simple_stream, db):
        op = MapPackets({"x": "renamed_x"})
        node = _make_node(op, (simple_stream,), db=db, cache_mode=CacheMode.LOG)
        source = node.as_source()
        # Before run, DerivedSource returns an empty stream (zero rows)
        assert list(source.iter_packets()) == []
        table = source.as_table()
        assert table.num_rows == 0
        assert "renamed_x" in table.column_names


# ---------------------------------------------------------------------------
# Repr
# ---------------------------------------------------------------------------


class TestOperatorNodeRepr:
    def test_repr(self, simple_stream):
        op = MapPackets({"x": "renamed_x"})
        node = _make_node(op, (simple_stream,))
        r = repr(node)
        assert "PersistentOperatorNode" in r
