# tests/test_core/operators/test_operator_node_non_active.py
"""Tests verifying OperatorNode iteration is read-only (PLT-1182)."""

from __future__ import annotations

from unittest.mock import patch

import pyarrow as pa
import pytest

from orcapod.core.nodes import OperatorNode
from orcapod.core.operators import Join, MapPackets
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.types import CacheMode


# ---------------------------------------------------------------------------
# Fixtures and helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def simple_source() -> ArrowTableStream:
    """Single-tag stream: id (tag), x (packet), 3 rows."""
    return ArrowTableStream(
        pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int64()),
                "x": pa.array([10, 20, 30], type=pa.int64()),
            }
        ),
        tag_columns=["id"],
    )


@pytest.fixture
def map_op() -> MapPackets:
    return MapPackets({"x": "renamed_x"})


def _node(operator, streams, *, db=None, cache_mode=CacheMode.OFF):
    """Build an OperatorNode, attaching DB only when provided."""
    kwargs: dict = dict(operator=operator, input_streams=streams, cache_mode=cache_mode)
    if db is not None:
        kwargs["pipeline_database"] = db
    return OperatorNode(**kwargs)


# ---------------------------------------------------------------------------
# Core invariant: iteration never triggers computation
# ---------------------------------------------------------------------------


class TestIterPacketsIsPassive:
    def test_iter_packets_without_run_returns_empty(self, simple_source, map_op):
        """iter_packets() must not call operator.process before run()."""
        node = _node(map_op, (simple_source,))
        with patch.object(map_op, "process", wraps=map_op.process) as spy:
            result = list(node.iter_packets())
        assert result == []
        spy.assert_not_called()

    def test_as_table_without_run_returns_empty_table(self, simple_source, map_op):
        """as_table() must return a zero-row table with correct schema before run()."""
        node = _node(map_op, (simple_source,))
        table = node.as_table()
        assert table.num_rows == 0
        assert "id" in table.column_names
        assert "renamed_x" in table.column_names

    def test_dunder_iter_without_run_returns_empty(self, simple_source, map_op):
        """list(node) must return [] before run()."""
        node = _node(map_op, (simple_source,))
        assert list(node) == []

    def test_flow_without_run_returns_empty(self, simple_source, map_op):
        """node.flow() must return [] before run()."""
        node = _node(map_op, (simple_source,))
        assert node.flow() == []

    def test_iter_packets_after_run_returns_results(self, simple_source, map_op):
        """After run(), iter_packets() returns the computed packets."""
        node = _node(map_op, (simple_source,))
        node.run()
        result = list(node.iter_packets())
        assert len(result) == 3
        for _, packet in result:
            assert "renamed_x" in packet.keys()

    def test_as_table_after_run_returns_results(self, simple_source, map_op):
        """After run(), as_table() returns the computed table."""
        node = _node(map_op, (simple_source,))
        node.run()
        table = node.as_table()
        assert table.num_rows == 3
        assert "renamed_x" in table.column_names


# ---------------------------------------------------------------------------
# Cascade isolation: iterating OperatorNode(B) must not trigger OperatorNode(A)
# ---------------------------------------------------------------------------


class TestCascadeIsolation:
    def test_iter_packets_does_not_cascade_upstream_operator(self, simple_source):
        """Key regression test for PLT-1182.

        Chain: SourceNode → OperatorNode(A) → OperatorNode(B)
        Iterating B without run() must not call A's operator.process.
        """
        op_a = MapPackets({"x": "y"})
        op_b = MapPackets({"y": "z"})
        node_a = _node(op_a, (simple_source,))
        node_b = _node(op_b, (node_a,))

        with patch.object(op_a, "process", wraps=op_a.process) as spy_a:
            result = list(node_b.iter_packets())

        assert result == []
        spy_a.assert_not_called()

    def test_pipeline_run_then_iterate(self, simple_source):
        """After pipeline.run() (simulated), iteration returns correct results."""
        op_a = MapPackets({"x": "y"})
        op_b = MapPackets({"y": "z"})
        node_a = _node(op_a, (simple_source,))
        node_b = _node(op_b, (node_a,))

        # Simulate pipeline.run() — topological order
        node_a.run()
        node_b.run()

        result_a = list(node_a.iter_packets())
        result_b = list(node_b.iter_packets())
        assert len(result_a) == 3
        assert len(result_b) == 3
        for _, packet in result_b:
            assert "z" in packet.keys()


# ---------------------------------------------------------------------------
# CacheMode semantics
# ---------------------------------------------------------------------------


class TestCacheModeNonActive:
    def test_iter_packets_no_db_no_run_returns_empty(self, simple_source, map_op):
        """No DB, no run() → empty (step 3 fallback)."""
        node = OperatorNode(operator=map_op, input_streams=(simple_source,))
        assert list(node.iter_packets()) == []

    def test_iter_packets_replay_mode_no_records_returns_empty(
        self, simple_source, map_op
    ):
        """REPLAY + fresh DB (no LOG run) → empty table with correct schema."""
        db = InMemoryArrowDatabase()
        node = _node(map_op, (simple_source,), db=db, cache_mode=CacheMode.REPLAY)
        result = list(node.iter_packets())
        assert result == []

    def test_iter_packets_replay_mode_returns_db_contents(
        self, simple_source, map_op
    ):
        """REPLAY + prior LOG run → iter_packets returns DB records without run()."""
        db = InMemoryArrowDatabase()
        # Populate DB via LOG run
        node_log = _node(map_op, (simple_source,), db=db, cache_mode=CacheMode.LOG)
        node_log.run()

        # Fresh REPLAY node — no in-memory cache
        node_replay = _node(
            map_op, (simple_source,), db=db, cache_mode=CacheMode.REPLAY
        )
        result = list(node_replay.iter_packets())
        assert len(result) == 3
        for _, packet in result:
            assert "renamed_x" in packet.keys()

    def test_iter_packets_log_mode_no_run_returns_empty(
        self, simple_source, map_op
    ):
        """LOG mode: DB has prior records but iter_packets() without run() returns empty.

        This verifies the CacheMode guard in _load_cached_stream_from_db.
        """
        db = InMemoryArrowDatabase()
        # Populate DB via a LOG run so the guard is meaningfully exercised
        node_log = _node(map_op, (simple_source,), db=db, cache_mode=CacheMode.LOG)
        node_log.run()
        # Confirm records exist so we know the guard, not an empty DB, returns empty
        assert db.get_all_records(node_log.pipeline_path) is not None

        # Fresh LOG node (same DB, no in-memory cache)
        node_fresh = _node(
            map_op, (simple_source,), db=db, cache_mode=CacheMode.LOG
        )
        result = list(node_fresh.iter_packets())
        assert result == []
