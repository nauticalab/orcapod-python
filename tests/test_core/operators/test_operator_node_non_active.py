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
