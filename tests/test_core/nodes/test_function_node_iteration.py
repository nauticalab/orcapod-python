"""Tests for the refactored FunctionNode iteration semantics.

After ENG-379:
- iter_packets() is strictly read-only — never triggers computation
- Computation only via run() / execute() / async_execute()
- execute() supports a concurrent path via asyncio.gather
"""
from __future__ import annotations

from unittest.mock import patch

import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.core.executors import LocalExecutor


def _make_source(n: int = 3) -> ArrowTableSource:
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
        },
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        ),
    )
    return ArrowTableSource(table, tag_columns=["id"])


def _make_node(n: int = 3, db: InMemoryArrowDatabase | None = None) -> FunctionNode:
    def double(x: int) -> int:
        return x * 2

    pf = PythonPacketFunction(double, output_keys="result")
    pod = FunctionPod(pf)
    pipeline_db = db if db is not None else InMemoryArrowDatabase()
    return FunctionNode(pod, _make_source(n=n), pipeline_database=pipeline_db)


class TestIterPacketsReadOnly:
    def test_fresh_node_no_db_yields_nothing(self):
        """iter_packets() on a fresh node with no run() and empty DB yields nothing."""
        node = _make_node()
        assert list(node.iter_packets()) == []

    def test_iter_does_not_call_process_packet_internal(self):
        """iter_packets() never calls _process_packet_internal under any non-compute path."""
        node = _make_node()
        with patch.object(node, "_process_packet_internal") as mock_proc:
            list(node.iter_packets())
            mock_proc.assert_not_called()

    def test_iter_after_db_populated_hot_loads_without_compute(self):
        """iter_packets() on a node with DB records hot-loads without _process_packet_internal."""
        db = InMemoryArrowDatabase()
        node1 = _make_node(n=3, db=db)
        node1.run()  # populate DB

        node2 = _make_node(n=3, db=db)
        with patch.object(node2, "_process_packet_internal") as mock_proc:
            results = list(node2.iter_packets())
            mock_proc.assert_not_called()
        assert len(results) == 3

    def test_after_run_iter_yields_from_cache_no_db_query(self):
        """After run(), iter_packets() yields from _cached_output_packets without DB query."""
        node = _make_node()
        node.run()
        initial_count = len(node._cached_output_packets)
        assert initial_count == 3

        with patch.object(node, "_load_cached_entries") as mock_load:
            results = list(node.iter_packets())
            mock_load.assert_not_called()
        assert len(results) == 3

    def test_iter_twice_same_order_db_queried_once(self):
        """Two successive iter_packets() calls return same order; DB queried at most once."""
        db = InMemoryArrowDatabase()
        node1 = _make_node(n=3, db=db)
        node1.run()

        node2 = _make_node(n=3, db=db)
        with patch.object(node2, "_load_cached_entries", wraps=node2._load_cached_entries) as mock_load:
            first = [(t["id"], p["result"]) for t, p in node2.iter_packets()]
            second = [(t["id"], p["result"]) for t, p in node2.iter_packets()]
            assert mock_load.call_count <= 1  # at most one DB query
        assert first == second

    def test_cached_output_packets_keyed_by_entry_id_strings(self):
        """After run(), _cached_output_packets keys are entry_id strings, not ints."""
        node = _make_node()
        node.run()
        assert len(node._cached_output_packets) == 3
        for key in node._cached_output_packets:
            assert isinstance(key, str), f"Expected str key, got {type(key)}: {key!r}"

    def test_as_table_fresh_node_returns_empty_no_compute(self):
        """as_table() on a fresh node with no run() and empty DB returns empty table."""
        node = _make_node()
        with patch.object(node, "_process_packet_internal") as mock_proc:
            table = node.as_table()
            mock_proc.assert_not_called()
        assert isinstance(table, pa.Table)
        assert len(table) == 0

    def test_run_cache_only_is_noop(self):
        """run() on a CACHE_ONLY node returns without error and without computation."""
        from orcapod.pipeline.serialization import LoadStatus

        node = _make_node()
        node._load_status = LoadStatus.CACHE_ONLY
        node._input_stream = None  # simulate no upstream

        with patch.object(node, "execute") as mock_exec:
            node.run()
            mock_exec.assert_not_called()

    def test_run_unavailable_raises(self):
        """run() on an UNAVAILABLE node raises RuntimeError."""
        from orcapod.pipeline.serialization import LoadStatus

        node = _make_node()
        node._load_status = LoadStatus.UNAVAILABLE
        with pytest.raises(RuntimeError, match="unavailable"):
            node.run()

    def test_execute_concurrent_error_policy_continue(self):
        """execute() fires on_packet_crash per failing packet and returns successes when error_policy='continue'."""
        errors = []

        def sometimes_fail(x: int) -> int:
            if x == 1:
                raise ValueError("intentional failure")
            return x * 2

        pf = PythonPacketFunction(sometimes_fail, output_keys="result")
        pf.executor = LocalExecutor()  # sets executor (LocalExecutor.supports_concurrent_execution is False)
        pod = FunctionPod(pf)
        db = InMemoryArrowDatabase()
        node = FunctionNode(pod, _make_source(n=3), pipeline_database=db)

        from orcapod.pipeline.observer import NoOpObserver

        class CapturingObserver(NoOpObserver):
            def on_packet_crash(self, node_label, tag, packet, exc):
                errors.append(exc)

        results = node.execute(node._input_stream, observer=CapturingObserver(), error_policy="continue")
        assert len(errors) == 1
        assert isinstance(errors[0], ValueError)
        # Two non-failing packets should succeed
        assert len(results) == 2
