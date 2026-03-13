"""Specification-derived integration tests for DB-backed caching flows.

Tests FunctionNode and OperatorNode caching behavior
as documented in the design specification.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import (
    FunctionNode,
    OperatorNode,
)
from orcapod.core.operators import Join
from orcapod.core.packet_function import CachedPacketFunction, PythonPacketFunction
from orcapod.core.sources import ArrowTableSource, DerivedSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.types import CacheMode


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _double(x: int) -> int:
    return x * 2


def _make_source(n: int = 3) -> ArrowTableSource:
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
        }
    )
    return ArrowTableSource(table, tag_columns=["id"])


# ===================================================================
# FunctionNode caching
# ===================================================================


class TestFunctionNodeCaching:
    """Per design: first run computes and stores; second run replays cached."""

    def test_first_run_computes_all(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        source = _make_source(3)
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        node = FunctionNode(
            function_pod=pod,
            input_stream=source,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node.run()
        records = node.get_all_records()
        assert records is not None
        assert records.num_rows == 3

    def test_second_run_uses_cache(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        source = _make_source(3)
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        # First run
        node1 = FunctionNode(
            function_pod=pod,
            input_stream=source,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node1.run()

        # Second run with same inputs — should use cached results
        node2 = FunctionNode(
            function_pod=pod,
            input_stream=source,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        packets = list(node2.iter_packets())
        assert len(packets) == 3


class TestDerivedSourceReingestion:
    """Per design: FunctionNode → DerivedSource → further pipeline."""

    def test_derived_source_as_pipeline_input(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        source = _make_source(3)
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()

        node = FunctionNode(
            function_pod=pod,
            input_stream=source,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node.run()

        # Create DerivedSource from the node's results
        derived = node.as_source()
        assert isinstance(derived, DerivedSource)

        # Should be able to iterate packets from derived source
        packets = list(derived.iter_packets())
        assert len(packets) == 3


# ===================================================================
# OperatorNode caching
# ===================================================================


class TestOperatorNodeCaching:
    """Per design: CacheMode.LOG stores results, REPLAY loads from DB."""

    def test_log_mode_stores_results(self):
        source_a = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2, 3], type=pa.int64()),
                    "age": pa.array([25, 30, 35], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )
        source_b = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([2, 3, 4], type=pa.int64()),
                    "score": pa.array([85, 90, 95], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )
        join = Join()
        db = InMemoryArrowDatabase()
        node = OperatorNode(
            operator=join,
            input_streams=[source_a, source_b],
            pipeline_database=db,
            cache_mode=CacheMode.LOG,
        )
        node.run()
        records = node.get_all_records()
        assert records is not None
        assert records.num_rows == 2

    def test_replay_mode_loads_from_db(self):
        source_a = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2, 3], type=pa.int64()),
                    "age": pa.array([25, 30, 35], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )
        source_b = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([2, 3, 4], type=pa.int64()),
                    "score": pa.array([85, 90, 95], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )
        join = Join()
        db = InMemoryArrowDatabase()

        # First: LOG
        node1 = OperatorNode(
            operator=join,
            input_streams=[source_a, source_b],
            pipeline_database=db,
            cache_mode=CacheMode.LOG,
        )
        node1.run()

        # Second: REPLAY
        node2 = OperatorNode(
            operator=join,
            input_streams=[source_a, source_b],
            pipeline_database=db,
            cache_mode=CacheMode.REPLAY,
        )
        node2.run()
        table = node2.as_table()
        assert table.num_rows == 2


# ===================================================================
# CachedPacketFunction end-to-end
# ===================================================================


class TestCachedPacketFunctionEndToEnd:
    """End-to-end test of CachedPacketFunction with InMemoryArrowDatabase."""

    def test_full_caching_flow(self):
        db = InMemoryArrowDatabase()
        inner_pf = PythonPacketFunction(_double, output_keys="result")
        cached_pf = CachedPacketFunction(inner_pf, result_database=db)
        cached_pf.set_auto_flush(True)

        from orcapod.core.datagrams.tag_packet import Packet

        # Process multiple packets
        for x in [1, 2, 3]:
            result = cached_pf.call(Packet({"x": x}))
            assert result is not None
            assert result["result"] == x * 2

        # All should be cached
        all_outputs = cached_pf.get_all_cached_outputs()
        assert all_outputs is not None
        assert all_outputs.num_rows == 3

        # Re-calling should use cache
        for x in [1, 2, 3]:
            result = cached_pf.call(Packet({"x": x}))
            assert result is not None
            assert result["result"] == x * 2
