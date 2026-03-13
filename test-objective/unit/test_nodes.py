"""Specification-derived tests for FunctionNode, OperatorNode, and
Persistent variants.

Tests based on design specification:
- FunctionNode: in-memory function pod execution as a stream
- FunctionNode: two-phase iteration (cached first, compute missing)
- OperatorNode: operator execution as a stream
- PersistentOperatorNode: CacheMode behavior (OFF/LOG/REPLAY)
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import (
    FunctionNode,
    OperatorNode,
    FunctionNode,
    PersistentOperatorNode,
)
from orcapod.core.operators import Join
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import DerivedSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.types import CacheMode


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _double(x: int) -> int:
    return x * 2


def _make_stream(n: int = 3) -> ArrowTableStream:
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
        }
    )
    return ArrowTableStream(table, tag_columns=["id"])


def _make_joinable_streams() -> tuple[ArrowTableStream, ArrowTableStream]:
    left = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "age": pa.array([25, 30, 35], type=pa.int64()),
        }
    )
    right = pa.table(
        {
            "id": pa.array([2, 3, 4], type=pa.int64()),
            "score": pa.array([85, 90, 95], type=pa.int64()),
        }
    )
    return (
        ArrowTableStream(left, tag_columns=["id"]),
        ArrowTableStream(right, tag_columns=["id"]),
    )


# ===================================================================
# FunctionNode
# ===================================================================


class TestFunctionNode:
    """Per design, FunctionNode wraps a FunctionPod for stream-based execution."""

    def test_iter_packets(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream(3)
        node = FunctionNode(function_pod=pod, input_stream=stream)
        packets = list(node.iter_packets())
        assert len(packets) == 3
        for tag, packet in packets:
            assert "result" in packet.keys()

    def test_process_packet(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()
        node = FunctionNode(function_pod=pod, input_stream=stream)
        # Get first tag/packet from input
        tag, packet = next(iter(stream.iter_packets()))
        out_tag, out_packet = node.process_packet(tag, packet)
        assert out_packet is not None
        assert "result" in out_packet.keys()

    def test_producer_is_function_pod(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()
        node = FunctionNode(function_pod=pod, input_stream=stream)
        assert node.producer is pod

    def test_upstreams(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()
        node = FunctionNode(function_pod=pod, input_stream=stream)
        assert stream in node.upstreams

    def test_clear_cache(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()
        node = FunctionNode(function_pod=pod, input_stream=stream)
        list(node.iter_packets())
        node.clear_cache()
        # Should be able to iterate again after clearing
        packets = list(node.iter_packets())
        assert len(packets) == 3


# ===================================================================
# FunctionNode
# ===================================================================


class TestFunctionNode:
    """Per design: two-phase iteration — Phase 1 returns cached records,
    Phase 2 computes missing. Uses pipeline_hash for DB path scoping."""

    def test_caches_computed_results(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream(3)
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        node = FunctionNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        # First iteration computes all
        packets = list(node.iter_packets())
        assert len(packets) == 3

    def test_run_eagerly_processes_all(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream(3)
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        node = FunctionNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node.run()
        # After run, results should be in DB
        records = node.get_all_records()
        assert records is not None
        assert records.num_rows == 3

    def test_as_source_returns_derived_source(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream(3)
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        node = FunctionNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        node.run()
        source = node.as_source()
        assert isinstance(source, DerivedSource)

    def test_pipeline_path_uses_pipeline_hash(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        node = FunctionNode(
            function_pod=pod,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        path = node.pipeline_path
        assert isinstance(path, tuple)
        assert len(path) > 0


# ===================================================================
# OperatorNode
# ===================================================================


class TestOperatorNode:
    """Per design, OperatorNode wraps an operator for stream-based execution."""

    def test_delegates_to_operator(self):
        join = Join()
        s1, s2 = _make_joinable_streams()
        node = OperatorNode(operator=join, input_streams=[s1, s2])
        node.run()
        table = node.as_table()
        assert table.num_rows == 2  # Inner join on id=2, id=3

    def test_clear_cache(self):
        join = Join()
        s1, s2 = _make_joinable_streams()
        node = OperatorNode(operator=join, input_streams=[s1, s2])
        node.run()
        node.clear_cache()
        # Should be able to run again
        node.run()
        table = node.as_table()
        assert table.num_rows == 2


# ===================================================================
# PersistentOperatorNode
# ===================================================================


class TestPersistentOperatorNode:
    """Per design, supports CacheMode: OFF (always compute), LOG (compute+store),
    REPLAY (load from DB)."""

    def test_cache_mode_off(self):
        join = Join()
        s1, s2 = _make_joinable_streams()
        db = InMemoryArrowDatabase()
        node = PersistentOperatorNode(
            operator=join,
            input_streams=[s1, s2],
            pipeline_database=db,
            cache_mode=CacheMode.OFF,
        )
        node.run()
        table = node.as_table()
        assert table.num_rows == 2

    def test_cache_mode_log(self):
        join = Join()
        s1, s2 = _make_joinable_streams()
        db = InMemoryArrowDatabase()
        node = PersistentOperatorNode(
            operator=join,
            input_streams=[s1, s2],
            pipeline_database=db,
            cache_mode=CacheMode.LOG,
        )
        node.run()
        # Results should be stored in DB
        records = node.get_all_records()
        assert records is not None
        assert records.num_rows == 2

    def test_cache_mode_replay(self):
        join = Join()
        s1, s2 = _make_joinable_streams()
        db = InMemoryArrowDatabase()

        # First: LOG to populate DB
        node1 = PersistentOperatorNode(
            operator=join,
            input_streams=[s1, s2],
            pipeline_database=db,
            cache_mode=CacheMode.LOG,
        )
        node1.run()

        # Second: REPLAY to load from DB
        node2 = PersistentOperatorNode(
            operator=join,
            input_streams=[s1, s2],
            pipeline_database=db,
            cache_mode=CacheMode.REPLAY,
        )
        node2.run()
        table = node2.as_table()
        assert table.num_rows == 2

    def test_as_source_returns_derived_source(self):
        join = Join()
        s1, s2 = _make_joinable_streams()
        db = InMemoryArrowDatabase()
        node = PersistentOperatorNode(
            operator=join,
            input_streams=[s1, s2],
            pipeline_database=db,
            cache_mode=CacheMode.LOG,
        )
        node.run()
        source = node.as_source()
        assert isinstance(source, DerivedSource)
