"""Specification-derived tests for FunctionPod and FunctionPodStream.

Tests based on FunctionPodProtocol and documented behaviors:
- FunctionPod wraps a PacketFunction for per-packet transformation
- Never inspects or modifies tags
- Exactly one input stream
- output_schema() prediction matches actual output
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.datagrams.tag_packet import Packet, Tag
from orcapod.core.function_pod import FunctionPod, FunctionPodStream, function_pod
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.types import Schema


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _double(x: int) -> int:
    return x * 2


def _add(x: int, y: int) -> int:
    return x + y


def _make_stream(n: int = 3) -> ArrowTableSource:
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
        }
    )
    return ArrowTableSource(table, tag_columns=["id"])


def _make_two_col_stream(n: int = 3) -> ArrowTableSource:
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
            "y": pa.array([i * 10 for i in range(n)], type=pa.int64()),
        }
    )
    return ArrowTableSource(table, tag_columns=["id"])


# ---------------------------------------------------------------------------
# FunctionPod construction and processing
# ---------------------------------------------------------------------------


class TestFunctionPodProcess:
    """Per FunctionPodProtocol, process() accepts exactly one stream and
    returns a FunctionPodStream."""

    def test_process_returns_function_pod_stream(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()
        result = pod.process(stream)
        assert isinstance(result, FunctionPodStream)

    def test_callable_alias(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()
        result = pod(stream)
        assert isinstance(result, FunctionPodStream)

    def test_validate_inputs_rejects_multiple_streams(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        s1 = _make_stream()
        s2 = _make_stream()
        with pytest.raises(Exception):
            pod.validate_inputs(s1, s2)

    def test_validate_inputs_accepts_single_stream(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()
        pod.validate_inputs(stream)  # Should not raise


class TestFunctionPodTagInvariant:
    """Per the strict boundary: function pods NEVER inspect or modify tags."""

    def test_tags_pass_through_unchanged(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()
        result = pod.process(stream)

        input_tags = [tag for tag, _ in stream.iter_packets()]
        output_tags = [tag for tag, _ in result.iter_packets()]

        for in_tag, out_tag in zip(input_tags, output_tags):
            # Tag data columns should be identical
            assert in_tag.keys() == out_tag.keys()
            for key in in_tag.keys():
                assert in_tag[key] == out_tag[key]

    def test_packets_are_transformed(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()
        result = pod.process(stream)

        for tag, packet in result.iter_packets():
            assert "result" in packet.keys()


class TestFunctionPodOutputSchema:
    """Per PodProtocol, output_schema() must match the actual output."""

    def test_output_schema_matches_actual(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()

        predicted_tag_schema, predicted_packet_schema = pod.output_schema(stream)
        result = pod.process(stream)
        actual_tag_schema, actual_packet_schema = result.output_schema()

        # Tag schemas should match
        assert set(predicted_tag_schema.keys()) == set(actual_tag_schema.keys())
        # Packet schemas should match
        assert set(predicted_packet_schema.keys()) == set(actual_packet_schema.keys())


# ---------------------------------------------------------------------------
# FunctionPodStream
# ---------------------------------------------------------------------------


class TestFunctionPodStream:
    """Per design, FunctionPodStream is lazy — computation happens on iteration."""

    def test_producer_is_function_pod(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()
        result = pod.process(stream)
        assert result.producer is pod

    def test_upstreams_contains_input_stream(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()
        result = pod.process(stream)
        assert stream in result.upstreams

    def test_keys_matches_output_schema(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()
        result = pod.process(stream)
        tag_keys, packet_keys = result.keys()
        tag_schema, packet_schema = result.output_schema()
        assert set(tag_keys) == set(tag_schema.keys())
        assert set(packet_keys) == set(packet_schema.keys())

    def test_as_table_materialization(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream(3)
        result = pod.process(stream)
        table = result.as_table()
        assert isinstance(table, pa.Table)
        assert table.num_rows == 3

    def test_iter_packets_yields_correct_count(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream(5)
        result = pod.process(stream)
        packets = list(result.iter_packets())
        assert len(packets) == 5

    def test_clear_cache_forces_recompute(self):
        pf = PythonPacketFunction(_double, output_keys="result")
        pod = FunctionPod(packet_function=pf)
        stream = _make_stream()
        result = pod.process(stream)
        # Materialize
        list(result.iter_packets())
        # Clear and re-iterate
        result.clear_cache()
        packets = list(result.iter_packets())
        assert len(packets) == 3


# ---------------------------------------------------------------------------
# @function_pod decorator
# ---------------------------------------------------------------------------


class TestFunctionPodDecorator:
    """Per design, the @function_pod decorator adds a .pod attribute."""

    def test_decorator_creates_pod_attribute(self):
        @function_pod(output_keys="result")
        def my_double(x: int) -> int:
            return x * 2

        assert hasattr(my_double, "pod")
        assert isinstance(my_double.pod, FunctionPod)

    def test_decorated_function_still_callable(self):
        @function_pod(output_keys="result")
        def my_double(x: int) -> int:
            return x * 2

        # The pod can process streams
        stream = _make_stream()
        result = my_double.pod.process(stream)
        packets = list(result.iter_packets())
        assert len(packets) == 3
