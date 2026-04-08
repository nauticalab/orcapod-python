"""
Tests for FunctionPod.

Covers:
- FunctionPodProtocol protocol conformance
- Construction and properties
- process() and __call__()
- Input packet schema validation
- process_packet()
- Multi-stream (join) input
"""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa
import pytest

from orcapod.core.datagrams import Packet, Tag
from orcapod.core.function_pod import FunctionPodStream, FunctionPod
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.streams import ArrowTableStream
from orcapod.protocols.core_protocols import FunctionPodProtocol

from ..conftest import make_int_stream, to_upper


# ---------------------------------------------------------------------------
# 1. Protocol conformance
# ---------------------------------------------------------------------------


class TestSimpleFunctionPodProtocolConformance:
    def test_satisfies_function_pod_protocol(self, double_pod):
        assert isinstance(double_pod, FunctionPodProtocol), (
            "FunctionPod does not satisfy the FunctionPodProtocol protocol"
        )

    def test_has_packet_function_property(self, double_pod, double_pf):
        assert double_pod.packet_function is double_pf

    def test_has_uri_property(self, double_pod):
        uri = double_pod.uri
        assert isinstance(uri, tuple)
        assert len(uri) > 0
        assert all(isinstance(part, str) for part in uri)

    def test_has_validate_inputs_method(self, double_pod):
        double_pod.validate_inputs(make_int_stream())

    def test_has_process_packet_method(self, double_pod):
        tag = Tag({"id": 0})
        packet = Packet({"x": 5})
        out_tag, out_packet = double_pod.process_packet(tag, packet)
        assert out_tag is tag
        assert out_packet is not None

    def test_has_argument_symmetry_method(self, double_pod):
        double_pod.argument_symmetry([make_int_stream()])

    def test_has_output_schema_method(self, double_pod):
        tag_schema, packet_schema = double_pod.output_schema(make_int_stream())
        assert isinstance(tag_schema, Mapping)
        assert isinstance(packet_schema, Mapping)


# ---------------------------------------------------------------------------
# 2. Construction and properties
# ---------------------------------------------------------------------------


class TestSimpleFunctionPodConstruction:
    def test_stores_packet_function(self, double_pod, double_pf):
        assert double_pod.packet_function is double_pf

    def test_uri_contains_function_name(self, double_pod, double_pf):
        assert double_pf.canonical_function_name in double_pod.uri

    def test_uri_contains_version(self, double_pod, double_pf):
        assert f"v{double_pf.major_version}" in double_pod.uri

    def test_output_schema_packet_matches_pf_output_schema(self, double_pod, double_pf):
        _, packet_schema = double_pod.output_schema(make_int_stream())
        assert packet_schema == double_pf.output_packet_schema


# ---------------------------------------------------------------------------
# 3. process() and __call__()
# ---------------------------------------------------------------------------


class TestSimpleFunctionPodProcess:
    def test_process_returns_function_pod_stream(self, double_pod):
        assert isinstance(double_pod.process(make_int_stream()), FunctionPodStream)

    def test_call_returns_function_pod_stream(self, double_pod):
        assert isinstance(double_pod(make_int_stream()), FunctionPodStream)

    def test_call_delegates_to_process(self, double_pod):
        stream = make_int_stream(n=4)
        via_process = double_pod.process(stream)
        via_call = double_pod(stream)
        assert len(list(via_process.iter_packets())) == len(
            list(via_call.iter_packets())
        )

    def test_output_stream_producer_is_pod(self, double_pod):
        assert double_pod.process(make_int_stream()).producer is double_pod

    def test_output_stream_upstream_is_input(self, double_pod):
        input_stream = make_int_stream()
        assert input_stream in double_pod.process(input_stream).upstreams

    def test_schema_mismatch_raises(self):
        pod = FunctionPod(
            packet_function=PythonPacketFunction(to_upper, output_keys="result")
        )
        with pytest.raises(ValueError):
            pod.process(make_int_stream())

    def test_no_streams_raises(self, double_pod):
        with pytest.raises(ValueError):
            double_pod.process()

    def test_label_propagates_to_stream(self, double_pod):
        result = double_pod.process(make_int_stream(), label="my_label")
        assert result.label == "my_label"


# ---------------------------------------------------------------------------
# 4. Input schema validation
# ---------------------------------------------------------------------------


class TestSimpleFunctionPodInputSchemaValidation:
    def test_compatible_stream_does_not_raise(self, double_pod):
        double_pod.validate_inputs(make_int_stream())

    def test_wrong_key_name_raises(self, double_pod):
        stream = ArrowTableStream(
            pa.table(
                {
                    "id": pa.array([0, 1, 2], type=pa.int64()),
                    "z": pa.array([0, 1, 2], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )
        with pytest.raises(ValueError):
            double_pod.process(stream)

    def test_wrong_packet_type_raises(self, double_pod):
        stream = ArrowTableStream(
            pa.table(
                {
                    "id": pa.array([0, 1, 2], type=pa.int64()),
                    "x": pa.array(["a", "b", "c"], type=pa.large_string()),
                }
            ),
            tag_columns=["id"],
        )
        with pytest.raises(ValueError):
            double_pod.process(stream)

    def test_missing_required_key_raises(self, add_pod):
        stream = ArrowTableStream(
            pa.table(
                {
                    "id": pa.array([0, 1], type=pa.int64()),
                    "x": pa.array([0, 1], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )
        with pytest.raises(ValueError):
            add_pod.process(stream)

    def test_missing_optional_key_does_not_raise(self):
        def add_with_default(x: int, y: int = 10) -> int:
            return x + y

        pod = FunctionPod(
            packet_function=PythonPacketFunction(add_with_default, output_keys="result")
        )
        stream = ArrowTableStream(
            pa.table(
                {
                    "id": pa.array([0, 1], type=pa.int64()),
                    "x": pa.array([0, 1], type=pa.int64()),
                },
                schema=pa.schema(
                    [
                        pa.field("id", pa.int64(), nullable=False),
                        pa.field("x", pa.int64(), nullable=False),
                    ]
                ),
            ),
            tag_columns=["id"],
        )
        pod.validate_inputs(stream)

    def test_missing_optional_key_uses_default_value(self):
        def add_with_default(x: int, y: int = 10) -> int:
            return x + y

        pod = FunctionPod(
            packet_function=PythonPacketFunction(add_with_default, output_keys="result")
        )
        stream = ArrowTableStream(
            pa.table(
                {
                    "id": pa.array([0, 1], type=pa.int64()),
                    "x": pa.array([3, 5], type=pa.int64()),
                },
                schema=pa.schema(
                    [
                        pa.field("id", pa.int64(), nullable=False),
                        pa.field("x", pa.int64(), nullable=False),
                    ]
                ),
            ),
            tag_columns=["id"],
        )
        table = pod.process(stream).as_table()
        assert table.column("result").to_pylist() == [13, 15]


# ---------------------------------------------------------------------------
# 5. process_packet()
# ---------------------------------------------------------------------------


class TestSimpleFunctionPodProcessPacket:
    def test_returns_tag_and_packet_tuple(self, double_pod):
        result = double_pod.process_packet(Tag({"id": 0}), Packet({"x": 7}))
        assert len(result) == 2

    def test_output_tag_is_input_tag(self, double_pod):
        tag = Tag({"id": 42})
        out_tag, _ = double_pod.process_packet(tag, Packet({"x": 3}))
        assert out_tag is tag

    def test_output_packet_has_correct_value(self, double_pod):
        _, out_packet = double_pod.process_packet(Tag({"id": 0}), Packet({"x": 6}))
        assert out_packet is not None
        assert out_packet["result"] == 12  # 6 * 2


# ---------------------------------------------------------------------------
# 6. Multi-stream (join) input
# ---------------------------------------------------------------------------


class TestSimpleFunctionPodMultiStream:
    def test_two_streams_are_joined_before_processing(self, add_pod):
        n = 3
        stream_x = ArrowTableStream(
            pa.table(
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
            ),
            tag_columns=["id"],
        )
        stream_y = ArrowTableStream(
            pa.table(
                {
                    "id": pa.array(list(range(n)), type=pa.int64()),
                    "y": pa.array([i * 10 for i in range(n)], type=pa.int64()),
                },
                schema=pa.schema(
                    [
                        pa.field("id", pa.int64(), nullable=False),
                        pa.field("y", pa.int64(), nullable=False),
                    ]
                ),
            ),
            tag_columns=["id"],
        )
        result = add_pod.process(stream_x, stream_y)
        assert isinstance(result, FunctionPodStream)
        packets = list(result.iter_packets())
        assert len(packets) == n
        for i, (_, packet) in enumerate(packets):
            assert packet["result"] == i + i * 10  # x + y
