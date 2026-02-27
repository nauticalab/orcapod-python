"""
Tests for SimpleFunctionPod, FunctionPodStream, and the function_pod decorator.

Covers:
- FunctionPod protocol conformance for SimpleFunctionPod
- Stream protocol conformance for FunctionPodStream
- Core behaviour: process(), __call__(), process_packet()
- FunctionPodStream: keys(), output_schema(), iter_packets(), as_table()
- Caching and repeatability of iteration
- Multi-stream (join) input
- Schema validation error path
- function_pod decorator: pod attachment, protocol conformance, end-to-end processing
"""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa
import pytest

from orcapod.core.datagrams import DictPacket, DictTag
from orcapod.core.function_pod import FunctionPodStream, SimpleFunctionPod, function_pod
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.streams import TableStream
from orcapod.protocols.core_protocols import FunctionPod, Stream

# ---------------------------------------------------------------------------
# Helper functions and fixtures
# ---------------------------------------------------------------------------


def double(x: int) -> int:
    return x * 2


def add(x: int, y: int) -> int:
    return x + y


def to_upper(name: str) -> str:
    return name.upper()


@pytest.fixture
def double_pf() -> PythonPacketFunction:
    return PythonPacketFunction(double, output_keys="result")


@pytest.fixture
def add_pf() -> PythonPacketFunction:
    return PythonPacketFunction(add, output_keys="result")


@pytest.fixture
def double_pod(double_pf) -> SimpleFunctionPod:
    return SimpleFunctionPod(packet_function=double_pf)


@pytest.fixture
def add_pod(add_pf) -> SimpleFunctionPod:
    return SimpleFunctionPod(packet_function=add_pf)


def make_int_stream(n: int = 3) -> TableStream:
    """TableStream with tag=id (int), packet=x (int)."""
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
        }
    )
    return TableStream(table, tag_columns=["id"])


def make_two_col_stream(n: int = 3) -> TableStream:
    """TableStream with tag=id, packet={x, y} for add_pf."""
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
            "y": pa.array([i * 10 for i in range(n)], type=pa.int64()),
        }
    )
    return TableStream(table, tag_columns=["id"])


# ---------------------------------------------------------------------------
# 1. SimpleFunctionPod — FunctionPod protocol conformance
# ---------------------------------------------------------------------------


class TestSimpleFunctionPodProtocolConformance:
    def test_satisfies_function_pod_protocol(self, double_pod):
        assert isinstance(double_pod, FunctionPod), (
            "SimpleFunctionPod does not satisfy the FunctionPod protocol"
        )

    def test_has_packet_function_property(self, double_pod, double_pf):
        assert double_pod.packet_function is double_pf

    def test_has_uri_property(self, double_pod):
        uri = double_pod.uri
        assert isinstance(uri, tuple)
        assert len(uri) > 0
        assert all(isinstance(part, str) for part in uri)

    def test_has_validate_inputs_method(self, double_pod):
        stream = make_int_stream()
        # Compatible stream — must not raise
        double_pod.validate_inputs(stream)

    def test_has_process_packet_method(self, double_pod):
        tag = DictTag({"id": 0})
        packet = DictPacket({"x": 5})
        out_tag, out_packet = double_pod.process_packet(tag, packet)
        assert out_tag is tag
        assert out_packet is not None

    def test_has_argument_symmetry_method(self, double_pod):
        stream = make_int_stream()
        # Should not raise
        double_pod.argument_symmetry([stream])

    def test_has_output_schema_method(self, double_pod):
        stream = make_int_stream()
        tag_schema, packet_schema = double_pod.output_schema(stream)
        assert isinstance(tag_schema, Mapping)
        assert isinstance(packet_schema, Mapping)


# ---------------------------------------------------------------------------
# 2. SimpleFunctionPod — construction and properties
# ---------------------------------------------------------------------------


class TestSimpleFunctionPodConstruction:
    def test_stores_packet_function(self, double_pod, double_pf):
        assert double_pod.packet_function is double_pf

    def test_uri_contains_function_name(self, double_pod, double_pf):
        assert double_pf.canonical_function_name in double_pod.uri

    def test_uri_contains_version(self, double_pod, double_pf):
        version_component = f"v{double_pf.major_version}"
        assert version_component in double_pod.uri

    def test_output_schema_packet_matches_pf_output_schema(self, double_pod, double_pf):
        stream = make_int_stream()
        _, packet_schema = double_pod.output_schema(stream)
        assert packet_schema == double_pf.output_packet_schema


# ---------------------------------------------------------------------------
# 3. SimpleFunctionPod — process() and __call__()
# ---------------------------------------------------------------------------


class TestSimpleFunctionPodProcess:
    def test_process_returns_function_pod_stream(self, double_pod):
        stream = make_int_stream()
        result = double_pod.process(stream)
        assert isinstance(result, FunctionPodStream)

    def test_call_returns_function_pod_stream(self, double_pod):
        stream = make_int_stream()
        result = double_pod(stream)
        assert isinstance(result, FunctionPodStream)

    def test_call_delegates_to_process(self, double_pod):
        stream = make_int_stream(n=4)
        via_process = double_pod.process(stream)
        via_call = double_pod(stream)
        # Both produce streams with the same row count
        assert len(list(via_process.iter_packets())) == len(
            list(via_call.iter_packets())
        )

    def test_output_stream_source_is_pod(self, double_pod):
        stream = make_int_stream()
        result = double_pod.process(stream)
        assert result.source is double_pod

    def test_output_stream_upstream_is_input(self, double_pod):
        input_stream = make_int_stream()
        result = double_pod.process(input_stream)
        assert input_stream in result.upstreams

    def test_schema_mismatch_raises(self):
        """process() should raise when stream schema is incompatible."""
        string_pf = PythonPacketFunction(to_upper, output_keys="result")
        pod = SimpleFunctionPod(packet_function=string_pf)
        # int stream is incompatible with string function
        int_stream = make_int_stream()
        with pytest.raises(ValueError):
            pod.process(int_stream)

    def test_no_streams_raises(self, double_pod):
        with pytest.raises(ValueError):
            double_pod.process()

    def test_label_propagates_to_stream(self, double_pod):
        stream = make_int_stream()
        result = double_pod.process(stream, label="my_label")
        assert result.label == "my_label"


# ---------------------------------------------------------------------------
# 4. SimpleFunctionPod — input packet schema compatibility
# ---------------------------------------------------------------------------


class TestSimpleFunctionPodInputSchemaValidation:
    def test_compatible_stream_does_not_raise(self, double_pod):
        """Stream whose packet schema matches the function's input schema is accepted."""
        double_pod.validate_inputs(make_int_stream())

    def test_wrong_key_name_raises(self, double_pod):
        """Stream packet with a key that doesn't match any function parameter raises."""
        # double_pod expects packet key 'x'; provide 'z' instead
        stream = TableStream(
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
        """Stream whose packet value type is incompatible with the function signature raises."""
        # double_pod expects int; provide str
        stream = TableStream(
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
        """Stream missing a required key (no default) raises."""
        # add_pod expects both 'x' and 'y' (neither has a default); provide only 'x'
        stream = TableStream(
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
        """Stream omitting a key that has a default value is accepted."""

        def add_with_default(x: int, y: int = 10) -> int:
            return x + y

        pod = SimpleFunctionPod(
            packet_function=PythonPacketFunction(add_with_default, output_keys="result")
        )
        # stream provides only 'x'; 'y' has default=10 so validation must pass
        stream = TableStream(
            pa.table(
                {
                    "id": pa.array([0, 1], type=pa.int64()),
                    "x": pa.array([0, 1], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )
        pod.validate_inputs(stream)  # must not raise

    def test_missing_optional_key_uses_default_value(self):
        """When a packet omits an optional field, the function's default value is used."""

        def add_with_default(x: int, y: int = 10) -> int:
            return x + y

        pod = SimpleFunctionPod(
            packet_function=PythonPacketFunction(add_with_default, output_keys="result")
        )
        stream = TableStream(
            pa.table(
                {
                    "id": pa.array([0, 1], type=pa.int64()),
                    "x": pa.array([3, 5], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )
        result = pod.process(stream)
        table = result.as_table()
        # y defaults to 10, so results should be 3+10=13 and 5+10=15
        assert table.column("result").to_pylist() == [13, 15]


# ---------------------------------------------------------------------------
# 5. SimpleFunctionPod — process_packet()
# ---------------------------------------------------------------------------


class TestSimpleFunctionPodProcessPacket:
    def test_returns_tag_and_packet_tuple(self, double_pod):
        tag = DictTag({"id": 0})
        packet = DictPacket({"x": 7})
        result = double_pod.process_packet(tag, packet)
        assert len(result) == 2

    def test_output_tag_is_input_tag(self, double_pod):
        tag = DictTag({"id": 42})
        packet = DictPacket({"x": 3})
        out_tag, _ = double_pod.process_packet(tag, packet)
        assert out_tag is tag

    def test_output_packet_has_correct_value(self, double_pod):
        tag = DictTag({"id": 0})
        packet = DictPacket({"x": 6})
        _, out_packet = double_pod.process_packet(tag, packet)
        assert out_packet is not None
        assert out_packet["result"] == 12  # 6 * 2


# ---------------------------------------------------------------------------
# 6. FunctionPodStream — Stream protocol conformance
# ---------------------------------------------------------------------------


class TestFunctionPodStreamProtocolConformance:
    def test_satisfies_stream_protocol(self, double_pod):
        stream = double_pod.process(make_int_stream())
        assert isinstance(stream, Stream), (
            "FunctionPodStream does not satisfy the Stream protocol"
        )

    def test_has_source_property(self, double_pod):
        result = double_pod.process(make_int_stream())
        _ = result.source

    def test_has_upstreams_property(self, double_pod):
        result = double_pod.process(make_int_stream())
        upstreams = result.upstreams
        assert isinstance(upstreams, tuple)

    def test_has_keys_method(self, double_pod):
        result = double_pod.process(make_int_stream())
        tag_keys, packet_keys = result.keys()
        assert isinstance(tag_keys, tuple)
        assert isinstance(packet_keys, tuple)

    def test_has_output_schema_method(self, double_pod):
        result = double_pod.process(make_int_stream())
        tag_schema, packet_schema = result.output_schema()
        assert isinstance(tag_schema, Mapping)
        assert isinstance(packet_schema, Mapping)

    def test_has_iter_packets_method(self, double_pod):
        result = double_pod.process(make_int_stream())
        it = result.iter_packets()
        pair = next(it)
        assert len(pair) == 2

    def test_has_as_table_method(self, double_pod):
        result = double_pod.process(make_int_stream())
        table = result.as_table()
        assert isinstance(table, pa.Table)


# ---------------------------------------------------------------------------
# 7. FunctionPodStream — keys() and output_schema()
# ---------------------------------------------------------------------------


class TestFunctionPodStreamKeysAndSchema:
    def test_tag_keys_come_from_input_stream(self, double_pod):
        result = double_pod.process(make_int_stream())
        tag_keys, _ = result.keys()
        assert "id" in tag_keys

    def test_packet_keys_come_from_function_output(self, double_pod):
        result = double_pod.process(make_int_stream())
        _, packet_keys = result.keys()
        assert "result" in packet_keys

    def test_packet_keys_do_not_include_input_keys(self, double_pod):
        result = double_pod.process(make_int_stream())
        _, packet_keys = result.keys()
        assert "x" not in packet_keys

    def test_output_schema_keys_match_keys_method(self, double_pod):
        result = double_pod.process(make_int_stream())
        tag_keys, packet_keys = result.keys()
        tag_schema, packet_schema = result.output_schema()
        assert set(tag_schema.keys()) == set(tag_keys)
        assert set(packet_schema.keys()) == set(packet_keys)

    def test_packet_schema_type_is_correct(self, double_pod):
        result = double_pod.process(make_int_stream())
        _, packet_schema = result.output_schema()
        assert packet_schema["result"] is int


# ---------------------------------------------------------------------------
# 8. FunctionPodStream — iter_packets()
# ---------------------------------------------------------------------------


class TestFunctionPodStreamIterPackets:
    def test_yields_correct_count(self, double_pod):
        n = 5
        result = double_pod.process(make_int_stream(n=n))
        pairs = list(result.iter_packets())
        assert len(pairs) == n

    def test_each_pair_has_tag_and_packet(self, double_pod):
        from orcapod.protocols.core_protocols.datagrams import Packet, Tag

        result = double_pod.process(make_int_stream())
        for tag, packet in result.iter_packets():
            assert isinstance(tag, Tag)
            assert isinstance(packet, Packet)

    def test_output_packet_values_are_doubled(self, double_pod):
        n = 4
        result = double_pod.process(make_int_stream(n=n))
        for i, (tag, packet) in enumerate(result.iter_packets()):
            assert packet["result"] == i * 2

    def test_iter_is_repeatable_after_first_pass(self, double_pod):
        """Second iteration must produce the same values as the first (cache path)."""
        result = double_pod.process(make_int_stream(n=3))
        first = [(tag["id"], packet["result"]) for tag, packet in result.iter_packets()]
        second = [
            (tag["id"], packet["result"]) for tag, packet in result.iter_packets()
        ]
        assert first == second

    def test_iter_delegates_from_dunder_iter(self, double_pod):
        result = double_pod.process(make_int_stream(n=3))
        via_iter = list(result)
        via_method = list(result.iter_packets())
        assert len(via_iter) == len(via_method)


# ---------------------------------------------------------------------------
# 9. FunctionPodStream — as_table()
# ---------------------------------------------------------------------------


class TestFunctionPodStreamAsTable:
    def test_returns_pyarrow_table(self, double_pod):
        result = double_pod.process(make_int_stream())
        assert isinstance(result.as_table(), pa.Table)

    def test_table_has_correct_row_count(self, double_pod):
        n = 4
        result = double_pod.process(make_int_stream(n=n))
        assert len(result.as_table()) == n

    def test_table_contains_tag_columns(self, double_pod):
        result = double_pod.process(make_int_stream())
        table = result.as_table()
        assert "id" in table.column_names

    def test_table_contains_packet_columns(self, double_pod):
        result = double_pod.process(make_int_stream())
        table = result.as_table()
        assert "result" in table.column_names

    def test_table_result_values_are_correct(self, double_pod):
        n = 3
        result = double_pod.process(make_int_stream(n=n))
        table = result.as_table()
        results = table.column("result").to_pylist()
        assert results == [i * 2 for i in range(n)]

    def test_as_table_is_idempotent(self, double_pod):
        """Calling as_table() twice must return the same data."""
        result = double_pod.process(make_int_stream(n=3))
        t1 = result.as_table()
        t2 = result.as_table()
        assert t1.equals(t2)

    def test_all_info_adds_extra_columns(self, double_pod):
        result = double_pod.process(make_int_stream())
        default = result.as_table()
        with_info = result.as_table(all_info=True)
        assert len(with_info.column_names) >= len(default.column_names)


# ---------------------------------------------------------------------------
# 10. Multi-stream (join) input
# ---------------------------------------------------------------------------


class TestSimpleFunctionPodMultiStream:
    def test_two_streams_are_joined_before_processing(self, add_pod):
        """add_pod requires {x, y}; split them across two streams joined on id."""
        n = 3
        stream_x = TableStream(
            pa.table(
                {
                    "id": pa.array(list(range(n)), type=pa.int64()),
                    "x": pa.array(list(range(n)), type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )
        stream_y = TableStream(
            pa.table(
                {
                    "id": pa.array(list(range(n)), type=pa.int64()),
                    "y": pa.array([i * 10 for i in range(n)], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )
        result = add_pod.process(stream_x, stream_y)
        assert isinstance(result, FunctionPodStream)
        packets = list(result.iter_packets())
        assert len(packets) == n
        for i, (_, packet) in enumerate(packets):
            assert packet["result"] == i + i * 10  # x + y


# ---------------------------------------------------------------------------
# 11. function_pod decorator
# ---------------------------------------------------------------------------


# Module-level decorated functions (lambdas are forbidden by the decorator)
@function_pod(output_keys="result")
def triple(x: int) -> int:
    return x * 3


@function_pod(output_keys=["total", "diff"], version="v1.0")
def stats(a: int, b: int) -> tuple[int, int]:
    return a + b, a - b


@function_pod(output_keys="result", function_name="custom_name")
def renamed(x: int) -> int:
    return x + 1


class TestFunctionPodDecorator:
    # --- attachment ---

    def test_decorated_function_has_pod_attribute(self):
        assert hasattr(triple, "pod")

    def test_pod_attribute_is_simple_function_pod(self):
        assert isinstance(triple.pod, SimpleFunctionPod)

    def test_pod_satisfies_function_pod_protocol(self):
        assert isinstance(triple.pod, FunctionPod)

    # --- original callable is preserved ---

    def test_decorated_function_is_still_callable(self):
        assert callable(triple)

    def test_decorated_function_returns_correct_value(self):
        assert triple(x=4) == 12

    # --- pod properties ---

    def test_pod_canonical_name_matches_function_name(self):
        assert triple.pod.packet_function.canonical_function_name == "triple"

    def test_explicit_function_name_overrides(self):
        assert renamed.pod.packet_function.canonical_function_name == "custom_name"

    def test_pod_version_is_set(self):
        assert stats.pod.packet_function.major_version == 1

    def test_pod_output_keys_are_set(self):
        packet_schema = stats.pod.packet_function.output_packet_schema
        assert "total" in packet_schema
        assert "diff" in packet_schema

    def test_pod_uri_is_non_empty_tuple_of_strings(self):
        uri = triple.pod.uri
        assert isinstance(uri, tuple)
        assert len(uri) > 0
        assert all(isinstance(part, str) for part in uri)

    # --- lambda is rejected ---

    def test_lambda_raises_value_error(self):
        with pytest.raises(ValueError):
            function_pod(output_keys="result")(lambda x: x)

    # --- end-to-end processing via pod.process() ---

    def test_pod_process_returns_function_pod_stream(self):
        stream = make_int_stream(n=3)
        result = triple.pod.process(stream)
        assert isinstance(result, FunctionPodStream)

    def test_pod_process_output_satisfies_stream_protocol(self):
        stream = make_int_stream(n=3)
        result = triple.pod.process(stream)
        assert isinstance(result, Stream)

    def test_pod_process_correct_values(self):
        n = 4
        stream = make_int_stream(n=n)
        result = triple.pod.process(stream)
        for i, (_, packet) in enumerate(result.iter_packets()):
            assert packet["result"] == i * 3

    def test_pod_process_correct_row_count(self):
        n = 5
        stream = make_int_stream(n=n)
        result = triple.pod.process(stream)
        assert len(list(result.iter_packets())) == n

    def test_pod_call_operator_same_as_process(self):
        stream = make_int_stream(n=3)
        via_process = list(triple.pod.process(stream).iter_packets())
        via_call = list(triple.pod(stream).iter_packets())
        assert [(t["id"], p["result"]) for t, p in via_process] == [
            (t["id"], p["result"]) for t, p in via_call
        ]

    def test_multiple_output_keys_end_to_end(self):
        # stats expects {a: int, b: int}; build a stream with those columns
        n = 3
        stream = TableStream(
            pa.table(
                {
                    "id": pa.array(list(range(n)), type=pa.int64()),
                    "a": pa.array(list(range(n)), type=pa.int64()),
                    "b": pa.array(list(range(n)), type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )
        result = stats.pod.process(stream)
        for i, (_, packet) in enumerate(result.iter_packets()):
            assert packet["total"] == i + i  # a + b where a=b=i
            assert packet["diff"] == 0  # a - b

    def test_pod_as_table_has_correct_columns(self):
        stream = make_int_stream(n=3)
        table = triple.pod.process(stream).as_table()
        assert "id" in table.column_names
        assert "result" in table.column_names
