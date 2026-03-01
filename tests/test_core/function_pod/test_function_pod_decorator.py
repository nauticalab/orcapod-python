"""
Tests for the function_pod decorator.

Covers:
- PodProtocol attachment and protocol conformance
- Original callable preserved
- PodProtocol properties (name, version, output keys, URI)
- Lambda rejection
- End-to-end processing via pod.process() and pod()
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPodStream, FunctionPod, function_pod
from orcapod.protocols.core_protocols import FunctionPodProtocol, StreamProtocol

from ..conftest import make_int_stream
from orcapod.core.streams import ArrowTableStream


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


# ---------------------------------------------------------------------------
# 1. PodProtocol attachment
# ---------------------------------------------------------------------------


class TestFunctionPodDecoratorAttachment:
    def test_decorated_function_has_pod_attribute(self):
        assert hasattr(triple, "pod")

    def test_pod_attribute_is_simple_function_pod(self):
        assert isinstance(triple.pod, FunctionPod)

    def test_pod_satisfies_function_pod_protocol(self):
        assert isinstance(triple.pod, FunctionPodProtocol)

    def test_decorated_function_is_still_callable(self):
        assert callable(triple)

    def test_decorated_function_returns_correct_value(self):
        assert triple(x=4) == 12


# ---------------------------------------------------------------------------
# 2. PodProtocol properties
# ---------------------------------------------------------------------------


class TestFunctionPodDecoratorProperties:
    def test_canonical_name_matches_function_name(self):
        assert triple.pod.packet_function.canonical_function_name == "triple"

    def test_explicit_function_name_overrides(self):
        assert renamed.pod.packet_function.canonical_function_name == "custom_name"

    def test_version_is_set(self):
        assert stats.pod.packet_function.major_version == 1

    def test_output_keys_are_set(self):
        schema = stats.pod.packet_function.output_packet_schema
        assert "total" in schema
        assert "diff" in schema

    def test_uri_is_non_empty_tuple_of_strings(self):
        uri = triple.pod.uri
        assert isinstance(uri, tuple)
        assert len(uri) > 0
        assert all(isinstance(part, str) for part in uri)


# ---------------------------------------------------------------------------
# 3. Lambda rejection
# ---------------------------------------------------------------------------


class TestFunctionPodDecoratorLambdaRejection:
    def test_lambda_raises_value_error(self):
        with pytest.raises(ValueError):
            function_pod(output_keys="result")(lambda x: x)


# ---------------------------------------------------------------------------
# 4. End-to-end processing
# ---------------------------------------------------------------------------


class TestFunctionPodDecoratorEndToEnd:
    def test_pod_process_returns_function_pod_stream(self):
        assert isinstance(triple.pod.process(make_int_stream(n=3)), FunctionPodStream)

    def test_pod_process_output_satisfies_stream_protocol(self):
        assert isinstance(triple.pod.process(make_int_stream(n=3)), StreamProtocol)

    def test_pod_process_correct_values(self):
        for i, (_, packet) in enumerate(
            triple.pod.process(make_int_stream(n=4)).iter_packets()
        ):
            assert packet["result"] == i * 3

    def test_pod_process_correct_row_count(self):
        assert len(list(triple.pod.process(make_int_stream(n=5)).iter_packets())) == 5

    def test_pod_call_operator_same_as_process(self):
        stream = make_int_stream(n=3)
        via_process = [
            (t["id"], p["result"]) for t, p in triple.pod.process(stream).iter_packets()
        ]
        via_call = [
            (t["id"], p["result"]) for t, p in triple.pod(stream).iter_packets()
        ]
        assert via_process == via_call

    def test_multiple_output_keys_end_to_end(self):
        n = 3
        stream = ArrowTableStream(
            pa.table(
                {
                    "id": pa.array(list(range(n)), type=pa.int64()),
                    "a": pa.array(list(range(n)), type=pa.int64()),
                    "b": pa.array(list(range(n)), type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )
        for i, (_, packet) in enumerate(stats.pod.process(stream).iter_packets()):
            assert packet["total"] == i + i
            assert packet["diff"] == 0

    def test_as_table_has_correct_columns(self):
        table = triple.pod.process(make_int_stream(n=3)).as_table()
        assert "id" in table.column_names
        assert "result" in table.column_names
