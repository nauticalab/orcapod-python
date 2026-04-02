"""
Extended tests for function_pod.py covering:
- _FunctionPodBase — handle_input_streams
- WrappedFunctionPod — delegation, uri, validate_inputs, output_schema, process
- FunctionPodStream — as_table() with content_hash and sort_by_tags column configs
- function_pod decorator with result_database — creates CachedPacketFunction, caching works
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.function_pod import (
    FunctionPod,
    WrappedFunctionPod,
    function_pod,
)
from orcapod.core.packet_function import CachedPacketFunction, PythonPacketFunction
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.protocols.core_protocols import StreamProtocol

from ..conftest import make_int_stream


# ---------------------------------------------------------------------------
# 1. _FunctionPodBase — handle_input_streams with 0 streams
# ---------------------------------------------------------------------------


class TestTrackedPacketFunctionPodHandleInputStreams:
    def test_zero_streams_raises(self, double_pod):
        with pytest.raises(ValueError, match="At least one input stream"):
            double_pod.handle_input_streams()

    def test_single_stream_passthrough(self, double_pod):
        stream = make_int_stream()
        result = double_pod.handle_input_streams(stream)
        assert result is stream

    def test_multiple_streams_returns_joined_stream(self, add_pod):
        stream_x = ArrowTableStream(
            pa.table(
                {"id": pa.array([0, 1], type=pa.int64()), "x": pa.array([0, 1], type=pa.int64())},
                schema=pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("x", pa.int64(), nullable=False)]),
            ),
            tag_columns=["id"],
        )
        stream_y = ArrowTableStream(
            pa.table(
                {"id": pa.array([0, 1], type=pa.int64()), "y": pa.array([10, 20], type=pa.int64())},
                schema=pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("y", pa.int64(), nullable=False)]),
            ),
            tag_columns=["id"],
        )
        result = add_pod.handle_input_streams(stream_x, stream_y)
        assert isinstance(result, StreamProtocol)
        assert len([p for p in result.iter_packets()]) == 2


# ---------------------------------------------------------------------------
# 2. WrappedFunctionPod — construction and delegation
# ---------------------------------------------------------------------------


class TestWrappedFunctionPodDelegation:
    @pytest.fixture
    def wrapped(self, double_pod) -> WrappedFunctionPod:
        return WrappedFunctionPod(function_pod=double_pod)

    def test_uri_delegates_to_inner_pod(self, wrapped, double_pod):
        assert wrapped.uri == double_pod.uri

    def test_label_delegates_to_inner_pod(self, wrapped, double_pod):
        assert wrapped.computed_label() == double_pod.label

    def test_validate_inputs_delegates(self, wrapped):
        wrapped.validate_inputs(make_int_stream())

    def test_output_schema_delegates(self, wrapped, double_pod):
        stream = make_int_stream()
        assert wrapped.output_schema(stream) == double_pod.output_schema(stream)

    def test_argument_symmetry_delegates(self, wrapped, double_pod):
        stream = make_int_stream()
        assert wrapped.argument_symmetry([stream]) == double_pod.argument_symmetry(
            [stream]
        )

    def test_process_delegates_to_inner_pod(self, wrapped):
        stream = make_int_stream(n=3)
        result = wrapped.process(stream)
        assert isinstance(result, StreamProtocol)
        packets = list(result.iter_packets())
        assert len(packets) == 3
        for i, (_, packet) in enumerate(packets):
            assert packet["result"] == i * 2


# ---------------------------------------------------------------------------
# 3. FunctionPodStream — as_table() with content_hash column config
# ---------------------------------------------------------------------------


class TestFunctionPodStreamContentHash:
    def test_content_hash_adds_default_column(self, double_pod):
        stream = double_pod.process(make_int_stream(n=3))
        table = stream.as_table(columns={"content_hash": True})
        assert "_content_hash" in table.column_names

    def test_content_hash_with_custom_name(self, double_pod):
        stream = double_pod.process(make_int_stream(n=3))
        table = stream.as_table(columns={"content_hash": "my_hash"})
        assert "my_hash" in table.column_names
        assert "_content_hash" not in table.column_names

    def test_content_hash_column_has_correct_length(self, double_pod):
        n = 4
        stream = double_pod.process(make_int_stream(n=n))
        table = stream.as_table(columns={"content_hash": True})
        assert len(table.column("_content_hash")) == n

    def test_content_hash_values_are_strings(self, double_pod):
        stream = double_pod.process(make_int_stream(n=3))
        table = stream.as_table(columns={"content_hash": True})
        for val in table.column("_content_hash").to_pylist():
            assert isinstance(val, str)
            assert len(val) > 0

    def test_content_hash_is_idempotent(self, double_pod):
        stream = double_pod.process(make_int_stream(n=3))
        t1 = stream.as_table(columns={"content_hash": True})
        t2 = stream.as_table(columns={"content_hash": True})
        assert (
            t1.column("_content_hash").to_pylist()
            == t2.column("_content_hash").to_pylist()
        )

    def test_no_content_hash_by_default(self, double_pod):
        stream = double_pod.process(make_int_stream(n=3))
        table = stream.as_table()
        assert "_content_hash" not in table.column_names


# ---------------------------------------------------------------------------
# 4. FunctionPodStream — as_table() with sort_by_tags column config
# ---------------------------------------------------------------------------


class TestFunctionPodStreamSortByTags:
    def test_sort_by_tags_returns_sorted_table(self, double_pod):
        n = 5
        schema = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("x", pa.int64(), nullable=False)])
        table = pa.table(
            {
                "id": pa.array(list(reversed(range(n))), type=pa.int64()),
                "x": pa.array(list(reversed(range(n))), type=pa.int64()),
            },
            schema=schema,
        )
        stream = double_pod.process(ArrowTableStream(table, tag_columns=["id"]))
        result = stream.as_table(columns={"sort_by_tags": True})
        ids: list[int] = result.column("id").to_pylist()  # type: ignore[assignment]
        assert ids == sorted(ids)

    def test_default_table_may_be_unsorted(self, double_pod):
        n = 5
        reversed_ids = list(reversed(range(n)))
        schema = pa.schema([pa.field("id", pa.int64(), nullable=False), pa.field("x", pa.int64(), nullable=False)])
        table = pa.table(
            {
                "id": pa.array(reversed_ids, type=pa.int64()),
                "x": pa.array(reversed_ids, type=pa.int64()),
            },
            schema=schema,
        )
        stream = double_pod.process(ArrowTableStream(table, tag_columns=["id"]))
        result = stream.as_table()
        ids: list[int] = result.column("id").to_pylist()  # type: ignore[assignment]
        assert ids == reversed_ids


# ---------------------------------------------------------------------------
# 5. function_pod decorator with result_database
# ---------------------------------------------------------------------------


class TestFunctionPodDecoratorWithDatabase:
    def test_creates_cached_packet_function(self):
        db = InMemoryArrowDatabase()

        @function_pod(output_keys="result", result_database=db)
        def square(x: int) -> int:
            return x * x

        assert isinstance(square.pod.packet_function, CachedPacketFunction)

    def test_pod_is_still_simple_function_pod(self):
        db = InMemoryArrowDatabase()

        @function_pod(output_keys="result", result_database=db)
        def cube(x: int) -> int:
            return x * x * x

        assert isinstance(cube.pod, FunctionPod)

    def test_cache_miss_then_hit(self):
        db = InMemoryArrowDatabase()
        call_count = 0

        @function_pod(output_keys="result", result_database=db)
        def counted_double(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        list(counted_double.pod.process(make_int_stream(n=2)).iter_packets())
        first_count = call_count

        list(counted_double.pod.process(make_int_stream(n=2)).iter_packets())
        assert call_count == first_count

    def test_cached_results_match_direct_results(self):
        db = InMemoryArrowDatabase()

        @function_pod(output_keys="result", result_database=db)
        def triple_cached(x: int) -> int:
            return x * 3

        first = list(triple_cached.pod.process(make_int_stream(n=3)).iter_packets())
        second = list(triple_cached.pod.process(make_int_stream(n=3)).iter_packets())

        for (_, p1), (_, p2) in zip(first, second):
            assert p1["result"] == p2["result"]

    def test_without_result_database_packet_function_is_plain(self):
        @function_pod(output_keys="result")
        def plain(x: int) -> int:
            return x + 1

        assert isinstance(plain.pod.packet_function, PythonPacketFunction)
        assert not isinstance(plain.pod.packet_function, CachedPacketFunction)
