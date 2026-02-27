"""
Extended tests for function_pod.py covering:
- WrappedFunctionPod — delegation, uri, validate_inputs, output_schema, process
- FunctionPodNode — construction, pipeline_path, uri, validate_inputs, process_packet,
                    add_pipeline_record, output_schema, argument_symmetry, process/__call__
- FunctionPodNodeStream — iter_packets, as_table, refresh_cache, output_schema
- function_pod decorator with result_database — creates CachedPacketFunction, caching works
- FunctionPodStream.as_table() content_hash and sort_by_tags column configs
- TrackedPacketFunctionPod — handle_input_streams with 0 streams raises
"""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa
import pytest

from orcapod.core.datagrams import DictPacket, DictTag
from orcapod.core.function_pod import (
    FunctionPodNode,
    FunctionPodNodeStream,
    FunctionPodStream,
    SimpleFunctionPod,
    WrappedFunctionPod,
    function_pod,
)
from orcapod.core.packet_function import CachedPacketFunction, PythonPacketFunction
from orcapod.core.streams import TableStream
from orcapod.databases import InMemoryArrowDatabase
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
# 1. TrackedPacketFunctionPod — handle_input_streams with 0 streams
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
        stream_x = TableStream(
            pa.table(
                {
                    "id": pa.array([0, 1], type=pa.int64()),
                    "x": pa.array([0, 1], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )
        stream_y = TableStream(
            pa.table(
                {
                    "id": pa.array([0, 1], type=pa.int64()),
                    "y": pa.array([10, 20], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )
        result = add_pod.handle_input_streams(stream_x, stream_y)
        # result should be a joined stream
        assert isinstance(result, Stream)


# ---------------------------------------------------------------------------
# 2. WrappedFunctionPod — construction and delegation
# ---------------------------------------------------------------------------


class TestWrappedFunctionPodDelegation:
    @pytest.fixture
    def wrapped(self, double_pod) -> WrappedFunctionPod:
        """WrappedFunctionPod wrapping double_pod."""
        return WrappedFunctionPod(function_pod=double_pod)

    def test_uri_delegates_to_inner_pod(self, wrapped, double_pod):
        assert wrapped.uri == double_pod.uri

    def test_label_delegates_to_inner_pod(self, wrapped, double_pod):
        assert wrapped.computed_label() == double_pod.label

    def test_validate_inputs_delegates(self, wrapped):
        stream = make_int_stream()
        # Should not raise for compatible stream
        wrapped.validate_inputs(stream)

    def test_output_schema_delegates(self, wrapped, double_pod):
        stream = make_int_stream()
        wrapped_schema = wrapped.output_schema(stream)
        pod_schema = double_pod.output_schema(stream)
        assert wrapped_schema == pod_schema

    def test_argument_symmetry_delegates(self, wrapped, double_pod):
        stream = make_int_stream()
        assert wrapped.argument_symmetry([stream]) == double_pod.argument_symmetry(
            [stream]
        )

    def test_process_delegates_to_inner_pod(self, wrapped):
        stream = make_int_stream(n=3)
        result = wrapped.process(stream)
        assert isinstance(result, Stream)
        packets = list(result.iter_packets())
        assert len(packets) == 3
        for i, (_, packet) in enumerate(packets):
            assert packet["result"] == i * 2  # double


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
        """Calling as_table() twice with content_hash must give same hash values."""
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
        # Build a stream with tags in reverse order
        n = 5
        table = pa.table(
            {
                "id": pa.array(list(reversed(range(n))), type=pa.int64()),
                "x": pa.array(list(reversed(range(n))), type=pa.int64()),
            }
        )
        stream = double_pod.process(TableStream(table, tag_columns=["id"]))
        result = stream.as_table(columns={"sort_by_tags": True})
        ids = result.column("id").to_pylist()
        assert ids == sorted(ids)

    def test_default_table_may_be_unsorted(self, double_pod):
        """When sort_by_tags is not set, row order follows input order."""
        n = 5
        reversed_ids = list(reversed(range(n)))
        table = pa.table(
            {
                "id": pa.array(reversed_ids, type=pa.int64()),
                "x": pa.array(reversed_ids, type=pa.int64()),
            }
        )
        stream = double_pod.process(TableStream(table, tag_columns=["id"]))
        result = stream.as_table()
        # Without sort, order should match input (reversed)
        ids = result.column("id").to_pylist()
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

        # With a result_database, the inner packet_function should be CachedPacketFunction
        assert isinstance(square.pod.packet_function, CachedPacketFunction)

    def test_pod_is_still_simple_function_pod(self):
        db = InMemoryArrowDatabase()

        @function_pod(output_keys="result", result_database=db)
        def cube(x: int) -> int:
            return x * x * x

        assert isinstance(cube.pod, SimpleFunctionPod)

    def test_cache_miss_then_hit(self):
        db = InMemoryArrowDatabase()
        call_count = 0

        @function_pod(output_keys="result", result_database=db)
        def counted_double(x: int) -> int:
            nonlocal call_count
            call_count += 1
            return x * 2

        stream = make_int_stream(n=2)
        # First pass — cache miss → inner function called
        list(counted_double.pod.process(stream).iter_packets())
        first_count = call_count

        stream2 = make_int_stream(n=2)
        # Second pass — should hit cache → inner function NOT called again
        list(counted_double.pod.process(stream2).iter_packets())
        assert call_count == first_count  # no new calls

    def test_cached_results_match_direct_results(self):
        db = InMemoryArrowDatabase()

        @function_pod(output_keys="result", result_database=db)
        def triple_cached(x: int) -> int:
            return x * 3

        stream1 = make_int_stream(n=3)
        stream2 = make_int_stream(n=3)
        first = list(triple_cached.pod.process(stream1).iter_packets())
        second = list(triple_cached.pod.process(stream2).iter_packets())

        for (_, p1), (_, p2) in zip(first, second):
            assert p1["result"] == p2["result"]

    def test_without_result_database_packet_function_is_plain(self):
        @function_pod(output_keys="result")
        def plain(x: int) -> int:
            return x + 1

        assert isinstance(plain.pod.packet_function, PythonPacketFunction)
        assert not isinstance(plain.pod.packet_function, CachedPacketFunction)


# ---------------------------------------------------------------------------
# 6. FunctionPodNode — construction
# ---------------------------------------------------------------------------


class TestFunctionPodNodeConstruction:
    @pytest.fixture
    def node(self, double_pf) -> FunctionPodNode:
        db = InMemoryArrowDatabase()
        stream = make_int_stream(n=3)
        return FunctionPodNode(
            packet_function=double_pf,
            input_stream=stream,
            pipeline_database=db,
        )

    def test_construction_succeeds(self, node):
        assert node is not None

    def test_pipeline_path_is_tuple_of_strings(self, node):
        path = node.pipeline_path
        assert isinstance(path, tuple)
        assert all(isinstance(p, str) for p in path)

    def test_uri_is_tuple_of_strings(self, node):
        uri = node.uri
        assert isinstance(uri, tuple)
        assert all(isinstance(part, str) for part in uri)

    def test_uri_contains_node_component(self, node):
        uri_str = ":".join(node.uri)
        assert "node:" in uri_str

    def test_uri_contains_tag_component(self, node):
        uri_str = ":".join(node.uri)
        assert "tag:" in uri_str

    def test_pipeline_path_includes_uri(self, node):
        for part in node.uri:
            assert part in node.pipeline_path

    def test_incompatible_stream_raises_on_construction(self, double_pf):
        db = InMemoryArrowDatabase()
        # double_pf expects 'x'; provide 'z'
        bad_stream = TableStream(
            pa.table(
                {
                    "id": pa.array([0, 1], type=pa.int64()),
                    "z": pa.array([0, 1], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )
        with pytest.raises(ValueError):
            FunctionPodNode(
                packet_function=double_pf,
                input_stream=bad_stream,
                pipeline_database=db,
            )

    def test_result_database_defaults_to_pipeline_database(self, double_pf):
        db = InMemoryArrowDatabase()
        stream = make_int_stream(n=2)
        node = FunctionPodNode(
            packet_function=double_pf,
            input_stream=stream,
            pipeline_database=db,
        )
        # result_database not provided → same db is used with _result suffix in path
        assert node._pipeline_database is db

    def test_separate_result_database_accepted(self, double_pf):
        pipeline_db = InMemoryArrowDatabase()
        result_db = InMemoryArrowDatabase()
        stream = make_int_stream(n=2)
        node = FunctionPodNode(
            packet_function=double_pf,
            input_stream=stream,
            pipeline_database=pipeline_db,
            result_database=result_db,
        )
        assert node._pipeline_database is pipeline_db


# ---------------------------------------------------------------------------
# 7. FunctionPodNode — validate_inputs and argument_symmetry
# ---------------------------------------------------------------------------


class TestFunctionPodNodeValidation:
    @pytest.fixture
    def node(self, double_pf) -> FunctionPodNode:
        db = InMemoryArrowDatabase()
        return FunctionPodNode(
            packet_function=double_pf,
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )

    def test_validate_inputs_with_no_streams_succeeds(self, node):
        node.validate_inputs()  # must not raise

    def test_validate_inputs_with_any_stream_raises(self, node):
        extra = make_int_stream(n=2)
        with pytest.raises(ValueError):
            node.validate_inputs(extra)

    def test_argument_symmetry_empty_raises(self, node):
        # expects no external streams
        with pytest.raises(ValueError):
            node.argument_symmetry([make_int_stream()])

    def test_argument_symmetry_no_streams_returns_empty(self, node):
        result = node.argument_symmetry([])
        assert result == ()


# ---------------------------------------------------------------------------
# 8. FunctionPodNode — output_schema
# ---------------------------------------------------------------------------


class TestFunctionPodNodeOutputSchema:
    @pytest.fixture
    def node(self, double_pf) -> FunctionPodNode:
        db = InMemoryArrowDatabase()
        return FunctionPodNode(
            packet_function=double_pf,
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )

    def test_output_schema_returns_two_mappings(self, node):
        tag_schema, packet_schema = node.output_schema()
        assert isinstance(tag_schema, Mapping)
        assert isinstance(packet_schema, Mapping)

    def test_packet_schema_matches_function_output(self, node, double_pf):
        _, packet_schema = node.output_schema()
        assert packet_schema == double_pf.output_packet_schema

    def test_tag_schema_matches_input_stream(self, node):
        tag_schema, _ = node.output_schema()
        # tag from make_int_stream has 'id'
        assert "id" in tag_schema


# ---------------------------------------------------------------------------
# 9. FunctionPodNode — process_packet and add_pipeline_record
# ---------------------------------------------------------------------------


class TestFunctionPodNodeProcessPacket:
    @pytest.fixture
    def node(self, double_pf) -> FunctionPodNode:
        db = InMemoryArrowDatabase()
        return FunctionPodNode(
            packet_function=double_pf,
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )

    def test_process_packet_returns_tag_and_packet(self, node):
        tag = DictTag({"id": 0})
        packet = DictPacket({"x": 5})
        out_tag, out_packet = node.process_packet(tag, packet)
        assert out_tag is tag
        assert out_packet is not None

    def test_process_packet_value_correct(self, node):
        tag = DictTag({"id": 0})
        packet = DictPacket({"x": 6})
        _, out_packet = node.process_packet(tag, packet)
        assert out_packet["result"] == 12  # 6 * 2

    def test_process_packet_adds_pipeline_record(self, node, double_pf):
        tag = DictTag({"id": 0})
        packet = DictPacket({"x": 3})
        node.process_packet(tag, packet)
        # after calling process_packet, pipeline db should have at least one record
        db = node._pipeline_database
        db.flush()
        all_records = db.get_all_records(node.pipeline_path)
        assert all_records is not None
        assert all_records.num_rows >= 1

    def test_process_packet_second_call_same_input_deduplicates(self, node):
        tag = DictTag({"id": 0})
        packet = DictPacket({"x": 3})
        node.process_packet(tag, packet)
        node.process_packet(tag, packet)  # same tag+packet → should not double-insert
        db = node._pipeline_database
        db.flush()
        all_records = db.get_all_records(node.pipeline_path)
        assert all_records is not None
        assert all_records.num_rows == 1  # deduplicated


# ---------------------------------------------------------------------------
# 10. FunctionPodNode — process() / __call__()
# ---------------------------------------------------------------------------


class TestFunctionPodNodeProcess:
    @pytest.fixture
    def node(self, double_pf) -> FunctionPodNode:
        db = InMemoryArrowDatabase()
        return FunctionPodNode(
            packet_function=double_pf,
            input_stream=make_int_stream(n=3),
            pipeline_database=db,
        )

    def test_process_returns_function_pod_node_stream(self, node):
        result = node.process()
        assert isinstance(result, FunctionPodNodeStream)

    def test_call_operator_returns_function_pod_node_stream(self, node):
        result = node()
        assert isinstance(result, FunctionPodNodeStream)

    def test_process_with_extra_streams_raises(self, node):
        with pytest.raises(ValueError):
            node.process(make_int_stream(n=2))

    def test_process_output_is_stream_protocol(self, node):
        result = node.process()
        assert isinstance(result, Stream)


# ---------------------------------------------------------------------------
# 11. FunctionPodNodeStream — iter_packets and as_table
# ---------------------------------------------------------------------------


class TestFunctionPodNodeStream:
    @pytest.fixture
    def node_stream(self, double_pf) -> FunctionPodNodeStream:
        db = InMemoryArrowDatabase()
        input_stream = make_int_stream(n=3)
        node = FunctionPodNode(
            packet_function=double_pf,
            input_stream=input_stream,
            pipeline_database=db,
        )
        return node.process()

    def test_iter_packets_yields_correct_count(self, node_stream):
        packets = list(node_stream.iter_packets())
        assert len(packets) == 3

    def test_iter_packets_correct_values(self, node_stream):
        for i, (_, packet) in enumerate(node_stream.iter_packets()):
            assert packet["result"] == i * 2

    def test_iter_is_repeatable(self, node_stream):
        first = [(t["id"], p["result"]) for t, p in node_stream.iter_packets()]
        second = [(t["id"], p["result"]) for t, p in node_stream.iter_packets()]
        assert first == second

    def test_dunder_iter_delegates_to_iter_packets(self, node_stream):
        via_iter = list(node_stream)
        via_method = list(node_stream.iter_packets())
        assert len(via_iter) == len(via_method)

    def test_as_table_returns_pyarrow_table(self, node_stream):
        table = node_stream.as_table()
        assert isinstance(table, pa.Table)

    def test_as_table_has_correct_row_count(self, node_stream):
        table = node_stream.as_table()
        assert len(table) == 3

    def test_as_table_contains_tag_columns(self, node_stream):
        table = node_stream.as_table()
        assert "id" in table.column_names

    def test_as_table_contains_packet_columns(self, node_stream):
        table = node_stream.as_table()
        assert "result" in table.column_names

    def test_source_is_fp_node(self, node_stream, double_pf):
        assert isinstance(node_stream.source, FunctionPodNode)

    def test_upstreams_contains_input_stream(self, node_stream):
        upstreams = node_stream.upstreams
        assert isinstance(upstreams, tuple)
        assert len(upstreams) == 1

    def test_output_schema_matches_node_output_schema(self, node_stream):
        tag_schema, packet_schema = node_stream.output_schema()
        assert isinstance(tag_schema, Mapping)
        assert isinstance(packet_schema, Mapping)
        assert "result" in packet_schema

    def test_as_table_content_hash_column(self, node_stream):
        table = node_stream.as_table(columns={"content_hash": True})
        assert "_content_hash" in table.column_names
        assert len(table.column("_content_hash")) == 3

    def test_as_table_sort_by_tags(self, double_pf):
        db = InMemoryArrowDatabase()
        reversed_table = pa.table(
            {
                "id": pa.array([4, 3, 2, 1, 0], type=pa.int64()),
                "x": pa.array([4, 3, 2, 1, 0], type=pa.int64()),
            }
        )
        input_stream = TableStream(reversed_table, tag_columns=["id"])
        node = FunctionPodNode(
            packet_function=double_pf,
            input_stream=input_stream,
            pipeline_database=db,
        )
        node_stream = node.process()
        result = node_stream.as_table(columns={"sort_by_tags": True})
        ids = result.column("id").to_pylist()
        assert ids == sorted(ids)


# ---------------------------------------------------------------------------
# 12. FunctionPodNodeStream — refresh_cache
# ---------------------------------------------------------------------------


class TestFunctionPodNodeStreamRefreshCache:
    def test_refresh_cache_clears_output_when_upstream_modified(self, double_pf):
        db = InMemoryArrowDatabase()
        input_stream = make_int_stream(n=3)
        node = FunctionPodNode(
            packet_function=double_pf,
            input_stream=input_stream,
            pipeline_database=db,
        )
        node_stream = node.process()

        # Consume the stream to populate cache
        list(node_stream.iter_packets())
        assert len(node_stream._cached_output_packets) == 3

        # Simulate upstream modification by manually updating timestamps
        import time

        time.sleep(0.01)
        input_stream._update_modified_time()

        # refresh_cache should clear the output cache
        node_stream.refresh_cache()
        assert len(node_stream._cached_output_packets) == 0
        assert node_stream._cached_output_table is None

    def test_refresh_cache_no_op_when_not_stale(self, double_pf):
        db = InMemoryArrowDatabase()
        input_stream = make_int_stream(n=3)
        node = FunctionPodNode(
            packet_function=double_pf,
            input_stream=input_stream,
            pipeline_database=db,
        )
        node_stream = node.process()

        # Consume stream
        list(node_stream.iter_packets())
        cached_count = len(node_stream._cached_output_packets)

        # Do NOT update upstream; refresh should be a no-op
        node_stream.refresh_cache()
        assert len(node_stream._cached_output_packets) == cached_count


# ---------------------------------------------------------------------------
# 13. FunctionPodNode with pipeline_path_prefix
# ---------------------------------------------------------------------------


class TestFunctionPodNodePipelinePathPrefix:
    def test_prefix_prepended_to_pipeline_path(self, double_pf):
        db = InMemoryArrowDatabase()
        prefix = ("my_pipeline", "stage_1")
        node = FunctionPodNode(
            packet_function=double_pf,
            input_stream=make_int_stream(n=2),
            pipeline_database=db,
            pipeline_path_prefix=prefix,
        )
        pipeline_path = node.pipeline_path
        assert pipeline_path[: len(prefix)] == prefix

    def test_no_prefix_pipeline_path_equals_uri(self, double_pf):
        db = InMemoryArrowDatabase()
        node = FunctionPodNode(
            packet_function=double_pf,
            input_stream=make_int_stream(n=2),
            pipeline_database=db,
        )
        assert node.pipeline_path == node.uri


# ---------------------------------------------------------------------------
# 14. FunctionPodNode — result path uses _result suffix when no separate db
# ---------------------------------------------------------------------------


class TestFunctionPodNodeResultPath:
    def test_result_records_stored_under_result_suffix_path(self, double_pf):
        db = InMemoryArrowDatabase()
        stream = make_int_stream(n=2)
        node = FunctionPodNode(
            packet_function=double_pf,
            input_stream=stream,
            pipeline_database=db,
        )
        # Process some packets so results are stored
        tag = DictTag({"id": 0})
        packet = DictPacket({"x": 5})
        node.process_packet(tag, packet)
        db.flush()

        # Results should be stored under a path ending in "_result"
        result_path = node._cached_packet_function.record_path
        assert result_path[-1] == "_result" or any(
            "_result" in part for part in result_path
        )
