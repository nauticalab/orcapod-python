"""Tests for store_result on all node types."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.nodes import SourceNode
from orcapod.core.sources import ArrowTableSource


@pytest.fixture
def source_and_node():
    table = pa.table(
        {
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "value": pa.array([1, 2], type=pa.int64()),
        }
    )
    src = ArrowTableSource(table, tag_columns=["key"])
    node = SourceNode(src)
    return src, node


class TestSourceNodeStoreResult:
    def test_store_result_noop_without_db(self, source_and_node):
        """store_result should be a no-op when no DB is configured."""
        _, node = source_and_node
        packets = list(node.iter_packets())
        # Should not raise
        node.store_result(packets)


from orcapod.core.nodes import OperatorNode
from orcapod.core.operators import SelectPacketColumns
from orcapod.databases import InMemoryArrowDatabase
from orcapod.types import CacheMode


@pytest.fixture
def operator_with_db():
    table = pa.table(
        {
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "value": pa.array([10, 20], type=pa.int64()),
        }
    )
    src = ArrowTableSource(table, tag_columns=["key"])
    op = SelectPacketColumns(columns=["value"])
    db = InMemoryArrowDatabase()
    node = OperatorNode(
        op,
        input_streams=[src],
        pipeline_database=db,
        cache_mode=CacheMode.LOG,
    )
    return node, db


@pytest.fixture
def operator_no_db():
    table = pa.table(
        {
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "value": pa.array([10, 20], type=pa.int64()),
        }
    )
    src = ArrowTableSource(table, tag_columns=["key"])
    op = SelectPacketColumns(columns=["value"])
    return OperatorNode(op, input_streams=[src])


class TestOperatorNodeStoreResult:
    def test_store_result_writes_to_db_in_log_mode(self, operator_with_db):
        node, db = operator_with_db
        stream = node._operator.process(*node._input_streams)
        output = list(stream.iter_packets())
        node.store_result(output)

        records = node.get_all_records()
        assert records is not None
        assert records.num_rows == 2

    def test_store_result_noop_in_off_mode(self, operator_no_db):
        node = operator_no_db
        stream = node._operator.process(*node._input_streams)
        output = list(stream.iter_packets())
        node.store_result(output)

    def test_get_cached_output_returns_none_in_off_mode(self, operator_no_db):
        assert operator_no_db.get_cached_output() is None

    def test_get_cached_output_returns_none_in_log_mode(self, operator_with_db):
        node, _ = operator_with_db
        assert node.get_cached_output() is None

    def test_get_cached_output_returns_stream_in_replay_mode(self, operator_with_db):
        node, db = operator_with_db
        stream = node._operator.process(*node._input_streams)
        output = list(stream.iter_packets())
        node.store_result(output)

        node._cache_mode = CacheMode.REPLAY
        cached = node.get_cached_output()
        assert cached is not None
        cached_packets = list(cached.iter_packets())
        assert len(cached_packets) == 2


class TestOperatorNodeOperatorProperty:
    def test_operator_property_returns_operator(self, operator_no_db):
        assert operator_no_db.operator is operator_no_db._operator


from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode
from orcapod.core.packet_function import PythonPacketFunction


def double_value(value: int) -> int:
    return value * 2


@pytest.fixture
def function_node_with_db():
    table = pa.table(
        {
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "value": pa.array([1, 2], type=pa.int64()),
        }
    )
    src = ArrowTableSource(table, tag_columns=["key"])
    pf = PythonPacketFunction(double_value, output_keys="result")
    pod = FunctionPod(pf)
    pipeline_db = InMemoryArrowDatabase()
    result_db = InMemoryArrowDatabase()
    node = FunctionNode(
        pod,
        src,
        pipeline_database=pipeline_db,
        result_database=result_db,
    )
    return node, pipeline_db, result_db


@pytest.fixture
def function_node_no_db():
    table = pa.table(
        {
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "value": pa.array([1, 2], type=pa.int64()),
        }
    )
    src = ArrowTableSource(table, tag_columns=["key"])
    pf = PythonPacketFunction(double_value, output_keys="result")
    pod = FunctionPod(pf)
    return FunctionNode(pod, src)


class TestFunctionNodeProcessPacket:
    def test_process_packet_does_not_write_pipeline_record(self, function_node_with_db):
        """process_packet handles computation but NOT pipeline provenance."""
        node, pipeline_db, _ = function_node_with_db
        packets = list(node._input_stream.iter_packets())
        tag, packet = packets[0]

        tag_out, result = node.process_packet(tag, packet)
        assert result is not None

        records = pipeline_db.get_all_records(node.pipeline_path)
        assert records is None

    def test_process_packet_writes_to_result_db(self, function_node_with_db):
        """process_packet handles function-level memoization via CachedFunctionPod."""
        node, _, result_db = function_node_with_db
        packets = list(node._input_stream.iter_packets())
        tag, packet = packets[0]

        tag_out, result = node.process_packet(tag, packet)
        assert result is not None

        cached_results = node._cached_function_pod.get_all_cached_outputs()
        assert cached_results is not None
        assert cached_results.num_rows == 1

    def test_process_packet_returns_correct_result(self, function_node_no_db):
        node = function_node_no_db
        packets = list(node._input_stream.iter_packets())
        tag, packet = packets[0]

        tag_out, result = node.process_packet(tag, packet)
        assert result is not None
        assert result.as_dict()["result"] == 2


class TestFunctionNodeStoreResult:
    def test_store_result_writes_pipeline_record(self, function_node_with_db):
        node, pipeline_db, _ = function_node_with_db
        packets = list(node._input_stream.iter_packets())
        tag, packet = packets[0]

        tag_out, result = node.process_packet(tag, packet)
        node.store_result(tag, packet, result)

        records = pipeline_db.get_all_records(node.pipeline_path)
        assert records is not None
        assert records.num_rows == 1

    def test_store_result_does_not_write_to_result_db(self, function_node_with_db):
        """store_result only writes pipeline records, not result cache."""
        node, _, result_db = function_node_with_db
        packets = list(node._input_stream.iter_packets())
        tag, packet = packets[0]

        tag_out, result = node.process_packet(tag, packet)
        cached_before = node._cached_function_pod.get_all_cached_outputs()
        count_before = cached_before.num_rows if cached_before is not None else 0

        node.store_result(tag, packet, result)
        cached_after = node._cached_function_pod.get_all_cached_outputs()
        count_after = cached_after.num_rows if cached_after is not None else 0
        assert count_after == count_before

    def test_store_result_noop_without_db(self, function_node_no_db):
        node = function_node_no_db
        packets = list(node._input_stream.iter_packets())
        tag, packet = packets[0]

        tag_out, result = node.process_packet(tag, packet)
        node.store_result(tag, packet, result)

    def test_store_result_handles_none_output(self, function_node_with_db):
        node, pipeline_db, _ = function_node_with_db
        packets = list(node._input_stream.iter_packets())
        tag, packet = packets[0]

        node.store_result(tag, packet, None)
        records = pipeline_db.get_all_records(node.pipeline_path)
        assert records is None
