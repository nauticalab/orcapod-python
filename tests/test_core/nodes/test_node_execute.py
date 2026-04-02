"""Tests for node execute methods (persistence, caching).

Note: execute / execute_packet are internal methods for orchestrators.
The caller guarantees input identity — no schema validation is performed.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase


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
    src = ArrowTableSource(table, tag_columns=["key"], infer_nullable=True)
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
    src = ArrowTableSource(table, tag_columns=["key"], infer_nullable=True)
    pf = PythonPacketFunction(double_value, output_keys="result")
    pod = FunctionPod(pf)
    return FunctionNode(pod, src)


class TestFunctionNodeExecutePacket:
    def test_returns_correct_result(self, function_node_no_db):
        node = function_node_no_db
        packets = list(node._input_stream.iter_packets())
        tag, packet = packets[0]
        tag_out, result = node.execute_packet(tag, packet)
        assert result is not None
        assert result.as_dict()["result"] == 2

    def test_writes_pipeline_record(self, function_node_with_db):
        node, pipeline_db, _ = function_node_with_db
        packets = list(node._input_stream.iter_packets())
        tag, packet = packets[0]
        node.execute_packet(tag, packet)
        records = pipeline_db.get_all_records(node.node_identity_path)
        assert records is not None
        assert records.num_rows == 1

    def test_writes_to_result_db(self, function_node_with_db):
        node, _, _ = function_node_with_db
        packets = list(node._input_stream.iter_packets())
        tag, packet = packets[0]
        node.execute_packet(tag, packet)
        cached = node._cached_function_pod.get_all_cached_outputs()
        assert cached is not None
        assert cached.num_rows == 1

    def test_caches_internally(self, function_node_with_db):
        node, _, _ = function_node_with_db
        packets = list(node._input_stream.iter_packets())
        tag, packet = packets[0]
        node.execute_packet(tag, packet)
        assert len(node._cached_output_packets) == 1


class TestFunctionNodeExecute:
    def test_returns_materialized_results(self, function_node_with_db):
        node, _, _ = function_node_with_db
        results = node.execute(node._input_stream)
        assert isinstance(results, list)
        assert len(results) == 2
        values = sorted([pkt.as_dict()["result"] for _, pkt in results])
        assert values == [2, 4]

    def test_writes_pipeline_records(self, function_node_with_db):
        node, pipeline_db, _ = function_node_with_db
        node.execute(node._input_stream)
        records = pipeline_db.get_all_records(node.node_identity_path)
        assert records is not None
        assert records.num_rows == 2

    def test_caches_internally(self, function_node_with_db):
        node, _, _ = function_node_with_db
        node.execute(node._input_stream)
        assert len(node._cached_output_packets) == 2


# ------------------------------------------------------------------
# OperatorNode.execute() tests
# ------------------------------------------------------------------

from orcapod.core.nodes import OperatorNode
from orcapod.core.operators import SelectPacketColumns
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
    return node, db, src


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
    node = OperatorNode(op, input_streams=[src])
    return node, src


class TestOperatorNodeExecute:
    def test_returns_materialized_results(self, operator_no_db):
        node, src = operator_no_db
        results = node.execute(src)
        assert isinstance(results, list)
        assert len(results) == 2

    def test_writes_to_db_in_log_mode(self, operator_with_db):
        node, db, src = operator_with_db
        node.execute(src)
        records = node.get_all_records()
        assert records is not None
        assert records.num_rows == 2

    def test_caches_internally(self, operator_no_db):
        node, src = operator_no_db
        node.execute(src)
        cached = list(node.iter_packets())
        assert len(cached) == 2

    def test_noop_db_in_off_mode(self, operator_no_db):
        node, src = operator_no_db
        results = node.execute(src)
        assert len(results) == 2
        assert node.get_all_records() is None
