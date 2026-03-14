"""Tests for FunctionNode.get_cached_results."""

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
    return node


class TestGetCachedResults:
    def test_returns_empty_dict_when_no_db(self):
        table = pa.table(
            {
                "key": pa.array(["a"], type=pa.large_string()),
                "value": pa.array([1], type=pa.int64()),
            }
        )
        src = ArrowTableSource(table, tag_columns=["key"])
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionNode(pod, src)
        assert node.get_cached_results([]) == {}

    def test_returns_empty_dict_when_db_empty(self, function_node_with_db):
        assert function_node_with_db.get_cached_results(["nonexistent"]) == {}

    def test_returns_cached_results_for_matching_entry_ids(self, function_node_with_db):
        node = function_node_with_db
        packets = list(node._input_stream.iter_packets())

        entry_ids = []
        for tag, packet in packets:
            tag_out, result = node.process_packet(tag, packet)
            node.store_result(tag, packet, result)
            entry_ids.append(node.compute_pipeline_entry_id(tag, packet))

        cached = node.get_cached_results(entry_ids)
        assert len(cached) == 2
        assert all(eid in cached for eid in entry_ids)

    def test_filters_to_requested_entry_ids_only(self, function_node_with_db):
        node = function_node_with_db
        packets = list(node._input_stream.iter_packets())

        entry_ids = []
        for tag, packet in packets:
            tag_out, result = node.process_packet(tag, packet)
            node.store_result(tag, packet, result)
            entry_ids.append(node.compute_pipeline_entry_id(tag, packet))

        cached = node.get_cached_results([entry_ids[0]])
        assert len(cached) == 1
        assert entry_ids[0] in cached
        assert entry_ids[1] not in cached
