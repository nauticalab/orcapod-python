"""Tests for populate_cache on all node types."""

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


class TestSourceNodePopulateCache:
    def test_iter_packets_uses_cache_when_populated(self, source_and_node):
        src, node = source_and_node
        original = list(node.iter_packets())
        assert len(original) == 2

        # Populate cache with only the first packet
        node.populate_cache([original[0]])

        cached = list(node.iter_packets())
        assert len(cached) == 1

    def test_iter_packets_delegates_to_stream_when_no_cache(self, source_and_node):
        _, node = source_and_node
        result = list(node.iter_packets())
        assert len(result) == 2


from orcapod.core.nodes import OperatorNode
from orcapod.core.operators import SelectPacketColumns


@pytest.fixture
def operator_node_with_data():
    table = pa.table(
        {
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "value": pa.array([10, 20], type=pa.int64()),
        }
    )
    src = ArrowTableSource(table, tag_columns=["key"])
    op = SelectPacketColumns(columns=["value"])
    node = OperatorNode(op, input_streams=[src])
    return node


class TestOperatorNodePopulateCache:
    def test_iter_packets_uses_cache_when_populated(self, operator_node_with_data):
        node = operator_node_with_data
        node.run()
        original = list(node.iter_packets())
        assert len(original) == 2

        node.clear_cache()
        node.populate_cache([original[0]])
        cached = list(node.iter_packets())
        assert len(cached) == 1

    def test_populate_cache_empty_list_clears(self, operator_node_with_data):
        node = operator_node_with_data
        node.run()
        assert len(list(node.iter_packets())) == 2

        node.populate_cache([])
        # After clearing, run() should recompute
        node.run()
        assert len(list(node.iter_packets())) == 2


from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode
from orcapod.core.packet_function import PythonPacketFunction


class TestFunctionNodePopulateCache:
    def test_iter_packets_uses_cache_when_populated(self):
        table = pa.table(
            {
                "key": pa.array(["a", "b"], type=pa.large_string()),
                "value": pa.array([1, 2], type=pa.int64()),
            }
        )
        src = ArrowTableSource(table, tag_columns=["key"])

        def double_value(value: int) -> int:
            return value * 2

        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionNode(pod, src)

        original = list(node.iter_packets())
        assert len(original) == 2

        node.clear_cache()
        node.populate_cache([original[0]])
        cached = list(node.iter_packets())
        assert len(cached) == 1
