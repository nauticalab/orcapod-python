"""Tests for node protocol TypeGuard dispatch functions."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.nodes import FunctionNode, OperatorNode, SourceNode
from orcapod.protocols.node_protocols import (
    is_function_node,
    is_operator_node,
    is_source_node,
)


@pytest.fixture
def _sample_source():
    from orcapod.core.sources import ArrowTableSource

    table = pa.table(
        {
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "value": pa.array([1, 2], type=pa.int64()),
        }
    )
    return ArrowTableSource(table, tag_columns=["key"], infer_nullable=True)


@pytest.fixture
def source_node(_sample_source):
    return SourceNode(_sample_source)


@pytest.fixture
def function_node(_sample_source):
    from orcapod.core.function_pod import FunctionPod
    from orcapod.core.packet_function import PythonPacketFunction

    def double(value: int) -> int:
        return value * 2

    pf = PythonPacketFunction(double, output_keys="result")
    pod = FunctionPod(pf)
    return FunctionNode(pod, _sample_source)


@pytest.fixture
def operator_node(_sample_source):
    from orcapod.core.operators import SelectPacketColumns

    op = SelectPacketColumns(columns=["value"])
    return OperatorNode(op, input_streams=[_sample_source])


class TestTypeGuardDispatch:
    """TypeGuard functions correctly narrow node types."""

    def test_is_source_node_true(self, source_node):
        assert is_source_node(source_node) is True

    def test_is_source_node_false_for_function(self, function_node):
        assert is_source_node(function_node) is False

    def test_is_function_node_true(self, function_node):
        assert is_function_node(function_node) is True

    def test_is_function_node_false_for_operator(self, operator_node):
        assert is_function_node(operator_node) is False

    def test_is_operator_node_true(self, operator_node):
        assert is_operator_node(operator_node) is True

    def test_is_operator_node_false_for_source(self, source_node):
        assert is_operator_node(source_node) is False
