"""Tests for PodInvocation, FunctionInvocation, OperatorInvocation."""
from __future__ import annotations

import pytest
import pyarrow as pa

from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import Join
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.sources import ArrowTableSource


def _make_source(tag_col: str, data_col: str) -> ArrowTableSource:
    table = pa.table({
        tag_col: pa.array(["a", "b"], type=pa.large_string()),
        data_col: pa.array([1, 2], type=pa.int64()),
    })
    return ArrowTableSource(table, tag_columns=[tag_col], infer_nullable=True)


def _make_function_pod() -> FunctionPod:
    def double(value: int) -> int:
        return value * 2
    return FunctionPod(PythonDataFunction(double, output_keys="result"))


class TestFunctionInvocation:
    def test_import(self):
        from orcapod.pipeline.pod_invocation import FunctionInvocation
        assert FunctionInvocation is not None

    def test_hash_stability(self):
        from orcapod.pipeline.pod_invocation import FunctionInvocation
        pod = _make_function_pod()
        stream = _make_source("key", "value")
        inv1 = FunctionInvocation(pod=pod, input_streams=(stream,))
        inv2 = FunctionInvocation(pod=pod, input_streams=(stream,))
        assert inv1.content_hash() == inv2.content_hash()

    def test_hash_matches_function_node(self):
        """FunctionInvocation hash must equal FunctionNode hash — critical invariant."""
        from orcapod.pipeline.pod_invocation import FunctionInvocation
        from orcapod.core.nodes.function_node import FunctionNode
        pod = _make_function_pod()
        stream = _make_source("key", "value")
        inv = FunctionInvocation(pod=pod, input_streams=(stream,))
        node = FunctionNode(function_pod=pod, input_stream=stream)
        assert inv.content_hash() == node.content_hash()

    def test_label_stored(self):
        from orcapod.pipeline.pod_invocation import FunctionInvocation
        pod = _make_function_pod()
        stream = _make_source("key", "value")
        inv = FunctionInvocation(pod=pod, input_streams=(stream,), label="my_node")
        assert inv.label == "my_node"

    def test_pod_and_input_streams_accessible(self):
        from orcapod.pipeline.pod_invocation import FunctionInvocation
        pod = _make_function_pod()
        stream = _make_source("key", "value")
        inv = FunctionInvocation(pod=pod, input_streams=(stream,))
        assert inv.pod is pod
        assert inv.input_streams == (stream,)

    def test_isinstance_distinguishable_from_operator(self):
        from orcapod.pipeline.pod_invocation import FunctionInvocation, OperatorInvocation
        pod = _make_function_pod()
        stream = _make_source("key", "value")
        inv = FunctionInvocation(pod=pod, input_streams=(stream,))
        assert isinstance(inv, FunctionInvocation)
        assert not isinstance(inv, OperatorInvocation)


class TestOperatorInvocation:
    def test_import(self):
        from orcapod.pipeline.pod_invocation import OperatorInvocation
        assert OperatorInvocation is not None

    def test_hash_stability(self):
        from orcapod.pipeline.pod_invocation import OperatorInvocation
        join = Join()
        s1 = _make_source("key", "value")
        s2 = _make_source("key", "score")
        inv1 = OperatorInvocation(pod=join, input_streams=(s1, s2))
        inv2 = OperatorInvocation(pod=join, input_streams=(s1, s2))
        assert inv1.content_hash() == inv2.content_hash()

    def test_hash_matches_operator_node(self):
        """OperatorInvocation hash must equal OperatorNode hash — critical invariant."""
        from orcapod.pipeline.pod_invocation import OperatorInvocation
        from orcapod.core.nodes.operator_node import OperatorNode
        join = Join()
        s1 = _make_source("key", "value")
        s2 = _make_source("key", "score")
        inv = OperatorInvocation(pod=join, input_streams=(s1, s2))
        node = OperatorNode(operator=join, input_streams=(s1, s2))
        assert inv.content_hash() == node.content_hash()

    def test_commutative_operator_same_hash_regardless_of_input_order(self):
        """Join is commutative — hash must be order-independent."""
        from orcapod.pipeline.pod_invocation import OperatorInvocation
        join = Join()
        s1 = _make_source("key", "value")
        s2 = _make_source("key", "score")
        inv_ab = OperatorInvocation(pod=join, input_streams=(s1, s2))
        inv_ba = OperatorInvocation(pod=join, input_streams=(s2, s1))
        assert inv_ab.content_hash() == inv_ba.content_hash()

    def test_isinstance_distinguishable_from_function(self):
        from orcapod.pipeline.pod_invocation import FunctionInvocation, OperatorInvocation
        join = Join()
        s1 = _make_source("key", "value")
        s2 = _make_source("key", "score")
        inv = OperatorInvocation(pod=join, input_streams=(s1, s2))
        assert isinstance(inv, OperatorInvocation)
        assert not isinstance(inv, FunctionInvocation)
