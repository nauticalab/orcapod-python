# tests/test_pipeline/test_node_protocols.py
"""Tests for revised node protocols."""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, AsyncMock

from orcapod.protocols.node_protocols import (
    SourceNodeProtocol,
    FunctionNodeProtocol,
    OperatorNodeProtocol,
    is_source_node,
    is_function_node,
    is_operator_node,
)


class TestSourceNodeProtocol:
    def test_requires_execute(self):
        """SourceNodeProtocol requires execute method."""

        class GoodSource:
            node_type = "source"

            def execute(self, *, observer=None):
                return []

            async def async_execute(self, output, *, observer=None):
                pass

        assert isinstance(GoodSource(), SourceNodeProtocol)

    def test_rejects_old_iter_packets_only(self):
        """SourceNodeProtocol no longer accepts iter_packets alone."""

        class OldSource:
            node_type = "source"

            def iter_packets(self):
                return iter([])

        assert not isinstance(OldSource(), SourceNodeProtocol)


class TestFunctionNodeProtocol:
    def test_requires_execute_and_async_execute(self):
        class GoodFunction:
            node_type = "function"

            def execute(self, input_stream, *, observer=None):
                return []

            async def async_execute(self, input_channel, output, *, observer=None):
                pass

        assert isinstance(GoodFunction(), FunctionNodeProtocol)

    def test_rejects_old_protocol(self):
        """Old protocol with get_cached_results etc. is not sufficient."""

        class OldFunction:
            node_type = "function"

            def get_cached_results(self, entry_ids):
                return {}

            def compute_pipeline_entry_id(self, tag, packet):
                return ""

            def execute_packet(self, tag, packet):
                return (tag, None)

            def execute(self, input_stream):
                return []

        # Missing async_execute → not a valid FunctionNodeProtocol
        assert not isinstance(OldFunction(), FunctionNodeProtocol)


class TestOperatorNodeProtocol:
    def test_requires_execute_and_async_execute(self):
        class GoodOperator:
            node_type = "operator"

            def execute(self, *input_streams, observer=None):
                return []

            async def async_execute(self, inputs, output, *, observer=None):
                pass

        assert isinstance(GoodOperator(), OperatorNodeProtocol)

    def test_rejects_old_protocol(self):
        """Old protocol with get_cached_output is not sufficient."""

        class OldOperator:
            node_type = "operator"

            def execute(self, *input_streams):
                return []

            def get_cached_output(self):
                return None

        # Missing async_execute → not valid
        assert not isinstance(OldOperator(), OperatorNodeProtocol)


class TestTypeGuardDispatch:
    def test_dispatch_source(self):
        node = MagicMock()
        node.node_type = "source"
        assert is_source_node(node)
        assert not is_function_node(node)
        assert not is_operator_node(node)

    def test_dispatch_function(self):
        node = MagicMock()
        node.node_type = "function"
        assert is_function_node(node)

    def test_dispatch_operator(self):
        node = MagicMock()
        node.node_type = "operator"
        assert is_operator_node(node)


import pyarrow as pa
from orcapod.core.sources import ArrowTableSource
from orcapod.core.nodes import SourceNode


class TestSourceNodeExecute:
    def _make_source_node(self):
        table = pa.table({
            "key": pa.array(["a", "b", "c"], type=pa.large_string()),
            "value": pa.array([1, 2, 3], type=pa.int64()),
        })
        src = ArrowTableSource(table, tag_columns=["key"])
        return SourceNode(src)

    def test_execute_returns_list(self):
        node = self._make_source_node()
        result = node.execute()
        assert isinstance(result, list)
        assert len(result) == 3

    def test_execute_populates_cached_results(self):
        node = self._make_source_node()
        node.execute()
        assert node._cached_results is not None
        assert len(node._cached_results) == 3

    def test_execute_with_observer(self):
        node = self._make_source_node()
        events = []

        class Obs:
            def on_node_start(self, n):
                events.append(("start", n.node_type))
            def on_node_end(self, n):
                events.append(("end", n.node_type))
            def on_packet_start(self, n, t, p):
                pass
            def on_packet_end(self, n, t, ip, op, cached):
                pass

        node.execute(observer=Obs())
        assert events == [("start", "source"), ("end", "source")]

    def test_execute_without_observer(self):
        """execute() works fine with no observer."""
        node = self._make_source_node()
        result = node.execute()
        assert len(result) == 3


import pytest
from orcapod.channels import Channel


class TestSourceNodeAsyncExecuteProtocol:
    @pytest.mark.asyncio
    async def test_tightened_signature(self):
        """async_execute takes output only, no inputs."""
        table = pa.table({
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "value": pa.array([1, 2], type=pa.int64()),
        })
        src = ArrowTableSource(table, tag_columns=["key"])
        node = SourceNode(src)

        output_ch = Channel(buffer_size=16)
        await node.async_execute(output_ch.writer, observer=None)
        rows = await output_ch.reader.collect()
        assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_async_execute_with_observer(self):
        table = pa.table({
            "key": pa.array(["a"], type=pa.large_string()),
            "value": pa.array([1], type=pa.int64()),
        })
        src = ArrowTableSource(table, tag_columns=["key"])
        node = SourceNode(src)
        events = []

        class Obs:
            def on_node_start(self, n):
                events.append("start")
            def on_node_end(self, n):
                events.append("end")
            def on_packet_start(self, n, t, p):
                pass
            def on_packet_end(self, n, t, ip, op, cached):
                pass

        output_ch = Channel(buffer_size=16)
        await node.async_execute(output_ch.writer, observer=Obs())
        assert events == ["start", "end"]


# ===========================================================================
# FunctionNode.execute() with observer
# ===========================================================================

from orcapod.core.function_pod import FunctionPod
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.nodes import FunctionNode


def double_value(value: int) -> int:
    return value * 2


class TestFunctionNodeExecute:
    def _make_function_node(self):
        table = pa.table({
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "value": pa.array([1, 2], type=pa.int64()),
        })
        src = ArrowTableSource(table, tag_columns=["key"])
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)
        return FunctionNode(pod, src)

    def test_execute_with_observer(self):
        node = self._make_function_node()
        events = []

        class Obs:
            def on_node_start(self, n):
                events.append(("node_start", n.node_type))
            def on_node_end(self, n):
                events.append(("node_end", n.node_type))
            def on_packet_start(self, n, t, p):
                events.append(("packet_start",))
            def on_packet_end(self, n, t, ip, op, cached):
                events.append(("packet_end", cached))

        input_stream = node._input_stream
        result = node.execute(input_stream, observer=Obs())

        assert len(result) == 2
        assert events[0] == ("node_start", "function")
        assert events[-1] == ("node_end", "function")
        packet_events = [e for e in events if e[0].startswith("packet")]
        assert len(packet_events) == 4  # 2 start + 2 end

    def test_execute_without_observer(self):
        node = self._make_function_node()
        input_stream = node._input_stream
        result = node.execute(input_stream)
        assert len(result) == 2
        values = sorted([pkt.as_dict()["result"] for _, pkt in result])
        assert values == [2, 4]


# ===========================================================================
# FunctionNode.async_execute() with tightened signature
# ===========================================================================


class TestFunctionNodeAsyncExecute:
    @pytest.mark.asyncio
    async def test_tightened_signature(self):
        """async_execute takes single input_channel, not Sequence."""
        table = pa.table({
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "value": pa.array([1, 2], type=pa.int64()),
        })
        src = ArrowTableSource(table, tag_columns=["key"])
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionNode(pod, src)

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)

        for tag, packet in src.iter_packets():
            await input_ch.writer.send((tag, packet))
        await input_ch.writer.close()

        await node.async_execute(input_ch.reader, output_ch.writer)
        rows = await output_ch.reader.collect()
        assert len(rows) == 2
        values = sorted([pkt.as_dict()["result"] for _, pkt in rows])
        assert values == [2, 4]

    @pytest.mark.asyncio
    async def test_async_execute_with_observer(self):
        table = pa.table({
            "key": pa.array(["a"], type=pa.large_string()),
            "value": pa.array([1], type=pa.int64()),
        })
        src = ArrowTableSource(table, tag_columns=["key"])
        pf = PythonPacketFunction(double_value, output_keys="result")
        pod = FunctionPod(pf)
        node = FunctionNode(pod, src)

        events = []

        class Obs:
            def on_node_start(self, n):
                events.append("node_start")
            def on_node_end(self, n):
                events.append("node_end")
            def on_packet_start(self, n, t, p):
                events.append("pkt_start")
            def on_packet_end(self, n, t, ip, op, cached):
                events.append("pkt_end")

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)
        for tag, packet in src.iter_packets():
            await input_ch.writer.send((tag, packet))
        await input_ch.writer.close()

        await node.async_execute(input_ch.reader, output_ch.writer, observer=Obs())
        assert "node_start" in events
        assert "node_end" in events


# ===========================================================================
# OperatorNode.execute() with observer + cache check
# ===========================================================================

from orcapod.core.nodes import OperatorNode
from orcapod.core.operators.join import Join


class TestOperatorNodeExecute:
    def _make_join_node(self):
        table_a = pa.table({
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "value": pa.array([10, 20], type=pa.int64()),
        })
        table_b = pa.table({
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "score": pa.array([100, 200], type=pa.int64()),
        })
        src_a = ArrowTableSource(table_a, tag_columns=["key"])
        src_b = ArrowTableSource(table_b, tag_columns=["key"])
        return OperatorNode(Join(), input_streams=[src_a, src_b])

    def test_execute_with_observer(self):
        node = self._make_join_node()
        events = []

        class Obs:
            def on_node_start(self, n):
                events.append(("node_start", n.node_type))
            def on_node_end(self, n):
                events.append(("node_end", n.node_type))
            def on_packet_start(self, n, t, p):
                pass
            def on_packet_end(self, n, t, ip, op, cached):
                pass

        result = node.execute(*node._input_streams, observer=Obs())
        assert len(result) == 2
        assert events == [("node_start", "operator"), ("node_end", "operator")]

    def test_execute_without_observer(self):
        node = self._make_join_node()
        result = node.execute(*node._input_streams)
        assert len(result) == 2


# ===========================================================================
# OperatorNode.async_execute() with observer
# ===========================================================================

from orcapod.core.operators import SelectPacketColumns


class TestOperatorNodeAsyncExecute:
    @pytest.mark.asyncio
    async def test_async_execute_with_observer(self):
        table_a = pa.table({
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "value": pa.array([10, 20], type=pa.int64()),
        })
        src_a = ArrowTableSource(table_a, tag_columns=["key"])
        op = SelectPacketColumns(columns=["value"])
        op_node = OperatorNode(op, input_streams=[src_a])

        events = []

        class Obs:
            def on_node_start(self, n):
                events.append("start")
            def on_node_end(self, n):
                events.append("end")
            def on_packet_start(self, n, t, p):
                pass
            def on_packet_end(self, n, t, ip, op, cached):
                pass

        input_ch = Channel(buffer_size=16)
        output_ch = Channel(buffer_size=16)
        for tag, packet in src_a.iter_packets():
            await input_ch.writer.send((tag, packet))
        await input_ch.writer.close()

        await op_node.async_execute(
            [input_ch.reader], output_ch.writer, observer=Obs()
        )
        assert "start" in events
        assert "end" in events
