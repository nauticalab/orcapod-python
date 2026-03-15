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
