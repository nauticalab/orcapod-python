import pytest
from orcapod.protocols.node_protocols import FunctionNodeProtocol, OperatorNodeProtocol
from orcapod.protocols.pipeline_protocols import PipelineProtocol


def test_function_node_protocol_has_set_ephemeral_store():
    assert hasattr(FunctionNodeProtocol, "set_ephemeral_store")


def test_operator_node_protocol_has_set_ephemeral_store():
    assert hasattr(OperatorNodeProtocol, "set_ephemeral_store")


def test_pipeline_protocol_has_set_ephemeral_store():
    assert hasattr(PipelineProtocol, "set_ephemeral_store")
