"""Tests for the OperatorNode / OperatorJobNode split."""
from __future__ import annotations

import pytest

from orcapod.errors import PipelineJobRequiredError
from orcapod.types import Schema


@pytest.fixture
def source_pair():
    from orcapod.core.nodes.source_node import SourceNode

    tag_schema = Schema({"id": int})
    data_schema_a = Schema({"a": float})
    data_schema_b = Schema({"b": float})
    node_a = SourceNode(name="src_a", tag_schema=tag_schema, data_schema=data_schema_a)
    node_b = SourceNode(name="src_b", tag_schema=tag_schema, data_schema=data_schema_b)
    return node_a, node_b


class TestThinOperatorNode:
    def test_iter_data_raises_pipeline_job_required(self, source_pair):
        from orcapod.core.nodes.operator_node import OperatorNode
        from orcapod.core.operators.join import Join

        op = Join()
        node_a, node_b = source_pair
        op_node = OperatorNode(operator=op, input_streams=(node_a, node_b))
        with pytest.raises(PipelineJobRequiredError):
            list(op_node.iter_data())

    def test_node_type(self, source_pair):
        from orcapod.core.nodes.operator_node import OperatorNode
        from orcapod.core.operators.join import Join

        op = Join()
        node_a, node_b = source_pair
        op_node = OperatorNode(operator=op, input_streams=(node_a, node_b))
        assert op_node.node_type == "operator"

    def test_operator_node_does_not_accept_db_params(self, source_pair):
        """OperatorNode must NOT accept pipeline_database param."""
        from orcapod.core.nodes.operator_node import OperatorNode
        import inspect

        sig = inspect.signature(OperatorNode.__init__)
        assert "pipeline_database" not in sig.parameters, (
            "OperatorNode must not accept pipeline_database — use OperatorJobNode instead"
        )

    def test_as_node_returns_self(self, source_pair):
        from orcapod.core.nodes.operator_node import OperatorNode
        from orcapod.core.operators.join import Join

        op = Join()
        node_a, node_b = source_pair
        op_node = OperatorNode(operator=op, input_streams=(node_a, node_b))
        assert op_node.as_node() is op_node


class TestOperatorJobNodeHashParity:
    def test_content_hash_matches_operator_node(self, source_pair):
        from orcapod.core.nodes.operator_node import OperatorJobNode, OperatorNode
        from orcapod.core.operators.join import Join

        op = Join()
        node_a, node_b = source_pair
        thin = OperatorNode(operator=op, input_streams=(node_a, node_b))
        job = OperatorJobNode(operator=op, input_streams=(node_a, node_b))
        assert thin.content_hash() == job.content_hash()

    def test_pipeline_hash_matches_operator_node(self, source_pair):
        from orcapod.core.nodes.operator_node import OperatorJobNode, OperatorNode
        from orcapod.core.operators.join import Join

        op = Join()
        node_a, node_b = source_pair
        thin = OperatorNode(operator=op, input_streams=(node_a, node_b))
        job = OperatorJobNode(operator=op, input_streams=(node_a, node_b))
        assert thin.pipeline_hash() == job.pipeline_hash()

    def test_as_node_returns_operator_node(self, source_pair):
        from orcapod.core.nodes.operator_node import OperatorJobNode, OperatorNode
        from orcapod.core.operators.join import Join

        op = Join()
        node_a, node_b = source_pair
        job = OperatorJobNode(operator=op, input_streams=(node_a, node_b))
        thin = job.as_node()
        assert isinstance(thin, OperatorNode)
        assert thin.content_hash() == job.content_hash()


class TestSiblingHierarchy:
    """OperatorNode and OperatorJobNode must be siblings, not parent/child."""

    def test_operator_node_not_subclass_of_operator_job_node(self):
        from orcapod.core.nodes.operator_node import OperatorJobNode, OperatorNode

        assert not issubclass(OperatorNode, OperatorJobNode), (
            "OperatorNode must NOT inherit from OperatorJobNode"
        )

    def test_operator_job_node_not_subclass_of_operator_node(self):
        from orcapod.core.nodes.operator_node import OperatorJobNode, OperatorNode

        assert not issubclass(OperatorJobNode, OperatorNode), (
            "OperatorJobNode must NOT inherit from OperatorNode"
        )

    def test_both_inherit_from_base(self):
        from orcapod.core.nodes.operator_node import OperatorJobNode, OperatorNode, OperatorNodeBase

        assert issubclass(OperatorNode, OperatorNodeBase)
        assert issubclass(OperatorJobNode, OperatorNodeBase)
