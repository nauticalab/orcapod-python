"""Tests for the FunctionNode / FunctionJobNode split."""
from __future__ import annotations

import pytest

from orcapod.errors import PipelineJobRequiredError
from orcapod.types import Schema


@pytest.fixture
def simple_setup():
    """A minimal function pod + source node fixture."""
    from orcapod.core.function_pod import FunctionPod
    from orcapod.core.data_function import PythonDataFunction
    from orcapod.core.nodes.source_node import SourceNode

    tag_schema = Schema({"id": int})
    data_schema = Schema({"value": float})

    source_node = SourceNode(name="src", tag_schema=tag_schema, data_schema=data_schema)

    def double(value: float) -> float:
        return value * 2

    pf = PythonDataFunction(double, output_keys="result")
    pod = FunctionPod(pf)

    return {
        "source_node": source_node,
        "pod": pod,
    }


class TestThinFunctionNode:
    def test_iter_data_raises_pipeline_job_required(self, simple_setup):
        from orcapod.core.nodes.function_node import FunctionNode

        fn = FunctionNode(
            function_pod=simple_setup["pod"],
            input_stream=simple_setup["source_node"],
        )
        with pytest.raises(PipelineJobRequiredError):
            list(fn.iter_data())

    def test_content_hash_is_stable(self, simple_setup):
        from orcapod.core.nodes.function_node import FunctionNode

        fn = FunctionNode(
            function_pod=simple_setup["pod"],
            input_stream=simple_setup["source_node"],
        )
        h1 = fn.content_hash()
        h2 = fn.content_hash()
        assert h1 == h2

    def test_node_type(self, simple_setup):
        from orcapod.core.nodes.function_node import FunctionNode

        fn = FunctionNode(
            function_pod=simple_setup["pod"],
            input_stream=simple_setup["source_node"],
        )
        assert fn.node_type == "function"

    def test_output_schema(self, simple_setup):
        from orcapod.core.nodes.function_node import FunctionNode

        fn = FunctionNode(
            function_pod=simple_setup["pod"],
            input_stream=simple_setup["source_node"],
        )
        tag_s, data_s = fn.output_schema()
        assert "result" in data_s

    def test_function_node_does_not_accept_db_params(self, simple_setup):
        """FunctionNode must NOT accept pipeline_database param."""
        from orcapod.core.nodes.function_node import FunctionNode
        import inspect

        sig = inspect.signature(FunctionNode.__init__)
        assert "pipeline_database" not in sig.parameters, (
            "FunctionNode must not accept pipeline_database — use FunctionJobNode instead"
        )


class TestFunctionJobNodeHashParity:
    """FunctionJobNode must have identical content_hash / pipeline_hash to FunctionNode."""

    def test_content_hash_matches_function_node(self, simple_setup):
        from orcapod.core.nodes.function_node import FunctionJobNode, FunctionNode

        fn = FunctionNode(
            function_pod=simple_setup["pod"],
            input_stream=simple_setup["source_node"],
        )
        fjn = FunctionJobNode(
            function_pod=simple_setup["pod"],
            input_stream=simple_setup["source_node"],
        )
        assert fn.content_hash() == fjn.content_hash()

    def test_pipeline_hash_matches_function_node(self, simple_setup):
        from orcapod.core.nodes.function_node import FunctionJobNode, FunctionNode

        fn = FunctionNode(
            function_pod=simple_setup["pod"],
            input_stream=simple_setup["source_node"],
        )
        fjn = FunctionJobNode(
            function_pod=simple_setup["pod"],
            input_stream=simple_setup["source_node"],
        )
        assert fn.pipeline_hash() == fjn.pipeline_hash()

    def test_as_node_returns_function_node(self, simple_setup):
        from orcapod.core.nodes.function_node import FunctionJobNode, FunctionNode

        fjn = FunctionJobNode(
            function_pod=simple_setup["pod"],
            input_stream=simple_setup["source_node"],
        )
        fn = fjn.as_node()
        assert isinstance(fn, FunctionNode)
        assert fn.content_hash() == fjn.content_hash()


class TestSiblingHierarchy:
    """FunctionNode and FunctionJobNode must be siblings, not parent/child."""

    def test_function_node_not_subclass_of_function_job_node(self):
        from orcapod.core.nodes.function_node import FunctionJobNode, FunctionNode

        assert not issubclass(FunctionNode, FunctionJobNode), (
            "FunctionNode must NOT inherit from FunctionJobNode"
        )

    def test_function_job_node_not_subclass_of_function_node(self):
        from orcapod.core.nodes.function_node import FunctionJobNode, FunctionNode

        assert not issubclass(FunctionJobNode, FunctionNode), (
            "FunctionJobNode must NOT inherit from FunctionNode"
        )

    def test_both_inherit_from_base(self):
        from orcapod.core.nodes.function_node import FunctionJobNode, FunctionNode, FunctionNodeBase

        assert issubclass(FunctionNode, FunctionNodeBase)
        assert issubclass(FunctionJobNode, FunctionNodeBase)
