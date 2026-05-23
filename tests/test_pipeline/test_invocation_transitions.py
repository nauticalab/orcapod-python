"""Tests for to_invocations / from_invocations and cross-class transitions."""
from __future__ import annotations

import pytest
import pyarrow as pa

from orcapod.core.function_pod import FunctionPod
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.core.nodes.source_node import SourceNode, SourceJobNode
from orcapod.pipeline import Pipeline
from orcapod.pipeline.job import PipelineJob


def _src(tag_col: str, data_col: str) -> ArrowTableSource:
    table = pa.table({
        tag_col: pa.array(["a", "b"], type=pa.large_string()),
        data_col: pa.array([1, 2], type=pa.int64()),
    })
    return ArrowTableSource(table, tag_columns=[tag_col], infer_nullable=True)


def _fn() -> FunctionPod:
    def double(value: int) -> int:
        return value * 2
    return FunctionPod(PythonDataFunction(double, output_keys="result"))


class TestToInvocations:
    def test_returns_invocation_graph(self):
        from orcapod.pipeline.base import InvocationGraph
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(stream, label="out")
        graph = pipeline.to_invocations()
        assert isinstance(graph, InvocationGraph)

    def test_invocation_graph_contains_one_invocation(self):
        from orcapod.pipeline.pod_invocation import FunctionInvocation
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(stream, label="out")
        graph = pipeline.to_invocations()
        assert len(graph.invocations) == 1
        assert isinstance(graph.invocations[0], FunctionInvocation)

    def test_invocation_graph_source_streams_has_source_node(self):
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(stream, label="out")
        graph = pipeline.to_invocations()
        assert len(graph.source_streams) == 1
        source = list(graph.source_streams.values())[0]
        assert isinstance(source, SourceNode)


class TestFromInvocations:
    def test_pipeline_from_invocations_roundtrip(self):
        """Pipeline → to_invocations() → Pipeline.from_invocations() must produce equivalent graph."""
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(stream, label="out")
        graph = pipeline.to_invocations()
        pipeline2 = Pipeline.from_invocations(graph)
        assert "out" in pipeline2._nodes
        assert len(pipeline2._persistent_node_map) == len(pipeline._persistent_node_map)

    def test_pipeline_to_job_via_from_invocations(self):
        """PipelineJob.from_invocations(pipeline.to_invocations()) must produce bound-ready job."""
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(stream, label="out")
        job = PipelineJob.from_invocations(pipeline.to_invocations())
        assert "out" in job._nodes
        # Source nodes should be unbound SourceJobNodes
        source_nodes = [
            n for n in job._persistent_node_map.values()
            if isinstance(n, SourceJobNode)
        ]
        assert len(source_nodes) == 1
        assert source_nodes[0].bound_source is None


class TestFromPipeline:
    def test_from_pipeline_thin_composition(self):
        """PipelineJob.from_pipeline(pipeline) must produce the same result as going via invocations."""
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(stream, label="out")
        job = PipelineJob.from_pipeline(pipeline)
        assert "out" in job._nodes

    def test_as_pipeline_thin_composition(self):
        """job.as_pipeline() must produce a Pipeline structurally equivalent to the original."""
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(stream, label="out")
        job = PipelineJob.from_pipeline(pipeline)
        pipeline2 = job.as_pipeline()
        assert "out" in pipeline2._nodes


class TestSourceJobNodeFromStream:
    def test_sjn_from_sjn_copies_bound_source(self):
        """SJN → SJN: bound_source is copied; the input SJN is NOT used as bound_source."""
        concrete = _src("key", "value")
        sjn = SourceJobNode(
            name="data",
            tag_schema=concrete.output_schema()[0],
            data_schema=concrete.output_schema()[1],
            bound_source=concrete,
        )
        sjn2 = SourceJobNode.from_stream(sjn)
        assert isinstance(sjn2, SourceJobNode)
        assert sjn2.bound_source is concrete  # bound_source copied, not sjn itself
        assert sjn2.bound_source is not sjn   # sjn itself is NOT the bound_source

    def test_sjn_from_source_node_creates_unbound(self):
        """SourceNode → SJN: creates unbound SourceJobNode with matching schema."""
        from orcapod.types import Schema
        sn = SourceNode(
            name="data",
            tag_schema=Schema({"key": str}),
            data_schema=Schema({"value": int}),
        )
        sjn = SourceJobNode.from_stream(sn)
        assert isinstance(sjn, SourceJobNode)
        assert sjn.bound_source is None
        assert sjn.name == "data"

    def test_sjn_from_concrete_stream_creates_bound(self):
        """Concrete stream → SJN: creates bound SourceJobNode; hash equals bound source hash."""
        concrete = _src("key", "value")
        sjn = SourceJobNode.from_stream(concrete)
        assert isinstance(sjn, SourceJobNode)
        assert sjn.bound_source is concrete
        assert sjn.content_hash() == concrete.content_hash()

    def test_unbound_sjn_hash_equals_source_node_hash(self):
        """Unbound SJN must have same content_hash as the corresponding SourceNode."""
        from orcapod.types import Schema
        sn = SourceNode(
            name="data",
            tag_schema=Schema({"key": str}),
            data_schema=Schema({"value": int}),
        )
        sjn = SourceJobNode.from_stream(sn)
        assert sjn.content_hash() == sn.content_hash()


class TestCrossWithBlockReconnection:
    def test_pipelinejob_same_concrete_source_in_two_blocks_same_hash(self):
        """PipelineJob: same concrete source in two with-blocks produces the same source node hash."""
        pod1 = _fn()

        def double2(value: int) -> int:
            return value * 3

        pod2 = FunctionPod(PythonDataFunction(double2, output_keys="result"))
        concrete = _src("key", "value")

        job = PipelineJob(name="test", auto_compile=False)
        with job:
            out1 = pod1(concrete, label="step1")
        source_hash_first = list(
            h for h, n in job._persistent_node_map.items()
            if isinstance(n, SourceJobNode)
        )[0]
        with job:
            pod2(concrete, label="step2")
        source_hash_second = list(
            h for h, n in job._persistent_node_map.items()
            if isinstance(n, SourceJobNode)
        )[0]
        assert source_hash_first == source_hash_second

    def test_pipeline_source_hash_after_compile_differs_from_concrete_stream_hash(self):
        """Pipeline: SourceNode's own content_hash() is schema-based, not data-inclusive."""
        from orcapod.core.nodes.source_node import SourceNode
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(stream, label="out")
        original_hash = stream.content_hash().to_string()
        # After compile, the source entry is a SourceNode.
        # Its own content_hash() is schema-based and must differ from the
        # data-inclusive hash of the original concrete ArrowTableSource.
        source_nodes = {
            h: n for h, n in pipeline._persistent_node_map.items()
            if isinstance(n, SourceNode)
        }
        assert len(source_nodes) == 1
        source_node = list(source_nodes.values())[0]
        assert source_node.content_hash().to_string() != original_hash
