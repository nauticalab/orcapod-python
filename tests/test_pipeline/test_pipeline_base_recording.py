"""Tests for the unified recording path in AbstractPipelineBase.

These tests verify the new _record_invocation / _invocation_lut / _source_streams
mechanics by exercising them through Pipeline (concrete subclass).
"""
from __future__ import annotations

import pytest
import pyarrow as pa

from orcapod.core.function_pod import FunctionPod
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.pipeline import Pipeline


def _src(tag_col: str, data_col: str) -> ArrowTableSource:
    table = pa.table({
        tag_col: pa.array(["a", "b"], type=pa.large_string()),
        data_col: pa.array([1, 2], type=pa.int64()),
    })
    return ArrowTableSource(table, tag_columns=[tag_col], infer_nullable=True)


def _fn(name: str = "double") -> FunctionPod:
    def fn(value: int) -> int:
        return value * 2
    fn.__name__ = name
    return FunctionPod(PythonDataFunction(fn, output_keys="result"))


class TestRecordFunctionPodInvocation:
    def test_record_function_pod_adds_to_invocation_lut(self):
        """After recording a function pod, _invocation_lut must contain the invocation."""
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test", auto_compile=False)
        with pipeline:
            pod(stream, label="out")
        # _invocation_lut is the new field; _node_lut is the legacy compiled field
        assert len(pipeline._invocation_lut) == 1

    def test_record_function_pod_captures_source_stream(self):
        """The upstream concrete stream must land in _source_streams."""
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test", auto_compile=False)
        with pipeline:
            pod(stream, label="out")
        stream_hash = stream.content_hash().to_string()
        assert stream_hash in pipeline._source_streams

    def test_invocation_lut_additive_across_with_blocks(self):
        """Opening a second with-block appends to _invocation_lut, not replaces."""
        # pod1 consumes 'value' and produces 'result'; pod2 consumes 'result'
        def fn1(value: int) -> int:
            return value * 2
        fn1.__name__ = "fn1"
        pod1 = FunctionPod(PythonDataFunction(fn1, output_keys="result"))

        def fn2(result: int) -> int:
            return result * 3
        fn2.__name__ = "fn2"
        pod2 = FunctionPod(PythonDataFunction(fn2, output_keys="result2"))

        s = _src("key", "value")
        pipeline = Pipeline(name="test", auto_compile=False)
        with pipeline:
            out1 = pod1(s, label="step1")
        with pipeline:
            pod2(out1, label="step2")
        assert len(pipeline._invocation_lut) == 2

    def test_compile_rebuilds_persistent_node_map_from_scratch(self):
        """compile() must produce correct _persistent_node_map from _invocation_lut."""
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(stream, label="out")
        assert "out" in pipeline._nodes
        assert len(pipeline._persistent_node_map) == 2  # 1 source + 1 function node

    def test_compile_creates_source_node_for_unregistered_upstream(self):
        """Streams with no recorded invocation must become SourceNode at compile time."""
        from orcapod.core.nodes.source_node import SourceNode
        pod = _fn()
        stream = _src("key", "value")
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(stream, label="out")
        source_hash = stream.content_hash().to_string()
        assert isinstance(pipeline._persistent_node_map[source_hash], SourceNode)


class TestSourceNodeDisambiguation:
    """Source-node slot names must be unique after compile().

    When multiple concrete sources share the same label (e.g. two unlabelled
    ArrowTableSource inputs both default to "ArrowTableSource"), compile()
    renames them "ArrowTableSource_1", "ArrowTableSource_2", …  The suffix
    counter skips any numbers already taken by explicitly-named nodes, so the
    result is never "ArrowTableSource_1_1".
    """

    def test_two_unnamed_sources_get_distinct_suffixed_names(self):
        """Two unlabelled concrete sources of the same type are disambiguated."""
        from orcapod.core.nodes.source_node import SourceNode

        s1 = _src("key", "value")   # label → "ArrowTableSource"
        s2 = _src("key", "score")   # label → "ArrowTableSource"
        join = Join()
        pipeline = Pipeline(name="test")
        with pipeline:
            join(s1, s2, label="joined")

        source_nodes = [
            n for n in pipeline._persistent_node_map.values()
            if isinstance(n, SourceNode)
        ]
        names = sorted(n.name for n in source_nodes)
        assert names == ["ArrowTableSource_1", "ArrowTableSource_2"]

    def test_three_unnamed_sources_get_sequential_suffixes(self):
        """Three unnamed sources of the same type receive _1, _2, _3."""
        from orcapod.core.nodes.source_node import SourceNode
        from orcapod.core.operators import Join

        s1 = _src("key", "v1")
        s2 = _src("key", "v2")
        s3 = _src("key", "v3")
        join = Join()
        pipeline = Pipeline(name="test")
        with pipeline:
            join(s1, s2, s3, label="joined")

        names = sorted(
            n.name
            for n in pipeline._persistent_node_map.values()
            if isinstance(n, SourceNode)
        )
        assert names == ["ArrowTableSource_1", "ArrowTableSource_2", "ArrowTableSource_3"]

    def test_suffix_skips_already_taken_numbers(self):
        """If ArrowTableSource_1 is already a slot name, colliding sources start at _2."""
        from orcapod.core.nodes.source_node import SourceNode

        # One explicitly-named source occupies "ArrowTableSource_1".
        s_named = _src("key", "named_val")
        s_named.label = "ArrowTableSource_1"   # explicit label

        # Two unlabelled sources would normally want _1 and _2.
        s1 = _src("key", "v1")   # label → "ArrowTableSource"
        s2 = _src("key", "v2")   # label → "ArrowTableSource"
        join = Join()
        pipeline = Pipeline(name="test")
        with pipeline:
            join(s_named, s1, s2, label="joined")

        names = sorted(
            n.name
            for n in pipeline._persistent_node_map.values()
            if isinstance(n, SourceNode)
        )
        # s_named keeps "ArrowTableSource_1"; the two unnamed ones get _2 and _3.
        assert "ArrowTableSource_1" in names
        assert "ArrowTableSource_2" in names
        assert "ArrowTableSource_3" in names


class TestRecordOperatorPodInvocation:
    def test_record_operator_pod_adds_to_invocation_lut(self):
        """After recording an operator, _invocation_lut must contain the invocation."""
        join = Join()
        s1 = _src("key", "value")
        s2 = _src("key", "score")
        pipeline = Pipeline(name="test", auto_compile=False)
        with pipeline:
            join(s1, s2, label="joined")
        assert len(pipeline._invocation_lut) == 1

    def test_record_operator_captures_both_source_streams(self):
        join = Join()
        s1 = _src("key", "value")
        s2 = _src("key", "score")
        pipeline = Pipeline(name="test", auto_compile=False)
        with pipeline:
            join(s1, s2, label="joined")
        h1 = s1.content_hash().to_string()
        h2 = s2.content_hash().to_string()
        assert h1 in pipeline._source_streams
        assert h2 in pipeline._source_streams
