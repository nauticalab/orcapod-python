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


class TestSourceNodeNaming:
    """Source node names are derived from ``RootSource.source_id``, not the label.

    The ``source_id`` is the canonical identity of a source: file-backed sources
    use the file path or table name; in-memory sources use a hash of their data.
    This guarantees uniqueness by construction — no disambiguation loop is needed.

    Two nodes that share a name but have different schemas indicate a caller
    error (same ``source_id`` assigned to conceptually different inputs) and
    cause ``compile()`` to raise ``InconsistentSourceError``.
    """

    def test_root_source_name_derived_from_source_id(self):
        """A RootSource with an explicit source_id produces a SN with that name."""
        from orcapod.core.nodes.source_node import SourceNode

        source = ArrowTableSource(
            pa.table({
                "key": pa.array(["a", "b"], type=pa.large_string()),
                "value": pa.array([1, 2], type=pa.int64()),
            }),
            tag_columns=["key"],
            source_id="my_dataset",
            infer_nullable=True,
        )
        pod = _fn()
        pipeline = Pipeline(name="test")
        with pipeline:
            pod(source, label="out")

        source_nodes = [
            n for n in pipeline._persistent_node_map.values()
            if isinstance(n, SourceNode)
        ]
        assert len(source_nodes) == 1
        assert source_nodes[0].name == "my_dataset"

    def test_two_different_sources_get_distinct_source_id_names(self):
        """Two ArrowTableSources with different data get distinct hash-based names."""
        from orcapod.core.nodes.source_node import SourceNode

        s1 = _src("key", "value")   # different data column → different source_id
        s2 = _src("key", "score")
        join = Join()
        pipeline = Pipeline(name="test")
        with pipeline:
            join(s1, s2, label="joined")

        source_nodes = [
            n for n in pipeline._persistent_node_map.values()
            if isinstance(n, SourceNode)
        ]
        names = [n.name for n in source_nodes]
        assert len(names) == 2
        # Names are distinct (each is the source's own source_id hash)
        assert names[0] != names[1]
        # Names are not the fallback class name
        assert all("ArrowTableSource" not in nm for nm in names)

    def test_explicit_source_id_overrides_hash_name(self):
        """An ArrowTableSource with an explicit source_id uses that as the SN name."""
        from orcapod.core.nodes.source_node import SourceNode

        s1 = ArrowTableSource(
            pa.table({
                "key": pa.array(["a"], type=pa.large_string()),
                "v1": pa.array([1], type=pa.int64()),
            }),
            tag_columns=["key"],
            source_id="left_input",
            infer_nullable=True,
        )
        s2 = ArrowTableSource(
            pa.table({
                "key": pa.array(["a"], type=pa.large_string()),
                "v2": pa.array([2], type=pa.int64()),
            }),
            tag_columns=["key"],
            source_id="right_input",
            infer_nullable=True,
        )
        join = Join()
        pipeline = Pipeline(name="test")
        with pipeline:
            join(s1, s2, label="joined")

        names = sorted(
            n.name
            for n in pipeline._persistent_node_map.values()
            if isinstance(n, SourceNode)
        )
        assert names == ["left_input", "right_input"]

    def test_same_source_id_different_schema_raises_inconsistent_source_error(self):
        """Two sources with the same source_id but different schemas raise an error."""
        from orcapod.errors import InconsistentSourceError

        s1 = ArrowTableSource(
            pa.table({
                "key": pa.array(["a"], type=pa.large_string()),
                "value": pa.array([1], type=pa.int64()),
            }),
            tag_columns=["key"],
            source_id="shared_id",
            infer_nullable=True,
        )
        s2 = ArrowTableSource(
            pa.table({
                "key": pa.array(["a"], type=pa.large_string()),
                "score": pa.array([1.0], type=pa.float64()),  # different data column
            }),
            tag_columns=["key"],
            source_id="shared_id",
            infer_nullable=True,
        )
        join = Join()
        pipeline = Pipeline(name="test", auto_compile=False)
        with pipeline:
            join(s1, s2, label="joined")

        with pytest.raises(InconsistentSourceError, match="shared_id"):
            pipeline.compile()


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
