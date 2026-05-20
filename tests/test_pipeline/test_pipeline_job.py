# tests/test_pipeline/test_pipeline_job.py
from __future__ import annotations

from typing import cast

import pyarrow as pa
import pytest

from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode, OperatorNode, SourceNode
from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.core.sources.source_spec import SourceSpec
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import SourceSpecMismatchError
from orcapod.pipeline.job import PipelineJob


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_source(tag_col: str, data_col: str, data: dict) -> ArrowTableSource:
    table = pa.table(
        {
            tag_col: pa.array(data[tag_col], type=pa.large_string()),
            data_col: pa.array(data[data_col], type=pa.int64()),
        }
    )
    return ArrowTableSource(table, tag_columns=[tag_col], infer_nullable=True)


def _make_two_sources():
    src_a = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
    src_b = _make_source("key", "score", {"key": ["a", "b"], "score": [100, 200]})
    return src_a, src_b


def add_values(value: int, score: int) -> int:
    return value + score


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def store():
    return InMemoryArrowDatabase()


# ---------------------------------------------------------------------------
# Tests: with-block recording
# ---------------------------------------------------------------------------


class TestPipelineJobRecording:
    def test_with_concrete_sources_auto_creates_specs(self, store):
        """Concrete sources in with-block become SourceSpecs in job.pipeline."""
        src_a, src_b = _make_two_sources()

        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b)

        # Pipeline should have SourceSpec leaf nodes
        source_nodes = [
            n for n in job.pipeline._node_graph.nodes() if isinstance(n, SourceNode)
        ]
        assert all(isinstance(n.stream, SourceSpec) for n in source_nodes)

    def test_concrete_source_stored_in_sources(self, store):
        """Concrete sources from with-block are stored by label in job.sources."""
        src_a, src_b = _make_two_sources()
        src_a._label = "source_a"
        src_b._label = "source_b"

        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b)

        assert "source_a" in job.sources
        assert "source_b" in job.sources
        assert job.sources["source_a"] is src_a
        assert job.sources["source_b"] is src_b

    def test_spec_leaf_not_added_to_sources(self, store):
        """SourceSpec leaves are NOT added to job.sources (they're unbound)."""
        src_a, _ = _make_two_sources()
        tag_b, data_b = _make_source("key", "score", {"key": ["a"], "score": [1]}).output_schema()
        spec_b = SourceSpec("spec_b", tag_schema=tag_b, data_schema=data_b)

        job = PipelineJob(store=store)
        with job:
            Join()(src_a, spec_b)

        assert "spec_b" not in job.sources

    def test_pipeline_extracted_after_with_block(self, store):
        """job.pipeline is a compiled Pipeline after the with block."""
        from orcapod.pipeline.graph import Pipeline

        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b)

        assert isinstance(job.pipeline, Pipeline)
        assert job.pipeline._compiled


# ---------------------------------------------------------------------------
# Tests: bind()
# ---------------------------------------------------------------------------


class TestPipelineJobBind:
    def test_bind_sources_returns_new_job(self, store):
        """bind(sources=...) returns a new PipelineJob; original is unchanged."""
        src_a, src_b = _make_two_sources()
        tag_a, data_a = src_a.output_schema()
        tag_b, data_b = src_b.output_schema()
        spec_a = SourceSpec("a", tag_schema=tag_a, data_schema=data_a)
        spec_b = SourceSpec("b", tag_schema=tag_b, data_schema=data_b)

        job = PipelineJob(store=store)
        with job:
            Join()(spec_a, spec_b)

        job2 = job.bind(sources={"a": src_a, "b": src_b})
        assert job2 is not job
        assert job.sources == {}  # original unchanged
        assert "a" in job2.sources and "b" in job2.sources

    def test_bind_store_returns_new_job(self, store):
        src_a, src_b = _make_two_sources()
        job = PipelineJob()
        with job:
            Join()(src_a, src_b)

        new_store = InMemoryArrowDatabase()
        job2 = job.bind(store=new_store)
        assert job2.store is new_store
        assert job.store is None  # original unchanged

    def test_bind_preserves_existing_sources(self, store):
        """bind(sources=...) merges new sources with existing ones."""
        src_a, src_b = _make_two_sources()
        tag_a, data_a = src_a.output_schema()
        tag_b, data_b = src_b.output_schema()
        spec_a = SourceSpec("a", tag_schema=tag_a, data_schema=data_a)
        spec_b = SourceSpec("b", tag_schema=tag_b, data_schema=data_b)

        job = PipelineJob(store=store)
        with job:
            Join()(spec_a, spec_b)

        job2 = job.bind(sources={"a": src_a})
        job3 = job2.bind(sources={"b": src_b})

        assert "a" in job3.sources
        assert "b" in job3.sources

    def test_bind_validates_schema_at_bind_time(self, store):
        """bind() raises SourceSpecMismatchError for incompatible sources."""
        src_a, src_b = _make_two_sources()
        tag_a, data_a = src_a.output_schema()
        # Create a spec that requires an extra column the source doesn't have
        from orcapod.types import Schema
        wrong_spec = SourceSpec("a", tag_schema=tag_a, data_schema=Schema({"value": int, "extra": str}))

        job = PipelineJob(store=store)
        with job:
            Join()(wrong_spec, src_b)

        with pytest.raises(SourceSpecMismatchError):
            job.bind(sources={"a": src_a})

    def test_pipeline_bind_wraps_in_job(self, store):
        """Pipeline.bind() returns a PipelineJob holding that pipeline."""
        from orcapod.pipeline.graph import Pipeline

        src_a, src_b = _make_two_sources()
        tag_a, data_a = src_a.output_schema()
        tag_b, data_b = src_b.output_schema()
        spec_a = SourceSpec("a", tag_schema=tag_a, data_schema=data_a)
        spec_b = SourceSpec("b", tag_schema=tag_b, data_schema=data_b)

        pipeline = Pipeline(name="p")
        with pipeline:
            Join()(spec_a, spec_b)

        job = pipeline.bind(sources={"a": src_a, "b": src_b}, store=store)
        assert isinstance(job, PipelineJob)
        assert job.pipeline is pipeline


# ---------------------------------------------------------------------------
# Tests: completeness
# ---------------------------------------------------------------------------


class TestPipelineJobCompleteness:
    def test_unbound_specs_lists_unbound(self, store):
        """unbound_specs() lists SourceSpec names not in job.sources."""
        src_a, src_b = _make_two_sources()
        tag_b, data_b = src_b.output_schema()
        spec_b = SourceSpec("spec_b", tag_schema=tag_b, data_schema=data_b)

        job = PipelineJob(store=store)
        with job:
            Join()(src_a, spec_b)

        unbound = job.unbound_specs()
        assert len(unbound) == 1
        assert unbound[0].name == "spec_b"

    def test_unbound_specs_empty_when_all_bound(self, store):
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b)  # both auto-bound from labels

        assert job.unbound_specs() == []

    def test_is_complete_true_when_all_bound_with_store(self, store):
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b)

        assert job.is_complete()

    def test_is_complete_false_when_store_missing(self):
        src_a, src_b = _make_two_sources()
        job = PipelineJob()  # no store
        with job:
            Join()(src_a, src_b)

        assert not job.is_complete()

    def test_is_complete_false_when_specs_unbound(self, store):
        src_a, src_b = _make_two_sources()
        tag_b, data_b = src_b.output_schema()
        spec_b = SourceSpec("spec_b", tag_schema=tag_b, data_schema=data_b)

        job = PipelineJob(store=store)
        with job:
            Join()(src_a, spec_b)

        assert not job.is_complete()
