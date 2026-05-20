# tests/test_pipeline/test_pipeline_job.py
from __future__ import annotations

import json
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
        src_a.label = "source_a"
        src_b.label = "source_b"

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
            Join()(src_a, src_b)  # both auto-bound via content-hash-based spec names

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


# ---------------------------------------------------------------------------
# Tests: is_runnable and __repr__
# ---------------------------------------------------------------------------


class TestPipelineJobIsRunnable:
    def test_is_runnable_true_when_all_upstreams_bound(self, store):
        """is_runnable returns True when all upstream SourceSpecs are bound."""
        src_a, src_b = _make_two_sources()

        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b, label="joiner")

        assert job.is_runnable("joiner")

    def test_is_runnable_false_when_spec_unbound(self, store):
        """is_runnable returns False when an upstream SourceSpec is unbound."""
        src_a, src_b = _make_two_sources()
        tag_b, data_b = src_b.output_schema()
        spec_b = SourceSpec("spec_b", tag_schema=tag_b, data_schema=data_b)

        job = PipelineJob(store=store)
        with job:
            Join()(src_a, spec_b, label="joiner")

        assert not job.is_runnable("joiner")

    def test_is_runnable_false_for_unknown_label(self, store):
        """is_runnable returns False for a node label that doesn't exist."""
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b)

        assert not job.is_runnable("nonexistent")

    def test_repr_includes_class_name(self, store):
        """repr() includes PipelineJob class name."""
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b)

        r = repr(job)
        assert "PipelineJob" in r


# ---------------------------------------------------------------------------
# Tests: PipelineJob.run()
# ---------------------------------------------------------------------------


class TestPipelineJobRun:
    def test_run_executes_all_nodes(self, store):
        """run() executes all nodes when all specs are bound."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)

        job = PipelineJob(store=store)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        job.run()

        node = job.pipeline.compiled_nodes["adder"]
        records = node.get_all_records()
        assert records is not None
        assert records.num_rows == 2

    def test_run_produces_correct_values(self, store):
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)

        job = PipelineJob(store=store)
        with job:
            joined = Join()(src_a, src_b, label="joiner")
            pod(joined, label="adder")

        job.run()

        table = job.pipeline.compiled_nodes["adder"].as_table()
        totals = sorted(cast(list[int], table.column("total").to_pylist()))
        assert totals == [110, 220]  # a: 10+100, b: 20+200

    def test_run_partial_execution_skips_unbound_subgraph(self, store):
        """Nodes with unbound upstream SourceSpecs are excluded from execution."""
        src_a, src_b = _make_two_sources()
        tag_b, data_b = src_b.output_schema()
        spec_b = SourceSpec("spec_b", tag_schema=tag_b, data_schema=data_b)
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)

        job = PipelineJob(store=store)
        with job:
            joined = Join()(src_a, spec_b)
            pod(joined, label="adder")

        result = job.run()
        # Unresolved specs should be reported
        assert "spec_b" in result.unresolved_specs

    def test_run_is_non_mutating(self, store):
        """run() returns a new PipelineJob; original job object is not the same."""
        src_a, src_b = _make_two_sources()

        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b, label="joiner")

        result = job.run()
        assert result is not job
        # The returned job shares the same pipeline (compilation is reused)
        assert result.pipeline is job.pipeline

    def test_run_requires_store(self):
        """run() without a store raises ValueError."""
        src_a, src_b = _make_two_sources()

        job = PipelineJob()  # no store
        with job:
            Join()(src_a, src_b)

        with pytest.raises(ValueError, match="store"):
            job.run()

    def test_run_twice_is_safe(self, store):
        """Calling run() twice on the same job does not raise."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)

        job = PipelineJob(store=store)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        job.run()
        job.run()  # second run should not raise

        table = job.pipeline.compiled_nodes["adder"].as_table()
        totals = sorted(cast(list[int], table.column("total").to_pylist()))
        assert totals == [110, 220]


class TestPipelineJobEndToEnd:
    def test_end_to_end_source_join_function(self, store):
        """Two sources → Join → FunctionPod all execute correctly."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)

        job = PipelineJob(store=store)
        with job:
            joined = Join()(src_a, src_b, label="joiner")
            pod(joined, label="adder")

        assert isinstance(job.pipeline.compiled_nodes["joiner"], OperatorNode)
        assert isinstance(job.pipeline.compiled_nodes["adder"], FunctionNode)

        job.run()

        fn_records = job.pipeline.compiled_nodes["adder"].get_all_records()
        assert fn_records is not None
        assert fn_records.num_rows == 2

        table = job.pipeline.compiled_nodes["adder"].as_table()
        totals = sorted(cast(list[int], table.column("total").to_pylist()))
        assert totals == [110, 220]

    def test_bind_then_run(self, store):
        """Pipeline.bind() + job.run() produces correct results."""
        from orcapod.pipeline.graph import Pipeline

        src_a, src_b = _make_two_sources()
        tag_a, data_a = src_a.output_schema()
        tag_b, data_b = src_b.output_schema()
        spec_a = SourceSpec("src_a", tag_schema=tag_a, data_schema=data_a)
        spec_b = SourceSpec("src_b", tag_schema=tag_b, data_schema=data_b)
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)

        pipeline = Pipeline(name="bp")
        with pipeline:
            joined = Join()(spec_a, spec_b)
            pod(joined, label="adder")

        job = pipeline.bind(
            sources={"src_a": src_a, "src_b": src_b},
            store=store,
        )
        job.run()

        table = job.pipeline.compiled_nodes["adder"].as_table()
        totals = sorted(cast(list[int], table.column("total").to_pylist()))
        assert totals == [110, 220]


class TestPipelineJobSerialization:
    def test_save_creates_file(self, store, tmp_path):
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b)

        path = tmp_path / "job.json"
        job.save(str(path))
        assert path.exists()

    def test_save_has_version(self, store, tmp_path):
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b)

        path = tmp_path / "job.json"
        job.save(str(path))
        data = json.loads(path.read_text())
        assert data["orcapod_pipeline_job_version"] == "0.1.0"

    def test_save_includes_run_block(self, store, tmp_path):
        """Unsaved template has status=pending and null run fields."""
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b)

        path = tmp_path / "job.json"
        job.save(str(path))
        data = json.loads(path.read_text())
        assert data["run"]["status"] == "pending"
        assert data["run"]["run_id"] is None

    def test_load_roundtrip_restores_pipeline(self, store, tmp_path):
        """load() restores the pipeline topology."""
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b, label="joiner")

        path = tmp_path / "job.json"
        job.save(str(path))
        loaded = PipelineJob.load(str(path))

        assert "joiner" in loaded.pipeline.compiled_nodes

    def test_load_roundtrip_after_run(self, store, tmp_path):
        """Save after run() → load → pipeline topology and run status preserved."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)

        job = PipelineJob(store=store)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        completed = job.run()
        path = tmp_path / "completed.json"
        completed.save(str(path))

        # Verify the file has correct status
        data = json.loads(path.read_text())
        assert data["run"]["status"] == "complete"

        # Load and verify topology
        loaded = PipelineJob.load(str(path))
        assert "adder" in loaded.pipeline.compiled_nodes

    def test_load_version_mismatch_raises(self, store, tmp_path):
        """PipelineJob.load() raises ValueError for an unsupported format version."""
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b)
        path = tmp_path / "job_bad.json"
        job.save(str(path))
        data = json.loads(path.read_text())
        data["orcapod_pipeline_job_version"] = "99.0.0"
        path.write_text(json.dumps(data))
        with pytest.raises(ValueError, match="version"):
            PipelineJob.load(str(path))

    def test_load_before_run_has_run_false(self, store, tmp_path):
        """Loaded job saved before run() has _has_run=False."""
        src_a, src_b = _make_two_sources()
        job = PipelineJob(store=store)
        with job:
            Join()(src_a, src_b)
        path = tmp_path / "pending.json"
        job.save(str(path))
        loaded = PipelineJob.load(str(path), store=store)
        assert loaded._has_run is False

    def test_load_after_run_restores_has_run_true(self, store, tmp_path):
        """Loaded job saved after run() has _has_run=True."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)
        job = PipelineJob(store=store)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")
        completed = job.run()
        path = tmp_path / "completed.json"
        completed.save(str(path))
        loaded = PipelineJob.load(str(path), store=store)
        assert loaded._has_run is True

    def test_load_after_partial_run_restores_unresolved_specs(self, store, tmp_path):
        """Loaded job preserves unresolved_specs from a partial run."""
        src_a, src_b = _make_two_sources()
        tag_b, data_b = src_b.output_schema()
        spec_b = SourceSpec("unbound_b", tag_schema=tag_b, data_schema=data_b)
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)
        job = PipelineJob(store=store)
        with job:
            joined = Join()(src_a, spec_b)
            pod(joined, label="adder")
        result = job.run()
        assert "unbound_b" in result.unresolved_specs
        path = tmp_path / "partial.json"
        result.save(str(path))
        loaded = PipelineJob.load(str(path), store=store)
        assert "unbound_b" in loaded.unresolved_specs
