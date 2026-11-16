# tests/test_pipeline/test_pipeline_job.py
from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import pyarrow as pa
import pytest

import orcapod
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode, OperatorNode
from orcapod.core.nodes.source_node import SourceNode
from orcapod.core.operators import Join
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import SourceSpecMismatchError
from orcapod.pipeline.job import PipelineJob
from orcapod.types import Schema

_FIXTURES = Path(__file__).parent / "fixtures"


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
    return ArrowTableSource(
        table, tag_columns=[tag_col], tag_only_columns=True, infer_nullable=True
    )


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
        """Concrete sources in with-block become SourceNodes in job.pipeline."""
        src_a, src_b = _make_two_sources()

        job = PipelineJob(database=store)
        with job:
            Join()(src_a, src_b)

        # Pipeline should have SourceNode leaf nodes
        source_nodes = [
            n for n in job.pipeline.dag.nodes() if isinstance(n, SourceNode)
        ]
        assert len(source_nodes) == 2
        assert all(isinstance(n, SourceNode) for n in source_nodes)

    def test_concrete_source_stored_in_sources(self, store):
        """Concrete sources from with-block are stored by source_id in job.sources."""
        src_a = ArrowTableSource(
            pa.table(
                {
                    "key": pa.array(["a", "b"], type=pa.large_string()),
                    "value": pa.array([10, 20], type=pa.int64()),
                }
            ),
            tag_columns=["key"],
            tag_only_columns=True,
            source_id="source_a",
            infer_nullable=True,
        )
        src_b = ArrowTableSource(
            pa.table(
                {
                    "key": pa.array(["a", "b"], type=pa.large_string()),
                    "score": pa.array([100, 200], type=pa.int64()),
                }
            ),
            tag_columns=["key"],
            tag_only_columns=True,
            source_id="source_b",
            infer_nullable=True,
        )

        job = PipelineJob(database=store)
        with job:
            Join()(src_a, src_b)

        assert "source_a" in job.sources
        assert "source_b" in job.sources
        assert job.sources["source_a"] is src_a
        assert job.sources["source_b"] is src_b

    def test_spec_leaf_not_added_to_sources(self, store):
        """SourceNode leaves are NOT added to job.sources (they're unbound)."""
        src_a, _ = _make_two_sources()
        tag_b, data_b = _make_source(
            "key", "score", {"key": ["a"], "score": [1]}
        ).output_schema()
        node_b = SourceNode(name="spec_b", tag_schema=tag_b, data_schema=data_b)

        job = PipelineJob(database=store)
        with job:
            Join()(src_a, node_b)

        assert "spec_b" not in job.sources

    def test_pipeline_extracted_after_with_block(self, store):
        """job.pipeline is a compiled Pipeline after the with block."""
        from orcapod.pipeline.graph import Pipeline

        src_a, src_b = _make_two_sources()
        job = PipelineJob(database=store)
        with job:
            Join()(src_a, src_b)

        assert isinstance(job.pipeline, Pipeline)
        assert job.pipeline._compiled


# ---------------------------------------------------------------------------
# Tests: bind()
# ---------------------------------------------------------------------------


class TestPipelineJobBind:
    def test_bind_sources_mutates_job(self, store):
        """bind(sources=...) mutates the job in place and returns None."""
        src_a, src_b = _make_two_sources()
        tag_a, data_a = src_a.output_schema()
        tag_b, data_b = src_b.output_schema()
        node_a = SourceNode(name="a", tag_schema=tag_a, data_schema=data_a)
        node_b = SourceNode(name="b", tag_schema=tag_b, data_schema=data_b)

        job = PipelineJob(database=store)
        with job:
            Join()(node_a, node_b)

        result = job.bind(sources={"a": src_a, "b": src_b})
        assert result is None  # bind() is now mutating and returns None
        assert "a" in job.sources and "b" in job.sources

    def test_bind_store_mutates_job(self, store):
        """bind(database=...) updates the database in place and returns None."""
        src_a, src_b = _make_two_sources()
        job = PipelineJob()
        with job:
            Join()(src_a, src_b)

        new_store = InMemoryArrowDatabase()
        result = job.bind(database=new_store)
        assert result is None  # bind() returns None
        assert job.database is new_store

    def test_bind_preserves_existing_sources(self, store):
        """bind(sources=...) merges new sources with existing ones."""
        src_a, src_b = _make_two_sources()
        tag_a, data_a = src_a.output_schema()
        tag_b, data_b = src_b.output_schema()
        node_a = SourceNode(name="a", tag_schema=tag_a, data_schema=data_a)
        node_b = SourceNode(name="b", tag_schema=tag_b, data_schema=data_b)

        job = PipelineJob(database=store)
        with job:
            Join()(node_a, node_b)

        job.bind(sources={"a": src_a})
        job.bind(sources={"b": src_b})

        assert "a" in job.sources
        assert "b" in job.sources

    def test_bind_validates_schema_at_bind_time(self, store):
        """bind() raises SourceSpecMismatchError for incompatible sources."""
        src_a, src_b = _make_two_sources()
        tag_a, data_a = src_a.output_schema()
        # Create a node that requires an extra column the source doesn't have
        wrong_node = SourceNode(
            name="a", tag_schema=tag_a, data_schema=Schema({"value": int, "extra": str})
        )

        job = PipelineJob(database=store)
        with job:
            Join()(wrong_node, src_b)

        with pytest.raises(SourceSpecMismatchError):
            job.bind(sources={"a": src_a})

    def test_from_pipeline_wraps_in_job(self, store):
        """PipelineJob.from_pipeline() returns a PipelineJob holding that pipeline."""
        from orcapod.pipeline.graph import Pipeline

        src_a, src_b = _make_two_sources()
        tag_a, data_a = src_a.output_schema()
        tag_b, data_b = src_b.output_schema()
        node_a = SourceNode(name="a", tag_schema=tag_a, data_schema=data_a)
        node_b = SourceNode(name="b", tag_schema=tag_b, data_schema=data_b)

        pipeline = Pipeline(name="p")
        with pipeline:
            Join()(node_a, node_b)

        job = PipelineJob.from_pipeline(
            pipeline, sources={"a": src_a, "b": src_b}, database=store
        )
        assert isinstance(job, PipelineJob)
        # compiled_pipeline is computed lazily (schema-normalised) — not the
        # original object, but it must be a fully compiled Pipeline.
        assert job.compiled_pipeline is not None
        assert job.compiled_pipeline._compiled

    def test_from_pipeline_propagates_name(self, store):
        """PipelineJob.from_pipeline() must propagate the pipeline's name to the job."""
        from orcapod.pipeline.graph import Pipeline

        src_a, src_b = _make_two_sources()
        tag_a, data_a = src_a.output_schema()
        tag_b, data_b = src_b.output_schema()
        node_a = SourceNode(name="a", tag_schema=tag_a, data_schema=data_a)
        node_b = SourceNode(name="b", tag_schema=tag_b, data_schema=data_b)

        pipeline = Pipeline(name="my_pipeline")
        with pipeline:
            Join()(node_a, node_b)

        job = PipelineJob.from_pipeline(
            pipeline, sources={"a": src_a, "b": src_b}, database=store
        )
        assert job._name == ("my_pipeline",), (
            "PipelineJob._name should match Pipeline.name after from_pipeline()"
        )


# ---------------------------------------------------------------------------
# Tests: completeness
# ---------------------------------------------------------------------------


class TestPipelineJobCompleteness:
    def test_unbound_specs_lists_unbound(self, store):
        """unbound_sources lists names of unbound SourceJobNode slots."""
        src_a, src_b = _make_two_sources()
        tag_b, data_b = src_b.output_schema()
        node_b = SourceNode(name="spec_b", tag_schema=tag_b, data_schema=data_b)

        job = PipelineJob(database=store)
        with job:
            Join()(src_a, node_b)

        assert job.unbound_sources == ["spec_b"]

    def test_unbound_specs_empty_when_all_bound(self, store):
        src_a, src_b = _make_two_sources()
        job = PipelineJob(database=store)
        with job:
            Join()(src_a, src_b)  # both auto-bound via content-hash-based spec names

        assert job.unbound_sources == []

    def test_is_complete_true_when_all_bound_with_store(self, store):
        src_a, src_b = _make_two_sources()
        job = PipelineJob(database=store)
        with job:
            Join()(src_a, src_b)

        assert job.is_complete()

    def test_is_complete_false_when_store_missing(self):
        src_a, src_b = _make_two_sources()
        job = PipelineJob()  # no database
        with job:
            Join()(src_a, src_b)

        assert not job.is_complete()

    def test_is_complete_false_when_specs_unbound(self, store):
        src_a, src_b = _make_two_sources()
        tag_b, data_b = src_b.output_schema()
        node_b = SourceNode(name="spec_b", tag_schema=tag_b, data_schema=data_b)

        job = PipelineJob(database=store)
        with job:
            Join()(src_a, node_b)

        assert not job.is_complete()


# ---------------------------------------------------------------------------
# Tests: unbound_sources
# ---------------------------------------------------------------------------


class TestUnboundSources:
    def test_unbound_sources_returns_names_of_unbound_source_nodes(self, store):
        """unbound_sources lists the name of each unbound SourceJobNode slot."""
        src_a, src_b = _make_two_sources()
        tag_b, data_b = src_b.output_schema()
        node_b = SourceNode(name="spec_b", tag_schema=tag_b, data_schema=data_b)

        job = PipelineJob(database=store)
        with job:
            Join()(src_a, node_b)

        assert job.unbound_sources == ["spec_b"]

    def test_unbound_sources_empty_when_all_bound(self, store):
        """unbound_sources is empty when all sources are bound."""
        src_a, src_b = _make_two_sources()
        job = PipelineJob(database=store)
        with job:
            Join()(src_a, src_b)

        assert job.unbound_sources == []

    def test_unbound_sources_empty_before_compile(self):
        """unbound_sources returns [] when job is not yet compiled."""
        job = PipelineJob()
        assert job.unbound_sources == []

    def test_unbound_sources_reflects_bind_call(self, store):
        """After binding a source, it no longer appears in unbound_sources."""
        src_a, src_b = _make_two_sources()
        tag_b, data_b = src_b.output_schema()
        node_b = SourceNode(name="spec_b", tag_schema=tag_b, data_schema=data_b)

        job = PipelineJob(database=store)
        with job:
            Join()(src_a, node_b)

        assert "spec_b" in job.unbound_sources
        job.bind(sources={"spec_b": src_b})
        assert job.unbound_sources == []


# ---------------------------------------------------------------------------
# Tests: is_runnable and __repr__
# ---------------------------------------------------------------------------


class TestPipelineJobIsRunnable:
    def test_is_runnable_true_when_all_upstreams_bound(self, store):
        """is_runnable returns True when all upstream SourceSpecs are bound."""
        src_a, src_b = _make_two_sources()

        job = PipelineJob(database=store)
        with job:
            Join()(src_a, src_b, label="joiner")

        assert job.is_runnable("joiner")

    def test_is_runnable_false_when_spec_unbound(self, store):
        """is_runnable returns False when an upstream SourceNode is unbound."""
        src_a, src_b = _make_two_sources()
        tag_b, data_b = src_b.output_schema()
        node_b = SourceNode(name="spec_b", tag_schema=tag_b, data_schema=data_b)

        job = PipelineJob(database=store)
        with job:
            Join()(src_a, node_b, label="joiner")

        assert not job.is_runnable("joiner")

    def test_is_runnable_false_for_unknown_label(self, store):
        """is_runnable returns False for a node label that doesn't exist."""
        src_a, src_b = _make_two_sources()
        job = PipelineJob(database=store)
        with job:
            Join()(src_a, src_b)

        assert not job.is_runnable("nonexistent")

    def test_repr_includes_class_name(self, store):
        """repr() includes PipelineJob class name."""
        src_a, src_b = _make_two_sources()
        job = PipelineJob(database=store)
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

        job = PipelineJob(database=store)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        _ = job.run()

        node = job.nodes["adder"]
        records = node.get_all_records()
        assert records is not None
        assert records.num_rows == 2

    def test_run_produces_correct_values(self, store):
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)

        job = PipelineJob(database=store)
        with job:
            joined = Join()(src_a, src_b, label="joiner")
            pod(joined, label="adder")

        _ = job.run()

        table = job.nodes["adder"].as_table()
        totals = sorted(cast(list[int], table.column("total").to_pylist()))
        assert totals == [110, 220]  # a: 10+100, b: 20+200

    def test_run_partial_execution_skips_unbound_subgraph(self, store):
        """Nodes with unbound upstream SourceNodes are excluded from execution."""
        src_a, src_b = _make_two_sources()
        tag_b, data_b = src_b.output_schema()
        node_b = SourceNode(name="spec_b", tag_schema=tag_b, data_schema=data_b)
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)

        job = PipelineJob(database=store)
        with job:
            joined = Join()(src_a, node_b)
            pod(joined, label="adder")

        _ = job.run()
        # Unbound sources should be reported
        assert "spec_b" in job.unbound_sources

    def test_run_returns_its_summary_and_mutates_in_place(self, store):
        """run() returns the run's summary, kept as last_run, and sets _has_run / _run_id."""
        from orcapod.pipeline import RunSummary

        src_a, src_b = _make_two_sources()

        job = PipelineJob(database=store)
        with job:
            Join()(src_a, src_b, label="joiner")

        assert job.last_run is None
        summary = job.run()
        assert isinstance(summary, RunSummary)
        assert job.last_run is summary
        assert job._has_run is True
        assert job._run_id is not None

    def test_run_does_not_mutate_blueprint_nodes(self, store):
        """build_execution_graph() must not mutate the original pipeline's _nodes.

        The pipeline blueprint is a shared, reusable object. Running one job must
        not replace blueprint template nodes with live exec nodes — that would break
        subsequent jobs (and ``PipelineJob.from_pipeline()`` callers) that share the
        same pipeline.
        """
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)

        job = PipelineJob(database=store)
        with job:
            joined = Join()(src_a, src_b, label="joiner")
            pod(joined, label="adder")

        blueprint = job.pipeline
        # Capture the identity of each template node before running
        blueprint_node_ids_before = {
            label: id(node) for label, node in blueprint._nodes.items()
        }

        job.run()

        # Blueprint _nodes must still point to the exact same objects
        blueprint_node_ids_after = {
            label: id(node) for label, node in blueprint._nodes.items()
        }
        assert blueprint_node_ids_before == blueprint_node_ids_after, (
            "run() mutated blueprint._nodes — exec nodes replaced template nodes"
        )

    def test_run_requires_store(self):
        """run() without a database raises ValueError."""
        src_a, src_b = _make_two_sources()

        job = PipelineJob()  # no database
        with job:
            Join()(src_a, src_b)

        with pytest.raises(ValueError, match="database"):
            job.run()

    def test_run_twice_is_safe(self, store):
        """Calling run() twice on the same job does not raise."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)

        job = PipelineJob(database=store)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        job.run()
        _ = job.run()  # second run should not raise

        table = job.nodes["adder"].as_table()
        totals = sorted(cast(list[int], table.column("total").to_pylist()))
        assert totals == [110, 220]

    def test_run_accepts_explicit_sync_orchestrator(self):
        from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

        def double_x(x: int) -> int:
            return x * 2

        db = InMemoryArrowDatabase()
        src = _make_source("key", "x", {"key": ["a", "b"], "x": [1, 2]})
        pf = PythonDataFunction(double_x, output_keys="result")
        pod = FunctionPod(data_function=pf)

        job = PipelineJob(name="orch_param", database=db)
        with job:
            pod(src, label="doubler")

        _ = job.run(orchestrator=SyncPipelineOrchestrator())
        records = job.nodes["doubler"].get_all_records()
        assert records is not None
        assert records.num_rows == 2

    def test_run_accepts_async_orchestrator(self):
        from orcapod.pipeline import AsyncPipelineOrchestrator

        def double_x(x: int) -> int:
            return x * 2

        db = InMemoryArrowDatabase()
        src = _make_source("key", "x", {"key": ["a", "b"], "x": [1, 2]})
        pf = PythonDataFunction(double_x, output_keys="result")
        pod = FunctionPod(data_function=pf)

        job = PipelineJob(name="async_orch", database=db)
        with job:
            pod(src, label="doubler")

        _ = job.run(orchestrator=AsyncPipelineOrchestrator())
        records = job.nodes["doubler"].get_all_records()
        assert records is not None
        assert records.num_rows == 2


class TestPipelineJobEndToEnd:
    def test_end_to_end_source_join_function(self, store):
        """Two sources → Join → FunctionPod all execute correctly."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)

        job = PipelineJob(database=store)
        with job:
            joined = Join()(src_a, src_b, label="joiner")
            pod(joined, label="adder")

        assert isinstance(job.pipeline.nodes["joiner"], OperatorNode)
        assert isinstance(job.pipeline.nodes["adder"], FunctionNode)

        _ = job.run()

        fn_records = job.nodes["adder"].get_all_records()
        assert fn_records is not None
        assert fn_records.num_rows == 2

        table = job.nodes["adder"].as_table()
        totals = sorted(cast(list[int], table.column("total").to_pylist()))
        assert totals == [110, 220]

    def test_from_pipeline_then_run(self, store):
        """PipelineJob.from_pipeline() + job.run() produces correct results."""
        from orcapod.pipeline.graph import Pipeline

        src_a, src_b = _make_two_sources()
        tag_a, data_a = src_a.output_schema()
        tag_b, data_b = src_b.output_schema()
        node_a = SourceNode(name="src_a", tag_schema=tag_a, data_schema=data_a)
        node_b = SourceNode(name="src_b", tag_schema=tag_b, data_schema=data_b)
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)

        pipeline = Pipeline(name="bp")
        with pipeline:
            joined = Join()(node_a, node_b)
            pod(joined, label="adder")

        job = PipelineJob.from_pipeline(
            pipeline,
            sources={"src_a": src_a, "src_b": src_b},
            database=store,
        )
        _ = job.run()

        table = job.nodes["adder"].as_table()
        totals = sorted(cast(list[int], table.column("total").to_pylist()))
        assert totals == [110, 220]


class TestPipelineJobSerialization:
    def test_save_creates_file(self, store, tmp_path):
        src_a, src_b = _make_two_sources()
        job = PipelineJob(database=store)
        with job:
            Join()(src_a, src_b)

        path = tmp_path / "completed.json"
        job.save(str(path))
        assert path.exists()

    def test_save_has_version(self, store, tmp_path):
        src_a, src_b = _make_two_sources()
        job = PipelineJob(database=store)
        with job:
            Join()(src_a, src_b)

        path = tmp_path / "completed.json"
        job.save(str(path))
        data = json.loads(path.read_text())
        assert data["orcapod_pipeline_job_version"] == "0.2.0"

    def test_save_stamps_orcapod_version(self, store, tmp_path):
        src_a, src_b = _make_two_sources()
        job = PipelineJob(database=store)
        with job:
            Join()(src_a, src_b)
        path = tmp_path / "stamped.json"
        job.save(str(path))
        data = json.loads(path.read_text())
        assert data["orcapod_version"] == orcapod.__version__
        assert data["pipeline"]["orcapod_version"] == orcapod.__version__
        assert data["orcapod_pipeline_job_version"] == "0.2.0"
        assert data["pipeline"]["orcapod_pipeline_version"] == "0.2.0"

    def test_format_0_2_0_roundtrip_keeps_stamp_and_nodes(self, store, tmp_path):
        src_a, src_b = _make_two_sources()
        job = PipelineJob(database=store)
        with job:
            Join()(src_a, src_b)
        path = tmp_path / "roundtrip.json"
        job.save(str(path))
        loaded = PipelineJob.load(str(path))
        assert set(loaded.pipeline.nodes) == set(job.pipeline.nodes)
        resaved = tmp_path / "resaved.json"
        loaded.save(str(resaved))
        data = json.loads(resaved.read_text())
        assert data["orcapod_pipeline_job_version"] == "0.2.0"
        assert data["orcapod_version"] == orcapod.__version__

    def test_format_0_1_0_fixture_loads_unchanged(self):
        """A job file written by main before the bump (0.1.0, no stamp) loads.

        The fixture was produced by ``fixtures/make_job_format_0_1_0.py`` run
        against the source of origin/main commit cdb4f160.
        """
        path = _FIXTURES / "job_format_0_1_0.json"
        data = json.loads(path.read_text())
        assert data["orcapod_pipeline_job_version"] == "0.1.0"
        assert "orcapod_version" not in data
        # The file keeps the pre-ITL-1082 key ``bindings.store``: it loads with an
        # explicit database (the saved block is ignored).
        loaded = PipelineJob.load(str(path), database=InMemoryArrowDatabase())
        # Hashes recomputed on load equal the ones the 0.1.0 writer stored.
        stored = {n["label"]: n for n in data["pipeline"]["nodes"].values()}
        assert set(loaded.pipeline.nodes) == set(stored)
        for label, node in loaded.pipeline.nodes.items():
            assert node.content_hash().to_string() == stored[label]["content_hash"]
            assert node.pipeline_hash().to_string() == stored[label]["pipeline_hash"]

    def test_saved_file_names_the_database_under_bindings_database(
        self, store, tmp_path
    ):
        """save() writes the database config as ``bindings.database``; load() reads it."""
        src_a, src_b = _make_two_sources()
        job = PipelineJob(database=store)
        with job:
            Join()(src_a, src_b)
        path = tmp_path / "job.json"
        job.save(str(path))
        bindings = json.loads(path.read_text())["bindings"]
        assert "store" not in bindings
        assert bindings["database"] == store.to_config()
        loaded = PipelineJob.load(str(path))
        assert loaded.database is not None
        assert loaded.database.to_config() == store.to_config()

    def _file_with_old_key(self, store, tmp_path) -> Path:
        """A saved job whose database block sits under the pre-rename key."""
        src_a, src_b = _make_two_sources()
        job = PipelineJob(database=store)
        with job:
            Join()(src_a, src_b)
        path = tmp_path / "old.json"
        job.save(str(path))
        data = json.loads(path.read_text())
        data["bindings"]["store"] = data["bindings"].pop("database")
        path.write_text(json.dumps(data))
        return path

    def test_file_with_the_old_store_key_raises_without_a_database(
        self, store, tmp_path
    ):
        """No silent load without the database, no fallback read of the old key."""
        path = self._file_with_old_key(store, tmp_path)
        with pytest.raises(ValueError) as excinfo:
            PipelineJob.load(str(path))
        message = str(excinfo.value)
        assert "bindings.store" in message
        assert "bindings.database" in message
        assert "saved before the rename" in message
        assert "database=" in message

    def test_file_with_the_old_store_key_loads_with_an_explicit_database(
        self, store, tmp_path
    ):
        """An explicit database overrides the saved block, so the old key is moot."""
        path = self._file_with_old_key(store, tmp_path)
        loaded = PipelineJob.load(str(path), database=store)
        assert loaded.database is store

    @pytest.mark.parametrize("version", ["9.9.9", "0.3.0", ""])
    def test_unknown_job_format_version_is_rejected(self, store, tmp_path, version):
        src_a, src_b = _make_two_sources()
        job = PipelineJob(database=store)
        with job:
            Join()(src_a, src_b)
        path = tmp_path / "unknown.json"
        job.save(str(path))
        data = json.loads(path.read_text())
        data["orcapod_pipeline_job_version"] = version
        path.write_text(json.dumps(data))
        with pytest.raises(ValueError, match="Unsupported PipelineJob format version"):
            PipelineJob.load(str(path))

    @pytest.mark.parametrize("variant", ["absent", "0.0.1", "99.0.0"])
    def test_load_ignores_orcapod_version(self, store, tmp_path, variant):
        """The stamp is informational: absent or unfamiliar, the file loads."""
        src_a, src_b = _make_two_sources()
        job = PipelineJob(database=store)
        with job:
            Join()(src_a, src_b)
        path = tmp_path / "job_version.json"
        job.save(str(path))
        data = json.loads(path.read_text())
        for obj in (data, data["pipeline"]):
            if variant == "absent":
                del obj["orcapod_version"]
            else:
                obj["orcapod_version"] = variant
        path.write_text(json.dumps(data))
        loaded = PipelineJob.load(str(path))
        assert set(loaded.pipeline.nodes) == set(job.pipeline.nodes)

    def test_save_includes_run_block(self, store, tmp_path):
        """Unsaved template has status=pending and null run fields."""
        src_a, src_b = _make_two_sources()
        job = PipelineJob(database=store)
        with job:
            Join()(src_a, src_b)

        path = tmp_path / "completed.json"
        job.save(str(path))
        data = json.loads(path.read_text())
        assert data["run"]["status"] == "pending"
        assert data["run"]["run_id"] is None

    def test_load_roundtrip_restores_pipeline(self, store, tmp_path):
        """load() restores the pipeline topology."""
        src_a, src_b = _make_two_sources()
        job = PipelineJob(database=store)
        with job:
            Join()(src_a, src_b, label="joiner")

        path = tmp_path / "completed.json"
        job.save(str(path))
        loaded = PipelineJob.load(str(path))

        assert "joiner" in loaded.pipeline.nodes

    def test_load_roundtrip_after_run(self, store, tmp_path):
        """Save after run() → load → pipeline topology and run status preserved."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)

        job = PipelineJob(database=store)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        _ = job.run()
        path = tmp_path / "completed.json"
        job.save(str(path))

        # Verify the file has correct status
        data = json.loads(path.read_text())
        assert data["run"]["status"] == "complete"

        # Load and verify topology
        loaded = PipelineJob.load(str(path))
        assert "adder" in loaded.pipeline.nodes  # pipeline blueprint has node

    def test_load_version_mismatch_raises(self, store, tmp_path):
        """PipelineJob.load() raises ValueError for an unsupported format version."""
        src_a, src_b = _make_two_sources()
        job = PipelineJob(database=store)
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
        job = PipelineJob(database=store)
        with job:
            Join()(src_a, src_b)
        path = tmp_path / "pending.json"
        job.save(str(path))
        loaded = PipelineJob.load(str(path), database=store)
        assert loaded._has_run is False

    def test_load_after_run_restores_has_run_true(self, store, tmp_path):
        """Loaded job saved after run() has _has_run=True."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)
        job = PipelineJob(database=store)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")
        _ = job.run()
        path = tmp_path / "completed.json"
        job.save(str(path))
        loaded = PipelineJob.load(str(path), database=store)
        assert loaded._has_run is True

    def test_load_restores_pipeline_name(self, store, tmp_path):
        """PipelineJob.load() must restore _name from the blueprint."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)

        job = PipelineJob(name="named_pipeline", database=store)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        path = tmp_path / "named.json"
        job.save(str(path))
        loaded = PipelineJob.load(str(path), database=store)

        assert loaded._name == ("named_pipeline",), (
            "PipelineJob.load() should restore _name from the saved pipeline name"
        )

    def test_load_after_partial_run_preserves_unbound_sources(self, store, tmp_path):
        """Loaded job reports unbound sources for slots not yet bound."""
        src_a, src_b = _make_two_sources()
        tag_b, data_b = src_b.output_schema()
        node_b = SourceNode(name="unbound_b", tag_schema=tag_b, data_schema=data_b)
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)
        job = PipelineJob(database=store)
        with job:
            joined = Join()(src_a, node_b)
            pod(joined, label="adder")
        _ = job.run()
        assert "unbound_b" in job.unbound_sources
        path = tmp_path / "partial.json"
        job.save(str(path))
        loaded = PipelineJob.load(str(path), database=store)
        assert "unbound_b" in loaded.unbound_sources

    def test_load_bind_works_after_load(self, store, tmp_path):
        """bind() must work on a loaded job — _persistent_node_map is populated."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)
        job = PipelineJob(database=store)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        path = tmp_path / "completed.json"
        job.save(str(path))

        # Load without sources so bind() is needed.
        loaded = PipelineJob.load(str(path))

        # bind() must not raise "no matching source slot".
        loaded.bind(
            sources={src_a.source_id: src_a, src_b.source_id: src_b}, database=store
        )

        assert loaded._sources[src_a.source_id] is src_a
        assert loaded._sources[src_b.source_id] is src_b

    def test_load_is_not_runnable_after_bind(self, store, tmp_path):
        """Binding every source leaves a loaded job read-only."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)
        job = PipelineJob(database=store)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        path = tmp_path / "completed.json"
        job.save(str(path))

        loaded = PipelineJob.load(str(path))

        # Before binding, is_runnable() should return False.
        assert loaded.is_runnable("adder") is False

        loaded.bind(
            sources={src_a.source_id: src_a, src_b.source_id: src_b}, database=store
        )

        # Binding sources permits reads, not execution of a loaded job.
        assert loaded.is_runnable("adder") is False

    def test_load_nodes_are_job_nodes(self, store, tmp_path):
        """job.nodes on a loaded job returns FunctionJobNode, not blueprint FunctionNode."""
        from orcapod.core.nodes.function_node import FunctionJobNode

        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)
        job = PipelineJob(database=store)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")
        job.save(str(tmp_path / "completed.json"))

        loaded = PipelineJob.load(str(tmp_path / "completed.json"), database=store)
        adder = loaded.nodes.get("adder")
        assert adder is not None, "Expected 'adder' in loaded job nodes"
        assert isinstance(adder, FunctionJobNode), (
            f"Expected FunctionJobNode after load(), got {type(adder).__name__}"
        )

    def test_load_get_all_records_returns_results(self, store, tmp_path):
        """After run() + save() + load(), get_all_records() returns the computed results."""
        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)
        job = PipelineJob(database=store)
        with job:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        _ = job.run()
        original_records = job.nodes["adder"].get_all_records()
        assert original_records is not None, "run() produced no records"

        path = tmp_path / "completed.json"
        job.save(str(path))

        loaded = PipelineJob.load(str(path), database=store)
        loaded_records = loaded.nodes["adder"].get_all_records()
        assert loaded_records is not None, (
            "get_all_records() returned None on a loaded job; "
            "FunctionJobNode stubs may not be wired to the database."
        )
        assert loaded_records.num_rows == original_records.num_rows, (
            f"Expected {original_records.num_rows} rows, got {loaded_records.num_rows}"
        )
        # Verify the computed values are intact
        assert sorted(loaded_records.column("total").to_pylist()) == sorted(
            original_records.column("total").to_pylist()
        )


# ---------------------------------------------------------------------------
# Tests: PipelineJob.from_pipeline()
# ---------------------------------------------------------------------------


@pytest.fixture
def compiled_pipeline():
    """A compiled Pipeline with SourceNode leaves (no concrete sources)."""
    from orcapod.pipeline.graph import Pipeline

    src_a, src_b = _make_two_sources()
    tag_a, data_a = src_a.output_schema()
    tag_b, data_b = src_b.output_schema()
    node_a = SourceNode(name="slot_a", tag_schema=tag_a, data_schema=data_a)
    node_b = SourceNode(name="slot_b", tag_schema=tag_b, data_schema=data_b)

    pipeline = Pipeline(name="test_pipeline")
    with pipeline:
        Join()(node_a, node_b, label="joiner")
    return pipeline


@pytest.fixture
def db():
    """An in-memory database."""
    return InMemoryArrowDatabase()


@pytest.fixture
def source_a():
    """Concrete source matching slot_a schema (key tag, value data)."""
    return _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})


@pytest.fixture
def source_b():
    """Concrete source matching slot_b schema (key tag, score data)."""
    return _make_source("key", "score", {"key": ["a", "b"], "score": [100, 200]})


@pytest.fixture
def pipeline_job(compiled_pipeline):
    """A PipelineJob with a compiled pipeline but not fully bound."""
    return PipelineJob.from_pipeline(compiled_pipeline)


@pytest.fixture
def pipeline_job_complete(compiled_pipeline, db, source_a, source_b):
    """A PipelineJob with store and all sources bound."""
    return PipelineJob.from_pipeline(
        compiled_pipeline,
        database=db,
        sources={"slot_a": source_a, "slot_b": source_b},
    )


@pytest.fixture
def pipeline_job_with_sources(store):
    """A PipelineJob created via with-block using concrete sources + a FunctionPod."""
    src_a, src_b = _make_two_sources()
    pf = PythonDataFunction(add_values, output_keys="total")
    pod = FunctionPod(data_function=pf)

    job = PipelineJob(database=store)
    with job:
        joined = Join()(src_a, src_b, label="joiner")
        pod(joined, label="adder")
    return job


class TestPipelineJobUsesJobNodes:
    """PipelineJob._persistent_node_map must contain only JobNode variants after recording."""

    def test_persistent_map_has_source_job_nodes(self, pipeline_job_with_sources):
        """Source entries in PipelineJob._persistent_node_map must be SourceJobNode."""
        from orcapod.core.nodes.source_node import SourceJobNode, SourceNodeBase

        assert pipeline_job_with_sources._persistent_node_map is not None, (
            "_persistent_node_map must be set after with-block"
        )
        for node in pipeline_job_with_sources._persistent_node_map.values():
            if isinstance(node, SourceNodeBase):
                assert isinstance(node, SourceJobNode), (
                    f"Expected SourceJobNode but got {type(node).__name__}"
                )

    def test_persistent_map_has_function_job_nodes(self, pipeline_job_with_sources):
        """Function entries in PipelineJob._persistent_node_map must be FunctionJobNode."""
        from orcapod.core.nodes.function_node import FunctionJobNode, FunctionNodeBase

        assert pipeline_job_with_sources._persistent_node_map is not None
        fn_nodes = [
            n
            for n in pipeline_job_with_sources._persistent_node_map.values()
            if isinstance(n, FunctionNodeBase)
        ]
        assert len(fn_nodes) >= 1, "Expected at least one FunctionJobNode"
        for node in fn_nodes:
            assert isinstance(node, FunctionJobNode), (
                f"Expected FunctionJobNode but got {type(node).__name__}"
            )

    def test_persistent_map_has_operator_job_nodes(self, pipeline_job_with_sources):
        """Operator entries in PipelineJob._persistent_node_map must be OperatorJobNode."""
        from orcapod.core.nodes.operator_node import OperatorJobNode, OperatorNodeBase

        assert pipeline_job_with_sources._persistent_node_map is not None
        op_nodes = [
            n
            for n in pipeline_job_with_sources._persistent_node_map.values()
            if isinstance(n, OperatorNodeBase)
        ]
        assert len(op_nodes) >= 1, "Expected at least one OperatorJobNode"
        for node in op_nodes:
            assert isinstance(node, OperatorJobNode), (
                f"Expected OperatorJobNode but got {type(node).__name__}"
            )

    def test_blueprint_pipeline_still_has_lightweight_nodes(
        self, pipeline_job_with_sources
    ):
        """The compiled pipeline's _persistent_node_map still has lightweight nodes."""
        from orcapod.core.nodes.function_node import FunctionJobNode
        from orcapod.core.nodes.operator_node import OperatorJobNode

        for node in pipeline_job_with_sources.pipeline._persistent_node_map.values():
            assert not isinstance(node, FunctionJobNode), (
                "Blueprint pipeline must not contain FunctionJobNode"
            )
            assert not isinstance(node, OperatorJobNode), (
                "Blueprint pipeline must not contain OperatorJobNode"
            )


class TestFromPipeline:
    """PipelineJob.from_pipeline() creates a runnable job from a compiled Pipeline."""

    def test_from_pipeline_creates_pipeline_job(self, compiled_pipeline, db):
        """from_pipeline returns a PipelineJob with the same topology."""
        job = PipelineJob.from_pipeline(compiled_pipeline, database=db)
        assert isinstance(job, PipelineJob)

    def test_from_pipeline_raises_for_stub_nodes(self, db, tmp_path):
        """from_pipeline() raises RuntimeError if the pipeline has stub nodes (loaded without live pods)."""
        from orcapod.pipeline.graph import Pipeline

        src_a, src_b = _make_two_sources()
        pf = PythonDataFunction(add_values, output_keys="total")
        pod = FunctionPod(data_function=pf)

        pipeline = Pipeline(name="stub_test")
        with pipeline:
            joined = Join()(src_a, src_b)
            pod(joined, label="adder")

        path = tmp_path / "pipeline.json"
        pipeline.save(str(path))

        # Load the pipeline without live pods — FunctionNode._function_pod will be None.
        loaded_pipeline = Pipeline.load(str(path))

        with pytest.raises(RuntimeError, match="function_pod is None"):
            PipelineJob.from_pipeline(loaded_pipeline, database=db)

    def test_from_pipeline_with_sources_binds_them(
        self, compiled_pipeline, db, source_a
    ):
        """Sources passed to from_pipeline are immediately bound."""
        job = PipelineJob.from_pipeline(
            compiled_pipeline, database=db, sources={"slot_a": source_a}
        )
        assert "slot_a" in job._sources

    def test_pipeline_bind_removed(self, compiled_pipeline):
        """Pipeline.bind() no longer exists."""
        assert not hasattr(compiled_pipeline, "bind"), (
            "Pipeline.bind() must be removed — use PipelineJob.from_pipeline() instead"
        )


class TestMutatingBind:
    """PipelineJob.bind() mutates in place and returns None."""

    def test_bind_returns_none(self, pipeline_job, source_a):
        result = pipeline_job.bind(sources={"slot_a": source_a})
        assert result is None

    def test_bind_mutates_sources(self, pipeline_job, source_a):
        pipeline_job.bind(sources={"slot_a": source_a})
        assert "slot_a" in pipeline_job._sources

    def test_bind_mutates_store(self, pipeline_job, db):
        pipeline_job.bind(database=db)
        assert pipeline_job._database is db


class TestAsPipeline:
    """PipelineJob.as_pipeline() returns a lightweight Pipeline."""

    def test_as_pipeline_returns_pipeline(self, pipeline_job_complete):
        from orcapod.pipeline.graph import Pipeline

        pipeline = pipeline_job_complete.as_pipeline()
        assert isinstance(pipeline, Pipeline)

    def test_as_pipeline_node_hashes_match(self, pipeline_job_complete):
        """as_pipeline() blueprint nodes have matching hashes to job nodes."""
        job = pipeline_job_complete
        pipeline = job.as_pipeline()

        for node_hash in job._persistent_node_map:
            assert node_hash in pipeline._persistent_node_map
