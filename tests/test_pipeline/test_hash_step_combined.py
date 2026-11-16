"""Behaviour that holds only where the v0.3 hash-step branches meet (ITL-967).

The combined change: starfix 0.5 (ITL-910), data context v1 and the table layouts
(ITL-944), the pre-v0.3 table scan (ITL-954), the hash label rule (ITL-945), the
wrong-hash fixes (ITL-942) and logical type identifiers (ITL-935). Each test here is
about a point where two of them touch, or a review finding fixed on the combined branch.
"""

from __future__ import annotations

import json
import pathlib
import re
from typing import Any, cast

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.contexts import stored_data_contexts
from orcapod.core.nodes.operator_node import OperatorJobNode
from orcapod.core.operators import SelectDataColumns
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import SideEffectLogDataContextError
from orcapod.side_effects import SideEffectPod, sink_pod
from orcapod.sources import DictSource
from orcapod.types import CacheMode

JOB_NAME = "itl967"
ROWS = [{"k": "a", "x": 1}, {"k": "b", "x": 2}]
CURRENT = "std:v1:default"
BARE_SHA256 = re.compile(r"[0-9a-f]{64}")

delivered: list[int] = []


@function_pod(output_keys="double")
def double(x: int) -> int:
    """Double ``x``."""
    return 2 * x


def _deliver(x: int, ctx: Any) -> None:
    """The side effect of the sink: record what was delivered."""
    delivered.append(x)


@pytest.fixture(autouse=True)
def _reset() -> None:
    """Forget the deliveries of earlier tests."""
    delivered.clear()


def _operator_job(store: InMemoryArrowDatabase) -> PipelineJob:
    """A job with function node ``dbl`` and a stored (``LOG``) operator node ``sel``."""
    job = PipelineJob(name=JOB_NAME, database=store)
    with job:
        source = DictSource(
            data=ROWS, tag_columns=["k"], tag_only_columns=True, source_id="src"
        )
        out = double.pod(source, label="dbl")
        SelectDataColumns(["double"])(out, label="sel")
    # OperatorNode.attach_databases is the public way to turn on LOG / REPLAY mode.
    job.nodes["sel"].attach_databases(
        pipeline_database=store.at(JOB_NAME), cache_mode=CacheMode.LOG
    )
    return job


@function_pod(output_keys="sent", ctx_arg="ctx")
def notify(x: int, ctx: Any) -> int:
    """Record a delivery; a function pod that takes the invocation context."""
    delivered.append(x)
    return x


def _notify_job(store: InMemoryArrowDatabase) -> PipelineJob:
    """A job whose only stored node is a function pod that takes the invocation context."""
    job = PipelineJob(name=JOB_NAME, database=store)
    with job:
        source = DictSource(
            data=ROWS, tag_columns=["k"], tag_only_columns=True, source_id="src"
        )
        notify.pod(source, label="notify")
    return job


def _add_table_written_before_v03(
    store: InMemoryArrowDatabase, current_layout: str, old_layout: str
) -> tuple[str, ...]:
    """Store a table of the same pod at an older layout, under another hash."""
    (current_path,) = [p for p in store.list_sources() if p[-1] == current_layout]
    old_path = current_path[:-2] + ("schema:" + "0" * 64, old_layout)
    key = pa.array([b"arrow_v0.1:00"], pa.large_binary())
    store.add_records(
        old_path,
        pa.table({"__record_id": key, "record_id_hash": key}),
        record_id_column="__record_id",
    )
    store.flush()
    return old_path


def _sink_job(store: InMemoryArrowDatabase) -> PipelineJob:
    """A job whose only stored node is a sink reading the source directly."""
    job = PipelineJob(name=JOB_NAME, database=store)
    with job:
        source = DictSource(
            data=ROWS, tag_columns=["k"], tag_only_columns=True, source_id="src"
        )
        sink = sink_pod(_deliver)
        assert isinstance(sink, SideEffectPod)
        sink.process(source, label="sink")
    return job


class TestStoredPathsAndKeys:
    """Layouts (ITL-944), bare digests in paths and labelled row keys (ITL-945) together."""

    def test_every_table_path_holds_bare_digests_and_ends_with_a_current_layout(self):
        """Table paths end in a current layout and hold digests without a hasher label."""
        store = InMemoryArrowDatabase()
        _ = _operator_job(store).run()

        paths = sorted(store.list_sources())
        assert sorted(path[-1] for path in paths) == ["odb_v1", "pdb_v2", "rdb_v2"]
        for path in paths:
            for component in path:
                assert "semantic_v" not in component, path
                assert "arrow_v" not in component, path
                _, _, digest = component.rpartition(":")
                if component.startswith(("schema:", "instance:", "node:")):
                    assert BARE_SHA256.fullmatch(digest), path

    def test_operator_cache_rows_carry_a_labelled_key_and_the_data_context(self):
        """Operator cache rows have an ``arrow_v1:`` key and name the current data
        context.
        """
        store = InMemoryArrowDatabase()
        _ = _operator_job(store).run()

        (path,) = [p for p in store.list_sources() if p[-1] == "odb_v1"]
        table = store.get_all_records(path, record_id_column="_record_hash")
        assert table is not None
        keys = cast(list[bytes], table.column("_record_hash").to_pylist())
        assert len(keys) == len(ROWS)
        assert all(key.startswith(b"arrow_v1:") for key in keys)
        assert table.column("__data_context").to_pylist() == [CURRENT] * len(ROWS)
        assert stored_data_contexts(store, path) == {CURRENT: len(ROWS)}


class TestReadOnlyOperatorNode:
    """A node loaded without its operator reads its stored rows (CodeRabbit, PR #315)."""

    def _read_only_node(
        self, store: InMemoryArrowDatabase, tmp_path: pathlib.Path
    ) -> OperatorJobNode:
        """Run and save the operator job, then rebuild its ``sel`` node without an
        operator.
        """
        job = _operator_job(store)
        _ = job.run()
        saved = tmp_path / "job.json"
        job.save(str(saved))
        nodes = json.loads(saved.read_text())["pipeline"]["nodes"]
        (descriptor,) = [d for d in nodes.values() if d.get("label") == "sel"]
        # A read-only node finds its rows through the descriptor's "pipeline_path" and
        # "content_hash". A saved job file carries no "pipeline_path" today and a
        # content hash that differs from the one the run stored (ITL-904), so both
        # are taken from the node that ran.
        ran = job.nodes["sel"]
        descriptor["pipeline_path"] = list(ran.node_identity_path)
        descriptor["content_hash"] = ran.content_hash().to_string()
        # from_descriptor with no operator is how a job file whose operator cannot be
        # rebuilt gives a read-only node.
        return OperatorJobNode.from_descriptor(
            descriptor,
            operator=None,
            input_streams=(),
            databases={"pipeline": store.at(JOB_NAME)},
        )

    def test_it_names_the_data_context_saved_with_it(self, tmp_path: pathlib.Path):
        """The read-only node reports the current data context key."""
        node = self._read_only_node(InMemoryArrowDatabase(), tmp_path)
        assert node.data_context_key == CURRENT

    def test_it_reads_the_rows_stored_under_its_data_context(
        self, tmp_path: pathlib.Path
    ):
        """The read-only node returns the rows the run stored."""
        node = self._read_only_node(InMemoryArrowDatabase(), tmp_path)
        records = node.get_all_records()
        assert records is not None
        assert sorted(cast(list[int], records["double"].to_pylist())) == [2, 4]


class TestReadOnlyFunctionNode:
    """A function node loaded without its pod names its data context (CodeRabbit, PR #323)."""

    def test_it_names_the_data_context_saved_with_it(self, tmp_path: pathlib.Path):
        """The key comes from the descriptor when there is no function pod to ask."""
        from orcapod.core.nodes.function_node import FunctionJobNode

        store = InMemoryArrowDatabase()
        job = _operator_job(store)
        _ = job.run()
        saved = tmp_path / "job.json"
        job.save(str(saved))
        nodes = json.loads(saved.read_text())["pipeline"]["nodes"]
        (descriptor,) = [d for d in nodes.values() if d.get("label") == "dbl"]
        database = store.at(JOB_NAME)
        node = FunctionJobNode.from_descriptor(
            descriptor,
            input_stream=None,
            databases={"pipeline": database, "result": database.at("_result")},
        )
        assert node.data_context_key == CURRENT


class TestSideEffectNodeChecksEveryRun:
    """A sink looks for completions it cannot match before every run (CodeRabbit, PR #315)."""

    def test_a_table_written_before_v03_that_appears_between_runs_is_refused(self):
        """A sink's second run raises on an old log added after its first run."""
        store = InMemoryArrowDatabase()
        job = _sink_job(store)
        _ = job.run()
        assert sorted(delivered) == [1, 2]
        delivered.clear()

        # A log of the same pod at the layout Orcapod v0.2 wrote, under another hash.
        old_path = _add_table_written_before_v03(store, "tdb_v2", "tdb_v1")

        # The same job object runs the sink again (a node holding a current run is not
        # run, ITL-1095, so it is cleared first): the check is not remembered from the
        # first run.
        job.nodes["sink"].clear_cache()
        with pytest.raises(SideEffectLogDataContextError) as info:
            _ = job.run()
        assert old_path in info.value.record_paths
        assert delivered == []

    def test_a_function_pod_with_ctx_arg_checks_before_every_run_too(self):
        """The other kind of side-effecting node refuses on its second run as well."""
        store = InMemoryArrowDatabase()
        job = _notify_job(store)
        _ = job.run()
        assert sorted(delivered) == [1, 2]
        delivered.clear()
        job.nodes["notify"].clear_cache()  # a current node is not run again (ITL-1095)
        _ = job.run()  # nothing new in the database: runs, and delivers nothing again
        assert delivered == []

        old_path = _add_table_written_before_v03(store, "pdb_v2", "pdb_v1")

        job.nodes["notify"].clear_cache()
        with pytest.raises(SideEffectLogDataContextError) as info:
            _ = job.run()
        assert old_path in info.value.record_paths
        assert delivered == []

    def test_the_error_does_not_offer_a_fresh_store_as_a_safe_retry(self):
        """The error message warns that a fresh database repeats every side effect."""
        error = SideEffectLogDataContextError(
            (("j", "side_effects", "notify", "schema:abc", "tdb_v1"),),
            "std:v0.1:default",
            CURRENT,
            node_label="notify",
        )
        message = str(error)
        assert "Use a fresh database" not in message
        assert (
            "A fresh database holds no completions, so on it the node performs every "
            "side effect again" in message
        )


class TestTablesOfNodesTheRunLeavesOut:
    """A node the run excludes does not hide its pre-v0.3 tables (CodeRabbit, PR #324)."""

    def test_job_total_counts_the_tables_of_an_excluded_node(
        self, caplog: pytest.LogCaptureFixture
    ):
        """A function node behind an unbound source reports nothing itself; the job does."""
        import logging

        from orcapod.core.nodes.source_node import SourceNode

        store = InMemoryArrowDatabase()
        tag_schema, data_schema = DictSource(
            data=ROWS, tag_columns=["k"], tag_only_columns=True, source_id="src"
        ).output_schema()
        job = PipelineJob(name=JOB_NAME, database=store)
        with job:
            unbound = SourceNode(
                name="src", tag_schema=tag_schema, data_schema=data_schema
            )
            double.pod(unbound, label="dbl")
        key = pa.array([b"arrow_v0.1:00"], pa.large_binary())
        old_path = (JOB_NAME, "double", "0" * 64, "v0", "python.function.v0", "pdb_v1")
        store.add_records(
            old_path, pa.table({"__record_id": key}), record_id_column="__record_id"
        )
        store.flush()

        with caplog.at_level(logging.WARNING):
            _ = job.run()

        totals = [
            record.getMessage()
            for record in caplog.records
            if "more table" in record.getMessage()
        ]
        assert len(totals) == 1, [r.getMessage() for r in caplog.records]
        assert "/".join(old_path) in totals[0]
