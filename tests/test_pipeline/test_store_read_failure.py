"""Regression tests for ITL-884 at the job level: an unreadable store fails the run.

Before ITL-884 a Delta result table that existed but could not be read was
returned as "no rows": the job warned that the data "may have been deleted
externally", swallowed the per-row read errors under ``error_policy="continue"``,
finished without raising, and the node showed no records. Both orchestrators
must now raise ``StoreReadError`` instead, and a table that does not exist must
still be computed.
"""

from __future__ import annotations

import json
import shutil
from collections.abc import Callable
from pathlib import Path

import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase
from orcapod.errors import StoreReadError
from orcapod.pipeline import PipelineJob
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.observer import NoOpObserver
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

_CALLS: list[int] = []
# Tables the function damages on its next call, standing in for another process
# breaking a table while the job runs.
_DAMAGE_ON_NEXT_CALL: list[Path] = []


@function_pod(output_keys="y")
def double(x: int) -> int:
    """Record the call, damage any queued table, and return twice ``x``."""
    _CALLS.append(x)
    while _DAMAGE_ON_NEXT_CALL:
        require_unknown_reader_feature(_DAMAGE_ON_NEXT_CALL.pop())
    return 2 * x


@function_pod(output_keys="z")
def triple(x: int) -> int:
    """Return three times ``x``."""
    return 3 * x


ORCHESTRATORS: dict[str, Callable[[], object]] = {
    "sync": lambda: SyncPipelineOrchestrator(error_policy="continue"),
    "async": lambda: AsyncPipelineOrchestrator(error_policy="continue"),
}


@pytest.fixture(autouse=True)
def _reset_calls():
    """Start and end every test with no recorded calls and no queued damage."""
    _CALLS.clear()
    _DAMAGE_ON_NEXT_CALL.clear()
    yield
    _CALLS.clear()
    _DAMAGE_ON_NEXT_CALL.clear()


@pytest.fixture(params=list(ORCHESTRATORS))
def make_orchestrator(request) -> Callable[[], object]:
    """Return a factory for the parametrized orchestrator."""
    return ORCHESTRATORS[request.param]


def build_job(store_path: Path, keys: str = "abc") -> PipelineJob:
    """Build a job that doubles ``x`` for one row per key in ``keys``."""
    source = ArrowTableSource(
        pa.table({"k": list(keys), "x": list(range(1, len(keys) + 1))}),
        tag_columns=["k"],
        infer_nullable=True,
    )
    job = PipelineJob(name="itl884", store=DeltaTableDatabase(base_path=store_path))
    with job:
        _ = double.pod(source, label="dbl")
    return job


def table_dirs(store_path: Path) -> tuple[Path, Path]:
    """The (result table, pipeline table) directories the job wrote."""
    dirs = {p.parent for p in store_path.rglob("*.parquet")}
    result = [d for d in dirs if "_result" in d.parts]
    pipeline = [d for d in dirs if "_result" not in d.parts]
    assert len(result) == 1 and len(pipeline) == 1, sorted(dirs)
    return result[0], pipeline[0]


def delete_data_files(table: Path) -> None:
    """Delete a table's data files, leaving its log intact."""
    for f in table.glob("*.parquet"):
        f.unlink()


def require_unknown_reader_feature(table: Path) -> None:
    """Commit a log version that requires a reader feature deltalake does not know.

    Data files already loaded stay readable through the job's open table handle,
    but loading the table again, as every flush does, fails.
    """
    log = table / "_delta_log"
    version = max(int(p.stem) for p in log.glob("*.json")) + 1
    protocol = {
        "minReaderVersion": 3,
        "minWriterVersion": 7,
        "readerFeatures": ["orcapodMadeUpFeature"],
        "writerFeatures": ["orcapodMadeUpFeature"],
    }
    (log / f"{version:020d}.json").write_text(json.dumps({"protocol": protocol}))


@pytest.fixture
def stored(tmp_path: Path, make_orchestrator) -> Path:
    """A store in which the job has computed and stored all three rows."""
    store_path = tmp_path / "store"
    build_job(store_path).run(orchestrator=make_orchestrator())
    assert len(_CALLS) == 3
    _CALLS.clear()
    return store_path


def test_unreadable_result_table_fails_the_run(stored, make_orchestrator):
    """An unreadable result table fails the run before any function call."""
    result_table, _ = table_dirs(stored)
    delete_data_files(result_table)

    job = build_job(stored)
    with pytest.raises(StoreReadError) as info:
        job.run(orchestrator=make_orchestrator())

    assert Path(info.value.table) == result_table
    assert _CALLS == []


def test_unreadable_result_table_fails_the_run_under_continue(
    stored, make_orchestrator
):
    """The per-row lookup's read failure fails the run under ``continue``."""
    # With the pipeline table gone, the job has no stored tag rows to join, so the
    # first read of the result table is the per-row cache lookup, which runs inside
    # the error_policy="continue" handler.
    result_table, pipeline_table = table_dirs(stored)
    delete_data_files(result_table)
    shutil.rmtree(pipeline_table)

    job = build_job(stored)
    with pytest.raises(StoreReadError) as info:
        job.run(orchestrator=make_orchestrator())

    assert Path(info.value.table) == result_table
    assert _CALLS == []


def test_store_read_failure_in_a_row_flush_fails_the_run(stored, make_orchestrator):
    """A read failure in the flush that stores a row's result fails the run."""
    # Rows d and e are new. The result table breaks while d is computed, after
    # d's cache lookup succeeded, so the failure surfaces in the flush that stores
    # d's result, inside the error_policy="continue" per-row handler. Before the
    # fix the flush wrapped the StoreReadError in a RuntimeError, the handler
    # skipped d as a failed row, computed e, and the run ended with RuntimeError.
    result_table, _ = table_dirs(stored)
    _DAMAGE_ON_NEXT_CALL.append(result_table)

    orchestrator = make_orchestrator()
    job = build_job(stored, keys="abcde")
    with pytest.raises(StoreReadError) as info:
        job.run(orchestrator=orchestrator)

    assert Path(info.value.table) == result_table
    # The failure surfaces when the node commits its batch (ITL-919), here at its
    # end: e is computed too, and neither d nor e is stored.
    assert sorted(_CALLS) == [4, 5]


def test_store_read_failure_with_per_row_commits_stops_at_the_row(
    stored, make_orchestrator
):
    """With ``commit_every_rows=1`` a read failure surfaces at the row it broke."""
    result_table, _ = table_dirs(stored)
    _DAMAGE_ON_NEXT_CALL.append(result_table)

    orchestrator = make_orchestrator()
    job = build_job(stored, keys="abcde")
    with pytest.raises(StoreReadError) as info:
        job.run(orchestrator=orchestrator, commit_every_rows=1)

    assert Path(info.value.table) == result_table
    assert info.value.node_label is not None
    if isinstance(orchestrator, SyncPipelineOrchestrator):
        # The run stops at d's commit: e is never computed.
        assert _CALLS == [4]
    else:
        # The async pod computes rows concurrently, so e may already be in flight
        # when d's commit fails.
        assert 4 in _CALLS and len(_CALLS) <= 2


def test_absent_tables_are_computed(stored, make_orchestrator):
    """Tables that do not exist are computed and stored, not reported unreadable."""
    result_table, pipeline_table = table_dirs(stored)
    shutil.rmtree(result_table)
    shutil.rmtree(pipeline_table)

    build_job(stored).run(orchestrator=make_orchestrator())
    assert sorted(_CALLS) == [1, 2, 3]

    # The recomputed rows were stored: a fresh job over the same store computes
    # nothing. (Reading the node right after its own run can miss rows, ITL-864.)
    _CALLS.clear()
    build_job(stored).run(orchestrator=make_orchestrator())
    assert _CALLS == []


class _FailsToStartNode(NoOpObserver):
    """An observer whose ``on_node_start`` raises for one node label."""

    def __init__(self, label: str) -> None:
        """Fail ``on_node_start`` for the node labelled ``label``."""
        self.label = label

    def on_node_start(
        self, node_label: str, node_hash: str, tag_schema: object = None
    ) -> None:
        """Raise ``ValueError`` when the node labelled ``self.label`` starts."""
        if node_label == self.label:
            raise ValueError(f"observer failed for {node_label}")


def test_unrelated_concurrent_failure_does_not_hide_store_read_error(tmp_path):
    """An unrelated concurrent failure does not hide the ``StoreReadError``."""
    # Node "dbl" raises StoreReadError while loading its stored results and node
    # "tri" fails in the observer's on_node_start. Both fail before their first
    # await, so both land in the async TaskGroup's exception group. Before the fix,
    # re-raising inside "except* StoreReadError" left the unmatched ValueError in
    # the group and the caller got an ExceptionGroup, whatever the Python version.
    stored = tmp_path / "store"
    _ = build_job(stored).run()
    result_table, _ = table_dirs(stored)
    delete_data_files(result_table)

    source = ArrowTableSource(
        pa.table({"k": ["a", "b", "c"], "x": [1, 2, 3]}),
        tag_columns=["k"],
        infer_nullable=True,
    )
    job = PipelineJob(name="itl884", store=DeltaTableDatabase(base_path=stored))
    with job:
        _ = double.pod(source, label="dbl")
        _ = triple.pod(source, label="tri")

    with pytest.raises(StoreReadError) as info:
        job.run(
            orchestrator=AsyncPipelineOrchestrator(),
            observer=_FailsToStartNode("tri"),
        )

    assert type(info.value) is StoreReadError
    assert Path(info.value.table) == result_table
    assert info.value.__cause__ is not None
    notes = getattr(info.value, "__notes__", [])
    assert any("ValueError: observer failed for tri" in note for note in notes), notes
