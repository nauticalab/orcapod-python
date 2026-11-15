"""A run checks each source for new data first (ITL-1102).

Developer, November 14, 2026: "polling source should update its modified time the moment
it finds more data compared to the last time"; "For table back database, run should
trigger a check / reload attempt to see if there is any new data and if so, update modified
time"; "For check that fails, issues a warning".

At the start of ``job.run()``, ``run_async()`` and a job node's ``run()`` (``load_only``
included), every source feeding the run is asked once, through
``check_for_new_data()``, whether its data changed, before the state rule (ITL-1095)
decides which nodes run. ``head()`` and the preview never ask.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import warnings
from typing import Any
from unittest import mock

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import SourceCheckFailedWarning
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import DictSource
from orcapod.core.sources.polling_source import PollingSource
from orcapod.types import Cursor, PollingConfig, Schema

CALLS: list[int] = []
ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}


@function_pod(output_keys="y")
def inc(x: int) -> int:
    """Return ``x + 1``."""
    CALLS.append(x)
    return x + 1


@pytest.fixture(autouse=True)
def _reset() -> None:
    CALLS.clear()


def _run(job: PipelineJob, orchestrator: str, **kwargs: Any) -> Any:
    return job.run(orchestrator=ORCHESTRATORS[orchestrator](), **kwargs)


# ---------------------------------------------------------------------------
# A polling source over a list the test appends to
# ---------------------------------------------------------------------------


class _GrowingImpl:
    """A ``DynamicSourceProtocol`` over ``rows``: the cursor is how many were fetched.

    ``modified_at``, when set, is the cursor time a fetch reports (the time the source
    says its content changed).
    """

    def __init__(self) -> None:
        self.rows: list[dict[str, int]] = [{"k": i, "x": 10 * i} for i in range(3)]
        self.modified_at: dt.datetime | None = None
        self.empty_batch_pending = False

    def identity(self) -> str:
        return "growing"

    def to_config(self) -> None:
        return None

    @classmethod
    def from_config(cls, config: Any) -> _GrowingImpl:
        return cls()

    def schema(self) -> Schema:
        return Schema({"k": int, "x": int})

    async def poll(self, cursor: Cursor[int] | None = None) -> bool:
        taken = cursor.value if cursor is not None else 0
        return self.empty_batch_pending or taken < len(self.rows)

    async def fetch(self, cursor: Cursor[int] | None = None) -> Any:
        taken = cursor.value if cursor is not None else 0
        self.empty_batch_pending = False
        new = self.rows[taken:]
        data = {
            "k": pa.array([r["k"] for r in new], pa.int64()),
            "x": pa.array([r["x"] for r in new], pa.int64()),
        }
        return Cursor(value=len(self.rows), modified_at=self.modified_at), data

    async def close(self) -> None:
        pass


def _polling(impl: _GrowingImpl) -> PollingSource[int]:
    return PollingSource(
        impl,
        tag_columns="k",
        polling_config=PollingConfig(
            interval=0.02, duration=0.1, max_missed_intervals=50
        ),
        source_id="growing",
    )


def _polling_job(impl: _GrowingImpl) -> PipelineJob:
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        inc.pod(_polling(impl), label="rowwise")
    return job


def _ys(job: PipelineJob) -> list[int]:
    return sorted(r["y"] for r in job.nodes["rowwise"].as_table().to_pylist())


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_nothing_new_a_second_run_computes_nothing(orchestrator: str) -> None:
    job = _polling_job(_GrowingImpl())
    _run(job, orchestrator)
    assert sorted(CALLS) == [0, 10, 20]
    CALLS.clear()
    with mock.patch.object(
        InMemoryArrowDatabase, "add_records", side_effect=AssertionError("store write")
    ):
        summary = _run(job, orchestrator)
    assert CALLS == []
    assert summary.nodes["rowwise"].held == 3
    assert summary.unchecked_sources == ()


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_new_rows_are_processed_by_the_next_run(orchestrator: str) -> None:
    impl = _GrowingImpl()
    job = _polling_job(impl)
    _run(job, orchestrator)
    CALLS.clear()
    impl.rows.append({"k": 3, "x": 30})
    summary = _run(job, orchestrator)
    assert CALLS == [30]  # only the input without a stored result is computed
    assert summary.nodes["rowwise"].computed == 1
    assert summary.nodes["rowwise"].reused == 3
    assert _ys(job) == [1, 11, 21, 31]


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_new_rows_reported_with_an_earlier_cursor_time_are_still_processed(
    orchestrator: str,
) -> None:
    """A cursor's ``modified_at`` may be before the node's last run (rows written before
    the run, fetched after it); the source's modified time moves to the moment it found
    them, so the node below it is stale."""
    impl = _GrowingImpl()
    impl.modified_at = dt.datetime(2020, 1, 1, tzinfo=dt.timezone.utc)
    job = _polling_job(impl)
    _run(job, orchestrator)
    CALLS.clear()
    impl.rows.append({"k": 3, "x": 30})
    _run(job, orchestrator)
    assert CALLS == [30]
    assert _ys(job) == [1, 11, 21, 31]


def test_a_poll_that_finds_no_rows_does_not_move_the_modified_time() -> None:
    impl = _GrowingImpl()
    source = _polling(impl)
    assert source.check_for_new_data() is True  # the first fetch takes the 3 rows
    before = source.last_modified
    impl.empty_batch_pending = True  # poll says yes, the fetch returns no rows
    assert source.check_for_new_data() is False
    assert source.check_for_new_data() is False  # poll says no
    assert source.last_modified == before
    impl.rows.append({"k": 3, "x": 30})
    assert source.check_for_new_data() is True
    assert source.last_modified is not None and before is not None
    assert source.last_modified > before


def test_run_async_checks_the_sources_too() -> None:
    impl = _GrowingImpl()
    job = _polling_job(impl)
    asyncio.run(job.run_async())
    CALLS.clear()
    assert asyncio.run(job.run_async()).nodes["rowwise"].held == 3
    assert CALLS == []
    impl.rows.append({"k": 3, "x": 30})
    asyncio.run(job.run_async())
    assert CALLS == [30]


# ---------------------------------------------------------------------------
# A source whose check counts calls, finds a change on demand, or fails
# ---------------------------------------------------------------------------


class _CheckedSource(DictSource):
    """A ``DictSource`` whose ``check_for_new_data`` is counted and can raise."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.checks = 0
        self.error: Exception | None = None

    def check_for_new_data(self) -> bool:
        self.checks += 1
        if self.error is not None:
            raise self.error
        return False


ROWS = [{"k": i, "x": 10 * i} for i in range(3)]


def _checked_job(source: _CheckedSource) -> PipelineJob:
    job = PipelineJob(name="j", database=InMemoryArrowDatabase())
    with job:
        rows: Any = inc.pod(source, label="rowwise")
        _ = rows.select_data_columns(["y"], label="picked")
        _ = source.drop_nulls(label="again")  # the source feeds a second node
    return job


def _source() -> _CheckedSource:
    return _CheckedSource(ROWS, tag_columns=["k"], source_id="s")


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_each_source_is_checked_once_per_run(orchestrator: str) -> None:
    source = _source()
    job = _checked_job(source)
    _run(job, orchestrator)
    assert source.checks == 1
    _run(job, orchestrator)
    assert source.checks == 2


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_load_only_run_checks_and_computes_nothing(orchestrator: str) -> None:
    source = _source()
    job = _checked_job(source)
    _run(job, orchestrator, load_only=True)
    assert source.checks == 1
    assert CALLS == []


def test_a_node_run_checks_each_source_once() -> None:
    source = _source()
    job = _checked_job(source)
    job.nodes["picked"].run()
    assert source.checks == 1
    job.nodes["picked"].run()  # current: nothing runs, the source is still checked
    assert source.checks == 2
    assert CALLS == [0, 10, 20]


def test_head_and_the_preview_never_check() -> None:
    source = _source()
    job = _checked_job(source)
    _run(job, "sync")
    checks = source.checks
    node = job.nodes["rowwise"]
    _ = node.head()
    _ = node._repr_html_()  # the notebook preview
    _ = source.head()
    _ = source._repr_html_()
    assert source.checks == checks


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_failed_check_warns_once_and_the_run_goes_on(orchestrator: str) -> None:
    source = _source()
    job = _checked_job(source)
    _run(job, orchestrator)
    CALLS.clear()
    source.error = ConnectionError("server unreachable")
    with pytest.warns(SourceCheckFailedWarning) as caught:
        summary = _run(job, orchestrator)
    failed = [w for w in caught if issubclass(w.category, SourceCheckFailedWarning)]
    assert len(failed) == 1
    message = str(failed[0].message)
    assert "'s'" in message
    assert "ConnectionError: server unreachable" in message
    assert "proceeds with the data the source already holds" in message
    assert CALLS == []  # the job was current: nothing ran
    assert summary.unchecked_sources == ("s",)
    assert "sources not checked for new data (see the warning): s" in repr(summary)
    assert "sources not checked for new data" in summary._repr_html_()


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_stale_job_runs_on_the_held_data_when_its_check_fails(
    orchestrator: str,
) -> None:
    source = _source()
    job = _checked_job(source)
    _run(job, orchestrator)
    before = source.last_modified
    job.nodes["rowwise"].clear_cache()
    source.error = OSError("boom")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        summary = _run(job, orchestrator)
    assert [w.category for w in caught] == [SourceCheckFailedWarning]
    assert summary.nodes["rowwise"].reused == 3
    assert source.last_modified == before
    assert len(job.nodes["picked"].as_table()) == 3


def test_a_failed_check_in_a_node_run_is_reported_in_its_summary() -> None:
    source = _source()
    job = _checked_job(source)
    source.error = OSError("boom")
    with pytest.warns(SourceCheckFailedWarning):
        job.nodes["picked"].run()
    last_run = job.nodes["picked"].last_run
    assert last_run is not None
    assert last_run.unchecked_sources == ("s",)


def test_a_check_that_keeps_failing_warns_at_every_run() -> None:
    """Python's default filter shows a warning once per text and line; every run that
    cannot check a source still says so, at the line that ran it."""
    source = _source()
    job = _checked_job(source)
    source.error = OSError("boom")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("default")
        for _ in range(2):
            _ = _run(job, "sync")
    failed = [w for w in caught if issubclass(w.category, SourceCheckFailedWarning)]
    assert len(failed) == 2
    assert all(w.filename == __file__ for w in failed)


def test_a_source_node_run_checks_its_bound_source() -> None:
    source = _source()
    job = _checked_job(source)
    _ = job.nodes["s"].run()
    assert source.checks == 1
    source.error = OSError("boom")
    with pytest.warns(SourceCheckFailedWarning, match="'s'"):
        view = job.nodes["s"].run(load_only=True)
    assert len(view.as_table()) == 3  # the rows the source already holds


def test_a_held_node_run_with_a_failed_check_keeps_its_last_run() -> None:
    """A node.run() that runs nothing makes no summary: the warning is the report."""
    source = _source()
    job = _checked_job(source)
    job.nodes["picked"].run()
    first = job.nodes["picked"].last_run
    source.error = OSError("boom")
    with pytest.warns(SourceCheckFailedWarning):
        job.nodes["picked"].run()
    assert job.nodes["picked"].last_run is first
    assert first is not None and first.unchecked_sources == ()
