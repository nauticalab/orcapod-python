"""A read of a side-effect job node delivers nothing (ITL-1091).

After ``job.run()``, every read of a side-effect job node (``iter_data``, ``async_iter_data``,
``as_table``, the dataframe methods, ``flow``, ``head`` and the notebook preview, and a
downstream node's read) returns the rows the run passed through and delivers nothing, on both
orchestrators and with either ``track_completion``. ``node.run()`` does the work ``job.run()``
does for the node. A node with no rows from a run in this process returns no rows and
delivers nothing.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Collection
from pathlib import Path
from typing import Any, Literal

import pyarrow as pa
import pytest
from typing_extensions import override

from orcapod import function_pod
from orcapod.core.sources import DictSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import StoreWriteError
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.side_effects import SideEffectJobNode, SideEffectPod, SideEffectPodConfig
from orcapod.streams import ViewStatus
from orcapod.types import ColumnConfig, Schema

ROWS = 6
SORTED = {"sort_by_tags": True}

Orchestrator = Literal["sync", "async"]


def _source() -> DictSource:
    """Six rows: tag ``k``, data ``x``."""
    return DictSource(
        [{"k": i, "x": i} for i in range(ROWS)], tag_columns=["k"], source_id="s"
    )


def _job(
    *,
    track_completion: bool = True,
    on_error: Literal["raise", "log"] = "raise",
    drop_on_failure: bool = True,
    failing: Collection[int] = (),
    upstream: bool = False,
    downstream: bool = False,
    database: ArrowDatabaseProtocol | None = None,
) -> tuple[PipelineJob, list[int]]:
    """A job with a side-effect node ``delivered``, and the list of its deliveries.

    Args:
        track_completion: As for ``SideEffectPodConfig``.
        on_error: As for ``SideEffectPodConfig``.
        drop_on_failure: As for ``SideEffectPodConfig``.
        failing: Values of ``x`` whose delivery raises.
        upstream: Put a function node ``up`` (``x`` passed on unchanged) before the
            side-effect node.
        downstream: Put a function node ``down`` (``y = x + 100``) after it.
        database: The job's database; a new in-memory one by default.
    """
    sent: list[int] = []

    def deliver(ctx: Any, x: int) -> None:
        """The side effect: record ``x``, or fail for the values in ``failing``."""
        if x in failing:
            raise RuntimeError(f"delivery of {x} failed")
        sent.append(x)

    @function_pod(output_keys="x")
    def same(x: int) -> int:
        """Pass ``x`` on."""
        return x

    @function_pod(output_keys="y")
    def plus(x: int) -> int:
        """``x + 100``."""
        return x + 100

    job = PipelineJob(
        name="se",
        database=database if database is not None else InMemoryArrowDatabase(),
    )
    pod = SideEffectPod(
        deliver,
        config=SideEffectPodConfig(
            track_completion=track_completion,
            on_error=on_error,
            drop_on_failure=drop_on_failure,
        ),
    )
    with job:
        stream: StreamProtocol = _source()
        if upstream:
            stream = same.pod(stream, label="up")
        delivered = pod(stream, label="delivered")
        if downstream:
            _ = plus.pod(delivered, label="down")
    return job, sent


def _run(job: PipelineJob, orchestrator: Orchestrator) -> None:
    """Run ``job`` on the named orchestrator."""
    if orchestrator == "async":
        _ = job.run(orchestrator=AsyncPipelineOrchestrator())
    else:
        _ = job.run()


def _reads(node: Any) -> dict[str, Callable[[], Any]]:
    """Every public read of ``node``, by name."""

    async def collect() -> list[Any]:
        return [row async for row in node.async_iter_data()]

    return {
        "head": lambda: node.head(100),
        "preview": node._repr_html_,
        "iter_data": lambda: list(node.iter_data()),
        "__iter__": lambda: list(node),
        "async_iter_data": lambda: asyncio.run(collect()),
        "as_table": node.as_table,
        "as_polars_df": node.as_polars_df,
        "as_df": node.as_df,
        "as_lazy_frame": lambda: node.as_lazy_frame().collect(),
        "as_pandas_df": node.as_pandas_df,
        "flow": node.flow,
    }


def _user_table(table: pa.Table) -> pa.Table:
    """``table`` with its user columns only (``k``, ``x``)."""
    return table.select(["k", "x"])


# ---------------------------------------------------------------------------
# Every read after a run delivers nothing and returns the rows the run passed through
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
@pytest.mark.parametrize("track_completion", [True, False])
def test_every_read_after_a_run_delivers_nothing(
    orchestrator: Orchestrator, track_completion: bool
) -> None:
    """Each read returns the run's rows; the deliveries stay those of the run."""
    job, sent = _job(track_completion=track_completion)
    _run(job, orchestrator)
    assert len(sent) == ROWS
    node = job.nodes["delivered"]
    for name, read in _reads(node).items():
        _ = read()
        assert len(sent) == ROWS, f"{name} delivered again"
    assert len(list(node.iter_data())) == ROWS
    assert _user_table(node.as_table(columns=SORTED)).equals(
        _user_table(_source().as_table(columns=SORTED))
    )
    head = node.head(100)
    assert head.status is ViewStatus.ROWS
    assert len(head) == ROWS


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
def test_node_run_delivers_what_the_log_does_not_record(
    orchestrator: Orchestrator,
) -> None:
    """With ``track_completion``, ``node.run()`` after a run delivers nothing."""
    job, sent = _job(track_completion=True)
    _run(job, orchestrator)
    view = job.nodes["delivered"].run()
    assert len(sent) == ROWS
    assert len(view) == ROWS
    _run(job, orchestrator)
    assert len(sent) == ROWS


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
def test_node_run_without_track_completion_delivers_again_only_after_clear_cache(
    orchestrator: Orchestrator,
) -> None:
    """Without ``track_completion``, a node that holds a current run delivers nothing on
    ``node.run()`` (ITL-1095); after ``clear_cache()`` it delivers every row again, as
    ``job.run()`` does."""
    job, sent = _job(track_completion=False)
    _run(job, orchestrator)
    view = job.nodes["delivered"].run()
    assert len(sent) == ROWS
    assert len(view) == ROWS
    job.nodes["delivered"].clear_cache()
    view = job.nodes["delivered"].run()
    assert len(sent) == 2 * ROWS
    assert len(view) == ROWS
    # job.run() follows the same rule: the node holds a current run, so it delivers
    # nothing; cleared, it delivers every row again.
    _run(job, orchestrator)
    assert len(sent) == 2 * ROWS
    job.nodes["delivered"].clear_cache()
    _run(job, orchestrator)
    assert len(sent) == 3 * ROWS
    # The reads after either kind of run deliver nothing.
    for name, read in _reads(job.nodes["delivered"]).items():
        _ = read()
        assert len(sent) == 3 * ROWS, f"{name} delivered again"


def test_node_run_on_a_job_never_run_delivers_and_logs_as_a_job_run() -> None:
    """``node.run()`` before any ``job.run()`` delivers once; the job run then delivers nothing."""
    job, sent = _job(track_completion=True)
    node = job.nodes["delivered"]
    view = node.run()
    assert len(sent) == ROWS
    assert len(view) == ROWS
    assert len(node.as_table()) == ROWS
    _ = job.run()
    assert len(sent) == ROWS


@pytest.mark.parametrize("track_completion", [True, False])
def test_an_async_run_holds_exactly_its_own_rows(track_completion: bool) -> None:
    """After two async runs, the node holds the rows of the last run only."""
    job, sent = _job(track_completion=track_completion)
    _run(job, "async")
    # A node holding a current run is not run again (ITL-1095): clear it first.
    job.nodes["delivered"].clear_cache()
    _run(job, "async")
    node = job.nodes["delivered"]
    assert len(node.as_table()) == ROWS
    assert len(list(node.iter_data())) == ROWS
    assert len(sent) == (ROWS if track_completion else 2 * ROWS)


# ---------------------------------------------------------------------------
# The rows read equal the rows the run passed downstream
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
@pytest.mark.parametrize("track_completion", [True, False])
def test_downstream_reads_deliver_nothing(
    orchestrator: Orchestrator, track_completion: bool
) -> None:
    """A function node after the side-effect node reads it without delivering."""
    job, sent = _job(track_completion=track_completion, downstream=True)
    _run(job, orchestrator)
    down = job.nodes["down"]
    _ = down.as_table()
    _ = down.run()
    assert len(sent) == ROWS
    assert sorted(down.as_table()["k"].to_pylist()) == sorted(
        job.nodes["delivered"].as_table()["k"].to_pylist()
    )
    assert len(sent) == ROWS


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
@pytest.mark.parametrize("drop_on_failure", [True, False])
def test_rows_whose_delivery_failed_keep_todays_row_set(
    orchestrator: Orchestrator, drop_on_failure: bool, caplog: pytest.LogCaptureFixture
) -> None:
    """Under ``on_error="log"`` a read returns the rows the run passed downstream."""
    caplog.set_level(logging.CRITICAL)
    job, sent = _job(
        on_error="log",
        drop_on_failure=drop_on_failure,
        failing={2, 3},
        downstream=True,
    )
    _run(job, orchestrator)
    assert sorted(sent) == [0, 1, 4, 5]
    node = job.nodes["delivered"]
    passed = sorted(node.as_table()["k"].to_pylist())
    assert passed == ([0, 1, 4, 5] if drop_on_failure else list(range(ROWS)))
    assert sorted(job.nodes["down"].as_table()["k"].to_pylist()) == passed
    assert sorted(sent) == [0, 1, 4, 5]


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
def test_a_run_that_raises_leaves_no_rows_to_read(orchestrator: Orchestrator) -> None:
    """After a run that raised, a read returns no rows and delivers nothing."""
    job, sent = _job(failing={3})
    with pytest.raises(RuntimeError):
        _run(job, orchestrator)
    count = len(sent)
    node = job.nodes["delivered"]
    for name, read in _reads(node).items():
        _ = read()
        assert len(sent) == count, f"{name} delivered again"
    assert len(node.as_table()) == 0
    assert node.head().status is ViewStatus.NOT_COMPUTED


# ---------------------------------------------------------------------------
# No rows from a run: nothing is delivered and no rows are returned
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("track_completion", [True, False])
def test_a_read_before_any_run_delivers_nothing(track_completion: bool) -> None:
    """A fresh job over a store with deliveries recorded returns no rows and delivers nothing."""
    database = InMemoryArrowDatabase()
    first, first_sent = _job(track_completion=track_completion, database=database)
    _ = first.run()
    assert len(first_sent) == ROWS
    job, sent = _job(track_completion=track_completion, database=database)
    node = job.nodes["delivered"]
    for name, read in _reads(node).items():
        _ = read()
        assert sent == [], f"{name} delivered"
    table = node.as_table()
    assert table.num_rows == 0
    assert table.column_names == ["k", "x"]
    head = node.head()
    assert head.status is ViewStatus.NOT_COMPUTED
    assert "run() delivers its side effects" in head.header()


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
def test_rows_held_from_a_run_are_dropped_when_an_upstream_changes(
    orchestrator: Orchestrator,
) -> None:
    """An upstream node that ran since the run makes the held rows out of date: no rows."""
    job, sent = _job(upstream=True)
    _run(job, orchestrator)
    node = job.nodes["delivered"]
    assert len(node.as_table()) == ROWS
    # The upstream runs again: a node that holds a current run runs nothing (ITL-1095),
    # so it is cleared first.
    job.nodes["up"].clear_cache()
    _ = job.nodes["up"].run()
    for name, read in _reads(node).items():
        _ = read()
        assert len(sent) == ROWS, f"{name} delivered again"
    assert len(node.as_table()) == 0
    assert node.head().status is ViewStatus.NOT_COMPUTED


# ---------------------------------------------------------------------------
# node.run() commits the invocation log; head() after an empty run never polls
# ---------------------------------------------------------------------------


def test_node_run_commits_the_invocation_log(tmp_path: Path) -> None:
    """The deliveries ``node.run()`` logged are found by a new job over the same store."""
    job, sent = _job(database=DeltaTableDatabase(tmp_path / "store"))
    _ = job.nodes["delivered"].run()
    assert len(sent) == ROWS
    # A new database object reads only what was committed to the store.
    again, again_sent = _job(database=DeltaTableDatabase(tmp_path / "store"))
    _ = again.run()
    assert again_sent == []


class _SchemaOnlyByPolling(ArrowTableStream):
    """A stream of no rows whose schema, once ``armed``, could be read only by polling."""

    armed: bool = False

    def _schema_requires_io(self, config: ColumnConfig) -> bool:
        """Say that reading the schema would poll, once armed."""
        return self.armed

    @override
    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Refuse once armed: reading the schema would poll."""
        if self.armed:
            raise AssertionError("the schema was read: a poll")
        return super().output_schema(columns=columns, all_info=all_info)


def test_head_after_a_run_that_passed_on_no_rows_never_polls() -> None:
    """With no rows held and columns known only by polling, ``head()`` reads no schema."""
    empty = pa.table(
        {"k": pa.array([], type=pa.int64()), "x": pa.array([], type=pa.int64())}
    )
    upstream = _SchemaOnlyByPolling(empty, tag_columns=["k"])
    sent: list[int] = []

    def deliver(ctx: Any, x: int) -> None:
        """The side effect: record ``x``."""
        sent.append(x)

    node = SideEffectJobNode(
        side_effect_pod=SideEffectPod(deliver), input_stream=upstream
    )
    assert len(node.run()) == 0
    upstream.armed = True
    head = node.head(all_info=True)
    _ = node._repr_html_()  # the notebook preview: the public display hook
    assert head.status is ViewStatus.ROWS
    assert len(head) == 0
    assert "its last run passed on no rows" in head.header()
    assert sent == []


class _FailingFlushDatabase(InMemoryArrowDatabase):
    """An in-memory database whose commit fails."""

    @override
    def flush(self) -> None:
        """Fail, as a store that cannot be written does."""
        raise OSError("the store cannot be written")


def test_node_run_whose_log_commit_fails_leaves_no_rows() -> None:
    """A ``node.run()`` that raises at its commit leaves the node no rows to read."""
    job, sent = _job()
    node = job.nodes["delivered"]
    # The job hands its nodes scoped views of its database; attach the failing one
    # to the node itself.
    node.attach_databases(pipeline_database=_FailingFlushDatabase())
    with pytest.raises(StoreWriteError):
        _ = node.run()
    assert len(sent) == ROWS
    assert list(node.iter_data()) == []
    assert node.head().status is ViewStatus.NOT_COMPUTED
    assert len(sent) == ROWS
