"""``job.run()`` follows the state rule of ``node.run()`` (ITL-1095).

Developer, November 14, 2026 ("let's align now"): ``job.run()`` and ``run_async()`` apply
the same rule as ``node.run()``. A node that holds a current completed run (not stale; for
a full run, not filled by a load-only run) is not run again: its rows feed the nodes after
it. Never-run, stale and (for a full run) load-only-filled nodes run. So a second
``job.run()`` of an unchanged job computes nothing, reads and writes nothing in the database
and delivers no side effect.
"""

from __future__ import annotations

import asyncio
from typing import Any, Literal
from unittest import mock

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import OrcapodDatabaseWriteError
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.side_effects import SideEffectPod, tap_pod
from orcapod.sources import DictSource

CALLS: list[str] = []
DELIVERED: list[int] = []


@function_pod(output_keys="y")
def inc(x: int) -> int:
    """Return ``x + 1``."""
    CALLS.append("inc")
    return x + 1


@function_pod(output_keys="total")
def total(y: list[int]) -> int:
    """Return the sum of a group's values."""
    CALLS.append("total")
    return sum(y)


def _deliver(y: int, ctx: Any) -> None:
    """A side effect: record the delivered value."""
    DELIVERED.append(y)


ROWS = [{"g": i % 2, "k": i, "x": 10 * i} for i in range(6)]
ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}


@pytest.fixture(autouse=True)
def _reset() -> None:
    """Start every test with no recorded calls or deliveries."""
    CALLS.clear()
    DELIVERED.clear()


def _source(data: list[dict[str, int]]) -> DictSource:
    """An in-memory source of ``data`` with tags ``g`` and ``k``."""
    return DictSource(data, tag_columns=["g", "k"], source_id="s")


def _build(data: list[dict[str, int]], db: Any) -> PipelineJob:
    """``rowwise`` -> ``tap`` (side effect, no completion tracking) -> ``grouped`` ->
    ``summed``."""
    job = PipelineJob(name="j", database=db)
    with job:
        rows = inc.pod(_source(data), label="rowwise")
        tap = tap_pod(_deliver)
        assert isinstance(tap, SideEffectPod)
        tapped: Any = tap.process(rows, label="tap")
        grouped = tapped.group_by("g", label="grouped")
        total.pod(grouped, label="summed")
    return job


def _run(job: PipelineJob, orchestrator: str, **kwargs: Any) -> Any:
    """Run ``job`` on the named orchestrator."""
    return job.run(orchestrator=ORCHESTRATORS[orchestrator](), **kwargs)


def _totals(job: PipelineJob) -> list[tuple[int, int]]:
    """The ``(g, total)`` rows of ``summed``, sorted."""
    return sorted(
        (r["g"], r["total"]) for r in job.nodes["summed"].as_table().to_pylist()
    )


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_second_run_of_an_unchanged_job_does_nothing(orchestrator: str) -> None:
    """No computation, store access, source read or delivery; every node held."""
    job = _build(ROWS, InMemoryArrowDatabase())
    _run(job, orchestrator)
    assert len(DELIVERED) == 6
    CALLS.clear()
    DELIVERED.clear()
    with (
        mock.patch.object(
            InMemoryArrowDatabase,
            "get_all_records",
            side_effect=AssertionError("database read"),
        ),
        mock.patch.object(
            InMemoryArrowDatabase,
            "add_records",
            side_effect=AssertionError("database write"),
        ),
        mock.patch.object(
            DictSource, "iter_data", side_effect=AssertionError("source read")
        ),
    ):
        summary = _run(job, orchestrator)
    assert CALLS == []
    assert DELIVERED == []
    assert _totals(job) == [(0, 63), (1, 93)]
    rowwise = summary.nodes["rowwise"]
    assert (rowwise.computed, rowwise.reused, rowwise.held) == (0, 0, 6)
    assert summary.nodes["summed"].held == 2
    assert "rowwise: 6 rows: held from the previous run" in repr(summary)


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_cleared_node_runs_again_with_the_nodes_below_it(orchestrator: str) -> None:
    """Clearing ``summed`` runs it alone, from the rows ``grouped`` holds."""
    job = _build(ROWS, InMemoryArrowDatabase())
    _run(job, orchestrator)
    CALLS.clear()
    DELIVERED.clear()
    job.nodes["summed"].clear_cache()
    summary = _run(job, orchestrator)
    assert CALLS == []  # its results are stored: read, not computed
    assert DELIVERED == []
    assert summary.nodes["summed"].reused == 2
    assert summary.nodes["rowwise"].held == 6
    assert _totals(job) == [(0, 63), (1, 93)]


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_clearing_a_side_effect_node_delivers_it_again(orchestrator: str) -> None:
    """``clear_cache()`` on a side-effect node makes the next run deliver again."""
    job = _build(ROWS, InMemoryArrowDatabase())
    _run(job, orchestrator)
    DELIVERED.clear()
    job.nodes["tap"].clear_cache()
    _run(job, orchestrator)
    assert len(DELIVERED) == 6
    assert CALLS.count("inc") == 6  # only the first run computed


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_rebound_source_runs_every_node_below_it(orchestrator: str) -> None:
    """Binding a new source object makes every node below it run; new inputs compute."""
    job = _build(ROWS[:3], InMemoryArrowDatabase())
    _run(job, orchestrator)
    CALLS.clear()
    job.bind(sources={next(iter(job.sources)): _source(ROWS)})
    _run(job, orchestrator)
    assert CALLS.count("inc") == 3
    assert _totals(job) == [(0, 63), (1, 93)]


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_full_run_after_a_load_only_run_runs_the_load_only_nodes(
    orchestrator: str,
) -> None:
    """Load-only rows are never current for a full run: it computes what was skipped."""
    db = InMemoryArrowDatabase()
    _run(_build(ROWS[:3], db), orchestrator)
    CALLS.clear()
    DELIVERED.clear()
    job = _build(ROWS, db)
    _run(job, orchestrator, load_only=True)
    assert CALLS == [] and DELIVERED == []
    _run(job, orchestrator)
    assert CALLS.count("inc") == 3
    assert len(DELIVERED) == 6
    assert _totals(job) == [(0, 63), (1, 93)]


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_load_only_run_after_a_full_run_does_nothing(orchestrator: str) -> None:
    """A full run's rows are current for a load-only run too."""
    job = _build(ROWS, InMemoryArrowDatabase())
    _run(job, orchestrator)
    CALLS.clear()
    summary = _run(job, orchestrator, load_only=True)
    assert CALLS == []
    assert summary.nodes["rowwise"].held == 6


def test_run_async_follows_the_rule() -> None:
    """``run_async`` holds current nodes as ``run`` does."""
    job = _build(ROWS, InMemoryArrowDatabase())
    asyncio.run(job.run_async())
    CALLS.clear()
    DELIVERED.clear()
    summary = asyncio.run(job.run_async())
    assert CALLS == [] and DELIVERED == []
    assert summary.nodes["summed"].held == 2


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_failed_commit_clears_only_the_nodes_that_ran(orchestrator: str) -> None:
    """A failed end-of-run commit clears the nodes that ran; held nodes keep rows."""
    job = _build(ROWS, InMemoryArrowDatabase())
    _run(job, orchestrator)
    job.nodes["summed"].clear_cache()

    def fail(self: Any) -> None:
        """A flush that always fails."""
        raise OSError("disk full")

    with (
        mock.patch.object(InMemoryArrowDatabase, "flush", fail),
        pytest.raises(OrcapodDatabaseWriteError),
    ):
        _run(job, orchestrator)
    assert job.nodes["summed"].as_table().num_rows == 0
    assert job.nodes["rowwise"].as_table().num_rows == 6
    assert job.nodes["grouped"].as_table().num_rows == 2


@pytest.mark.parametrize("error_policy", ["continue", "fail_fast"])
@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_partly_held_run_under_each_error_policy(
    orchestrator: str, error_policy: Literal["continue", "fail_fast"]
) -> None:
    """Held nodes feed the nodes that run, on both orchestrators and error policies."""
    job = _build(ROWS, InMemoryArrowDatabase())
    kind = ORCHESTRATORS[orchestrator]
    job.run(orchestrator=kind(error_policy=error_policy))
    CALLS.clear()
    job.nodes["summed"].clear_cache()
    summary = job.run(orchestrator=kind(error_policy=error_policy))
    assert CALLS == []
    assert summary.nodes["rowwise"].held == 6
    assert summary.nodes["summed"].reused == 2
    assert _totals(job) == [(0, 63), (1, 93)]
