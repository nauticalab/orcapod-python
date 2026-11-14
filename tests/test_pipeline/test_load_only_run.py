"""``job.run(load_only=True)``: fill the nodes from what is stored, compute nothing (ITL-1095).

Developer, November 13, 2026: "it might help for run to have an argument ... load_only=True
where it would fill the pipeline only based on what can be loaded ... and does not trigger
computation of any missing entries." A load-only run reads the sources, runs operators on
the rows that reach them, and fills each function node with the results already stored for
this run's inputs. No user function is called, no side effect is delivered, nothing is
written to the store. Inputs without a stored result are skipped and counted.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.side_effects import SideEffectPod, tap_pod
from orcapod.sources import DictSource
from orcapod.types import CacheMode

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
    CALLS.clear()
    DELIVERED.clear()


def _build(
    data: list[dict[str, int]],
    db: Any,
    cache_mode: CacheMode | None = None,
    with_tap: bool = False,
) -> PipelineJob:
    """``rowwise`` (function) -> ``grouped`` (GroupBy) -> ``summed`` (function).

    With ``with_tap``, a side-effect node ``tap`` sits between ``rowwise`` and the group.
    """
    src = DictSource(data, tag_columns=["g", "k"], source_id="s")
    job = PipelineJob(name="j", database=db)
    with job:
        rows: Any = inc.pod(src, label="rowwise")
        if with_tap:
            tap = tap_pod(_deliver)
            assert isinstance(tap, SideEffectPod)
            rows = tap.process(rows, label="tap")
        grouped = rows.group_by("g", label="grouped")
        total.pod(grouped, label="summed")
    if cache_mode is not None:
        job.nodes["grouped"].attach_databases(
            pipeline_database=db.at("j"), cache_mode=cache_mode
        )
    return job


def _snapshot(db: InMemoryArrowDatabase) -> dict[tuple[str, ...], int]:
    """Every table of the store with its row count."""
    db.flush()
    counts: dict[tuple[str, ...], int] = {}
    for path in db.list_table_paths():
        records = db.get_all_records(path)
        counts[path] = 0 if records is None else records.num_rows
    return counts


def _run(job: PipelineJob, orchestrator: str, **kwargs: Any) -> Any:
    return job.run(orchestrator=ORCHESTRATORS[orchestrator](), **kwargs)


def _totals(job: PipelineJob) -> list[tuple[int, int]]:
    return sorted(
        (r["g"], r["total"]) for r in job.nodes["summed"].as_table().to_pylist()
    )


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_load_only_run_over_a_full_store_equals_a_run_and_computes_nothing(
    orchestrator: str,
) -> None:
    db = InMemoryArrowDatabase()
    _run(_build(ROWS, db), orchestrator)
    expected = _build(ROWS, InMemoryArrowDatabase())
    _run(expected, orchestrator)
    CALLS.clear()
    before = _snapshot(db)

    job = _build(ROWS, db)
    summary = _run(job, orchestrator, load_only=True)

    assert CALLS == []
    assert _snapshot(db) == before
    assert job.nodes["rowwise"].as_table().num_rows == 6
    assert _totals(job) == _totals(expected)
    assert summary.load_only
    rowwise = summary.nodes["rowwise"]
    assert (rowwise.computed, rowwise.reused, rowwise.not_stored) == (0, 6, 0)
    assert rowwise.inputs_incomplete == ()
    assert summary.nodes["summed"].inputs_incomplete == ()


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_inputs_without_a_stored_result_are_skipped_and_counted(
    orchestrator: str,
) -> None:
    """The store holds a 3-row run; the source now has 6 rows."""
    db = InMemoryArrowDatabase()
    _run(_build(ROWS[:3], db), orchestrator)
    CALLS.clear()
    before = _snapshot(db)

    job = _build(ROWS, db)
    summary = _run(job, orchestrator, load_only=True)

    assert CALLS == []
    assert _snapshot(db) == before
    assert sorted(r["k"] for r in job.nodes["rowwise"].as_table().to_pylist()) == [
        0,
        1,
        2,
    ]
    rowwise = summary.nodes["rowwise"]
    assert (rowwise.computed, rowwise.reused, rowwise.not_stored) == (0, 3, 3)
    # The groups below hold only the loaded rows: those of the earlier 3-row run, whose
    # totals are stored. The summary says their inputs are incomplete.
    assert _totals(job) == [(0, 22), (1, 11)]
    summed = summary.nodes["summed"]
    assert (summed.reused, summed.not_stored) == (2, 0)
    assert summed.inputs_incomplete == ("rowwise",)
    text = repr(summary)
    assert "rowwise: 6 rows: 3 loaded, 3 not stored (skipped)" in text
    assert "summed: 2 rows: 2 loaded (inputs incomplete: rowwise)" in text


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_load_only_run_over_an_empty_store_fills_nothing_and_writes_nothing(
    orchestrator: str,
) -> None:
    db = InMemoryArrowDatabase()
    job = _build(ROWS, db)
    summary = _run(job, orchestrator, load_only=True)
    assert CALLS == []
    assert _snapshot(db) == {}
    for label in ("rowwise", "grouped", "summed"):
        assert job.nodes[label].as_table().num_rows == 0, label
    assert summary.nodes["rowwise"].not_stored == 6
    assert summary.nodes["summed"].reused == 0


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_load_only_run_delivers_no_side_effect_and_passes_the_rows_on(
    orchestrator: str,
) -> None:
    db = InMemoryArrowDatabase()
    _run(_build(ROWS, db, with_tap=True), orchestrator)
    assert len(DELIVERED) == 6
    DELIVERED.clear()
    CALLS.clear()
    before = _snapshot(db)

    job = _build(ROWS, db, with_tap=True)
    _run(job, orchestrator, load_only=True)

    assert DELIVERED == []
    assert CALLS == []
    assert _snapshot(db) == before
    assert _totals(job) == [(0, 63), (1, 93)]


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_log_operator_writes_nothing_in_a_load_only_run(orchestrator: str) -> None:
    db = InMemoryArrowDatabase()
    _run(_build(ROWS[:3], db, cache_mode=CacheMode.LOG), orchestrator)
    before = _snapshot(db)

    job = _build(ROWS[:3], db, cache_mode=CacheMode.LOG)
    _run(job, orchestrator, load_only=True)

    assert _snapshot(db) == before
    assert job.nodes["grouped"].as_table().num_rows == 2


def test_run_async_takes_load_only() -> None:
    db = InMemoryArrowDatabase()
    _run(_build(ROWS, db), "sync")
    CALLS.clear()
    job = _build(ROWS, db)
    summary = asyncio.run(job.run_async(load_only=True))
    assert CALLS == []
    assert summary.load_only
    assert job.nodes["rowwise"].as_table().num_rows == 6


def test_load_only_is_keyword_only() -> None:
    job = _build(ROWS, InMemoryArrowDatabase())
    with pytest.raises(TypeError):
        job.run(None, None, True, 100, True)  # pyright: ignore[reportCallIssue]  # load_only is keyword-only


def test_a_full_run_summary_is_not_load_only() -> None:
    summary = _build(ROWS, InMemoryArrowDatabase()).run()
    assert not summary.load_only
    assert summary.nodes["rowwise"].not_stored == 0
    assert summary.nodes["rowwise"].inputs_incomplete == ()
