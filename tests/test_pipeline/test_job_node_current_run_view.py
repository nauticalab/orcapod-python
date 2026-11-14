"""A job node shows the current run; ``node.as_source()`` shows what is stored (ITL-1095).

Developer, November 13, 2026: a job node itself (``as_table``, ``iter_data``, the dataframe
methods, ``flow``, ``head``, the preview, ``run``) is the current run view; ``as_source()``
is the view of everything stored for the node. Before a run in this process a node is empty
and stale, whatever its store holds; a run fills it.

One store holds the results of two earlier runs (6 source rows, then 3). A fresh job over
that store, and a job loaded from a saved file, read empty from every node until they run.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.streams.view import ViewStatus
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import DictSource
from orcapod.types import CacheMode

CALLS: list[str] = []


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


ROWS = [{"g": i % 2, "k": i, "x": 10 * i} for i in range(6)]
ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}
JOB_NODES = ("rowwise", "grouped", "summed")


@pytest.fixture(autouse=True)
def _reset_calls() -> None:
    CALLS.clear()


def _build(
    data: list[dict[str, int]],
    db: Any,
    cache_mode: CacheMode | None = None,
) -> PipelineJob:
    """``rowwise`` (function) -> ``grouped`` (GroupBy operator) -> ``summed`` (function)."""
    src = DictSource(data, tag_columns=["g", "k"], source_id="s")
    job = PipelineJob(name="j", database=db)
    with job:
        rowwise: Any = inc.pod(src, label="rowwise")
        grouped = rowwise.group_by("g", label="grouped")
        total.pod(grouped, label="summed")
    if cache_mode is not None:
        job.nodes["grouped"].attach_databases(
            pipeline_database=db.at("j"), cache_mode=cache_mode
        )
    return job


def _populated_store(
    orchestrator: str = "sync", cache_mode: CacheMode | None = None
) -> InMemoryArrowDatabase:
    """A store holding the results of a run over 6 rows and a run over 3 rows."""
    db = InMemoryArrowDatabase()
    for data in (ROWS, ROWS[:3]):
        _build(data, db, cache_mode).run(orchestrator=ORCHESTRATORS[orchestrator]())
    CALLS.clear()
    return db


def _assert_empty(node: Any, columns: list[str]) -> None:
    """Every read of ``node`` returns zero rows with the node's columns."""
    table = node.as_table()
    assert table.num_rows == 0
    assert table.column_names == columns
    assert list(node.iter_data()) == []
    assert node.as_polars_df().height == 0
    assert node.head().table.num_rows == 0
    assert node.head().table.column_names == columns


def _assert_points_to_run_and_stored_view(node: Any) -> None:
    """The head of a node that has not run says so and names both ways in."""
    head = node.head()
    assert head.status is ViewStatus.NOT_COMPUTED
    header = head.header()
    assert "not run" in header
    assert "run()" in header
    assert "as_source()" in header
    assert "not run" in node._repr_html_()


class TestFreshJobOverAPopulatedStore:
    """A job built over a store that holds results reads empty until it runs."""

    @pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
    def test_every_read_of_every_node_is_empty_before_a_run(
        self, orchestrator: str
    ) -> None:
        db = _populated_store(orchestrator)
        job = _build(ROWS[:3], db)
        expected_columns = {
            label: _build(ROWS[:3], db).nodes[label].as_table().column_names
            for label in JOB_NODES
        }
        for label in JOB_NODES:
            node = job.nodes[label]
            _assert_empty(node, expected_columns[label])
            _assert_points_to_run_and_stored_view(node)
        assert CALLS == []

    def test_the_stored_view_shows_every_stored_record(self) -> None:
        db = _populated_store()
        job = _build(ROWS[:3], db)
        # Both earlier runs' rows: 6 rowwise results, 2 + 2 group totals.
        assert job.nodes["rowwise"].as_source().as_table().num_rows == 6
        assert job.nodes["summed"].as_source().as_table().num_rows == 4
        assert job.nodes["rowwise"].as_table().num_rows == 0

    def test_every_node_of_a_fresh_job_is_stale(self) -> None:
        job = _build(ROWS[:3], _populated_store())
        for label in JOB_NODES:
            assert job.nodes[label].is_stale, label

    @pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
    def test_a_run_fills_the_nodes_with_this_runs_rows(self, orchestrator: str) -> None:
        db = _populated_store(orchestrator)
        job = _build(ROWS[:3], db)
        job.run(orchestrator=ORCHESTRATORS[orchestrator]())
        assert CALLS == []  # every result was stored
        assert job.nodes["rowwise"].as_table().num_rows == 3
        assert sorted(
            (r["g"], r["total"]) for r in job.nodes["summed"].as_table().to_pylist()
        ) == [(0, 22), (1, 11)]
        for label in JOB_NODES:
            assert not job.nodes[label].is_stale, label
            assert job.nodes[label].head().status is ViewStatus.ROWS, label


@pytest.mark.parametrize("mode", [CacheMode.OFF, CacheMode.LOG, CacheMode.REPLAY])
def test_an_operator_node_reads_empty_before_a_run_in_every_cache_mode(
    mode: CacheMode,
) -> None:
    """REPLAY included: before a run it no longer shows every logged run."""
    # LOG runs fill the operator cache, so REPLAY has stored rows to (not) show.
    db = _populated_store(cache_mode=CacheMode.LOG)
    job = _build(ROWS[:3], db, cache_mode=mode)
    node = job.nodes["grouped"]
    _assert_empty(node, ["g", "y"])
    _assert_points_to_run_and_stored_view(node)
    assert node.is_stale


class TestAfterARun:
    """What empties a node that held a run: clearing it, or a change upstream."""

    @pytest.mark.parametrize("label", list(JOB_NODES))
    def test_clear_cache_empties_the_node_and_makes_it_stale(self, label: str) -> None:
        job = _build(ROWS[:3], InMemoryArrowDatabase())
        job.run()
        node = job.nodes[label]
        columns = node.as_table().column_names
        node.clear_cache()
        assert node.is_stale
        _assert_empty(node, columns)

    def test_a_rebound_source_empties_its_dependent_nodes(self) -> None:
        job = _build(ROWS[:3], InMemoryArrowDatabase())
        job.run()
        source_name = next(iter(job.sources))  # the one source slot
        job.bind(
            sources={
                source_name: DictSource(ROWS, tag_columns=["g", "k"], source_id="s")
            }
        )
        for label in JOB_NODES:
            node = job.nodes[label]
            assert node.is_stale, label
            assert node.as_table().num_rows == 0, label
            assert node.head().status is ViewStatus.NOT_COMPUTED, label


def test_a_loaded_job_reads_empty_until_it_runs(tmp_path: Path) -> None:
    """A loaded job's nodes are empty and do not raise; ``as_source()`` reads the store."""
    db = InMemoryArrowDatabase()
    job = _build(ROWS, db)
    job.run()
    job.save(tmp_path / "job.json")
    loaded = PipelineJob.load(tmp_path / "job.json", database=db)
    for label in JOB_NODES:
        node = loaded.nodes[label]
        assert node.as_table().num_rows == 0, label
        assert list(node.iter_data()) == [], label
        assert node.head().status is ViewStatus.NOT_COMPUTED, label
        assert "not run" in node.head().header(), label
    assert loaded.nodes["rowwise"].as_source().as_table().num_rows == 6
