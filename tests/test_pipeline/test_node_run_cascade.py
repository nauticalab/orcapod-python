"""``node.run()`` runs the job nodes it depends on, by their state (ITL-1095).

Developer, November 13, 2026: "given that the initially loaded job is empty, I think it's
fair that node.run will cascade node.run on all nodes it depends on recursively.
Critically, node.run(load_only=True) would trigger the upstream node.run also with
load_only=True." And: "if it's not stale, it shouldn't even trigger a run and simply return
what's in the stream cached already." A node filled by a load-only run has its own status
and is never treated as complete by a full run.

The cascade runs the node's ancestors and the node on the synchronous orchestrator, as
``job.run()`` runs that sub-graph: same rows, same stored records, same counts
(``node.last_run``), and the same end-of-run commit.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import Join
from orcapod.core.streams import ViewStatus
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import RowFailuresWarning
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


@function_pod(output_keys="z")
def dbl(y: int) -> int:
    """Return ``2 * y``."""
    CALLS.append("dbl")
    return 2 * y


@function_pod(output_keys="w")
def neg(y: int) -> int:
    """Return ``-y``."""
    CALLS.append("neg")
    return -y


@function_pod(output_keys="y")
def fails_on_20(x: int) -> int:
    """Return ``x + 1``; raise for ``x == 20``."""
    if x == 20:
        raise ValueError("x is 20")
    return x + 1


def _deliver(y: int, ctx: Any) -> None:
    """A side effect: record the delivered value."""
    DELIVERED.append(y)


ROWS = [{"g": i % 2, "k": i, "x": 10 * i} for i in range(6)]


@pytest.fixture(autouse=True)
def _reset() -> None:
    CALLS.clear()
    DELIVERED.clear()


def _source(data: list[dict[str, int]]) -> DictSource:
    return DictSource(data, tag_columns=["g", "k"], source_id="s")


def _build(
    data: list[dict[str, int]],
    db: Any,
    *,
    cache_mode: CacheMode | None = None,
    with_tap: bool = False,
) -> PipelineJob:
    """``rowwise`` -> [``tap``] -> ``grouped`` (GroupBy) -> ``summed``."""
    job = PipelineJob(name="j", database=db)
    with job:
        rows: Any = inc.pod(_source(data), label="rowwise")
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


def _snapshot(db: Any) -> dict[tuple[str, ...], int]:
    db.flush()
    counts: dict[tuple[str, ...], int] = {}
    for path in db.list_table_paths():
        records = db.get_all_records(path)
        counts[path] = 0 if records is None else records.num_rows
    return counts


def _totals(table: Any) -> list[tuple[int, int]]:
    return sorted((r["g"], r["total"]) for r in table.to_pylist())


class TestCascade:
    """``node.run()`` first runs every job node it depends on, each once."""

    def test_a_node_of_a_fresh_job_runs_its_inputs_first(self) -> None:
        job = _build(ROWS, InMemoryArrowDatabase())
        view = job.nodes["summed"].run()
        assert sorted(CALLS) == ["inc"] * 6 + ["total"] * 2
        assert _totals(view.as_table()) == [(0, 63), (1, 93)]
        # Every node of the cascade holds its run, as after job.run().
        for label in ("rowwise", "grouped", "summed"):
            assert job.nodes[label].head().status is ViewStatus.ROWS, label
        expected = _build(ROWS, InMemoryArrowDatabase())
        expected.run()
        for label in ("rowwise", "summed"):
            assert (
                job.nodes[label]
                .as_table()
                .sort_by("g")
                .equals(expected.nodes[label].as_table().sort_by("g"))
            ), label

    def test_the_cascade_reports_its_counts_on_the_node(self) -> None:
        db = InMemoryArrowDatabase()
        _build(ROWS[:3], db).run()
        CALLS.clear()
        job = _build(ROWS, db)
        _ = job.nodes["summed"].run()
        summary = job.nodes["summed"].last_run
        assert summary is not None
        assert not summary.load_only
        rowwise = summary.nodes["rowwise"]
        assert (rowwise.computed, rowwise.reused) == (3, 3)
        assert summary.nodes["summed"].computed == 2
        assert "summed" in repr(summary)

    def test_a_diamond_runs_each_ancestor_once(self) -> None:
        job = PipelineJob(name="d", database=InMemoryArrowDatabase())
        with job:
            top = inc.pod(_source(ROWS), label="top")
            left = dbl.pod(top, label="left")
            right = neg.pod(top, label="right")
            Join()(left, right, label="joined")
        view = job.nodes["joined"].run()
        assert view.as_table().num_rows == 6
        assert sorted(CALLS) == sorted(["inc"] * 6 + ["dbl"] * 6 + ["neg"] * 6)

    def test_failed_rows_warn_as_a_job_run_does(self) -> None:
        job = PipelineJob(name="f", database=InMemoryArrowDatabase())
        with job:
            fails_on_20.pod(_source(ROWS), label="node")
        with pytest.warns(
            RowFailuresWarning, match="1 of 6 rows failed in node 'node'"
        ):
            view = job.nodes["node"].run()
        assert view.as_table().num_rows == 5


class TestState:
    """A node that holds a current run is not run again; a stale one is."""

    def test_a_second_run_of_an_unchanged_node_does_nothing(self) -> None:
        db = InMemoryArrowDatabase()
        job = _build(ROWS, db)
        first = job.nodes["summed"].run()
        CALLS.clear()
        with (
            mock.patch.object(
                InMemoryArrowDatabase,
                "get_all_records",
                side_effect=AssertionError("store read"),
            ),
            mock.patch.object(
                InMemoryArrowDatabase,
                "add_records",
                side_effect=AssertionError("store write"),
            ),
        ):
            second = job.nodes["summed"].run()
        assert CALLS == []
        assert second.as_table().equals(first.as_table())

    def test_a_held_ancestor_is_not_run_again(self) -> None:
        job = _build(ROWS, InMemoryArrowDatabase())
        _ = job.nodes["rowwise"].run()
        stamped = job.nodes["rowwise"].last_modified
        CALLS.clear()
        _ = job.nodes["summed"].run()
        assert job.nodes["rowwise"].last_modified == stamped
        assert sorted(CALLS) == ["total", "total"]

    def test_a_node_runs_again_after_its_source_changed(self) -> None:
        job = _build(ROWS[:3], InMemoryArrowDatabase())
        _ = job.nodes["summed"].run()
        CALLS.clear()
        job.bind(sources={next(iter(job.sources)): _source(ROWS)})
        assert job.nodes["summed"].is_stale
        view = job.nodes["summed"].run()
        assert CALLS.count("inc") == 3
        assert _totals(view.as_table()) == [(0, 63), (1, 93)]


class TestLoadOnly:
    """``node.run(load_only=True)`` loads through the cascade; its rows have a status."""

    def test_a_load_only_cascade_computes_and_writes_nothing(self) -> None:
        db = InMemoryArrowDatabase()
        _build(ROWS[:3], db).run()
        CALLS.clear()
        before = _snapshot(db)
        job = _build(ROWS, db)
        view = job.nodes["summed"].run(load_only=True)
        assert CALLS == []
        assert _snapshot(db) == before
        assert _totals(view.as_table()) == [(0, 22), (1, 11)]
        summary = job.nodes["summed"].last_run
        assert summary is not None and summary.load_only
        assert summary.nodes["rowwise"].not_stored == 3
        rowwise_head = job.nodes["rowwise"].head()
        assert rowwise_head.status is ViewStatus.LOADED_ONLY
        assert "3 loaded, 3 inputs without a stored result" in rowwise_head.header()
        summed_head = job.nodes["summed"].head()
        assert summed_head.status is ViewStatus.LOADED_ONLY
        assert "inputs incomplete: rowwise" in summed_head.header()

    def test_a_full_run_after_a_load_only_run_computes_what_is_missing(self) -> None:
        db = InMemoryArrowDatabase()
        _build(ROWS[:3], db).run()
        CALLS.clear()
        job = _build(ROWS, db)
        _ = job.nodes["summed"].run(load_only=True)
        assert CALLS == []
        view = job.nodes["summed"].run()
        assert CALLS.count("inc") == 3
        assert _totals(view.as_table()) == [(0, 63), (1, 93)]
        assert job.nodes["rowwise"].head().status is ViewStatus.ROWS

    def test_a_load_only_run_of_a_load_only_node_returns_what_it_holds(self) -> None:
        db = InMemoryArrowDatabase()
        _build(ROWS[:3], db).run()
        job = _build(ROWS, db)
        first = job.nodes["summed"].run(load_only=True)
        stamped = job.nodes["rowwise"].last_modified
        second = job.nodes["summed"].run(load_only=True)
        assert job.nodes["rowwise"].last_modified == stamped
        assert second.as_table().equals(first.as_table())

    def test_a_load_only_cascade_uses_a_fully_run_ancestor(self) -> None:
        job = _build(ROWS, InMemoryArrowDatabase())
        _ = job.nodes["rowwise"].run()
        stamped = job.nodes["rowwise"].last_modified
        _ = job.nodes["summed"].run(load_only=True)
        assert job.nodes["rowwise"].last_modified == stamped

    def test_a_load_only_run_that_found_everything_stays_loaded_only(self) -> None:
        db = InMemoryArrowDatabase()
        _build(ROWS, db).run()
        job = _build(ROWS, db)
        _ = job.nodes["rowwise"].run(load_only=True)
        head = job.nodes["rowwise"].head()
        assert head.status is ViewStatus.LOADED_ONLY
        assert "6 loaded, 0 inputs without a stored result" in head.header()


class TestSideEffects:
    def test_a_side_effect_node_follows_the_state_rule(self) -> None:
        """Stale before a run; while it holds a current run nothing re-delivers it."""
        job = _build(ROWS, InMemoryArrowDatabase(), with_tap=True)
        tap = job.nodes["tap"]
        assert tap.is_stale
        _ = job.nodes["summed"].run()
        assert len(DELIVERED) == 6
        assert tap.holds_current_run()
        _ = job.nodes["summed"].run()
        _ = tap.run()
        assert len(DELIVERED) == 6
        tap.clear_cache()
        assert job.nodes["summed"].is_stale
        _ = job.nodes["summed"].run()
        assert len(DELIVERED) == 12

    def test_a_load_only_run_of_a_side_effect_node_holds_its_rows(self) -> None:
        db = InMemoryArrowDatabase()
        _build(ROWS, db, with_tap=True).run()
        DELIVERED.clear()
        job = _build(ROWS, db, with_tap=True)
        _ = job.nodes["summed"].run(load_only=True)
        tap = job.nodes["tap"]
        assert DELIVERED == []
        assert tap.as_table().num_rows == 6
        assert tap.holds_current_run(load_only=True)
        assert not tap.holds_current_run()

    def test_a_full_cascade_delivers_a_load_only_cascade_does_not(self) -> None:
        db = InMemoryArrowDatabase()
        job = _build(ROWS, db, with_tap=True)
        _ = job.nodes["summed"].run(load_only=True)
        assert DELIVERED == []
        _ = job.nodes["summed"].run()
        assert sorted(DELIVERED) == sorted(r["x"] + 1 for r in ROWS)


def test_the_cascade_commits_every_write(tmp_path: Path) -> None:
    """A store opened again through a new object holds every result and record."""
    path = tmp_path / "store"
    job = _build(ROWS, DeltaTableDatabase(path), cache_mode=CacheMode.LOG)
    _ = job.nodes["summed"].run()
    CALLS.clear()
    probe = _build(ROWS, DeltaTableDatabase(path), cache_mode=CacheMode.LOG)
    for label, rows in (("rowwise", 6), ("grouped", 2), ("summed", 2)):
        records = probe.nodes[label].get_all_records()
        assert records is not None and records.num_rows == rows, label
    again = _build(ROWS, DeltaTableDatabase(path))
    summary = again.run()
    assert CALLS == []
    assert summary.nodes["rowwise"].reused == 6
    assert summary.nodes["summed"].reused == 2


class TestFailedFinalCommit:
    """A run whose end-of-run commit fails leaves no node holding its rows."""

    @staticmethod
    def _flush_calls(run: Any) -> int:
        """How many flushes ``run()`` makes on an in-memory store."""
        calls = 0
        original = InMemoryArrowDatabase.flush

        def counting(self: Any) -> None:
            nonlocal calls
            calls += 1
            original(self)

        with mock.patch.object(InMemoryArrowDatabase, "flush", counting):
            run()
        return calls

    @staticmethod
    def _failing_last_flush(total_calls: int) -> Any:
        """A flush that fails on call number ``total_calls`` (the final commit)."""
        calls = 0
        original = InMemoryArrowDatabase.flush

        def flush(self: Any) -> None:
            nonlocal calls
            calls += 1
            if calls == total_calls:
                raise OSError("disk full")
            original(self)

        return mock.patch.object(InMemoryArrowDatabase, "flush", flush)

    @pytest.mark.parametrize("orchestrator", ["sync", "async"])
    def test_a_job_run_whose_commit_fails_leaves_every_node_empty(
        self, orchestrator: str
    ) -> None:
        from orcapod.errors import StoreWriteError
        from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator

        kinds = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}
        total = self._flush_calls(
            lambda: _build(ROWS, InMemoryArrowDatabase()).run(
                orchestrator=kinds[orchestrator]()
            )
        )
        job = _build(ROWS, InMemoryArrowDatabase())
        with self._failing_last_flush(total), pytest.raises(StoreWriteError):
            job.run(orchestrator=kinds[orchestrator]())
        for label in ("rowwise", "grouped", "summed"):
            node = job.nodes[label]
            assert node.as_table().num_rows == 0, label
            assert node.head().status is ViewStatus.NOT_COMPUTED, label
            assert node.is_stale, label
        job.run(orchestrator=kinds[orchestrator]())
        assert _totals(job.nodes["summed"].as_table()) == [(0, 63), (1, 93)]

    def test_a_cascade_whose_commit_fails_leaves_every_node_empty(self) -> None:
        from orcapod.errors import StoreWriteError

        total = self._flush_calls(
            lambda: _build(ROWS, InMemoryArrowDatabase()).nodes["summed"].run()
        )
        job = _build(ROWS, InMemoryArrowDatabase())
        with self._failing_last_flush(total), pytest.raises(StoreWriteError):
            job.nodes["summed"].run()
        for label in ("rowwise", "grouped", "summed"):
            assert job.nodes[label].as_table().num_rows == 0, label
            assert job.nodes[label].is_stale, label
        view = job.nodes["summed"].run()
        assert _totals(view.as_table()) == [(0, 63), (1, 93)]
