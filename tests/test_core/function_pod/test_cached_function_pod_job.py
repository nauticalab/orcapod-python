"""A pod-cached function pod in a ``PipelineJob``, and its table (ITL-988).

- The job records the pod's source and function node, like an uncached pod, and runs,
  saves and loads it (in memory and on Delta, with the sync and the async orchestrator).
- The pod cache is honoured in a job: a row is computed once.
- The plain table of a ``CachedFunctionPod`` stream has the columns of an uncached pod's
  table whether a row was computed or served from the cache; meta columns come only on
  request, and then the same ones in both states.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from orcapod import PipelineJob
from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import Join
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.sources import DictSource
from orcapod.types import ColumnConfig

ORCHESTRATORS: dict[str, Callable[[], Any]] = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}
ROWS = [{"k": 1, "x": 2, "y": 3}, {"k": 2, "x": 5, "y": 5}]


def make_source(rows: list[dict[str, int]] | None = None) -> DictSource:
    return DictSource(rows or ROWS, tag_columns=["k"], source_id="s")


class Counter:
    """An ``add`` function that records the ``x`` of every call."""

    def __init__(self) -> None:
        self.calls: list[int] = []

    def fn(self) -> Callable[[int, int], int]:
        def add(x: int, y: int) -> int:
            self.calls.append(x)
            return x + y

        return add

    def pod(self) -> FunctionPod:
        return FunctionPod.from_fn(self.fn(), output_keys="total")


def make_db(kind: str, tmp_path: Path, name: str):
    if kind == "memory":
        return InMemoryArrowDatabase()
    return DeltaTableDatabase(tmp_path / name)


def columns(table) -> list[tuple[str, str]]:
    return [(f.name, str(f.type)) for f in table.schema]


# -- the table ---------------------------------------------------------------------


@pytest.mark.parametrize("all_info", [False, True])
def test_table_columns_do_not_depend_on_cache_state(all_info: bool) -> None:
    counter = Counter()
    cached = CachedFunctionPod(
        function_pod=counter.pod(), result_database=InMemoryArrowDatabase()
    )
    computed = cached.process(make_source()).as_table(all_info=all_info)
    served = cached.process(make_source()).as_table(all_info=all_info)
    assert counter.calls == [2, 5]  # the second table came from the cache
    assert columns(computed) == columns(served)


def test_plain_table_equals_the_uncached_pods_table() -> None:
    counter = Counter()
    uncached = counter.pod().process(make_source()).as_table()
    cached = CachedFunctionPod(
        function_pod=counter.pod(), result_database=InMemoryArrowDatabase()
    )
    for _ in range(2):  # computed, then served
        table = cached.process(make_source()).as_table()
        assert columns(table) == columns(uncached)
        assert table.to_pylist() == uncached.to_pylist()


def test_meta_columns_only_when_asked_for_and_the_same_in_both_states() -> None:
    cached = CachedFunctionPod(
        function_pod=Counter().pod(), result_database=InMemoryArrowDatabase()
    )
    computed = cached.process(make_source()).as_table(columns={"meta": True})
    served = cached.process(make_source()).as_table(columns={"meta": True})
    assert columns(computed) == columns(served)
    assert [n for n, _ in columns(computed) if n.startswith("__")] == ["__computed"]
    assert computed.column("__computed").to_pylist() == [True, True]
    assert served.column("__computed").to_pylist() == [False, False]


@pytest.mark.parametrize("columns", [ColumnConfig(all_info=True), {"all_info": True}])
def test_all_info_in_the_column_config_keeps_the_meta_columns(columns: Any) -> None:
    cached = CachedFunctionPod(
        function_pod=Counter().pod(), result_database=InMemoryArrowDatabase()
    )
    table = cached.process(make_source()).as_table(columns=columns)
    assert "__computed" in table.column_names


# -- the job -----------------------------------------------------------------------


def run_job(pod, store, orchestrator: str) -> PipelineJob:
    job = PipelineJob(name="j", database=store)
    with job:
        pod.process(make_source(), label="add")
    job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    return job


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
@pytest.mark.parametrize("kind", ["memory", "delta"])
def test_cached_pod_is_recorded_and_run_like_an_uncached_pod(
    kind: str, orchestrator: str, tmp_path: Path
) -> None:
    counter = Counter()
    plain = run_job(counter.pod(), make_db(kind, tmp_path, "plain"), orchestrator)
    cached_pod = CachedFunctionPod(
        function_pod=counter.pod(), result_database=make_db(kind, tmp_path, "pod")
    )
    job = run_job(cached_pod, make_db(kind, tmp_path, "cached"), orchestrator)

    assert list(job.nodes) == list(plain.nodes) == ["s", "add"]
    assert job.nodes["add"].pipeline_hash() == plain.nodes["add"].pipeline_hash()
    assert job.nodes["add"].content_hash() == plain.nodes["add"].content_hash()
    assert columns(job.nodes["add"].as_table()) == columns(
        plain.nodes["add"].as_table()
    )
    assert (
        job.nodes["add"].as_table().to_pylist()
        == plain.nodes["add"].as_table().to_pylist()
    )


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
@pytest.mark.parametrize("kind", ["memory", "delta"])
def test_row_is_computed_once_and_stored_in_both_caches(
    kind: str, orchestrator: str, tmp_path: Path
) -> None:
    counter = Counter()
    pod_db = make_db(kind, tmp_path, "pod")
    cached_pod = CachedFunctionPod(function_pod=counter.pod(), result_database=pod_db)
    store = make_db(kind, tmp_path, "cached")

    run_job(cached_pod, store, orchestrator)
    assert sorted(counter.calls) == [2, 5]
    pod_rows = cached_pod.get_all_cached_outputs()
    assert pod_rows is not None and pod_rows.num_rows == 2
    assert sorted(counter.calls) == [2, 5]

    # A second run over the same database computes nothing.
    run_job(cached_pod, store, orchestrator)
    assert sorted(counter.calls) == [2, 5]


def test_row_in_the_pod_cache_is_not_recomputed_by_a_job() -> None:
    counter = Counter()
    cached_pod = CachedFunctionPod(
        function_pod=counter.pod(), result_database=InMemoryArrowDatabase()
    )
    cached_pod.process(make_source(ROWS[:1])).as_table()  # fills the pod cache
    counter.calls.clear()
    run_job(cached_pod, InMemoryArrowDatabase(), "sync")
    assert counter.calls == [5]


def test_inputs_are_joined_once() -> None:
    counter = Counter()
    cached_pod = CachedFunctionPod(
        function_pod=counter.pod(), result_database=InMemoryArrowDatabase()
    )
    left = DictSource([{"k": 1, "x": 2}], tag_columns=["k"], source_id="l")
    right = DictSource([{"k": 1, "y": 3}], tag_columns=["k"], source_id="r")
    joins: list[int] = []
    original = Join.process

    def counting(self, *streams, **kwargs):
        joins.append(len(streams))
        return original(self, *streams, **kwargs)

    # Counting the calls of the join is the test: the public result is the same.
    Join.process = counting  # type: ignore[method-assign]
    try:
        stream = cached_pod.process(left, right)
    finally:
        Join.process = original  # type: ignore[method-assign]
    assert joins == [2]
    assert stream.as_table().to_pylist() == [{"k": 1, "total": 5}]


# -- save and load -----------------------------------------------------------------


def saved(job: PipelineJob, path: Path) -> dict[str, Any]:
    job.save(path)
    return json.loads(path.read_text())


@pytest.mark.parametrize("kind", ["memory", "delta"])
def test_saved_job_equals_the_uncached_pods_and_loads(
    kind: str, tmp_path: Path
) -> None:
    counter = Counter()
    plain_store = make_db(kind, tmp_path, "plain")
    plain = run_job(counter.pod(), plain_store, "sync")
    store = make_db(kind, tmp_path, "cached")
    cached_pod = CachedFunctionPod(
        function_pod=counter.pod(), result_database=make_db(kind, tmp_path, "pod")
    )
    job = run_job(cached_pod, store, "sync")

    plain_saved = saved(plain, tmp_path / "plain.json")
    cached_saved = saved(job, tmp_path / "cached.json")
    # Fields that differ between any two saves: the run id and the database's location.
    for doc in (plain_saved, cached_saved):
        doc["run"].pop("run_id")
        doc["bindings"].pop("database")
    assert cached_saved == plain_saved

    # A loaded job's stored results are read through the stored view (its nodes are
    # empty until it runs, ITL-1095); the pod cache is not part of it.
    loaded = PipelineJob.load(tmp_path / "cached.json", database=store)
    assert loaded.nodes["add"].as_source().as_table().to_pylist() == [
        {"k": 1, "total": 5},
        {"k": 2, "total": 10},
    ]
