"""Function nodes inside ``PipelineJob.run`` commit their writes in batches (ITL-919).

``PipelineJob.run(commit_every_rows=N)`` makes each function node commit its results and
pipeline records every N rows and when it finishes, instead of after every row. These
tests check, on a ``DeltaTableDatabase`` whose table history counts commits:

- the commit count (one per N rows per table, appends only),
- the option's validation,
- that a node or cached pod used on its own still commits after every row,
- a failed commit: ``StoreWriteError`` naming the node and no row, earlier batches kept,
- that a failing node commits its ephemeral results before the run's error propagates,
- that each row's base entry id is computed once,
- that a node's records read right after the run are all there (ITL-930).
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import deltalake
import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.nodes.function_node import FunctionJobNode
from orcapod.contexts import get_default_context
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.databases.extension_aware_database import ExtensionAwareDatabase
from orcapod.databases.storage_utils import delta_to_pyarrow_dataset
from orcapod.errors import StoreWriteError
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.databases.utils import as_table_committing
from orcapod.sources import DictSource
from orcapod.types import NodeConfig

ORCHESTRATORS: dict[str, Callable[..., Any]] = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}


@function_pod(output_keys="y")
def double(x: int) -> int:
    """Return twice ``x``."""
    return 2 * x


@function_pod(output_keys="y")
def double_until_7(x: int) -> int:
    """Return twice ``x``, failing for ``x == 7``."""
    if x == 7:
        raise ValueError("x is 7")
    return 2 * x


_DOUBLE_CALLS: list[int] = []


@function_pod(output_keys="y")
def counted_double(x: int) -> int:
    """Return twice ``x``, logging the call."""
    _DOUBLE_CALLS.append(x)
    return 2 * x


def build_job(store: Any, n_rows: int, pod: Any = double) -> PipelineJob:
    """A job with one function node ``dbl`` over ``n_rows`` rows."""
    job = PipelineJob(name="batched", database=store)
    with job:
        source = DictSource(
            data=[{"k": i, "x": i} for i in range(n_rows)],
            tag_columns=["k"],
            source_id="src",
        )
        pod.pod(source, label="dbl")
    return job


def table_dirs(root: Path) -> dict[str, Path]:
    """The Delta tables under ``root``, keyed ``result`` or ``pipeline``."""
    tables = {log.parent for log in root.rglob("_delta_log")}
    by_kind = {
        ("result" if "_result" in t.relative_to(root).parts else "pipeline"): t
        for t in tables
    }
    assert len(by_kind) == len(tables) == 2, sorted(tables)
    return by_kind


def operations(table: Path) -> list[str]:
    """The table's commit operations, oldest first."""
    history = deltalake.DeltaTable(str(table)).history()
    return [str(entry["operation"]) for entry in reversed(history)]


def read_column(table: Path, column: str) -> list[Any]:
    """The committed values of ``column`` in ``table``, read from a fresh handle."""
    dataset = delta_to_pyarrow_dataset(deltalake.DeltaTable(str(table)))
    return dataset.to_table(columns=[column])[column].to_pylist()


def stored_rows(table: Path) -> int:
    """The number of rows committed to ``table``, read from a fresh handle."""
    return delta_to_pyarrow_dataset(deltalake.DeltaTable(str(table))).count_rows()


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize(
    ("n_rows", "every", "commits"), [(250, 100, 3), (250, None, 3), (40, 999, 1)]
)
def test_each_table_is_committed_once_per_batch(
    tmp_path: Path, orchestrator: str, n_rows: int, every: int | None, commits: int
) -> None:
    """250 rows at 100 per commit (the default) give 3 commits per table, all appends."""
    job = build_job(DeltaTableDatabase(tmp_path), n_rows)
    if every is None:
        job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    else:
        job.run(orchestrator=ORCHESTRATORS[orchestrator](), commit_every_rows=every)
    for kind, table in table_dirs(tmp_path).items():
        assert operations(table) == ["WRITE"] * commits, kind
        assert stored_rows(table) == n_rows, kind
    assert job.nodes["dbl"].as_table().num_rows == n_rows


@pytest.mark.parametrize("value", [0, -1, True, 1.5, "10", None])
def test_commit_every_rows_must_be_a_positive_integer(
    tmp_path: Path, value: Any
) -> None:
    """Anything but a positive ``int`` is refused before the run starts."""
    job = build_job(DeltaTableDatabase(tmp_path), 3)
    with pytest.raises(
        ValueError, match="commit_every_rows must be a positive integer"
    ):
        job.run(commit_every_rows=value)
    assert not list(tmp_path.rglob("_delta_log"))


def test_a_node_run_on_its_own_commits_every_row(tmp_path: Path) -> None:
    """Outside ``PipelineJob.run`` a node's results are committed row by row."""
    job = build_job(DeltaTableDatabase(tmp_path), 5)
    job.nodes["dbl"].run()
    result_table = table_dirs(tmp_path)["result"]
    assert operations(result_table) == ["WRITE"] * 5


def test_a_cached_pod_used_on_its_own_commits_every_row(tmp_path: Path) -> None:
    """A ``CachedFunctionPod`` used directly commits each stored result."""
    cached = CachedFunctionPod(
        double.pod,  # pyright: ignore[reportArgumentType]  # a decorated pod satisfies the protocol at run time
        result_database=DeltaTableDatabase(tmp_path),
    )
    source = DictSource(
        data=[{"k": i, "x": i} for i in range(4)], tag_columns=["k"], source_id="src"
    )
    for tag, data in source.iter_data():
        cached.process_data(tag, data)
    (table,) = {log.parent for log in tmp_path.rglob("_delta_log")}
    assert operations(table) == ["WRITE"] * 4


class FailingCommits:
    """A ``DeltaTableDatabase`` whose commits fail after the first ``ok`` succeed.

    A node's commit flushes its result table (``flush_batch``) and then the store
    (``flush``); every view shares the counts.
    """

    def __init__(self, inner: DeltaTableDatabase, state: dict[str, int]) -> None:
        """Wrap ``inner``; ``state["ok"]`` commit calls succeed, later ones fail."""
        self._inner = inner
        self._state = state

    def at(self, *path_components: str) -> FailingCommits:
        """A view sharing the commit counts."""
        return FailingCommits(self._inner.at(*path_components), self._state)

    def _commit(self, commit: Callable[[], None]) -> None:
        self._state["calls"] += 1
        if self._state["calls"] > self._state["ok"]:
            raise OSError("disk unavailable while committing")
        commit()

    def flush(self) -> None:
        """Commit every pending write, unless commits fail by now."""
        self._commit(self._inner.flush)

    def flush_batch(self, record_path: tuple[str, ...]) -> None:
        """Commit one table's pending writes, unless commits fail by now."""
        self._commit(lambda: self._inner.flush_batch(record_path))

    def __getattr__(self, name: str) -> Any:
        """Delegate everything else."""
        return getattr(self._inner, name)


@pytest.mark.parametrize("error_policy", ["continue", "fail_fast"])
@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_failed_commit_names_the_node_and_keeps_earlier_batches(
    tmp_path: Path, orchestrator: str, error_policy: str
) -> None:
    """The second batch's commit fails: the run raises, the first batch is kept."""
    # The run first commits what was pending before it (one call); the node's first
    # commit is two calls: the result table, then the store.
    store = FailingCommits(DeltaTableDatabase(tmp_path), {"calls": 0, "ok": 3})
    job = build_job(store, 250)
    with pytest.raises(StoreWriteError) as info:
        job.run(
            orchestrator=ORCHESTRATORS[orchestrator](error_policy=error_policy),
            commit_every_rows=100,
        )
    err = info.value
    assert err.flush is True
    assert err.node_label == "dbl"
    assert err.tag is None
    assert "node 'dbl'" in str(err) and "for row" not in str(err)
    assert isinstance(err.__cause__, OSError)
    # The job's flush after the failure retried the batch and failed again.
    assert any("Flushing pending writes" in n for n in getattr(err, "__notes__", []))
    tables = table_dirs(tmp_path)
    for kind, table in tables.items():
        assert operations(table) == ["WRITE"], kind
    assert stored_rows(tables["pipeline"]) == 100
    if orchestrator == "sync":
        assert stored_rows(tables["result"]) == 100
    else:
        # Results go first: results of rows still on their way to being recorded
        # when the first batch was committed are stored too.
        assert 100 <= stored_rows(tables["result"]) < 150


def ephemeral_rows(path: Path) -> int:
    """Rows committed to the ephemeral store at ``path`` (read afresh)."""
    (table,) = {log.parent for log in path.rglob("_delta_log")}
    return stored_rows(table)


def test_a_node_failing_on_a_row_commits_its_ephemeral_results(
    tmp_path: Path,
) -> None:
    """A function error under ``fail_fast``: rows before it are in the ephemeral store."""
    job = build_job(DeltaTableDatabase(tmp_path / "store"), 10, pod=double_until_7)
    job.set_ephemeral_store(DeltaTableDatabase(tmp_path / "ephemeral"))
    job.apply_node_config(NodeConfig(is_result_ephemeral=True))
    # The sync orchestrator only: the async one does not apply error_policy and
    # skips a row whose function raised.
    with pytest.raises(ValueError, match="x is 7"):
        job.run(
            orchestrator=SyncPipelineOrchestrator(error_policy="fail_fast"),
            commit_every_rows=100,
        )
    assert ephemeral_rows(tmp_path / "ephemeral") == 7  # rows 0 to 6


class FailPipelineRecords:
    """A store whose pipeline-record writes fail after the first ``ok``."""

    def __init__(self, inner: Any, state: dict[str, int]) -> None:
        """Wrap ``inner``; every view shares ``state``."""
        self._inner = inner
        self._state = state

    def at(self, *path_components: str) -> FailPipelineRecords:
        """A view sharing the write count."""
        return FailPipelineRecords(self._inner.at(*path_components), self._state)

    def add_record(self, record_path: tuple[str, ...], *args: Any, **kwargs: Any):
        """Fail pipeline-record writes after the first ``ok``."""
        self._state["n"] += 1
        if self._state["n"] > self._state["ok"]:
            raise OSError("disk unavailable")
        return self._inner.add_record(record_path, *args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        """Delegate everything else."""
        return getattr(self._inner, name)


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_node_failing_on_a_store_write_commits_its_ephemeral_results(
    tmp_path: Path, orchestrator: str
) -> None:
    """A failed pipeline-record write: earlier rows' results are in the ephemeral store."""
    # With ephemeral results, the job store only receives pipeline records.
    store = FailPipelineRecords(
        DeltaTableDatabase(tmp_path / "store"), {"n": 0, "ok": 5}
    )
    job = build_job(store, 50)
    job.set_ephemeral_store(DeltaTableDatabase(tmp_path / "ephemeral"))
    job.apply_node_config(NodeConfig(is_result_ephemeral=True))
    with pytest.raises(StoreWriteError) as info:
        job.run(orchestrator=ORCHESTRATORS[orchestrator](), commit_every_rows=100)
    assert info.value.node_label == "dbl"
    committed = ephemeral_rows(tmp_path / "ephemeral")
    if orchestrator == "sync":
        assert committed == 6  # rows 0 to 5: row 5's record was the failed write
    else:
        assert committed >= 6  # results of rows computed concurrently too


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_base_entry_id_is_computed_once_per_row(
    orchestrator: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Each row's base entry id is computed once, not once per use."""
    # Counts calls of a public node method through a class-level spy; the test is
    # about how often the node computes the id.
    calls = {"n": 0}
    original = FunctionJobNode.compute_base_entry_id

    def counting(self: FunctionJobNode, tag: Any, input_data: Any) -> bytes:
        calls["n"] += 1
        return original(self, tag, input_data)

    monkeypatch.setattr(FunctionJobNode, "compute_base_entry_id", counting)
    job = build_job(InMemoryArrowDatabase(), 20)
    job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    assert calls["n"] == 20


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
@pytest.mark.parametrize("source_id", ["s", None])
def test_records_read_right_after_the_run_are_complete(
    tmp_path: Path, orchestrator: str, source_id: str | None
) -> None:
    """``get_all_records`` right after ``run`` returns every row (ITL-930)."""
    job = PipelineJob(name="itl930", database=DeltaTableDatabase(tmp_path))
    with job:
        source = DictSource(
            data=[{"k": "b", "x": 2}, {"k": "c", "x": 3}],
            tag_columns=["k"],
            source_id=source_id,
        )
        double.pod(source, label="double")
    job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    records = job.nodes["double"].get_all_records()
    assert records is not None and records.num_rows == 2
    assert job.nodes["double"].as_table().num_rows == 2


@pytest.mark.parametrize("store", ["in_memory", "delta"])
@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_rerun_of_an_ephemeral_node_recomputes_and_records_every_row(
    tmp_path: Path, orchestrator: str, store: str
) -> None:
    """A rerun with a fresh ephemeral store recomputes each row (ITL-923).

    The first run's results are gone, so each row is computed again and gets a
    second pipeline record (recomputation index 1); the node reads 5 rows.
    """

    def make_store() -> Any:
        return (
            DeltaTableDatabase(tmp_path / "store")
            if store == "delta"
            else shared_in_memory
        )

    shared_in_memory = InMemoryArrowDatabase()
    calls_before = len(_DOUBLE_CALLS)
    jobs: list[PipelineJob] = []
    for _ in range(2):
        job = PipelineJob(name="ephemeral", database=make_store())
        jobs.append(job)
        with job:
            source = DictSource(
                data=[{"k": i, "x": i} for i in range(5)],
                tag_columns=["k"],
                source_id="src",
            )
            counted_double.pod(source, label="dbl")
        job.apply_node_config(NodeConfig(is_result_ephemeral=True))
        job.set_ephemeral_store(InMemoryArrowDatabase())
        job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    assert len(_DOUBLE_CALLS) - calls_before == 10
    node = jobs[-1].nodes["dbl"]
    assert node.as_table().num_rows == 5
    records = node.get_all_records()
    assert records is not None and records.num_rows == 5
    if store == "delta":
        (table,) = {log.parent for log in (tmp_path / "store").rglob("_delta_log")}
        index = read_column(table, "__pipeline_recomputation_index")
        assert sorted(index) == [0] * 5 + [1] * 5


@pytest.mark.parametrize(
    ("batch_size", "every", "refused"),
    [(None, 1000, True), (None, 999, False), (50, 50, True), (50, 49, False)],
)
def test_commit_every_rows_must_be_below_the_delta_batch_size(
    tmp_path: Path, batch_size: int | None, every: int, refused: bool
) -> None:
    """At or above the store's ``batch_size`` the store would commit on its own."""
    store = (
        DeltaTableDatabase(tmp_path)
        if batch_size is None
        else DeltaTableDatabase(tmp_path, batch_size=batch_size)
    )
    job = build_job(store, 3)
    if refused:
        expected = rf"commit_every_rows={every} must be smaller than the store's batch_size={store.batch_size}"
        with pytest.raises(ValueError, match=expected):
            job.run(commit_every_rows=every)
        assert not list(tmp_path.rglob("_delta_log"))
    else:
        job.run(commit_every_rows=every)
        assert job.nodes["dbl"].as_table().num_rows == 3


def test_a_store_without_batch_size_takes_any_commit_size() -> None:
    """The in-memory store commits only when asked: any positive size works."""
    job = build_job(InMemoryArrowDatabase(), 3)
    job.run(commit_every_rows=5000)
    assert job.nodes["dbl"].as_table().num_rows == 3


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_results_are_committed_before_their_records_just_below_batch_size(
    tmp_path: Path, orchestrator: str
) -> None:
    """Every committed pipeline record points at a result committed no later.

    ``commit_every_rows`` is one below the store's ``batch_size``. On the async
    orchestrator results run ahead of their records, so the result table can reach
    ``batch_size`` between the node's commits; the store then commits that table
    alone, never the pipeline records with it.
    """
    store = DeltaTableDatabase(tmp_path, batch_size=50)
    build_job(store, 120).run(
        orchestrator=ORCHESTRATORS[orchestrator](), commit_every_rows=49
    )
    tables = table_dirs(tmp_path)
    pipeline_history = deltalake.DeltaTable(str(tables["pipeline"])).history()
    result_history = deltalake.DeltaTable(str(tables["result"])).history()
    # 120 rows at 49 per commit: 3 pipeline-record commits, none by the store alone.
    assert len(pipeline_history) == 3
    for entry in pipeline_history:
        records = deltalake.DeltaTable(
            str(tables["pipeline"]), version=entry["version"]
        )
        stored_at = entry["timestamp"]
        result_versions = [
            r["version"] for r in result_history if r["timestamp"] <= stored_at
        ]
        assert result_versions, entry
        results = deltalake.DeltaTable(
            str(tables["result"]), version=max(result_versions)
        )
        referenced = set(
            delta_to_pyarrow_dataset(records)
            .to_table(columns=["__data_id"])["__data_id"]
            .to_pylist()
        )
        committed = set(
            delta_to_pyarrow_dataset(results)
            .to_table(columns=["__record_id"])["__record_id"]
            .to_pylist()
        )
        assert referenced <= committed, entry["version"]


def _assert_records_point_at_committed_results(root: Path) -> None:
    """Every committed pipeline-record version references only committed results."""
    tables = table_dirs(root)
    result_history = deltalake.DeltaTable(str(tables["result"])).history()
    for entry in deltalake.DeltaTable(str(tables["pipeline"])).history():
        records = deltalake.DeltaTable(
            str(tables["pipeline"]), version=entry["version"]
        )
        result_versions = [
            r["version"] for r in result_history if r["timestamp"] <= entry["timestamp"]
        ]
        assert result_versions, entry
        results = deltalake.DeltaTable(
            str(tables["result"]), version=max(result_versions)
        )
        referenced = set(read_column_of(records, "__data_id"))
        assert referenced <= set(read_column_of(results, "__record_id")), entry


def read_column_of(table: deltalake.DeltaTable, column: str) -> list[Any]:
    """The values of ``column`` in a loaded table version."""
    return (
        delta_to_pyarrow_dataset(table).to_table(columns=[column])[column].to_pylist()
    )


def _wrapped(store: Any, times: int) -> Any:
    """``store`` inside ``times`` layers of ``ExtensionAwareDatabase``."""
    converter = get_default_context().type_converter
    for _ in range(times):
        store = ExtensionAwareDatabase(store, converter)  # pyright: ignore[reportArgumentType]  # the context's converter; two TypeConverterProtocol definitions exist
    return store


def test_a_wrapped_delta_store_is_table_committing(tmp_path: Path) -> None:
    """Wrappers expose the Delta store's batch_size and flush_batch, and only then."""
    assert as_table_committing(_wrapped(DeltaTableDatabase(tmp_path), 1)) is not None
    assert as_table_committing(_wrapped(InMemoryArrowDatabase(), 2)) is None
    assert as_table_committing(InMemoryArrowDatabase()) is None


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_doubly_wrapped_delta_store_keeps_the_commit_rules(
    tmp_path: Path, orchestrator: str
) -> None:
    """Through two wrappers: the batch-size rule applies and results commit first."""
    store = _wrapped(DeltaTableDatabase(tmp_path, batch_size=50), 2)
    table_committing = as_table_committing(store)
    assert table_committing is not None and table_committing.batch_size == 50
    with pytest.raises(ValueError, match="batch_size=50"):
        build_job(store, 3).run(commit_every_rows=50)
    build_job(store, 120).run(
        orchestrator=ORCHESTRATORS[orchestrator](), commit_every_rows=49
    )
    _assert_records_point_at_committed_results(tmp_path)


def test_writes_pending_before_the_run_are_committed_first(tmp_path: Path) -> None:
    """Rows the store held pending before ``run`` are committed before the run writes."""
    store = DeltaTableDatabase(tmp_path)
    leftover = pa.table({"v": pa.array([1], pa.int64())})
    store.add_record(("leftover",), b"left-1", leftover)  # pending, never flushed
    build_job(store, 30).run()
    leftover_commit = deltalake.DeltaTable(str(tmp_path / "leftover")).history()
    first_job_commit = min(
        entry["timestamp"]
        for table in table_dirs_except(tmp_path, "leftover")
        for entry in deltalake.DeltaTable(str(table)).history()
    )
    assert leftover_commit[0]["timestamp"] <= first_job_commit


def table_dirs_except(root: Path, name: str) -> list[Path]:
    """The Delta tables under ``root`` except the one named ``name``."""
    return [log.parent for log in root.rglob("_delta_log") if log.parent.name != name]


def test_a_failed_commit_of_writes_pending_before_the_run_stops_it_first(
    tmp_path: Path,
) -> None:
    """If writes left pending before the run cannot be committed, nothing is computed."""
    calls_before = len(_DOUBLE_CALLS)
    store = FailingCommits(DeltaTableDatabase(tmp_path), {"calls": 0, "ok": 0})
    leftover = pa.table({"v": pa.array([1], pa.int64())})
    store.add_record(("leftover",), b"left-1", leftover)  # pending, never committed
    job = PipelineJob(name="batched", database=store)  # pyright: ignore[reportArgumentType]  # failing wrapper of a real store
    with job:
        counted_double.pod(
            DictSource(data=[{"k": 1, "x": 1}], tag_columns=["k"], source_id="src"),
            label="dbl",
        )
    with pytest.raises(StoreWriteError) as info:
        job.run()
    assert info.value.flush is True
    notes = getattr(info.value, "__notes__", [])
    assert any("left pending in the store before this run started" in n for n in notes)
    assert len(_DOUBLE_CALLS) == calls_before
    # Nothing was written: the leftover row is still pending, and no table exists.
    assert not list(tmp_path.rglob("_delta_log"))
    pending = store.get_record_by_id(("leftover",), b"left-1")
    assert pending is not None and pending.column("v").to_pylist() == [1]
