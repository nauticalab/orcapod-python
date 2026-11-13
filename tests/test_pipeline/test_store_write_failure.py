"""A failed store write must fail ``PipelineJob.run()`` (ITL-878).

Before the fix, a node whose write to the store failed was skipped like a row whose
function had raised: ``run()`` returned normally and the node read back fewer rows (often
zero). These tests drive ``PipelineJob`` through its public interface on both orchestrators,
with a test-double store that fails chosen writes, and with the SQLite trigger from the
report (a logical-type output on ``SQLiteConnector``).
"""

from __future__ import annotations

import asyncio
import pathlib
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.channels import Channel
from orcapod.core.operators import SelectDataColumns
from orcapod.errors import StoreWriteError
from orcapod.databases import (
    ConnectorArrowDatabase,
    InMemoryArrowDatabase,
    SQLiteConnector,
)
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.pipeline.observer import NoOpObserver
from orcapod.sources import DictSource
from orcapod.types import CacheMode

JOB_NAME = "itl878"

# Markers that select a table by its stored path.
RESULT_TABLE = "rdb_v2"  # a function node's result table
PIPELINE_TABLE = "pdb_v2"  # a function node's pipeline (tag) table
OPERATOR_TABLE = "SelectDataColumns"  # the operator node's LOG table


# ---------------------------------------------------------------------------
# Test-double store
# ---------------------------------------------------------------------------


@dataclass
class Faults:
    """Which writes a ``FaultyStore`` fails; shared by every view of one store."""

    write_marker: str | None = None
    succeed_first: int = 0
    on_flush: bool = False
    flush_marker: str | None = None
    fail_flush_after_write_failure: bool = False
    matching_writes: int = 0
    failed_paths: list[str] = field(default_factory=list)
    pending_paths: list[str] = field(default_factory=list)


class FaultyStore:
    """An ``InMemoryArrowDatabase`` that raises on chosen writes or on flush."""

    def __init__(self, inner: Any, faults: Faults) -> None:
        """Wrap ``inner``, failing the writes ``faults`` selects."""
        self._inner = inner
        self._faults = faults

    def at(self, *path_components: str) -> FaultyStore:
        """Return a view at ``path_components`` that shares this store's faults."""
        return FaultyStore(self._inner.at(*path_components), self._faults)

    def _check_write(self, record_path: tuple[str, ...]) -> None:
        """Raise ``OSError`` if the write to ``record_path`` is one to fail."""
        faults = self._faults
        full_path = "/".join(self._inner.base_path + tuple(record_path))
        faults.pending_paths.append(full_path)
        if faults.write_marker is None or faults.write_marker not in full_path:
            return
        faults.matching_writes += 1
        if faults.matching_writes > faults.succeed_first:
            faults.failed_paths.append("/".join(record_path))
            if faults.fail_flush_after_write_failure:
                faults.on_flush = True
            raise OSError(f"disk unavailable while writing {full_path}")

    def add_record(
        self, record_path, record_id, record, skip_duplicates=False, flush=False
    ):
        """Write one record unless the faults select this table."""
        self._check_write(record_path)
        self._inner.add_record(
            record_path, record_id, record, skip_duplicates=skip_duplicates, flush=flush
        )

    def add_records(
        self,
        record_path,
        records,
        record_id_column=None,
        skip_duplicates=False,
        flush=False,
    ):
        """Write a batch of records unless the faults select this table."""
        self._check_write(record_path)
        self._inner.add_records(
            record_path,
            records,
            record_id_column=record_id_column,
            skip_duplicates=skip_duplicates,
            flush=flush,
        )

    def flush(self) -> None:
        """Flush the wrapped store, or raise ``OSError`` if flushes fail.

        With ``flush_marker`` set, a flush fails only while a write to a matching
        table is pending, whichever view the flush is called on: every view shares
        one set of pending batches.
        """
        faults = self._faults
        if faults.on_flush:
            raise OSError("disk unavailable while flushing")
        marker = faults.flush_marker
        if marker is not None and any(marker in p for p in faults.pending_paths):
            raise OSError(f"disk unavailable while flushing a pending {marker} batch")
        self._inner.flush()
        faults.pending_paths.clear()

    def __getattr__(self, name: str) -> Any:
        """Delegate everything else to the wrapped store."""
        return getattr(self._inner, name)


# ---------------------------------------------------------------------------
# Pipeline under test
# ---------------------------------------------------------------------------

ROWS = [{"k": f"r{i}", "x": i} for i in range(3)]

calls: list[int] = []


@function_pod(output_keys="double")
def double(x: int) -> int:
    """Double ``x``, recording the call."""
    calls.append(x)
    return x * 2


@function_pod(output_keys="double")
def double_or_fail(x: int) -> int:
    """Double ``x``, raising for ``x == 1``."""
    if x == 1:
        raise RuntimeError("function failed for x == 1")
    return x * 2


@function_pod(output_keys="where")
def to_path(x: int) -> pathlib.Path:
    """Return a ``pathlib.Path`` output, a logical type SQLite cannot store."""
    return pathlib.Path(f"/data/{x}.txt")


def build_job(
    store: Any, rows: list[dict[str, Any]], *, pod=double, with_operator: bool = False
):
    """Build a job with one function node ``dbl`` and optionally an operator ``sel``."""
    job = PipelineJob(name=JOB_NAME, store=store)
    with job:
        source = DictSource(data=rows, tag_columns=["k"], source_id="src")
        out = pod.pod(source, label="dbl")
        if with_operator:
            SelectDataColumns(["double"])(out, label="sel")
    if with_operator:
        # OperatorNode.attach_databases is the public way to turn on LOG mode.
        job.nodes["sel"].attach_databases(
            pipeline_database=store.at(JOB_NAME), cache_mode=CacheMode.LOG
        )
    return job


@pytest.fixture(autouse=True)
def _reset_calls():
    """Clear the call log before each test."""
    calls.clear()


@pytest.fixture(params=["sync", "async"])
def orchestrator_cls(request):
    """Run each test on the sync and the async orchestrator."""
    return {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}[
        request.param
    ]


def assert_store_error(
    err: BaseException,
    faults: Faults,
    *,
    node_label: str | None,
    names_row: bool,
    flush: bool = False,
) -> None:
    """The run's error is a plain ``StoreWriteError`` naming node, row and table."""
    assert isinstance(err, StoreWriteError), f"{type(err).__name__}: {err}"
    message = str(err)
    if node_label is not None:
        assert err.node_label == node_label
        assert f"node {node_label!r}" in message, message
    if names_row:
        assert err.tag is not None and err.tag["k"] in {row["k"] for row in ROWS}
        assert f"for row {err.tag!r}" in message, message
    if faults.failed_paths:
        assert "/".join(err.record_path) == faults.failed_paths[0]
        assert faults.failed_paths[0] in message, message
    assert err.flush is flush
    if flush:
        assert err.tag is None and "for row" not in message, message
        assert "pending writes" in message and "earlier row" in message, message
    assert isinstance(err.__cause__, OSError)
    assert "does not recompute stored function results" in message


class FailFlushAtRunEnd(NoOpObserver):
    """Make every flush fail once the orchestrator has finished, so the job's own
    final flush is the one that fails (the result cache flushes after every row)."""

    def __init__(self, faults: Faults) -> None:
        """Arm ``faults`` at the end of the run."""
        self._faults = faults

    def on_run_end(self, run_id) -> None:
        """Make the following flushes fail."""
        self._faults.on_flush = True


def stored_rows(store: Any, label: str = "dbl") -> int:
    """Rows a fresh job over ``store`` reads back for ``label`` without running."""
    records = build_job(store, ROWS).nodes[label].get_all_records()
    return 0 if records is None else records.num_rows


# ---------------------------------------------------------------------------
# Function node writes
# ---------------------------------------------------------------------------


class TestFunctionNodeWriteFailure:
    """A function node's failed result or pipeline write fails the run."""

    def test_result_table_failure_on_first_row_raises(self, orchestrator_cls):
        """A result write failing on the first row raises, naming node, row and table."""
        inner = InMemoryArrowDatabase()
        faults = Faults(write_marker=RESULT_TABLE)
        job = build_job(FaultyStore(inner, faults), ROWS)

        with pytest.raises(StoreWriteError) as exc_info:
            job.run(orchestrator=orchestrator_cls())

        assert_store_error(exc_info.value, faults, node_label="dbl", names_row=True)

    def test_result_table_failure_on_later_row_keeps_earlier_rows(
        self, orchestrator_cls
    ):
        """Rows stored before the failure are kept and are not recomputed on a re-run."""
        inner = InMemoryArrowDatabase()
        faults = Faults(write_marker=RESULT_TABLE, succeed_first=1)
        job = build_job(FaultyStore(inner, faults), ROWS)

        with pytest.raises(StoreWriteError) as exc_info:
            job.run(orchestrator=orchestrator_cls())

        assert_store_error(exc_info.value, faults, node_label="dbl", names_row=True)
        assert stored_rows(inner) == 1

        # A re-run on a healthy store does not recompute the stored rows.
        calls.clear()
        rerun = build_job(inner, ROWS)
        rerun.run(orchestrator=orchestrator_cls())
        assert rerun.nodes["dbl"].as_table().num_rows == 3
        assert len(calls) == 2

    def test_pipeline_table_failure_raises(self, orchestrator_cls):
        """A pipeline-record write failure raises; the failing row's stored result is reused."""
        inner = InMemoryArrowDatabase()
        faults = Faults(write_marker=PIPELINE_TABLE)
        job = build_job(FaultyStore(inner, faults), ROWS)

        with pytest.raises(StoreWriteError) as exc_info:
            job.run(orchestrator=orchestrator_cls())

        assert_store_error(exc_info.value, faults, node_label="dbl", names_row=True)

        # The failing row's result was stored before its pipeline record failed, so a
        # healthy re-run reuses it instead of computing it again. (On the async
        # orchestrator a row still computing when the run stopped was never stored
        # and is computed again, so the total call count is not fixed.)
        failed_tag = exc_info.value.tag
        assert failed_tag is not None
        failed_x = next(row["x"] for row in ROWS if row["k"] == failed_tag["k"])
        calls.clear()
        rerun = build_job(inner, ROWS)
        rerun.run(orchestrator=orchestrator_cls())
        assert rerun.nodes["dbl"].as_table().num_rows == 3
        assert failed_x not in calls

    def test_failure_on_new_row_keeps_previously_stored_rows(self, orchestrator_cls):
        """A failure on a new row keeps the rows a previous run stored."""
        inner = InMemoryArrowDatabase()
        build_job(inner, ROWS[:2]).run(orchestrator=orchestrator_cls())
        assert stored_rows(inner) == 2

        faults = Faults(write_marker=RESULT_TABLE)
        job = build_job(FaultyStore(inner, faults), ROWS)
        with pytest.raises(StoreWriteError) as exc_info:
            job.run(orchestrator=orchestrator_cls())

        assert_store_error(exc_info.value, faults, node_label="dbl", names_row=True)
        assert stored_rows(inner) == 2

    @pytest.mark.parametrize("error_policy", ["continue", "fail_fast"])
    def test_store_failure_raises_under_any_error_policy(
        self, orchestrator_cls, error_policy
    ):
        """A store failure raises under both error policies."""
        faults = Faults(write_marker=RESULT_TABLE)
        job = build_job(FaultyStore(InMemoryArrowDatabase(), faults), ROWS)

        with pytest.raises(StoreWriteError) as exc_info:
            job.run(orchestrator=orchestrator_cls(error_policy=error_policy))

        assert_store_error(exc_info.value, faults, node_label="dbl", names_row=True)

    def test_auto_flush_failure_raises_naming_the_node(self, orchestrator_cls):
        """A failure of the result cache's flush after a row raises, naming the node."""
        faults = Faults(on_flush=True)
        store = FaultyStore(InMemoryArrowDatabase(), faults)
        job = build_job(store, ROWS)

        with pytest.raises(StoreWriteError) as exc_info:
            job.run(orchestrator=orchestrator_cls())

        assert_store_error(
            exc_info.value, faults, node_label="dbl", names_row=False, flush=True
        )

    def test_auto_flush_failure_of_an_earlier_write_names_no_row(
        self, orchestrator_cls
    ):
        """A result auto-flush that fails on another table's pending write names
        neither the row being stored nor the result table.

        The sync orchestrator meets the failure in the next row's result auto-flush.
        The async one may write every pipeline record after the last result flush,
        so the job's final flush can be the one that fails; either way it is a flush
        error that names no row.
        """
        faults = Faults(flush_marker=PIPELINE_TABLE)
        job = build_job(FaultyStore(InMemoryArrowDatabase(), faults), ROWS)

        with pytest.raises(StoreWriteError) as exc_info:
            job.run(orchestrator=orchestrator_cls())

        err = exc_info.value
        assert_store_error(err, faults, node_label=None, names_row=False, flush=True)
        if orchestrator_cls is SyncPipelineOrchestrator:
            assert err.node_label == "dbl"
        assert RESULT_TABLE not in "/".join(err.record_path)
        assert RESULT_TABLE not in str(err)

    def test_final_flush_failure_raises(self, orchestrator_cls):
        """A failure of the job's final flush raises ``StoreWriteError``."""
        faults = Faults()
        job = build_job(FaultyStore(InMemoryArrowDatabase(), faults), ROWS)

        with pytest.raises(StoreWriteError) as exc_info:
            job.run(orchestrator=orchestrator_cls(), observer=FailFlushAtRunEnd(faults))

        assert_store_error(
            exc_info.value, faults, node_label=None, names_row=False, flush=True
        )
        assert exc_info.value.record_path == (JOB_NAME,)

    def test_function_failure_is_still_skipped_under_continue(self, orchestrator_cls):
        """``error_policy="continue"`` keeps skipping rows whose function raised."""
        inner = InMemoryArrowDatabase()
        job = build_job(inner, ROWS, pod=double_or_fail)

        job.run(orchestrator=orchestrator_cls(error_policy="continue"))

        assert sorted(job.nodes["dbl"].as_table()["k"].to_pylist()) == ["r0", "r2"]


# ---------------------------------------------------------------------------
# Operator node writes (cache_mode=LOG)
# ---------------------------------------------------------------------------


class TestOperatorNodeWriteFailure:
    """An operator node's failed LOG write fails the run."""

    def test_operator_log_failure_raises_and_commits_upstream_rows(
        self, orchestrator_cls
    ):
        """The operator's failure raises and the upstream node's rows stay committed."""
        inner = InMemoryArrowDatabase()
        faults = Faults(write_marker=OPERATOR_TABLE)
        job = build_job(FaultyStore(inner, faults), ROWS, with_operator=True)

        with pytest.raises(StoreWriteError) as exc_info:
            job.run(orchestrator=orchestrator_cls())

        assert_store_error(exc_info.value, faults, node_label="sel", names_row=False)
        # The upstream function node wrote all its rows before the operator failed;
        # none of them may look dropped.
        assert stored_rows(inner) == 3

    def test_failed_flush_after_write_failure_is_noted(self, orchestrator_cls):
        """A failed best-effort flush is noted on the write error, which stays primary."""
        inner = InMemoryArrowDatabase()
        faults = Faults(
            write_marker=OPERATOR_TABLE, fail_flush_after_write_failure=True
        )
        job = build_job(FaultyStore(inner, faults), ROWS, with_operator=True)

        with pytest.raises(StoreWriteError) as exc_info:
            job.run(orchestrator=orchestrator_cls())

        err = exc_info.value
        assert_store_error(err, faults, node_label="sel", names_row=False)
        notes = getattr(err, "__notes__", [])
        assert any(
            "disk unavailable while flushing" in note
            and "may not have been stored" in note
            for note in notes
        ), notes

    def test_final_flush_failure_warns_pending_output(self, orchestrator_cls):
        """A failed final flush warns that output still pending may not be stored."""
        faults = Faults()
        job = build_job(
            FaultyStore(InMemoryArrowDatabase(), faults), ROWS, with_operator=True
        )

        with pytest.raises(StoreWriteError) as exc_info:
            job.run(orchestrator=orchestrator_cls(), observer=FailFlushAtRunEnd(faults))

        err = exc_info.value
        assert_store_error(err, faults, node_label=None, names_row=False, flush=True)
        notes = getattr(err, "__notes__", [])
        assert any(
            "pending" in note
            and "operator" in note
            and "may not have been stored" in note
            for note in notes
        ), notes


# ---------------------------------------------------------------------------
# The reported trigger: a logical-type output on SQLite (ITL-877 is the limit itself)
# ---------------------------------------------------------------------------


class TestSQLiteTrigger:
    """The reported trigger: a logical-type output on SQLite."""

    def test_logical_type_output_on_sqlite_raises(self, orchestrator_cls, tmp_path):
        """A ``pathlib.Path`` output on SQLite raises, naming the node."""
        store = ConnectorArrowDatabase(SQLiteConnector(str(tmp_path / "store.db")))
        job = build_job(store, ROWS, pod=to_path)

        with pytest.raises(StoreWriteError) as exc_info:
            job.run(orchestrator=orchestrator_cls())

        assert exc_info.value.node_label == "dbl"
        assert "node 'dbl'" in str(exc_info.value)
        assert "does not preserve Arrow metadata" in str(exc_info.value)


# ---------------------------------------------------------------------------
# A failing hook or observer callback never replaces a store error
# ---------------------------------------------------------------------------


def failing_hook(payload: Any) -> None:
    """A fail-loud post-run hook that always raises."""
    raise ValueError("post-run hook failed")


def make_hooked_triple(store: Any):
    """A cached pod with a fail-loud post-run hook, writing its results to ``store``."""

    @function_pod(
        output_keys="tripled", pod_cache_database=store, post_run_hooks=[failing_hook]
    )
    def triple(x: int) -> int:
        """Triple ``x``."""
        return x * 3

    return triple


def make_outer(inner: Any):
    """A function pod whose function runs ``inner`` through its stream."""

    @function_pod(output_keys="double")
    def via_inner(x: int) -> int:
        """Run ``inner`` on ``x`` through a one-row stream and return ``3x - x``."""
        stream = DictSource(data=[{"i": "i", "x": x}], tag_columns=["i"])
        return inner.pod(stream).as_table()["tripled"][0].as_py() - x

    return via_inner


def assert_hook_failure_noted(err: BaseException) -> None:
    """The error is the store error, with the hook failure recorded as a note."""
    assert isinstance(err, StoreWriteError), f"{type(err).__name__}: {err}"
    assert isinstance(err.__cause__, OSError)
    notes = getattr(err, "__notes__", [])
    assert any("post-run hook failed" in note for note in notes), notes


class CrashingObserver(NoOpObserver):
    """An observer whose crash and end callbacks raise once a row has crashed."""

    def __init__(self) -> None:
        """Start with no crashed row."""
        self.crashed = False

    def on_data_crash(self, node_label, tag, data, error) -> None:
        """Record the crash, then fail."""
        self.crashed = True
        raise RuntimeError("observer failed in on_data_crash")

    def on_node_end(self, node_label, node_hash) -> None:
        """Fail if a row has crashed."""
        if self.crashed:
            raise RuntimeError("observer failed in on_node_end")

    def on_run_end(self, run_id) -> None:
        """Fail if a row has crashed."""
        if self.crashed:
            raise RuntimeError("observer failed in on_run_end")


class TestCallbackFailureKeepsStoreError:
    """A hook or observer that raises never replaces a store error."""

    def test_failing_hook_keeps_store_error_standalone(self):
        """A standalone cached pod raises its store error, not the hook's."""
        faults = Faults(write_marker="")
        triple = make_hooked_triple(FaultyStore(InMemoryArrowDatabase(), faults))
        stream = DictSource(data=ROWS, tag_columns=["k"])

        with pytest.raises(StoreWriteError) as exc_info:
            triple.pod(stream).as_table()

        assert_hook_failure_noted(exc_info.value)

    def test_failing_hook_keeps_store_error_standalone_async(self):
        """The async path raises the store error too, rather than skipping the row."""
        faults = Faults(write_marker="")
        triple = make_hooked_triple(FaultyStore(InMemoryArrowDatabase(), faults))
        stream = DictSource(data=ROWS, tag_columns=["k"])
        input_ch: Channel[Any] = Channel(buffer_size=16)
        output_ch: Channel[Any] = Channel(buffer_size=16)

        async def feed() -> None:
            """Send the stream's rows into the input channel, then close it."""
            for tag, data in stream.iter_data():
                await input_ch.writer.send((tag, data))
            await input_ch.writer.close()

        async def run() -> None:
            """Feed the pod and run it concurrently."""
            await asyncio.gather(
                feed(), triple.pod.async_execute([input_ch.reader], output_ch.writer)
            )

        with pytest.raises(ExceptionGroup) as exc_info:
            asyncio.run(run())

        leaves = exc_info.value.exceptions
        assert all(isinstance(leaf, StoreWriteError) for leaf in leaves), leaves
        for leaf in leaves:
            assert_hook_failure_noted(leaf)

    @pytest.mark.parametrize("error_policy", ["continue", "fail_fast"])
    def test_failing_hook_in_nested_pod_fails_the_run(
        self, orchestrator_cls, error_policy
    ):
        """A node function running a cached pod whose write fails cannot skip the row."""
        faults = Faults(write_marker="")
        inner = make_hooked_triple(FaultyStore(InMemoryArrowDatabase(), faults))
        job = build_job(InMemoryArrowDatabase(), ROWS, pod=make_outer(inner))

        with pytest.raises(StoreWriteError) as exc_info:
            job.run(orchestrator=orchestrator_cls(error_policy=error_policy))

        assert_hook_failure_noted(exc_info.value)

    def test_failing_observer_keeps_store_error(self, orchestrator_cls):
        """Observer failures on the way out are noted on the store error."""
        faults = Faults(write_marker=RESULT_TABLE)
        job = build_job(FaultyStore(InMemoryArrowDatabase(), faults), ROWS)

        with pytest.raises(StoreWriteError) as exc_info:
            job.run(orchestrator=orchestrator_cls(), observer=CrashingObserver())

        err = exc_info.value
        assert_store_error(err, faults, node_label="dbl", names_row=True)
        notes = getattr(err, "__notes__", [])
        assert any("observer failed in on_data_crash" in note for note in notes), notes
        assert any("observer failed in on_run_end" in note for note in notes), notes


# ---------------------------------------------------------------------------
# Concurrent failures in the async orchestrator
# ---------------------------------------------------------------------------

# ``async def`` functions run on the event loop rather than in worker threads, so
# the order in which concurrent node tasks fail is the same on every run.


@function_pod(output_keys="double")
async def double_async(x: int) -> int:
    """Double ``x`` on the event loop."""
    return x * 2


@function_pod(output_keys="tripled")
async def triple_async(x: int) -> int:
    """Triple ``x`` on the event loop."""
    return x * 3


@function_pod(output_keys="tripled")
async def fail_async(x: int) -> int:
    """Always raise, on the event loop."""
    raise RuntimeError("function failed")


class FailingCrashObserver(NoOpObserver):
    """An observer whose ``on_data_crash`` raises for one node, failing that node."""

    def on_data_crash(self, node_label, tag, data, error) -> None:
        """Fail for crashes of node ``other`` only."""
        if node_label == "other":
            raise KeyError("observer failed for node 'other'")


def build_siblings(store: Any, other_pod: Any):
    """Two function nodes reading the same source: ``dbl`` and ``other``."""
    job = PipelineJob(name=JOB_NAME, store=store)
    with job:
        source = DictSource(data=ROWS, tag_columns=["k"], source_id="src")
        double_async.pod(source, label="dbl")
        other_pod.pod(source, label="other")
    return job


class TestConcurrentFailures:
    """Concurrent node failures still give one plain ``StoreWriteError``."""

    def test_store_error_and_other_failure_raise_one_store_error(self):
        """A store error mixed with another failure raises the store error alone."""
        faults = Faults(write_marker=RESULT_TABLE)
        job = build_siblings(FaultyStore(InMemoryArrowDatabase(), faults), fail_async)

        with pytest.raises(StoreWriteError) as exc_info:
            job.run(
                orchestrator=AsyncPipelineOrchestrator(),
                observer=FailingCrashObserver(),
            )

        err = exc_info.value
        assert_store_error(err, faults, node_label="dbl", names_row=True)
        notes = getattr(err, "__notes__", [])
        assert any("observer failed for node 'other'" in note for note in notes), notes
        # The whole group stays reachable for diagnosis.
        assert isinstance(err.__context__, ExceptionGroup)

    def test_several_store_errors_raise_the_first_in_node_order(
        self, orchestrator_cls, caplog
    ):
        """Several store errors raise the first node's, the same on both orchestrators."""
        faults = Faults(write_marker=RESULT_TABLE)
        job = build_siblings(FaultyStore(InMemoryArrowDatabase(), faults), triple_async)

        with caplog.at_level("WARNING"):
            with pytest.raises(StoreWriteError) as exc_info:
                job.run(orchestrator=orchestrator_cls())

        # The sync orchestrator meets ``dbl`` first; the async one raises the same.
        assert_store_error(exc_info.value, faults, node_label="dbl", names_row=True)
        if orchestrator_cls is AsyncPipelineOrchestrator:
            others = [
                record
                for record in caplog.records
                if record.levelname == "WARNING"
                and "Another store write failed" in record.getMessage()
            ]
            assert any("node 'other'" in r.getMessage() for r in others), others


# ---------------------------------------------------------------------------
# An output that cannot be turned into a record is a row failure, not a store error
# ---------------------------------------------------------------------------


@dataclass
class Pair:
    """A dataclass output type."""

    a: int
    b: int


@function_pod(output_keys="y")
def raises_for_one(x: int) -> int:
    """Raise for ``x == 1``: the reference function failure."""
    if x == 1:
        raise RuntimeError("function failed for x == 1")
    return x


@function_pod(output_keys="y")
def naive_datetime_for_one(x: int) -> datetime:
    """Return a naive ``datetime`` for ``x == 1``, rejected by the strict tz policy."""
    return datetime(2020, 1, 1 + x, tzinfo=None if x == 1 else timezone.utc)


@function_pod(output_keys="y")
def str_for_int_for_one(x: int) -> int:
    """Return a ``str`` for ``x == 1`` although ``int`` is declared."""
    return "abc" if x == 1 else x  # pyright: ignore[reportReturnType]  # the bad output under test


@function_pod(output_keys="y")
def int_for_dataclass_for_one(x: int) -> Pair:
    """Return an ``int`` for ``x == 1`` although a dataclass is declared."""
    return 7 if x == 1 else Pair(x, x)  # pyright: ignore[reportReturnType]  # the bad output under test


class CrashRecorder(NoOpObserver):
    """Record the node label and row key of every ``on_data_crash``."""

    def __init__(self) -> None:
        """Start with no crashes."""
        self.crashes: list[tuple[str, str]] = []

    def on_data_crash(self, node_label, tag, data, error) -> None:
        """Record the crash."""
        self.crashes.append((node_label, str(tag["k"])))


# The per-row handlers' log messages, both at DEBUG: sync ``FunctionNode.execute``
# (ITL-997) and async ``process_one`` (ITL-879).
ROW_FAILURE_LOGS = (
    "Data execution failed in %s, skipping: %s",
    "Data processing failed, skipping: %s",
)


def row_failure_outcome(
    pod: Any, orchestrator: Any, caplog: pytest.LogCaptureFixture
) -> dict[str, Any]:
    """Run ``pod`` as node ``dbl`` and describe what the caller sees."""
    store = InMemoryArrowDatabase()
    observer = CrashRecorder()
    job = build_job(store, ROWS, pod=pod)
    caplog.clear()
    with caplog.at_level("DEBUG", logger="orcapod"):
        try:
            job.run(orchestrator=orchestrator, observer=observer)
            raised = None
            rows = sorted(job.nodes["dbl"].as_table()["k"].to_pylist())
        except Exception as exc:
            raised = exc
            rows = None
    logs = [
        (record.levelname, record.msg)
        for record in caplog.records
        if record.msg in ROW_FAILURE_LOGS
    ]
    return {
        "raised": raised,
        "rows": rows,
        "crashes": observer.crashes,
        "stored": stored_rows(store),
        "logs": logs,
    }


class TestUnstorableOutputIsARowFailure:
    """An output that cannot become a record is reported like a function failure."""

    @pytest.mark.parametrize(
        "pod",
        [naive_datetime_for_one, str_for_int_for_one, int_for_dataclass_for_one],
        ids=["naive_datetime", "str_for_int", "int_for_dataclass"],
    )
    @pytest.mark.parametrize("error_policy", ["continue", "fail_fast"])
    def test_reported_like_a_function_failure(
        self, orchestrator_cls, error_policy, pod, caplog
    ):
        """Same raise-or-return, rows, log, observer report and stored rows as a raise."""
        expected = row_failure_outcome(
            raises_for_one, orchestrator_cls(error_policy=error_policy), caplog
        )
        actual = row_failure_outcome(
            pod, orchestrator_cls(error_policy=error_policy), caplog
        )

        assert not isinstance(actual["raised"], StoreWriteError), actual["raised"]
        assert (actual["raised"] is None) == (expected["raised"] is None), actual
        assert actual["rows"] == expected["rows"]
        assert actual["crashes"] == expected["crashes"] == [("dbl", "r1")]
        assert len(expected["logs"]) == 1, expected["logs"]
        assert actual["logs"] == expected["logs"]
        assert actual["stored"] == expected["stored"]
