"""Review fixes for the current-run rule and load-only runs (ITL-1095, PR #380 review).

Each test names the guarantee it protects: a run that raised (in its observer, its store,
its commit or its output) leaves no node holding its rows; a load-only run writes nothing,
not even pending writes of others; an operator filled by a load-only run says so; the
freshness check stays linear on a DAG of shared ancestors.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, Literal
from unittest import mock

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.nodes import OperatorJobNode
from orcapod.core.nodes.source_node import SourceJobNode
from orcapod.core.operators import Join, MapData, SelectDataColumns
from orcapod.core.streams import ArrowTableStream, ViewStatus
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import StoreWriteError
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.pipeline.observer import NoOpObserver
from orcapod.side_effects import SideEffectPod, SideEffectPodConfig, side_effect_pod
from orcapod.sources import ArrowTableSource, DictSource
from orcapod.types import CacheMode

CALLS: list[str] = []
FAIL = {"tap": False}


@function_pod(output_keys="y")
def inc(x: int) -> int:
    """Return ``x + 1``."""
    CALLS.append("inc")
    return x + 1


@function_pod(output_keys="z")
def dbl(y: int) -> int:
    """Return ``2 * y``."""
    CALLS.append("dbl")
    return 2 * y


@function_pod(output_keys="w")
def neg(z: int) -> int:
    """Return ``-z``."""
    CALLS.append("neg")
    return -z


@function_pod(output_keys="z")
def fails_on_21(y: int) -> int:
    """Return ``2 * y``; raise for ``y == 21``."""
    if y == 21:
        raise ValueError("y is 21")
    return 2 * y


@function_pod(output_keys="total")
def total(y: list[int]) -> int:
    """Return the sum of a group's values."""
    return sum(y)


def _deliver(y: int, ctx: Any) -> None:
    """A side effect that fails while ``FAIL['tap']`` is set."""
    if FAIL["tap"]:
        raise RuntimeError("delivery failed")


ROWS = [{"g": i % 2, "k": i, "x": 10 * i} for i in range(6)]
ORCHESTRATORS = {"sync": SyncPipelineOrchestrator, "async": AsyncPipelineOrchestrator}


@pytest.fixture(autouse=True)
def _reset() -> None:
    """Start every test with no recorded calls and a delivery that succeeds."""
    CALLS.clear()
    FAIL["tap"] = False


def _source(data: list[dict[str, int]] = ROWS) -> DictSource:
    """An in-memory source of ``data`` with tags ``g`` and ``k``."""
    return DictSource(data, tag_columns=["g", "k"], source_id="s")


def _grouped_job(data: list[dict[str, int]], db: Any) -> PipelineJob:
    """``rowwise`` -> ``grouped`` (GroupBy) -> ``summed``."""
    job = PipelineJob(name="j", database=db)
    with job:
        rows: Any = inc.pod(_source(data), label="rowwise")
        total.pod(rows.group_by("g", label="grouped"), label="summed")
    return job


def _chain_job(data: list[dict[str, int]], db: Any) -> PipelineJob:
    """``a`` (inc) -> ``b`` (dbl) -> ``c`` (neg)."""
    job = PipelineJob(name="c", database=db)
    with job:
        neg.pod(dbl.pod(inc.pod(_source(data), label="a"), label="b"), label="c")
    return job


class FailsAtNodeEnd(NoOpObserver):
    """An observer whose ``on_node_end`` raises for one node."""

    def __init__(self, label: str) -> None:
        """Keep the label of the node this observer acts at."""
        self.label = label

    def on_node_end(self, node_label: str, node_hash: str) -> None:
        """Raise at the node's end, as a broken observer would."""
        if node_label == self.label:
            raise RuntimeError("observer failed")

    def contextualize(self, *identity_path: str) -> FailsAtNodeEnd:
        """Keep this observer for every node."""
        return self


# ---------------------------------------------------------------------------
# A run that raised leaves no node holding its rows
# ---------------------------------------------------------------------------


def test_an_observer_failing_at_the_end_of_a_load_only_node_leaves_it_empty() -> None:
    """An observer raising at a load-only node's end leaves the node empty and stale."""
    db = InMemoryArrowDatabase()
    _grouped_job(ROWS, db).run()
    job = _grouped_job(ROWS, db)
    with pytest.raises(RuntimeError, match="observer failed"):
        job.run(load_only=True, observer=FailsAtNodeEnd("rowwise"))
    assert job.nodes["rowwise"].as_table().num_rows == 0
    assert job.nodes["rowwise"].is_stale


def test_a_failed_commit_of_a_node_run_alone_leaves_it_empty() -> None:
    """A node run alone whose commit fails holds nothing afterwards."""
    from orcapod.core.nodes import FunctionJobNode

    table = pa.table(
        {"k": [0, 1], "x": [1, 2]},
        schema=pa.schema(
            [
                pa.field("k", pa.int64(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        ),
    )
    node = FunctionJobNode(
        inc.pod,  # pyright: ignore[reportArgumentType]  # the decorator's pod type is not declared as FunctionPodProtocol
        ArrowTableStream(table, tag_columns=["k"]),
        pipeline_database=InMemoryArrowDatabase(),
    )

    def fail(self: Any) -> None:
        """A flush that always fails."""
        raise OSError("disk full")

    with (
        mock.patch.object(InMemoryArrowDatabase, "flush", fail),
        pytest.raises(StoreWriteError),
    ):
        node.run()
    assert node.as_table().num_rows == 0
    assert node.is_stale


def test_a_failed_run_clears_every_node_it_ran() -> None:
    """A downstream failure (fail_fast) leaves the upstream node that ran empty too.

    Sync only: the asynchronous orchestrator does not stop for ``fail_fast`` (ITL-879).
    """
    job = PipelineJob(name="f", database=InMemoryArrowDatabase())
    with job:
        fails_on_21.pod(inc.pod(_source(), label="up"), label="down")
    with pytest.raises(ValueError, match="y is 21"):
        job.run(orchestrator=SyncPipelineOrchestrator(error_policy="fail_fast"))
    assert job.nodes["up"].as_table().num_rows == 0
    assert job.nodes["up"].is_stale


def test_a_failed_cascade_clears_every_node_it_ran() -> None:
    """A node.run() cascade whose commit fails clears every node it ran."""
    job = PipelineJob(name="f", database=InMemoryArrowDatabase())
    with job:
        fails_on_21.pod(inc.pod(_source(), label="up"), label="down")

    def fail(self: Any) -> None:
        """A flush that always fails."""
        raise OSError("disk full")

    with (
        mock.patch.object(InMemoryArrowDatabase, "flush", fail),
        pytest.raises(StoreWriteError),
    ):
        job.nodes["down"].run()
    for label in ("up", "down"):
        assert job.nodes[label].as_table().num_rows == 0, label


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_failed_operator_store_leaves_the_operator_empty(orchestrator: str) -> None:
    """An operator whose LOG write fails holds nothing afterwards."""
    db = InMemoryArrowDatabase()
    job = PipelineJob(name="o", database=db)
    with job:
        SelectDataColumns(["x"])(_source(), label="sel")
    job.nodes["sel"].attach_databases(
        pipeline_database=db.at("o"), cache_mode=CacheMode.LOG
    )
    with (
        mock.patch.object(
            InMemoryArrowDatabase, "add_records", side_effect=OSError("disk full")
        ),
        pytest.raises((StoreWriteError, ExceptionGroup)),
    ):
        job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    assert job.nodes["sel"].as_table().num_rows == 0
    assert job.nodes["sel"].is_stale


def _tap_job(db: Any) -> PipelineJob:
    """``rows`` -> ``tap`` (side effect, raises while FAIL is set) -> ``down``."""
    tap = side_effect_pod(_deliver, config=SideEffectPodConfig(track_completion=False))
    assert isinstance(tap, SideEffectPod)
    job = PipelineJob(name="t", database=db)
    with job:
        tapped: Any = tap.process(inc.pod(_source(), label="rows"), label="tap")
        dbl.pod(tapped.select_data_columns("y"), label="down")
    return job


def test_a_failed_side_effect_run_makes_the_nodes_below_it_stale() -> None:
    """A failed side-effect run makes it and every node below it stale."""
    job = _tap_job(InMemoryArrowDatabase())
    job.run()
    assert job.nodes["down"].as_table().num_rows == 6
    job.nodes["tap"].clear_cache()
    FAIL["tap"] = True
    with pytest.raises(RuntimeError, match="delivery failed"):
        job.nodes["tap"].run()
    assert job.nodes["tap"].is_stale
    assert job.nodes["down"].is_stale
    assert job.nodes["down"].as_table().num_rows == 0


def test_a_side_effect_node_whose_output_cannot_close_holds_nothing() -> None:
    """A side-effect run whose output close fails holds no rows."""
    from orcapod.channels import Channel

    job = _tap_job(InMemoryArrowDatabase())
    job.run()
    node = job.nodes["tap"]
    node.clear_cache()

    class CloseFails:
        """An output writer that keeps what it is sent and raises on close."""

        async def send(self, item: Any) -> None:
            """Accept ``item``."""

        async def close(self) -> None:
            """Raise, as a broken channel would."""
            raise OSError("output cannot be closed")

    async def run() -> None:
        """Run the node's channel entry point."""
        inputs: Channel[Any] = Channel(buffer_size=16)
        for item in job.nodes["rows"].iter_data():
            await inputs.writer.send(item)
        await inputs.writer.close()
        # The node's channel entry point is what this test is about.
        await node.async_execute([inputs.reader], CloseFails())  # pyright: ignore[reportArgumentType]  # a test double of the writer

    with pytest.raises(OSError, match="cannot be closed"):
        asyncio.run(run())
    assert node.as_table().num_rows == 0
    assert node.is_stale


# ---------------------------------------------------------------------------
# A load-only run writes nothing, pending writes of others included
# ---------------------------------------------------------------------------


def _pending(db: InMemoryArrowDatabase) -> dict[str, int]:
    """The store's pending batches with their row counts (private on purpose: what a
    load-only run must leave as it found it)."""
    return {key: table.num_rows for key, table in db._pending_batches.items()}  # pyright: ignore[reportPrivateUsage]  # the pending batches are what this test is about


def _add_pending_write(db: InMemoryArrowDatabase) -> None:
    """Leave one uncommitted write of another table in ``db``."""
    db.add_records(
        ("elsewhere",),
        pa.table({"__rid": [b"r1"], "v": [1]}),
        record_id_column="__rid",
    )


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_load_only_run_leaves_pending_writes_as_it_found_them(
    orchestrator: str,
) -> None:
    """A load-only job run and cascade flush nothing: pending writes stay pending."""
    db = InMemoryArrowDatabase()
    _grouped_job(ROWS, db).run()
    _add_pending_write(db)
    before = _pending(db)
    assert before
    job = _grouped_job(ROWS, db)
    with mock.patch.object(
        InMemoryArrowDatabase, "flush", side_effect=AssertionError("flushed")
    ):
        job.run(orchestrator=ORCHESTRATORS[orchestrator](), load_only=True)
        cascade = _grouped_job(ROWS, db)
        cascade.nodes["summed"].run(load_only=True)
    assert _pending(db) == before
    assert job.nodes["summed"].as_table().num_rows == 2


# ---------------------------------------------------------------------------
# Operators: the standalone LOG write is committed; load-only output is marked
# ---------------------------------------------------------------------------


def _source_table() -> ArrowTableSource:
    """A three-row Arrow source with tag ``k`` and data ``x``, non-null."""
    table = pa.table(
        {"k": [0, 1, 2], "x": [1, 2, 3]},
        schema=pa.schema(
            [
                pa.field("k", pa.int64(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        ),
    )
    return ArrowTableSource(table, tag_columns=["k"], source_id="t")


def test_a_standalone_log_run_commits_its_write(tmp_path: Path) -> None:
    """A standalone LOG operator run commits its write before returning."""
    node = OperatorJobNode(
        operator=SelectDataColumns(["x"]),
        input_streams=[_source_table()],
        pipeline_database=DeltaTableDatabase(tmp_path),
        cache_mode=CacheMode.LOG,
    )
    node.run()
    reopened = OperatorJobNode(
        operator=SelectDataColumns(["x"]),
        input_streams=[_source_table()],
        pipeline_database=DeltaTableDatabase(tmp_path),
        cache_mode=CacheMode.LOG,
    )
    stored = reopened.get_all_records()
    assert stored is not None and stored.num_rows == 3


def test_a_standalone_log_run_after_a_load_only_run_stores(tmp_path: Path) -> None:
    """A full standalone run after a load-only run computes and stores."""
    db = InMemoryArrowDatabase()
    node = OperatorJobNode(
        operator=SelectDataColumns(["x"]),
        input_streams=[_source_table()],
        pipeline_database=db,
        cache_mode=CacheMode.LOG,
    )
    node.run(load_only=True)
    assert node.get_all_records() is None
    node.run()
    stored = node.get_all_records()
    assert stored is not None and stored.num_rows == 3


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_an_operator_filled_by_a_load_only_run_says_so(orchestrator: str) -> None:
    """An operator filled by a load-only run shows ``loaded_only``."""
    db = InMemoryArrowDatabase()
    _grouped_job(ROWS, db).run()
    job = _grouped_job(ROWS, db)
    job.run(orchestrator=ORCHESTRATORS[orchestrator](), load_only=True)
    grouped = job.nodes["grouped"]
    assert grouped.head().status is ViewStatus.LOADED_ONLY
    assert grouped.run(load_only=True).status is ViewStatus.LOADED_ONLY


def test_an_unrun_loaded_operator_without_its_operator_keeps_its_columns(
    tmp_path: Path,
) -> None:
    """An operator node rebuilt without its operator keeps its saved columns."""
    db = InMemoryArrowDatabase()
    job = PipelineJob(name="l", database=db)
    with job:
        Join()(
            DictSource([{"k": 0, "a": 1}], tag_columns=["k"], source_id="a"),
            DictSource([{"k": 0, "b": 2}], tag_columns=["k"], source_id="b"),
            label="joined",
        )
    job.run()
    job.save(tmp_path / "job.json")
    nodes = json.loads((tmp_path / "job.json").read_text())["pipeline"]["nodes"]
    (descriptor,) = [d for d in nodes.values() if d.get("label") == "joined"]
    # A node rebuilt without its operator is what this test is about.
    stub = OperatorJobNode.from_descriptor(
        descriptor, operator=None, input_streams=(), databases={}
    )
    table = stub.as_table()
    assert table.num_rows == 0
    assert sorted(table.column_names) == ["a", "b", "k"]


# ---------------------------------------------------------------------------
# Staleness and summaries
# ---------------------------------------------------------------------------


def test_a_bound_source_with_no_modified_time_makes_its_slot_unknown() -> None:
    """A source slot bound to a stream with no modified time has none."""
    stream = ArrowTableStream(pa.table({"k": [0], "x": [1]}), tag_columns=["k"])
    slot = SourceJobNode(name="s", bound_source=stream)
    assert slot.last_modified is not None
    stream._set_modified_time(None)  # pyright: ignore[reportPrivateUsage]  # an unknown modified time is what this test is about
    assert slot.last_modified is None


def test_freshness_of_shared_ancestors_is_checked_once_per_traversal() -> None:
    """Five split-and-join stages: the check visits each node once, not once per path.

    (Five stages: building a deeper job of this shape is itself slow, independently of
    this check.)
    """
    from orcapod.core.streams.base import StreamBase

    job = PipelineJob(name="d", database=InMemoryArrowDatabase())
    with job:
        current: Any = inc.pod(_source(), label="start")
        for stage in range(5):
            left = MapData({"y": f"l{stage}"})(current, label=f"left{stage}")
            right = MapData({"y": f"r{stage}"})(current, label=f"right{stage}")
            joined = Join()(left, right, label=f"join{stage}")
            picked = SelectDataColumns([f"l{stage}"])(joined, label=f"pick{stage}")
            current = MapData({f"l{stage}": "y"})(picked, label=f"back{stage}")
    job.run()
    original = StreamBase.is_stale.fget
    assert original is not None
    calls = 0

    def counting(self: Any) -> bool:
        """Count the staleness checks, then check."""
        nonlocal calls
        calls += 1
        return original(self)

    with mock.patch.object(StreamBase, "is_stale", property(counting)):
        assert job.nodes["back4"].holds_current_run()
    # One own-staleness check per job node of the ancestry (26 nodes), not per path.
    assert calls <= 26


def test_a_held_dependency_keeps_its_incomplete_inputs_in_the_summary() -> None:
    """A -> B -> C, load-only: A skipped inputs; C cleared and loaded again still names A."""
    db = InMemoryArrowDatabase()
    _chain_job(ROWS[:3], db).run()
    job = _chain_job(ROWS, db)
    job.run(load_only=True)
    job.nodes["c"].clear_cache()
    summary = job.run(load_only=True)
    assert summary.nodes["c"].inputs_incomplete == ("a",)


def test_a_run_with_every_node_held_says_so() -> None:
    """The headline of a run with every node held says there was nothing to do."""
    job = _grouped_job(ROWS, InMemoryArrowDatabase())
    job.run()
    summary = job.run()
    assert repr(summary).splitlines()[0] == (
        "Run of job 'j': nothing to do; every node holds a current run."
    )


# ---------------------------------------------------------------------------
# A loaded job missing a node (ITL-1094) fails load-only with a clear error
# ---------------------------------------------------------------------------


def _deliver_x(x: int, ctx: Any) -> None:
    """A side effect over the source's own column."""


def test_a_loaded_job_without_its_side_effect_node_names_it(tmp_path: Path) -> None:
    """A loaded job missing a side-effect node fails load-only naming ITL-1094."""
    tap = side_effect_pod(_deliver_x)
    assert isinstance(tap, SideEffectPod)
    db = InMemoryArrowDatabase()
    job = PipelineJob(name="m", database=db)
    with job:
        tapped: Any = tap.process(_source(), label="delivered")
        inc.pod(tapped, label="down")
    job.run()
    job.save(tmp_path / "job.json")
    loaded = PipelineJob.load(tmp_path / "job.json", database=db)
    loaded.bind(sources={"s": _source()})
    with pytest.raises(RuntimeError, match=r"'down'.*ITL-1094"):
        loaded.run(load_only=True)


# ---------------------------------------------------------------------------
# Second review round
# ---------------------------------------------------------------------------

FLAKY = {"fail": True}


@function_pod(output_keys="y")
def flaky(x: int) -> int:
    """Return ``x + 1``; raise for ``x == 20`` while ``FLAKY['fail']`` is set."""
    CALLS.append("flaky")
    if x == 20 and FLAKY["fail"]:
        raise ValueError("transient failure")
    return x + 1


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_a_run_with_failed_rows_is_retried_by_the_next_run(orchestrator: str) -> None:
    """A run that left failed rows is never current: the next run computes exactly
    the failed rows and reads the rest; a third, unchanged run runs nothing."""
    FLAKY["fail"] = True
    job = PipelineJob(name="r", database=InMemoryArrowDatabase())
    with job:
        flaky.pod(_source(), label="node")
    with pytest.warns(Warning):
        job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    assert job.nodes["node"].as_table().num_rows == 5
    CALLS.clear()
    FLAKY["fail"] = False
    summary = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    assert CALLS == ["flaky"]
    assert (summary.nodes["node"].computed, summary.nodes["node"].reused) == (1, 5)
    assert job.nodes["node"].as_table().num_rows == 6
    CALLS.clear()
    summary = job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    assert CALLS == []
    assert summary.nodes["node"].held == 6


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_an_unchanged_rerun_touches_no_store(orchestrator: str) -> None:
    """A re-run with nothing to run flushes nothing."""
    db = InMemoryArrowDatabase()
    job = _grouped_job(ROWS, db)
    job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    _add_pending_write(db)
    before = _pending(db)
    with mock.patch.object(
        InMemoryArrowDatabase, "flush", side_effect=AssertionError("flushed")
    ):
        job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    assert _pending(db) == before


def test_a_held_load_only_node_keeps_its_missing_inputs_in_the_summary() -> None:
    """A held load-only node still reports the inputs it skipped."""
    db = InMemoryArrowDatabase()
    _grouped_job(ROWS[:3], db).run()
    job = _grouped_job(ROWS, db)
    job.run(load_only=True)
    summary = job.run(load_only=True)
    rowwise = summary.nodes["rowwise"]
    assert (rowwise.held, rowwise.not_stored) == (3, 3)
    assert "held from the previous run (3 inputs without a stored result)" in repr(
        summary
    )


class _Stop(BaseException):
    """An interruption that is not an ``Exception``, as a cancellation is."""


class StopsAtNodeEnd(NoOpObserver):
    """An observer that interrupts the run at the end of one node."""

    def __init__(self, label: str) -> None:
        """Keep the label of the node this observer acts at."""
        self.label = label

    def on_node_end(self, node_label: str, node_hash: str) -> None:
        """Interrupt at the node's end."""
        if node_label == self.label:
            raise _Stop()

    def contextualize(self, *identity_path: str) -> StopsAtNodeEnd:
        """Keep this observer for every node."""
        return self


def test_an_interrupted_run_clears_the_nodes_it_ran() -> None:
    """A run interrupted by a non-Exception clears the nodes it ran."""
    job = _grouped_job(ROWS, InMemoryArrowDatabase())
    with pytest.raises(_Stop):
        job.run(observer=StopsAtNodeEnd("summed"))
    assert job.nodes["rowwise"].as_table().num_rows == 0
    assert job.nodes["rowwise"].is_stale


def test_a_loaded_operator_that_lost_an_input_names_it(tmp_path: Path) -> None:
    """A loaded operator whose saved input was not restored names ITL-1094."""
    tap = side_effect_pod(_deliver_x)
    assert isinstance(tap, SideEffectPod)
    db = InMemoryArrowDatabase()
    job = PipelineJob(name="m2", database=db)
    with job:
        tapped: Any = tap.process(_source(), label="delivered")
        Join()(
            tapped,
            DictSource(
                [{"g": r["g"], "k": r["k"], "w": r["x"]} for r in ROWS],
                tag_columns=["g", "k"],
                source_id="w",
            ),
            label="joined",
        )
    job.run()
    job.save(tmp_path / "job.json")
    loaded = PipelineJob.load(tmp_path / "job.json", database=db)
    loaded.bind(
        sources={
            "s": _source(),
            "w": DictSource(
                [{"g": r["g"], "k": r["k"], "w": r["x"]} for r in ROWS],
                tag_columns=["g", "k"],
                source_id="w",
            ),
        }
    )
    with pytest.raises(RuntimeError, match=r"'joined'.*ITL-1094"):
        loaded.run(load_only=True)


def test_an_async_cache_only_node_holds_the_rows_it_sent() -> None:
    """An async CACHE_ONLY node holds the stored rows it sent."""
    from orcapod.channels import Channel
    from orcapod.core.nodes import FunctionJobNode
    from orcapod.pipeline.serialization import LoadStatus

    table = pa.table(
        {"k": [0, 1], "x": [1, 2]},
        schema=pa.schema(
            [
                pa.field("k", pa.int64(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        ),
    )
    db = InMemoryArrowDatabase()

    def node() -> Any:
        """A function node over the two-row table, on the shared store."""
        return FunctionJobNode(
            inc.pod,  # pyright: ignore[reportArgumentType]  # the decorator's pod type is not declared as FunctionPodProtocol
            ArrowTableStream(table, tag_columns=["k"]),
            pipeline_database=db,
        )

    node().run()
    cached = node()
    # A node whose input is unavailable (as from_descriptor makes it) is the case here.
    cached._load_status = LoadStatus.CACHE_ONLY  # pyright: ignore[reportPrivateUsage]  # the load status is what this test is about

    async def run() -> list[Any]:
        """Run the node's channel entry point."""
        inputs: Channel[Any] = Channel(buffer_size=4)
        output: Channel[Any] = Channel(buffer_size=4)
        await inputs.writer.close()
        # The channel entry point is what this test is about.
        await cached.async_execute(inputs.reader, output.writer)
        return await output.reader.collect()

    sent = asyncio.run(run())
    assert len(sent) == 2
    assert cached.as_table().num_rows == 2


@pytest.mark.parametrize("orchestrator", list(ORCHESTRATORS))
def test_every_node_a_load_only_run_fills_says_so(orchestrator: str) -> None:
    """Function, operator and side-effect nodes filled by a load-only run show
    ``loaded_only``; a source node is read as in any run and shows its rows."""
    db = InMemoryArrowDatabase()
    _tap_job(db).run()
    job = _tap_job(db)
    job.run(orchestrator=ORCHESTRATORS[orchestrator](), load_only=True)
    for label in ("rows", "tap", "down"):
        assert job.nodes[label].head().status is ViewStatus.LOADED_ONLY, label
    assert job.nodes["tap"].run(load_only=True).status is ViewStatus.LOADED_ONLY
    assert job.nodes["s"].head().status is ViewStatus.ROWS


# ---------------------------------------------------------------------------
# Third review round
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("error_policy", ["continue", "fail_fast"])
def test_a_direct_async_run_drains_the_inputs_of_a_held_node(
    error_policy: Literal["continue", "fail_fast"],
) -> None:
    """A held node's incoming channel is read even when the caller passes the whole
    graph: a source with more rows than the channel buffer does not block the run."""
    from orcapod.pipeline.dag import OrcaDAG

    rows = [{"g": i % 2, "k": i, "x": i} for i in range(64)]
    job = _grouped_job(rows, InMemoryArrowDatabase())
    job.run()
    rowwise, grouped, summed = (job.nodes[n] for n in ("rowwise", "grouped", "summed"))
    source = rowwise.upstreams[0]
    dag: OrcaDAG[Any] = OrcaDAG()
    for upstream, downstream in (
        (source, rowwise),
        (rowwise, grouped),
        (grouped, summed),
    ):
        dag.add_edge(upstream, downstream)
    held = {rowwise: list(rowwise.iter_data())}
    summed.clear_cache()
    grouped.clear_cache()
    orchestrator = AsyncPipelineOrchestrator(buffer_size=2, error_policy=error_policy)

    async def run() -> None:
        """Run the whole graph with ``rowwise`` held, under a time limit."""
        await asyncio.wait_for(orchestrator.run_async(dag, held_rows=held), timeout=30)

    asyncio.run(run())
    assert summed.as_table().num_rows == 2


def test_a_loaded_job_runs_its_bound_branch_beside_a_broken_unbound_one(
    tmp_path: Path,
) -> None:
    """The ITL-1094 check covers only the nodes the run includes: a branch below an
    unbound source and a dropped node is left out, the bound branch loads."""
    tap = side_effect_pod(_deliver_x)
    assert isinstance(tap, SideEffectPod)
    db = InMemoryArrowDatabase()
    job = PipelineJob(name="m3", database=db)
    with job:
        tapped: Any = tap.process(_source(), label="delivered")
        inc.pod(tapped, label="broken")
        dbl.pod(
            inc.pod(
                DictSource(
                    [{"g": 0, "k": 9, "x": 5}],
                    tag_columns=["g", "k"],
                    source_id="other",
                ),
                label="up2",
            ),
            label="bound_branch",
        )
    job.run()
    job.save(tmp_path / "job.json")
    loaded = PipelineJob.load(tmp_path / "job.json", database=db)
    loaded.bind(
        sources={
            "other": DictSource(
                [{"g": 0, "k": 9, "x": 5}], tag_columns=["g", "k"], source_id="other"
            )
        }
    )
    loaded.run(load_only=True)
    assert loaded.nodes["bound_branch"].as_table().num_rows == 1
    assert loaded.nodes["broken"].as_table().num_rows == 0


def test_a_cache_only_node_keeps_its_rows_over_an_input_with_no_modified_time() -> None:
    """``as_table`` judges a ``CACHE_ONLY`` node as ``head`` and ``iter_data`` do: its
    unavailable input does not make the rows it holds stale."""
    from orcapod.channels import Channel
    from orcapod.core.nodes import FunctionJobNode
    from orcapod.pipeline.serialization import LoadStatus

    table = pa.table(
        {"k": [0, 1], "x": [1, 2]},
        schema=pa.schema(
            [
                pa.field("k", pa.int64(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
            ]
        ),
    )
    db = InMemoryArrowDatabase()
    stream = ArrowTableStream(table, tag_columns=["k"])
    FunctionJobNode(
        inc.pod,  # pyright: ignore[reportArgumentType]  # the decorator's pod type is not declared as FunctionPodProtocol
        stream,
        pipeline_database=db,
    ).run()
    cached = FunctionJobNode(
        inc.pod,  # pyright: ignore[reportArgumentType]  # the decorator's pod type is not declared as FunctionPodProtocol
        stream,
        pipeline_database=db,
    )
    # An unavailable input (as from_descriptor makes it) is the case here.
    cached._load_status = LoadStatus.CACHE_ONLY  # pyright: ignore[reportPrivateUsage]  # the load status is what this test is about

    async def run() -> None:
        """Feed nothing: a CACHE_ONLY node serves what is stored."""
        inputs: Channel[Any] = Channel(buffer_size=4)
        output: Channel[Any] = Channel(buffer_size=4)
        await inputs.writer.close()
        await cached.async_execute(inputs.reader, output.writer)
        _ = await output.reader.collect()

    asyncio.run(run())
    stream._set_modified_time(None)  # pyright: ignore[reportPrivateUsage]  # an input with no modified time is what this test is about
    assert cached.as_table().num_rows == 2
    assert len(list(cached.iter_data())) == 2


def test_a_commit_interrupted_by_a_non_exception_clears_the_nodes_it_ran() -> None:
    """An interruption during the end-of-run commit (the last flush of the run) leaves
    its outcome unknown: the nodes the run ran hold nothing."""
    original = InMemoryArrowDatabase.flush
    calls = 0

    def counting(self: Any) -> None:
        """Count the flushes of a successful run."""
        nonlocal calls
        calls += 1
        original(self)

    with mock.patch.object(InMemoryArrowDatabase, "flush", counting):
        _grouped_job(ROWS, InMemoryArrowDatabase()).nodes["summed"].run()
    total, calls = calls, 0

    def interrupt_last(self: Any) -> None:
        """Interrupt the final flush, as a ``KeyboardInterrupt`` would."""
        nonlocal calls
        calls += 1
        if calls == total:
            raise _Stop()
        original(self)

    job = _grouped_job(ROWS, InMemoryArrowDatabase())
    with (
        mock.patch.object(InMemoryArrowDatabase, "flush", interrupt_last),
        pytest.raises(_Stop),
    ):
        job.nodes["summed"].run()
    for label in ("rowwise", "grouped", "summed"):
        assert job.nodes[label].as_table().num_rows == 0, label


def test_a_job_run_whose_final_commit_is_interrupted_clears_the_nodes_it_ran() -> None:
    """An interruption during ``job.run()``'s end-of-run commit (its last flush) leaves
    the outcome unknown: the nodes the run ran hold nothing, as for ``node.run()``."""
    original = InMemoryArrowDatabase.flush
    calls = 0

    def counting(self: Any) -> None:
        """Count the flushes of a successful run."""
        nonlocal calls
        calls += 1
        original(self)

    with mock.patch.object(InMemoryArrowDatabase, "flush", counting):
        _grouped_job(ROWS, InMemoryArrowDatabase()).run()
    total, calls = calls, 0

    def interrupt_last(self: Any) -> None:
        """Interrupt the final flush, as a ``KeyboardInterrupt`` would."""
        nonlocal calls
        calls += 1
        if calls == total:
            raise _Stop()
        original(self)

    job = _grouped_job(ROWS, InMemoryArrowDatabase())
    with (
        mock.patch.object(InMemoryArrowDatabase, "flush", interrupt_last),
        pytest.raises(_Stop),
    ):
        job.run()
    for label in ("rowwise", "grouped", "summed"):
        assert job.nodes[label].as_table().num_rows == 0, label
        assert job.nodes[label].is_stale, label
