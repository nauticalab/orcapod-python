"""``error_policy`` means the same on both orchestrators (ITL-879).

Before the fix the async orchestrator stored ``error_policy`` and never used it: under
``"fail_fast"`` a row whose function raised was skipped as under ``"continue"`` and the run
finished. A failing node's exception also reached the caller inside an ``ExceptionGroup``.
Now ``"fail_fast"`` raises the row's own exception, as the sync orchestrator does, and an
async run raises one exception, never a group.

The jobs are driven through ``PipelineJob.run`` / ``run_async``. Every async run sits inside
an ``asyncio.timeout`` guard, so a hang fails the test instead of the suite.
"""

from __future__ import annotations

import asyncio
import logging
import warnings
from collections.abc import Callable, Iterator
from typing import Any, Literal

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import Batch
from orcapod.databases import InMemoryArrowDatabase
from orcapod.errors import RowFailuresWarning
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.pipeline import async_orchestrator as async_orchestrator_module
from orcapod.pipeline.observer import NoOpObserver
from orcapod.side_effects import InvocationContext, SideEffectPod, SideEffectPodConfig
from orcapod.sources import DictSource
from orcapod.types import CacheMode, PodConfig

RUN_TIMEOUT_SECONDS = 60
N_ROWS = 6

ORCHESTRATORS: dict[str, type[Any]] = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}

# Where the failing row sits among the source rows.
FAILING_POSITIONS = {"first": 0, "middle": N_ROWS // 2, "last": N_ROWS - 1}


# ---------------------------------------------------------------------------
# Functions under test: a row fails when its x is negative
# ---------------------------------------------------------------------------


def check(x: int) -> int:
    """Return ``x``; raise for a negative ``x``."""
    if x < 0:
        raise ValueError(f"bad x={x}")
    return x


checked = function_pod(output_keys="y")(check)


@function_pod(output_keys="z")
def double(y: int) -> int:
    """Return twice ``y``."""
    return 2 * y


@function_pod(output_keys="w")
def triple(y: int) -> int:
    """Return three times ``y``."""
    return 3 * y


def check_with_ctx(x: int, ctx: InvocationContext) -> int:
    """Return ``x``; raise for a negative ``x`` (a pod that takes the invocation context)."""
    if x < 0:
        raise ValueError(f"bad x={x}")
    return x


checked_with_ctx = FunctionPod.from_fn(
    check_with_ctx, output_keys="y", ctx_arg_name="ctx"
)


def deliver(x: int, ctx: InvocationContext) -> None:
    """A side effect that fails for a negative ``x``."""
    if x < 0:
        raise ValueError(f"bad x={x}")


def xs_failing_at(position: int) -> list[int]:
    """Source values ``0..N-1`` with the value at ``position`` made negative."""
    return [-(i + 1) if i == position else i for i in range(N_ROWS)]


# ---------------------------------------------------------------------------
# Jobs
# ---------------------------------------------------------------------------


def source_of(xs: list[int]) -> DictSource:
    """A source of rows ``k = r<i>``, ``x = xs[i]``."""
    rows = [{"k": f"r{i}", "x": x} for i, x in enumerate(xs)]
    return DictSource(
        data=rows, tag_columns=["k"], tag_only_columns=True, source_id="rows"
    )


def function_job(
    store: Any, xs: list[int], *, max_concurrency: int | None = None
) -> PipelineJob:
    """Source -> ``f`` (fails on a negative x) -> ``down`` (doubles)."""
    job = PipelineJob(name="itl879", database=store)
    with job:
        if max_concurrency is None:
            out = checked.pod(source_of(xs), label="f")
        else:
            pod = FunctionPod.from_fn(
                check,
                output_keys="y",
                pod_config=PodConfig(max_concurrency=max_concurrency),
            )
            out = pod(source_of(xs), label="f")
        double.pod(out, label="down")
    return job


def ctx_function_job(store: Any, xs: list[int]) -> PipelineJob:
    """Source -> ``f`` (a side-effect function pod failing on a negative x) -> ``down``."""
    job = PipelineJob(name="itl879", database=store)
    with job:
        out = checked_with_ctx(source_of(xs), label="f")
        double.pod(out, label="down")
    return job


def side_effect_job(
    store: Any, xs: list[int], on_error: Literal["raise", "log"]
) -> PipelineJob:
    """Source -> side-effect node ``se`` whose delivery fails on a negative x."""
    pod = SideEffectPod(
        deliver, config=SideEffectPodConfig(on_error=on_error), name="deliver"
    )
    job = PipelineJob(name="itl879", database=store)
    with job:
        pod(source_of(xs), label="se")
    return job


FUNCTION_JOBS: dict[str, Callable[[Any, list[int]], PipelineJob]] = {
    "function": function_job,
    "ctx_function": ctx_function_job,
}


class Recorder(NoOpObserver):
    """Records ``(event, node label, row key)`` for every observer call."""

    def __init__(self) -> None:
        """Start with no events."""
        self.events: list[tuple[str, str, str | None]] = []

    def on_node_start(self, node_label: str, node_hash: str, tag_schema: Any = None):
        """Record a node start."""
        self.events.append(("node_start", node_label, None))

    def on_node_end(self, node_label: str, node_hash: str) -> None:
        """Record a node end."""
        self.events.append(("node_end", node_label, None))

    def on_data_end(self, node_label, tag, input_data, output_data, cached) -> None:
        """Record a row end."""
        self.events.append(("data_end", node_label, str(tag.as_dict()["k"])))

    def on_data_crash(self, node_label, tag, data, error) -> None:
        """Record a row crash."""
        self.events.append(("data_crash", node_label, str(tag.as_dict()["k"])))

    def of(self, event: str, label: str) -> list[str | None]:
        """The row keys (or ``None``) of ``event`` calls for ``label``."""
        return [k for e, lbl, k in self.events if e == event and lbl == label]


def run_job(
    job: PipelineJob, orchestrator: str, policy: str, observer: Any = None
) -> Any:
    """Run ``job`` on ``orchestrator`` with ``policy``; async runs have a timeout guard."""
    orch = ORCHESTRATORS[orchestrator](error_policy=policy)
    if orchestrator == "sync":
        return job.run(orchestrator=orch, observer=observer)

    async def guarded() -> Any:
        """Run the job, failing after ``RUN_TIMEOUT_SECONDS`` instead of hanging."""
        async with asyncio.timeout(RUN_TIMEOUT_SECONDS):
            return await job.run_async(orchestrator=orch, observer=observer)

    return asyncio.run(guarded())


def ks(job: PipelineJob, label: str) -> list[str]:
    """The sorted row keys node ``label`` of ``job`` reads."""
    return sorted(job.nodes[label].as_table().column("k").to_pylist())


def stored(store: Any, build: Callable[[Any, list[int]], PipelineJob], label: str):
    """Every row ``store`` holds for ``label``: the node's stored view (``as_source()``;
    a node itself shows only its current run, ITL-1095)."""
    return build(store, xs_failing_at(0)).nodes[label].as_source().as_table()


@pytest.fixture(params=list(ORCHESTRATORS))
def orchestrator(request: pytest.FixtureRequest) -> str:
    """Each orchestrator's name."""
    return request.param


@pytest.fixture(params=list(FUNCTION_JOBS))
def build(request: pytest.FixtureRequest) -> Callable[[Any, list[int]], PipelineJob]:
    """Each kind of function job."""
    return FUNCTION_JOBS[request.param]


@pytest.fixture
def caught_warnings() -> Iterator[list[warnings.WarningMessage]]:
    """Every warning issued in the test."""
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        yield caught


def row_failure_warnings(caught: list[warnings.WarningMessage]) -> list[str]:
    """The messages of the ``RowFailuresWarning``s in ``caught``."""
    return [str(w.message) for w in caught if w.category is RowFailuresWarning]


# ---------------------------------------------------------------------------
# continue: already the same on both orchestrators; locked here
# ---------------------------------------------------------------------------


def test_continue_skips_and_reports_the_row(
    orchestrator: str, build: Callable[..., PipelineJob], caught_warnings: list[Any]
) -> None:
    """The failing row is skipped, counted, warned about once and seen by the observer."""
    position = FAILING_POSITIONS["middle"]
    job = build(InMemoryArrowDatabase(), xs_failing_at(position))
    recorder = Recorder()

    summary = run_job(job, orchestrator, "continue", recorder)

    node = summary.nodes["f"]
    assert (node.computed, node.reused, node.failed) == (N_ROWS - 1, 0, 1)
    assert isinstance(node.first_error, ValueError)
    assert str(node.first_error) == f"bad x={-(position + 1)}"
    assert summary.nodes["down"].computed == N_ROWS - 1
    messages = row_failure_warnings(caught_warnings)
    assert len(messages) == 1
    assert f"1 of {N_ROWS} rows failed in node 'f'" in messages[0]
    assert recorder.of("data_crash", "f") == [f"r{position}"]
    assert recorder.of("node_end", "f") == [None]
    assert recorder.of("node_end", "down") == [None]
    expected = [f"r{i}" for i in range(N_ROWS) if i != position]
    assert ks(job, "f") == expected
    assert ks(job, "down") == expected


# ---------------------------------------------------------------------------
# fail_fast
# ---------------------------------------------------------------------------


def test_fail_fast_raises_the_row_error(
    orchestrator: str, build: Callable[..., PipelineJob], caught_warnings: list[Any]
) -> None:
    """The run raises the row's own exception: same type and message on both orchestrators."""
    position = FAILING_POSITIONS["middle"]
    job = build(InMemoryArrowDatabase(), xs_failing_at(position))
    recorder = Recorder()

    with pytest.raises(ValueError) as info:
        run_job(job, orchestrator, "fail_fast", recorder)

    assert type(info.value) is ValueError
    assert str(info.value) == f"bad x={-(position + 1)}"
    assert getattr(info.value, "__notes__", []) == []
    assert job.last_run is None
    assert row_failure_warnings(caught_warnings) == []
    assert recorder.of("data_crash", "f") == [f"r{position}"]
    assert recorder.of("node_end", "f") == [None]


@pytest.mark.parametrize("position", list(FAILING_POSITIONS))
def test_fail_fast_stores_only_correct_rows(
    orchestrator: str, build: Callable[..., PipelineJob], position: str
) -> None:
    """Every stored row is its input's correct result; the failing row is never stored."""
    store = InMemoryArrowDatabase()
    xs = xs_failing_at(FAILING_POSITIONS[position])
    with pytest.raises(ValueError):
        run_job(build(store, xs), orchestrator, "fail_fast")

    by_key = {f"r{i}": x for i, x in enumerate(xs)}
    f_rows = stored(store, build, "f").to_pylist()
    for row in f_rows:
        assert row["y"] == by_key[row["k"]] >= 0
    for row in stored(store, build, "down").to_pylist():
        assert row["z"] == 2 * by_key[row["k"]]
        assert by_key[row["k"]] >= 0
    if orchestrator == "sync":
        # The sync run stores exactly the rows before the failing one.
        assert sorted(r["k"] for r in f_rows) == [
            f"r{i}" for i in range(FAILING_POSITIONS[position])
        ]


@pytest.mark.parametrize("max_concurrency", [1, None])
@pytest.mark.parametrize("position", list(FAILING_POSITIONS))
def test_downstream_never_completes_on_partial_input(
    max_concurrency: int | None, position: str
) -> None:
    """The failing node's output ends early; its downstream node must not finish on it.

    The failing node closes its output as it stops. Were the downstream node to read that
    as a normal end of input before the run's cancellation reached it, it would complete
    on part of its input. It must be cancelled instead: no ``on_node_end``.
    """
    job = function_job(
        InMemoryArrowDatabase(),
        xs_failing_at(FAILING_POSITIONS[position]),
        max_concurrency=max_concurrency,
    )
    recorder = Recorder()

    with pytest.raises(ValueError):
        run_job(job, "async", "fail_fast", recorder)

    assert recorder.of("node_end", "down") == []


def test_max_concurrency_one_raises_the_first_failing_row(orchestrator: str) -> None:
    """With one row at a time, the first failing row in input order is the one raised."""
    xs = [0, -1, 2, -3, 4, 5]
    job = function_job(InMemoryArrowDatabase(), xs, max_concurrency=1)

    with pytest.raises(ValueError, match=r"^bad x=-1$"):
        run_job(job, orchestrator, "fail_fast")


def rows_of(table: Any) -> list[tuple[tuple[str, Any], ...]]:
    """The rows of ``table``, each a sorted tuple of (column, value), sorted."""
    return sorted(tuple(sorted(row.items())) for row in table.to_pylist())


@pytest.mark.parametrize("position", list(FAILING_POSITIONS))
def test_fail_fast_leaves_the_nodes_with_no_run(
    orchestrator: str, build: Callable[..., PipelineJob], position: str
) -> None:
    """After a fail_fast raise, the failing node and the node after it hold no run.

    A run that raised defines no rows (ITL-1093): each node it ran reads as a node with no
    run, which is empty (ITL-1095), also the downstream node the async run cancelled; what
    was stored stays readable through ``as_source()``. Another job stores rows of other
    values first, so the stored view holds both jobs' rows.
    """
    store = InMemoryArrowDatabase()
    run_job(build(store, [100 + i for i in range(N_ROWS)]), orchestrator, "continue")
    xs = xs_failing_at(FAILING_POSITIONS[position])
    job = build(store, xs)
    run_job(job, orchestrator, "continue")
    for label in ("f", "down"):
        assert len(job.nodes[label].as_table()) == N_ROWS - 1, label

    with pytest.raises(ValueError):
        run_job(job, orchestrator, "fail_fast")

    for label in ("f", "down"):
        node = job.nodes[label]
        assert node.as_table().num_rows == 0, label
        assert node.is_stale, label
        # Every stored row stays: the other job's rows and this job's.
        assert len(rows_of(node.as_source().as_table())) == 2 * N_ROWS - 1, label


# The row ``sibling`` leaves uncomputed until the run that is cancelled.
SIBLING_X = 2
sibling_mode = {"mode": "fail"}


@function_pod(output_keys="w")
async def sibling(x: int) -> int:
    """Return ``x``; at ``SIBLING_X`` fail, or wait until cancelled, as ``sibling_mode`` says."""
    if x == SIBLING_X:
        if sibling_mode["mode"] == "fail":
            raise ValueError("sibling failed")
        await asyncio.Event().wait()
    return x


def branches_job(store: Any, xs: list[int]) -> PipelineJob:
    """Source -> ``f`` (fails on a negative x), and source -> ``g`` (``sibling``)."""
    job = PipelineJob(name="itl879", database=store)
    with job:
        source = source_of(xs)
        checked.pod(source, label="f")
        sibling.pod(source, label="g")
    return job


def test_a_node_cancelled_on_another_branch_holds_no_run() -> None:
    """A node the async run cancels, not downstream of the failure, holds no run either.

    ``g`` waits at its one uncomputed row until the failure of ``f`` cancels it. Its
    previous run's rows are not held: it reads as a node with no run (ITL-1093), which is
    empty (ITL-1095); its stored rows stay readable through ``as_source()``.
    """
    store = InMemoryArrowDatabase()
    other = [100 + i for i in range(N_ROWS)]
    run_job(branches_job(store, other), "async", "continue")
    xs = xs_failing_at(N_ROWS - 1)
    job = branches_job(store, xs)
    sibling_mode["mode"] = "fail"
    run_job(job, "async", "continue")
    assert len(job.nodes["g"].as_table()) == N_ROWS - 1

    sibling_mode["mode"] = "wait"
    try:
        with pytest.raises(ValueError, match=r"^bad x="):
            run_job(job, "async", "fail_fast")
    finally:
        sibling_mode["mode"] = "fail"

    g = job.nodes["g"]
    assert g.as_table().num_rows == 0
    assert g.is_stale
    assert len(rows_of(g.as_source().as_table())) == 2 * N_ROWS - 1


def test_an_observer_failing_at_node_end_is_called_once(orchestrator: str) -> None:
    """An ``on_node_end`` that raises is not called again; its error is the run's."""

    class FailsAtNodeEnd(Recorder):
        """A recorder whose ``on_node_end`` raises for node ``f``."""

        def on_node_end(self, node_label: str, node_hash: str) -> None:
            """Record the node end, then raise for ``f``."""
            super().on_node_end(node_label, node_hash)
            if node_label == "f":
                raise RuntimeError("observer failed")

    recorder = FailsAtNodeEnd()
    job = function_job(InMemoryArrowDatabase(), [1, 2, 3])
    with pytest.raises(RuntimeError, match="^observer failed$") as info:
        run_job(job, orchestrator, "continue", recorder)

    assert type(info.value) is RuntimeError
    assert recorder.of("node_end", "f") == [None]


def test_nothing_left_running_after_fail_fast(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """After the raise, no task of the run is pending and every channel is closed."""
    created: list[Any] = []

    # The orchestrator's channels are internal; record them through its module's names.
    class RecordingChannel(async_orchestrator_module.Channel[Any]):
        """A ``Channel`` that records itself in ``created``."""

        def __post_init__(self) -> None:
            """Set up the channel, then record it."""
            super().__post_init__()
            created.append(self)

    class RecordingBroadcastChannel(async_orchestrator_module.BroadcastChannel[Any]):
        """A ``BroadcastChannel`` that records itself in ``created``."""

        def __init__(self, buffer_size: int = 64) -> None:
            """Set up the channel with ``buffer_size``, then record it."""
            super().__init__(buffer_size)
            created.append(self)

    monkeypatch.setattr(async_orchestrator_module, "Channel", RecordingChannel)
    monkeypatch.setattr(
        async_orchestrator_module, "BroadcastChannel", RecordingBroadcastChannel
    )
    job = PipelineJob(name="itl879", database=InMemoryArrowDatabase())
    with job:
        out = checked.pod(source_of(xs_failing_at(2)), label="f")
        double.pod(out, label="down")
        triple.pod(out, label="down2")  # fan-out: a broadcast channel

    async def main() -> set[asyncio.Task[Any]]:
        """Run the job under a timeout guard; return the tasks still pending after it."""
        async with asyncio.timeout(RUN_TIMEOUT_SECONDS):
            with pytest.raises(ValueError):
                await job.run_async(
                    orchestrator=AsyncPipelineOrchestrator(error_policy="fail_fast")
                )
        return asyncio.all_tasks() - {asyncio.current_task()}

    assert asyncio.run(main()) == set()
    assert any(isinstance(c, RecordingBroadcastChannel) for c in created)
    for channel in created:
        # Closed state is private to the channel classes.
        closed = channel._closed
        assert closed.is_set() if isinstance(closed, asyncio.Event) else closed


def stored_aggregates(
    job_store: Any, policy: str, xs: list[int], mc: int | None
) -> int:
    """Run source -> ``f`` -> ``Batch`` node ``B`` (LOG mode) async; count ``B``'s stored rows."""
    log = InMemoryArrowDatabase()
    job = function_job(job_store, xs, max_concurrency=mc)
    with job:
        Batch()(job.nodes["f"], label="B")
    job.nodes["B"].attach_databases(pipeline_database=log, cache_mode=CacheMode.LOG)
    if policy == "fail_fast":
        with pytest.raises(ValueError):
            run_job(job, "async", policy)
    else:
        run_job(job, "async", policy)
    tables = [log.get_all_records(path) for path in log.list_table_paths()]
    return sum(table.num_rows for table in tables if table is not None)


@pytest.mark.parametrize("max_concurrency", [1, None])
@pytest.mark.parametrize("position", list(FAILING_POSITIONS))
def test_an_aggregate_downstream_stores_nothing_after_fail_fast(
    max_concurrency: int | None, position: str
) -> None:
    """A ``Batch`` node after the failing node never stores an aggregate of partial input.

    The failing node closes its output before the run cancels the others; the aggregate
    must not take that close for the end of its input. Under ``continue`` the same job
    stores its one aggregate, so the database is read where the aggregate would be.
    """
    xs = xs_failing_at(FAILING_POSITIONS[position])
    assert (
        stored_aggregates(InMemoryArrowDatabase(), "continue", xs, max_concurrency) == 1
    )
    assert (
        stored_aggregates(InMemoryArrowDatabase(), "fail_fast", xs, max_concurrency)
        == 0
    )


# ---------------------------------------------------------------------------
# Side-effect pods: their own on_error, not error_policy
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("policy", ["continue", "fail_fast"])
def test_side_effect_raise_is_the_delivery_error(
    orchestrator: str, policy: str
) -> None:
    """``on_error="raise"``: the run raises the delivery's own error, never a group."""
    job = side_effect_job(InMemoryArrowDatabase(), xs_failing_at(2), "raise")

    with pytest.raises(ValueError) as info:
        run_job(job, orchestrator, policy)

    assert type(info.value) is ValueError
    assert str(info.value) == "bad x=-3"


def test_side_effect_log_skips_and_warns(
    orchestrator: str, caplog: pytest.LogCaptureFixture
) -> None:
    """``on_error="log"``: logged at WARNING, the run goes on."""
    job = side_effect_job(InMemoryArrowDatabase(), xs_failing_at(2), "log")

    with caplog.at_level(logging.WARNING, logger="orcapod"):
        run_job(job, orchestrator, "fail_fast")

    assert any(
        r.levelno == logging.WARNING and "delivery failed: bad x=-3" in r.getMessage()
        for r in caplog.records
    )


# ---------------------------------------------------------------------------
# Choosing the one exception of an async run
# ---------------------------------------------------------------------------


class _Node:
    """A stand-in node: only its label is read."""

    def __init__(self, label: str) -> None:
        """Name the node ``label``."""
        self.label = label


def test_single_failure_takes_the_earliest_node_and_notes_the_others() -> None:
    """The failure of the node first in topological order wins, whatever the group order."""
    # Calls the private helper directly: the test is about its ordering rule, which an
    # end-to-end run cannot force into both group orders.
    from orcapod.pipeline.async_orchestrator import _single_failure

    topo_order = [_Node("up"), _Node("down")]
    for order in ("up_first", "down_first"):
        up_error = ValueError("up failed")
        down_error = KeyError("down failed")
        failed_nodes = {id(up_error): "up", id(down_error): "down"}
        leaves = (
            [up_error, down_error] if order == "up_first" else [down_error, up_error]
        )
        group = ExceptionGroup("run", [ExceptionGroup("node", leaves)])

        chosen = _single_failure(group, topo_order, failed_nodes)

        assert chosen is up_error
        assert up_error.__notes__ == [
            "Node 'down' also failed in the same run: KeyError: 'down failed'"
        ]


def test_single_failure_alone_is_raised_unchanged() -> None:
    """One failure is returned as it is, with no note."""
    # Calls the private helper directly, as above.
    from orcapod.pipeline.async_orchestrator import _single_failure

    error = ValueError("only")
    chosen = _single_failure(
        ExceptionGroup("run", [error]), [_Node("f")], {id(error): "f"}
    )
    assert chosen is error
    assert getattr(error, "__notes__", None) is None
