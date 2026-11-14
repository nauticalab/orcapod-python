"""An async run never deadlocks when one node feeds both inputs of a later operator (ITL-1075).

The async orchestrator joins nodes with bounded channels. ``SemiJoin`` collected its right
input before it read its left one. When one node fed both inputs, the channel into the
unread left input filled, every node between the shared node and that channel blocked in
turn, the shared node could not finish its right-side rows, and the run waited forever.
A function node that computes absorbs its whole input (one task per row), which hid the
deadlock on a first run; a run that finds every row stored streams with backpressure and
hung. A graph of operators only hangs on its first run.

An operator node in ``CacheMode.REPLAY`` never read its inputs at all: with more upstream
rows than a channel holds, the upstream blocked and the run hung, with no fan-out.

Each run here is awaited under ``asyncio.wait_for``: a deadlock fails the test with
``TimeoutError`` instead of hanging the suite. No test sleeps.
"""

from __future__ import annotations

import asyncio
import functools
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.channels import Channel
from orcapod.core.operators import (
    DropDataColumns,
    Join,
    MapData,
    MergeJoin,
    SelectDataColumns,
    SemiJoin,
)
from orcapod.databases import InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.protocols.core_protocols import DataProtocol, TagProtocol
from orcapod.sources import DictSource
from orcapod.types import CacheMode

# Seconds one run may take. A run here takes well under two seconds; a deadlocked run
# never ends.
RUN_TIMEOUT = 30.0

N_ROWS = 1000  # far above every channel buffer in the cycle

BUFFER_SIZES = [1, 16, 64]


def make_source(n_rows: int) -> DictSource:
    """``n_rows`` rows tagged ``(g, k)`` with one data column ``x``."""
    rows = [{"g": i % 4, "k": i, "x": i} for i in range(n_rows)]
    return DictSource(rows, tag_columns=["g", "k"], source_id="src")


def run_async_job(job: PipelineJob, buffer_size: int) -> None:
    """Run ``job`` with the async orchestrator; fail with ``TimeoutError`` on a deadlock."""
    orchestrator = AsyncPipelineOrchestrator(buffer_size=buffer_size)
    asyncio.run(asyncio.wait_for(job.run_async(orchestrator=orchestrator), RUN_TIMEOUT))


def output_rows(job: PipelineJob) -> list[dict[str, Any]]:
    """The rows of node ``OUT``, with system tags and source info, in a fixed order."""
    table = job.nodes["OUT"].as_table(columns={"system_tags": True, "source": True})
    names = sorted(table.column_names)
    return (
        table.select(names).sort_by([(name, "ascending") for name in names]).to_pylist()
    )


# ---------------------------------------------------------------------------
# A graph of operators only: A feeds the join that makes C, and the last operator.
# ---------------------------------------------------------------------------

LAST_OPERATORS = {
    "Join(C, A)": lambda c, a: Join()(c, a, label="OUT"),
    "MergeJoin(C, A)": lambda c, a: MergeJoin()(c, a, label="OUT"),
    "SemiJoin(C, A)": lambda c, a: SemiJoin()(c, a, label="OUT"),
    "SemiJoin(A, C)": lambda c, a: SemiJoin()(a, c, label="OUT"),
}


def operator_job(last: str) -> PipelineJob:
    """``A`` and ``B`` from one source, ``C`` from ``Join(A, B)``, then ``last`` of C and A."""
    job = PipelineJob(name="fan_out", database=InMemoryArrowDatabase())
    with job:
        src = make_source(N_ROWS)
        a = MapData({"x": "a"})(src, label="A")
        b = MapData({"x": "b"})(src, label="B")
        joined = Join()(a, b, label="J")
        c = MapData({"b": "c"})(DropDataColumns(["a"])(joined, label="D"), label="C")
        LAST_OPERATORS[last](c, a)
    return job


@functools.cache
def sync_operator_rows(last: str) -> list[dict[str, Any]]:
    """The rows of ``operator_job(last)`` run once with the sync orchestrator."""
    job = operator_job(last)
    job.run(orchestrator=SyncPipelineOrchestrator())
    return output_rows(job)


@pytest.mark.parametrize("buffer_size", BUFFER_SIZES)
@pytest.mark.parametrize("last", list(LAST_OPERATORS))
def test_operator_fan_out_completes_on_both_runs(last: str, buffer_size: int) -> None:
    """Runs 1 and 2 finish and give the rows of a sync run."""
    expected = sync_operator_rows(last)
    assert expected  # the comparison is not vacuous

    job = operator_job(last)
    for _ in range(2):
        run_async_job(job, buffer_size)
        assert output_rows(job) == expected


# ---------------------------------------------------------------------------
# The graph of the issue: function pods A, B, C; SemiJoin(C, A).
# ---------------------------------------------------------------------------


@function_pod(output_keys="a")
def plus_one(x: int) -> int:
    """Return ``x + 1``."""
    return x + 1


@function_pod(output_keys="b")
def plus_two(x: int) -> int:
    """Return ``x + 2``."""
    return x + 2


@function_pod(output_keys="c")
def add(a: int, b: int) -> int:
    """Return ``a + b``."""
    return a + b


def function_job(store: InMemoryArrowDatabase, n_rows: int) -> PipelineJob:
    """The issue's graph: ``SemiJoin(C, A)`` where ``A`` also feeds ``C``."""
    job = PipelineJob(name="fan_out_pods", database=store)
    with job:
        src = make_source(n_rows)
        a = plus_one.pod(src, label="A")
        b = plus_two.pod(src, label="B")
        c = add.pod(Join()(a, b, label="J"), label="C")
        SemiJoin()(c, a, label="OUT")
    return job


def test_function_fan_out_completes_when_every_row_is_stored() -> None:
    """The second run, which finds every row stored, finishes with the rows of the first.

    300 rows at buffer size 1 deadlocked on ``main``; 1,000 rows at 1, 16 and 64 are in
    the measured table of the pull request (too slow for the default suite).
    """
    expected_job = function_job(InMemoryArrowDatabase(), 300)
    expected_job.run(orchestrator=SyncPipelineOrchestrator())
    expected = expected_job.nodes["OUT"].as_table()

    job = function_job(InMemoryArrowDatabase(), 300)
    for _ in range(2):
        run_async_job(job, buffer_size=1)
        got = job.nodes["OUT"].as_table()
        assert got.sort_by([("k", "ascending")]).equals(
            expected.sort_by([("k", "ascending")])
        )


# ---------------------------------------------------------------------------
# An operator node in REPLAY mode: it serves its stored rows and must still read its input.
# ---------------------------------------------------------------------------

REPLAY_ROWS = 200  # above both buffer sizes tested


def replay_job(store: InMemoryArrowDatabase) -> PipelineJob:
    """``A`` (a function pod) feeding operator node ``OUT``, which replays stored rows.

    ``OUT`` first runs once in ``CacheMode.LOG`` with the sync orchestrator, so it has
    stored rows; then it is switched to ``CacheMode.REPLAY``.
    """
    job = PipelineJob(name="replay", database=store)
    with job:
        a = plus_one.pod(make_source(REPLAY_ROWS), label="A")
        SelectDataColumns(["a"])(a, label="OUT")
    out = job.nodes["OUT"]
    # attach_databases is the public way to turn on LOG / REPLAY mode.
    out.attach_databases(pipeline_database=store.at("replay"), cache_mode=CacheMode.LOG)
    job.run(orchestrator=SyncPipelineOrchestrator())
    out.attach_databases(
        pipeline_database=store.at("replay"), cache_mode=CacheMode.REPLAY
    )
    return job


@pytest.mark.parametrize("buffer_size", [1, 64])
def test_replay_operator_node_reads_its_input(buffer_size: int) -> None:
    """A REPLAY run finishes and does what a sync REPLAY run does: no more, no less.

    In both, the upstream ``A`` still runs over every row (the sync orchestrator executes
    every node), and ``OUT`` gives its stored rows.
    """
    expected_job = replay_job(InMemoryArrowDatabase())
    expected_job.run(orchestrator=SyncPipelineOrchestrator())

    job = replay_job(InMemoryArrowDatabase())
    run_async_job(job, buffer_size)

    for label in ("A", "OUT"):
        got = job.nodes[label].as_table().sort_by([("k", "ascending")])
        expected = expected_job.nodes[label].as_table().sort_by([("k", "ascending")])
        assert got.num_rows == REPLAY_ROWS
        assert got.equals(expected)


class _ReaderThatSignals:
    """Reads a channel and sets ``reading`` when a read starts."""

    def __init__(self, channel: Channel[tuple[TagProtocol, DataProtocol]]) -> None:
        self._reader = channel.reader
        self.reading = asyncio.Event()

    async def receive(self) -> tuple[TagProtocol, DataProtocol]:
        self.reading.set()
        return await self._reader.receive()

    def __aiter__(self) -> "_ReaderThatSignals":
        return self

    async def __anext__(self) -> tuple[TagProtocol, DataProtocol]:
        self.reading.set()
        return await self._reader.__anext__()

    async def collect(self) -> list[tuple[TagProtocol, DataProtocol]]:
        return [row async for row in self]


class _FailingOutput:
    """A writable channel whose sends fail: it stands for a failed downstream node.

    A send fails only once ``after`` is set.
    """

    def __init__(self, after: asyncio.Event) -> None:
        self._after = after

    async def send(self, item: tuple[TagProtocol, DataProtocol]) -> None:
        _ = await self._after.wait()
        raise RuntimeError("downstream failed")

    async def close(self) -> None:
        pass


def test_failed_replay_leaves_no_input_reader_running() -> None:
    """When a REPLAY node fails, its input readers have ended by the time it raises."""

    async def run() -> set[asyncio.Task[Any]]:
        # ``asyncio.timeout``, unlike ``asyncio.wait_for`` on 3.11, adds no task.
        async with asyncio.timeout(RUN_TIMEOUT):
            node = replay_job(InMemoryArrowDatabase()).nodes["OUT"]
            upstream: Channel[tuple[TagProtocol, DataProtocol]] = Channel(4)
            reader = _ReaderThatSignals(upstream)
            # The replay fails once the node's reader of the upstream is waiting on
            # it: the upstream stays open.
            with pytest.raises(RuntimeError, match="downstream failed"):
                await node.async_execute([reader], _FailingOutput(after=reader.reading))
        return asyncio.all_tasks() - {asyncio.current_task()}

    assert asyncio.run(run()) == set()
