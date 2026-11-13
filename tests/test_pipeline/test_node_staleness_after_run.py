"""After a completed run no job node is stale, on either orchestrator (ITL-1042).

A job node stamps its modified time when its run completes, after it has read all its inputs
and before it tells its downstream nodes it is done. So after ``job.run()``, sync or async,
every node is at least as new as its upstreams: ``head()`` shows the rows ``as_table()`` and
``iter_data()`` return, and none of them reads the store again.

The job: source ``A`` (tags ``id``, ``g``; data ``w``, ``x``; rows not in tag order) feeds the
function pod ``scaled``, which takes only ``x``, so calling it adds an automatic
``SelectDataColumns`` node before it (ITL-807). ``scaled`` feeds ``joined`` (``Join`` with
source ``B``) and ``grouped`` (``GroupBy`` on ``g``).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable, Iterator
from pathlib import Path
from typing import Any
from unittest import mock

import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.channels import Channel
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionJobNode
from orcapod.core.operators import GroupBy, Join
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.pipeline.job import PipelineJob
from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.streams import HeadStatus
from orcapod.types import CacheMode

# The input order of A's rows (not in tag order), and the order the async rows complete
# in when the gate is armed: the reverse.
A_IDS = [3, 1, 4, 0, 2]
COMPLETION_ORDER = list(reversed(A_IDS))

ORCHESTRATORS = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}

# The read methods of the store protocol: a read after a run is counted through them.
STORE_READS = (
    "get_all_records",
    "get_records_with_column_value",
    "get_records_by_ids",
    "get_record_by_id",
)

CALLS: list[int] = []


class _ReverseGate:
    """Make the gated rows of an async run complete in ``COMPLETION_ORDER``, without sleeps.

    The row ``x`` returns only after the row before it in the order has returned. Disarmed,
    rows return at once: a sync run calls them one by one, so an armed gate would wait forever.
    """

    def __init__(self) -> None:
        """Build a disarmed gate."""
        self.armed = False
        self.completed: list[int] = []
        self._events: dict[int, asyncio.Event] = {}

    def arm(self) -> None:
        """Arm the gate for the next run and forget earlier completions."""
        self.armed = True
        self.completed = []
        self._events = {}

    def disarm(self) -> None:
        """Let rows return as the scheduler runs them."""
        self.armed = False
        self._events = {}

    def _event(self, key: int) -> asyncio.Event:
        """Return the event set when row ``key`` returns."""
        return self._events.setdefault(key, asyncio.Event())

    async def wait_turn(self, key: int) -> None:
        """Return once the row before ``key`` in the order has returned.

        The timeout is a deadlock guard only; a passing run never reaches it.
        """
        if not self.armed:
            return
        position = COMPLETION_ORDER.index(key)
        if position:
            await asyncio.wait_for(
                self._event(COMPLETION_ORDER[position - 1]).wait(), timeout=30
            )

    def finish(self, key: int) -> None:
        """Record that row ``key`` returned and release the next one."""
        self.completed.append(key)
        if self.armed:
            self._event(key).set()


GATE = _ReverseGate()


@function_pod(output_keys="y")
async def scale(id_: int, x: int) -> int:
    """Return ``10 * x``, completing in the gate's order; counts every call."""
    await GATE.wait_turn(id_)
    CALLS.append(x)
    GATE.finish(id_)
    return 10 * x


@pytest.fixture(autouse=True)
def _reset() -> Iterator[None]:
    """Clear the call record and leave the gate disarmed around each test."""
    CALLS.clear()
    GATE.disarm()
    yield
    GATE.disarm()


def _table(columns: dict[str, list[int]]) -> pa.Table:
    """A table of non-nullable ``int64`` columns."""
    return pa.Table.from_arrays(
        [pa.array(values, type=pa.int64()) for values in columns.values()],
        schema=pa.schema(
            [pa.field(name, pa.int64(), nullable=False) for name in columns]
        ),
    )


def _store(kind: str, tmp_path: Path) -> ArrowDatabaseProtocol:
    """A fresh store of the given kind."""
    if kind == "in_memory":
        return InMemoryArrowDatabase()
    return DeltaTableDatabase(base_path=tmp_path / "store")


def _job(store: ArrowDatabaseProtocol) -> PipelineJob:
    """The job of the module docstring, ``joined`` cached in the store (LOG)."""
    a = ArrowTableSource(
        _table(
            {
                "id": A_IDS,
                "g": [i % 2 for i in A_IDS],
                "id_": A_IDS,
                "w": [0] * len(A_IDS),
                "x": [i + 1 for i in A_IDS],
            }
        ),
        tag_columns=["id", "g"],
        source_id="a",
        label="A",
    )
    b = ArrowTableSource(
        _table({"id": [2, 0, 4, 1, 3], "z": [20, 0, 40, 10, 30]}),
        tag_columns=["id"],
        source_id="b",
        label="B",
    )
    job = PipelineJob(name="itl1042", store=store)
    with job:
        scaled = scale.pod(a, label="scaled")
        Join()(scaled, b, label="joined")
        GroupBy(by=["g"])(scaled, label="grouped")
    # OperatorNode.attach_databases is the public way to store an operator's rows.
    job.nodes["joined"].attach_databases(
        pipeline_database=store.at("itl1042"), cache_mode=CacheMode.LOG
    )
    return job


def _run(job: PipelineJob, orchestrator: str, *, gated: bool) -> None:
    """Run ``job``; on the async orchestrator with ``gated``, rows complete in reverse."""
    if gated:
        GATE.arm()
    try:
        job.run(orchestrator=ORCHESTRATORS[orchestrator]())
    finally:
        GATE.disarm()


def _computed_nodes(job: PipelineJob) -> dict[str, Any]:
    """The job's nodes that compute and keep results (every node with ``is_stale``)."""
    return {
        label: node for label, node in job.nodes.items() if hasattr(node, "is_stale")
    }


def _count_store_reads(
    store: ArrowDatabaseProtocol,
) -> tuple[list[str], contextlib.ExitStack]:
    """Start counting reads of ``store`` (every view of it); closing the stack stops it."""
    reads: list[str] = []
    store_class = type(store)

    def counting(name: str) -> Callable[..., Any]:
        original = getattr(store_class, name)

        def read(self: Any, *args: Any, **kwargs: Any) -> Any:
            reads.append(name)
            return original(self, *args, **kwargs)

        return read

    patches = contextlib.ExitStack()
    for name in STORE_READS:
        patches.enter_context(mock.patch.object(store_class, name, counting(name)))
    return reads, patches


RUNS = [
    pytest.param(
        orchestrator, store_kind, gated, id=f"{orchestrator}-{store_kind}{tag}"
    )
    for store_kind in ("in_memory", "delta")
    for orchestrator, gated, tag in (
        ("sync", False, ""),
        ("async", False, ""),
        ("async", True, "-reversed"),
    )
]


@pytest.mark.parametrize(("orchestrator", "store_kind", "gated"), RUNS)
def test_no_node_is_stale_after_a_run(
    orchestrator: str, store_kind: str, gated: bool, tmp_path: Path
) -> None:
    """Right after a run every node is at least as new as its upstreams."""
    job = _job(_store(store_kind, tmp_path))
    _run(job, orchestrator, gated=gated)
    if gated:
        assert GATE.completed == COMPLETION_ORDER
    nodes = _computed_nodes(job)
    assert {"SelectDataColumns", "scaled", "joined", "grouped"} <= set(nodes)
    assert {label: node.is_stale for label, node in nodes.items()} == {
        label: False for label in nodes
    }


@pytest.mark.parametrize(("orchestrator", "store_kind", "gated"), RUNS)
def test_heads_show_the_rows_after_the_function_node_is_read(
    orchestrator: str, store_kind: str, gated: bool, tmp_path: Path
) -> None:
    """Reading ``scaled`` first does not make ``joined`` or ``grouped`` look stale."""
    job = _job(_store(store_kind, tmp_path))
    _run(job, orchestrator, gated=gated)
    _ = job.nodes["scaled"].as_table()
    _ = list(job.nodes["scaled"].iter_data())
    for label, node in _computed_nodes(job).items():
        head = node.head(100)
        table = node.as_table()
        assert head.status is HeadStatus.ROWS, (label, head.header())
        assert head.table.num_rows == table.num_rows == len(list(node.iter_data()))
        assert head.table.equals(table), label
        assert not node.is_stale, label
    assert job.nodes["scaled"].as_table().num_rows == len(A_IDS)
    assert job.nodes["joined"].as_table().num_rows == len(A_IDS)
    assert job.nodes["grouped"].as_table().num_rows == 2


@pytest.mark.parametrize(("orchestrator", "store_kind", "gated"), RUNS)
def test_reads_after_a_run_compute_nothing_and_read_no_store(
    orchestrator: str, store_kind: str, gated: bool, tmp_path: Path
) -> None:
    """After a run, every node's head, table and rows come from memory."""
    store = _store(store_kind, tmp_path)
    job = _job(store)
    _run(job, orchestrator, gated=gated)
    CALLS.clear()
    reads, counting = _count_store_reads(store)
    with counting:
        for node in _computed_nodes(job).values():
            _ = node.head(100)
            _ = node.as_table(all_info=True)
            _ = list(node.iter_data())
    assert reads == []
    assert CALLS == []


@pytest.mark.parametrize(("orchestrator", "store_kind", "gated"), RUNS)
def test_a_second_run_leaves_no_node_stale(
    orchestrator: str, store_kind: str, gated: bool, tmp_path: Path
) -> None:
    """A second run of the same job computes nothing again and leaves nothing stale."""
    job = _job(_store(store_kind, tmp_path))
    _run(job, orchestrator, gated=gated)
    _ = job.nodes["scaled"].as_table()
    CALLS.clear()
    _run(job, orchestrator, gated=False)
    assert CALLS == []
    for label, node in _computed_nodes(job).items():
        assert not node.is_stale, label
        assert node.head(100).status is HeadStatus.ROWS, label


@pytest.mark.parametrize("with_store", [False, True], ids=["no_store", "store"])
def test_a_function_node_is_stamped_before_its_output_closes(with_store: bool) -> None:
    """A reader that sees the node's output close sees the node already stamped.

    A downstream node stamps itself after it has seen that close, so it is then newer than
    the function node. Without a store the pod closes the writer it is given; the node must
    still stamp first.
    """
    source = ArrowTableSource(
        _table({"id": A_IDS, "id_": A_IDS, "x": [i + 1 for i in A_IDS]}),
        tag_columns=["id"],
        source_id="a",
        label="A",
    )
    store = InMemoryArrowDatabase() if with_store else None
    pod = scale.pod
    assert isinstance(pod, FunctionPod)
    node = FunctionJobNode(
        pod,
        source,
        pipeline_database=store,
        result_database=store,
    )
    constructed = node.last_modified
    assert constructed is not None
    seen_at_close: list[Any] = []

    async def run() -> None:
        """Feed the source to the node and record its stamp when the output closes."""
        inputs: Channel[Any] = Channel(buffer_size=16)
        output: Channel[Any] = Channel(buffer_size=16)

        async def feed() -> None:
            """Send every source row, then close the input."""
            for row in source.iter_data():
                await inputs.writer.send(row)
            await inputs.writer.close()

        async def read() -> None:
            """Read the output to its end, then note the node's modified time."""
            rows = await output.reader.collect()
            assert len(rows) == len(A_IDS)
            seen_at_close.append(node.last_modified)

        async with asyncio.TaskGroup() as group:
            group.create_task(feed())
            group.create_task(node.async_execute(inputs.reader, output.writer))
            group.create_task(read())

    asyncio.run(run())
    (stamp,) = seen_at_close
    assert stamp is not None and stamp > constructed
