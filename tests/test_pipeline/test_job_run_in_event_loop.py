"""A job runs with the async orchestrator from inside a running event loop (ITL-994).

A Jupyter kernel always runs an event loop, so ``asyncio.run`` cannot be called there. A job
run from inside a running loop must work, and must store exactly what a run outside a loop
stores: the same rows, record ids and hashes.
"""

from __future__ import annotations

import _thread
import asyncio
import threading
import time
import warnings
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import DictSource

# Columns that differ between any two runs by design, also between two script runs: the
# result's datagram uuid, the output's source-info token (which carries that uuid) and the
# time the result was computed.
PER_RUN_COLUMNS = ("__data_id", "_source_y", "__pod_ts")

STORES: dict[str, Callable[[Path], Any]] = {
    "in_memory": lambda _: InMemoryArrowDatabase(),
    "delta": lambda path: DeltaTableDatabase(path),
}


def run_inside_running_loop(job: PipelineJob) -> None:
    """Run ``job`` with the async orchestrator from inside a running event loop.

    The one place that names the call an attendee types in a notebook cell.
    """

    async def notebook_cell() -> None:
        """A notebook cell: the job run inside the kernel's running loop."""
        job.run(orchestrator=AsyncPipelineOrchestrator())

    asyncio.run(notebook_cell())


@function_pod(output_keys="y")
def double(x: int) -> int:
    """Return twice ``x``."""
    return 2 * x


def make_job(store: Any) -> PipelineJob:
    """A source of five rows feeding ``double``, recorded in a fresh job."""
    rows = [{"k": f"r{i}", "x": i} for i in range(5)]
    job = PipelineJob(name="loop_demo", database=store)
    with job:
        source = DictSource(data=rows, tag_columns=["k"], source_id="rows")
        double.pod(source, label="doubled")
    return job


def stored_rows(job: PipelineJob) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """The node's stored records and its table, minus per-run columns, sorted by tag."""
    records = job.nodes["doubled"].get_all_records(all_info=True)
    assert records is not None
    records = records.drop_columns(
        [c for c in PER_RUN_COLUMNS if c in records.column_names]
    )
    table = job.nodes["doubled"].as_table(all_info=True)
    table = table.drop_columns([c for c in PER_RUN_COLUMNS if c in table.column_names])

    def by_tag(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """The rows sorted by their tag ``k``."""
        return sorted(rows, key=lambda row: row["k"])

    return by_tag(records.to_pylist()), by_tag(table.to_pylist())


@pytest.mark.parametrize("store_kind", list(STORES))
def test_run_inside_running_loop_stores_what_a_script_run_stores(
    store_kind: str, tmp_path: Path
) -> None:
    """A run from inside a running loop stores the same rows, ids and hashes as a script run."""
    script_job = make_job(STORES[store_kind](tmp_path / "script"))
    script_job.run(orchestrator=AsyncPipelineOrchestrator())

    notebook_job = make_job(STORES[store_kind](tmp_path / "notebook"))
    run_inside_running_loop(notebook_job)

    assert stored_rows(notebook_job) == stored_rows(script_job)
    ys = sorted(notebook_job.nodes["doubled"].as_table().column("y").to_pylist())
    assert ys == [0, 2, 4, 6, 8]


def test_rerun_inside_running_loop_reuses_stored_rows(tmp_path: Path) -> None:
    """A second run inside a running loop reads the stored rows and changes none of them."""
    store = DeltaTableDatabase(tmp_path / "store")
    first_job = make_job(store)
    run_inside_running_loop(first_job)
    first = stored_rows(first_job)

    second_job = make_job(store)
    run_inside_running_loop(second_job)

    assert stored_rows(second_job) == first


def test_store_error_inside_running_loop_reaches_the_caller(tmp_path: Path) -> None:
    """A failure inside the run reaches the cell with its own type, not wrapped."""
    from orcapod.errors import StoreWriteError

    class FailingWrites:
        """A store whose every write fails."""

        def __init__(self, inner: Any) -> None:
            """Wrap ``inner``."""
            self._inner = inner

        def at(self, *path_components: str) -> FailingWrites:
            """A view whose writes fail too."""
            return FailingWrites(self._inner.at(*path_components))

        def add_record(self, *args: Any, **kwargs: Any) -> Any:
            """Fail."""
            raise OSError("disk unavailable")

        def add_records(self, *args: Any, **kwargs: Any) -> Any:
            """Fail."""
            raise OSError("disk unavailable")

        def __getattr__(self, name: str) -> Any:
            """Delegate everything else."""
            return getattr(self._inner, name)

    job = make_job(FailingWrites(InMemoryArrowDatabase()))
    with pytest.raises(StoreWriteError):
        run_inside_running_loop(job)


def test_run_inside_running_loop_returns_to_the_loop(tmp_path: Path) -> None:
    """The loop that called the run keeps working after it (a notebook's next cell)."""

    async def two_cells() -> int:
        """Run a job, then use the loop again, as the next cell would."""
        job = make_job(InMemoryArrowDatabase())
        job.run(orchestrator=AsyncPipelineOrchestrator())
        await asyncio.sleep(0)
        return pa.table({"n": [1]}).num_rows

    assert asyncio.run(two_cells()) == 1


@pytest.mark.parametrize("store_kind", list(STORES))
def test_await_run_async_stores_what_a_script_run_stores(
    store_kind: str, tmp_path: Path
) -> None:
    """``await job.run_async()`` stores the same rows, ids and hashes as a script run."""
    script_job = make_job(STORES[store_kind](tmp_path / "script"))
    script_job.run(orchestrator=AsyncPipelineOrchestrator())

    async_job = make_job(STORES[store_kind](tmp_path / "async"))

    async def async_code() -> None:
        """Async code awaiting the run."""
        await async_job.run_async()

    asyncio.run(async_code())

    assert stored_rows(async_job) == stored_rows(script_job)


def test_run_async_refuses_the_sync_orchestrator() -> None:
    """``run_async`` with a sync orchestrator raises ``TypeError`` naming ``job.run``."""
    job = make_job(InMemoryArrowDatabase())
    with pytest.raises(TypeError, match=r"job\.run"):
        _ = asyncio.run(
            job.run_async(
                orchestrator=SyncPipelineOrchestrator()  # pyright: ignore[reportArgumentType]  # the wrong orchestrator is the case under test
            )
        )


N_SLOW_ROWS = 200  # more than the default thread pool runs at once
slow_calls: list[int] = []


@function_pod(output_keys="y")
def slow_and_interrupted(x: int) -> int:
    """Press the interrupt button during the first row; take a while for every row."""
    slow_calls.append(x)
    if len(slow_calls) == 1:
        _thread.interrupt_main()
    time.sleep(0.05)
    return x


def test_interrupt_while_running_inside_a_loop_stops_the_run() -> None:
    """The notebook's interrupt button stops the run; nothing keeps running after it."""
    slow_calls.clear()
    rows = [{"k": f"r{i:02d}", "x": i} for i in range(N_SLOW_ROWS)]
    job = PipelineJob(name="interrupt_demo", database=InMemoryArrowDatabase())
    with job:
        source = DictSource(data=rows, tag_columns=["k"], source_id="rows")
        slow_and_interrupted.pod(source, label="slow")

    async def notebook_cell() -> None:
        """A notebook cell: the job run inside the kernel's running loop."""
        job.run(orchestrator=AsyncPipelineOrchestrator())

    # A bare loop, as a kernel has: asyncio.run would install its own SIGINT handler.
    loop = asyncio.new_event_loop()
    try:
        with pytest.raises(KeyboardInterrupt):
            loop.run_until_complete(notebook_cell())
    finally:
        loop.close()

    calls_when_stopped = len(slow_calls)
    assert calls_when_stopped < N_SLOW_ROWS
    time.sleep(0.2)
    assert len(slow_calls) == calls_when_stopped


STORE_WRITES = ("add_record", "add_records", "flush", "flush_batch", "set_metadata")


class CountingWrites:
    """A store that records the name of every write or commit made through it."""

    def __init__(self, inner: Any, writes: list[str]) -> None:
        """Wrap ``inner``; every view appends to the same ``writes``."""
        self._inner = inner
        self._writes = writes

    def at(self, *path_components: str) -> CountingWrites:
        """A view recording into the same list."""
        return CountingWrites(self._inner.at(*path_components), self._writes)

    def __getattr__(self, name: str) -> Any:
        """Delegate; record the calls of the write methods."""
        attr = getattr(self._inner, name)
        if name not in STORE_WRITES:
            return attr

        def recorded(*args: Any, **kwargs: Any) -> Any:
            """Record the write, then make it."""
            self._writes.append(name)
            return attr(*args, **kwargs)

        return recorded


stuck_release = threading.Event()
press_interrupt = threading.Event()
stuck_calls: list[int] = []
after_calls: list[int] = []


@function_pod(output_keys="y")
def stuck_on_first_row(x: int) -> int:
    """Row 0 presses the interrupt button twice and does not return until released."""
    stuck_calls.append(x)
    if x == 0 and press_interrupt.is_set():
        _thread.interrupt_main()  # first press: cancels the run, which then waits
        # Second press, while the run waits for this function to return.
        threading.Timer(1.0, _thread.interrupt_main).start()
        _ = stuck_release.wait(timeout=30)
    return x


@function_pod(output_keys="z")
def after_stuck(y: int) -> int:
    """Count the rows that reach the next node."""
    after_calls.append(y)
    return y + 1


def make_stuck_job(store: Any) -> PipelineJob:
    """Six rows through ``stuck_on_first_row`` and then ``after_stuck``."""
    rows = [{"k": f"r{i}", "x": i} for i in range(6)]
    job = PipelineJob(name="stuck_demo", database=store)
    with job:
        source = DictSource(data=rows, tag_columns=["k"], source_id="rows")
        stuck = stuck_on_first_row.pod(source, label="stuck")
        after_stuck.pod(stuck, label="after")
    return job


def bridge_threads() -> list[threading.Thread]:
    """The threads still running a job's async run."""
    return [
        t
        for t in threading.enumerate()
        if t.name == "orcapod-async-run" and t.is_alive()
    ]


@pytest.mark.parametrize("store_kind", list(STORES))
def test_second_interrupt_returns_and_a_late_function_writes_nothing(
    store_kind: str, tmp_path: Path
) -> None:
    """A second interrupt returns at once; the function returning later stores nothing.

    The first interrupt cancels the run, which then waits for row 0's function, still
    running on a thread where it cannot be interrupted. The second interrupt returns
    control with a warning naming the node. When the function returns afterwards, no
    store write happens and no row reaches the next node, and row 0's result is never
    stored: a later run computes it.
    """
    stuck_release.clear()
    press_interrupt.set()
    stuck_calls.clear()
    after_calls.clear()
    inner = STORES[store_kind](tmp_path / "store")
    writes: list[str] = []
    job = make_stuck_job(CountingWrites(inner, writes))

    async def notebook_cell() -> None:
        """A notebook cell: the job run inside the kernel's running loop."""
        job.run(orchestrator=AsyncPipelineOrchestrator())

    # A bare loop, as a kernel has: asyncio.run would install its own SIGINT handler.
    loop = asyncio.new_event_loop()
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with pytest.raises(KeyboardInterrupt):
                loop.run_until_complete(notebook_cell())
    finally:
        loop.close()

    messages = [str(w.message) for w in caught if w.category is RuntimeWarning]
    assert len(messages) == 1, messages
    assert "but a function in node 'stuck' has not returned yet" in messages[0]
    assert "Nothing more will be written to the store" in messages[0]
    assert "restart the kernel" in messages[0]
    assert bridge_threads(), "control returned before the stuck function did"

    writes_when_returned = list(writes)
    rows_after_when_returned = list(after_calls)
    stuck_release.set()
    deadline = time.monotonic() + 30
    while bridge_threads() and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not bridge_threads()

    assert writes == writes_when_returned
    assert after_calls == rows_after_when_returned

    # Row 0's result was never stored: a later run on the same store computes it.
    inner.flush()
    stuck_calls.clear()
    press_interrupt.clear()
    rerun = make_stuck_job(inner)
    _ = rerun.run()
    assert 0 in stuck_calls


async def async_stuck_on_first_row(x: int) -> int:
    """Row 0 presses the interrupt button twice and blocks the event loop until released."""
    if x == 0:
        _thread.interrupt_main()
        threading.Timer(1.0, _thread.interrupt_main).start()
        # A blocking wait inside an async function: the bug that blocks the loop.
        _ = stuck_release.wait(timeout=30)
    return x


POD_FORMS: dict[str, Callable[[], dict[str, Any]]] = {
    "plain": dict,
    "result_database": lambda: {"result_database": InMemoryArrowDatabase()},
    "pod_cache_database": lambda: {"pod_cache_database": InMemoryArrowDatabase()},
}


@pytest.mark.parametrize("pod_form", list(POD_FORMS))
def test_second_interrupt_names_an_async_function_blocking_the_loop(
    pod_form: str,
) -> None:
    """For an async function blocking the loop, the warning says rows may still be stored.

    The cancellation cannot run on a loop whose thread the function holds, so rows
    already in progress may be stored once it returns; the warning says so instead of
    promising that nothing more is written. The run still stops once it returns. The
    function is recognised as async in every form of pod: plain, with a result
    database (a wrapped data function) and with a pod cache.
    """
    stuck_release.clear()
    pod = function_pod(output_keys="y", **POD_FORMS[pod_form]())(
        async_stuck_on_first_row
    )
    rows = [{"k": f"r{i}", "x": i} for i in range(6)]
    job = PipelineJob(name="async_stuck_demo", database=InMemoryArrowDatabase())
    with job:
        source = DictSource(data=rows, tag_columns=["k"], source_id="rows")
        stuck = pod.pod(source, label="stuck")
        after_stuck.pod(stuck, label="after")

    async def notebook_cell() -> None:
        """A notebook cell: the job run inside the kernel's running loop."""
        job.run(orchestrator=AsyncPipelineOrchestrator())

    loop = asyncio.new_event_loop()
    try:
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with pytest.raises(KeyboardInterrupt):
                loop.run_until_complete(notebook_cell())
    finally:
        loop.close()

    messages = [str(w.message) for w in caught if w.category is RuntimeWarning]
    assert len(messages) == 1, messages
    assert "an async function in node 'stuck' is blocking the event loop" in messages[0]
    assert "rows already in progress may still be stored" in messages[0]
    assert "Nothing more will be written" not in messages[0]

    stuck_release.set()
    deadline = time.monotonic() + 30
    while bridge_threads() and time.monotonic() < deadline:
        time.sleep(0.05)
    assert not bridge_threads()


pod_task_cleanup: list[int] = []


async def wait_forever_then_clean_up(x: int) -> None:
    """A background task left pending by a pod; records its cancellation cleanup."""
    try:
        await asyncio.sleep(3600)
    except asyncio.CancelledError:
        pod_task_cleanup.append(x)
        raise


@function_pod(output_keys="y")
async def leaves_a_task_running(x: int) -> int:
    """Start a background task and return without awaiting it."""
    _ = asyncio.get_running_loop().create_task(wait_forever_then_clean_up(x))
    return x


def test_tasks_left_pending_by_a_pod_are_cancelled_and_cleaned_up() -> None:
    """Inside a running loop, as with ``asyncio.run``, leftover tasks get their cleanup."""
    pod_task_cleanup.clear()
    rows = [{"k": f"r{i}", "x": i} for i in range(3)]
    job = PipelineJob(name="leftover_demo", database=InMemoryArrowDatabase())
    with job:
        source = DictSource(data=rows, tag_columns=["k"], source_id="rows")
        leaves_a_task_running.pod(source, label="leaves")

    run_inside_running_loop(job)

    assert sorted(pod_task_cleanup) == [0, 1, 2]
