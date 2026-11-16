"""An async run whose node fails raises instead of hanging (ITL-932).

When a node fails, the async orchestrator's task group cancels every other task,
including the readers of full channels. The writers then closed those channels in
``finally`` blocks with a blocking ``put`` and waited forever: any failure with more
rows in flight than the channel buffers hold hung the run. Each job here runs in a
daemon thread, so a hang fails the test instead of hanging the suite.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import Join
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import InputValidationError, OrcapodDatabaseWriteError
from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
from orcapod.sources import ArrowTableSource, DictSource

N_ROWS = 250  # well above the channel buffers (16 and 64)


@function_pod(output_keys="y")
def double(x: int) -> int:
    """Return twice ``x``."""
    return 2 * x


class FailPipelineRecords:
    """A store whose pipeline-record writes fail after the first two."""

    def __init__(self, inner: Any, state: dict[str, int]) -> None:
        """Wrap ``inner``; every view shares ``state``."""
        self._inner = inner
        self._state = state

    def at(self, *path_components: str) -> FailPipelineRecords:
        """A view sharing the write count."""
        return FailPipelineRecords(self._inner.at(*path_components), self._state)

    def add_record(self, record_path: tuple[str, ...], *args: Any, **kwargs: Any):
        """Fail pipeline-record writes (not results) after the first two."""
        if "_result" not in self._inner.base_path:
            self._state["n"] += 1
            if self._state["n"] > 2:
                raise OSError("disk unavailable")
        return self._inner.add_record(record_path, *args, **kwargs)

    def __getattr__(self, name: str) -> Any:
        """Delegate everything else."""
        return getattr(self._inner, name)


STORES: dict[str, Callable[[Path], Any]] = {
    "in_memory": lambda _: InMemoryArrowDatabase(),
    "delta": lambda path: DeltaTableDatabase(path),
}


def run_in_thread(job: PipelineJob, timeout: float = 120.0) -> BaseException | None:
    """Run ``job`` asynchronously in a daemon thread; return what it raised.

    Fails the test if the run is still going after ``timeout`` seconds.
    """
    outcome: dict[str, BaseException | None] = {"error": None}

    def target() -> None:
        try:
            job.run(orchestrator=AsyncPipelineOrchestrator())
        except BaseException as exc:  # noqa: BLE001  # handed to the test thread
            outcome["error"] = exc

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    thread.join(timeout)
    assert not thread.is_alive(), "the async run hung instead of raising"
    return outcome["error"]


@pytest.mark.parametrize("store", list(STORES))
def test_function_node_store_failure_raises(tmp_path: Path, store: str) -> None:
    """A failed pipeline-record write at row 3 of 250 fails the run."""
    failing = FailPipelineRecords(STORES[store](tmp_path), {"n": 0})
    job = PipelineJob(name="nohang", database=failing)  # pyright: ignore[reportArgumentType]  # failing wrapper of a real store
    with job:
        source = DictSource(
            data=[{"k": i, "x": i} for i in range(N_ROWS)],
            tag_columns=["k"],
            source_id="src",
            tag_only_columns=True,
        )
        double.pod(source, label="dbl")
    error = run_in_thread(job)
    assert isinstance(error, OrcapodDatabaseWriteError), repr(error)
    assert error.node_label == "dbl"


@pytest.mark.parametrize("store", list(STORES))
def test_operator_node_failure_raises(tmp_path: Path, store: str) -> None:
    """A join that rejects its inputs while 250 rows are in flight fails the run."""
    job = PipelineJob(name="nohang", database=STORES[store](tmp_path))
    with job:
        # Both tag keys are Python ints, so the graph builds; their Arrow widths
        # differ, which the join rejects once rows arrive (type strictness).
        left = ArrowTableSource(
            pa.table(
                {
                    "k": pa.array(range(N_ROWS), pa.int32()),
                    "x": pa.array(range(N_ROWS), pa.int64()),
                }
            ),
            tag_columns=["k"],
            source_id="left",
            tag_only_columns=True,
        )
        right = ArrowTableSource(
            pa.table(
                {
                    "k": pa.array(range(N_ROWS), pa.int64()),
                    "z": pa.array(range(N_ROWS), pa.int64()),
                }
            ),
            tag_columns=["k"],
            source_id="right",
            tag_only_columns=True,
        )
        Join()(left, right, label="join")
    error = run_in_thread(job)
    assert error is not None, "the run should fail"
    if isinstance(error, BaseExceptionGroup):
        assert error.subgroup(InputValidationError) is not None, repr(error)
    else:
        assert isinstance(error, InputValidationError), repr(error)
