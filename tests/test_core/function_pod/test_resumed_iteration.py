"""A function pod stream read again after a partial iteration yields every row once (ITL-1039).

A ``for`` loop with ``break``, or ``next(iter(stream))`` in a notebook, stops reading a
stream early. Reading it again must give every input row exactly once, each with its own
output, in input order, and compute each row once. Before ITL-1039 the second read counted
input rows from 0 again while the input resumed where it had stopped: the rows read first
were yielded in place of the next ones, and those were lost.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest

from orcapod import function_pod
from orcapod.core.cached_function_pod import CachedFunctionPod
from orcapod.core.executors import LocalPythonFunctionExecutor
from orcapod.databases import InMemoryArrowDatabase
from orcapod.sources import DictSource

ROWS = 6
CALLS: list[int] = []


def new_pod() -> Any:
    """A fresh function pod of ``times_ten``.

    Setting a stream's executor sets it on the pod's data function, so each stream gets
    its own pod: a concurrent executor set by one test must not reach another.
    """

    @function_pod(output_keys="y")
    def times_ten(x: int) -> int:
        """Return ``10 * x``, recording the call."""
        CALLS.append(x)
        return 10 * x

    return times_ten.pod


@pytest.fixture(autouse=True)
def _reset_calls() -> None:
    """Forget the calls of earlier tests."""
    CALLS.clear()


class ConcurrentLocalExecutor(LocalPythonFunctionExecutor):
    """The local executor, declared concurrent, so the stream takes its concurrent path."""

    @property
    def supports_concurrent_execution(self) -> bool:
        """Whether rows may be computed concurrently: yes."""
        return True


def source() -> DictSource:
    """Six rows: tag ``k`` and data ``x``, both ``0..5``."""
    return DictSource([{"k": i, "x": i} for i in range(ROWS)], tag_columns=["k"])


def stream(kind: str) -> Any:
    """A function pod stream over ``source()`` of the kind the test names."""
    if kind == "plain":
        return new_pod()(source())
    if kind == "concurrent":
        pod_stream = new_pod()(source())
        pod_stream.executor = ConcurrentLocalExecutor()
        return pod_stream
    if kind == "cached":
        pod = CachedFunctionPod(new_pod(), result_database=InMemoryArrowDatabase())
        return pod(source())
    raise ValueError(kind)


KINDS = ["plain", "concurrent", "cached"]


def pairs(rows: Iterator[Any]) -> list[tuple[int, int]]:
    """``(k, y)`` of each row."""
    return [(tag.as_dict()["k"], data.as_dict()["y"]) for tag, data in rows]


EVERY_ROW = [(i, 10 * i) for i in range(ROWS)]


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("consumed", [1, 2, 5])
def test_a_partial_iteration_then_as_table_has_every_row(
    kind: str, consumed: int
) -> None:
    rows = stream(kind)
    partial = iter(rows.iter_data())
    for _ in range(consumed):
        next(partial)
    del partial

    table = rows.as_table()

    assert sorted(zip(table["k"].to_pylist(), table["y"].to_pylist())) == EVERY_ROW
    assert sorted(CALLS) == list(range(ROWS)), "each row is computed once"


@pytest.mark.parametrize("kind", KINDS)
def test_a_partial_iteration_then_a_full_iteration_yields_every_row_in_order(
    kind: str,
) -> None:
    rows = stream(kind)
    partial = iter(rows.iter_data())
    assert pairs(iter([next(partial), next(partial)])) == EVERY_ROW[:2]

    assert pairs(rows.iter_data()) == EVERY_ROW
    assert sorted(CALLS) == list(range(ROWS))


@pytest.mark.parametrize("kind", KINDS)
def test_interleaved_partial_iterations_each_yield_every_row_in_order(
    kind: str,
) -> None:
    rows = stream(kind)
    first = iter(rows.iter_data())
    second = iter(rows.iter_data())
    seen_first = [next(first)]
    seen_second = [next(second), next(second), next(second)]
    seen_first += [next(first), next(first), next(first), next(first)]
    seen_second += list(second)
    seen_first += list(first)

    assert pairs(iter(seen_first)) == EVERY_ROW
    assert pairs(iter(seen_second)) == EVERY_ROW
    assert sorted(CALLS) == list(range(ROWS))


@pytest.mark.parametrize("kind", KINDS)
def test_a_partial_iteration_then_head_shows_the_first_rows(kind: str) -> None:
    rows = stream(kind)
    partial = iter(rows.iter_data())
    next(partial), next(partial), next(partial)
    del partial
    rows.as_table()

    head = rows.head(4).as_table()

    assert list(zip(head["k"].to_pylist(), head["y"].to_pylist())) == EVERY_ROW[:4]


def test_switching_to_a_concurrent_executor_after_a_partial_iteration_keeps_every_row() -> (
    None
):
    rows = new_pod()(source())
    partial = iter(rows.iter_data())
    next(partial), next(partial)
    del partial
    rows.executor = ConcurrentLocalExecutor()

    assert pairs(rows.iter_data()) == EVERY_ROW


@pytest.mark.parametrize("orchestrator", ["sync", "async"])
def test_a_job_node_read_partially_then_in_full_has_every_row(
    orchestrator: str,
) -> None:
    """A job's function node does not share the pattern: its rows are keyed by entry id.

    Its ``iter_data`` reads the node's results and never its input, so stopping early
    changes nothing; a run reads the input in one pass of its own.
    """
    from orcapod import PipelineJob
    from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator

    job = PipelineJob(name="itl1039", database=InMemoryArrowDatabase())
    with job:
        new_pod()(source(), label="node")
    job.run(
        orchestrator=SyncPipelineOrchestrator()
        if orchestrator == "sync"
        else AsyncPipelineOrchestrator()
    )
    node = job.nodes["node"]
    partial = iter(node.iter_data())
    next(partial), next(partial)
    del partial

    assert sorted(pairs(node.iter_data())) == EVERY_ROW
    table = node.as_table()
    assert sorted(zip(table["k"].to_pylist(), table["y"].to_pylist())) == EVERY_ROW
    assert sorted(CALLS) == list(range(ROWS))


def raising_pod(failures: dict[int, int]) -> Any:
    """A fresh pod of ``times_ten`` that raises ``failures[x]`` times for each ``x``."""
    remaining = dict(failures)

    @function_pod(output_keys="y")
    def times_ten(x: int) -> int:
        """Return ``10 * x``; raise while ``x`` has failures left."""
        if remaining.get(x, 0) > 0:
            remaining[x] -= 1
            raise RuntimeError(f"transient failure for x={x}")
        CALLS.append(x)
        return 10 * x

    return times_ten.pod


def raising_stream(kind: str, failures: dict[int, int]) -> Any:
    """A stream of ``kind`` whose function raises as ``failures`` says."""
    pod = raising_pod(failures)
    if kind == "plain":
        return pod(source())
    if kind == "concurrent":
        stream = pod(source())
        stream.executor = ConcurrentLocalExecutor()
        return stream
    if kind == "cached":
        return CachedFunctionPod(pod, result_database=InMemoryArrowDatabase())(source())
    raise ValueError(kind)


def read_until_complete(rows: Any, failing_reads: int) -> None:
    """Read ``rows`` in full ``failing_reads`` times, each read raising a failure."""
    for _ in range(failing_reads):
        with pytest.raises(RuntimeError, match="transient failure"):
            list(rows.iter_data())


# How many times each x raises, and how many full reads raise before one succeeds. The
# sequential path stops at the first failure of a read; the concurrent path computes every
# row and raises once per read.
FAILURE_CASES = {
    "first_row_once": ({0: 1}, {"sequential": 1, "concurrent": 1}),
    "middle_row_once": ({2: 1}, {"sequential": 1, "concurrent": 1}),
    "last_row_once": ({ROWS - 1: 1}, {"sequential": 1, "concurrent": 1}),
    "same_row_twice": ({2: 2}, {"sequential": 2, "concurrent": 2}),
    "two_rows_once_each": ({1: 1, 4: 1}, {"sequential": 2, "concurrent": 1}),
}


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize("case", list(FAILURE_CASES))
def test_a_row_whose_function_raised_is_computed_again_by_the_next_read(
    kind: str, case: str
) -> None:
    """A failed row is neither lost nor left as a gap: a later read yields every row."""
    failures, failing_reads = FAILURE_CASES[case]
    rows = raising_stream(kind, failures)

    read_until_complete(
        rows, failing_reads["concurrent" if kind == "concurrent" else "sequential"]
    )

    assert pairs(rows.iter_data()) == EVERY_ROW
    assert pairs(rows.iter_data()) == EVERY_ROW, "a third read replays the cache"
    table = rows.as_table()
    assert sorted(zip(table["k"].to_pylist(), table["y"].to_pylist())) == EVERY_ROW
    assert sorted(CALLS) == list(range(ROWS)), "each row is computed once"


def upstream_raising_stream(
    kind: str, failing_x: int, upstream_started: bool
) -> tuple[Any, list[int]]:
    """``plus_one`` of ``times_ten``, whose upstream raises once for ``failing_x``.

    With ``upstream_started``, the upstream's first row was read before the downstream
    stream was made (so the upstream has its input open, and nothing it does later makes
    the downstream stale). Returns the downstream stream of ``kind`` and the list its
    function's calls go to.
    """
    downstream_calls: list[int] = []

    @function_pod(output_keys="z")
    def plus_one(y: int) -> int:
        """Return ``y + 1``, recording the call."""
        downstream_calls.append(y)
        return y + 1

    upstream = raising_pod({failing_x: 1})(source())
    if upstream_started:
        assert failing_x != 0
        next(iter(upstream.iter_data()))
    downstream_pod: Any = plus_one.pod
    if kind == "plain":
        return downstream_pod(upstream), downstream_calls
    if kind == "concurrent":
        stream = downstream_pod(upstream)
        stream.executor = ConcurrentLocalExecutor()
        return stream, downstream_calls
    if kind == "cached":
        pod = CachedFunctionPod(downstream_pod, result_database=InMemoryArrowDatabase())
        return pod(upstream), downstream_calls
    raise ValueError(kind)


@pytest.mark.parametrize("kind", KINDS)
@pytest.mark.parametrize(
    ("failing_x", "upstream_started"),
    [(0, False), (2, False), (ROWS - 1, False), (2, True), (ROWS - 1, True)],
)
def test_a_read_that_failed_upstream_loses_no_row(
    kind: str, failing_x: int, upstream_started: bool
) -> None:
    """An upstream stream raising mid-read: the next downstream read yields every row."""
    rows, downstream_calls = upstream_raising_stream(kind, failing_x, upstream_started)

    with pytest.raises(RuntimeError, match="transient failure"):
        list(rows.iter_data())

    every_row = [(i, 10 * i + 1) for i in range(ROWS)]
    got = [(tag.as_dict()["k"], data.as_dict()["z"]) for tag, data in rows.iter_data()]
    assert got == every_row
    table = rows.as_table()
    assert sorted(zip(table["k"].to_pylist(), table["z"].to_pylist())) == every_row
    assert sorted(CALLS) == list(range(ROWS)), "each upstream row is computed once"
    if upstream_started:
        # Only then: an upstream that opens its input after the downstream did makes
        # the downstream stale, which recomputes it on the next read (ITL-1052, a
        # defect of the staleness check, not a lost row).
        assert sorted(downstream_calls) == [10 * i for i in range(ROWS)], (
            "each downstream row is computed once"
        )
