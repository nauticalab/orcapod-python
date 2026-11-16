"""After a job run, the user is told which rows failed (ITL-997).

With the default ``error_policy="continue"`` a row whose function raises is skipped and the
run goes on. The user must still see, without configuring anything: the node, how many rows
failed out of how many, and the first error's type and message, with a pointer to the full
traceback. A clean run stays quiet.
"""

from __future__ import annotations

import ast
import warnings
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.errors import RowFailuresWarning
from orcapod.pipeline import (
    AsyncPipelineOrchestrator,
    RunSummary,
    SyncPipelineOrchestrator,
)
from orcapod.sources import DictSource

# --- The public names this feature adds, in one place ---------------------------------


def run(job: PipelineJob, orchestrator: Any) -> RunSummary:
    """Run ``job``; return what the user gets back (the run summary)."""
    return job.run(orchestrator=orchestrator)


def node_counts(summary: RunSummary, label: str) -> tuple[int, int, int]:
    """``(computed, from store, failed)`` rows of node ``label`` in ``summary``."""
    node = summary.nodes[label]
    return node.computed, node.reused, node.failed


def first_error(summary: RunSummary, label: str) -> BaseException | None:
    """The first error raised in node ``label`` during the run."""
    return summary.nodes[label].first_error


def last_run(job: PipelineJob) -> RunSummary:
    """The summary of the job's last completed run."""
    summary = job.last_run
    assert summary is not None
    return summary


# --------------------------------------------------------------------------------------

ORCHESTRATORS: dict[str, Callable[[], Any]] = {
    "sync": SyncPipelineOrchestrator,
    "async": AsyncPipelineOrchestrator,
}

STORES: dict[str, Callable[[Path], Any]] = {
    "in_memory": lambda _: InMemoryArrowDatabase(),
    "delta": lambda path: DeltaTableDatabase(path),
}


@function_pod(output_keys="y")
def even_only(x: int) -> int:
    """Return ``x``; raise for an odd ``x``."""
    if x % 2:
        raise ValueError(f"odd x={x}")
    return x


@function_pod(output_keys="y")
def always_fails(x: int) -> int:
    """Raise for every row, as a function with a typo does."""
    return x / undefined_name  # noqa: F821  # pyright: ignore[reportUndefinedVariable]  # the bug under test


@function_pod(output_keys="y")
def double(x: int) -> int:
    """Return twice ``x``."""
    return 2 * x


def make_job(store: Any, pod: Any, label: str, n_rows: int = 4) -> PipelineJob:
    """A source of ``n_rows`` rows (x = 0..n-1) feeding ``pod`` under ``label``."""
    rows = [{"k": f"r{i}", "x": i} for i in range(n_rows)]
    job = PipelineJob(name="summary_demo", database=store)
    with job:
        source = DictSource(
            data=rows, tag_columns=["k"], tag_only_columns=True, source_id="rows"
        )
        pod.pod(source, label=label)
    return job


@pytest.fixture(params=list(ORCHESTRATORS))
def orchestrator(request: pytest.FixtureRequest) -> Any:
    """Each orchestrator, freshly built."""
    return ORCHESTRATORS[request.param]()


@pytest.fixture(params=list(STORES))
def store(request: pytest.FixtureRequest, tmp_path: Path) -> Any:
    """Each store kind, empty."""
    return STORES[request.param](tmp_path / "store")


def test_some_rows_fail(store: Any, orchestrator: Any) -> None:
    """Rows that fail are counted and reported once; the others are computed and stored."""
    job = make_job(store, even_only, "evens")
    with pytest.warns(RowFailuresWarning) as caught:
        summary = run(job, orchestrator)

    assert node_counts(summary, "evens") == (2, 0, 2)
    error = first_error(summary, "evens")
    assert isinstance(error, ValueError)
    assert str(error).startswith("odd x=")
    assert len(caught) == 1
    message = str(caught[0].message)
    assert "2 of 4 rows failed in node 'evens'" in message
    assert "ValueError: odd x=" in message
    assert 'job.last_run.print_traceback("evens")' in message
    assert last_run(job) is summary
    # The rows that succeeded were computed and stored as before.
    ys = sorted(job.nodes["evens"].as_table().column("y").to_pylist())
    assert ys == [0, 2]


def test_every_row_fails(store: Any, orchestrator: Any) -> None:
    """A function failing on every row is reported with its error, not as an empty success."""
    job = make_job(store, always_fails, "broken")
    with pytest.warns(RowFailuresWarning, match="4 of 4 rows failed in node 'broken'"):
        summary = run(job, orchestrator)

    assert node_counts(summary, "broken") == (0, 0, 4)
    assert isinstance(first_error(summary, "broken"), NameError)
    assert summary.failed_rows == 4
    assert not summary.ok
    text = repr(summary)
    assert "broken" in text
    assert "NameError: name 'undefined_name' is not defined" in text
    assert 'job.last_run.print_traceback("broken")' in text


def test_clean_run_is_quiet(store: Any, orchestrator: Any) -> None:
    """A run without failed rows issues no warning and counts every row as computed."""
    job = make_job(store, double, "doubled")
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        summary = run(job, orchestrator)

    assert summary.ok
    assert summary.failed_rows == 0
    assert node_counts(summary, "doubled") == (4, 0, 0)
    assert first_error(summary, "doubled") is None


def test_rerun_counts_rows_from_the_store(store: Any, orchestrator: Any) -> None:
    """Running again counts the stored rows as from the database and computes only the new row."""
    run(make_job(store, double, "doubled"), orchestrator)
    summary = run(make_job(store, double, "doubled", n_rows=5), orchestrator)

    assert node_counts(summary, "doubled") == (1, 4, 0)


def test_fail_fast_still_raises(store: Any) -> None:
    """``error_policy='fail_fast'`` raises the row's error as before."""
    job = make_job(store, even_only, "evens")
    with pytest.raises(ValueError, match="odd x="):
        job.run(orchestrator=SyncPipelineOrchestrator(error_policy="fail_fast"))


def test_print_traceback_shows_the_function_frame(
    capsys: pytest.CaptureFixture[str],
) -> None:
    """``print_traceback`` prints the traceback down to the user's function."""
    job = make_job(InMemoryArrowDatabase(), always_fails, "broken")
    with pytest.warns(RowFailuresWarning):
        run(job, SyncPipelineOrchestrator())

    last_run(job).print_traceback("broken")

    err = capsys.readouterr().err
    assert "Traceback (most recent call last)" in err
    assert "in always_fails" in err
    assert "NameError: name 'undefined_name' is not defined" in err


def test_notebook_representation_names_the_failing_node() -> None:
    """The notebook table names the failing node, its error type and message."""
    job = make_job(InMemoryArrowDatabase(), even_only, "evens")
    with pytest.warns(RowFailuresWarning):
        summary = run(job, SyncPipelineOrchestrator())

    html = summary._repr_html_()  # the notebook display hook is what is under test
    assert "evens" in html
    assert "ValueError" in html
    assert "odd x=" in html


def test_run_async_returns_the_same_summary(store: Any) -> None:
    """``run_async`` returns the summary, warns, and keeps it as ``last_run``."""
    import asyncio

    job = make_job(store, even_only, "evens")
    with pytest.warns(RowFailuresWarning, match="2 of 4 rows failed in node 'evens'"):
        summary = asyncio.run(job.run_async())

    assert node_counts(summary, "evens") == (2, 0, 2)
    assert last_run(job) is summary


@function_pod(output_keys="z")
def plus_one(y: int) -> int:
    """Return ``y + 1``."""
    return y + 1


def test_node_with_empty_input_counts_zero_rows(store: Any, orchestrator: Any) -> None:
    """Every row fails upstream, so the next node receives none: it reports zeros."""
    rows = [{"k": f"r{i}", "x": i} for i in range(4)]
    job = PipelineJob(name="summary_chain", database=store)
    with job:
        source = DictSource(
            data=rows, tag_columns=["k"], tag_only_columns=True, source_id="rows"
        )
        broken = always_fails.pod(source, label="broken")
        plus_one.pod(broken, label="after")

    with pytest.warns(RowFailuresWarning, match="4 of 4 rows failed in node 'broken'"):
        summary = run(job, orchestrator)

    assert list(summary.nodes) == ["broken", "after"]
    assert node_counts(summary, "broken") == (0, 0, 4)
    assert node_counts(summary, "after") == (0, 0, 0)
    assert "after: 0 rows: 0 computed, 0 from store, 0 failed" in repr(summary)


def test_traceback_pointer_is_valid_python_for_any_label() -> None:
    """A label with quotes still gives a call that runs and prints the traceback."""
    label = "weight\"2's"
    job = make_job(InMemoryArrowDatabase(), always_fails, label)
    with pytest.warns(RowFailuresWarning) as caught:
        summary = run(job, SyncPipelineOrchestrator())

    pointer = repr(summary).splitlines()[-1].split("Full traceback: ", 1)[1]
    assert pointer in str(caught[0].message)
    call = ast.parse(pointer, mode="eval").body
    assert isinstance(call, ast.Call)
    assert ast.literal_eval(call.args[0]) == label


def test_every_failed_run_warns_also_from_the_same_line() -> None:
    """Re-running the same failing cell warns again; a user's ignore filter still applies.

    Python's default filter shows a given text from a given line once, and a notebook
    names a cell's file by its content, so an identical second run would warn nothing.
    """
    # One filter setting for both runs, as in a session: changing the filters between
    # the runs would reset Python's once-per-line record and hide the problem.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("default")
        for _ in range(2):
            job = make_job(InMemoryArrowDatabase(), always_fails, "broken")
            _ = job.run(orchestrator=SyncPipelineOrchestrator())  # the same line twice
    assert sum(1 for w in caught if issubclass(w.category, RowFailuresWarning)) == 2

    job = make_job(InMemoryArrowDatabase(), always_fails, "broken")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("default")
        warnings.filterwarnings("ignore", category=RowFailuresWarning)
        summary = job.run(orchestrator=SyncPipelineOrchestrator())
    assert not [w for w in caught if issubclass(w.category, RowFailuresWarning)]
    assert summary.failed_rows == 4
