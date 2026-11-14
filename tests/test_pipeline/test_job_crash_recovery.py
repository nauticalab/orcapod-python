"""A job killed mid-run keeps its committed batches and the rerun completes it (ITL-919).

Function nodes inside ``PipelineJob.run`` commit every ``commit_every_rows`` rows,
results before pipeline records. These tests kill a job hard (``os._exit`` in a
subprocess, as a power loss or ``kill -9`` would) on a ``DeltaTableDatabase``:

- in the middle of a batch: exactly the committed batches are stored, and the rerun
  computes only the rows that were not. On the async path rows finish, and so are
  recorded and committed, in any order: a batch holds the rows that finished first, not
  the first rows of the input;
- between a batch's result commit and its pipeline-record commit, also in a batch that
  starts with rows whose results were already stored: the results are kept, the rerun
  computes nothing for them and writes their pipeline records.

Every rerun ends with the same stored rows, ids and hashes as an uninterrupted run.
"""

from __future__ import annotations

import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any, cast

import deltalake
import pyarrow as pa
import pytest

from orcapod.databases.storage_utils import delta_to_pyarrow_dataset

N_ROWS = 250

# Runs one job over N_ROWS rows. Arguments: store path, calls file, commit_every_rows,
# orchestrator ("sync" or "async"), crash ("none", "fn:<x>" or "flush:<n>"), late row
# ("none" or a row number). "fn:<x>" exits while computing row x, once the batches of the
# rows before x are committed; "flush:<n>" exits on the n-th store-wide flush. The run's
# first flush commits what was pending before it; each later one is a node's commit,
# called right after the node committed its result table.
#
# The async orchestrator computes rows on a thread pool ahead of the stage that records
# them and commits every commit_every_rows rows, so without a wait row x could be
# computed before the batches before it are committed (ITL-1001). Row x and every row
# after it wait until the store has finished the run's first flush and one flush per
# batch before x, so the crash falls in the batch that holds x, after the earlier
# batches are committed and before the result of any row from x on is stored. On the
# sync path the rows come in order and the wait returns at once.
#
# The pool's rows also finish in any order, and the node records them in the order they
# finish. A late row (with "fn:<x>" only, async only) waits for the same commits, so it
# finishes after the batches before x are committed without it: a row of the input's
# first batches that a loaded machine finishes late, made deterministic.
#
# The wait has no time bound, because it cannot deadlock: the pool starts rows in input
# order, so rows from x on only take threads once every row before x has started, and
# all of those return except the one late row. run_job's subprocess timeout is the hang
# guard if a change ever breaks that.
JOB_SCRIPT = textwrap.dedent(
    """
    import os
    import sys
    import threading
    from typing import Any

    from orcapod import PipelineJob, function_pod
    from orcapod.databases import DeltaTableDatabase
    from orcapod.pipeline.async_orchestrator import AsyncPipelineOrchestrator
    from orcapod.sources import DictSource

    store_path, calls_path, every, orchestrator, crash, late = sys.argv[1:7]
    crash_kind, _, crash_arg = crash.partition(":")
    earlier_batches_committed = threading.Event()
    if crash_kind == "fn":
        flushes_before_crash = 1 + int(crash_arg) // int(every)


    def wait_for_earlier_batches() -> None:
        earlier_batches_committed.wait()


    @function_pod(output_keys="y")
    def double(x: int) -> int:
        with open(calls_path, "a") as calls:
            calls.write(f"{x}\\n")
        if crash_kind == "fn" and x >= int(crash_arg):
            wait_for_earlier_batches()
            if x == int(crash_arg):
                os._exit(3)
        if crash_kind == "fn" and late != "none" and x == int(late):
            wait_for_earlier_batches()
        return 2 * x


    class CrashOnFlush:
        def __init__(self, inner: Any, state: dict[str, int]) -> None:
            self._inner = inner
            self._state = state

        def at(self, *path: str) -> "CrashOnFlush":
            return CrashOnFlush(self._inner.at(*path), self._state)

        def flush(self) -> None:
            self._state["n"] += 1
            if crash_kind == "flush" and self._state["n"] == int(crash_arg):
                os._exit(3)
            self._inner.flush()
            if crash_kind == "fn" and self._state["n"] >= flushes_before_crash:
                earlier_batches_committed.set()

        def __getattr__(self, name: str) -> Any:
            return getattr(self._inner, name)


    store = CrashOnFlush(DeltaTableDatabase(store_path), {"n": 0})
    job = PipelineJob(name="crash", database=store)
    with job:
        source = DictSource(
            data=[{"k": i, "x": i} for i in range(%d)],
            tag_columns=["k"],
            source_id="src",
        )
        double.pod(source, label="dbl")
    job.run(
        orchestrator=AsyncPipelineOrchestrator() if orchestrator == "async" else None,
        commit_every_rows=int(every),
    )
    os._exit(0)
    """
    % N_ROWS
)


def run_job(
    tmp_path: Path,
    store: Path,
    every: int,
    orchestrator: str,
    crash: str = "none",
    late: int | None = None,
) -> tuple[int, list[int]]:
    """Run the job in a subprocess; return its exit code and the rows it computed."""
    script = tmp_path / "job.py"
    script.write_text(JOB_SCRIPT)
    calls = tmp_path / "calls.txt"
    calls.write_text("")
    done = subprocess.run(
        [sys.executable, str(script), str(store), str(calls), str(every)]
        + [orchestrator, crash, "none" if late is None else str(late)],
        capture_output=True,
        text=True,
        timeout=600,
    )
    assert done.returncode in (0, 3), done.stderr[-3000:]
    return done.returncode, [int(x) for x in calls.read_text().split()]


def tables(store: Path) -> dict[str, Path]:
    """The job's Delta tables, keyed ``result`` and ``pipeline`` (absent if none)."""
    found = {log.parent for log in store.rglob("_delta_log")}
    return {
        ("result" if "_result" in t.relative_to(store).parts else "pipeline"): t
        for t in found
    }


def read(table: Path | None) -> pa.Table | None:
    """All committed rows of ``table`` through a fresh handle, or ``None``."""
    if table is None:
        return None
    return delta_to_pyarrow_dataset(deltalake.DeltaTable(str(table))).to_table()


def row_count(store: Path, kind: str) -> int:
    """Committed rows of the ``kind`` table (0 if it does not exist)."""
    table = read(tables(store).get(kind))
    return 0 if table is None else table.num_rows


def stored_rows(store: Path, kind: str) -> list[int]:
    """The input rows (``x``) of the committed rows of the ``kind`` table, sorted.

    A pipeline record names its row by the tag ``k``, which equals ``x``; a result
    holds ``y = 2 * x``.
    """
    table = read(tables(store).get(kind))
    if table is None:
        return []
    # Both columns are non-null int64; to_pylist is typed as possibly None.
    if kind == "pipeline":
        return sorted(cast("list[int]", table["k"].to_pylist()))
    return sorted(y // 2 for y in cast("list[int]", table["y"].to_pylist()))


def stable(store: Path) -> dict[str, list[tuple[object, ...]]]:
    """The stored columns that an interrupted and an uninterrupted run must share.

    Result ids (uuids), timestamps and the ``computed`` flag legitimately differ;
    record ids, entry ids, the recomputation index, hashes, tags and values do not.
    """
    result = read(tables(store)["result"])
    pipeline = read(tables(store)["pipeline"])
    assert result is not None and pipeline is not None
    return {
        "result": sorted(
            zip(*(result[c].to_pylist() for c in ("__input_data_hash", "y")))
        ),
        "pipeline": sorted(
            zip(
                *(
                    pipeline[c].to_pylist()
                    for c in (
                        "__record_id",
                        "__pipeline_base_entry_id",
                        "__pipeline_recomputation_index",
                        "__input_data_hash",
                        "__output_data_hash",
                        "k",
                    )
                )
            )
        ),
    }


@pytest.fixture(scope="module")
def uninterrupted(tmp_path_factory: pytest.TempPathFactory) -> dict[str, list[Any]]:
    """The stable columns of a run that was never interrupted."""
    tmp = tmp_path_factory.mktemp("uninterrupted")
    code, calls = run_job(tmp, tmp / "store", 100, "sync")
    assert code == 0 and sorted(calls) == list(range(N_ROWS))
    return stable(tmp / "store")


@pytest.mark.parametrize(
    ("orchestrator", "late"),
    [("sync", None), ("async", None), ("async", 199)],
    ids=["sync", "async", "async-late-row"],
)
def test_crash_mid_batch_keeps_committed_batches(
    tmp_path: Path,
    orchestrator: str,
    late: int | None,
    uninterrupted: dict[str, list[Any]],
) -> None:
    """Killed at row 234 with 100 rows per commit: two whole batches stored.

    The rerun computes exactly the rows without a committed result. On the async path
    the two batches are the 200 rows that finished first, whichever they are; the
    late-row case makes row 199 finish after the second commit.
    """
    store = tmp_path / "store"
    code, _ = run_job(tmp_path, store, 100, orchestrator, crash="fn:234", late=late)
    assert code == 3
    recorded = stored_rows(store, "pipeline")
    computed = stored_rows(store, "result")
    # Two whole batches: 200 rows with a pipeline record, each with a committed result
    # (results are committed before their pipeline records).
    assert len(set(recorded)) == len(recorded) == 200
    assert set(recorded) <= set(computed)
    assert len(set(computed)) == len(computed)
    # Nothing from the crash row on is stored.
    assert max(computed) < 234
    if orchestrator == "sync":
        assert recorded == computed == list(range(200))
    if late is not None:
        # The late row finished after the second commit, so neither is stored.
        assert late not in computed

    code, calls = run_job(tmp_path, store, 100, orchestrator)
    assert code == 0
    # Exactly the rows without a committed result are computed again.
    assert sorted(calls) == sorted(set(range(N_ROWS)) - set(computed))
    assert stable(store) == uninterrupted


def test_crash_between_result_and_pipeline_commits(
    tmp_path: Path, uninterrupted: dict[str, list[Any]]
) -> None:
    """Results committed without their pipeline records are reused, not recomputed."""
    store = tmp_path / "store"
    # First commit: results of rows 0-99 committed, then the process dies.
    code, _ = run_job(tmp_path, store, 100, "sync", crash="flush:2")
    assert code == 3
    assert (row_count(store, "result"), row_count(store, "pipeline")) == (100, 0)

    # 150 rows per commit: the first batch starts with rows 0-99, whose stored
    # results are reused (pipeline records only), then computes rows 100-149. Its
    # result table is still committed first.
    code, calls = run_job(tmp_path, store, 150, "sync", crash="flush:2")
    assert code == 3
    assert sorted(calls) == list(range(100, 150))
    assert (row_count(store, "result"), row_count(store, "pipeline")) == (150, 0)

    code, calls = run_job(tmp_path, store, 100, "sync")
    assert code == 0
    assert sorted(calls) == list(range(150, N_ROWS))
    assert stable(store) == uninterrupted
