"""ITL-1040 measurement: appending to a pipeline table an async run wrote before the fix.

Usage (from the repository root):

    uv run python superpowers/specs/itl-1040/measure.py phase1 <dir>    # on main, unfixed
    uv run python superpowers/specs/itl-1040/measure.py phase1b         # on main, unfixed
    uv run python superpowers/specs/itl-1040/measure.py phase2 <dir>    # on the fixed build
    uv run python superpowers/specs/itl-1040/measure.py operator_node   # either build

See README.md in this folder for what each phase does and the results.
"""

from __future__ import annotations

import json
import shutil
import sys
import tempfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.feather as feather

from orcapod import PipelineJob, function_pod
from orcapod.core.operators import Join, SelectDataColumns
from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase
from orcapod.pipeline import AsyncPipelineOrchestrator, SyncPipelineOrchestrator
from orcapod.sources import ArrowTableSource
from orcapod.types import CacheMode

DUMP_ID = "__dump_record_id"


@function_pod(output_keys="y")
def times_ten(x: int) -> int:
    """Return ``10 * x``."""
    return 10 * x


def source(n: int) -> ArrowTableSource:
    """``n`` rows: tag ``k``; data ``x`` (the pod's input) and ``s`` (dropped)."""
    table = pa.table(
        {
            "k": [f"r{i}" for i in range(n)],
            "x": list(range(n)),
            "s": [f"s{i}" for i in range(n)],
        },
        schema=pa.schema(
            [
                pa.field("k", pa.large_string(), nullable=False),
                pa.field("x", pa.int64(), nullable=False),
                pa.field("s", pa.large_string(), nullable=False),
            ]
        ),
    )
    return ArrowTableSource(
        table, tag_columns=["k"], source_id="src", record_id_column="x"
    )


def run(store: Any, n: int, mode: str) -> tuple[PipelineJob, Any]:
    """Run ``times_ten`` over ``n`` rows on ``store`` (``mode`` "sync" or "async")."""
    job = PipelineJob(name="m", store=store)
    with job:
        times_ten.pod(source(n), label="n")  # the automatic select of x drops s
    orchestrator = (
        AsyncPipelineOrchestrator() if mode == "async" else SyncPipelineOrchestrator()
    )
    return job, job.run(orchestrator=orchestrator)


def dump_memory(store: InMemoryArrowDatabase, out: Path) -> None:
    """Write every table of ``store`` to ``out`` as Arrow files."""
    out.mkdir(parents=True, exist_ok=True)
    index = []
    for i, path in enumerate(store.list_table_paths()):
        table = store.get_all_records(path, record_id_column=DUMP_ID)
        assert table is not None
        feather.write_feather(table, out / f"{i}.arrow")
        index.append(list(path))
    (out / "paths.json").write_text(json.dumps(index))


def load_memory(out: Path) -> InMemoryArrowDatabase:
    """An in-memory store holding the tables ``dump_memory`` wrote."""
    store = InMemoryArrowDatabase()
    for i, path in enumerate(json.loads((out / "paths.json").read_text())):
        table = feather.read_table(out / f"{i}.arrow")
        store.add_records(tuple(path), table, record_id_column=DUMP_ID, flush=True)
    return store


def describe(job: PipelineJob, label: str) -> None:
    """Print the node's stored source-info columns and its table."""
    node = job.nodes["n"]
    records = node.get_all_records(all_info=True)
    assert records is not None
    source_columns = [c for c in records.column_names if c.startswith("_source")]
    print(
        f"  [{label}] records: {records.num_rows} rows; source columns {source_columns}"
    )
    if "_source_s" in records.column_names:
        values = dict(
            zip(
                records.column("k").to_pylist(), records.column("_source_s").to_pylist()
            )
        )
        print(f"  [{label}] _source_s by row: {dict(sorted(values.items()))}")
    table = node.as_table(all_info=True)
    print(f"  [{label}] as_table(all_info): {table.num_rows} rows")
    print(f"  [{label}] y: {sorted(table.column('y').to_pylist())}")


def attempt(label: str, append: Callable[[], tuple[PipelineJob, Any]]) -> None:
    """Run ``append`` and describe the result, or print how it failed."""
    print(label)
    try:
        job, summary = append()
    except BaseException as exc:  # noqa: BLE001  # the measurement reports any failure
        inner = exc.exceptions[0] if isinstance(exc, BaseExceptionGroup) else exc
        print("  FAILS:", type(exc).__name__, "/", type(inner).__name__)
        print("        ", str(inner).splitlines()[0][:220])
        return
    print("  run:", str(summary).splitlines()[-1].strip())
    describe(job, label)


def phase1(out: Path) -> None:
    """On main: async runs write r0..r3; the sync-after-async baseline."""
    shutil.rmtree(out, ignore_errors=True)
    for name in ("delta_then_async", "delta_then_sync", "delta_main_sync_append"):
        job, _ = run(DeltaTableDatabase(out / name), 4, "async")
        describe(job, f"main, async write, {name}")
    memory = InMemoryArrowDatabase()
    job, _ = run(memory, 4, "async")
    describe(job, "main, async write, memory")
    dump_memory(memory, out / "memory_dump")
    attempt(
        "baseline on main: sync run appends r4, r5 to the async-written Delta table",
        lambda: run(DeltaTableDatabase(out / "delta_main_sync_append"), 6, "sync"),
    )
    other = InMemoryArrowDatabase()
    run(other, 4, "async")
    attempt(
        "baseline on main: sync run appends r4, r5 to the async-written in-memory table",
        lambda: run(other, 6, "sync"),
    )


def phase1b() -> None:
    """On main: an async run appends r4, r5 to a sync-written table."""
    for kind in ("delta", "memory"):
        store: Any = (
            DeltaTableDatabase(Path(tempfile.mkdtemp()))
            if kind == "delta"
            else InMemoryArrowDatabase()
        )
        run(store, 4, "sync")
        attempt(
            f"baseline on main: async run appends to a sync-written {kind} table",
            lambda store=store: run(store, 6, "async"),
        )


def phase2(out: Path) -> None:
    """On the fixed build: append r4, r5 to the tables phase 1 wrote."""
    attempt(
        "delta, fixed async append",
        lambda: run(DeltaTableDatabase(out / "delta_then_async"), 6, "async"),
    )
    attempt(
        "delta, fixed sync append",
        lambda: run(DeltaTableDatabase(out / "delta_then_sync"), 6, "sync"),
    )
    attempt(
        "memory, fixed async append",
        lambda: run(load_memory(out / "memory_dump"), 6, "async"),
    )
    attempt(
        "memory, fixed sync append",
        lambda: run(load_memory(out / "memory_dump"), 6, "sync"),
    )


def operator_node() -> None:
    """A cached Join downstream of an async select: stored records, sync vs async."""

    def stored(orchestrator: Any) -> pa.Table:
        a = pa.table(
            {"k": ["a", "b"], "x": [1, 2], "s": ["p", "q"]},
            schema=pa.schema(
                [
                    pa.field("k", pa.large_string(), nullable=False),
                    pa.field("x", pa.int64(), nullable=False),
                    pa.field("s", pa.large_string(), nullable=False),
                ]
            ),
        )
        b = pa.table(
            {"k": ["a", "b"], "z": [10, 20]},
            schema=pa.schema(
                [
                    pa.field("k", pa.large_string(), nullable=False),
                    pa.field("z", pa.int64(), nullable=False),
                ]
            ),
        )
        store = InMemoryArrowDatabase()
        job = PipelineJob(name="j", store=store)
        with job:
            selected = SelectDataColumns(["x"])(
                ArrowTableSource(a, tag_columns=["k"], source_id="a")
            )
            Join()(
                selected,
                ArrowTableSource(b, tag_columns=["k"], source_id="b"),
                label="op",
            )
        job.nodes["op"].attach_databases(
            pipeline_database=store.at("j"), cache_mode=CacheMode.LOG
        )
        job.run(orchestrator=orchestrator)
        records = job.nodes["op"].get_all_records(all_info=True)
        assert records is not None
        return records.sort_by("k")

    sync = stored(SyncPipelineOrchestrator())
    async_ = stored(AsyncPipelineOrchestrator())
    for name, table in (("sync", sync), ("async", async_)):
        columns = [c for c in table.column_names if c.startswith("_source")]
        print(f"{name}: stored operator records, source-info columns {columns}")
    print("stored operator tables equal:", sync.equals(async_))


if __name__ == "__main__":
    command = sys.argv[1]
    if command == "phase1":
        phase1(Path(sys.argv[2]))
    elif command == "phase1b":
        phase1b()
    elif command == "phase2":
        phase2(Path(sys.argv[2]))
    elif command == "operator_node":
        operator_node()
    else:
        raise SystemExit(f"unknown command {command!r}")
