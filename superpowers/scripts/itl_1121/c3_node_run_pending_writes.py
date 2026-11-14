"""Comment 3: node.run() starts without the pre-run flush that job.run() makes (ITL-919).

The hazard ITL-919 guards against: a table-committing store reaches batch_size on a
table mid-run and commits pipeline records whose results are still pending. Check, on a
Delta store with a small batch_size and rows left pending before the run, that every
committed pipeline-record version only references committed results after node.run().
"""

import tempfile
from pathlib import Path

from typing import Any

import deltalake
import pyarrow as pa

from orcapod import PipelineJob, function_pod
from orcapod.databases import DeltaTableDatabase
from orcapod.databases.storage_utils import delta_to_pyarrow_dataset
from orcapod.sources import DictSource


@function_pod(output_keys="y")
def double(x: int) -> int:
    return 2 * x


def col(table: deltalake.DeltaTable, name: str) -> set[object]:
    return set(
        delta_to_pyarrow_dataset(table).to_table(columns=[name])[name].to_pylist()
    )


# Every Delta commit, in the order it happened: (table directory, rows written). Commit
# timestamps have millisecond resolution, so equal timestamps could not order two commits;
# the order of the write calls does.
COMMITS: list[tuple[str, pa.Table]] = []
_real_write = deltalake.write_deltalake


def _recording_write(table_or_uri: Any, data: Any, *args: Any, **kwargs: Any) -> Any:
    COMMITS.append(
        (str(table_or_uri), pa.table(data) if not isinstance(data, pa.Table) else data)
    )
    return _real_write(table_or_uri, data, *args, **kwargs)


deltalake.write_deltalake = _recording_write  # pyright: ignore[reportAttributeAccessIssue]  # a recording wrapper for this script only


def check(root: Path) -> None:
    pipeline = [
        t for path, t in COMMITS if "_result" not in path and "leftover" not in path
    ]
    results = [t for path, t in COMMITS if "_result" in path]
    tables = {
        ("result" if "_result" in t.relative_to(root).parts else "pipeline"): t
        for t in {log.parent for log in root.rglob("_delta_log")}
        if t.name != "leftover"
    }
    history = {
        kind: len(deltalake.DeltaTable(str(t)).history()) for kind, t in tables.items()
    }
    print(
        f"  recorded commits: pipeline {len(pipeline)}, result {len(results)}; "
        f"table histories: {history}"
    )
    committed: set[object] = set()
    bad = 0
    for path, table in COMMITS:
        if "_result" in path:
            committed |= set(table.column("__record_id").to_pylist())
        elif "leftover" not in path:
            if not set(table.column("__data_id").to_pylist()) <= committed:
                bad += 1
    print(f"  pipeline commits referencing a result not committed before them: {bad}")


for batch_size, n in [(50, 120), (5, 40)]:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        store = DeltaTableDatabase(root, batch_size=batch_size)
        for i in range(batch_size - 1):  # pending, never flushed
            store.add_record(
                ("leftover",),
                f"left-{i}".encode(),
                pa.table({"v": pa.array([i], pa.int64())}),
            )
        job = PipelineJob(name="j", database=store)
        with job:
            double.pod(
                DictSource(
                    [{"k": i, "x": i} for i in range(n)],
                    tag_columns=["k"],
                    source_id="s",
                ),
                label="dbl",
            )
        COMMITS.clear()
        print(f"batch_size={batch_size}, rows={n}: node.run()")
        job.nodes["dbl"].run()
        check(root)
