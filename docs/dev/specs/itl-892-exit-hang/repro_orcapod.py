"""ITL-892 through Orcapod: does a process that used a Delta store exit?

Modes:
    pipeline-sync    ``PipelineJob`` on ``DeltaTableDatabase``, sync orchestrator, then
                     ``get_all_records`` on the function node.
    pipeline-async   the same with the async orchestrator.
    plain-read       ``DeltaTableDatabase`` write, flush, ``get_all_records``.
    source-read      ``DeltaTableSource`` over a table written by ``write_deltalake``.
    rerun            ``pipeline-sync`` twice on the same store (second run is all cache hits).

Prints ``done`` and returns normally so the interpreter runs its shutdown.
"""

from __future__ import annotations

import argparse
import os
import tempfile
from pathlib import Path


def pipeline(store_dir: Path, orchestrator_name: str, runs: int = 1) -> None:
    """Run a one-pod pipeline on a Delta store and read the node's records back.

    Args:
        store_dir: Root of the Delta store.
        orchestrator_name: ``sync`` or ``async``.
        runs: How many times to build and run the pipeline.
    """
    import orcapod as op
    from orcapod.databases import DeltaTableDatabase
    from orcapod.pipeline import AsyncPipelineOrchestrator, PipelineJob
    from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

    orch = {
        "sync": SyncPipelineOrchestrator,
        "async": AsyncPipelineOrchestrator,
    }[orchestrator_name]

    @op.function_pod(output_keys=["result"])
    def double(x: int) -> int:
        """Return ``2 * x``."""
        return x * 2

    for _ in range(runs):
        source = op.sources.DictSource(
            [{"id": i, "x": 10 * i} for i in range(3)], tag_columns=["id"]
        )
        job = PipelineJob(database=DeltaTableDatabase(store_dir))
        with job:
            double.pod(source, label="double")
        job.run(orchestrator=orch())
        # Row counts are not checked: main still has the ITL-864 partial read.
        job.nodes["double"].get_all_records()


def plain_read(store_dir: Path) -> None:
    """Write three records, then read them through a fresh ``DeltaTableDatabase``.

    Args:
        store_dir: Root of the Delta store.
    """
    import pyarrow as pa

    from orcapod.databases import DeltaTableDatabase

    db = DeltaTableDatabase(store_dir)
    table = pa.table(
        {
            "__record_id": pa.array(["a", "b", "c"], pa.large_string()),
            "v": pa.array([1, 2, 3], pa.int64()),
        }
    )
    db.add_records(("t",), table, record_id_column="__record_id")
    db.flush()
    fresh = DeltaTableDatabase(store_dir)
    records = fresh.get_all_records(("t",), record_id_column="__record_id")
    # get_all_records returns None when a scan fails, so a broken patch must not pass.
    assert records is not None and records.num_rows == 3, records


def source_read(store_dir: Path) -> None:
    """Write a Delta table with deltalake and read it through ``DeltaTableSource``.

    Args:
        store_dir: Directory for the table.
    """
    import deltalake
    import pyarrow as pa

    import orcapod as op

    path = store_dir / "src"
    deltalake.write_deltalake(
        str(path),
        pa.table({"id": pa.array([1, 2, 3]), "x": pa.array([1.0, 2.0, 3.0])}),
    )
    src = op.sources.DeltaTableSource(path, tag_columns=["id"])
    assert src.as_table().num_rows == 3


def apply_patch(name: str) -> None:
    """Prototype a mitigation by wrapping deltalake, so ``src/`` stays unchanged.

    native-fs   Local tables are scanned through a native pyarrow filesystem rooted at
                the table, instead of deltalake's default
                ``PyFileSystem(DeltaStorageHandler)``; option (c).
    no-threads  Every ``Dataset.to_table`` built by deltalake runs with
                ``use_threads=False``; option (c), alternative.
    atexit-gc   An ``atexit`` hook runs ``gc.collect()``; option (b).
    atexit-sleep  An ``atexit`` hook releases the GIL for 50 ms so pool workers can drop
                their Python references before finalization; option (b), after
                apache/arrow#54467.
    """
    if name == "none":
        return
    if name == "atexit-sleep":
        import atexit
        import time

        atexit.register(time.sleep, 0.05)
        return
    if name == "atexit-gc":
        import atexit
        import gc

        atexit.register(gc.collect)
        return

    import deltalake
    import pyarrow.fs as pa_fs

    original = deltalake.DeltaTable.to_pyarrow_dataset

    def to_pyarrow_dataset(self, *args, **kwargs):  # noqa: ANN001, ANN002, ANN003, ANN202
        """Open the dataset with the selected prototype applied."""
        if name == "native-fs" and kwargs.get("filesystem") is None:
            uri = self.table_uri
            local = uri[len("file://") :] if uri.startswith("file://") else uri
            if "://" not in local:
                kwargs["filesystem"] = pa_fs.SubTreeFileSystem(
                    local, pa_fs.LocalFileSystem()
                )
        ds = original(self, *args, **kwargs)
        if name == "no-threads":
            to_table = ds.to_table

            class _NoThreads:
                """Dataset proxy whose ``to_table`` runs single-threaded."""

                def __getattr__(self, attr: str):  # noqa: ANN204
                    """Forward every other attribute to the wrapped dataset."""
                    return getattr(ds, attr)

                def to_table(self, *a, **kw):  # noqa: ANN002, ANN003, ANN202
                    """Scan the wrapped dataset with ``use_threads=False``."""
                    kw["use_threads"] = False
                    return to_table(*a, **kw)

            return _NoThreads()
        return ds

    deltalake.DeltaTable.to_pyarrow_dataset = to_pyarrow_dataset  # pyright: ignore[reportAttributeAccessIssue]  # experiment-only monkeypatch


def main() -> None:
    """Apply the selected patch, run the selected scenario and print ``done``."""
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "mode",
        choices=[
            "pipeline-sync",
            "pipeline-async",
            "plain-read",
            "source-read",
            "rerun",
        ],
    )
    ap.add_argument(
        "--patch",
        default="none",
        choices=["none", "native-fs", "no-threads", "atexit-gc", "atexit-sleep"],
        help="prototype a mitigation without editing src/",
    )
    args = ap.parse_args()
    apply_patch(os.environ.get("ITL892_PATCH") or str(args.patch))
    store = Path(tempfile.mkdtemp())
    if args.mode == "pipeline-sync":
        pipeline(store, "sync")
    elif args.mode == "pipeline-async":
        pipeline(store, "async")
    elif args.mode == "rerun":
        pipeline(store, "sync", runs=2)
    elif args.mode == "plain-read":
        plain_read(store)
    else:
        source_read(store)
    print("done", flush=True)


if __name__ == "__main__":
    main()
