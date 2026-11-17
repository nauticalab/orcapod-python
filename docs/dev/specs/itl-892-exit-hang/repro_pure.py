"""Minimal ITL-892 reproduction: pyarrow + deltalake only, no Orcapod.

Builds a small Delta table in a temporary directory, performs the operations chosen by
``--op``, prints ``done`` and returns normally so the interpreter runs its shutdown.

Ops:
    write          one ``write_deltalake`` (pyarrow input), no read.
    merge          one write, then four ``DeltaTable.merge`` calls, no read.
    to_table       one write, then ``DeltaTable.to_pyarrow_table()``.
    to_dataset     one write, then ``DeltaTable.to_pyarrow_dataset()`` (no scan).
    dataset_scan   one write, then ``to_pyarrow_dataset().to_table()``.
    itl864         the ITL-864 worker's script: write, four merges, and a
                   ``to_pyarrow_dataset(as_large_types=True).to_table()`` after each.
    pyarrow_only   pyarrow parquet write + dataset scan, deltalake imported but unused.
"""

from __future__ import annotations

import argparse
import atexit
import gc
import os
import sys
import tempfile


def build(path: str, i: int):
    """Build the one-row table written in round ``i``.

    Args:
        path: Table directory (unused; kept for the call sites).
        i: Round number.

    Returns:
        The table.
    """
    import pyarrow as pa

    return pa.table(
        {"k": pa.array([i], pa.int64()), "v": pa.array([str(i)], pa.large_string())}
    )


def run(op: str, read_via: str, rounds: int = 4) -> list[object]:
    """Perform ``op``; return the objects created so mitigations can drop them."""
    import deltalake
    import pyarrow as pa

    keep: list[object] = []
    path = tempfile.mkdtemp()

    def read() -> None:
        """Read the table through the selected reader, keeping every object it creates."""
        dt = deltalake.DeltaTable(path)
        keep.append(dt)
        if read_via == "dataset":
            ds = dt.to_pyarrow_dataset(as_large_types=True)
            keep.append(ds)
            keep.append(ds.to_table())
        elif read_via == "table":
            keep.append(dt.to_pyarrow_table())
        elif read_via == "native_fs":
            # Same dataset path, but a native pyarrow filesystem instead of the default
            # PyFileSystem(DeltaStorageHandler), so no Python file object is involved.
            import pyarrow.fs as pa_fs

            fs = pa_fs.SubTreeFileSystem(path, pa_fs.LocalFileSystem())
            ds = dt.to_pyarrow_dataset(as_large_types=True, filesystem=fs)
            keep.append(ds)
            keep.append(ds.to_table())
        elif read_via == "no_threads":
            ds = dt.to_pyarrow_dataset(as_large_types=True)
            keep.append(ds)
            keep.append(ds.to_table(use_threads=False))
        elif read_via == "querybuilder":
            ctx = deltalake.QueryBuilder().register("t", dt)
            keep.append(pa.table(ctx.execute("select * from t").read_all()))
        elif read_via == "files":
            import pyarrow.parquet as pq

            keep.append(pq.read_table(dt.file_uris()))
        elif read_via == "polars":
            import polars as pl

            keep.append(pl.read_delta(path))
        else:
            raise ValueError(read_via)

    if op == "pyarrow_only":
        import pyarrow.dataset as pads
        import pyarrow.parquet as pq

        pq.write_table(build(path, 0), os.path.join(path, "a.parquet"))
        keep.append(pads.dataset(path).to_table())
        return keep

    deltalake.write_deltalake(path, build(path, 0), mode="overwrite")
    if op == "write":
        return keep
    if op == "merge":
        for i in range(1, 5):
            deltalake.DeltaTable(path).merge(
                source=build(path, i),
                predicate="target.k = source.k",
                source_alias="source",
                target_alias="target",
            ).when_not_matched_insert_all().execute()
        return keep
    if op == "to_table":
        read_via = "table" if read_via == "dataset" else read_via
        read()
        return keep
    if op == "to_dataset":
        dt = deltalake.DeltaTable(path)
        keep += [dt, dt.to_pyarrow_dataset(as_large_types=True)]
        return keep
    if op == "dataset_scan":
        read()
        return keep
    if op == "itl864":
        read()
        for i in range(1, rounds + 1):
            deltalake.DeltaTable(path).merge(
                source=build(path, i),
                predicate="target.k = source.k",
                source_alias="source",
                target_alias="target",
            ).when_not_matched_insert_all().execute()
            read()
        return keep
    raise ValueError(op)


def main() -> None:
    """Run the selected operation, apply the selected mitigation and print ``done``."""
    ap = argparse.ArgumentParser()
    ap.add_argument("--op", default="itl864")
    ap.add_argument(
        "--read-via",
        default="dataset",
        choices=[
            "dataset",
            "table",
            "native_fs",
            "no_threads",
            "querybuilder",
            "files",
            "polars",
        ],
    )
    ap.add_argument("--rounds", type=int, default=4, help="merge rounds for itl864")
    ap.add_argument("--import-polars", action="store_true")
    ap.add_argument("--polars-first", action="store_true")
    ap.add_argument("--cpu-threads", type=int)
    ap.add_argument("--io-threads", type=int)
    ap.add_argument(
        "--mitigation",
        default="none",
        choices=[
            "none",
            "del",
            "atexit_del",
            "os_exit",
            "atexit_sleep",
            "atexit_sleep_long",
        ],
    )
    ap.add_argument("--mp", default="none", choices=["none", "fork", "spawn"])
    args = ap.parse_args()

    if args.polars_first:
        import polars  # noqa: F401
    import pyarrow as pa

    if args.import_polars:
        import polars  # noqa: F401
    if args.cpu_threads is not None:
        pa.set_cpu_count(args.cpu_threads)
    if args.io_threads is not None:
        pa.set_io_thread_count(args.io_threads)

    if args.mp != "none":
        import multiprocessing as mp

        ctx = mp.get_context(args.mp)
        p = ctx.Process(target=run, args=(args.op, args.read_via, args.rounds))
        p.start()
        p.join()
        if p.exitcode != 0:
            # A child that raised or died on a signal is a failed run, not a clean one.
            sys.exit(f"child process failed with exit code {p.exitcode}")
        keep: list[object] = []
    else:
        keep = run(args.op, args.read_via, args.rounds)

    print("done", flush=True)
    if args.mitigation == "del":
        keep.clear()
        gc.collect()
    elif args.mitigation == "atexit_del":
        atexit.register(lambda: (keep.clear(), gc.collect()))
    elif args.mitigation in ("atexit_sleep", "atexit_sleep_long"):
        # Release the GIL before finalization so pool workers can drop their Python
        # references while the interpreter can still give them the GIL (the idea of
        # apache/arrow#54467, approximated from Python).
        import time

        delay = 0.05 if args.mitigation == "atexit_sleep" else 0.5
        atexit.register(time.sleep, delay)
    elif args.mitigation == "os_exit":
        sys.stdout.flush()
        os._exit(0)


if __name__ == "__main__":
    main()
