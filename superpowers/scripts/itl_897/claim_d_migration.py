"""ITL-897 claim (d): what a v0.2.0 store sees after the pandas hash changes.

Historical record (November 12, 2026): this script belongs to the investigation of
November 11, 2026 and was run against ``main`` as it was then. It is kept as the record of
that investigation and is not maintained: on this branch the library has the final hash
(``orcapod/hashing/pandas_value_hash.py``), so the ``old`` phase no longer
writes what ``main`` wrote and the script no longer reproduces its results.

Two separate processes share one Delta store:

1. ``old``: the released (v0.2.0 / ``main``) pandas handlers.
2. ``new``: the prototype hash in ``prototype_v1.py``, patched into the handlers in that
   process only.

Pipeline: ``make(reverse) -> df`` (pandas output; its input is a bool, so its cache key does
not involve a pandas hash), then ``first(df) -> df.columns[0]`` (pandas input, so its cache
key does). A third node, ``height(df)``, does not depend on column order.

The script prints, per run, which pod bodies ran, what each node yields, and the raw row
counts in each node's tables, so we can see what recomputes, where old rows stay, and whether
anything looks dropped.

Run: ``uv run python superpowers/scripts/itl_897/claim_d_migration.py``
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import textwrap
from pathlib import Path

CHILD = textwrap.dedent(
    r"""
    import json, sys, warnings
    from pathlib import Path
    warnings.simplefilter("ignore")
    import pandas as pd

    MODE, STORE, LOG = sys.argv[1], Path(sys.argv[2]), Path(sys.argv[3])
    EPHEMERAL = sys.argv[4] == "ephemeral"

    if MODE == "new":
        sys.path.insert(0, sys.argv[5])
        import prototype_v1

        prototype_v1.install()

    from orcapod import PipelineJob, function_pod
    from orcapod.databases import DeltaTableDatabase
    from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator
    from orcapod.sources import DictSource

    def _log(event):
        with LOG.open("a") as fh:
            fh.write(json.dumps([MODE, event]) + "\n")

    @function_pod(output_keys="df")
    def make(reverse: bool) -> pd.DataFrame:
        _log("make")
        df = pd.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        return df[["b", "a"]] if reverse else df

    @function_pod(output_keys="first")
    def first(df: pd.DataFrame) -> str:
        _log("first")
        return str(df.columns[0])

    @function_pod(output_keys="height")
    def height(df: pd.DataFrame) -> int:
        _log("height")
        return len(df)

    source = DictSource(
        [{"k": "fwd", "reverse": False}, {"k": "rev", "reverse": True}],
        tag_columns=["k"], source_id="frames",
    )
    store = DeltaTableDatabase(STORE)
    job = PipelineJob(name="migration", store=store)
    with job:
        frames = make.pod(source, label="make")
        first.pod(frames, label="first")
        height.pod(frames, label="height")
    if EPHEMERAL:
        # make's results live only in this process's ephemeral store; a later process
        # sees make's pipeline records only, with their stored output data hash.
        from orcapod.databases import InMemoryArrowDatabase
        from orcapod.types import NodeConfig

        job.nodes["make"].node_config = NodeConfig(is_result_ephemeral=True)
        job.set_ephemeral_store(InMemoryArrowDatabase())
    job.run(orchestrator=SyncPipelineOrchestrator())

    out = {}
    for label in ("make", "first", "height"):
        node = job.nodes[label]
        rows = []
        for tag, data in node.iter_data():
            value = data[{"make": "df", "first": "first", "height": "height"}[label]]
            rows.append([tag["k"], list(value.columns) if label == "make" else value])
        out[label] = sorted(rows, key=str)
    print(json.dumps(out))
    """
)


def _count_rows(store: Path) -> dict[str, int]:
    """Row count of every Delta table under ``store``, as ``<node>/<pipeline|result>``."""
    import deltalake

    counts: dict[str, int] = {}
    for log_dir in sorted(store.rglob("_delta_log")):
        table_dir = log_dir.parent
        parts = table_dir.relative_to(store).parts  # (job, [_result,] node, ...)
        label = (
            f"{parts[2]}/result" if parts[1] == "_result" else f"{parts[1]}/pipeline"
        )
        counts[label] = deltalake.DeltaTable(str(table_dir)).to_pyarrow_table().num_rows
    return counts


def main(variant: str) -> None:
    print(f"\n## variant: {variant} make results")
    with tempfile.TemporaryDirectory() as tmp:
        store, log, script = (
            Path(tmp) / "store",
            Path(tmp) / "log",
            Path(tmp) / "child.py",
        )
        script.write_text(CHILD)
        env = {**os.environ, "RUST_LOG": "error"}
        for mode in ("old", "old", "new", "new"):
            log.write_text("") if not log.exists() else None
            before = log.read_text().count("\n")
            proc = subprocess.run(
                [
                    sys.executable,
                    str(script),
                    mode,
                    str(store),
                    str(log),
                    variant,
                    str(Path(__file__).parent),
                ],
                capture_output=True,
                text=True,
                env=env,
                check=False,
            )
            if proc.returncode != 0:
                print(proc.stderr[-3000:])
                raise SystemExit(f"{mode} run failed")
            ran = [
                json.loads(line)[1] for line in log.read_text().splitlines()[before:]
            ]
            print(f"\n### run with {mode} hash")
            print(f"pod bodies run: {sorted(ran) or 'none'}")
            for label, rows in json.loads(proc.stdout.strip().splitlines()[-1]).items():
                print(f"  {label:6} yields {len(rows)} rows: {rows}")
            print(f"  Delta row counts: {_count_rows(store)}")


if __name__ == "__main__":
    main("persistent")
    main("ephemeral")
