"""READ-ONLY: table sizes, and the cost of building ``SpiralDBTableSource`` on small ones.

Run after ``spiral login``, from the repository root:

    uv run python superpowers/specs/itl-926-live/size_probe.py edi-sample-v1-855632 \
        [--max-rows 200000] > sizes.txt

Step 1 reads each table's row count and size from Spiral's manifests (no data is scanned).
The counts are approximate: they are what the manifests record, before deletions and
compaction. Step 2 builds ``SpiralDBTableSource`` on every table with at most ``--max-rows``
rows, one table per child process so that each peak-memory figure is the table's own,
and reports the construction time and peak RSS. Tables above the cap are never read.
"""

from __future__ import annotations

import argparse
import json
import resource
import signal
import subprocess
import sys
import time
from typing import Any


def _peak_rss_mb() -> float:
    """Return this process's peak resident memory in MB."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / 1e6 if sys.platform == "darwin" else peak / 1e3


def _child(project: str, dataset: str, table: str) -> None:
    """Child phase: build one table's source and print its row count, time and memory."""
    from orcapod.sources import SpiralDBTableSource

    before = _peak_rss_mb()
    start = time.time()
    source = SpiralDBTableSource(project, table, dataset=dataset)
    built = time.time() - start
    rows = source.as_table().num_rows
    print(
        json.dumps(
            {
                "rows": rows,
                "seconds": round(built, 2),
                "rss_before_mb": round(before),
                "rss_peak_mb": round(_peak_rss_mb()),
            }
        )
    )


def _sizes(project_id: str, dataset: str) -> dict[str, dict[str, Any]]:
    """Return each table's manifest row count and sizes, without scanning data."""
    import spiral  # pyright: ignore[reportMissingImports]  (spiraldb extra, absent in the type-check job)

    sp = spiral.Spiral()
    project = sp.project(project_id)
    out: dict[str, dict[str, Any]] = {}
    for resource_ in sorted(project.list_tables(), key=lambda r: r.table):
        if resource_.dataset != dataset:
            continue
        table = project.table(f"{dataset}.{resource_.table}")
        snapshot = table.core.get_snapshot(asof=None)
        stats = sp.core.manifest_stats(snapshot)
        raw = sum(s.total_raw_size_bytes for _, s in stats.column_group_stats())
        out[resource_.table] = {
            "manifest_rows": stats.key_space_stats.total_rows,
            "raw_mb": round(raw / 1e6, 1),
            "stored_mb": round(
                sum(s.total_size_bytes for _, s in stats.column_group_stats()) / 1e6, 1
            ),
        }
    return out


def main() -> None:
    """Print table sizes, then build and measure the sources under the row cap."""
    parser = argparse.ArgumentParser(description="Read-only Spiral table size probe")
    parser.add_argument("project")
    parser.add_argument("--dataset", default="default")
    parser.add_argument("--max-rows", type=int, default=200_000)
    parser.add_argument("--child", default=None, help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.child:
        _child(args.project, args.dataset, args.child)
        return

    signal.alarm(1800)
    sizes = _sizes(args.project, args.dataset)
    for name, info in sizes.items():
        print(f"{name:40} {info}")
    print()
    for name, info in sizes.items():
        if info["manifest_rows"] > args.max_rows:
            print(f"{name:40} skipped ({info['manifest_rows']} rows > --max-rows)")
            continue
        proc = subprocess.run(
            [
                sys.executable,
                __file__,
                args.project,
                "--dataset",
                args.dataset,
                "--child",
                name,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        lines = proc.stdout.strip().splitlines()
        result = lines[-1] if proc.returncode == 0 and lines else proc.stderr[-400:]
        print(f"{name:40} {result}")


if __name__ == "__main__":
    main()
