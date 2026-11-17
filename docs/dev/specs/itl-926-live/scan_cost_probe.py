"""READ-ONLY: where the time goes when a small Spiral table is slow to scan.

Run after ``spiral login``, from the repository root:

    uv run python superpowers/specs/itl-926-live/scan_cost_probe.py edi-sample-v1-855632 \
        unit unit_location > scan_cost.txt

For each table it prints the number of column groups and fragments (from the manifests, no
data scan), then times one scan per column group (SpiralDB cannot scan the key columns
alone), the full table, and the full table again (to see whether a second read is cached).
It reads nothing else and writes nothing.
"""

from __future__ import annotations

import signal
import sys
import time


def main() -> None:
    """Print manifest counts and scan times for each table named on the command line."""
    signal.alarm(900)
    import spiral  # pyright: ignore[reportMissingImports]  (spiraldb extra, absent in the type-check job)

    project_id, tables = sys.argv[1], sys.argv[2:]
    sp = spiral.Spiral()
    project = sp.project(project_id)
    for name in tables:
        table = project.table(f"default.{name}")
        snapshot = table.core.get_snapshot(asof=None)
        stats = sp.core.manifest_stats(snapshot)
        groups = stats.column_group_stats()
        print(f"## {name}")
        print(
            f"  column groups: {len(groups)}; key-space fragments: "
            f"{stats.key_space_stats.total_fragments} "
            f"(L0 {stats.key_space_stats.l0_fragments})"
        )
        for group, group_stats in groups:
            print(
                f"    {group}: {group_stats.total_fragments} fragments "
                f"(L0 {group_stats.l0_fragments}), {group_stats.total_rows} rows"
            )
        # SpiralDB cannot scan key columns alone, so time one scan per column group (its
        # top-level column, which brings the keys along), then the full table twice.
        scans = [
            (f"group {path}", table.select(path))
            for path in sorted(
                str(group).split(".", 1)[1] for group, _ in groups if "." in str(group)
            )
        ]
        scans += [("full", table.select()), ("full again", table.select())]
        for label, expr in scans:
            start = time.time()
            try:
                rows = sp.scan(expr).to_table().num_rows
            except Exception as exc:  # noqa: BLE001 - report and go on
                print(f"  scan {label}: {type(exc).__name__}: {str(exc)[:200]}")
                continue
            print(f"  scan {label}: {rows} rows in {time.time() - start:.1f}s")


if __name__ == "__main__":
    main()
