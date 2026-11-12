"""READ-ONLY inspection of a SpiralDB project's tables for ITL-926.

Calls only read methods (``list_tables``, ``Table.schema``, ``Table.key_schema``,
``Table.get_metadata`` and a ``limit``-ed scan), so it is safe on the workshop's EDI sample
projects. Run after ``spiral login``, from the repository root:

    uv run python superpowers/specs/itl-926-live/inspect_tables.py edi-sample-v1-855632 \
        [--tables session,experiment] [--limit 3] > inspect.txt

For every table it prints the key schema, the Arrow schema ``Table.schema().to_arrow()``
reports (``!`` marks a non-null field), the Arrow type and nullability a ``limit``-ed scan
returns where they differ, the null count in the sample, and any Arrow or key-value metadata.
The run aborts after ``--timeout`` seconds so it can never sit at a login prompt.
"""

from __future__ import annotations

import argparse
import signal
import time
from typing import Any


def _desc(f: Any) -> str:
    """Describe a field as its type, marking a non-null one with ``!``."""
    return f"{f.type}{'' if f.nullable else '!'}"


def main() -> None:
    """Print the key schema, Arrow schema and a sample of each table, reading only."""
    parser = argparse.ArgumentParser(description="Read-only SpiralDB table inspection")
    parser.add_argument("project")
    parser.add_argument("--dataset", default="default")
    parser.add_argument("--tables", default=None, help="comma-separated; default: all")
    parser.add_argument("--limit", type=int, default=3, help="rows to sample per table")
    parser.add_argument("--timeout", type=int, default=300)
    args = parser.parse_args()
    signal.alarm(args.timeout)

    import spiral  # pyright: ignore[reportMissingImports]  (spiraldb extra, absent in the type-check job)

    sp = spiral.Spiral()
    project = sp.project(args.project)
    names = sorted(r.table for r in project.list_tables() if r.dataset == args.dataset)
    if args.tables:
        wanted = args.tables.split(",")
        names = [n for n in names if n in wanted]
    print(f"{args.project}/{args.dataset}: {len(names)} tables")
    for name in names:
        table = project.table(f"{args.dataset}.{name}")
        key_schema: Any = table.key_schema
        if hasattr(key_schema, "to_arrow"):  # pyspiral >= 0.16 returns a spiral Schema
            key_schema = key_schema.to_arrow()
        schema = table.schema().to_arrow()
        t0 = time.time()
        sample = sp.scan(table.select(), limit=args.limit).to_table()
        print(
            f"\n## {name}  (sample {sample.num_rows} rows in {time.time() - t0:.1f}s)"
        )
        print("  key:", ", ".join(f"{f.name}:{_desc(f)}" for f in key_schema))
        for field in schema:
            line = f"  {field.name}: {_desc(field)}"
            if field.name in sample.schema.names:
                scanned = sample.schema.field(field.name)
                if _desc(scanned) != _desc(field):
                    line += f"  scan={_desc(scanned)}"
                line += f"  nulls={sample.column(field.name).null_count}"
            if field.metadata:
                line += f"  field_metadata={sorted(field.metadata)}"
            print(line)
        if sample.schema.metadata:
            print("  schema_metadata:", sorted(sample.schema.metadata))
        kv = table.get_metadata()
        if kv:
            print("  kv_metadata keys:", sorted(kv))


if __name__ == "__main__":
    main()
