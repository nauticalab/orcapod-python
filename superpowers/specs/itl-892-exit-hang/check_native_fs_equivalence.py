"""Option (c) check: a native filesystem returns exactly what deltalake's default returns.

Writes a Delta table with a spread of Arrow types (narrow ints, decimals, timestamps,
nested lists and structs, binary, nulls, non-nullable fields), appends and merges into
it, then reads it with ``to_pyarrow_dataset(as_large_types=True).to_table()`` twice:
once through the default ``PyFileSystem(DeltaStorageHandler)`` and once through
``SubTreeFileSystem(table_path, LocalFileSystem())``. Schemas (with field metadata and
nullability) and values must be identical, with and without a filter expression.
"""

from __future__ import annotations

import datetime as dt
import decimal
import tempfile

import deltalake
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pa_fs


def make(i: int) -> pa.Table:
    """Build 50 rows covering narrow, decimal, timestamp, binary, nested and null columns.

    Args:
        i: Batch number, which keeps the ids of each batch distinct.

    Returns:
        The table.
    """
    schema = pa.schema(
        [
            pa.field("id", pa.large_string(), nullable=False),
            pa.field("i8", pa.int8()),
            pa.field("f32", pa.float32()),
            pa.field("dec", pa.decimal128(12, 3)),
            pa.field("ts", pa.timestamp("us", tz="UTC")),
            pa.field("bin", pa.large_binary()),
            pa.field("lst", pa.large_list(pa.int16())),
            pa.field(
                "st",
                pa.struct(
                    [pa.field("a", pa.int32()), pa.field("b", pa.large_string())]
                ),
            ),
            pa.field("n", pa.large_string()),
        ]
    )
    rows = 50
    return pa.table(
        {
            "id": [f"r{i}-{k}" for k in range(rows)],
            "i8": [k % 100 for k in range(rows)],
            "f32": [k / 3 for k in range(rows)],
            "dec": [decimal.Decimal(k * 1001) / 1000 for k in range(rows)],
            "ts": [dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)] * rows,
            "bin": [bytes([k % 256]) * 3 for k in range(rows)],
            "lst": [[k, k + 1] if k % 5 else None for k in range(rows)],
            "st": [{"a": k, "b": str(k)} for k in range(rows)],
            "n": [None] * rows,
        },
        schema=schema,
    )


def read(
    table: deltalake.DeltaTable, native: bool, flt: pc.Expression | None
) -> pa.Table:
    """Read ``table`` through the default or the native filesystem, sorted by ``id``.

    Args:
        table: The Delta table.
        native: Read through a native ``SubTreeFileSystem`` instead of deltalake's.
        flt: Optional filter applied by the scan.

    Returns:
        The scanned rows.
    """
    kwargs = {}
    if native:
        kwargs["filesystem"] = pa_fs.SubTreeFileSystem(
            table.table_uri, pa_fs.LocalFileSystem()
        )
    ds = table.to_pyarrow_dataset(as_large_types=True, **kwargs)
    return ds.to_table(filter=flt).sort_by("id")


def main() -> None:
    """Compare default and native reads of one table across versions and filters."""
    path = tempfile.mkdtemp()
    deltalake.write_deltalake(path, make(0))
    deltalake.write_deltalake(path, make(1), mode="append")
    deltalake.DeltaTable(path).merge(
        source=make(2),
        predicate="target.id = source.id",
        source_alias="source",
        target_alias="target",
    ).when_not_matched_insert_all().execute()
    table = deltalake.DeltaTable(path)
    checks = 0
    rows = 0
    for flt in (
        None,
        pc.field("i8") > 10,
        pc.is_in(pc.field("id"), pa.array(["r1-3", "r2-4"])),
    ):
        a, b = read(table, False, flt), read(table, True, flt)
        assert a.schema.equals(b.schema, check_metadata=True), (a.schema, b.schema)
        assert a.equals(b), "values differ"
        checks += 1
        rows = a.num_rows
    print(
        f"files={len(table.file_uris())} last_filter_rows={rows} "
        f"checks={checks} identical schema+values"
    )
    print("done", flush=True)


if __name__ == "__main__":
    main()
