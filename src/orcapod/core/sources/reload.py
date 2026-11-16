"""Comparing the rows a source reads again with the rows it holds (ITL-1102).

A table-backed source that is asked at the start of a run whether its data changed
(``check_for_new_data``) reads its table again and compares. The comparison ignores row
order: a table read again in another order (an unordered ``SELECT``, a Delta table whose
files are scanned in another order) holds the same data, and must not make every node
below the source run again.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.ipc as pa_ipc
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")
    pa_ipc = LazyModule("pyarrow.ipc")
    pl = LazyModule("polars")


def keep_nullable_columns(read: pa.Table, held: pa.Schema) -> pa.Table:
    """Return ``read`` with each column that ``held`` has nullable marked nullable again.

    A source that infers nullability from its data (a CSV file read without a schema, a
    database whose connector gives no column information) reads a nullable column whose
    nulls are all gone as non-nullable. That column keeps its nullable type: its new
    values are valid values of it. Only a field that is nullable in ``held`` and
    non-nullable in ``read``, with the same name and type, changes; it is re-wrapped
    over the same arrays, never cast. Every other difference (a column added, removed
    or retyped, a null in a column ``held`` has non-nullable) is left for the caller's
    schema check to refuse.

    Args:
        read: The table as read again.
        held: The schema of the table the source holds.

    Returns:
        ``read``, or the same arrays under a schema with those fields nullable.
    """
    nullable = {field.name: field for field in held if field.nullable}
    fields = [
        field.with_nullable(True)
        if not field.nullable
        and field.name in nullable
        and field.type.equals(nullable[field.name].type)
        else field
        for field in read.schema
    ]
    schema = pa.schema(fields)
    if read.schema.metadata is not None:
        schema = schema.with_metadata(read.schema.metadata)
    if schema.equals(read.schema, check_metadata=True):
        return read
    return pa.Table.from_arrays(read.columns, schema=schema)


def same_rows(held: pa.Table, read: pa.Table, *, ordered: bool) -> bool:
    """Return whether two tables hold the same rows, in the same order if ``ordered``.

    The rule for row order (developer, November 14, 2026, ITL-1102): a source whose
    record ids are row positions (no ``record_id_columns``) compares in order, so a
    change of row order is a change and the ids are given again at once; a source
    with a ``record_id_columns`` compares in any order, so only content counts. A
    caller that cannot tell which rule applies passes ``ordered=True``: never assume.
    The model is deliberately simple; a fuller design is ITL-1103 / ITL-1104.

    Unordered, the tables are equal as multisets of rows: the same schema (field
    metadata included), the same number of rows, and each row as many times in one as
    in the other. Tables in the same order are compared directly; otherwise both are
    put in the order of a per-row hash, so a hash collision can only report a change
    that is not there. Values are compared exactly (Arrow's equality), except that a NaN equals
    a NaN: Arrow's equality never finds a NaN equal, which would make a table holding
    one look changed at every check. Where Arrow finds a difference, the rows are
    compared again with Polars' equality, which takes NaN as equal to NaN, and also
    -0.0 as equal to 0.0: a value changed only from 0.0 to -0.0 is not a change.

    Args:
        held: The rows the source holds.
        read: The rows just read again.
        ordered: Whether row order counts: ``True`` when record ids are row
            positions, or when the caller cannot tell.

    Returns:
        ``True`` if the rows are the same.
    """
    if held.num_rows != read.num_rows or not held.schema.equals(
        read.schema, check_metadata=True
    ):
        return False
    if held.equals(read) or _polars_equal(held, read):
        return True
    if ordered:
        return False
    held_hashes = _row_hashes(held)
    read_hashes = _row_hashes(read)
    held_order: pa.Array[Any] = pc.sort_indices(held_hashes)  # pyright: ignore[reportUnknownMemberType]  # pyarrow.compute is untyped
    read_order: pa.Array[Any] = pc.sort_indices(read_hashes)  # pyright: ignore[reportUnknownMemberType]  # pyarrow.compute is untyped
    held_sorted: pa.Table = held.take(held_order)
    read_sorted: pa.Table = read.take(read_order)
    return bool(held_sorted.equals(read_sorted)) or _polars_equal(
        held_sorted, read_sorted
    )


def _polars_equal(held: pa.Table, read: pa.Table) -> bool:
    """Return whether Polars finds two tables equal, row by row (NaN equals NaN).

    A table Polars cannot hold (a column of a type Polars does not know) is not equal.
    """
    try:
        return bool(pl.from_arrow(held).equals(pl.from_arrow(read)))  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportArgumentType]  # a table always gives a DataFrame
    except Exception:
        return False


def _row_hashes(table: pa.Table) -> "pa.Array[Any]":
    """Return one hash per row, used only to put two tables in a common order.

    Polars hashes the rows of tables it can hold. A table it cannot hold (a column of
    a type Polars does not know) is hashed row by row from its Arrow IPC bytes.
    """
    try:
        return pl.from_arrow(table).hash_rows(seed=0).to_arrow()  # pyright: ignore[reportAttributeAccessIssue, reportUnknownMemberType, reportUnknownVariableType]  # a table always gives a DataFrame
    except Exception:
        return pa.array(
            [_ipc_bytes(table.slice(i, 1)) for i in range(table.num_rows)],
            type=pa.large_binary(),
        )


def _ipc_bytes(row: pa.Table) -> bytes:
    """Return the Arrow IPC stream bytes of a one-row table."""
    sink = pa.BufferOutputStream()
    with pa_ipc.new_stream(sink, row.schema) as writer:
        writer.write_table(row)
    return sink.getvalue().to_pybytes()
