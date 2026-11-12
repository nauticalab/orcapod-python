"""Hash indexes over a table's rows, shared by the Arrow databases (ITL-919).

A database that answers per-row lookups by filtering its whole stored table makes a run
quadratic in its rows. These helpers keep, per stored table, lazily built hash indexes from
a column value to the positions of the rows holding it, extended in place when rows are
appended, and keep a growing table at a logarithmic number of record batches.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.compute as pc
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")


@dataclass
class KeyIndex:
    """Hash indexes over the rows of one stored table.

    Each column index maps a value to the positions, in ascending order, of the rows of
    ``table`` holding it. The indexes describe ``table`` only: they are valid while
    ``table`` is the table they were built for, and are rebuilt for any other table.

    Attributes:
        table: The indexed table.
        columns: Column name to its index, for the columns indexed so far.
    """

    table: "pa.Table"
    columns: dict[str, dict[Any, list[int]]] = field(default_factory=dict)

    def column(self, name: str) -> dict[Any, list[int]]:
        """Return the index of column ``name``, building it on first use.

        Args:
            name: A column of ``table``.

        Returns:
            The value-to-positions index of the column.
        """
        positions = self.columns.get(name)
        if positions is None:
            positions = {}
            for position, value in enumerate(self.table.column(name).to_pylist()):
                positions.setdefault(value, []).append(position)
            self.columns[name] = positions
        return positions

    def append(self, rows: "pa.Table", appended: "pa.Table") -> None:
        """Extend the indexes for ``rows`` appended after the indexed table's rows.

        Args:
            rows: The rows that were appended, with every indexed column.
            appended: The new table: the indexed table followed by ``rows``. It
                becomes the indexed table.
        """
        offset = self.table.num_rows
        for name, positions in self.columns.items():
            for position, value in enumerate(rows.column(name).to_pylist(), offset):
                positions.setdefault(value, []).append(position)
        self.table = appended


def is_indexed_lookup(arrow_type: "pa.DataType", value: Any) -> bool:
    """Whether an equality lookup of ``value`` in a column can use a hash index.

    Only binary and string columns queried with a value of the matching Python type
    qualify: for those, Python equality of the values is exactly Arrow's equality. Every
    other lookup filters the table, as Arrow defines that comparison.

    Args:
        arrow_type: The column's Arrow type.
        value: The value looked up.

    Returns:
        Whether the lookup can be answered from a hash index.
    """
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return type(value) is bytes
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return type(value) is str
    return False


def compact_tail(table: "pa.Table") -> "pa.Table":
    """Merge the table's trailing record batches while an earlier one is not larger.

    Called after each append, this keeps batch sizes strictly decreasing, like a binary
    counter: a table of N rows has at most log2(N) + 1 batches, and each row is copied
    O(log N) times over the table's life. Every Arrow operation on a table costs time per
    batch, so without it a table grown one append at a time makes every later operation
    cost time proportional to the appends so far.

    Args:
        table: The table just appended to.

    Returns:
        The same rows, in the same order, with the trailing batches merged.
    """
    batches = [batch for batch in table.to_batches() if batch.num_rows > 0]
    while len(batches) >= 2 and batches[-2].num_rows <= batches[-1].num_rows:
        merged = pa.Table.from_batches(batches[-2:]).combine_chunks()
        batches[-2:] = merged.to_batches()
    return pa.Table.from_batches(batches, schema=table.schema)


def rows_at(table: "pa.Table", positions: Sequence[int]) -> "pa.Table":
    """Return the rows of ``table`` at ``positions``, in the given order.

    Uses zero-copy one-row slices: ``take`` on a multi-chunk table concatenates every
    chunk first, copying all rows of the table.

    Args:
        table: The table to select from.
        positions: Row positions.

    Returns:
        The selected rows, with ``table``'s schema.
    """
    rows = [table.slice(position, 1) for position in positions]
    return pa.concat_tables(rows) if rows else table.slice(0, 0)


def matching_rows(
    key_index: KeyIndex,
    pairs: Sequence[tuple[str, Any]],
    expression: "pc.Expression",
) -> "pa.Table":
    """Return the rows of ``key_index.table`` where every column equals its value.

    The pairs that can use a hash index select the rows at the intersection of their
    indexes' positions. When every pair can, those rows are the result; when only some
    can, just those rows are filtered by ``expression``; when none can, the whole table
    is. Rows come back in table order.

    Args:
        key_index: The indexes of the table to search.
        pairs: ``(column, value)`` equality conditions, all to hold.
        expression: The same conditions as an Arrow expression.

    Returns:
        The matching rows (possibly none).
    """
    table = key_index.table
    indexed = [
        (column, value)
        for column, value in pairs
        if column in table.column_names
        and is_indexed_lookup(table.schema.field(column).type, value)
    ]
    if not indexed:
        return table.filter(expression)

    matches: set[int] | None = None
    for column, value in indexed:
        found = key_index.column(column).get(value, [])
        matches = set(found) if matches is None else matches.intersection(found)
    candidates = rows_at(table, sorted(matches or ()))
    if len(indexed) == len(pairs):
        return candidates
    return candidates.filter(expression)
