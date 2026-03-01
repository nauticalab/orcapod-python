# Collection of functions to work with Arrow table data that underlies streams and/or datagrams
from __future__ import annotations

from collections.abc import Collection
from typing import TYPE_CHECKING

from orcapod.system_constants import constants
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


def drop_columns_with_prefix(
    table: pa.Table,
    prefix: str | tuple[str, ...],
    exclude_columns: Collection[str] = (),
) -> pa.Table:
    """Drop columns with a specific prefix from an Arrow table."""
    columns_to_drop = [
        col
        for col in table.column_names
        if col.startswith(prefix) and col not in exclude_columns
    ]
    return table.drop(columns=columns_to_drop)


def drop_system_columns(
    table: pa.Table,
    system_column_prefix: tuple[str, ...] = (
        constants.META_PREFIX,
        constants.DATAGRAM_PREFIX,
    ),
) -> pa.Table:
    return drop_columns_with_prefix(table, system_column_prefix)


def get_system_columns(table: pa.Table) -> pa.Table:
    """Get system columns from an Arrow table."""
    return table.select(
        [
            col
            for col in table.column_names
            if col.startswith(constants.SYSTEM_TAG_PREFIX)
        ]
    )


def add_system_tag_column(
    table: pa.Table,
    system_tag_column_name: str,
    system_tag_values: str | Collection[str],
) -> pa.Table:
    """Add a system tags column to an Arrow table."""
    if not table.column_names:
        raise ValueError("Table is empty")
    if isinstance(system_tag_values, str):
        system_tag_values = [system_tag_values] * table.num_rows
    else:
        system_tag_values = list(system_tag_values)
        if len(system_tag_values) != table.num_rows:
            raise ValueError(
                "Length of system_tag_values must match number of rows in the table."
            )
    if not system_tag_column_name.startswith(constants.SYSTEM_TAG_PREFIX):
        system_tag_column_name = (
            f"{constants.SYSTEM_TAG_PREFIX}{system_tag_column_name}"
        )
    tags_column = pa.array(system_tag_values, type=pa.large_string())
    return table.append_column(system_tag_column_name, tags_column)


def append_to_system_tags(table: pa.Table, value: str) -> pa.Table:
    """Append a value to the system tags column in an Arrow table."""
    if not table.column_names:
        raise ValueError("Table is empty")

    column_name_map = {
        c: f"{c}{constants.BLOCK_SEPARATOR}{value}"
        if c.startswith(constants.SYSTEM_TAG_PREFIX)
        else c
        for c in table.column_names
    }
    return table.rename_columns(column_name_map)


def sort_system_tag_values(table: pa.Table) -> pa.Table:
    """Sort system tag values for columns that share the same base name.

    System tag columns that differ only by their canonical position (the final
    :N in the column name) represent streams with the same pipeline_hash that
    were joined. For commutativity, their values must be sorted per row so that
    the result is independent of input order.

    For each group of columns sharing the same base, values are sorted per row
    and reassigned in canonical position order (lowest position gets smallest value).
    """
    sys_tag_cols = [
        c for c in table.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
    ]

    if not sys_tag_cols:
        return table

    # Group by base (everything except the final :position)
    groups: dict[str, list[tuple[str, str]]] = {}
    for col in sys_tag_cols:
        base, sep, position = col.rpartition(constants.FIELD_SEPARATOR)
        if sep and position.isdigit():
            groups.setdefault(base, []).append((col, position))

    # For each group with >1 member, sort values per row
    for base, members in groups.items():
        if len(members) <= 1:
            continue

        # Sort members by position for consistent column ordering
        members.sort(key=lambda m: int(m[1]))
        col_names = [m[0] for m in members]

        # Get values for all columns in this group
        col_values = [table.column(c).to_pylist() for c in col_names]

        # Sort per row across the group
        sorted_col_values: list[list] = [[] for _ in col_names]
        for row_idx in range(table.num_rows):
            row_vals = [
                col_values[col_idx][row_idx] for col_idx in range(len(col_names))
            ]
            row_vals.sort()
            for col_idx, val in enumerate(row_vals):
                sorted_col_values[col_idx].append(val)

        # Replace columns with sorted values (preserve original positions)
        for col_idx, col_name in enumerate(col_names):
            orig_col_type = table.column(col_name).type
            tbl_idx = table.column_names.index(col_name)
            table = table.drop(col_name)
            table = table.add_column(
                tbl_idx,
                col_name,
                pa.array(sorted_col_values[col_idx], type=orig_col_type),
            )

    return table


def add_source_info(
    table: pa.Table,
    source_info: str | Collection[str] | None,
    exclude_prefixes: Collection[str] = (
        constants.META_PREFIX,
        constants.DATAGRAM_PREFIX,
    ),
    exclude_columns: Collection[str] = (),
) -> pa.Table:
    """Add source information to an Arrow table."""
    # Create a new column with the source information
    if source_info is None or isinstance(source_info, str):
        source_column = [source_info] * table.num_rows
    elif isinstance(source_info, Collection):
        if len(source_info) != table.num_rows:
            raise ValueError(
                "Length of source_info collection must match number of rows in the table."
            )
        source_column = source_info

    # identify columns for which source columns should be created

    for col in table.column_names:
        if col.startswith(tuple(exclude_prefixes)) or col in exclude_columns:
            continue
        source_column = pa.array(
            [f"{source_val}::{col}" for source_val in source_column],
            type=pa.large_string(),
        )
        table = table.append_column(f"{constants.SOURCE_PREFIX}{col}", source_column)

    return table
