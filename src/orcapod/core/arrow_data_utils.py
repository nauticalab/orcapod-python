# Collection of functions to work with Arrow table data that underlies streams and/or datagrams
from orcapod.utils.lazy_module import LazyModule
from typing import TYPE_CHECKING
from orcapod.system_constants import constants
from collections.abc import Collection

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


def drop_columns_with_prefix(
    table: "pa.Table",
    prefix: str | tuple[str, ...],
    exclude_columns: Collection[str] = (),
) -> "pa.Table":
    """Drop columns with a specific prefix from an Arrow table."""
    columns_to_drop = [
        col
        for col in table.column_names
        if col.startswith(prefix) and col not in exclude_columns
    ]
    return table.drop(columns=columns_to_drop)


def drop_system_columns(
    table: "pa.Table",
    system_column_prefix: tuple[str, ...] = (
        constants.META_PREFIX,
        constants.DATAGRAM_PREFIX,
    ),
) -> "pa.Table":
    return drop_columns_with_prefix(table, system_column_prefix)


def get_system_columns(table: "pa.Table") -> "pa.Table":
    """Get system columns from an Arrow table."""
    return table.select(
        [
            col
            for col in table.column_names
            if col.startswith(constants.SYSTEM_TAG_PREFIX)
        ]
    )


def add_system_tag_column(
    table: "pa.Table",
    system_tag_column_name: str,
    system_tag_values: str | Collection[str],
) -> "pa.Table":
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


def append_to_system_tags(table: "pa.Table", value: str) -> "pa.Table":
    """Append a value to the system tags column in an Arrow table."""
    if not table.column_names:
        raise ValueError("Table is empty")

    column_name_map = {
        c: f"{c}:{value}" if c.startswith(constants.SYSTEM_TAG_PREFIX) else c
        for c in table.column_names
    }
    return table.rename_columns(column_name_map)


def add_source_info(
    table: "pa.Table",
    source_info: str | Collection[str] | None,
    exclude_prefixes: Collection[str] = (
        constants.META_PREFIX,
        constants.DATAGRAM_PREFIX,
    ),
    exclude_columns: Collection[str] = (),
) -> "pa.Table":
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
