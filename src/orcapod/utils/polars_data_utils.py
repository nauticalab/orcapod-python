# Collection of functions to work with Arrow table data that underlies streams and/or datagrams
from __future__ import annotations

from collections.abc import Collection
from typing import TYPE_CHECKING

from orcapod.system_constants import constants
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
else:
    pl = LazyModule("polars")


def drop_columns_with_prefix(
    df: "pl.DataFrame",
    prefix: str | tuple[str, ...],
    exclude_columns: Collection[str] = (),
) -> "pl.DataFrame":
    """Drop columns with a specific prefix from a Polars DataFrame."""
    columns_to_drop = [
        col
        for col in df.columns
        if col.startswith(prefix) and col not in exclude_columns
    ]
    return df.drop(*columns_to_drop)


def drop_system_columns(
    df: "pl.DataFrame",
    system_column_prefix: tuple[str, ...] = (
        constants.META_PREFIX,
        constants.DATAGRAM_PREFIX,
    ),
) -> "pl.DataFrame":
    return drop_columns_with_prefix(df, system_column_prefix)


def get_system_columns(
    df: "pl.DataFrame",
    system_column_prefix: tuple[str, ...] = (
        constants.META_PREFIX,
        constants.DATAGRAM_PREFIX,
    ),
) -> "pl.DataFrame":
    """Get system columns from a Polars DataFrame."""
    return df.select(
        [col for col in df.columns if col.startswith(system_column_prefix)]
    )


def add_system_tag_column(
    df: "pl.DataFrame",
    system_tag_column_name: str,
    system_tag_values: str | Collection[str],
) -> "pl.DataFrame":
    """Add a system tags column to a Polars DataFrame."""
    if df.is_empty():
        raise ValueError("DataFrame is empty")
    if isinstance(system_tag_values, str):
        system_tag_values = [system_tag_values] * df.height
    else:
        system_tag_values = list(system_tag_values)
        if len(system_tag_values) != df.height:
            raise ValueError(
                "Length of system_tag_values must match number of rows in the DataFrame."
            )
    if not system_tag_column_name.startswith(constants.SYSTEM_TAG_PREFIX):
        system_tag_column_name = (
            f"{constants.SYSTEM_TAG_PREFIX}{system_tag_column_name}"
        )
    tags_column = pl.Series(
        system_tag_column_name, system_tag_values, dtype=pl.String()
    )
    return df.with_columns(tags_column)


def append_to_system_tags(df: "pl.DataFrame", value: str) -> "pl.DataFrame":
    """Append a value to the system tags column in an Arrow table."""
    if df.is_empty():
        raise ValueError("Table is empty")

    df.rename
    column_name_map = {
        c: f"{c}{constants.BLOCK_SEPARATOR}{value}"
        for c in df.columns
        if c.startswith(constants.SYSTEM_TAG_PREFIX)
    }
    return df.rename(column_name_map)


def add_source_info(
    df: "pl.DataFrame",
    source_info: str | Collection[str] | None,
    exclude_prefixes: Collection[str] = (
        constants.META_PREFIX,
        constants.DATAGRAM_PREFIX,
    ),
    exclude_columns: Collection[str] = (),
) -> "pl.DataFrame":
    """Add source information to an Arrow table."""
    # Create a new column with the source information
    if source_info is None or isinstance(source_info, str):
        source_column = [source_info] * df.height
    elif isinstance(source_info, Collection):
        if len(source_info) != df.height:
            raise ValueError(
                "Length of source_info collection must match number of rows in the table."
            )
        source_column = source_info

    # identify columns for which source columns should be created

    for col in df.columns:
        if col.startswith(tuple(exclude_prefixes)) or col in exclude_columns:
            continue
        source_column = pl.Series(
            f"{constants.SOURCE_PREFIX}{col}",
            [f"{source_val}::{col}" for source_val in source_column],
            dtype=pl.String(),
        )
        df = df.with_columns(source_column)

    return df
