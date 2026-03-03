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


def add_system_tag_columns(
    table: pa.Table,
    schema_hash: str,
    source_ids: str | Collection[str],
    record_ids: Collection[str],
) -> pa.Table:
    """Add paired source_id and record_id system tag columns to an Arrow table."""
    if not table.column_names:
        raise ValueError("Table is empty")

    # Normalize source_ids
    if isinstance(source_ids, str):
        source_ids = [source_ids] * table.num_rows
    else:
        source_ids = list(source_ids)
        if len(source_ids) != table.num_rows:
            raise ValueError(
                "Length of source_ids must match number of rows in the table."
            )

    record_ids = list(record_ids)
    if len(record_ids) != table.num_rows:
        raise ValueError("Length of record_ids must match number of rows in the table.")

    source_id_col_name = f"{constants.SYSTEM_TAG_SOURCE_ID_PREFIX}{constants.BLOCK_SEPARATOR}{schema_hash}"
    record_id_col_name = f"{constants.SYSTEM_TAG_RECORD_ID_PREFIX}{constants.BLOCK_SEPARATOR}{schema_hash}"

    source_id_array = pa.array(source_ids, type=pa.large_string())
    record_id_array = pa.array(record_ids, type=pa.large_string())

    table = table.append_column(source_id_col_name, source_id_array)
    table = table.append_column(record_id_col_name, record_id_array)
    return table


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


def _parse_system_tag_column(
    col_name: str,
) -> tuple[str, str, str] | None:
    """Parse a system tag column name into (field_type, provenance_path, position).

    For example:
        _tag_source_id::abc123::def456:0
        → field_type="source_id", provenance_path="abc123::def456", position="0"

        _tag_record_id::abc123::def456:0
        → field_type="record_id", provenance_path="abc123::def456", position="0"

    Returns None if the column doesn't end with a :position suffix.
    """
    # Strip the trailing :position
    base, sep, position = col_name.rpartition(constants.FIELD_SEPARATOR)
    if not sep or not position.isdigit():
        return None

    # Determine field type by checking known prefixes
    prefix = constants.SYSTEM_TAG_PREFIX
    if not base.startswith(prefix):
        return None

    after_prefix = base[len(prefix) :]  # e.g. "source_id::abc123::def456"

    # Extract field_type and provenance_path
    # field_type is everything before the first BLOCK_SEPARATOR
    field_type, block_sep, provenance_path = after_prefix.partition(
        constants.BLOCK_SEPARATOR
    )
    if not block_sep:
        return None

    return field_type, provenance_path, position


def sort_system_tag_values(table: pa.Table) -> pa.Table:
    """Sort paired system tag values for columns that share the same provenance path.

    System tag columns come in (source_id, record_id) pairs. Columns that differ
    only by their canonical position (the final :N) represent streams with the same
    pipeline_hash that were joined. For commutativity, paired (source_id, record_id)
    tuples must be sorted together per row so that the result is independent of
    input order.

    Algorithm:
    1. Parse each system tag column into (field_type, provenance_path, position)
    2. Group by provenance_path — source_id and record_id at the same path+position
       are paired
    3. For each group with >1 position, sort per-row by (source_id, record_id) tuples
    4. Assign sorted values back to both columns at each position
    """
    sys_tag_cols = [
        c for c in table.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
    ]

    if not sys_tag_cols:
        return table

    # Parse all system tag columns and group by provenance_path
    # groups[provenance_path][position] = {field_type: col_name}
    groups: dict[str, dict[str, dict[str, str]]] = {}
    for col in sys_tag_cols:
        parsed = _parse_system_tag_column(col)
        if parsed is None:
            continue
        field_type, provenance_path, position = parsed
        groups.setdefault(provenance_path, {}).setdefault(position, {})[field_type] = (
            col
        )

    source_id_field = constants.SYSTEM_TAG_SOURCE_ID_PREFIX[
        len(constants.SYSTEM_TAG_PREFIX) :
    ]
    record_id_field = constants.SYSTEM_TAG_RECORD_ID_PREFIX[
        len(constants.SYSTEM_TAG_PREFIX) :
    ]

    # For each provenance_path group with >1 position, sort paired tuples per row
    for provenance_path, positions in groups.items():
        if len(positions) <= 1:
            continue

        # Sort positions numerically
        sorted_positions = sorted(positions.keys(), key=int)

        # Collect paired column names for each position
        paired_cols: list[tuple[str | None, str | None]] = []
        for pos in sorted_positions:
            field_map = positions[pos]
            sid_col = field_map.get(source_id_field)
            rid_col = field_map.get(record_id_field)
            paired_cols.append((sid_col, rid_col))

        # Get values for all paired columns
        sid_values = []
        rid_values = []
        for sid_col, rid_col in paired_cols:
            sid_values.append(
                table.column(sid_col).to_pylist()
                if sid_col
                else [None] * table.num_rows
            )
            rid_values.append(
                table.column(rid_col).to_pylist()
                if rid_col
                else [None] * table.num_rows
            )

        # Sort per row by (source_id, record_id) tuples
        n_positions = len(sorted_positions)
        sorted_sid: list[list] = [[] for _ in range(n_positions)]
        sorted_rid: list[list] = [[] for _ in range(n_positions)]

        for row_idx in range(table.num_rows):
            row_tuples = [
                (sid_values[pos_idx][row_idx], rid_values[pos_idx][row_idx])
                for pos_idx in range(n_positions)
            ]
            row_tuples.sort()
            for pos_idx, (sid_val, rid_val) in enumerate(row_tuples):
                sorted_sid[pos_idx].append(sid_val)
                sorted_rid[pos_idx].append(rid_val)

        # Replace columns with sorted values
        for pos_idx, (sid_col, rid_col) in enumerate(paired_cols):
            if sid_col:
                orig_type = table.column(sid_col).type
                tbl_idx = table.column_names.index(sid_col)
                table = table.drop(sid_col)
                table = table.add_column(
                    tbl_idx,
                    sid_col,
                    pa.array(sorted_sid[pos_idx], type=orig_type),
                )
            if rid_col:
                orig_type = table.column(rid_col).type
                tbl_idx = table.column_names.index(rid_col)
                table = table.drop(rid_col)
                table = table.add_column(
                    tbl_idx,
                    rid_col,
                    pa.array(sorted_rid[pos_idx], type=orig_type),
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
