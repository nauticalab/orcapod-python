"""ConnectorArrowDatabase — generic ArrowDatabaseProtocol backed by any DBConnectorProtocol.

Implements the full ArrowDatabaseProtocol on top of any DBConnectorProtocol,
owning all record-management logic: record_path → table name mapping,
``__record_id`` column convention, in-memory pending-batch management,
deduplication, upsert, and flush.

Connector implementations (SQLiteConnector, PostgreSQLConnector, SpiralDBConnector)
need only satisfy DBConnectorProtocol; they do not implement ArrowDatabaseProtocol.

Example::

    connector = SQLiteConnector(":memory:")   # PLT-1076
    db = ConnectorArrowDatabase(connector)
    db.add_record(("results", "my_fn"), record_id="abc", record=table)
    db.flush()
"""
from __future__ import annotations

import re
from collections import defaultdict
from collections.abc import Collection, Mapping
from typing import TYPE_CHECKING, Any, cast

from orcapod.protocols.db_connector_protocol import ColumnInfo, DBConnectorProtocol
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.compute as pc
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")


def _arrow_schema_to_column_infos(schema: pa.Schema) -> list[ColumnInfo]:
    """Convert a PyArrow schema to a list of ColumnInfo."""
    return [
        ColumnInfo(name=field.name, arrow_type=field.type, nullable=field.nullable)
        for field in schema
    ]


class ConnectorArrowDatabase:
    """Generic ``ArrowDatabaseProtocol`` implementation backed by a ``DBConnectorProtocol``.

    Records are buffered in memory (pending batch) and written to the connector
    on ``flush()``. The ``record_path`` tuple is mapped to a sanitized SQL table
    name using ``"__".join(sanitized_parts)``.

    Args:
        connector: A ``DBConnectorProtocol`` implementation providing the
            underlying DB access (connection, type mapping, queries, writes).
        max_hierarchy_depth: Maximum allowed length for ``record_path`` tuples.
            Defaults to 10, matching ``InMemoryArrowDatabase``.
    """

    RECORD_ID_COLUMN = "__record_id"
    _ROW_INDEX_COLUMN = "__row_index"

    def __init__(
        self,
        connector: DBConnectorProtocol,
        max_hierarchy_depth: int = 10,
    ) -> None:
        self._connector = connector
        self.max_hierarchy_depth = max_hierarchy_depth
        self._pending_batches: dict[str, pa.Table] = {}
        self._pending_record_ids: dict[str, set[str]] = defaultdict(set)
        # Per-batch flag: True when the batch was added with skip_duplicates=True,
        # so flush() can pass skip_existing=True to the connector and let it use
        # native INSERT-OR-IGNORE semantics rather than Python-side prefiltering.
        self._pending_skip_existing: dict[str, bool] = {}

    # ── Path helpers ──────────────────────────────────────────────────────────

    def _get_record_key(self, record_path: tuple[str, ...]) -> str:
        return "/".join(record_path)

    def _path_to_table_name(self, record_path: tuple[str, ...]) -> str:
        """Map a record_path to a safe SQL table name.

        Each component is sanitized (non-alphanumeric chars → ``_``), then
        joined with ``__`` as separator. A ``t_`` prefix is added if the result
        starts with a digit to ensure a valid SQL identifier.
        """
        parts = [re.sub(r"[^a-zA-Z0-9_]", "_", part) for part in record_path]
        name = "__".join(parts)
        if name and name[0].isdigit():
            name = "t_" + name
        return name

    def _validate_record_path(self, record_path: tuple[str, ...]) -> None:
        if not record_path:
            raise ValueError("record_path cannot be empty")
        if len(record_path) > self.max_hierarchy_depth:
            raise ValueError(
                f"record_path depth {len(record_path)} exceeds maximum "
                f"{self.max_hierarchy_depth}"
            )
        for i, component in enumerate(record_path):
            if not component or not isinstance(component, str):
                raise ValueError(
                    f"record_path component {i} is invalid: {repr(component)}"
                )
            # "/" is the separator used by _get_record_key; "\0" is a common
            # string-boundary sentinel. Both would corrupt key round-tripping
            # in flush() where record_path is reconstructed via split("/").
            if "/" in component or "\0" in component:
                raise ValueError(
                    f"record_path component {repr(component)} contains an "
                    "invalid character ('/' or '\\0')"
                )

    # ── Record-ID column helpers ──────────────────────────────────────────────

    def _ensure_record_id_column(
        self, arrow_data: pa.Table, record_id: str
    ) -> pa.Table:
        if self.RECORD_ID_COLUMN not in arrow_data.column_names:
            key_array = pa.array(
                [record_id] * len(arrow_data), type=pa.large_string()
            )
            arrow_data = arrow_data.add_column(0, self.RECORD_ID_COLUMN, key_array)
        return arrow_data

    def _remove_record_id_column(self, arrow_data: pa.Table) -> pa.Table:
        if self.RECORD_ID_COLUMN in arrow_data.column_names:
            arrow_data = arrow_data.drop([self.RECORD_ID_COLUMN])
        return arrow_data

    def _handle_record_id_column(
        self, arrow_data: pa.Table, record_id_column: str | None
    ) -> pa.Table:
        if not record_id_column:
            return self._remove_record_id_column(arrow_data)
        if self.RECORD_ID_COLUMN in arrow_data.column_names:
            new_names = [
                record_id_column if name == self.RECORD_ID_COLUMN else name
                for name in arrow_data.schema.names
            ]
            return arrow_data.rename_columns(new_names)
        raise ValueError(
            f"Record ID column '{self.RECORD_ID_COLUMN}' not found in table."
        )

    # ── Deduplication ─────────────────────────────────────────────────────────

    def _deduplicate_within_table(self, table: pa.Table) -> pa.Table:
        """Keep the last occurrence of each record ID within a single table."""
        if table.num_rows <= 1:
            return table
        indices = pa.array(range(table.num_rows))
        table_with_idx = table.add_column(0, self._ROW_INDEX_COLUMN, indices)
        grouped = table_with_idx.group_by([self.RECORD_ID_COLUMN]).aggregate(
            [(self._ROW_INDEX_COLUMN, "max")]
        )
        max_indices = grouped[f"{self._ROW_INDEX_COLUMN}_max"].to_pylist()
        mask = pc.is_in(indices, pa.array(max_indices))
        return table.filter(mask)

    # ── Committed data access ─────────────────────────────────────────────────

    def _get_committed_table(
        self, record_path: tuple[str, ...]
    ) -> pa.Table | None:
        """Fetch all committed records for a path from the connector."""
        table_name = self._path_to_table_name(record_path)
        if table_name not in self._connector.get_table_names():
            return None
        batches = list(
            self._connector.iter_batches(f'SELECT * FROM "{table_name}"')
        )
        if not batches:
            return None
        return pa.Table.from_batches(batches)

    # ── Write methods ─────────────────────────────────────────────────────────

    def add_record(
        self,
        record_path: tuple[str, ...],
        record_id: str,
        record: pa.Table,
        skip_duplicates: bool = False,
        flush: bool = False,
    ) -> None:
        """Add a single record identified by ``record_id``."""
        data_with_id = self._ensure_record_id_column(record, record_id)
        self.add_records(
            record_path=record_path,
            records=data_with_id,
            record_id_column=self.RECORD_ID_COLUMN,
            skip_duplicates=skip_duplicates,
            flush=flush,
        )

    def add_records(
        self,
        record_path: tuple[str, ...],
        records: pa.Table,
        record_id_column: str | None = None,
        skip_duplicates: bool = False,
        flush: bool = False,
    ) -> None:
        """Add multiple records to the pending batch."""
        self._validate_record_path(record_path)
        if records.num_rows == 0:
            return

        if record_id_column is None:
            record_id_column = records.column_names[0]
        if record_id_column not in records.column_names:
            raise ValueError(
                f"record_id_column '{record_id_column}' not found in table columns: "
                f"{records.column_names}"
            )

        # Normalise to internal column name
        if record_id_column != self.RECORD_ID_COLUMN:
            rename_map = {record_id_column: self.RECORD_ID_COLUMN}
            records = records.rename_columns(
                [rename_map.get(c, c) for c in records.column_names]
            )

        records = self._deduplicate_within_table(records)
        record_key = self._get_record_key(record_path)
        input_ids = set(cast(list[str], records[self.RECORD_ID_COLUMN].to_pylist()))

        if skip_duplicates:
            # Only filter records that conflict with the in-flight pending batch.
            # Committed duplicates are handled at flush time via
            # upsert_records(skip_existing=True), which lets the connector use
            # native INSERT-OR-IGNORE semantics — no full-table read needed here.
            pending_conflicts = input_ids & self._pending_record_ids[record_key]
            if pending_conflicts:
                mask = pc.invert(
                    pc.is_in(
                        records[self.RECORD_ID_COLUMN],
                        pa.array(list(pending_conflicts)),
                    )
                )
                records = records.filter(mask)
            if records.num_rows == 0:
                return
            # Mark this pending slot so flush() uses skip_existing=True.
            self._pending_skip_existing[record_key] = True
        else:
            conflicts = input_ids & self._pending_record_ids[record_key]
            if conflicts:
                raise ValueError(
                    f"Records with IDs {conflicts} already exist in the pending batch. "
                    "Use skip_duplicates=True to skip them."
                )

        # Buffer in pending batch
        existing_pending = self._pending_batches.get(record_key)
        if existing_pending is None:
            self._pending_batches[record_key] = records
        else:
            self._pending_batches[record_key] = pa.concat_tables(
                [existing_pending, records]
            )
        self._pending_record_ids[record_key].update(
            cast(list[str], records[self.RECORD_ID_COLUMN].to_pylist())
        )

        if flush:
            self.flush()

    # ── base_path / at ────────────────────────────────────────────────────────

    @property
    def base_path(self) -> tuple[str, ...]:
        return ()

    def at(self, *path_components: str) -> "ConnectorArrowDatabase":
        return ConnectorArrowDatabase(
            connector=self._connector,
            max_hierarchy_depth=self.max_hierarchy_depth,
        )

    # ── Flush ─────────────────────────────────────────────────────────────────

    def flush(self) -> None:
        """Commit all pending batches to the connector via upsert."""
        for record_key in list(self._pending_batches.keys()):
            record_path = tuple(record_key.split("/"))
            table_name = self._path_to_table_name(record_path)
            pending = self._pending_batches.pop(record_key)
            self._pending_record_ids.pop(record_key, None)
            skip_existing = self._pending_skip_existing.pop(record_key, False)

            columns = _arrow_schema_to_column_infos(pending.schema)

            # Schema validation: if the table already exists, confirm the column
            # names and Arrow types match before writing.  Schema evolution is
            # intentionally out of scope; a clear ValueError is preferable to a
            # cryptic DB-level error or a silent partial write.
            existing_table_names = self._connector.get_table_names()
            if table_name in existing_table_names:
                existing_cols = {
                    c.name: c.arrow_type
                    for c in self._connector.get_column_info(table_name)
                }
                pending_cols = {c.name: c.arrow_type for c in columns}
                if existing_cols != pending_cols:
                    raise ValueError(
                        f"Schema mismatch for table {table_name!r}: "
                        f"existing columns {sorted(existing_cols)} differ from "
                        f"pending columns {sorted(pending_cols)}. "
                        "Schema evolution is not supported."
                    )

            self._connector.create_table_if_not_exists(
                table_name, columns, pk_column=self.RECORD_ID_COLUMN
            )
            self._connector.upsert_records(
                table_name,
                pending,
                id_column=self.RECORD_ID_COLUMN,
                skip_existing=skip_existing,
            )

    # ── Read methods ──────────────────────────────────────────────────────────

    def get_record_by_id(
        self,
        record_path: tuple[str, ...],
        record_id: str,
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> pa.Table | None:
        if flush:
            self.flush()
        record_key = self._get_record_key(record_path)

        # Check pending first
        if record_id in self._pending_record_ids.get(record_key, set()):
            pending = self._pending_batches[record_key]
            filtered = pending.filter(pc.field(self.RECORD_ID_COLUMN) == record_id)
            if filtered.num_rows > 0:
                return self._handle_record_id_column(filtered, record_id_column)

        # Check committed
        committed = self._get_committed_table(record_path)
        if committed is None:
            return None
        filtered = committed.filter(pc.field(self.RECORD_ID_COLUMN) == record_id)
        if filtered.num_rows == 0:
            return None
        return self._handle_record_id_column(filtered, record_id_column)

    def get_all_records(
        self,
        record_path: tuple[str, ...],
        record_id_column: str | None = None,
    ) -> pa.Table | None:
        record_key = self._get_record_key(record_path)
        parts: list[pa.Table] = []

        committed = self._get_committed_table(record_path)
        if committed is not None and committed.num_rows > 0:
            parts.append(committed)
        pending = self._pending_batches.get(record_key)
        if pending is not None and pending.num_rows > 0:
            parts.append(pending)

        if not parts:
            return None
        table = parts[0] if len(parts) == 1 else pa.concat_tables(parts)
        return self._handle_record_id_column(table, record_id_column)

    def get_records_by_ids(
        self,
        record_path: tuple[str, ...],
        record_ids: Collection[str],
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> pa.Table | None:
        if flush:
            self.flush()
        ids_list = list(record_ids)
        if not ids_list:
            return None
        all_records = self.get_all_records(
            record_path, record_id_column=self.RECORD_ID_COLUMN
        )
        if all_records is None:
            return None
        filtered = all_records.filter(
            pc.is_in(all_records[self.RECORD_ID_COLUMN], pa.array(ids_list))
        )
        if filtered.num_rows == 0:
            return None
        return self._handle_record_id_column(filtered, record_id_column)

    def get_records_with_column_value(
        self,
        record_path: tuple[str, ...],
        column_values: Collection[tuple[str, Any]] | Mapping[str, Any],
        record_id_column: str | None = None,
        flush: bool = False,
    ) -> pa.Table | None:
        if flush:
            self.flush()
        all_records = self.get_all_records(
            record_path, record_id_column=self.RECORD_ID_COLUMN
        )
        if all_records is None:
            return None

        if isinstance(column_values, Mapping):
            pairs = list(column_values.items())
        else:
            pairs = cast(list[tuple[str, Any]], list(column_values))

        expr = None
        for col, val in pairs:
            e = pc.field(col) == val
            expr = e if expr is None else expr & e  # type: ignore[assignment]

        filtered = all_records.filter(expr)
        if filtered.num_rows == 0:
            return None
        return self._handle_record_id_column(filtered, record_id_column)

    # ── Config ────────────────────────────────────────────────────────────────

    def to_config(self) -> dict[str, Any]:
        """Serialize configuration to a JSON-compatible dict."""
        return {
            "type": "connector_arrow_database",
            "connector": self._connector.to_config(),
            "max_hierarchy_depth": self.max_hierarchy_depth,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> ConnectorArrowDatabase:
        """Reconstruct a ConnectorArrowDatabase from config.

        Raises:
            NotImplementedError: Always — requires a connector registry that
                maps ``connector_type`` keys to ``from_config`` classmethods.
                Implement alongside connector classes in PLT-1074/1075/1076.
        """
        raise NotImplementedError(
            "ConnectorArrowDatabase.from_config requires a registered connector "
            "factory (connector_type → class). Implement in PLT-1074/1075/1076."
        )
