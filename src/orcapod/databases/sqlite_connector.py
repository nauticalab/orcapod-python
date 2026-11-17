"""SQLiteConnector — DBConnectorProtocol implementation backed by stdlib sqlite3.

Zero extra dependencies (sqlite3 is stdlib). Intended for local development,
CI integration tests, and pipeline prototyping. Not suitable for use on network
filesystems (NFS, SMB/CIFS) due to unreliable file locking.

Example::

    connector = SQLiteConnector(":memory:")
    db = ConnectorArrowDatabase(connector)
    db.add_record(("results", "my_fn"), record_id="abc", record=table)
    db.flush()
"""

from __future__ import annotations

import logging
import os
import sqlite3
import threading
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from orcapod.types import ColumnInfo

if TYPE_CHECKING:
    import pyarrow as pa
else:
    from orcapod.utils.lazy_module import LazyModule

    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module-level helpers (pure functions, no I/O)
# ---------------------------------------------------------------------------


def _sqlite_type_to_arrow(declared_type: str) -> pa.DataType:
    """Map a SQLite declared column type to an Arrow DataType.

    Follows SQLite's type affinity rules with an explicit BOOLEAN
    special-case to preserve round-trip fidelity.

    Args:
        declared_type: The declared column type string from PRAGMA table_info.

    Returns:
        The corresponding Arrow DataType.
    """
    t = declared_type.upper().strip()
    if t == "BOOLEAN":
        return pa.bool_()
    if "INT" in t:
        return pa.int64()
    if any(k in t for k in ("CHAR", "CLOB", "TEXT")):
        return pa.large_string()
    if "BLOB" in t or t == "":
        return pa.large_binary()
    if any(k in t for k in ("REAL", "FLOA", "DOUB")):
        return pa.float64()
    # NUMERIC affinity (and anything else)
    return pa.float64()


def _sqlite_sql_type(arrow_type: pa.DataType) -> str | None:
    """Return the SQLite column type for an Arrow type, or ``None`` when there is none.

    Args:
        arrow_type: The Arrow DataType to convert.

    Returns:
        A SQLite SQL type string (e.g. "INTEGER", "TEXT"), or ``None`` for a type
        SQLite has no column type for (lists, structs, dates, timestamps, extension
        types and the rest).
    """
    import pyarrow as _pa  # noqa: PLC0415 — needed at call time

    from orcapod.utils.arrow_utils import normalize_to_large_types  # noqa: PLC0415

    # View layouts map as their large variants, as the storability rule reads them.
    arrow_type = normalize_to_large_types(arrow_type, keep_null=True)
    if arrow_type == _pa.bool_():
        return "BOOLEAN"
    if _pa.types.is_integer(arrow_type):
        return "INTEGER"
    if _pa.types.is_floating(arrow_type):
        return "REAL"
    if _pa.types.is_string(arrow_type) or _pa.types.is_large_string(arrow_type):
        return "TEXT"
    if _pa.types.is_binary(arrow_type) or _pa.types.is_large_binary(arrow_type):
        return "BLOB"
    return None


def _arrow_type_to_sqlite_sql(arrow_type: pa.DataType) -> str:
    """Map an Arrow DataType to a SQLite SQL type string for CREATE TABLE.

    Args:
        arrow_type: The Arrow DataType to convert.

    Returns:
        A SQLite SQL type string (e.g. "INTEGER", "TEXT").

    Raises:
        ValueError: If SQLite has no column type for ``arrow_type``. There is no
            fallback to TEXT: a value of such a type would fail to bind or read
            back as text (ITL-1162).
    """
    sql_type = _sqlite_sql_type(arrow_type)
    if sql_type is None:
        raise ValueError(f"SQLite has no column type for Arrow type {arrow_type}")
    return sql_type


def _sqlite_unstorable_reason(arrow_type: pa.DataType) -> str | None:
    """Say why SQLite cannot store a column of ``arrow_type``, or ``None`` when it can.

    A type is stored when the column type it is written as reads back as the same
    type in its large variant: ``bool``, ``int64``, ``double``, ``large_string``
    and ``large_binary`` (and ``string`` / ``binary``, read back large).

    Args:
        arrow_type: The column's Arrow type.

    Returns:
        A short reason, or ``None`` when SQLite stores the type.
    """
    from orcapod.utils.arrow_utils import normalize_to_large_types  # noqa: PLC0415

    arrow_type = normalize_to_large_types(arrow_type, keep_null=True)
    sql_type = _sqlite_sql_type(arrow_type)
    if sql_type is None:
        return "SQLite has no column type for it"
    read_back = _sqlite_type_to_arrow(sql_type)
    if read_back != arrow_type:
        return f"SQLite stores it as {sql_type}, which reads back as {read_back}"
    return None


def _coerce_column(values: list[Any], arrow_type: pa.DataType) -> list[Any]:
    """Coerce raw SQLite Python values to match the target Arrow type.

    SQLite returns BOOLEAN column values as Python int (1/0). PyArrow
    raises ArrowInvalid when constructing pa.bool_() arrays from int values,
    so this helper converts them first.

    Args:
        values: Raw Python values from sqlite3 cursor rows.
        arrow_type: The target Arrow type for this column.

    Returns:
        The same list if no coercion is needed, otherwise a new list
        with coerced values.
    """
    import pyarrow as _pa  # noqa: PLC0415

    if arrow_type == _pa.bool_():
        return [bool(v) if v is not None else None for v in values]
    return values


def _field_has_metadata(field: pa.Field) -> bool:
    """Return True if ``field`` or any nested child carries Arrow metadata.

    Recursively walks struct and list type trees so that metadata on child
    fields is detected and not silently dropped.

    Args:
        field: The Arrow field to inspect.

    Returns:
        True if the field or any descendant carries field metadata or is an
        Arrow extension type; False otherwise.
    """
    import pyarrow as _pa  # noqa: PLC0415

    if isinstance(field.type, _pa.ExtensionType) or field.metadata:
        return True
    if _pa.types.is_struct(field.type):
        return any(
            _field_has_metadata(field.type.field(i))
            for i in range(field.type.num_fields)
        )
    if (
        _pa.types.is_list(field.type)
        or _pa.types.is_large_list(field.type)
        or _pa.types.is_fixed_size_list(field.type)
    ):
        return _field_has_metadata(field.type.value_field)
    return False


# ---------------------------------------------------------------------------
# SQLiteConnector
# ---------------------------------------------------------------------------


class SQLiteConnector:
    """DBConnectorProtocol implementation backed by stdlib sqlite3.

    Holds a single sqlite3.Connection opened at construction time.
    Thread-safe via an internal threading.RLock. Not suitable for use
    on network filesystems (NFS, SMB/CIFS) due to unreliable file locking.

    Args:
        db_path: Path to the SQLite database file, or ":memory:" for an
            in-process in-memory database. Defaults to ":memory:".
    """

    def __init__(self, db_path: str | os.PathLike = ":memory:") -> None:
        self._db_path = db_path
        self._conn: sqlite3.Connection | None = sqlite3.connect(
            str(db_path), check_same_thread=False, isolation_level=None
        )
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _require_open(self) -> sqlite3.Connection:
        """Return the open connection or raise RuntimeError if closed."""
        if self._conn is None:
            raise RuntimeError("SQLiteConnector is closed")
        return self._conn

    @staticmethod
    def _validate_table_name(table_name: str) -> None:
        """Validate that a table name is safe for use in a double-quoted SQL identifier.

        Args:
            table_name: The table name to validate.

        Raises:
            ValueError: If the table name contains a double-quote character.
        """
        if '"' in table_name:
            raise ValueError(
                f"Table name {table_name!r} contains an invalid double-quote character."
            )

    # ── Schema introspection ──────────────────────────────────────────────────

    def get_table_names(self) -> list[str]:
        """Return all user table names in this database (excludes views and SQLite internals).

        Returns:
            Sorted list of table name strings.
        """
        with self._lock:
            conn = self._require_open()
            cursor = conn.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type='table' AND name NOT LIKE 'sqlite_%' "
                "ORDER BY name"
            )
            return [row["name"] for row in cursor]

    def get_pk_columns(self, table_name: str) -> list[str]:
        """Return primary-key column names in key-sequence order.

        Args:
            table_name: Name of the table to introspect.

        Returns:
            List of PK column names, empty if no primary key or table doesn't exist.
        """
        with self._lock:
            conn = self._require_open()
            self._validate_table_name(table_name)
            cursor = conn.execute(f'PRAGMA table_info("{table_name}")')
            rows = [row for row in cursor if row["pk"] > 0]
            return [row["name"] for row in sorted(rows, key=lambda r: r["pk"])]

    def get_column_info(self, table_name: str) -> list[ColumnInfo]:
        """Return column metadata with Arrow-mapped types.

        Args:
            table_name: Name of the table to introspect.

        Returns:
            List of ColumnInfo objects; empty list if table doesn't exist.
        """
        with self._lock:
            conn = self._require_open()
            self._validate_table_name(table_name)
            cursor = conn.execute(f'PRAGMA table_info("{table_name}")')
            return [
                ColumnInfo(
                    name=row["name"],
                    arrow_type=_sqlite_type_to_arrow(row["type"]),
                    nullable=not bool(row["notnull"]),
                )
                for row in cursor
            ]

    # ── Read ──────────────────────────────────────────────────────────────────

    def iter_batches(
        self,
        query: str,
        params: Any = None,
        batch_size: int = 1000,
    ) -> Iterator[pa.RecordBatch]:
        """Execute a query and yield results as Arrow RecordBatches.

        Collects all batches inside the lock to avoid holding the lock across
        yield points (which would deadlock any concurrent caller).

        Column types are resolved from the table referenced in the FROM clause
        of the query (double-quoted or unquoted identifier). For computed or
        otherwise unknown columns the fallback is ``pa.large_string()``.

        Args:
            query: SQL query string. Table names should be double-quoted.
            params: Optional query parameters.
            batch_size: Maximum rows per yielded batch.

        Yields:
            Arrow RecordBatch objects.
        """
        import re as _re

        import pyarrow as _pa

        # Execute the query and resolve the schema under the lock, then fetch
        # the first batch while still holding it.  Subsequent batches re-acquire
        # the lock only for the duration of each fetchmany call so that other
        # connector operations are not blocked during Arrow construction or yield.
        with self._lock:
            conn = self._require_open()
            cursor = conn.execute(query, [] if params is None else params)
            if cursor.description is None:
                return
            col_names = [d[0] for d in cursor.description]

            # Resolve column types from the specific table named in the FROM
            # clause.  Using only the queried table avoids cross-table
            # column-name collisions that would arise from scanning all tables.
            # Fallback to large_string() for computed/aliased/unknown columns.
            type_lookup: dict[str, _pa.DataType] = {}
            table_match = _re.search(r'FROM\s+"([^"]+)"', query, _re.IGNORECASE)
            if not table_match:
                table_match = _re.search(r"FROM\s+(\w+)", query, _re.IGNORECASE)
            if table_match:
                queried_table = table_match.group(1)
                for ci in self.get_column_info(queried_table):
                    type_lookup[ci.name] = ci.arrow_type

            # rowid is an implicit SQLite integer column; it does not appear in
            # PRAGMA table_info, so it would otherwise fall back to large_string.
            if "rowid" in col_names and "rowid" not in type_lookup:
                type_lookup["rowid"] = _pa.int64()

            arrow_types = [
                type_lookup.get(name, _pa.large_string()) for name in col_names
            ]
            schema = _pa.schema(
                [_pa.field(name, atype) for name, atype in zip(col_names, arrow_types)]
            )

            chunk = cursor.fetchmany(batch_size)

        # Build and yield batches outside the lock; re-acquire briefly per fetchmany.
        while chunk:
            arrays = [
                _pa.array(
                    _coerce_column([row[i] for row in chunk], arrow_types[i]),
                    type=arrow_types[i],
                )
                for i in range(len(col_names))
            ]
            yield _pa.RecordBatch.from_arrays(arrays, schema=schema)
            with self._lock:
                self._require_open()  # guard: raise RuntimeError if closed mid-iteration
                chunk = cursor.fetchmany(batch_size)

    # ── Write ─────────────────────────────────────────────────────────────────

    def create_table_if_not_exists(
        self,
        table_name: str,
        columns: list[ColumnInfo],
        pk_column: str,
    ) -> None:
        """Create a table with the given columns if it does not already exist.

        Args:
            table_name: Table to create.
            columns: Column definitions with Arrow-mapped types.
            pk_column: Name of the column to use as the primary key.
        """
        col_names = [col.name for col in columns]
        if pk_column not in col_names:
            raise ValueError(
                f"pk_column {pk_column!r} not found in columns: {col_names}"
            )
        with self._lock:
            conn = self._require_open()
            self._validate_table_name(table_name)
            col_defs = []
            for col in columns:
                sql_type = _arrow_type_to_sqlite_sql(col.arrow_type)
                not_null = " NOT NULL" if not col.nullable else ""
                pk = " PRIMARY KEY" if col.name == pk_column else ""
                escaped = col.name.replace('"', '""')
                col_defs.append(f'    "{escaped}" {sql_type}{not_null}{pk}')
            ddl = f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n'
            ddl += ",\n".join(col_defs)
            ddl += "\n)"
            conn.execute(ddl)

    def upsert_records(
        self,
        table_name: str,
        records: pa.Table,
        id_column: str,
        skip_existing: bool = False,
    ) -> None:
        """Write records to a table using upsert semantics.

        Args:
            table_name: Target table (must already exist).
            records: Arrow table of records to write.
            id_column: Column used as the unique row identifier. Conflict
                detection is handled by the SQL PRIMARY KEY constraint;
                this argument is accepted for protocol compliance.
            skip_existing: If True, skip rows whose id already exists
                (INSERT OR IGNORE). If False, overwrite (INSERT OR REPLACE).
        """
        with self._lock:
            conn = self._require_open()
            self._validate_table_name(table_name)
            cols = list(records.column_names)
            col_list = ", ".join('"' + c.replace('"', '""') + '"' for c in cols)
            placeholders = ", ".join("?" for _ in cols)
            verb = "INSERT OR IGNORE" if skip_existing else "INSERT OR REPLACE"
            sql = f'{verb} INTO "{table_name}" ({col_list}) VALUES ({placeholders})'
            rows = (tuple(row[c] for c in cols) for row in records.to_pylist())
            conn.executemany(sql, rows)

    def unstorable_column_reason(
        self, name: str, arrow_type: pa.DataType, *, primary_key: bool
    ) -> str | None:
        """Say why SQLite cannot store a column, or ``None`` when it can (ITL-1162).

        SQLite stores a column only when it reads it back as written:
        ``bool``, ``int64``, ``double``, ``large_string`` and ``large_binary``
        (``string`` and ``binary`` read back in their large variants). Narrower
        integers and floats read back widened, and lists, structs, dates,
        timestamps and extension types have no SQLite column type. The name and
        the key role do not matter to SQLite.

        Args:
            name: The column's name.
            arrow_type: The column's Arrow type.
            primary_key: Whether the column is the table's primary key.

        Returns:
            A short reason, or ``None`` when SQLite stores the column.
        """
        return _sqlite_unstorable_reason(arrow_type)

    def unstorable_table_name_reason(self, table_name: str) -> str | None:
        """Say why SQLite cannot hold a table of this name, or ``None`` (ITL-1162).

        SQLite has no identifier length limit, but reserves table names that
        start with ``sqlite_`` (in any case) for itself.

        Args:
            table_name: The table name.

        Returns:
            A short reason, or ``None`` when SQLite holds the name.
        """
        if table_name.lower().startswith("sqlite_"):
            return "SQLite reserves table names that start with 'sqlite_'"
        return None

    def validate_records(self, records: pa.Table) -> None:
        """Reject tables carrying any Arrow field or schema metadata.

        ``SQLiteConnector`` does not preserve Arrow field or schema metadata
        across read/write cycles — any metadata present would be silently lost.
        This includes Arrow extension types, which encode their identity in field
        metadata and would be demoted to their plain storage type on read.
        Nested field metadata (inside struct or list columns) is also detected.

        Args:
            records: Arrow table to validate.

        Raises:
            ValueError: If any field (or any nested child field) carries
                metadata, or if the schema itself carries metadata.
        """
        problem_fields = [f.name for f in records.schema if _field_has_metadata(f)]
        has_schema_meta = bool(records.schema.metadata)
        if problem_fields or has_schema_meta:
            parts: list[str] = []
            if problem_fields:
                fields_str = ", ".join(repr(n) for n in problem_fields)
                parts.append(f"fields with metadata: {fields_str}")
            if has_schema_meta:
                parts.append("schema-level metadata")
            raise ValueError(
                f"SQLiteConnector does not preserve Arrow metadata "
                f"({', '.join(parts)})."
            )

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def close(self) -> None:
        """Close the database connection. Idempotent."""
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def __enter__(self) -> SQLiteConnector:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    # ── Serialization ─────────────────────────────────────────────────────────

    def to_config(self) -> dict[str, Any]:
        """Serialize connection configuration to a JSON-compatible dict.

        Returns:
            Dict with ``connector_type`` and ``db_path`` keys.
        """
        return {
            "connector_type": "sqlite",
            "db_path": str(self._db_path),
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> SQLiteConnector:
        """Reconstruct a SQLiteConnector from a config dict.

        Args:
            config: Dict with ``connector_type`` and ``db_path`` keys.

        Returns:
            A new SQLiteConnector instance.

        Raises:
            ValueError: If ``connector_type`` is not ``"sqlite"``.
        """
        if config.get("connector_type") != "sqlite":
            raise ValueError(
                f"Expected connector_type 'sqlite', got {config.get('connector_type')!r}"
            )
        return cls(db_path=config["db_path"])
