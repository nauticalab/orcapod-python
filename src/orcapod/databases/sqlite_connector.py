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


def _arrow_type_to_sqlite_sql(arrow_type: pa.DataType) -> str:
    """Map an Arrow DataType to a SQLite SQL type string for CREATE TABLE.

    Args:
        arrow_type: The Arrow DataType to convert.

    Returns:
        A SQLite SQL type string (e.g. "INTEGER", "TEXT").
    """
    import pyarrow as _pa  # noqa: PLC0415 — needed at call time

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
    logger.warning("Unsupported Arrow type %r; mapping to TEXT", arrow_type)
    return "TEXT"


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

    # ── Schema introspection ──────────────────────────────────────────────────

    def get_table_names(self) -> list[str]:
        """Return all table names in this database (excludes views).

        Returns:
            Sorted list of table name strings.
        """
        raise NotImplementedError

    def get_pk_columns(self, table_name: str) -> list[str]:
        """Return primary-key column names in key-sequence order.

        Args:
            table_name: Name of the table to introspect.

        Returns:
            List of PK column names, empty if no primary key.
        """
        raise NotImplementedError

    def get_column_info(self, table_name: str) -> list[ColumnInfo]:
        """Return column metadata with Arrow-mapped types.

        Args:
            table_name: Name of the table to introspect.

        Returns:
            List of ColumnInfo objects; empty list if table doesn't exist.
        """
        raise NotImplementedError

    # ── Read ──────────────────────────────────────────────────────────────────

    def iter_batches(
        self,
        query: str,
        params: Any = None,
        batch_size: int = 1000,
    ) -> Iterator[pa.RecordBatch]:
        """Execute a query and yield results as Arrow RecordBatches.

        Args:
            query: SQL query string. Table names should be double-quoted.
            params: Optional query parameters.
            batch_size: Maximum rows per yielded batch.

        Yields:
            Arrow RecordBatch objects.
        """
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError

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
        raise NotImplementedError
