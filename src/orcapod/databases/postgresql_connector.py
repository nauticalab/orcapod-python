"""PostgreSQLConnector — DBConnectorProtocol implementation backed by psycopg3.

Uses a named server-side cursor for iter_batches so PostgreSQL streams results
incrementally rather than buffering the full result set in memory.

Example::

    connector = PostgreSQLConnector("postgresql://user:pass@localhost:5432/mydb")
    db = ConnectorArrowDatabase(connector)
    db.add_record(("results", "my_fn"), record_id="abc", record=table)
    db.flush()
"""
from __future__ import annotations

import itertools
import logging
import re
import threading
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from orcapod.types import ColumnInfo
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import psycopg
    import pyarrow as pa
else:
    psycopg = LazyModule("psycopg")
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Module-level helpers (pure functions, no I/O)
# These are kept module-level so AsyncPostgreSQLConnector (future) can reuse
# them without subclassing.
# ---------------------------------------------------------------------------


def _pg_type_to_arrow(pg_type_name: str, udt_name: str) -> pa.DataType:
    """Map a PostgreSQL type name to an Arrow DataType.

    Args:
        pg_type_name: The ``data_type`` value from ``information_schema.columns``
            (e.g. ``"integer"``, ``"text"``, ``"ARRAY"``).
        udt_name: The ``udt_name`` value from ``information_schema.columns``
            (e.g. ``"int4"``, ``"text"``, ``"_int4"`` for int4 arrays).

    Returns:
        The corresponding Arrow DataType. Falls back to ``pa.large_string()``
        for unknown types (with a logged warning).
    """
    import pyarrow as _pa

    # Array type: udt_name starts with "_", element type follows
    if pg_type_name.upper() == "ARRAY":
        elem_udt = udt_name.lstrip("_")
        elem_type = _pg_type_to_arrow(elem_udt, elem_udt)
        return _pa.large_list(elem_type)

    t = udt_name.lower()

    if t == "bool":
        return _pa.bool_()
    if t in ("int2", "smallint"):
        return _pa.int32()
    if t in ("int4", "integer", "int"):
        return _pa.int32()
    if t in ("int8", "bigint"):
        return _pa.int64()
    if t in ("float4", "real"):
        return _pa.float32()
    if t in ("float8", "double precision", "numeric", "decimal"):
        return _pa.float64()
    if t in ("text", "varchar", "character varying", "char", "bpchar",
             "name", "uuid", "json", "jsonb", "time", "timetz"):
        if t in ("time", "timetz"):
            logger.warning("PostgreSQL type %r mapped to pa.large_string() (known gap)", t)
        if t == "uuid":
            # TODO: revisit mapping once PLT-1162 decides on a canonical UUID Arrow type
            pass
        return _pa.large_string()
    if t == "bytea":
        return _pa.large_binary()
    if t == "date":
        return _pa.date32()
    if t in ("timestamp", "timestamp without time zone"):
        return _pa.timestamp("us")
    if t in ("timestamptz", "timestamp with time zone"):
        return _pa.timestamp("us", tz="UTC")

    # Fallback: also handle pg_type_name-based matches for information_schema values
    p = pg_type_name.lower()
    if p == "timestamp without time zone":
        return _pa.timestamp("us")
    if p == "timestamp with time zone":
        return _pa.timestamp("us", tz="UTC")
    if p in ("character varying", "character"):
        return _pa.large_string()
    if p in ("time without time zone", "time with time zone"):
        logger.warning("PostgreSQL type %r mapped to pa.large_string() (known gap)", p)
        return _pa.large_string()

    logger.warning("Unknown PostgreSQL type %r (udt_name=%r); mapping to pa.large_string()", pg_type_name, udt_name)
    return _pa.large_string()


def _arrow_type_to_pg_sql(arrow_type: pa.DataType) -> str:
    """Map an Arrow DataType to a PostgreSQL SQL type string for CREATE TABLE.

    Args:
        arrow_type: The Arrow DataType to convert.

    Returns:
        A PostgreSQL SQL type string (e.g. ``"TEXT"``, ``"BIGINT"``).

    Raises:
        ValueError: If ``arrow_type`` is ``pa.large_list(...)`` — array columns
            are not supported in CREATE TABLE (they are read-only from existing tables).
    """
    import pyarrow as _pa

    if arrow_type == _pa.bool_():
        return "BOOLEAN"
    if _pa.types.is_integer(arrow_type):
        if arrow_type in (_pa.int8(), _pa.int16(), _pa.int32(),
                          _pa.uint8(), _pa.uint16()):
            return "INTEGER"
        return "BIGINT"
    if arrow_type == _pa.float32():
        return "REAL"
    if _pa.types.is_floating(arrow_type):
        return "DOUBLE PRECISION"
    if _pa.types.is_string(arrow_type) or _pa.types.is_large_string(arrow_type):
        return "TEXT"
    if _pa.types.is_binary(arrow_type) or _pa.types.is_large_binary(arrow_type):
        return "BYTEA"
    if arrow_type == _pa.date32():
        return "DATE"
    if _pa.types.is_timestamp(arrow_type):
        return "TIMESTAMPTZ" if arrow_type.tz is not None else "TIMESTAMP"
    if _pa.types.is_large_list(arrow_type) or _pa.types.is_list(arrow_type):
        raise ValueError(
            f"Arrow type {arrow_type!r} (list/array) is not supported for "
            "CREATE TABLE. Array columns can only be read from existing tables."
        )
    logger.warning("Unsupported Arrow type %r; mapping to TEXT", arrow_type)
    return "TEXT"


def _resolve_column_type_lookup(
    query: str,
    connector: "PostgreSQLConnector",
) -> dict[str, pa.DataType]:
    """Parse the FROM clause of query to find the source table, then return
    a column-name → Arrow-type dict from get_column_info.

    Returns an empty dict if no single unambiguous table can be identified,
    causing iter_batches to fall back to pa.large_string() for all columns.

    Args:
        query: SQL query string.
        connector: The connector to call get_column_info on.

    Returns:
        Dict mapping column name to Arrow DataType.
    """
    # Match FROM "table_name" or FROM table_name (case-insensitive)
    match = re.search(r'FROM\s+"([^"]+)"', query, re.IGNORECASE)
    if not match:
        match = re.search(r'FROM\s+(\w+)', query, re.IGNORECASE)
    if not match:
        return {}
    table_name = match.group(1)
    return {ci.name: ci.arrow_type for ci in connector.get_column_info(table_name)}


# ---------------------------------------------------------------------------
# PostgreSQLConnector
# ---------------------------------------------------------------------------


class PostgreSQLConnector:
    """DBConnectorProtocol implementation backed by psycopg3 (psycopg).

    Holds a single psycopg.Connection opened at construction time.
    Thread-safe via an internal threading.RLock.

    Uses named server-side cursors in iter_batches so PostgreSQL streams
    results row-by-row rather than buffering the full result set.

    Args:
        dsn: libpq connection string.
            URI form: ``"postgresql://user:pass@host:5432/dbname"``
            Keyword form: ``"host=localhost dbname=mydb user=alice"``
    """

    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._conn: Any = psycopg.connect(dsn, autocommit=False)
        self._lock = threading.RLock()
        self._cursor_seq = itertools.count()

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _require_open(self) -> Any:
        """Return the open connection or raise RuntimeError if closed."""
        if self._conn is None:
            raise RuntimeError("PostgreSQLConnector is closed")
        return self._conn

    @staticmethod
    def _validate_table_name(table_name: str) -> None:
        """Raise ValueError if table_name contains a double-quote character."""
        if '"' in table_name:
            raise ValueError(
                f"Table name {table_name!r} contains an invalid double-quote character."
            )

    # ── Schema introspection ──────────────────────────────────────────────────

    def get_table_names(self) -> list[str]:
        """Return all user table names in this database (sorted, excludes views)."""
        with self._lock:
            conn = self._require_open()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = current_schema()
                      AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                    """
                )
                return [row[0] for row in cur.fetchall()]

    def get_pk_columns(self, table_name: str) -> list[str]:
        """Return primary-key column names in key-sequence order."""
        self._validate_table_name(table_name)
        with self._lock:
            conn = self._require_open()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT kcu.column_name
                    FROM information_schema.key_column_usage kcu
                    JOIN information_schema.table_constraints tc
                      ON kcu.constraint_name = tc.constraint_name
                     AND kcu.table_schema    = tc.table_schema
                     AND kcu.table_name      = tc.table_name
                    WHERE tc.constraint_type = 'PRIMARY KEY'
                      AND kcu.table_schema   = current_schema()
                      AND kcu.table_name     = %s
                    ORDER BY kcu.ordinal_position
                    """,
                    (table_name,),
                )
                return [row[0] for row in cur.fetchall()]

    def get_column_info(self, table_name: str) -> list[ColumnInfo]:
        """Return column metadata with Arrow-mapped types."""
        self._validate_table_name(table_name)
        with self._lock:
            conn = self._require_open()
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT column_name, data_type, udt_name, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = current_schema()
                      AND table_name   = %s
                    ORDER BY ordinal_position
                    """,
                    (table_name,),
                )
                return [
                    ColumnInfo(
                        name=row[0],
                        arrow_type=_pg_type_to_arrow(row[1], row[2]),
                        nullable=(row[3].upper() == "YES"),
                    )
                    for row in cur.fetchall()
                ]

    # ── Read ──────────────────────────────────────────────────────────────────

    def iter_batches(
        self,
        query: str,
        params: Any = None,
        batch_size: int = 1000,
    ) -> Iterator[pa.RecordBatch]:
        """Execute a query and yield results as Arrow RecordBatches."""
        import pyarrow as _pa

        with self._lock:
            conn = self._require_open()
            cursor_name = f"orcapod_{next(self._cursor_seq)}"
            cur = conn.cursor(name=cursor_name)

        try:
            with self._lock:
                self._require_open()
                cur.execute(query, params)
                if cur.description is None:
                    return
                col_names = [d.name for d in cur.description]
                type_lookup = _resolve_column_type_lookup(query, self)
                arrow_types = [type_lookup.get(n, _pa.large_string()) for n in col_names]
                schema = _pa.schema(
                    [_pa.field(n, t) for n, t in zip(col_names, arrow_types)]
                )
                rows = cur.fetchmany(batch_size)

            while rows:
                arrays = [
                    _pa.array([r[i] for r in rows], type=t)
                    for i, t in enumerate(arrow_types)
                ]
                yield _pa.RecordBatch.from_arrays(arrays, schema=schema)
                with self._lock:
                    self._require_open()
                    rows = cur.fetchmany(batch_size)
        finally:
            cur.close()

    # ── Write ─────────────────────────────────────────────────────────────────

    def create_table_if_not_exists(
        self,
        table_name: str,
        columns: list[ColumnInfo],
        pk_column: str,
    ) -> None:
        """Create a table with the given columns if it does not already exist."""
        col_names = [col.name for col in columns]
        if pk_column not in col_names:
            raise ValueError(
                f"pk_column {pk_column!r} not found in columns: {col_names}"
            )
        self._validate_table_name(table_name)
        with self._lock:
            conn = self._require_open()
            col_defs = []
            for col in columns:
                pg_type = _arrow_type_to_pg_sql(col.arrow_type)
                not_null = " NOT NULL" if not col.nullable else ""
                pk = " PRIMARY KEY" if col.name == pk_column else ""
                escaped = col.name.replace('"', '""')
                col_defs.append(f'    "{escaped}" {pg_type}{not_null}{pk}')
            ddl = f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n'
            ddl += ",\n".join(col_defs)
            ddl += "\n)"
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()

    def upsert_records(
        self,
        table_name: str,
        records: pa.Table,
        id_column: str,
        skip_existing: bool = False,
    ) -> None:
        """Write records to a table using upsert semantics."""
        self._validate_table_name(table_name)
        with self._lock:
            conn = self._require_open()
            cols = list(records.column_names)
            col_list = ", ".join('"' + c.replace('"', '""') + '"' for c in cols)
            placeholders = ", ".join("%s" for _ in cols)

            if skip_existing:
                conflict_clause = f'ON CONFLICT ("{id_column}") DO NOTHING'
            else:
                non_pk_cols = [c for c in cols if c != id_column]
                if non_pk_cols:
                    set_clause = ", ".join(
                        f'"{c}" = EXCLUDED."{c}"' for c in non_pk_cols
                    )
                    conflict_clause = (
                        f'ON CONFLICT ("{id_column}") DO UPDATE SET {set_clause}'
                    )
                else:
                    conflict_clause = f'ON CONFLICT ("{id_column}") DO NOTHING'

            sql = (
                f'INSERT INTO "{table_name}" ({col_list}) VALUES ({placeholders}) '
                f'{conflict_clause}'
            )
            rows = [tuple(row[c] for c in cols) for row in records.to_pylist()]
            with conn.cursor() as cur:
                cur.executemany(sql, rows)
            conn.commit()

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def close(self) -> None:
        """Close the database connection. Idempotent."""
        with self._lock:
            if self._conn is not None:
                self._conn.close()
                self._conn = None

    def __enter__(self) -> PostgreSQLConnector:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    # ── Serialization ─────────────────────────────────────────────────────────

    def to_config(self) -> dict[str, Any]:
        """Serialize connection configuration to a JSON-compatible dict."""
        return {
            "connector_type": "postgresql",
            "dsn": self._dsn,
        }

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> PostgreSQLConnector:
        """Reconstruct a PostgreSQLConnector from a config dict."""
        if config.get("connector_type") != "postgresql":
            raise ValueError(
                f"Expected connector_type 'postgresql', got "
                f"{config.get('connector_type')!r}"
            )
        return cls(dsn=config["dsn"])
