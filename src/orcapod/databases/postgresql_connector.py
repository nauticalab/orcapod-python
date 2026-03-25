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
# PostgreSQLConnector (stub — full implementation added in Task 5)
# ---------------------------------------------------------------------------


class PostgreSQLConnector:
    """DBConnectorProtocol implementation backed by psycopg3.

    Full implementation is added in Task 5. This stub exists so that
    type-mapping helpers and tests can be imported without a live database.
    """

    def __init__(self, dsn: str, **connect_kwargs: Any) -> None:
        self._dsn = dsn
        self._connect_kwargs = connect_kwargs

    def get_column_info(self, table_name: str) -> list[ColumnInfo]:
        raise NotImplementedError("PostgreSQLConnector.get_column_info not yet implemented")
