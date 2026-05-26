# tests/test_databases/test_postgresql_connector.py
"""Unit tests for PostgreSQLConnector — no live database required."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pyarrow as pa
import pytest

from orcapod.databases.postgresql_connector import (
    PostgreSQLConnector,
    _arrow_type_to_pg_sql,
    _pg_type_to_arrow,
)
from orcapod.protocols.async_db_connector_protocol import AsyncDBConnectorProtocol
from orcapod.protocols.db_connector_protocol import DBConnectorProtocol


class TestPgTypeToArrow:
    def test_bool(self):
        assert _pg_type_to_arrow("bool", "bool") == pa.bool_()

    def test_int2(self):
        assert _pg_type_to_arrow("int2", "int2") == pa.int32()

    def test_int4(self):
        assert _pg_type_to_arrow("int4", "int4") == pa.int32()

    def test_int8(self):
        assert _pg_type_to_arrow("int8", "int8") == pa.int64()

    def test_float4(self):
        assert _pg_type_to_arrow("float4", "float4") == pa.float32()

    def test_float8(self):
        assert _pg_type_to_arrow("float8", "float8") == pa.float64()

    def test_numeric(self):
        assert _pg_type_to_arrow("numeric", "numeric") == pa.float64()

    def test_text(self):
        assert _pg_type_to_arrow("text", "text") == pa.large_string()

    def test_varchar(self):
        assert _pg_type_to_arrow("character varying", "varchar") == pa.large_string()

    def test_char(self):
        assert _pg_type_to_arrow("character", "bpchar") == pa.large_string()

    def test_name(self):
        assert _pg_type_to_arrow("name", "name") == pa.large_string()

    def test_bytea(self):
        assert _pg_type_to_arrow("bytea", "bytea") == pa.large_binary()

    def test_uuid(self):
        assert _pg_type_to_arrow("uuid", "uuid") == pa.large_string()

    def test_json(self):
        assert _pg_type_to_arrow("json", "json") == pa.large_string()

    def test_jsonb(self):
        assert _pg_type_to_arrow("jsonb", "jsonb") == pa.large_string()

    def test_date(self):
        assert _pg_type_to_arrow("date", "date") == pa.date32()

    def test_timestamp(self):
        assert _pg_type_to_arrow("timestamp without time zone", "timestamp") == pa.timestamp("us")

    def test_timestamptz(self):
        assert _pg_type_to_arrow("timestamp with time zone", "timestamptz") == pa.timestamp("us", tz="UTC")

    def test_time_is_string(self):
        result = _pg_type_to_arrow("time without time zone", "time")
        assert result == pa.large_string()

    def test_timetz_is_string(self):
        result = _pg_type_to_arrow("time with time zone", "timetz")
        assert result == pa.large_string()

    def test_int4_array(self):
        result = _pg_type_to_arrow("ARRAY", "_int4")
        assert result == pa.large_list(pa.int32())

    def test_text_array(self):
        result = _pg_type_to_arrow("ARRAY", "_text")
        assert result == pa.large_list(pa.large_string())

    def test_unknown_falls_back_to_large_string(self):
        result = _pg_type_to_arrow("weird_type", "weird_type")
        assert result == pa.large_string()


class TestArrowTypeToPgSql:
    def test_bool(self):
        assert _arrow_type_to_pg_sql(pa.bool_()) == "BOOLEAN"

    def test_int32(self):
        assert _arrow_type_to_pg_sql(pa.int32()) == "INTEGER"

    def test_int64(self):
        assert _arrow_type_to_pg_sql(pa.int64()) == "BIGINT"

    def test_float32(self):
        assert _arrow_type_to_pg_sql(pa.float32()) == "REAL"

    def test_float64(self):
        assert _arrow_type_to_pg_sql(pa.float64()) == "DOUBLE PRECISION"

    def test_large_string(self):
        assert _arrow_type_to_pg_sql(pa.large_string()) == "TEXT"

    def test_string(self):
        assert _arrow_type_to_pg_sql(pa.utf8()) == "TEXT"

    def test_large_binary(self):
        assert _arrow_type_to_pg_sql(pa.large_binary()) == "BYTEA"

    def test_binary(self):
        assert _arrow_type_to_pg_sql(pa.binary()) == "BYTEA"

    def test_date32(self):
        assert _arrow_type_to_pg_sql(pa.date32()) == "DATE"

    def test_timestamp_no_tz(self):
        assert _arrow_type_to_pg_sql(pa.timestamp("us")) == "TIMESTAMP"

    def test_timestamp_with_tz(self):
        assert _arrow_type_to_pg_sql(pa.timestamp("us", tz="UTC")) == "TIMESTAMPTZ"

    def test_large_list_raises(self):
        with pytest.raises(ValueError, match="not supported"):
            _arrow_type_to_pg_sql(pa.large_list(pa.int32()))

    def test_unknown_falls_back_to_text(self):
        # pa.null() is an unusual type not in the mapping
        result = _arrow_type_to_pg_sql(pa.null())
        assert result == "TEXT"


class TestPostgreSQLConnectorScaffold:
    def test_isinstance_dbconnector_protocol(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            connector = PostgreSQLConnector("postgresql://localhost/test")
            assert isinstance(connector, DBConnectorProtocol)
            connector._conn = None  # skip actual close


class TestConfig:
    def test_to_config(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            connector = PostgreSQLConnector("postgresql://user:pass@localhost:5432/mydb")
            config = connector.to_config()
            assert config["connector_type"] == "postgresql"
            assert config["dsn"] == "postgresql://user:pass@localhost:5432/mydb"
            connector._conn = None

    def test_from_config_roundtrip(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            config = {"connector_type": "postgresql", "dsn": "postgresql://localhost/test"}
            connector = PostgreSQLConnector.from_config(config)
            assert isinstance(connector, PostgreSQLConnector)
            connector._conn = None

    def test_from_config_wrong_type_raises(self) -> None:
        with pytest.raises(ValueError, match="postgresql"):
            PostgreSQLConnector.from_config({"connector_type": "sqlite", "dsn": "x"})

    def test_from_config_missing_type_raises(self) -> None:
        with pytest.raises(ValueError, match="postgresql"):
            PostgreSQLConnector.from_config({"dsn": "postgresql://localhost/test"})


class TestLifecycleUnit:
    def test_close_is_idempotent(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            connector = PostgreSQLConnector("postgresql://localhost/test")
            connector.close()
            connector.close()  # must not raise
            assert mock_conn.close.call_count == 1

    def test_require_open_raises_after_close(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            connector = PostgreSQLConnector("postgresql://localhost/test")
            connector.close()
            with pytest.raises(RuntimeError, match="closed"):
                connector._require_open()

    def test_context_manager(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn
            with PostgreSQLConnector("postgresql://localhost/test") as c:
                c._require_open()  # must not raise
            with pytest.raises(RuntimeError, match="closed"):
                c._require_open()


class TestValidateTableName:
    def test_raises_on_double_quote(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            connector = PostgreSQLConnector("postgresql://localhost/test")
            with pytest.raises(ValueError, match="double-quote"):
                connector._validate_table_name('bad"name')
            connector._conn = None

    def test_valid_name_does_not_raise(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            connector = PostgreSQLConnector("postgresql://localhost/test")
            connector._validate_table_name("valid_table_name")  # must not raise
            connector._conn = None


class TestSchemaIntrospectionUnit:
    """Unit tests for get_table_names and get_pk_columns using mocked cursor."""

    def _make_connector(self) -> PostgreSQLConnector:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            return PostgreSQLConnector("postgresql://localhost/test")

    def test_get_table_names_returns_sorted_list(self) -> None:
        connector = self._make_connector()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = lambda s: s
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = [("alpha",), ("beta",)]  # ORDER BY in SQL
        connector._conn.cursor.return_value = mock_cursor
        result = connector.get_table_names()
        assert result == ["alpha", "beta"]
        connector._conn = None

    def test_get_pk_columns_single_pk(self) -> None:
        connector = self._make_connector()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = lambda s: s
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = [("id",)]
        connector._conn.cursor.return_value = mock_cursor
        result = connector.get_pk_columns("my_table")
        assert result == ["id"]
        connector._conn = None

    def test_get_pk_columns_no_pk(self) -> None:
        connector = self._make_connector()
        mock_cursor = MagicMock()
        mock_cursor.__enter__ = lambda s: s
        mock_cursor.__exit__ = MagicMock(return_value=False)
        mock_cursor.fetchall.return_value = []
        connector._conn.cursor.return_value = mock_cursor
        result = connector.get_pk_columns("my_table")
        assert result == []
        connector._conn = None


class TestAsyncLifecycle:
    """Unit tests for PostgreSQLConnector async lifecycle methods."""

    def _make_connector(self) -> PostgreSQLConnector:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            return PostgreSQLConnector("postgresql://localhost/test")

    def test_isinstance_async_protocol(self) -> None:
        connector = self._make_connector()
        assert isinstance(connector, AsyncDBConnectorProtocol)
        connector._conn = None

    @pytest.mark.asyncio
    async def test_aenter_sets_async_conn(self) -> None:
        connector = self._make_connector()
        mock_async_conn = AsyncMock()
        with patch("psycopg.AsyncConnection.connect", new_callable=AsyncMock,
                   return_value=mock_async_conn):
            result = await connector.__aenter__()
        assert result is connector
        assert connector._async_conn is mock_async_conn
        connector._conn = None

    @pytest.mark.asyncio
    async def test_aexit_closes_async_and_sync(self) -> None:
        connector = self._make_connector()
        mock_async_conn = AsyncMock()
        connector._async_conn = mock_async_conn
        await connector.__aexit__(None, None, None)
        mock_async_conn.close.assert_called_once()
        assert connector._async_conn is None
        assert connector._conn is None  # sync also closed

    @pytest.mark.asyncio
    async def test_async_close_idempotent(self) -> None:
        connector = self._make_connector()
        mock_async_conn = AsyncMock()
        connector._async_conn = mock_async_conn
        await connector.async_close()
        await connector.async_close()  # must not raise
        mock_async_conn.close.assert_called_once()

    def test_require_async_open_raises_before_aenter(self) -> None:
        connector = self._make_connector()
        with pytest.raises(RuntimeError, match="async context manager"):
            connector._require_async_open()
        connector._conn = None

    def test_require_async_open_returns_conn_after_aenter(self) -> None:
        connector = self._make_connector()
        mock_async_conn = MagicMock()
        connector._async_conn = mock_async_conn
        result = connector._require_async_open()
        assert result is mock_async_conn
        connector._conn = None


class TestAsyncSchemaIntrospectionUnit:
    """Unit tests for async schema introspection — mocked cursor."""

    def _make_connector_with_async_conn(self) -> PostgreSQLConnector:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            connector = PostgreSQLConnector("postgresql://localhost/test")
        connector._async_conn = AsyncMock()
        return connector

    def _mock_cursor_returning(self, connector: PostgreSQLConnector,
                               rows: list) -> AsyncMock:
        """Wire up connector._async_conn.cursor() to return rows from fetchall."""
        mock_cursor = AsyncMock()
        mock_cursor.fetchall.return_value = rows
        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_cursor)
        mock_cm.__aexit__ = AsyncMock(return_value=False)
        # cursor() must be a sync call returning an async context manager;
        # override the AsyncMock attribute with a plain MagicMock so that
        # conn.cursor() returns mock_cm directly (not a coroutine).
        connector._async_conn.cursor = MagicMock(return_value=mock_cm)
        return mock_cursor

    @pytest.mark.asyncio
    async def test_async_get_table_names(self) -> None:
        connector = self._make_connector_with_async_conn()
        self._mock_cursor_returning(connector, [("alpha",), ("beta",)])
        result = await connector.async_get_table_names()
        assert result == ["alpha", "beta"]
        connector._conn = None

    @pytest.mark.asyncio
    async def test_async_get_pk_columns_single(self) -> None:
        connector = self._make_connector_with_async_conn()
        self._mock_cursor_returning(connector, [("id",)])
        result = await connector.async_get_pk_columns("my_table")
        assert result == ["id"]
        connector._conn = None

    @pytest.mark.asyncio
    async def test_async_get_pk_columns_empty(self) -> None:
        connector = self._make_connector_with_async_conn()
        self._mock_cursor_returning(connector, [])
        result = await connector.async_get_pk_columns("my_table")
        assert result == []
        connector._conn = None

    @pytest.mark.asyncio
    async def test_async_get_pk_columns_raises_on_bad_name(self) -> None:
        connector = self._make_connector_with_async_conn()
        with pytest.raises(ValueError, match="double-quote"):
            await connector.async_get_pk_columns('bad"name')
        connector._conn = None

    @pytest.mark.asyncio
    async def test_async_get_column_info(self) -> None:
        connector = self._make_connector_with_async_conn()
        self._mock_cursor_returning(connector, [
            ("id", "text", "text", "NO"),
            ("val", "double precision", "float8", "YES"),
        ])
        result = await connector.async_get_column_info("my_table")
        assert len(result) == 2
        assert result[0].name == "id"
        assert result[0].arrow_type == pa.large_string()
        assert result[0].nullable is False
        assert result[1].name == "val"
        assert result[1].arrow_type == pa.float64()
        assert result[1].nullable is True
        connector._conn = None

    @pytest.mark.asyncio
    async def test_async_get_column_info_raises_on_bad_name(self) -> None:
        connector = self._make_connector_with_async_conn()
        with pytest.raises(ValueError, match="double-quote"):
            await connector.async_get_column_info('bad"name')
        connector._conn = None

    @pytest.mark.asyncio
    async def test_schema_methods_raise_without_aenter(self) -> None:
        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            connector = PostgreSQLConnector("postgresql://localhost/test")
        # _async_conn is None — all methods should raise
        with pytest.raises(RuntimeError, match="async context manager"):
            await connector.async_get_table_names()
        connector._conn = None
