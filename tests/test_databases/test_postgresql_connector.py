# tests/test_databases/test_postgresql_connector.py
"""Unit tests for PostgreSQLConnector — no live database required."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from orcapod.databases.postgresql_connector import (
    PostgreSQLConnector,
    _arrow_type_to_pg_sql,
    _pg_type_to_arrow,
)
from orcapod.protocols.db_connector_protocol import DBConnectorProtocol
from orcapod.types import ColumnInfo


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
