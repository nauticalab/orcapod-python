"""Tests for SQLiteConnector — DBConnectorProtocol backed by sqlite3."""
from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
import pytest

from orcapod.databases.sqlite_connector import (
    SQLiteConnector,
    _arrow_type_to_sqlite_sql,
    _coerce_column,
    _sqlite_type_to_arrow,
)
from orcapod.protocols.db_connector_protocol import DBConnectorProtocol
from orcapod.types import ColumnInfo


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def connector() -> Iterator[SQLiteConnector]:
    c = SQLiteConnector(":memory:")
    yield c
    try:
        c.close()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Type helpers
# ---------------------------------------------------------------------------


class TestSqliteTypeToArrow:
    def test_boolean(self):
        assert _sqlite_type_to_arrow("BOOLEAN") == pa.bool_()
        assert _sqlite_type_to_arrow("boolean") == pa.bool_()

    def test_integer_affinity(self):
        for t in ("INTEGER", "INT", "BIGINT", "TINYINT", "MEDIUMINT"):
            assert _sqlite_type_to_arrow(t) == pa.int64(), t

    def test_text_affinity(self):
        for t in ("TEXT", "VARCHAR(255)", "NCHAR(10)", "CLOB"):
            assert _sqlite_type_to_arrow(t) == pa.large_string(), t

    def test_blob_affinity(self):
        assert _sqlite_type_to_arrow("BLOB") == pa.large_binary()
        assert _sqlite_type_to_arrow("") == pa.large_binary()

    def test_real_affinity(self):
        for t in ("REAL", "FLOAT", "DOUBLE", "DOUBLE PRECISION"):
            assert _sqlite_type_to_arrow(t) == pa.float64(), t

    def test_numeric_affinity(self):
        for t in ("NUMERIC", "DECIMAL(10,2)", "NUMBER"):
            assert _sqlite_type_to_arrow(t) == pa.float64(), t


class TestArrowTypeToSqliteSql:
    def test_integers(self):
        for t in (pa.int8(), pa.int16(), pa.int32(), pa.int64(),
                  pa.uint8(), pa.uint16(), pa.uint32(), pa.uint64()):
            assert _arrow_type_to_sqlite_sql(t) == "INTEGER", t

    def test_floats(self):
        assert _arrow_type_to_sqlite_sql(pa.float32()) == "REAL"
        assert _arrow_type_to_sqlite_sql(pa.float64()) == "REAL"

    def test_strings(self):
        assert _arrow_type_to_sqlite_sql(pa.utf8()) == "TEXT"
        assert _arrow_type_to_sqlite_sql(pa.large_utf8()) == "TEXT"

    def test_binary(self):
        assert _arrow_type_to_sqlite_sql(pa.binary()) == "BLOB"
        assert _arrow_type_to_sqlite_sql(pa.large_binary()) == "BLOB"

    def test_bool(self):
        assert _arrow_type_to_sqlite_sql(pa.bool_()) == "BOOLEAN"


class TestSQLiteConnectorScaffold:
    def test_isinstance_dbconnector_protocol(self) -> None:
        connector = SQLiteConnector(":memory:")
        assert isinstance(connector, DBConnectorProtocol)
        connector.close()


class TestCoerceColumn:
    def test_bool_coercion(self):
        assert _coerce_column([1, 0, 1], pa.bool_()) == [True, False, True]

    def test_bool_with_none(self):
        assert _coerce_column([1, None, 0], pa.bool_()) == [True, None, False]

    def test_non_bool_passthrough(self):
        vals = [1, 2, 3]
        assert _coerce_column(vals, pa.int64()) is vals


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


class TestLifecycle:
    def test_constructor_opens_connection(self, connector: SQLiteConnector) -> None:
        # Verify connection is open via the internal guard (does not raise = open)
        connector._require_open()  # must not raise

    def test_close_is_idempotent(self, connector: SQLiteConnector) -> None:
        connector.close()
        connector.close()  # must not raise

    def test_context_manager_closes_on_exit(self) -> None:
        with SQLiteConnector(":memory:") as c:
            c._require_open()  # must not raise inside block
        # After __exit__, the connection must be closed
        with pytest.raises(RuntimeError, match="closed"):
            c._require_open()

    def test_require_open_raises_after_close(self, connector: SQLiteConnector) -> None:
        connector.close()
        with pytest.raises(RuntimeError, match="closed"):
            connector._require_open()
