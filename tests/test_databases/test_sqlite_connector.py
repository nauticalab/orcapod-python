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


# ---------------------------------------------------------------------------
# Schema introspection
# ---------------------------------------------------------------------------


class TestGetTableNames:
    def test_empty_database(self, connector: SQLiteConnector) -> None:
        assert connector.get_table_names() == []

    def test_returns_table_names(self, connector: SQLiteConnector) -> None:
        connector._conn.execute('CREATE TABLE "foo" (id INTEGER PRIMARY KEY)')
        connector._conn.execute('CREATE TABLE "bar" (id INTEGER PRIMARY KEY)')
        assert connector.get_table_names() == ["bar", "foo"]  # sorted

    def test_excludes_views(self, connector: SQLiteConnector) -> None:
        connector._conn.execute('CREATE TABLE "base" (id INTEGER PRIMARY KEY)')
        connector._conn.execute('CREATE VIEW "v_base" AS SELECT * FROM "base"')
        assert connector.get_table_names() == ["base"]


class TestGetPkColumns:
    def test_single_pk(self, connector: SQLiteConnector) -> None:
        connector._conn.execute(
            'CREATE TABLE "t" (id TEXT PRIMARY KEY, val REAL)'
        )
        assert connector.get_pk_columns("t") == ["id"]

    def test_composite_pk_order_preserved(self, connector: SQLiteConnector) -> None:
        connector._conn.execute(
            'CREATE TABLE "t" (a TEXT, b INTEGER, c REAL, PRIMARY KEY (a, b))'
        )
        assert connector.get_pk_columns("t") == ["a", "b"]

    def test_no_pk_returns_empty(self, connector: SQLiteConnector) -> None:
        connector._conn.execute('CREATE TABLE "t" (val TEXT)')
        assert connector.get_pk_columns("t") == []

    def test_nonexistent_table_returns_empty(self, connector: SQLiteConnector) -> None:
        assert connector.get_pk_columns("no_such_table") == []

    def test_raises_on_invalid_table_name(self, connector: SQLiteConnector) -> None:
        with pytest.raises(ValueError, match="double-quote"):
            connector.get_pk_columns('table"name')


class TestGetColumnInfo:
    def test_all_affinities(self, connector: SQLiteConnector) -> None:
        connector._conn.execute(
            """CREATE TABLE "t" (
                a TEXT,
                b INTEGER,
                c REAL,
                d BLOB,
                e NUMERIC,
                f BOOLEAN
            )"""
        )
        infos = {ci.name: ci for ci in connector.get_column_info("t")}
        assert infos["a"].arrow_type == pa.large_string()
        assert infos["b"].arrow_type == pa.int64()
        assert infos["c"].arrow_type == pa.float64()
        assert infos["d"].arrow_type == pa.large_binary()
        assert infos["e"].arrow_type == pa.float64()
        assert infos["f"].arrow_type == pa.bool_()

    def test_nullable_from_notnull(self, connector: SQLiteConnector) -> None:
        connector._conn.execute(
            'CREATE TABLE "t" (a TEXT NOT NULL, b TEXT)'
        )
        infos = {ci.name: ci for ci in connector.get_column_info("t")}
        assert infos["a"].nullable is False
        assert infos["b"].nullable is True

    def test_table_with_zero_rows_returns_column_metadata(self, connector: SQLiteConnector) -> None:
        connector._conn.execute(
            'CREATE TABLE "t" (id INTEGER PRIMARY KEY, val TEXT)'
        )
        infos = connector.get_column_info("t")
        assert len(infos) == 2
        assert infos[0].name == "id"

    def test_nonexistent_table_returns_empty(self, connector: SQLiteConnector) -> None:
        assert connector.get_column_info("no_such_table") == []

    def test_raises_on_invalid_table_name(self, connector: SQLiteConnector) -> None:
        with pytest.raises(ValueError, match="double-quote"):
            connector.get_column_info('table"name')


# ---------------------------------------------------------------------------
# Write path
# ---------------------------------------------------------------------------


class TestCreateTableIfNotExists:
    def _make_columns(self) -> list[ColumnInfo]:
        return [
            ColumnInfo("__record_id", pa.large_string(), nullable=False),
            ColumnInfo("value", pa.float64(), nullable=True),
        ]

    def test_creates_table(self, connector: SQLiteConnector) -> None:
        connector.create_table_if_not_exists("my_table", self._make_columns(), "__record_id")
        assert "my_table" in connector.get_table_names()

    def test_idempotent(self, connector: SQLiteConnector) -> None:
        cols = self._make_columns()
        connector.create_table_if_not_exists("my_table", cols, "__record_id")
        connector.create_table_if_not_exists("my_table", cols, "__record_id")  # must not raise

    def test_pk_column_set(self, connector: SQLiteConnector) -> None:
        connector.create_table_if_not_exists("t", self._make_columns(), "__record_id")
        assert connector.get_pk_columns("t") == ["__record_id"]

    def test_column_types_match(self, connector: SQLiteConnector) -> None:
        connector.create_table_if_not_exists("t", self._make_columns(), "__record_id")
        infos = {ci.name: ci for ci in connector.get_column_info("t")}
        assert infos["__record_id"].arrow_type == pa.large_string()
        assert infos["value"].arrow_type == pa.float64()

    def test_not_null_respected(self, connector: SQLiteConnector) -> None:
        connector.create_table_if_not_exists("t", self._make_columns(), "__record_id")
        infos = {ci.name: ci for ci in connector.get_column_info("t")}
        assert infos["__record_id"].nullable is False
        assert infos["value"].nullable is True

    def test_raises_on_invalid_table_name(self, connector: SQLiteConnector) -> None:
        with pytest.raises(ValueError, match="double-quote"):
            connector.create_table_if_not_exists('table"name', self._make_columns(), "__record_id")


class TestUpsertRecords:
    def _setup_table(self, connector: SQLiteConnector) -> None:
        cols = [
            ColumnInfo("__record_id", pa.large_string(), nullable=False),
            ColumnInfo("value", pa.float64(), nullable=True),
        ]
        connector.create_table_if_not_exists("t", cols, "__record_id")

    def _make_table(self, ids: list[str], values: list[float | None]) -> pa.Table:
        import pyarrow as pa
        return pa.table({
            "__record_id": pa.array(ids, type=pa.large_string()),
            "value": pa.array(values, type=pa.float64()),
        })

    def test_insert_new_records(self, connector: SQLiteConnector) -> None:
        self._setup_table(connector)
        records = self._make_table(["a", "b"], [1.0, 2.0])
        connector.upsert_records("t", records, "__record_id")
        cursor = connector._conn.execute('SELECT COUNT(*) FROM "t"')
        assert cursor.fetchone()[0] == 2

    def test_replace_existing_records(self, connector: SQLiteConnector) -> None:
        self._setup_table(connector)
        records_v1 = self._make_table(["a"], [1.0])
        connector.upsert_records("t", records_v1, "__record_id")
        records_v2 = self._make_table(["a"], [99.0])
        connector.upsert_records("t", records_v2, "__record_id", skip_existing=False)
        cursor = connector._conn.execute('SELECT value FROM "t" WHERE "__record_id" = "a"')
        assert cursor.fetchone()[0] == 99.0

    def test_skip_existing_keeps_original(self, connector: SQLiteConnector) -> None:
        self._setup_table(connector)
        records_v1 = self._make_table(["a"], [1.0])
        connector.upsert_records("t", records_v1, "__record_id")
        records_v2 = self._make_table(["a"], [99.0])
        connector.upsert_records("t", records_v2, "__record_id", skip_existing=True)
        cursor = connector._conn.execute('SELECT value FROM "t" WHERE "__record_id" = "a"')
        assert cursor.fetchone()[0] == 1.0

    def test_raises_on_invalid_table_name(self, connector: SQLiteConnector) -> None:
        records = self._make_table(["a"], [1.0])
        with pytest.raises(ValueError, match="double-quote"):
            connector.upsert_records('table"name', records, "__record_id")
