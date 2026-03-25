# tests/test_databases/test_postgresql_connector_integration.py
"""Integration tests for PostgreSQLConnector — requires a live PostgreSQL instance.

Run with: pytest -m postgres tests/test_databases/test_postgresql_connector_integration.py

Connects to a running PostgreSQL instance via standard PG* environment variables:

    PGHOST      (default: localhost)
    PGPORT      (default: 5432)
    PGDATABASE  (default: testdb)
    PGUSER      (default: postgres)
    PGPASSWORD  (default: postgres)

In CI, a PostgreSQL service container is started by the GitHub Actions workflow
and these variables are set automatically. Locally, point them at any running
PostgreSQL instance (e.g. `docker run -e POSTGRES_PASSWORD=postgres -p 5432:5432 postgres:16`).

Each test gets an isolated schema (created fresh, dropped after), so tests can
share a single database without interfering with each other.
"""
from __future__ import annotations

import os
import uuid
from collections.abc import Iterator

import psycopg
import pyarrow as pa
import pytest

from orcapod.databases import ConnectorArrowDatabase, PostgreSQLConnector
from orcapod.protocols.db_connector_protocol import DBConnectorProtocol
from orcapod.types import ColumnInfo


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _base_dsn() -> str:
    """Build a DSN from standard PG* environment variables."""
    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "5432")
    dbname = os.environ.get("PGDATABASE", "testdb")
    user = os.environ.get("PGUSER", "postgres")
    password = os.environ.get("PGPASSWORD", "postgres")
    return f"host={host} port={port} dbname={dbname} user={user} password={password}"


@pytest.fixture
def connector() -> Iterator[PostgreSQLConnector]:
    """Function-scoped connector with per-test schema isolation.

    Creates a fresh schema for each test and drops it on teardown, so every
    test starts with an empty namespace without needing a dedicated database.
    """
    base_dsn = _base_dsn()
    schema = f"test_{uuid.uuid4().hex[:12]}"
    dsn = f"{base_dsn} options=-csearch_path={schema}"

    with psycopg.connect(base_dsn, autocommit=True) as admin:
        admin.execute(f"CREATE SCHEMA {schema}")

    c = PostgreSQLConnector(dsn)
    yield c
    c.close()

    with psycopg.connect(base_dsn, autocommit=True) as admin:
        admin.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


def _make_columns() -> list[ColumnInfo]:
    return [
        ColumnInfo("__record_id", pa.large_string(), nullable=False),
        ColumnInfo("value", pa.float64(), nullable=True),
    ]


def _make_table(ids: list[str], values: list[float | None]) -> pa.Table:
    return pa.table({
        "__record_id": pa.array(ids, type=pa.large_string()),
        "value": pa.array(values, type=pa.float64()),
    })


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestLifecycle:
    def test_open_and_close(self, connector: PostgreSQLConnector) -> None:
        connector._require_open()  # must not raise
        connector.close()
        with pytest.raises(RuntimeError, match="closed"):
            connector._require_open()

    def test_close_is_idempotent(self, connector: PostgreSQLConnector) -> None:
        connector.close()
        connector.close()  # must not raise

    def test_context_manager(self) -> None:
        dsn = _base_dsn()
        with PostgreSQLConnector(dsn) as c:
            c._require_open()
        with pytest.raises(RuntimeError, match="closed"):
            c._require_open()


# ---------------------------------------------------------------------------
# Schema introspection
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestGetTableNames:
    def test_empty_database(self, connector: PostgreSQLConnector) -> None:
        assert connector.get_table_names() == []

    def test_returns_table_names(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "foo" (id INTEGER PRIMARY KEY)')
            cur.execute('CREATE TABLE "bar" (id INTEGER PRIMARY KEY)')
        connector._conn.commit()
        assert connector.get_table_names() == ["bar", "foo"]

    def test_excludes_views(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "base_tbl" (id INTEGER PRIMARY KEY)')
            cur.execute('CREATE VIEW "v_base" AS SELECT * FROM "base_tbl"')
        connector._conn.commit()
        assert "v_base" not in connector.get_table_names()
        assert "base_tbl" in connector.get_table_names()


@pytest.mark.postgres
class TestGetPkColumns:
    def test_single_pk(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (id TEXT PRIMARY KEY, val REAL)')
        connector._conn.commit()
        assert connector.get_pk_columns("t") == ["id"]

    def test_composite_pk_order_preserved(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute(
                'CREATE TABLE "t" (a TEXT, b INTEGER, c REAL, PRIMARY KEY (a, b))'
            )
        connector._conn.commit()
        assert connector.get_pk_columns("t") == ["a", "b"]

    def test_no_pk_returns_empty(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (val TEXT)')
        connector._conn.commit()
        assert connector.get_pk_columns("t") == []

    def test_nonexistent_table_returns_empty(self, connector: PostgreSQLConnector) -> None:
        assert connector.get_pk_columns("no_such_table") == []

    def test_raises_on_invalid_table_name(self, connector: PostgreSQLConnector) -> None:
        with pytest.raises(ValueError, match="double-quote"):
            connector.get_pk_columns('table"name')


@pytest.mark.postgres
class TestGetColumnInfo:
    def test_all_pg_types(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE "t" (
                    a BOOLEAN,
                    b SMALLINT,
                    c INTEGER,
                    d BIGINT,
                    e REAL,
                    f DOUBLE PRECISION,
                    g TEXT,
                    h BYTEA,
                    i UUID,
                    j JSONB,
                    k DATE,
                    l TIMESTAMP,
                    m TIMESTAMPTZ
                )
            """)
        connector._conn.commit()
        infos = {ci.name: ci for ci in connector.get_column_info("t")}
        assert infos["a"].arrow_type == pa.bool_()
        assert infos["b"].arrow_type == pa.int32()
        assert infos["c"].arrow_type == pa.int32()
        assert infos["d"].arrow_type == pa.int64()
        assert infos["e"].arrow_type == pa.float32()
        assert infos["f"].arrow_type == pa.float64()
        assert infos["g"].arrow_type == pa.large_string()
        assert infos["h"].arrow_type == pa.large_binary()
        assert infos["i"].arrow_type == pa.large_string()  # TODO: PLT-1162
        assert infos["j"].arrow_type == pa.large_string()
        assert infos["k"].arrow_type == pa.date32()
        assert infos["l"].arrow_type == pa.timestamp("us")
        assert infos["m"].arrow_type == pa.timestamp("us", tz="UTC")

    def test_nullable_flag(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (a TEXT NOT NULL, b TEXT)')
        connector._conn.commit()
        infos = {ci.name: ci for ci in connector.get_column_info("t")}
        assert infos["a"].nullable is False
        assert infos["b"].nullable is True

    def test_nonexistent_table_returns_empty(self, connector: PostgreSQLConnector) -> None:
        assert connector.get_column_info("no_such_table") == []

    def test_raises_on_invalid_table_name(self, connector: PostgreSQLConnector) -> None:
        with pytest.raises(ValueError, match="double-quote"):
            connector.get_column_info('bad"name')

    def test_array_column(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (ids INTEGER[])')
        connector._conn.commit()
        infos = {ci.name: ci for ci in connector.get_column_info("t")}
        assert infos["ids"].arrow_type == pa.large_list(pa.int32())


# ---------------------------------------------------------------------------
# Write path
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestCreateTableIfNotExists:
    def test_creates_table(self, connector: PostgreSQLConnector) -> None:
        connector.create_table_if_not_exists("tbl", _make_columns(), "__record_id")
        assert "tbl" in connector.get_table_names()

    def test_idempotent(self, connector: PostgreSQLConnector) -> None:
        cols = _make_columns()
        connector.create_table_if_not_exists("tbl", cols, "__record_id")
        connector.create_table_if_not_exists("tbl", cols, "__record_id")  # must not raise

    def test_pk_column_set(self, connector: PostgreSQLConnector) -> None:
        connector.create_table_if_not_exists("tbl", _make_columns(), "__record_id")
        assert connector.get_pk_columns("tbl") == ["__record_id"]

    def test_not_null_respected(self, connector: PostgreSQLConnector) -> None:
        connector.create_table_if_not_exists("tbl", _make_columns(), "__record_id")
        infos = {ci.name: ci for ci in connector.get_column_info("tbl")}
        assert infos["__record_id"].nullable is False
        assert infos["value"].nullable is True

    def test_raises_when_pk_column_not_in_columns(self, connector: PostgreSQLConnector) -> None:
        with pytest.raises(ValueError, match="not found in columns"):
            connector.create_table_if_not_exists("tbl", _make_columns(), "missing_pk")

    def test_raises_on_invalid_table_name(self, connector: PostgreSQLConnector) -> None:
        with pytest.raises(ValueError, match="double-quote"):
            connector.create_table_if_not_exists('bad"name', _make_columns(), "__record_id")


@pytest.mark.postgres
class TestUpsertRecords:
    def setup_table(self, connector: PostgreSQLConnector) -> None:
        connector.create_table_if_not_exists("tbl", _make_columns(), "__record_id")

    def test_insert_new_records(self, connector: PostgreSQLConnector) -> None:
        self.setup_table(connector)
        connector.upsert_records("tbl", _make_table(["a", "b"], [1.0, 2.0]), "__record_id")
        batches = list(connector.iter_batches('SELECT * FROM "tbl"'))
        assert sum(b.num_rows for b in batches) == 2

    def test_replace_existing(self, connector: PostgreSQLConnector) -> None:
        self.setup_table(connector)
        connector.upsert_records("tbl", _make_table(["a"], [1.0]), "__record_id")
        connector.upsert_records("tbl", _make_table(["a"], [99.0]), "__record_id", skip_existing=False)
        batches = list(connector.iter_batches('SELECT * FROM "tbl" WHERE "__record_id" = \'a\''))
        table = pa.Table.from_batches(batches)
        assert table.column("value")[0].as_py() == 99.0

    def test_skip_existing_keeps_original(self, connector: PostgreSQLConnector) -> None:
        self.setup_table(connector)
        connector.upsert_records("tbl", _make_table(["a"], [1.0]), "__record_id")
        connector.upsert_records("tbl", _make_table(["a"], [99.0]), "__record_id", skip_existing=True)
        batches = list(connector.iter_batches('SELECT * FROM "tbl" WHERE "__record_id" = \'a\''))
        table = pa.Table.from_batches(batches)
        assert table.column("value")[0].as_py() == 1.0

    def test_raises_on_invalid_table_name(self, connector: PostgreSQLConnector) -> None:
        with pytest.raises(ValueError, match="double-quote"):
            connector.upsert_records('bad"name', _make_table(["a"], [1.0]), "__record_id")


# ---------------------------------------------------------------------------
# iter_batches
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestIterBatches:
    def setup_data(self, connector: PostgreSQLConnector) -> None:
        cols = [
            ColumnInfo("id", pa.large_string(), nullable=False),
            ColumnInfo("score", pa.float64(), nullable=True),
            ColumnInfo("count", pa.int64(), nullable=True),
            ColumnInfo("active", pa.bool_(), nullable=True),
        ]
        connector.create_table_if_not_exists("data", cols, "id")
        records = pa.table({
            "id": pa.array(["a", "b", "c"], type=pa.large_string()),
            "score": pa.array([1.5, 2.5, 3.5], type=pa.float64()),
            "count": pa.array([10, 20, 30], type=pa.int64()),
            "active": pa.array([True, False, True], type=pa.bool_()),
        })
        connector.upsert_records("data", records, "id")

    def test_returns_all_rows(self, connector: PostgreSQLConnector) -> None:
        self.setup_data(connector)
        batches = list(connector.iter_batches('SELECT * FROM "data"'))
        assert sum(b.num_rows for b in batches) == 3

    def test_correct_types_roundtrip(self, connector: PostgreSQLConnector) -> None:
        self.setup_data(connector)
        table = pa.Table.from_batches(connector.iter_batches('SELECT * FROM "data"'))
        schema = {f.name: f.type for f in table.schema}
        assert schema["score"] == pa.float64()
        assert schema["count"] == pa.int64()
        assert schema["active"] == pa.bool_()

    def test_batch_size(self, connector: PostgreSQLConnector) -> None:
        self.setup_data(connector)
        batches = list(connector.iter_batches('SELECT * FROM "data"', batch_size=2))
        assert len(batches) == 2
        assert batches[0].num_rows == 2
        assert batches[1].num_rows == 1

    def test_empty_result(self, connector: PostgreSQLConnector) -> None:
        self.setup_data(connector)
        batches = list(connector.iter_batches('SELECT * FROM "data" WHERE 1=0'))
        assert batches == []

    def test_early_termination_closes_cursor(self, connector: PostgreSQLConnector) -> None:
        """Abandoning the generator mid-iteration must not leak server-side cursors."""
        self.setup_data(connector)
        gen = connector.iter_batches('SELECT * FROM "data"', batch_size=1)
        next(gen)  # consume one batch
        gen.close()  # abandon — must send CLOSE to PG without raising
        # Subsequent queries must still work
        batches = list(connector.iter_batches('SELECT * FROM "data"'))
        assert sum(b.num_rows for b in batches) == 3


# ---------------------------------------------------------------------------
# Type roundtrip
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestTypeRoundtrip:
    """Write each PG type and read it back; verify Arrow types are correct."""

    def test_bool_roundtrip(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (id TEXT PRIMARY KEY, v BOOLEAN)')
            cur.execute('INSERT INTO "t" VALUES (%s, %s)', ("r1", True))
        connector._conn.commit()
        table = pa.Table.from_batches(connector.iter_batches('SELECT * FROM "t"'))
        assert table.schema.field("v").type == pa.bool_()
        assert table.column("v")[0].as_py() is True

    def test_int_types_roundtrip(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (i2 SMALLINT, i4 INTEGER, i8 BIGINT)')
            cur.execute('INSERT INTO "t" VALUES (%s, %s, %s)', (1, 2, 3))
        connector._conn.commit()
        table = pa.Table.from_batches(connector.iter_batches('SELECT * FROM "t"'))
        assert table.schema.field("i2").type == pa.int32()
        assert table.schema.field("i4").type == pa.int32()
        assert table.schema.field("i8").type == pa.int64()

    def test_timestamptz_roundtrip(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (id TEXT PRIMARY KEY, ts TIMESTAMPTZ)')
            cur.execute("INSERT INTO \"t\" VALUES ('r1', '2024-01-15 12:00:00+00')")
        connector._conn.commit()
        table = pa.Table.from_batches(connector.iter_batches('SELECT * FROM "t"'))
        assert table.schema.field("ts").type == pa.timestamp("us", tz="UTC")

    def test_int_array_roundtrip(self, connector: PostgreSQLConnector) -> None:
        with connector._conn.cursor() as cur:
            cur.execute('CREATE TABLE "t" (id TEXT PRIMARY KEY, vals INTEGER[])')
            cur.execute("INSERT INTO \"t\" VALUES ('r1', ARRAY[1,2,3])")
        connector._conn.commit()
        infos = {ci.name: ci for ci in connector.get_column_info("t")}
        assert infos["vals"].arrow_type == pa.large_list(pa.int32())


# ---------------------------------------------------------------------------
# ConnectorArrowDatabase integration
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestConnectorArrowDatabaseWithPostgreSQL:
    @pytest.fixture
    def db(self, connector: PostgreSQLConnector) -> ConnectorArrowDatabase:
        return ConnectorArrowDatabase(connector)

    def test_add_and_get_record(self, db: ConnectorArrowDatabase) -> None:
        record = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
        db.add_record(("fn", "test"), record_id="r1", record=record, flush=True)
        result = db.get_record_by_id(("fn", "test"), "r1")
        assert result is not None
        assert result.num_rows == 1
        assert result.column("x")[0].as_py() == 3  # last row kept (dedup)

    def test_add_multiple_records(self, db: ConnectorArrowDatabase) -> None:
        r1 = pa.table({"x": pa.array([1], type=pa.int64())})
        r2 = pa.table({"x": pa.array([2], type=pa.int64())})
        db.add_record(("fn", "multi"), record_id="r1", record=r1)
        db.add_record(("fn", "multi"), record_id="r2", record=r2)
        db.flush()
        result = db.get_all_records(("fn", "multi"))
        assert result is not None
        assert result.num_rows == 2

    def test_skip_duplicates(self, db: ConnectorArrowDatabase) -> None:
        record_v1 = pa.table({"x": pa.array([1], type=pa.int64())})
        record_v2 = pa.table({"x": pa.array([99], type=pa.int64())})
        db.add_record(("fn", "dup"), record_id="r1", record=record_v1, flush=True)
        db.add_record(("fn", "dup"), record_id="r1", record=record_v2, skip_duplicates=True, flush=True)
        result = db.get_record_by_id(("fn", "dup"), "r1")
        assert result is not None
        assert result.column("x")[0].as_py() == 1  # original preserved

    def test_schema_mismatch_raises(self, db: ConnectorArrowDatabase) -> None:
        r1 = pa.table({"x": pa.array([1], type=pa.int64())})
        db.add_record(("fn", "mismatch"), record_id="r1", record=r1, flush=True)
        r2 = pa.table({"x": pa.array([2.0], type=pa.float64())})
        db.add_record(("fn", "mismatch"), record_id="r2", record=r2)
        with pytest.raises(ValueError, match="Schema mismatch"):
            db.flush()
