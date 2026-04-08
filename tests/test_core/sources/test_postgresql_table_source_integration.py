"""Integration tests for PostgreSQLTableSource — requires a live PostgreSQL instance.

Run with:
    pytest -m postgres tests/test_core/sources/test_postgresql_table_source_integration.py

Connects via standard PG* environment variables:

    PGHOST      (default: localhost)
    PGPORT      (default: 5432)
    PGDATABASE  (default: testdb)
    PGUSER      (default: postgres)
    PGPASSWORD  (default: postgres)

Each test gets an isolated schema created fresh and dropped on teardown.
"""
from __future__ import annotations

import os
import uuid
from collections.abc import Iterator

import pyarrow as pa
import pytest

psycopg = pytest.importorskip("psycopg")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _base_dsn() -> str:
    host = os.environ.get("PGHOST", "localhost")
    port = os.environ.get("PGPORT", "5432")
    dbname = os.environ.get("PGDATABASE", "testdb")
    user = os.environ.get("PGUSER", "postgres")
    password = os.environ.get("PGPASSWORD", "postgres")
    return f"host={host} port={port} dbname={dbname} user={user} password={password}"


@pytest.fixture
def pg_schema() -> Iterator[str]:
    """Create a fresh per-test schema; drop it on teardown."""
    base_dsn = _base_dsn()
    schema = f"test_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(base_dsn, autocommit=True) as admin:
        admin.execute(f"CREATE SCHEMA {schema}")
    yield schema
    with psycopg.connect(base_dsn, autocommit=True) as admin:
        admin.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")


@pytest.fixture
def schema_dsn(pg_schema: str) -> str:
    """DSN that pins the search_path to the test schema."""
    return f"{_base_dsn()} options=-csearch_path={pg_schema}"


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestSinglePKTable:
    """Source backed by a table with a single-column PK."""

    def test_pk_column_is_tag(self, schema_dsn: str) -> None:
        from orcapod.core.sources import PostgreSQLTableSource

        with psycopg.connect(schema_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'CREATE TABLE "measurements" '
                    '(session_id TEXT PRIMARY KEY, trial INTEGER, response REAL)'
                )
                cur.executemany(
                    'INSERT INTO "measurements" VALUES (%s, %s, %s)',
                    [("s1", 1, 0.1), ("s2", 2, 0.2), ("s3", 3, 0.3)],
                )
            conn.commit()

        src = PostgreSQLTableSource(schema_dsn, "measurements")
        tag_schema, _ = src.output_schema()
        assert "session_id" in tag_schema

    def test_non_pk_columns_in_packet_schema(self, schema_dsn: str) -> None:
        from orcapod.core.sources import PostgreSQLTableSource

        with psycopg.connect(schema_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'CREATE TABLE "measurements" '
                    '(session_id TEXT PRIMARY KEY, trial INTEGER, response REAL)'
                )
                cur.executemany(
                    'INSERT INTO "measurements" VALUES (%s, %s, %s)',
                    [("s1", 1, 0.1)],
                )
            conn.commit()

        src = PostgreSQLTableSource(schema_dsn, "measurements")
        _, packet_schema = src.output_schema()
        assert "trial" in packet_schema
        assert "response" in packet_schema

    def test_iter_packets_count_matches_rows(self, schema_dsn: str) -> None:
        from orcapod.core.sources import PostgreSQLTableSource

        with psycopg.connect(schema_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'CREATE TABLE "measurements" '
                    '(session_id TEXT PRIMARY KEY, trial INTEGER, response REAL)'
                )
                cur.executemany(
                    'INSERT INTO "measurements" VALUES (%s, %s, %s)',
                    [("s1", 1, 0.1), ("s2", 2, 0.2), ("s3", 3, 0.3)],
                )
            conn.commit()

        src = PostgreSQLTableSource(schema_dsn, "measurements")
        assert len(list(src.iter_packets())) == 3

    def test_tag_values_are_correct(self, schema_dsn: str) -> None:
        from orcapod.core.sources import PostgreSQLTableSource

        with psycopg.connect(schema_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'CREATE TABLE "measurements" '
                    '(session_id TEXT PRIMARY KEY, trial INTEGER, response REAL)'
                )
                cur.executemany(
                    'INSERT INTO "measurements" VALUES (%s, %s, %s)',
                    [("s1", 1, 0.1), ("s2", 2, 0.2), ("s3", 3, 0.3)],
                )
            conn.commit()

        src = PostgreSQLTableSource(schema_dsn, "measurements")
        tag_values = sorted([tags["session_id"] for tags, _ in src.iter_packets()])
        assert tag_values == ["s1", "s2", "s3"]


@pytest.mark.postgres
class TestCompositePKTable:
    """Source backed by a table with a composite PK."""

    def test_both_pk_columns_are_tags(self, schema_dsn: str) -> None:
        from orcapod.core.sources import PostgreSQLTableSource

        with psycopg.connect(schema_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'CREATE TABLE "events" '
                    '(user_id TEXT, event_id INTEGER, payload TEXT, '
                    'PRIMARY KEY (user_id, event_id))'
                )
                cur.executemany(
                    'INSERT INTO "events" VALUES (%s, %s, %s)',
                    [("u1", 1, "click"), ("u1", 2, "scroll")],
                )
            conn.commit()

        src = PostgreSQLTableSource(schema_dsn, "events")
        tag_schema, _ = src.output_schema()
        assert "user_id" in tag_schema
        assert "event_id" in tag_schema


@pytest.mark.postgres
class TestExplicitTagOverride:
    """tag_columns override overrides the PK."""

    def test_explicit_tag_columns_override_pk(self, schema_dsn: str) -> None:
        from orcapod.core.sources import PostgreSQLTableSource

        with psycopg.connect(schema_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'CREATE TABLE "measurements" '
                    '(session_id TEXT PRIMARY KEY, trial INTEGER, response REAL)'
                )
                cur.executemany(
                    'INSERT INTO "measurements" VALUES (%s, %s, %s)',
                    [("s1", 1, 0.1)],
                )
            conn.commit()

        src = PostgreSQLTableSource(
            schema_dsn, "measurements", tag_columns=["trial"]
        )
        tag_schema, _ = src.output_schema()
        assert "trial" in tag_schema
        assert "session_id" not in tag_schema


@pytest.mark.postgres
@pytest.mark.integration
class TestPipelineIntegration:
    """PostgreSQLTableSource drives a full pipeline end-to-end."""

    def test_postgresql_source_in_pipeline(self, schema_dsn: str) -> None:
        from orcapod.core.function_pod import FunctionPod
        from orcapod.core.packet_function import PythonPacketFunction
        from orcapod.core.sources import PostgreSQLTableSource
        from orcapod.databases import InMemoryArrowDatabase
        from orcapod.pipeline import Pipeline
        from orcapod.pipeline.sync_orchestrator import SyncPipelineOrchestrator

        with psycopg.connect(schema_dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    'CREATE TABLE "measurements" '
                    '(session_id TEXT PRIMARY KEY, trial INTEGER NOT NULL, response REAL NOT NULL)'
                )
                cur.executemany(
                    'INSERT INTO "measurements" VALUES (%s, %s, %s)',
                    [("s1", 1, 0.1), ("s2", 2, 0.2), ("s3", 3, 0.3)],
                )
            conn.commit()

        def double_response(trial: int, response: float) -> float:
            return response * 2.0

        src = PostgreSQLTableSource(schema_dsn, "measurements")
        pf = PythonPacketFunction(double_response, output_keys="doubled")
        pod = FunctionPod(pf)

        pipeline = Pipeline(
            name="pg_integration", pipeline_database=InMemoryArrowDatabase()
        )
        with pipeline:
            pod(src, label="doubler")

        orch = SyncPipelineOrchestrator()
        result = orch.run(pipeline._node_graph)

        fn_outputs = [
            v for k, v in result.node_outputs.items() if k.node_type == "function"
        ]
        assert len(fn_outputs) == 1
        assert len(fn_outputs[0]) == 3

        doubled_values = sorted([pkt.as_dict()["doubled"] for _, pkt in fn_outputs[0]])
        assert doubled_values == pytest.approx([0.2, 0.4, 0.6])

        tag_values = sorted([tags["session_id"] for tags, _ in fn_outputs[0]])
        assert tag_values == ["s1", "s2", "s3"]
