"""Tests for SQLiteTableSource.

Test sections:
 1. Import / export sanity
 2. Protocol conformance
 3. PK as default tag columns (single and composite)
 4. Explicit tag column override
 5. ROWID fallback (no explicit PK)
 6. Error cases (missing table, empty table)
 7. Stream behaviour
 8. Deterministic hashing
 9. Config round-trip — PK table (file-backed db)
10. Config round-trip — ROWID-only table (file-backed db)
11. Integration: SQLiteTableSource in a pipeline
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
import pyarrow as pa
import pytest


# ---------------------------------------------------------------------------
# Helpers: create SQLite tables via raw sqlite3
# ---------------------------------------------------------------------------


def _create_table_with_pk(conn: sqlite3.Connection) -> None:
    """Create 'measurements' with single-column PK and insert 3 rows."""
    conn.execute(
        "CREATE TABLE measurements (session_id TEXT PRIMARY KEY, trial INTEGER, response REAL)"
    )
    conn.executemany(
        "INSERT INTO measurements VALUES (?, ?, ?)",
        [("s1", 1, 0.1), ("s2", 2, 0.2), ("s3", 3, 0.3)],
    )
    conn.commit()


def _create_table_with_composite_pk(conn: sqlite3.Connection) -> None:
    """Create 'events' with a composite PK and insert 2 rows."""
    conn.execute(
        "CREATE TABLE events (user_id TEXT, event_id INTEGER, payload TEXT, "
        "PRIMARY KEY (user_id, event_id))"
    )
    conn.executemany(
        "INSERT INTO events VALUES (?, ?, ?)",
        [("u1", 1, "click"), ("u1", 2, "scroll")],
    )
    conn.commit()


def _create_table_without_pk(conn: sqlite3.Connection) -> None:
    """Create 'logs' with no explicit PK (ROWID-only) and insert 3 rows."""
    conn.execute("CREATE TABLE logs (message TEXT, level TEXT)")
    conn.executemany(
        "INSERT INTO logs VALUES (?, ?)",
        [("boot", "INFO"), ("error occurred", "ERROR"), ("shutdown", "INFO")],
    )
    conn.commit()


def _create_empty_table(conn: sqlite3.Connection) -> None:
    """Create 'empty_tbl' with a PK but no rows."""
    conn.execute("CREATE TABLE empty_tbl (id TEXT PRIMARY KEY, val INTEGER)")
    conn.commit()


# ---------------------------------------------------------------------------
# Fixtures — file-backed so SQLiteTableSource can open the same DB
# ---------------------------------------------------------------------------


@pytest.fixture
def pk_db(tmp_path: Path) -> str:
    """File-backed SQLite DB with single-PK 'measurements' table."""
    db_path = str(tmp_path / "pk.db")
    conn = sqlite3.connect(db_path)
    _create_table_with_pk(conn)
    conn.close()
    return db_path


@pytest.fixture
def composite_pk_db(tmp_path: Path) -> str:
    """File-backed SQLite DB with composite-PK 'events' table."""
    db_path = str(tmp_path / "composite_pk.db")
    conn = sqlite3.connect(db_path)
    _create_table_with_composite_pk(conn)
    conn.close()
    return db_path


@pytest.fixture
def rowid_db(tmp_path: Path) -> str:
    """File-backed SQLite DB with ROWID-only 'logs' table."""
    db_path = str(tmp_path / "rowid.db")
    conn = sqlite3.connect(db_path)
    _create_table_without_pk(conn)
    conn.close()
    return db_path


@pytest.fixture
def empty_db(tmp_path: Path) -> str:
    """File-backed SQLite DB with an empty 'empty_tbl' table."""
    db_path = str(tmp_path / "empty.db")
    conn = sqlite3.connect(db_path)
    _create_empty_table(conn)
    conn.close()
    return db_path


@pytest.fixture
def empty_db_path(tmp_path: Path) -> str:
    """File-backed SQLite DB with no tables."""
    db_path = str(tmp_path / "notables.db")
    conn = sqlite3.connect(db_path)
    conn.close()
    return db_path


# ===========================================================================
# 1. Import / export sanity
# ===========================================================================


def test_import_from_core_sources():
    from orcapod.core.sources import SQLiteTableSource
    assert SQLiteTableSource is not None


def test_import_from_orcapod_sources():
    from orcapod.sources import SQLiteTableSource
    assert SQLiteTableSource is not None


def test_in_core_sources_all():
    import orcapod.core.sources as m
    assert "SQLiteTableSource" in m.__all__


# ===========================================================================
# 2. Protocol conformance
# ===========================================================================


class TestProtocolConformance:
    def test_is_source_protocol(self, pk_db):
        from orcapod.core.sources import SQLiteTableSource
        from orcapod.protocols.core_protocols import SourceProtocol
        src = SQLiteTableSource(pk_db, "measurements")
        assert isinstance(src, SourceProtocol)

    def test_is_stream_protocol(self, pk_db):
        from orcapod.core.sources import SQLiteTableSource
        from orcapod.protocols.core_protocols import StreamProtocol
        src = SQLiteTableSource(pk_db, "measurements")
        assert isinstance(src, StreamProtocol)

    def test_is_pipeline_element_protocol(self, pk_db):
        from orcapod.core.sources import SQLiteTableSource
        from orcapod.protocols.hashing_protocols import PipelineElementProtocol
        src = SQLiteTableSource(pk_db, "measurements")
        assert isinstance(src, PipelineElementProtocol)


# ===========================================================================
# 3. PK as default tag columns
# ===========================================================================


class TestPKAsDefaultTags:
    def test_single_pk_is_tag_column(self, pk_db):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_db, "measurements")
        tag_schema, _ = src.output_schema()
        assert "session_id" in tag_schema

    def test_pk_not_in_packet_schema(self, pk_db):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_db, "measurements")
        _, packet_schema = src.output_schema()
        assert "session_id" not in packet_schema

    def test_non_pk_columns_in_packet_schema(self, pk_db):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_db, "measurements")
        _, packet_schema = src.output_schema()
        assert "trial" in packet_schema
        assert "response" in packet_schema

    def test_composite_pk_all_columns_are_tags(self, composite_pk_db):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(composite_pk_db, "events")
        tag_schema, _ = src.output_schema()
        assert "user_id" in tag_schema
        assert "event_id" in tag_schema

    def test_default_source_id_is_table_name(self, pk_db):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_db, "measurements")
        assert src.source_id == "measurements"

    def test_explicit_source_id_overrides_default(self, pk_db):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_db, "measurements", source_id="meas")
        assert src.source_id == "meas"


# ===========================================================================
# 4. Explicit tag column override
# ===========================================================================


class TestExplicitTagOverride:
    def test_explicit_tag_columns_override_pk(self, pk_db):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(
            pk_db, "measurements", tag_columns=["trial"]
        )
        tag_schema, _ = src.output_schema()
        assert "trial" in tag_schema
        assert "session_id" not in tag_schema

    def test_multiple_explicit_tag_columns(self, pk_db):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(
            pk_db,
            "measurements",
            tag_columns=["session_id", "trial"],
        )
        tag_schema, _ = src.output_schema()
        assert "session_id" in tag_schema
        assert "trial" in tag_schema


# ===========================================================================
# 5. ROWID fallback
# ===========================================================================


class TestRowidFallback:
    def test_rowid_only_table_uses_rowid_as_tag(self, rowid_db):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(rowid_db, "logs")
        tag_schema, _ = src.output_schema()
        assert "rowid" in tag_schema

    def test_rowid_is_not_in_packet_schema(self, rowid_db):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(rowid_db, "logs")
        _, packet_schema = src.output_schema()
        assert "rowid" not in packet_schema

    def test_rowid_values_are_positive_integers(self, rowid_db):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(rowid_db, "logs")
        for tags, _ in src.iter_packets():
            assert isinstance(tags["rowid"], int)
            assert tags["rowid"] > 0

    def test_rowid_type_is_int64(self, rowid_db):
        """Verify rowid is actually typed as int64, not large_string."""
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(rowid_db, "logs")
        # The raw stream table (before tag/packet split) holds all columns.
        # We can verify the Arrow type via the internal stream table.
        raw = src._stream._table  # ArrowTableStream stores the enriched table
        assert "rowid" in raw.schema.names
        assert raw.schema.field("rowid").type == pa.int64()

    def test_all_rows_returned_for_rowid_table(self, rowid_db):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(rowid_db, "logs")
        packets = list(src.iter_packets())
        assert len(packets) == 3


# ===========================================================================
# 6. Error cases
# ===========================================================================


class TestErrorCases:
    def test_missing_table_raises_value_error(self, empty_db_path):
        from orcapod.core.sources import SQLiteTableSource
        with pytest.raises(ValueError, match="not found in database"):
            SQLiteTableSource(empty_db_path, "nonexistent")

    def test_empty_table_raises_value_error(self, empty_db):
        from orcapod.core.sources import SQLiteTableSource
        with pytest.raises(ValueError, match="is empty"):
            SQLiteTableSource(empty_db, "empty_tbl")


# ===========================================================================
# 7. Stream behaviour
# ===========================================================================


class TestStreamBehaviour:
    def test_producer_is_none(self, pk_db):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_db, "measurements")
        assert src.producer is None

    def test_upstreams_is_empty(self, pk_db):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_db, "measurements")
        assert src.upstreams == ()

    def test_iter_packets_yields_one_per_row(self, pk_db):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_db, "measurements")
        packets = list(src.iter_packets())
        assert len(packets) == 3

    def test_iter_packets_tags_contain_pk(self, pk_db):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_db, "measurements")
        for tags, _ in src.iter_packets():
            assert "session_id" in tags

    def test_output_schema_returns_two_schemas(self, pk_db):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_db, "measurements")
        result = src.output_schema()
        assert len(result) == 2

    def test_as_table_returns_pyarrow_table(self, pk_db):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_db, "measurements")
        t = src.as_table()
        assert isinstance(t, pa.Table)

    def test_as_table_row_count_matches_source(self, pk_db):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_db, "measurements")
        assert src.as_table().num_rows == 3


# ===========================================================================
# 8. Deterministic hashing
# ===========================================================================


class TestDeterministicHashing:
    def test_pipeline_hash_is_deterministic(self, pk_db):
        from orcapod.core.sources import SQLiteTableSource
        src1 = SQLiteTableSource(pk_db, "measurements")
        src2 = SQLiteTableSource(pk_db, "measurements")
        assert src1.pipeline_hash() == src2.pipeline_hash()

    def test_content_hash_is_deterministic(self, pk_db):
        from orcapod.core.sources import SQLiteTableSource
        src1 = SQLiteTableSource(pk_db, "measurements")
        src2 = SQLiteTableSource(pk_db, "measurements")
        assert src1.content_hash() == src2.content_hash()

    def test_different_tag_columns_yields_different_pipeline_hash(self, pk_db):
        from orcapod.core.sources import SQLiteTableSource
        src1 = SQLiteTableSource(pk_db, "measurements")
        src2 = SQLiteTableSource(
            pk_db, "measurements", tag_columns=["trial"]
        )
        assert src1.pipeline_hash() != src2.pipeline_hash()


# ===========================================================================
# 9. to_config / from_config (basic serialization shape)
# ===========================================================================


class TestConfigSerialization:
    def test_to_config_has_no_connector_key(self, pk_db):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_db, "measurements")
        cfg = src.to_config()
        assert "connector" not in cfg

    def test_to_config_has_source_type_sqlite_table(self, pk_db):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_db, "measurements")
        cfg = src.to_config()
        assert cfg["source_type"] == "sqlite_table"

    def test_to_config_has_db_path(self, pk_db):
        from orcapod.core.sources import SQLiteTableSource
        src = SQLiteTableSource(pk_db, "measurements")
        cfg = src.to_config()
        assert cfg["db_path"] == str(pk_db)
