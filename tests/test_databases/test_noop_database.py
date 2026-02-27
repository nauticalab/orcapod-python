"""
Tests for NoOpArrowDatabase.

Verifies that:
- The class satisfies the ArrowDatabaseProtocol protocol
- All write operations complete without raising
- All read operations return None regardless of prior writes
- flush() is a no-op
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.databases import NoOpArrowDatabase
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

PATH = ("test", "path")


@pytest.fixture
def db() -> NoOpArrowDatabase:
    return NoOpArrowDatabase()


def make_table(**columns: list) -> pa.Table:
    return pa.table({k: pa.array(v) for k, v in columns.items()})


# ---------------------------------------------------------------------------
# 1. Protocol conformance
# ---------------------------------------------------------------------------


class TestProtocolConformance:
    def test_satisfies_arrow_database_protocol(self, db):
        assert isinstance(db, ArrowDatabaseProtocol)

    def test_has_add_record(self, db):
        assert callable(db.add_record)

    def test_has_add_records(self, db):
        assert callable(db.add_records)

    def test_has_get_record_by_id(self, db):
        assert callable(db.get_record_by_id)

    def test_has_get_all_records(self, db):
        assert callable(db.get_all_records)

    def test_has_get_records_by_ids(self, db):
        assert callable(db.get_records_by_ids)

    def test_has_get_records_with_column_value(self, db):
        assert callable(db.get_records_with_column_value)

    def test_has_flush(self, db):
        assert callable(db.flush)


# ---------------------------------------------------------------------------
# 2. Write operations do not raise
# ---------------------------------------------------------------------------


class TestWriteOperationsAreNoOps:
    def test_add_record_does_not_raise(self, db):
        db.add_record(PATH, "id1", make_table(x=[1, 2]))

    def test_add_record_with_skip_duplicates_does_not_raise(self, db):
        db.add_record(PATH, "id1", make_table(x=[1]), skip_duplicates=True)

    def test_add_record_with_flush_does_not_raise(self, db):
        db.add_record(PATH, "id1", make_table(x=[1]), flush=True)

    def test_add_records_does_not_raise(self, db):
        db.add_records(PATH, make_table(x=[1, 2, 3]))

    def test_add_records_with_record_id_column_does_not_raise(self, db):
        db.add_records(
            PATH,
            make_table(rid=["a", "b"], x=[1, 2]),
            record_id_column="rid",
        )

    def test_add_records_with_skip_duplicates_does_not_raise(self, db):
        db.add_records(PATH, make_table(x=[1]), skip_duplicates=True)

    def test_add_records_with_flush_does_not_raise(self, db):
        db.add_records(PATH, make_table(x=[1]), flush=True)

    def test_flush_does_not_raise(self, db):
        db.flush()

    def test_flush_after_writes_does_not_raise(self, db):
        db.add_record(PATH, "id1", make_table(x=[1]))
        db.add_records(PATH, make_table(x=[2, 3]))
        db.flush()


# ---------------------------------------------------------------------------
# 3. Read operations always return None
# ---------------------------------------------------------------------------


class TestReadOperationsReturnNone:
    def test_get_record_by_id_returns_none_on_empty_db(self, db):
        assert db.get_record_by_id(PATH, "id1") is None

    def test_get_record_by_id_returns_none_after_write(self, db):
        db.add_record(PATH, "id1", make_table(x=[42]))
        assert db.get_record_by_id(PATH, "id1") is None

    def test_get_record_by_id_with_record_id_column_returns_none(self, db):
        assert db.get_record_by_id(PATH, "id1", record_id_column="rid") is None

    def test_get_all_records_returns_none_on_empty_db(self, db):
        assert db.get_all_records(PATH) is None

    def test_get_all_records_returns_none_after_write(self, db):
        db.add_records(PATH, make_table(x=[1, 2, 3]))
        assert db.get_all_records(PATH) is None

    def test_get_all_records_with_record_id_column_returns_none(self, db):
        assert db.get_all_records(PATH, record_id_column="rid") is None

    def test_get_records_by_ids_returns_none_on_empty_db(self, db):
        assert db.get_records_by_ids(PATH, ["id1", "id2"]) is None

    def test_get_records_by_ids_returns_none_after_write(self, db):
        db.add_record(PATH, "id1", make_table(x=[1]))
        assert db.get_records_by_ids(PATH, ["id1"]) is None

    def test_get_records_by_ids_empty_collection_returns_none(self, db):
        assert db.get_records_by_ids(PATH, []) is None

    def test_get_records_with_column_value_returns_none_on_empty_db(self, db):
        assert db.get_records_with_column_value(PATH, {"x": 1}) is None

    def test_get_records_with_column_value_returns_none_after_write(self, db):
        db.add_records(PATH, make_table(x=[1, 2, 3]))
        assert db.get_records_with_column_value(PATH, {"x": 1}) is None

    def test_get_records_with_column_value_accepts_list_of_tuples(self, db):
        assert db.get_records_with_column_value(PATH, [("x", 1), ("y", 2)]) is None


# ---------------------------------------------------------------------------
# 4. Reads return None across different paths
# ---------------------------------------------------------------------------


class TestPathIsolation:
    def test_different_paths_all_return_none(self, db):
        paths = [("a",), ("a", "b"), ("a", "b", "c"), ("x", "y")]
        for path in paths:
            db.add_records(path, make_table(val=[1, 2]))
        for path in paths:
            assert db.get_all_records(path) is None

    def test_reads_on_unwritten_path_return_none(self, db):
        db.add_records(("written",), make_table(x=[1]))
        assert db.get_all_records(("never_written",)) is None
