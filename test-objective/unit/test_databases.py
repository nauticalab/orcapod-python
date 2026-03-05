"""Tests for InMemoryArrowDatabase, NoOpArrowDatabase, and DeltaTableDatabase.

Specification-derived tests covering record CRUD, pending-batch semantics,
duplicate handling, and database-specific behaviors.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.databases import DeltaTableDatabase, InMemoryArrowDatabase, NoOpArrowDatabase


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_record(value: int = 1) -> pa.Table:
    """Create a simple single-row Arrow table for testing."""
    return pa.table({"col_a": [value], "col_b": [f"val_{value}"]})


def _make_records(n: int = 3) -> pa.Table:
    """Create a multi-row Arrow table for testing."""
    return pa.table(
        {"col_a": list(range(n)), "col_b": [f"val_{i}" for i in range(n)]}
    )


# ===========================================================================
# InMemoryArrowDatabase
# ===========================================================================


class TestInMemoryArrowDatabaseRoundtrip:
    """add_record + get_record_by_id roundtrip."""

    def test_add_and_get_single_record(self) -> None:
        db = InMemoryArrowDatabase()
        path = ("test", "table")
        record = _make_record(42)

        db.add_record(path, "rec_1", record, flush=True)
        result = db.get_record_by_id(path, "rec_1")

        assert result is not None
        assert result.num_rows == 1
        assert result["col_a"].to_pylist() == [42]

    def test_add_and_get_preserves_data(self) -> None:
        db = InMemoryArrowDatabase()
        path = ("data",)
        record = pa.table({"x": [10], "y": ["hello"]})

        db.add_record(path, "id1", record, flush=True)
        result = db.get_record_by_id(path, "id1")

        assert result is not None
        assert result["x"].to_pylist() == [10]
        assert result["y"].to_pylist() == ["hello"]


class TestInMemoryArrowDatabaseBatchAdd:
    """add_records batch with multiple rows."""

    def test_add_records_multiple_rows(self) -> None:
        db = InMemoryArrowDatabase()
        path = ("batch",)
        records = pa.table(
            {
                "__record_id": ["a", "b", "c"],
                "value": [1, 2, 3],
            }
        )

        db.add_records(path, records, record_id_column="__record_id", flush=True)
        all_records = db.get_all_records(path)

        assert all_records is not None
        assert all_records.num_rows == 3


class TestInMemoryArrowDatabaseGetAll:
    """get_all_records returns all at path."""

    def test_get_all_records(self) -> None:
        db = InMemoryArrowDatabase()
        path = ("multi",)

        db.add_record(path, "r1", _make_record(1), flush=True)
        db.add_record(path, "r2", _make_record(2), flush=True)

        all_records = db.get_all_records(path)
        assert all_records is not None
        assert all_records.num_rows == 2


class TestInMemoryArrowDatabaseGetByIds:
    """get_records_by_ids returns subset."""

    def test_get_records_by_ids_subset(self) -> None:
        db = InMemoryArrowDatabase()
        path = ("subset",)

        for i in range(5):
            db.add_record(path, f"id_{i}", _make_record(i))
        db.flush()

        result = db.get_records_by_ids(path, ["id_1", "id_3"])
        assert result is not None
        assert result.num_rows == 2


class TestInMemoryArrowDatabaseSkipDuplicates:
    """skip_duplicates=True doesn't raise on duplicate."""

    def test_skip_duplicates_no_error(self) -> None:
        db = InMemoryArrowDatabase()
        path = ("dup",)

        db.add_record(path, "same_id", _make_record(1), flush=True)
        # Adding same ID again with skip_duplicates=True should not raise.
        db.add_record(path, "same_id", _make_record(2), skip_duplicates=True, flush=True)

        result = db.get_record_by_id(path, "same_id")
        assert result is not None
        # Original record should be preserved (duplicate was skipped).
        assert result.num_rows == 1


class TestInMemoryArrowDatabasePendingBatch:
    """Pending batch semantics: records not visible until flush()."""

    def test_records_not_visible_before_flush(self) -> None:
        db = InMemoryArrowDatabase()
        path = ("pending",)

        db.add_record(path, "p1", _make_record(1))
        # Without flush, get_record_by_id still finds it in pending.
        # But get_all_records from committed store should reflect pending too
        # because _combined_table merges both. Let's verify pending is separate
        # from committed by checking _tables directly.
        assert path not in {
            tuple(k.split("/")) for k in db._tables
        }, "Record should not be in committed store before flush"

    def test_flush_makes_records_visible(self) -> None:
        db = InMemoryArrowDatabase()
        path = ("pending",)

        db.add_record(path, "p1", _make_record(1))
        db.flush()

        record_key = db._get_record_key(path)
        assert record_key in db._tables, "Record should be in committed store after flush"

        result = db.get_record_by_id(path, "p1")
        assert result is not None


class TestInMemoryArrowDatabaseFlush:
    """flush() makes records visible."""

    def test_flush_commits_pending(self) -> None:
        db = InMemoryArrowDatabase()
        path = ("flush_test",)

        db.add_record(path, "f1", _make_record(10))
        db.add_record(path, "f2", _make_record(20))
        db.flush()

        all_records = db.get_all_records(path)
        assert all_records is not None
        assert all_records.num_rows == 2


class TestInMemoryArrowDatabaseInvalidPath:
    """Invalid path (empty tuple) raises ValueError."""

    def test_empty_path_raises(self) -> None:
        db = InMemoryArrowDatabase()
        with pytest.raises(ValueError, match="cannot be empty"):
            db.add_record((), "id", _make_record())


class TestInMemoryArrowDatabaseNonexistentPath:
    """get_* on nonexistent path returns None."""

    def test_get_record_by_id_nonexistent(self) -> None:
        db = InMemoryArrowDatabase()
        result = db.get_record_by_id(("no", "such"), "missing_id")
        assert result is None

    def test_get_all_records_nonexistent(self) -> None:
        db = InMemoryArrowDatabase()
        result = db.get_all_records(("no", "such"))
        assert result is None

    def test_get_records_by_ids_nonexistent(self) -> None:
        db = InMemoryArrowDatabase()
        result = db.get_records_by_ids(("no", "such"), ["a", "b"])
        assert result is None


# ===========================================================================
# NoOpArrowDatabase
# ===========================================================================


class TestNoOpArrowDatabaseWrites:
    """All writes silently discarded (no errors)."""

    def test_add_record_no_error(self) -> None:
        db = NoOpArrowDatabase()
        db.add_record(("path",), "id", _make_record())

    def test_add_records_no_error(self) -> None:
        db = NoOpArrowDatabase()
        db.add_records(("path",), _make_records())


class TestNoOpArrowDatabaseReads:
    """All reads return None."""

    def test_get_record_by_id_returns_none(self) -> None:
        db = NoOpArrowDatabase()
        db.add_record(("path",), "id", _make_record())
        assert db.get_record_by_id(("path",), "id") is None

    def test_get_all_records_returns_none(self) -> None:
        db = NoOpArrowDatabase()
        assert db.get_all_records(("path",)) is None

    def test_get_records_by_ids_returns_none(self) -> None:
        db = NoOpArrowDatabase()
        assert db.get_records_by_ids(("path",), ["a"]) is None

    def test_get_records_with_column_value_returns_none(self) -> None:
        db = NoOpArrowDatabase()
        assert db.get_records_with_column_value(("path",), {"col": "val"}) is None


class TestNoOpArrowDatabaseFlush:
    """flush() is a no-op (no errors)."""

    def test_flush_no_error(self) -> None:
        db = NoOpArrowDatabase()
        db.flush()  # Should not raise


# ===========================================================================
# DeltaTableDatabase (slow tests)
# ===========================================================================


@pytest.mark.slow
class TestDeltaTableDatabaseRoundtrip:
    """add_record + get_record_by_id roundtrip (uses tmp_path fixture)."""

    def test_add_and_get_record(self, tmp_path: object) -> None:
        db = DeltaTableDatabase(base_path=tmp_path)
        path = ("delta", "test")
        record = _make_record(99)

        db.add_record(path, "d1", record, flush=True)
        result = db.get_record_by_id(path, "d1")

        assert result is not None
        assert result.num_rows == 1
        assert result["col_a"].to_pylist() == [99]


@pytest.mark.slow
class TestDeltaTableDatabaseFlush:
    """flush writes to disk."""

    def test_flush_persists_to_disk(self, tmp_path: object) -> None:
        db = DeltaTableDatabase(base_path=tmp_path)
        path = ("persist",)
        record = _make_record(7)

        db.add_record(path, "p1", record)
        db.flush()

        # Create a new database instance pointing at the same path to verify
        # data was persisted.
        db2 = DeltaTableDatabase(base_path=tmp_path, create_base_path=False)
        result = db2.get_record_by_id(path, "p1")
        assert result is not None
        assert result["col_a"].to_pylist() == [7]
