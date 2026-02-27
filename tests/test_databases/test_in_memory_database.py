"""
Tests for InMemoryArrowDatabase against the ArrowDatabaseProtocol protocol.

Mirrors test_delta_table_database.py — same behavioural assertions, no filesystem.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.databases import InMemoryArrowDatabase
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def db():
    return InMemoryArrowDatabase()


def make_table(**columns: list) -> pa.Table:
    """Build a small PyArrow table from keyword column lists."""
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
# 2. Empty-table cases
# ---------------------------------------------------------------------------


class TestEmptyTable:
    PATH = ("source", "v1")

    def test_get_record_by_id_returns_none_when_empty(self, db):
        assert db.get_record_by_id(self.PATH, "id-1", flush=True) is None

    def test_get_all_records_returns_none_when_empty(self, db):
        assert db.get_all_records(self.PATH) is None

    def test_get_records_by_ids_returns_none_when_empty(self, db):
        assert db.get_records_by_ids(self.PATH, ["id-1"], flush=True) is None

    def test_get_records_with_column_value_returns_none_when_empty(self, db):
        assert (
            db.get_records_with_column_value(self.PATH, {"value": 1}, flush=True)
            is None
        )


# ---------------------------------------------------------------------------
# 3. add_record / get_record_by_id round-trip
# ---------------------------------------------------------------------------


class TestAddRecordRoundTrip:
    PATH = ("source", "v1")

    def test_added_record_retrievable_from_pending(self, db):
        record = make_table(value=[42])
        db.add_record(self.PATH, "id-1", record)
        result = db.get_record_by_id(self.PATH, "id-1")
        assert result is not None
        assert result.column("value").to_pylist() == [42]

    def test_added_record_retrievable_after_flush(self, db):
        record = make_table(value=[99])
        db.add_record(self.PATH, "id-2", record)
        db.flush()
        result = db.get_record_by_id(self.PATH, "id-2", flush=True)
        assert result is not None
        assert result.column("value").to_pylist() == [99]

    def test_record_id_column_not_in_result_by_default(self, db):
        record = make_table(value=[1])
        db.add_record(self.PATH, "id-3", record)
        result = db.get_record_by_id(self.PATH, "id-3")
        assert result is not None
        assert InMemoryArrowDatabase.RECORD_ID_COLUMN not in result.column_names

    def test_record_id_column_exposed_when_requested(self, db):
        record = make_table(value=[1])
        db.add_record(self.PATH, "id-4", record)
        db.flush()
        result = db.get_record_by_id(
            self.PATH, "id-4", record_id_column="my_id", flush=True
        )
        assert result is not None
        assert "my_id" in result.column_names
        assert result.column("my_id").to_pylist() == ["id-4"]

    def test_unknown_record_returns_none(self, db):
        record = make_table(value=[1])
        db.add_record(self.PATH, "id-5", record)
        db.flush()
        assert db.get_record_by_id(self.PATH, "nonexistent", flush=True) is None


# ---------------------------------------------------------------------------
# 4. add_records / get_all_records
# ---------------------------------------------------------------------------


class TestAddRecordsRoundTrip:
    PATH = ("multi", "v1")

    def test_add_records_bulk_and_retrieve_all(self, db):
        records = make_table(__record_id=["a", "b", "c"], value=[10, 20, 30])
        db.add_records(self.PATH, records, record_id_column="__record_id")
        db.flush()
        result = db.get_all_records(self.PATH)
        assert result is not None
        assert result.num_rows == 3

    def test_get_all_records_includes_pending(self, db):
        records = make_table(__record_id=["x", "y"], value=[1, 2])
        db.add_records(self.PATH, records, record_id_column="__record_id")
        # do NOT flush — should still be visible
        result = db.get_all_records(self.PATH)
        assert result is not None
        assert result.num_rows == 2

    def test_first_column_used_as_record_id_by_default(self, db):
        records = make_table(id=["r1", "r2"], score=[5, 6])
        db.add_records(self.PATH, records)
        db.flush()
        result = db.get_all_records(self.PATH)
        assert result is not None
        assert result.num_rows == 2


# ---------------------------------------------------------------------------
# 5. Duplicate handling
# ---------------------------------------------------------------------------


class TestDuplicateHandling:
    PATH = ("dup", "v1")

    def test_skip_duplicates_true_does_not_raise(self, db):
        record = make_table(value=[1])
        db.add_record(self.PATH, "dup-id", record)
        db.flush()
        # same id again — should silently skip
        db.add_record(self.PATH, "dup-id", make_table(value=[2]), skip_duplicates=True)

    def test_skip_duplicates_false_raises_on_pending_duplicate(self, db):
        record = make_table(value=[1])
        db.add_record(self.PATH, "dup-id2", record)
        with pytest.raises(ValueError):
            db.add_records(
                self.PATH,
                make_table(__record_id=["dup-id2"], value=[99]),
                record_id_column="__record_id",
                skip_duplicates=False,
            )

    def test_within_batch_deduplication_keeps_last(self, db):
        records = make_table(__record_id=["same", "same"], value=[1, 2])
        db.add_records(self.PATH, records, record_id_column="__record_id")
        db.flush()
        result = db.get_all_records(self.PATH)
        assert result is not None
        assert result.num_rows == 1
        assert result.column("value").to_pylist() == [2]


# ---------------------------------------------------------------------------
# 6. get_records_by_ids
# ---------------------------------------------------------------------------


class TestGetRecordsByIds:
    PATH = ("byids", "v1")

    def _populate(self, db):
        records = make_table(__record_id=["a", "b", "c"], value=[10, 20, 30])
        db.add_records(self.PATH, records, record_id_column="__record_id")
        db.flush()

    def test_retrieves_subset(self, db):
        self._populate(db)
        result = db.get_records_by_ids(self.PATH, ["a", "c"], flush=True)
        assert result is not None
        assert result.num_rows == 2

    def test_returns_none_for_missing_ids(self, db):
        self._populate(db)
        result = db.get_records_by_ids(self.PATH, ["z"], flush=True)
        assert result is None

    def test_empty_id_list_returns_none(self, db):
        self._populate(db)
        assert db.get_records_by_ids(self.PATH, [], flush=True) is None


# ---------------------------------------------------------------------------
# 7. get_records_with_column_value
# ---------------------------------------------------------------------------


class TestGetRecordsWithColumnValue:
    PATH = ("colval", "v1")

    def _populate(self, db):
        records = make_table(__record_id=["p", "q", "r"], category=["A", "B", "A"])
        db.add_records(self.PATH, records, record_id_column="__record_id")
        db.flush()

    def test_filters_by_column_value(self, db):
        self._populate(db)
        result = db.get_records_with_column_value(
            self.PATH, {"category": "A"}, flush=True
        )
        assert result is not None
        assert result.num_rows == 2

    def test_no_match_returns_none(self, db):
        self._populate(db)
        result = db.get_records_with_column_value(
            self.PATH, {"category": "Z"}, flush=True
        )
        assert result is None

    def test_accepts_mapping_and_collection_of_tuples(self, db):
        self._populate(db)
        result_mapping = db.get_records_with_column_value(
            self.PATH, {"category": "B"}, flush=True
        )
        result_tuples = db.get_records_with_column_value(
            self.PATH, [("category", "B")], flush=True
        )
        assert result_mapping is not None
        assert result_tuples is not None
        assert result_mapping.num_rows == result_tuples.num_rows


# ---------------------------------------------------------------------------
# 8. Hierarchical record_path
# ---------------------------------------------------------------------------


class TestHierarchicalPath:
    def test_deep_path_stores_and_retrieves(self, db):
        path = ("org", "project", "dataset", "v1")
        record = make_table(x=[7])
        db.add_record(path, "deep-id", record)
        db.flush()
        result = db.get_record_by_id(path, "deep-id", flush=True)
        assert result is not None
        assert result.column("x").to_pylist() == [7]

    def test_different_paths_are_independent(self, db):
        path_a = ("ns", "a")
        path_b = ("ns", "b")
        db.add_record(path_a, "id-1", make_table(v=[1]))
        db.add_record(path_b, "id-1", make_table(v=[2]))
        db.flush()
        result_a = db.get_record_by_id(path_a, "id-1", flush=True)
        result_b = db.get_record_by_id(path_b, "id-1", flush=True)
        assert result_a.column("v").to_pylist() == [1]
        assert result_b.column("v").to_pylist() == [2]

    def test_invalid_empty_path_raises(self, db):
        with pytest.raises(ValueError):
            db.add_record((), "id-1", make_table(v=[1]))

    def test_path_with_unsafe_characters_raises(self, db):
        with pytest.raises(ValueError):
            db.add_record(("bad/path",), "id-1", make_table(v=[1]))


# ---------------------------------------------------------------------------
# 9. Flush behaviour
# ---------------------------------------------------------------------------


class TestFlushBehaviour:
    PATH = ("flush", "v1")

    def test_flush_writes_pending_to_store(self, db):
        db.add_record(self.PATH, "f1", make_table(v=[1]))
        db.add_record(self.PATH, "f2", make_table(v=[2]))
        assert "flush/v1" in db._pending_batches  # records are buffered
        db.flush()
        assert "flush/v1" not in db._pending_batches  # pending cleared after flush
        result = db.get_all_records(self.PATH)
        assert result is not None
        assert result.num_rows == 2

    def test_multiple_flushes_accumulate_records(self, db):
        db.add_record(self.PATH, "m1", make_table(v=[10]))
        db.flush()
        db.add_record(self.PATH, "m2", make_table(v=[20]))
        db.flush()
        result = db.get_all_records(self.PATH)
        assert result is not None
        assert result.num_rows == 2
