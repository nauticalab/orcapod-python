"""Integration tests for SQLiteConnector with ConnectorArrowDatabase."""
from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa
import pytest

from orcapod.databases import ConnectorArrowDatabase, SQLiteConnector


@pytest.fixture
def db() -> Iterator[ConnectorArrowDatabase]:
    connector = SQLiteConnector(":memory:")
    database = ConnectorArrowDatabase(connector)
    yield database
    connector.close()


class TestConnectorArrowDatabaseWithSQLite:
    def test_add_and_get_record(self, db: ConnectorArrowDatabase) -> None:
        # add_record stamps all rows with the same __record_id, so within-batch
        # deduplication keeps only the last row (same behaviour as InMemoryArrowDatabase).
        record = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
        db.add_record(("fn", "test"), record_id="r1", record=record, flush=True)
        result = db.get_record_by_id(("fn", "test"), "r1")
        assert result is not None
        assert result.num_rows == 1
        assert result.column("x")[0].as_py() == 3  # last row kept

    def test_add_multiple_records(self, db: ConnectorArrowDatabase) -> None:
        r1 = pa.table({"x": pa.array([1], type=pa.int64())})
        r2 = pa.table({"x": pa.array([2], type=pa.int64())})
        db.add_record(("fn", "test"), record_id="r1", record=r1)
        db.add_record(("fn", "test"), record_id="r2", record=r2)
        db.flush()
        all_records = db.get_all_records(("fn", "test"))
        assert all_records is not None
        assert all_records.num_rows == 2

    def test_skip_duplicates(self, db: ConnectorArrowDatabase) -> None:
        record_v1 = pa.table({"x": pa.array([1], type=pa.int64())})
        record_v2 = pa.table({"x": pa.array([99], type=pa.int64())})
        db.add_record(("fn", "test"), record_id="r1", record=record_v1, flush=True)

        # skip_duplicates=True: must not raise, original value must be preserved
        db.add_record(("fn", "test"), record_id="r1", record=record_v2, skip_duplicates=True, flush=True)
        result = db.get_record_by_id(("fn", "test"), "r1")
        assert result is not None
        assert result.column("x")[0].as_py() == 1  # original preserved, not overwritten

        # skip_duplicates=False (default): must raise ValueError on duplicate pending batch
        db2 = ConnectorArrowDatabase(SQLiteConnector(":memory:"))
        db2.add_record(("fn", "test"), record_id="r1", record=record_v1)
        with pytest.raises(ValueError):
            db2.add_record(("fn", "test"), record_id="r1", record=record_v2)
        db2._connector.close()

    def test_second_flush_schema_consistency(self, db: ConnectorArrowDatabase) -> None:
        """ConnectorArrowDatabase validates schema on second flush — must match exactly."""
        r1 = pa.table({"x": pa.array([1], type=pa.int64())})
        r2 = pa.table({"x": pa.array([2], type=pa.int64())})
        db.add_record(("fn", "test"), record_id="r1", record=r1, flush=True)
        db.add_record(("fn", "test"), record_id="r2", record=r2, flush=True)  # second flush
        all_records = db.get_all_records(("fn", "test"))
        assert all_records is not None
        assert all_records.num_rows == 2

    def test_bool_column_roundtrip(self, db: ConnectorArrowDatabase) -> None:
        """BOOLEAN columns must survive write-flush-read without becoming int.

        add_record deduplicates all rows to the last one, so the 3-row table
        collapses to a single row with flag=True (the last value).
        """
        record = pa.table({
            "flag": pa.array([False, False, True], type=pa.bool_()),
        })
        db.add_record(("fn", "test"), record_id="r1", record=record, flush=True)
        result = db.get_record_by_id(("fn", "test"), "r1")
        assert result is not None
        flag_col = result.column("flag")
        assert flag_col.type == pa.bool_()
        assert result.num_rows == 1
        assert flag_col[0].as_py() is True  # last row kept

    def test_second_flush_schema_mismatch_raises(self, db: ConnectorArrowDatabase) -> None:
        """ConnectorArrowDatabase.flush() raises ValueError on schema mismatch."""
        r1 = pa.table({"x": pa.array([1], type=pa.int64())})
        db.add_record(("fn", "test"), record_id="r1", record=r1, flush=True)

        # Incompatible schema: different column type for 'x'
        r2 = pa.table({"x": pa.array([2.0], type=pa.float64())})
        db.add_record(("fn", "test"), record_id="r2", record=r2)
        with pytest.raises(ValueError, match="Schema mismatch"):
            db.flush()

    def test_get_records_by_ids(self, db: ConnectorArrowDatabase) -> None:
        for i in range(5):
            r = pa.table({"x": pa.array([i], type=pa.int64())})
            db.add_record(("fn", "test"), record_id=f"r{i}", record=r)
        db.flush()
        result = db.get_records_by_ids(("fn", "test"), ["r1", "r3"])
        assert result is not None
        assert result.num_rows == 2
