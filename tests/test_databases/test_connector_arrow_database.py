"""
Comprehensive tests for ColumnInfo, DBConnectorProtocol, and ConnectorArrowDatabase.

Structure mirrors test_in_memory_database.py so the two databases have
symmetric coverage. All tests use a MockDBConnector — no external DB required.

Test sections:
 1. ColumnInfo dataclass
 2. DBConnectorProtocol structural conformance (via MockDBConnector)
 3. ConnectorArrowDatabase — protocol conformance
 4. Empty-table cases
 5. add_record / get_record_by_id round-trip
 6. add_records / get_all_records
 7. Duplicate handling
 8. get_records_by_ids
 9. get_records_with_column_value
10. Hierarchical record_path + _path_to_table_name
11. Flush behaviour (pending cleared, connector receives data)
12. Config (to_config shape, from_config raises NotImplementedError)
13. Context-manager lifecycle (connector.close is called)
"""
from __future__ import annotations

import re
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pytest

# ---------------------------------------------------------------------------
# Imports under test — all of these will fail until the modules are created
# ---------------------------------------------------------------------------

from orcapod.protocols.db_connector_protocol import ColumnInfo, DBConnectorProtocol
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.databases import ConnectorArrowDatabase


# ---------------------------------------------------------------------------
# MockDBConnector — in-memory DBConnectorProtocol for tests
# ---------------------------------------------------------------------------


class MockDBConnector:
    """Pure in-memory implementation of DBConnectorProtocol for testing.

    Holds data as ``dict[str, pa.Table]``.  ``iter_batches`` parses the
    table name from a bare ``SELECT * FROM "table_name"`` query.
    ``upsert_records`` implements insert-or-replace / insert-or-ignore
    semantics in memory, mirroring what a real connector would do in SQL.
    """

    def __init__(
        self,
        tables: dict[str, pa.Table] | None = None,
        pk_columns: dict[str, list[str]] | None = None,
    ):
        self._tables: dict[str, pa.Table] = dict(tables or {})
        self._pk_columns: dict[str, list[str]] = dict(pk_columns or {})
        self.close_called = False

    # ── Schema introspection ──────────────────────────────────────────────────

    def get_table_names(self) -> list[str]:
        return list(self._tables.keys())

    def get_pk_columns(self, table_name: str) -> list[str]:
        return list(self._pk_columns.get(table_name, []))

    def get_column_info(self, table_name: str) -> list[ColumnInfo]:
        schema = self._tables[table_name].schema
        return [
            ColumnInfo(name=f.name, arrow_type=f.type, nullable=f.nullable)
            for f in schema
        ]

    # ── Read ──────────────────────────────────────────────────────────────────

    def iter_batches(
        self, query: str, params: Any = None, batch_size: int = 1000
    ) -> Iterator[pa.RecordBatch]:
        match = re.search(r'FROM\s+"?(\w+)"?', query, re.IGNORECASE)
        if not match:
            return
        table_name = match.group(1)
        table = self._tables.get(table_name)
        if table is None or table.num_rows == 0:
            return
        for batch in table.to_batches(max_chunksize=batch_size):
            yield batch

    # ── Write ─────────────────────────────────────────────────────────────────

    def create_table_if_not_exists(
        self, table_name: str, columns: list[ColumnInfo], pk_column: str
    ) -> None:
        if table_name not in self._tables:
            self._tables[table_name] = pa.table(
                {c.name: pa.array([], type=c.arrow_type) for c in columns}
            )
            self._pk_columns.setdefault(table_name, [pk_column])

    def upsert_records(
        self,
        table_name: str,
        records: pa.Table,
        id_column: str,
        skip_existing: bool = False,
    ) -> None:
        existing = self._tables.get(table_name)
        if existing is None or existing.num_rows == 0:
            self._tables[table_name] = records
            return
        new_ids = set(records[id_column].to_pylist())
        if skip_existing:
            existing_ids = set(existing[id_column].to_pylist())
            already_there = new_ids & existing_ids
            mask = pc.invert(
                pc.is_in(records[id_column], pa.array(list(already_there)))
            )
            to_add = records.filter(mask)
            if to_add.num_rows > 0:
                self._tables[table_name] = pa.concat_tables([existing, to_add])
        else:
            # INSERT OR REPLACE: remove old rows with matching id, then append
            mask = pc.invert(pc.is_in(existing[id_column], pa.array(list(new_ids))))
            kept = existing.filter(mask)
            self._tables[table_name] = pa.concat_tables([kept, records])

    # ── Lifecycle ─────────────────────────────────────────────────────────────

    def close(self) -> None:
        self.close_called = True

    def __enter__(self) -> "MockDBConnector":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()

    # ── Serialization ─────────────────────────────────────────────────────────

    def to_config(self) -> dict[str, Any]:
        return {"connector_type": "mock"}

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "MockDBConnector":
        return cls()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_table(**columns: list) -> pa.Table:
    """Build a small PyArrow table from keyword column lists."""
    return pa.table({k: pa.array(v) for k, v in columns.items()})


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def connector():
    return MockDBConnector()


@pytest.fixture
def db(connector):
    return ConnectorArrowDatabase(connector)


# ===========================================================================
# 1. ColumnInfo dataclass
# ===========================================================================


class TestColumnInfo:
    def test_basic_construction(self):
        col = ColumnInfo(name="my_col", arrow_type=pa.int64())
        assert col.name == "my_col"
        assert col.arrow_type == pa.int64()
        assert col.nullable is True  # default

    def test_nullable_false(self):
        col = ColumnInfo(name="pk", arrow_type=pa.large_string(), nullable=False)
        assert col.nullable is False

    def test_frozen_immutable(self):
        col = ColumnInfo(name="x", arrow_type=pa.float32())
        with pytest.raises((AttributeError, TypeError)):
            col.name = "y"  # type: ignore[misc]

    def test_equality(self):
        a = ColumnInfo(name="v", arrow_type=pa.int32())
        b = ColumnInfo(name="v", arrow_type=pa.int32())
        assert a == b

    def test_inequality_different_type(self):
        a = ColumnInfo(name="v", arrow_type=pa.int32())
        b = ColumnInfo(name="v", arrow_type=pa.int64())
        assert a != b


# ===========================================================================
# 2. DBConnectorProtocol structural conformance
# ===========================================================================


class TestDBConnectorProtocolConformance:
    """MockDBConnector must satisfy DBConnectorProtocol structural checks."""

    def test_mock_satisfies_protocol(self, connector):
        assert isinstance(connector, DBConnectorProtocol)

    def test_has_get_table_names(self, connector):
        assert callable(connector.get_table_names)

    def test_has_get_pk_columns(self, connector):
        assert callable(connector.get_pk_columns)

    def test_has_get_column_info(self, connector):
        assert callable(connector.get_column_info)

    def test_has_iter_batches(self, connector):
        assert callable(connector.iter_batches)

    def test_has_create_table_if_not_exists(self, connector):
        assert callable(connector.create_table_if_not_exists)

    def test_has_upsert_records(self, connector):
        assert callable(connector.upsert_records)

    def test_has_close(self, connector):
        assert callable(connector.close)

    def test_has_to_config(self, connector):
        assert callable(connector.to_config)

    def test_to_config_has_connector_type_key(self, connector):
        config = connector.to_config()
        assert "connector_type" in config

    def test_context_manager_calls_close(self, connector):
        assert not connector.close_called
        with connector:
            pass
        assert connector.close_called


# ===========================================================================
# 3. ConnectorArrowDatabase — protocol conformance
# ===========================================================================


class TestConnectorArrowDatabaseConformance:
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

    def test_has_to_config(self, db):
        assert callable(db.to_config)

    def test_has_from_config(self, db):
        assert callable(db.from_config)


# ===========================================================================
# 4. Empty-table cases
# ===========================================================================


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


# ===========================================================================
# 5. add_record / get_record_by_id round-trip
# ===========================================================================


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
        assert ConnectorArrowDatabase.RECORD_ID_COLUMN not in result.column_names

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

    def test_multi_row_record_deduplicates_to_last_row(self, db):
        # add_record stamps ALL rows with the same __record_id value, so
        # within-batch deduplication (keep-last) leaves a single row.
        # This mirrors InMemoryArrowDatabase behaviour by design.
        record = make_table(x=[1, 2, 3])
        db.add_record(self.PATH, "multi-row", record)
        db.flush()
        result = db.get_record_by_id(self.PATH, "multi-row", flush=True)
        assert result is not None
        assert result.num_rows == 1
        assert result.column("x").to_pylist() == [3]  # last row kept


# ===========================================================================
# 6. add_records / get_all_records
# ===========================================================================


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

    def test_pending_and_committed_combined(self, db):
        """Records added before and after flush should both appear in get_all_records."""
        db.add_records(
            self.PATH,
            make_table(__record_id=["a"], v=[1]),
            record_id_column="__record_id",
            flush=True,
        )
        db.add_records(
            self.PATH,
            make_table(__record_id=["b"], v=[2]),
            record_id_column="__record_id",
        )
        result = db.get_all_records(self.PATH)
        assert result is not None
        assert result.num_rows == 2

    def test_empty_records_table_is_ignored(self, db):
        empty = make_table(__record_id=[], v=[])
        db.add_records(self.PATH, empty, record_id_column="__record_id")
        assert db.get_all_records(self.PATH) is None


# ===========================================================================
# 7. Duplicate handling
# ===========================================================================


class TestDuplicateHandling:
    PATH = ("dup", "v1")

    def test_skip_duplicates_true_does_not_raise(self, db):
        db.add_record(self.PATH, "dup-id", make_table(value=[1]))
        db.flush()
        # same id again with skip_duplicates=True — should silently skip
        db.add_record(self.PATH, "dup-id", make_table(value=[2]), skip_duplicates=True)

    def test_skip_duplicates_preserves_original_value(self, db):
        db.add_record(self.PATH, "dup-id", make_table(value=[1]), flush=True)
        db.add_record(
            self.PATH, "dup-id", make_table(value=[99]), skip_duplicates=True, flush=True
        )
        result = db.get_record_by_id(self.PATH, "dup-id", flush=True)
        assert result is not None
        assert result.column("value").to_pylist() == [1]  # original preserved

    def test_skip_duplicates_false_raises_on_pending_duplicate(self, db):
        db.add_record(self.PATH, "dup-id2", make_table(value=[1]))
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


# ===========================================================================
# 8. get_records_by_ids
# ===========================================================================


class TestGetRecordsByIds:
    PATH = ("byids", "v1")

    @pytest.fixture(autouse=True)
    def populate(self, db):
        records = make_table(__record_id=["a", "b", "c"], value=[10, 20, 30])
        db.add_records(self.PATH, records, record_id_column="__record_id")
        db.flush()

    def test_retrieves_subset(self, db):
        result = db.get_records_by_ids(self.PATH, ["a", "c"], flush=True)
        assert result is not None
        assert result.num_rows == 2

    def test_returns_none_for_missing_ids(self, db):
        result = db.get_records_by_ids(self.PATH, ["z"], flush=True)
        assert result is None

    def test_empty_id_list_returns_none(self, db):
        assert db.get_records_by_ids(self.PATH, [], flush=True) is None

    def test_retrieves_single_id(self, db):
        result = db.get_records_by_ids(self.PATH, ["b"], flush=True)
        assert result is not None
        assert result.num_rows == 1


# ===========================================================================
# 9. get_records_with_column_value
# ===========================================================================


class TestGetRecordsWithColumnValue:
    PATH = ("colval", "v1")

    @pytest.fixture(autouse=True)
    def populate(self, db):
        records = make_table(
            __record_id=["p", "q", "r"], category=["A", "B", "A"]
        )
        db.add_records(self.PATH, records, record_id_column="__record_id")
        db.flush()

    def test_filters_by_column_value(self, db):
        result = db.get_records_with_column_value(
            self.PATH, {"category": "A"}, flush=True
        )
        assert result is not None
        assert result.num_rows == 2

    def test_no_match_returns_none(self, db):
        result = db.get_records_with_column_value(
            self.PATH, {"category": "Z"}, flush=True
        )
        assert result is None

    def test_accepts_mapping_and_collection_of_tuples(self, db):
        result_mapping = db.get_records_with_column_value(
            self.PATH, {"category": "B"}, flush=True
        )
        result_tuples = db.get_records_with_column_value(
            self.PATH, [("category", "B")], flush=True
        )
        assert result_mapping is not None and result_tuples is not None
        assert result_mapping.num_rows == result_tuples.num_rows


# ===========================================================================
# 10. Hierarchical record_path + _path_to_table_name
# ===========================================================================


class TestHierarchicalPath:
    def test_deep_path_stores_and_retrieves(self, db):
        path = ("org", "project", "dataset", "v1")
        db.add_record(path, "deep-id", make_table(x=[7]))
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

    def test_path_exceeding_max_depth_raises(self, db):
        path = tuple(f"part{i}" for i in range(db.max_hierarchy_depth + 1))
        with pytest.raises(ValueError, match="exceeds maximum"):
            db.add_record(path, "id-1", make_table(v=[1]))


class TestPathToTableName:
    def test_simple_single_component(self, db):
        assert db._path_to_table_name(("results",)) == "results"

    def test_two_components_joined_with_double_underscore(self, db):
        assert db._path_to_table_name(("results", "my_fn")) == "results__my_fn"

    def test_special_chars_replaced_with_underscore(self, db):
        name = db._path_to_table_name(("a:b", "c/d"))
        assert "__" in name
        assert ":" not in name
        assert "/" not in name

    def test_digit_prefix_gets_t_prefix(self, db):
        name = db._path_to_table_name(("1abc",))
        assert name.startswith("t_")

    def test_digit_prefix_only_when_first_char_is_digit(self, db):
        name = db._path_to_table_name(("abc1",))
        assert not name.startswith("t_")

    def test_same_path_always_yields_same_table_name(self, db):
        path = ("foo", "bar", "baz")
        assert db._path_to_table_name(path) == db._path_to_table_name(path)


# ===========================================================================
# 11. Flush behaviour
# ===========================================================================


class TestFlushBehaviour:
    PATH = ("flush", "v1")

    def test_flush_writes_pending_to_connector(self, db, connector):
        db.add_record(self.PATH, "f1", make_table(v=[1]))
        db.add_record(self.PATH, "f2", make_table(v=[2]))
        # pending key exists before flush
        record_key = db._get_record_key(self.PATH)
        assert record_key in db._pending_batches
        db.flush()
        # pending cleared after flush
        assert record_key not in db._pending_batches
        # connector now has the table
        table_name = db._path_to_table_name(self.PATH)
        assert table_name in connector.get_table_names()

    def test_flush_inline_via_flush_kwarg(self, db, connector):
        db.add_record(self.PATH, "x", make_table(v=[5]), flush=True)
        table_name = db._path_to_table_name(self.PATH)
        assert table_name in connector.get_table_names()

    def test_multiple_flushes_accumulate_records(self, db):
        db.add_record(self.PATH, "m1", make_table(v=[10]))
        db.flush()
        db.add_record(self.PATH, "m2", make_table(v=[20]))
        db.flush()
        result = db.get_all_records(self.PATH)
        assert result is not None
        assert result.num_rows == 2

    def test_second_flush_on_existing_table_upserts(self, db):
        """Flushing the same path twice should not duplicate rows."""
        db.add_record(self.PATH, "u1", make_table(v=[1]), flush=True)
        db.add_record(self.PATH, "u1", make_table(v=[99]), skip_duplicates=True, flush=True)
        result = db.get_all_records(self.PATH)
        assert result is not None
        # skip_existing=True means original is preserved, row count stays 1
        assert result.num_rows == 1

    def test_noop_flush_does_not_raise(self, db):
        """Flushing with nothing pending should be a no-op."""
        db.flush()  # no error


# ===========================================================================
# 12. Config
# ===========================================================================


class TestConfig:
    def test_to_config_has_type_key(self, db):
        config = db.to_config()
        assert config.get("type") == "connector_arrow_database"

    def test_to_config_includes_connector_config(self, db):
        config = db.to_config()
        assert "connector" in config
        assert config["connector"]["connector_type"] == "mock"

    def test_to_config_includes_max_hierarchy_depth(self, db):
        config = db.to_config()
        assert "max_hierarchy_depth" in config

    def test_from_config_raises_not_implemented(self, db):
        config = db.to_config()
        with pytest.raises(NotImplementedError):
            ConnectorArrowDatabase.from_config(config)
