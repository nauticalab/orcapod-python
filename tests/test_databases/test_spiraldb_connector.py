"""Tests for SpiralDBConnector — DBConnectorProtocol backed by SpiralDB."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from orcapod.databases.spiraldb_connector import SpiralDBConnector, _parse_table_name
from orcapod.protocols.db_connector_protocol import DBConnectorProtocol
from orcapod.types import ColumnInfo


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_sp():
    with patch("orcapod.databases.spiraldb_connector.sp") as mock:
        yield mock


@pytest.fixture()
def mock_project(mock_sp):
    project = MagicMock(name="MockProject")
    mock_sp.Spiral.return_value.project.return_value = project
    return project


@pytest.fixture()
def connector(mock_sp, mock_project):
    return SpiralDBConnector(project_id="test-project-123", dataset="default")


# ---------------------------------------------------------------------------
# _parse_table_name helper
# ---------------------------------------------------------------------------


class TestParseTableName:
    def test_double_quoted_identifier(self):
        assert _parse_table_name('SELECT * FROM "my_table"') == "my_table"

    def test_unquoted_fallback(self):
        assert _parse_table_name("SELECT * FROM my_table") == "my_table"

    def test_case_insensitive_from(self):
        assert _parse_table_name('select * from "signals"') == "signals"

    def test_raises_on_no_table_name(self):
        with pytest.raises(ValueError, match="Cannot parse table name"):
            _parse_table_name("SELECT 1")


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


class TestProtocolConformance:
    def test_isinstance_check_passes(self, connector):
        assert isinstance(connector, DBConnectorProtocol)


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


class TestLifecycle:
    def test_close_sets_closed_flag(self, connector):
        assert not connector._closed
        connector.close()
        assert connector._closed

    def test_double_close_is_safe(self, connector):
        connector.close()
        connector.close()  # must not raise

    def test_context_manager_closes_on_exit(self, mock_sp, mock_project):
        c = SpiralDBConnector(project_id="p", dataset="default")
        with c:
            assert not c._closed
        assert c._closed

    def test_context_manager_does_not_suppress_exceptions(self, mock_sp, mock_project):
        c = SpiralDBConnector(project_id="p", dataset="default")
        with pytest.raises(RuntimeError, match="boom"):
            with c:
                raise RuntimeError("boom")
        assert c._closed  # still closed even when exception raised

    def test_methods_raise_after_close(self, connector, mock_project):
        connector.close()
        with pytest.raises(RuntimeError, match="closed"):
            connector.get_table_names()
        with pytest.raises(RuntimeError, match="closed"):
            connector.get_pk_columns("t")
        with pytest.raises(RuntimeError, match="closed"):
            connector.get_column_info("t")
        with pytest.raises(RuntimeError, match="closed"):
            list(connector.iter_batches('SELECT * FROM "t"'))
        with pytest.raises(RuntimeError, match="closed"):
            connector.create_table_if_not_exists("t", [], "id")
        with pytest.raises(RuntimeError, match="closed"):
            connector.upsert_records("t", pa.table({"id": pa.array([], type=pa.string())}), "id")
        with pytest.raises(RuntimeError, match="closed"):
            connector.to_config()


# ---------------------------------------------------------------------------
# Schema introspection
# ---------------------------------------------------------------------------


class TestGetTableNames:
    def test_returns_sorted_plain_names_for_dataset(self, connector, mock_project):
        r1 = MagicMock(dataset="default", table="signals")
        r2 = MagicMock(dataset="other", table="signals")  # excluded — wrong dataset
        r3 = MagicMock(dataset="default", table="events")
        mock_project.list_tables.return_value = [r1, r2, r3]
        assert connector.get_table_names() == ["events", "signals"]

    def test_empty_project_returns_empty_list(self, connector, mock_project):
        mock_project.list_tables.return_value = []
        assert connector.get_table_names() == []

    def test_all_tables_in_other_dataset_returns_empty(self, connector, mock_project):
        r1 = MagicMock(dataset="other", table="foo")
        mock_project.list_tables.return_value = [r1]
        assert connector.get_table_names() == []


class TestGetPkColumns:
    def test_single_pk(self, connector, mock_project):
        mock_table = MagicMock()
        mock_table.key_schema.names = ["id"]
        mock_project.table.return_value = mock_table
        assert connector.get_pk_columns("my_table") == ["id"]
        mock_project.table.assert_called_once_with("default.my_table")

    def test_composite_pk_preserves_order(self, connector, mock_project):
        mock_table = MagicMock()
        mock_table.key_schema.names = ["session_id", "timestamp", "probe_id"]
        mock_project.table.return_value = mock_table
        assert connector.get_pk_columns("spike_data") == [
            "session_id", "timestamp", "probe_id"
        ]

    def test_empty_key_schema_returns_empty_list(self, connector, mock_project):
        mock_table = MagicMock()
        mock_table.key_schema.names = []
        mock_project.table.return_value = mock_table
        assert connector.get_pk_columns("no_key_table") == []


class TestGetColumnInfo:
    def test_arrow_types_pass_through_unchanged(self, connector, mock_project):
        arrow_schema = pa.schema([
            pa.field("id", pa.string(), nullable=False),
            pa.field("value", pa.float64(), nullable=True),
            pa.field("count", pa.int64(), nullable=True),
        ])
        mock_table = MagicMock()
        mock_table.schema.return_value.to_arrow.return_value = arrow_schema
        mock_project.table.return_value = mock_table
        result = connector.get_column_info("my_table")
        assert result == [
            ColumnInfo("id", pa.string(), nullable=False),
            ColumnInfo("value", pa.float64(), nullable=True),
            ColumnInfo("count", pa.int64(), nullable=True),
        ]
        mock_project.table.assert_called_once_with("default.my_table")

    def test_nonexistent_table_propagates_pyspiral_exception(self, connector, mock_project):
        mock_project.table.side_effect = RuntimeError("table not found")
        with pytest.raises(RuntimeError, match="table not found"):
            connector.get_column_info("nonexistent")


# ---------------------------------------------------------------------------
# iter_batches
# ---------------------------------------------------------------------------


class TestIterBatches:
    def _wire_scan(self, mock_sp, mock_project, batches: list) -> None:
        """Wire mock_sp so that scan().to_record_batches() returns ``batches``."""
        mock_project.table.return_value.select.return_value = MagicMock()
        mock_sp.Spiral.return_value.scan.return_value.to_record_batches.return_value = iter(
            batches
        )

    def test_full_scan_yields_batches(self, connector, mock_sp, mock_project):
        batch = pa.record_batch(
            {"id": pa.array(["a", "b"]), "v": pa.array([1, 2])}
        )
        self._wire_scan(mock_sp, mock_project, [batch])
        result = list(connector.iter_batches('SELECT * FROM "my_table"'))
        assert len(result) == 1
        assert result[0] == batch
        mock_project.table.assert_called_once_with("default.my_table")

    def test_empty_table_yields_no_batches(self, connector, mock_sp, mock_project):
        self._wire_scan(mock_sp, mock_project, [])
        result = list(connector.iter_batches('SELECT * FROM "empty_table"'))
        assert result == []

    def test_multiple_batches_all_yielded(self, connector, mock_sp, mock_project):
        b1 = pa.record_batch({"id": pa.array(["a"])})
        b2 = pa.record_batch({"id": pa.array(["b"])})
        self._wire_scan(mock_sp, mock_project, [b1, b2])
        result = list(connector.iter_batches('SELECT * FROM "t"'))
        assert len(result) == 2

    def test_params_not_none_emits_warning(self, connector, mock_sp, mock_project, caplog):
        import logging
        self._wire_scan(mock_sp, mock_project, [])
        with caplog.at_level(logging.WARNING):
            list(connector.iter_batches('SELECT * FROM "t"', params={"x": 1}))
        assert any("params" in msg.lower() for msg in caplog.messages)

    def test_batch_size_passed_to_spiral(self, connector, mock_sp, mock_project):
        self._wire_scan(mock_sp, mock_project, [])
        list(connector.iter_batches('SELECT * FROM "t"', batch_size=500))
        mock_sp.Spiral.return_value.scan.return_value.to_record_batches.assert_called_with(
            batch_size=500
        )

    def test_nonexistent_table_propagates_exception(self, connector, mock_sp, mock_project):
        mock_project.table.side_effect = RuntimeError("table not found")
        with pytest.raises(RuntimeError, match="table not found"):
            list(connector.iter_batches('SELECT * FROM "missing"'))

    def test_tbl_select_called_for_full_scan(self, connector, mock_sp, mock_project):
        self._wire_scan(mock_sp, mock_project, [])
        list(connector.iter_batches('SELECT * FROM "t"'))
        mock_project.table.return_value.select.assert_called_once_with()


# ---------------------------------------------------------------------------
# create_table_if_not_exists
# ---------------------------------------------------------------------------


class TestCreateTableIfNotExists:
    def test_creates_table_with_pk_key_schema(self, connector, mock_project):
        columns = [
            ColumnInfo("id", pa.string(), nullable=False),
            ColumnInfo("value", pa.float64(), nullable=True),
        ]
        connector.create_table_if_not_exists("my_table", columns, pk_column="id")
        mock_project.create_table.assert_called_once_with(
            "default.my_table",
            key_schema=[("id", pa.string())],
            exist_ok=True,
        )

    def test_exist_ok_always_true(self, connector, mock_project):
        columns = [ColumnInfo("id", pa.int64(), nullable=False)]
        connector.create_table_if_not_exists("t", columns, pk_column="id")
        connector.create_table_if_not_exists("t", columns, pk_column="id")
        assert mock_project.create_table.call_count == 2
        for call_args in mock_project.create_table.call_args_list:
            assert call_args.kwargs.get("exist_ok") is True

    def test_raises_if_pk_column_not_in_columns(self, connector, mock_project):
        columns = [ColumnInfo("id", pa.string(), nullable=False)]
        with pytest.raises(ValueError, match="pk_column"):
            connector.create_table_if_not_exists("t", columns, pk_column="missing_col")

    def test_raises_if_columns_empty(self, connector, mock_project):
        with pytest.raises(ValueError, match="pk_column"):
            connector.create_table_if_not_exists("t", columns=[], pk_column="id")

    def test_non_pk_columns_not_included_in_key_schema(self, connector, mock_project):
        columns = [
            ColumnInfo("id", pa.string(), nullable=False),
            ColumnInfo("value", pa.float64()),
            ColumnInfo("label", pa.string()),
        ]
        connector.create_table_if_not_exists("t", columns, pk_column="id")
        call_kwargs = mock_project.create_table.call_args.kwargs
        assert call_kwargs["key_schema"] == [("id", pa.string())]


# ---------------------------------------------------------------------------
# upsert_records
# ---------------------------------------------------------------------------


class TestUpsertRecords:
    def _make_mock_table(self, mock_project, pk_cols: list[str]) -> MagicMock:
        mock_table = MagicMock()
        mock_table.key_schema.names = pk_cols
        mock_project.table.return_value = mock_table
        return mock_table

    # --- skip_existing=False (default: always upsert-by-key) ----------------

    def test_write_called_with_full_records(self, connector, mock_project):
        mock_table = self._make_mock_table(mock_project, ["id"])
        records = pa.table({"id": ["a", "b"], "value": [1.0, 2.0]})
        connector.upsert_records("my_table", records, id_column="id")
        mock_table.write.assert_called_once_with(records)

    def test_raises_if_id_column_not_in_key_schema(self, connector, mock_project):
        self._make_mock_table(mock_project, ["id"])
        records = pa.table({"id": ["a"], "value": [1.0]})
        with pytest.raises(ValueError, match="id_column"):
            connector.upsert_records("my_table", records, id_column="wrong_col")

    def test_raises_if_pk_column_missing_from_records(self, connector, mock_project):
        self._make_mock_table(mock_project, ["id", "session_id"])
        records = pa.table({"id": ["a"]})  # missing session_id
        with pytest.raises(ValueError, match="missing key column"):
            connector.upsert_records("my_table", records, id_column="id")

    def test_raises_if_empty_key_schema(self, connector, mock_project):
        # id_column can never be `in []`, so this always raises ValueError
        self._make_mock_table(mock_project, [])
        records = pa.table({"id": ["a"]})
        with pytest.raises(ValueError, match="id_column"):
            connector.upsert_records("my_table", records, id_column="id")

    # --- skip_existing=True (scan + filter + write novel rows only) ----------

    def test_skip_existing_writes_only_novel_rows(self, connector, mock_sp, mock_project):
        mock_table = self._make_mock_table(mock_project, ["id"])
        existing = pa.table({"id": ["a"], "value": [1.0]})
        new_records = pa.table({"id": ["a", "b"], "value": [1.0, 2.0]})
        mock_sp.Spiral.return_value.scan.return_value.to_table.return_value = existing

        connector.upsert_records("my_table", new_records, id_column="id", skip_existing=True)

        written = mock_table.write.call_args[0][0]
        assert written.column("id").to_pylist() == ["b"]

    def test_skip_existing_noop_when_all_rows_exist(self, connector, mock_sp, mock_project):
        mock_table = self._make_mock_table(mock_project, ["id"])
        existing = pa.table({"id": ["a", "b"], "value": [1.0, 2.0]})
        new_records = pa.table({"id": ["a", "b"], "value": [10.0, 20.0]})
        mock_sp.Spiral.return_value.scan.return_value.to_table.return_value = existing

        connector.upsert_records("my_table", new_records, id_column="id", skip_existing=True)

        mock_table.write.assert_not_called()

    def test_skip_existing_composite_pk(self, connector, mock_sp, mock_project):
        mock_table = self._make_mock_table(mock_project, ["session_id", "ts"])
        existing = pa.table({
            "session_id": pa.array(["s1"]),
            "ts": pa.array([1], type=pa.int64()),
            "v": pa.array([10.0]),
        })
        new_records = pa.table({
            "session_id": pa.array(["s1", "s1"]),
            "ts": pa.array([1, 2], type=pa.int64()),  # (s1,1) exists; (s1,2) is novel
            "v": pa.array([99.0, 5.0]),
        })
        mock_sp.Spiral.return_value.scan.return_value.to_table.return_value = existing

        connector.upsert_records("my_table", new_records, id_column="session_id", skip_existing=True)

        written = mock_table.write.call_args[0][0]
        assert written.column("ts").to_pylist() == [2]

    def test_skip_existing_raises_if_id_column_not_in_key_schema(self, connector, mock_project):
        self._make_mock_table(mock_project, ["id"])
        records = pa.table({"id": ["a"]})
        with pytest.raises(ValueError, match="id_column"):
            connector.upsert_records("t", records, id_column="wrong", skip_existing=True)

    def test_skip_existing_raises_if_pk_missing_from_records(self, connector, mock_project):
        self._make_mock_table(mock_project, ["id", "ts"])
        records = pa.table({"id": ["a"]})  # missing ts
        with pytest.raises(ValueError, match="missing key column"):
            connector.upsert_records("t", records, id_column="id", skip_existing=True)
