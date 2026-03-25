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
