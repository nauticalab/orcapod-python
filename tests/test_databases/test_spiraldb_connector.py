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
