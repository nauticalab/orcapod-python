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
