"""Tests for database to_config / from_config serialization."""

from orcapod.databases.delta_lake_databases import DeltaTableDatabase
from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
from orcapod.databases.noop_database import NoOpArrowDatabase


class TestDeltaTableDatabaseConfig:
    def test_to_config_includes_type(self, tmp_path):
        db = DeltaTableDatabase(base_path=str(tmp_path / "delta_db"))
        config = db.to_config()
        assert config["type"] == "delta_table"

    def test_to_config_includes_base_path(self, tmp_path):
        db = DeltaTableDatabase(base_path=str(tmp_path / "delta_db"))
        config = db.to_config()
        assert config["base_path"] == str(tmp_path / "delta_db")

    def test_to_config_includes_all_settings(self, tmp_path):
        db = DeltaTableDatabase(
            base_path=str(tmp_path / "delta_db"),
            batch_size=500,
            max_hierarchy_depth=5,
            allow_schema_evolution=False,
        )
        config = db.to_config()
        assert config["batch_size"] == 500
        assert config["max_hierarchy_depth"] == 5
        assert config["allow_schema_evolution"] is False

    def test_round_trip(self, tmp_path):
        db = DeltaTableDatabase(
            base_path=str(tmp_path / "delta_db"),
            batch_size=500,
            max_hierarchy_depth=5,
        )
        config = db.to_config()
        restored = DeltaTableDatabase.from_config(config)
        assert restored.to_config() == config


class TestInMemoryDatabaseConfig:
    def test_to_config_includes_type(self):
        db = InMemoryArrowDatabase()
        config = db.to_config()
        assert config["type"] == "in_memory"

    def test_round_trip(self):
        db = InMemoryArrowDatabase(max_hierarchy_depth=5)
        config = db.to_config()
        restored = InMemoryArrowDatabase.from_config(config)
        assert restored.to_config() == config

    def test_to_config_includes_base_path(self):
        db = InMemoryArrowDatabase()
        assert db.to_config()["base_path"] == []

    def test_round_trip_preserves_base_path(self):
        db = InMemoryArrowDatabase()
        scoped = db.at("pipeline", "v1")
        config = scoped.to_config()
        restored = InMemoryArrowDatabase.from_config(config)
        assert restored.base_path == ("pipeline", "v1")
        assert isinstance(restored.base_path, tuple)


def test_connector_to_config_includes_base_path():
    from unittest.mock import MagicMock
    from orcapod.databases import ConnectorArrowDatabase
    mock_connector = MagicMock()
    mock_connector.to_config.return_value = {"type": "mock"}
    db = ConnectorArrowDatabase(connector=mock_connector)
    scoped = db.at("pipeline", "v1")
    config = scoped.to_config()
    assert config["base_path"] == ["pipeline", "v1"]


class TestNoOpDatabaseConfig:
    def test_to_config_includes_type(self):
        db = NoOpArrowDatabase()
        config = db.to_config()
        assert config["type"] == "noop"

    def test_round_trip(self):
        db = NoOpArrowDatabase()
        config = db.to_config()
        restored = NoOpArrowDatabase.from_config(config)
        assert restored.to_config() == config

    def test_to_config_includes_base_path(self):
        db = NoOpArrowDatabase()
        assert db.to_config()["base_path"] == []

    def test_round_trip_preserves_base_path(self):
        db = NoOpArrowDatabase()
        scoped = db.at("pipeline", "v1")
        config = scoped.to_config()
        restored = NoOpArrowDatabase.from_config(config)
        assert restored.base_path == ("pipeline", "v1")
        assert isinstance(restored.base_path, tuple)
