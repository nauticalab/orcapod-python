"""Tests for scoped database serialization round-trips."""
import pytest
from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
from orcapod.pipeline.serialization import DatabaseRegistry, resolve_database_from_config


class TestScopedDatabaseToConfig:
    def test_root_to_config_unchanged(self):
        """Root instance emits normal format regardless of registry."""
        db = InMemoryArrowDatabase()
        registry = DatabaseRegistry()
        config = db.to_config(db_registry=registry)
        assert config["type"] != "scoped"

    def test_scoped_to_config_without_registry_emits_root_format(self):
        """Without registry, scoped instance falls back to existing format."""
        db = InMemoryArrowDatabase()
        scoped = db.at("pipeline", "node")
        config = scoped.to_config()
        assert config["type"] != "scoped"

    def test_scoped_to_config_with_registry_emits_scoped_format(self):
        """With registry, scoped instance emits compact scoped format."""
        db = InMemoryArrowDatabase()
        scoped = db.at("pipeline", "node")
        registry = DatabaseRegistry()
        config = scoped.to_config(db_registry=registry)
        assert config["type"] == "scoped"
        assert "ref" in config
        assert config["path"] == ["pipeline", "node"]

    def test_scoped_to_config_registers_root_in_registry(self):
        """Root config is stored in the registry under the ref key."""
        db = InMemoryArrowDatabase()
        scoped = db.at("pipeline", "node")
        registry = DatabaseRegistry()
        config = scoped.to_config(db_registry=registry)
        registry_dict = registry.to_dict()
        assert config["ref"] in registry_dict

    def test_double_scoped_path_is_full_path(self):
        """Double-scoped instance serializes the full path from root."""
        db = InMemoryArrowDatabase()
        scoped = db.at("pipeline").at("node")
        registry = DatabaseRegistry()
        config = scoped.to_config(db_registry=registry)
        assert config["path"] == ["pipeline", "node"]


class TestResolveFromScopedConfig:
    def test_resolve_scoped_config_returns_scoped_db(self):
        """Resolving a scoped config returns a scoped database instance."""
        db = InMemoryArrowDatabase()
        scoped_original = db.at("pipeline", "node")
        registry = DatabaseRegistry()
        scoped_config = scoped_original.to_config(db_registry=registry)
        registry_dict = registry.to_dict()

        resolved = resolve_database_from_config(scoped_config, db_registry=registry_dict)
        assert resolved._scoped_path == ("pipeline", "node")

    def test_resolve_scoped_config_has_correct_path_prefix(self):
        """Resolved scoped database has correct _path_prefix for routing."""
        db = InMemoryArrowDatabase()
        scoped_original = db.at("pipeline", "node")
        registry = DatabaseRegistry()
        scoped_config = scoped_original.to_config(db_registry=registry)
        registry_dict = registry.to_dict()

        resolved = resolve_database_from_config(scoped_config, db_registry=registry_dict)
        assert resolved._path_prefix == ("pipeline", "node")

    def test_resolve_non_scoped_config_ignores_registry(self):
        """Non-scoped config resolves normally even if registry is provided."""
        db = InMemoryArrowDatabase()
        config = db.to_config()
        resolved = resolve_database_from_config(config, db_registry={})
        assert resolved is not None
        assert resolved._scoped_path == ()

    def test_round_trip_preserves_scoped_path(self):
        """Full round-trip: serialize scoped DB, resolve, check _scoped_path."""
        db = InMemoryArrowDatabase()
        scoped = db.at("a", "b", "c")
        registry = DatabaseRegistry()
        config = scoped.to_config(db_registry=registry)
        registry_dict = registry.to_dict()

        resolved = resolve_database_from_config(config, db_registry=registry_dict)
        assert resolved._scoped_path == ("a", "b", "c")

    def test_resolve_scoped_config_without_registry_raises(self):
        """Resolving a scoped config without registry raises ValueError."""
        scoped_config = {"type": "scoped", "ref": "db_abc123", "path": ["a"]}
        with pytest.raises(ValueError, match="db_registry required"):
            resolve_database_from_config(scoped_config)
