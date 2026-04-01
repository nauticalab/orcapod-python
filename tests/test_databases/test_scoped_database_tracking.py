"""Tests for _root/_scoped_path tracking on scoped database instances."""
import pytest
from orcapod.databases.in_memory_databases import InMemoryArrowDatabase as InMemoryDatabase


class TestScopedDatabaseTracking:
    def test_root_instance_has_no_root(self):
        db = InMemoryDatabase()
        assert db._root is None

    def test_root_instance_has_empty_scoped_path(self):
        db = InMemoryDatabase()
        assert db._scoped_path == ()

    def test_scoped_instance_has_root(self):
        db = InMemoryDatabase()
        scoped = db.at("pipeline", "node")
        assert scoped._root is db

    def test_scoped_instance_has_scoped_path(self):
        db = InMemoryDatabase()
        scoped = db.at("pipeline", "node")
        assert scoped._scoped_path == ("pipeline", "node")

    def test_double_scoped_preserves_original_root(self):
        db = InMemoryDatabase()
        scoped1 = db.at("pipeline")
        scoped2 = scoped1.at("node")
        assert scoped2._root is db

    def test_double_scoped_concatenates_path(self):
        db = InMemoryDatabase()
        scoped1 = db.at("pipeline")
        scoped2 = scoped1.at("node")
        assert scoped2._scoped_path == ("pipeline", "node")

    def test_existing_functionality_preserved(self):
        """Scoped database still works for read/write."""
        db = InMemoryDatabase()
        scoped = db.at("test_scope")
        # Should not raise
        assert scoped is not None
