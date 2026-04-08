"""Tests for _root/_scoped_path tracking on scoped database instances."""
import pytest
import pyarrow as pa
from orcapod.databases.in_memory_databases import InMemoryArrowDatabase as InMemoryDatabase
from orcapod.databases.noop_database import NoOpArrowDatabase


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
        """Scoped database still functions: write through scoped view, read back via root."""
        db = InMemoryDatabase()
        scoped = db.at("test_scope")

        # Verify routing: _path_prefix on the scoped instance matches the scope components
        assert scoped._path_prefix == ("test_scope",)

        # Write a record through the scoped view and read it back
        record = pa.table({"value": pa.array([42], type=pa.int64())})
        scoped.add_record(
            record_path=("records",),
            record_id="rec1",
            record=record,
            flush=True,
        )

        result = scoped.get_record_by_id(
            record_path=("records",),
            record_id="rec1",
        )
        assert result is not None
        assert result.column("value")[0].as_py() == 42


class TestNoOpScopedTracking:
    def test_root_instance_has_no_root(self):
        db = NoOpArrowDatabase()
        assert db._root is None

    def test_root_instance_has_empty_scoped_path(self):
        db = NoOpArrowDatabase()
        assert db._scoped_path == ()

    def test_scoped_instance_has_root(self):
        db = NoOpArrowDatabase()
        scoped = db.at("a", "b")
        assert scoped._root is db

    def test_scoped_instance_has_scoped_path(self):
        db = NoOpArrowDatabase()
        scoped = db.at("a", "b")
        assert scoped._scoped_path == ("a", "b")
