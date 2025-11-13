"""Tests for SQLiteCacher."""

import sqlite3
import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

from orcapod.hashing.string_cachers import SQLiteCacher


def test_basic_operations():
    """Test basic get/set/clear operations."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_file = Path(temp_dir) / "test_cache.db"
        cacher = SQLiteCacher(db_file, sync_probability=1.0)

        # Test empty cache
        assert cacher.get_cached("nonexistent") is None

        # Test set and get
        cacher.set_cached("key1", "value1")
        assert cacher.get_cached("key1") == "value1"

        # Test overwrite
        cacher.set_cached("key1", "new_value1")
        assert cacher.get_cached("key1") == "new_value1"

        # Test multiple keys
        cacher.set_cached("key2", "value2")
        assert cacher.get_cached("key1") == "new_value1"
        assert cacher.get_cached("key2") == "value2"

        # Test clear
        cacher.clear_cache()
        assert cacher.get_cached("key1") is None
        assert cacher.get_cached("key2") is None


def test_database_initialization():
    """Test that database schema is created correctly."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_file = Path(temp_dir) / "schema_test.db"
        cacher = SQLiteCacher(db_file)  # noqa: F841

        # Check that table exists with correct schema
        with sqlite3.connect(db_file) as conn:
            cursor = conn.execute("""
                SELECT sql FROM sqlite_master
                WHERE type='table' AND name='cache_entries'
            """)
            schema = cursor.fetchone()[0]

            assert "key TEXT PRIMARY KEY" in schema
            assert "value TEXT NOT NULL" in schema
            assert "last_accessed REAL" in schema

            # Check that index exists
            cursor = conn.execute("""
                SELECT name FROM sqlite_master
                WHERE type='index' AND name='idx_last_accessed'
            """)
            assert cursor.fetchone() is not None


def test_persistence_across_instances():
    """Test that data persists across different cacher instances."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_file = Path(temp_dir) / "persistent_cache.db"

        # First instance
        cacher1 = SQLiteCacher(db_file, sync_probability=1.0)
        cacher1.set_cached("key1", "value1")
        cacher1.set_cached("key2", "value2")
        cacher1.force_sync()

        # Second instance should load existing data
        cacher2 = SQLiteCacher(db_file)
        assert cacher2.get_cached("key1") == "value1"
        assert cacher2.get_cached("key2") == "value2"

        # Add more data in second instance
        cacher2.set_cached("key3", "value3")
        cacher2.force_sync()

        # Third instance should see all data
        cacher3 = SQLiteCacher(db_file)
        assert cacher3.get_cached("key1") == "value1"
        assert cacher3.get_cached("key2") == "value2"
        assert cacher3.get_cached("key3") == "value3"


def test_memory_to_database_fallback():
    """Test loading from database when not in memory cache."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_file = Path(temp_dir) / "fallback_cache.db"

        # Create cacher with small memory cache
        cacher = SQLiteCacher(db_file, max_size=2, sync_probability=1.0)

        # Add items beyond memory capacity
        cacher.set_cached("key1", "value1")
        cacher.set_cached("key2", "value2")
        cacher.set_cached("key3", "value3")  # This should evict key1 from memory

        # key1 should not be in memory but should be retrievable from database
        assert cacher.get_cached("key1") == "value1"  # Loaded from DB
        assert cacher.get_cached("key2") == "value2"  # In memory
        assert cacher.get_cached("key3") == "value3"  # In memory


def test_lru_behavior_with_database_loading():
    """Test LRU behavior when loading items from database."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_file = Path(temp_dir) / "lru_db_cache.db"
        cacher = SQLiteCacher(db_file, max_size=2, sync_probability=1.0)

        # Fill memory cache
        cacher.set_cached("key1", "value1")
        cacher.set_cached("key2", "value2")

        # Add third item (evicts key1 from memory)
        cacher.set_cached("key3", "value3")

        # Access key1 from database (should load into memory, evicting key2)
        assert cacher.get_cached("key1") == "value1"

        # Now key2 should need to be loaded from database
        assert cacher.get_cached("key2") == "value2"


def test_sync_probability():
    """Test probabilistic syncing behavior."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_file = Path(temp_dir) / "prob_cache.db"

        # Test with 0% sync probability
        cacher = SQLiteCacher(db_file, sync_probability=0.0)
        cacher.set_cached("key1", "value1")

        # Data should be in memory but not in database yet
        assert cacher.get_cached("key1") == "value1"

        # Check database directly - should be empty
        with sqlite3.connect(db_file) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM cache_entries")
            count = cursor.fetchone()[0]
            assert count == 0

        # Force sync should write to database
        cacher.force_sync()

        with sqlite3.connect(db_file) as conn:
            cursor = conn.execute(
                "SELECT value FROM cache_entries WHERE key = ?", ("key1",)
            )
            result = cursor.fetchone()
            assert result[0] == "value1"


def test_timestamp_updates():
    """Test that timestamps are updated correctly."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_file = Path(temp_dir) / "timestamp_cache.db"
        cacher = SQLiteCacher(db_file, sync_probability=1.0)

        # Add item
        cacher.set_cached("key1", "value1")

        # Get initial timestamp
        with sqlite3.connect(db_file) as conn:
            cursor = conn.execute(
                "SELECT last_accessed FROM cache_entries WHERE key = ?", ("key1",)
            )
            initial_time = cursor.fetchone()[0]

        # Wait a bit and access the key
        time.sleep(0.1)
        cacher.get_cached("key1")
        cacher.force_sync()

        # Check that timestamp was updated
        with sqlite3.connect(db_file) as conn:
            cursor = conn.execute(
                "SELECT last_accessed FROM cache_entries WHERE key = ?", ("key1",)
            )
            new_time = cursor.fetchone()[0]
            assert new_time > initial_time


def test_database_error_handling():
    """Test handling of database errors."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_file = Path(temp_dir) / "error_cache.db"
        cacher = SQLiteCacher(db_file)

        # Mock database operations to raise errors
        with patch("sqlite3.connect") as mock_connect:
            mock_conn = MagicMock()
            mock_connect.return_value.__enter__.return_value = mock_conn
            mock_conn.execute.side_effect = sqlite3.Error("Database error")

            with patch("logging.error") as mock_log:
                # Should handle database errors gracefully
                result = cacher.get_cached("any_key")
                assert result is None
                mock_log.assert_called_once()


def test_clear_cache_removes_from_database():
    """Test that clear_cache removes data from database."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_file = Path(temp_dir) / "clear_cache.db"
        cacher = SQLiteCacher(db_file, sync_probability=1.0)

        # Add some data
        cacher.set_cached("key1", "value1")
        cacher.set_cached("key2", "value2")

        # Verify data exists in database
        with sqlite3.connect(db_file) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM cache_entries")
            count = cursor.fetchone()[0]
            assert count == 2

        # Clear cache
        cacher.clear_cache()

        # Verify database is empty
        with sqlite3.connect(db_file) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM cache_entries")
            count = cursor.fetchone()[0]
            assert count == 0

        # Verify memory cache is empty
        assert cacher.get_cached("key1") is None
        assert cacher.get_cached("key2") is None


def test_thread_safety():
    """Test thread safety of database operations."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_file = Path(temp_dir) / "thread_safe_cache.db"
        cacher = SQLiteCacher(db_file, max_size=50, sync_probability=0.2)

        results = {}
        errors = []

        def worker(thread_id: int):
            try:
                for i in range(20):
                    key = f"thread{thread_id}_key{i}"
                    value = f"thread{thread_id}_value{i}"
                    cacher.set_cached(key, value)

                    # Mix of memory and database reads
                    if i % 3 == 0:
                        cacher.force_sync()

                # Verify data
                thread_results = []
                for i in range(20):
                    key = f"thread{thread_id}_key{i}"
                    result = cacher.get_cached(key)
                    thread_results.append(result)

                results[thread_id] = thread_results

            except Exception as e:
                errors.append(e)

        # Start multiple threads
        threads = []
        for i in range(3):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        # Wait for completion
        for t in threads:
            t.join()

        # Check for errors
        assert not errors, f"Thread safety errors: {errors}"

        # Final sync
        cacher.force_sync()


def test_loading_respects_memory_limit():
    """Test that loading from database respects memory cache limit."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_file = Path(temp_dir) / "memory_limit_cache.db"

        # First, populate database with many items
        cacher1 = SQLiteCacher(
            db_file, max_size=None, sync_probability=1.0
        )  # Unlimited memory
        for i in range(100):
            cacher1.set_cached(f"key{i}", f"value{i}")
        cacher1.force_sync()

        # Second instance with limited memory
        cacher2 = SQLiteCacher(db_file, max_size=10)

        # Should only load 10 items into memory (respecting limit)
        # But should be able to access any item from database
        assert cacher2.get_cached("key5") == "value5"  # Should work
        assert cacher2.get_cached("key95") == "value95"  # Should work


def test_directory_creation():
    """Test that parent directories are created for database file."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create nested path that doesn't exist
        db_file = Path(temp_dir) / "nested" / "dirs" / "cache.db"

        cacher = SQLiteCacher(db_file)
        cacher.set_cached("key1", "value1")

        # Database file and directories should be created
        assert db_file.exists()
        assert db_file.parent.exists()


def test_force_sync_with_dirty_keys():
    """Test that force_sync only syncs dirty keys."""
    with tempfile.TemporaryDirectory() as temp_dir:
        db_file = Path(temp_dir) / "dirty_sync_cache.db"
        cacher = SQLiteCacher(db_file, sync_probability=0.0)  # Never auto-sync

        # Add data (should be dirty)
        cacher.set_cached("key1", "value1")
        cacher.set_cached("key2", "value2")

        # Force sync
        cacher.force_sync()

        # Verify data is in database
        with sqlite3.connect(db_file) as conn:
            cursor = conn.execute("SELECT key, value FROM cache_entries ORDER BY key")
            rows = cursor.fetchall()
            assert len(rows) == 2
            assert rows[0] == ("key1", "value1")
            assert rows[1] == ("key2", "value2")

        # Add more data after sync
        cacher.set_cached("key3", "value3")

        # Another force sync should only add the new key
        cacher.force_sync()

        with sqlite3.connect(db_file) as conn:
            cursor = conn.execute("SELECT COUNT(*) FROM cache_entries")
            count = cursor.fetchone()[0]
            assert count == 3
