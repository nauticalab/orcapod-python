"""Tests for InMemoryHashCacher and SqliteHashCacher."""

import sqlite3
from pathlib import Path

import pytest
from upath import UPath

from orcapod.hashing.file_hashers import FileHashKey
from orcapod.types import ContentHash

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_key(path_str: str, mtime_ns: int = 1000, size: int = 100) -> FileHashKey:
    return FileHashKey(path=UPath(path_str), mtime_ns=mtime_ns, size=size)


def make_hash(method: str = "sha256", digest: bytes = b"\xab" * 32) -> ContentHash:
    return ContentHash(method=method, digest=digest)


# ---------------------------------------------------------------------------
# InMemoryHashCacher
# ---------------------------------------------------------------------------


class TestInMemoryHashCacher:
    def test_miss_returns_none(self):
        from orcapod.hashing.hash_cachers import InMemoryHashCacher
        cacher = InMemoryHashCacher()
        assert cacher.get(make_key("/a/b.txt")) is None

    def test_put_then_get_returns_hit(self):
        from orcapod.hashing.hash_cachers import InMemoryHashCacher
        cacher = InMemoryHashCacher()
        key = make_key("/a/b.txt")
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) == value

    def test_different_mtime_is_different_key(self):
        from orcapod.hashing.hash_cachers import InMemoryHashCacher
        cacher = InMemoryHashCacher()
        key1 = make_key("/a/b.txt", mtime_ns=1000)
        key2 = make_key("/a/b.txt", mtime_ns=2000)
        cacher.put(key1, make_hash(digest=b"\xaa" * 32))
        assert cacher.get(key2) is None

    def test_different_size_is_different_key(self):
        from orcapod.hashing.hash_cachers import InMemoryHashCacher
        cacher = InMemoryHashCacher()
        key1 = make_key("/a/b.txt", size=100)
        key2 = make_key("/a/b.txt", size=200)
        cacher.put(key1, make_hash(digest=b"\xaa" * 32))
        assert cacher.get(key2) is None

    def test_clear_removes_all_entries(self):
        from orcapod.hashing.hash_cachers import InMemoryHashCacher
        cacher = InMemoryHashCacher()
        key = make_key("/a/b.txt")
        cacher.put(key, make_hash())
        cacher.clear()
        assert cacher.get(key) is None


# ---------------------------------------------------------------------------
# SqliteHashCacher
# ---------------------------------------------------------------------------


class TestSqliteHashCacher:
    def test_miss_returns_none(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher
        cacher = SqliteHashCacher(tmp_path / "cache.db")
        assert cacher.get(make_key("/a/b.txt")) is None

    def test_put_then_get_returns_hit(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher
        cacher = SqliteHashCacher(tmp_path / "cache.db")
        key = make_key("/a/b.txt")
        value = make_hash()
        cacher.put(key, value)
        result = cacher.get(key)
        assert result is not None
        assert result.method == value.method
        assert result.digest == value.digest

    def test_different_mtime_is_different_key(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher
        cacher = SqliteHashCacher(tmp_path / "cache.db")
        key1 = make_key("/a/b.txt", mtime_ns=1000)
        key2 = make_key("/a/b.txt", mtime_ns=2000)
        cacher.put(key1, make_hash(digest=b"\xaa" * 32))
        assert cacher.get(key2) is None

    def test_different_size_is_different_key(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher
        cacher = SqliteHashCacher(tmp_path / "cache.db")
        key1 = make_key("/a/b.txt", size=100)
        key2 = make_key("/a/b.txt", size=200)
        cacher.put(key1, make_hash(digest=b"\xaa" * 32))
        assert cacher.get(key2) is None

    def test_persistence_across_instances(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher
        db = tmp_path / "cache.db"
        key = make_key("/a/b.txt")
        value = make_hash()

        cacher1 = SqliteHashCacher(db)
        cacher1.put(key, value)
        cacher1.close()

        cacher2 = SqliteHashCacher(db)
        result = cacher2.get(key)
        assert result is not None
        assert result.method == value.method
        assert result.digest == value.digest

    def test_wal_mode_enabled(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher
        db = tmp_path / "cache.db"
        SqliteHashCacher(db)
        with sqlite3.connect(db) as conn:
            cursor = conn.execute("PRAGMA journal_mode")
            mode = cursor.fetchone()[0]
        assert mode == "wal"

    def test_clear_empties_table(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher
        cacher = SqliteHashCacher(tmp_path / "cache.db")
        key = make_key("/a/b.txt")
        cacher.put(key, make_hash())
        cacher.clear()
        assert cacher.get(key) is None

    def test_env_var_path_override(self, tmp_path, monkeypatch):
        from orcapod.hashing.hash_cachers import SqliteHashCacher
        db = tmp_path / "env_cache.db"
        monkeypatch.setenv("ORCAPOD_HASH_CACHE_DB", str(db))
        cacher = SqliteHashCacher()
        assert cacher.db_path == db

    def test_context_manager_closes_connection(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher
        db = tmp_path / "cache.db"
        with SqliteHashCacher(db) as cacher:
            cacher.put(make_key("/a/b.txt"), make_hash())
        # After __exit__, thread-local conn should be None — verify by
        # reopening and checking persistence still works
        cacher2 = SqliteHashCacher(db)
        assert cacher2.get(make_key("/a/b.txt")) is not None

    def test_idempotent_put(self, tmp_path):
        from orcapod.hashing.hash_cachers import SqliteHashCacher
        cacher = SqliteHashCacher(tmp_path / "cache.db")
        key = make_key("/a/b.txt")
        v1 = make_hash(digest=b"\xaa" * 32)
        v2 = make_hash(digest=b"\xbb" * 32)
        cacher.put(key, v1)
        cacher.put(key, v2)  # INSERT OR REPLACE
        result = cacher.get(key)
        assert result.digest == v2.digest


# ---------------------------------------------------------------------------
# enable_file_hash_caching()
# ---------------------------------------------------------------------------


@pytest.fixture()
def restore_default_file_handler():
    """Restore the default FileHandler and DirectoryHandler after each test."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.directory_type import Directory
    from orcapod.extension_types.file_type import File

    context = get_default_context()
    registry = context.semantic_hasher.type_handler_registry
    original_file_handler = registry.get_handler_for_type(File)
    original_dir_handler = registry.get_handler_for_type(Directory)
    yield
    registry.register(File, original_file_handler)
    registry.register(Directory, original_dir_handler)


class TestEnableFileHashCaching:
    def test_registers_cached_file_hasher(self, restore_default_file_handler, tmp_path):
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.extension_types.file_type import File
        from orcapod.hashing.file_hashers import CachedFileHasher

        enable_file_hash_caching(db_path=tmp_path / "cache.db")

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        assert isinstance(handler.file_hasher, CachedFileHasher)

    def test_double_call_logs_warning_and_does_not_double_wrap(
        self, restore_default_file_handler, tmp_path, caplog
    ):
        import logging
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.extension_types.file_type import File
        from orcapod.hashing.file_hashers import CachedFileHasher, FileHasher

        enable_file_hash_caching(db_path=tmp_path / "cache.db")

        with caplog.at_level(logging.WARNING):
            enable_file_hash_caching(db_path=tmp_path / "cache2.db")

        assert "already has a CachedFileHasher" in caplog.text

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        # Outer is CachedFileHasher
        assert isinstance(handler.file_hasher, CachedFileHasher)
        # Inner (the base hasher) must NOT be a CachedFileHasher
        assert isinstance(handler.file_hasher.file_hasher, FileHasher)

    def test_preserves_base_hasher_algorithm(
        self, restore_default_file_handler, tmp_path
    ):
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.extension_types.file_type import File

        enable_file_hash_caching(db_path=tmp_path / "cache.db")

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        # The base FileHasher should still use sha256 (from v0.1.json)
        assert handler.file_hasher.file_hasher.algorithm == "sha256"

    def test_directory_handler_uses_cached_file_hasher(
        self, restore_default_file_handler, tmp_path
    ):
        """After enable_file_hash_caching(), DirectoryHandler uses CachedFileHasher."""
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.extension_types.directory_type import Directory
        from orcapod.hashing.file_hashers import CachedFileHasher

        enable_file_hash_caching(db_path=tmp_path / "cache.db")

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        dir_handler = registry.get_handler_for_type(Directory)
        assert isinstance(dir_handler.directory_hasher.file_hasher, CachedFileHasher)

    def test_shared_cache_between_file_and_directory_handlers(
        self, restore_default_file_handler, tmp_path
    ):
        """FileHandler and DirectoryHandler share the exact same CachedFileHasher instance."""
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.extension_types.directory_type import Directory
        from orcapod.extension_types.file_type import File

        enable_file_hash_caching(db_path=tmp_path / "cache.db")

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry

        file_hasher = registry.get_handler_for_type(File).file_hasher
        dir_file_hasher = registry.get_handler_for_type(Directory).directory_hasher.file_hasher

        assert file_hasher is dir_file_hasher

    def test_read_only_kwarg_passes_through_to_cacher(
        self, restore_default_file_handler, tmp_path
    ):
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.extension_types.file_type import File
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        enable_file_hash_caching(db_path=tmp_path / "cache.db", read_only=True)

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        cacher = handler.file_hasher.cacher

        assert isinstance(cacher, SqliteHashCacher)
        assert cacher._read_only is True

    def test_min_cache_size_bytes_kwarg_passes_through_to_cacher(
        self, restore_default_file_handler, tmp_path
    ):
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.extension_types.file_type import File
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        enable_file_hash_caching(
            db_path=tmp_path / "cache.db", min_cache_size_bytes=1024
        )

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        cacher = handler.file_hasher.cacher

        assert isinstance(cacher, SqliteHashCacher)
        assert cacher._min_cache_size_bytes == 1024

    def test_match_mtime_kwarg_passes_through_to_cacher(
        self, restore_default_file_handler, tmp_path
    ):
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.extension_types.file_type import File
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        enable_file_hash_caching(db_path=tmp_path / "cache.db", match_mtime=False)

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        cacher = handler.file_hasher.cacher

        assert isinstance(cacher, SqliteHashCacher)
        assert cacher._match_mtime is False


class TestEnableFileHashCachingConninfo:
    def test_conninfo_and_db_path_raises(self, restore_default_file_handler, tmp_path):
        """Providing both conninfo and db_path raises ValueError."""
        from orcapod.contexts import enable_file_hash_caching

        with pytest.raises(ValueError, match="not both"):
            enable_file_hash_caching(
                db_path=tmp_path / "x.db",
                conninfo="postgresql://unused",
            )


# ---------------------------------------------------------------------------
# SqliteHashCacher schema version detection
# ---------------------------------------------------------------------------


def _make_v0_db(path: Path) -> None:
    """Create a V0 SQLite cache database (no cached_at column)."""
    import sqlite3

    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute(
            """
            CREATE TABLE file_hash_cache (
                path      TEXT    NOT NULL,
                mtime_ns  INTEGER NOT NULL,
                size      INTEGER NOT NULL,
                hash      BLOB    NOT NULL,
                PRIMARY KEY (path, mtime_ns, size)
            ) WITHOUT ROWID
            """
        )
        # Leave user_version at 0 (default).
        conn.commit()


class TestSqliteSchemaVersion:
    def test_fresh_db_is_stamped_v1(self, tmp_path):
        """A freshly created SQLite cache database gets user_version = 1."""
        import sqlite3
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        db = tmp_path / "cache.db"
        SqliteHashCacher(db)

        with sqlite3.connect(db) as conn:
            version = conn.execute("PRAGMA user_version").fetchone()[0]
        assert version == 1

    def test_v0_db_raises_with_migration_hint(self, tmp_path):
        """Opening a V0 database (no cached_at) raises ValueError with migration command."""
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        db = tmp_path / "old_cache.db"
        _make_v0_db(db)

        with pytest.raises(ValueError, match="migrate_hash_cache"):
            SqliteHashCacher(db)

    def test_v0_db_error_mentions_db_path(self, tmp_path):
        """The migration error message includes the database path."""
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        db = tmp_path / "old_cache.db"
        _make_v0_db(db)

        with pytest.raises(ValueError, match=str(db)):
            SqliteHashCacher(db)

    def test_existing_v1_db_reopens_without_error(self, tmp_path):
        """Opening a V1 database a second time succeeds."""
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        db = tmp_path / "cache.db"
        SqliteHashCacher(db).close()
        # Should not raise.
        SqliteHashCacher(db).close()

    def test_manually_bumped_version_with_missing_column_raises(self, tmp_path):
        """A DB with user_version=1 but no cached_at column raises ValueError.

        Guards against manually-corrupted databases where user_version was
        bumped without actually adding the cached_at column.
        """
        import sqlite3
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        db = tmp_path / "corrupt.db"
        with sqlite3.connect(db) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """
                CREATE TABLE file_hash_cache (
                    path      TEXT    NOT NULL,
                    mtime_ns  INTEGER NOT NULL,
                    size      INTEGER NOT NULL,
                    hash      BLOB    NOT NULL,
                    PRIMARY KEY (path, mtime_ns, size)
                ) WITHOUT ROWID
                """
            )
            # Manually stamp user_version = 1 without adding cached_at.
            conn.execute("PRAGMA user_version = 1")
            conn.commit()

        with pytest.raises(ValueError, match="cached_at"):
            SqliteHashCacher(db)


# ---------------------------------------------------------------------------
# migrate_hash_cache script
# ---------------------------------------------------------------------------


class TestMigrateHashCache:
    def test_migrates_v0_to_v1(self, tmp_path):
        """migrate_sqlite_hash_cache adds cached_at and stamps version 1."""
        import sqlite3
        from orcapod.hashing.migrate_hash_cache import migrate_sqlite_hash_cache

        db = tmp_path / "old.db"
        _make_v0_db(db)
        migrate_sqlite_hash_cache(db)

        with sqlite3.connect(db) as conn:
            version = conn.execute("PRAGMA user_version").fetchone()[0]
            columns = {
                row[1] for row in conn.execute("PRAGMA table_info(file_hash_cache)")
            }
        assert version == 1
        assert "cached_at" in columns

    def test_migration_idempotent(self, tmp_path, capsys):
        """Running migration twice on a V1 database prints a message and exits cleanly."""
        from orcapod.hashing.migrate_hash_cache import migrate_sqlite_hash_cache
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        db = tmp_path / "cache.db"
        SqliteHashCacher(db).close()  # creates V1 DB
        migrate_sqlite_hash_cache(db)

        out = capsys.readouterr().out
        assert "nothing to migrate" in out

    def test_missing_db_raises_file_not_found(self, tmp_path):
        """migrate_sqlite_hash_cache raises FileNotFoundError for a missing path."""
        from orcapod.hashing.migrate_hash_cache import migrate_sqlite_hash_cache

        with pytest.raises(FileNotFoundError):
            migrate_sqlite_hash_cache(tmp_path / "nonexistent.db")

    def test_non_cache_db_raises_value_error(self, tmp_path):
        """migrate_sqlite_hash_cache raises ValueError if the expected table is absent."""
        import sqlite3
        from orcapod.hashing.migrate_hash_cache import migrate_sqlite_hash_cache

        db = tmp_path / "other.db"
        with sqlite3.connect(db) as conn:
            conn.execute("CREATE TABLE unrelated (id INTEGER PRIMARY KEY)")
        with pytest.raises(ValueError, match="file_hash_cache"):
            migrate_sqlite_hash_cache(db)

    def test_preserved_rows_after_migration(self, tmp_path):
        """Rows written before migration are readable after migration."""
        import sqlite3
        from orcapod.hashing.migrate_hash_cache import migrate_sqlite_hash_cache

        db = tmp_path / "old.db"
        _make_v0_db(db)
        with sqlite3.connect(db) as conn:
            conn.execute(
                "INSERT INTO file_hash_cache (path, mtime_ns, size, hash) "
                "VALUES (?, ?, ?, ?)",
                ("/a/b.txt", 1000, 100, b"sha256:\xab" * 4),
            )
            conn.commit()

        migrate_sqlite_hash_cache(db)

        with sqlite3.connect(db) as conn:
            row = conn.execute(
                "SELECT path, cached_at FROM file_hash_cache WHERE path=?",
                ("/a/b.txt",),
            ).fetchone()
        assert row is not None
        assert row[0] == "/a/b.txt"
        assert row[1] == 0  # default for migrated rows

    def test_cached_at_column_present_but_unstamped(self, tmp_path, capsys):
        """A DB that has cached_at but user_version=0 just gets stamped to V1."""
        import sqlite3
        from orcapod.hashing.migrate_hash_cache import migrate_sqlite_hash_cache

        db = tmp_path / "unstamped.db"
        # Build a DB that has the cached_at column but user_version still = 0
        with sqlite3.connect(db) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """
                CREATE TABLE file_hash_cache (
                    path      TEXT    NOT NULL,
                    mtime_ns  INTEGER NOT NULL,
                    size      INTEGER NOT NULL,
                    hash      BLOB    NOT NULL,
                    cached_at INTEGER NOT NULL DEFAULT 0,
                    PRIMARY KEY (path, mtime_ns, size)
                ) WITHOUT ROWID
                """
            )
            # user_version stays at 0 (default)
            conn.commit()

        migrate_sqlite_hash_cache(db)

        out = capsys.readouterr().out
        assert "stamped schema version" in out

        with sqlite3.connect(db) as conn:
            version = conn.execute("PRAGMA user_version").fetchone()[0]
        assert version == 1

    def test_put_after_migration_sets_nonzero_cached_at(self, tmp_path):
        """put() on a migrated DB writes the current epoch, not 0.

        The migration adds cached_at with DEFAULT 0 (SQLite ALTER TABLE only
        accepts literal defaults).  SqliteHashCacher.put() must explicitly
        supply cached_at so migrated databases get real timestamps on new rows.
        """
        import sqlite3
        import time
        from orcapod.hashing.hash_cachers import SqliteHashCacher
        from orcapod.hashing.migrate_hash_cache import migrate_sqlite_hash_cache

        db = tmp_path / "migrated.db"
        _make_v0_db(db)
        migrate_sqlite_hash_cache(db)

        before = int(time.time()) - 1
        cacher = SqliteHashCacher(db)
        cacher.put(make_key("/new.txt", mtime_ns=5000, size=300), make_hash(digest=b"\x99" * 32))
        cacher.close()

        with sqlite3.connect(db) as conn:
            row = conn.execute(
                "SELECT cached_at FROM file_hash_cache WHERE path='/new.txt'"
            ).fetchone()
        assert row is not None
        assert row[0] > before, f"cached_at={row[0]} should be > {before}"

    def test_main_entry_point(self, tmp_path, capsys):
        """main() parses argv, runs migration, exits cleanly."""
        import sys
        from unittest.mock import patch
        from orcapod.hashing.migrate_hash_cache import main
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        db = tmp_path / "v1.db"
        SqliteHashCacher(db).close()  # creates a V1 DB

        # Patch sys.argv to supply the db_path argument
        with patch.object(sys, "argv", ["migrate_hash_cache", str(db)]):
            main()

        out = capsys.readouterr().out
        assert "nothing to migrate" in out
