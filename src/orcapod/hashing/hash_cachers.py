"""File hash cacher implementations.

Provides ``InMemoryHashCacher`` (testing/ephemeral use) and
``SqliteHashCacher`` (persistent, production-grade) — both implementing
``CacherProtocol[FileHashKey, ContentHash]``.
"""

import os
import sqlite3
import threading
from pathlib import Path

from orcapod.hashing.file_hashers import FileHashKey
from orcapod.types import ContentHash


class InMemoryHashCacher:
    """Dict-backed file hash cacher for testing and ephemeral in-process use.

    No persistence, no thread-safety guarantees beyond the GIL, no eviction.
    Use ``SqliteHashCacher`` for production workloads.
    """

    def __init__(self) -> None:
        self._cache: dict[FileHashKey, ContentHash] = {}

    def get(self, key: FileHashKey) -> ContentHash | None:
        """Return the cached ``ContentHash`` for ``key``, or ``None`` on miss.

        Args:
            key: File hash cache key.

        Returns:
            Cached ``ContentHash``, or ``None`` if not found.
        """
        return self._cache.get(key)

    def put(self, key: FileHashKey, value: ContentHash) -> None:
        """Store ``value`` under ``key``.

        Args:
            key: File hash cache key.
            value: ``ContentHash`` to store.
        """
        self._cache[key] = value

    def clear(self) -> None:
        """Remove all entries from the cache."""
        self._cache.clear()


class SqliteHashCacher:
    """SQLite-backed file hash cacher.

    Stores file hashes keyed on ``(path, mtime_ns, size)`` in a local
    SQLite database. Uses WAL mode for single-writer/multi-reader
    concurrency and thread-local connections for thread safety.

    The hash is stored as a BLOB in ``{method}:{raw_digest}`` format via
    ``ContentHash.to_prefixed_digest()``.

    Args:
        db_path: Path to the SQLite database file. Defaults to
            ``~/.orcapod/file_hash_cache.db`` or the
            ``ORCAPOD_HASH_CACHE_DB`` environment variable.

    Note:
        Heavy multi-writer scenarios are a known SQLite limitation. A Turso
        / libSQL migration is planned as a follow-up issue.
    """

    DEFAULT_DB_PATH = Path.home() / ".orcapod" / "file_hash_cache.db"

    def __init__(self, db_path: Path | None = None) -> None:
        self.db_path = Path(
            db_path
            or os.environ.get("ORCAPOD_HASH_CACHE_DB")
            or self.DEFAULT_DB_PATH
        )
        self._local = threading.local()
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """Create the cache table and enable WAL mode.

        Uses a dedicated one-shot connection so schema setup happens once
        on construction, independent of the thread-local connection pool.
        """
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS file_hash_cache (
                    path      TEXT    NOT NULL,
                    mtime_ns  INTEGER NOT NULL,
                    size      INTEGER NOT NULL,
                    hash      BLOB    NOT NULL,
                    cached_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                    PRIMARY KEY (path, mtime_ns, size)
                ) WITHOUT ROWID
                """
            )
            conn.commit()

    def _connection(self) -> sqlite3.Connection:
        """Return this thread's connection, opening it on first use."""
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self.db_path)
            conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn = conn
        return conn

    def get(self, key: FileHashKey) -> ContentHash | None:
        """Return the cached ``ContentHash`` for ``key``, or ``None`` on miss.

        Args:
            key: File hash cache key.

        Returns:
            Cached ``ContentHash``, or ``None`` if not found.
        """
        conn = self._connection()
        cursor = conn.execute(
            "SELECT hash FROM file_hash_cache WHERE path=? AND mtime_ns=? AND size=?",
            (str(key.path), key.mtime_ns, key.size),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        blob: bytes = row[0]
        method_bytes, digest = blob.split(b":", 1)
        return ContentHash(method=method_bytes.decode("ascii"), digest=digest)

    def put(self, key: FileHashKey, value: ContentHash) -> None:
        """Store ``value`` under ``key``.

        Uses ``INSERT OR REPLACE`` so writes are idempotent.

        Args:
            key: File hash cache key.
            value: ``ContentHash`` to store.
        """
        conn = self._connection()
        conn.execute(
            """
            INSERT OR REPLACE INTO file_hash_cache (path, mtime_ns, size, hash)
            VALUES (?, ?, ?, ?)
            """,
            (str(key.path), key.mtime_ns, key.size, value.to_prefixed_digest()),
        )
        conn.commit()

    def clear(self) -> None:
        """Delete all rows from the cache table."""
        conn = self._connection()
        conn.execute("DELETE FROM file_hash_cache")
        conn.commit()

    def close(self) -> None:
        """Close this thread's database connection."""
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None

    def __enter__(self) -> "SqliteHashCacher":
        """Return self for use as a context manager."""
        return self

    def __exit__(self, *_: object) -> None:
        """Close the thread-local connection on exit."""
        self.close()
