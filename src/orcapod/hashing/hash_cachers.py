"""File hash cacher implementations.

Provides ``InMemoryHashCacher`` (testing/ephemeral use) and
``SqliteHashCacher`` (persistent, production-grade) — both implementing
``CacherProtocol[FileHashKey, ContentHash]``.
"""

import os
import sqlite3
import threading
from pathlib import Path

# Current SQLite schema version.  Stored in PRAGMA user_version.
# V0 (default) = legacy schema without the cached_at column.
# V1            = current schema: added cached_at column.
_SQLITE_SCHEMA_VERSION = 1

from orcapod.hashing.file_hashers import FileHashKey
from orcapod.types import ContentHash


class InMemoryHashCacher:
    """Dict-backed file hash cacher for testing and ephemeral in-process use.

    No persistence, no thread-safety guarantees beyond the GIL, no eviction.
    Use ``SqliteHashCacher`` for production workloads.

    Args:
        read_only: When ``True``, all ``put()`` calls are silent no-ops.
            ``get()`` still works normally. Defaults to ``False``.
        min_cache_size_bytes: When set to a positive integer, files whose
            ``key.size`` is strictly below this threshold are not inserted.
            ``None`` and ``0`` disable the threshold (default behaviour).
            Negative values raise ``ValueError``. Defaults to ``None``.
    """

    def __init__(
        self,
        *,
        read_only: bool = False,
        min_cache_size_bytes: int | None = None,
    ) -> None:
        if min_cache_size_bytes is not None and min_cache_size_bytes < 0:
            raise ValueError(
                f"min_cache_size_bytes must be None or a non-negative integer, "
                f"got {min_cache_size_bytes!r}"
            )
        self._cache: dict[FileHashKey, ContentHash] = {}
        self._read_only = read_only
        self._min_cache_size_bytes = min_cache_size_bytes

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

        No-ops silently when ``read_only=True`` or when ``key.size`` is below
        ``min_cache_size_bytes``.

        Args:
            key: File hash cache key.
            value: ``ContentHash`` to store.
        """
        if self._read_only:
            return
        if self._min_cache_size_bytes is not None and self._min_cache_size_bytes > 0 and key.size < self._min_cache_size_bytes:
            return
        self._cache[key] = value

    def clear(self) -> None:
        """Remove all entries from the cache."""
        self._cache.clear()

    def __repr__(self) -> str:
        return (
            f"InMemoryHashCacher("
            f"read_only={self._read_only!r}, "
            f"min_cache_size_bytes={self._min_cache_size_bytes!r})"
        )


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
        read_only: When ``True``, all ``put()`` calls are silent no-ops.
            ``get()`` still works normally. Defaults to ``False``.
        min_cache_size_bytes: When set to a positive integer, files whose
            ``key.size`` is strictly below this threshold are not inserted.
            ``None`` and ``0`` disable the threshold (default behaviour).
            Negative values raise ``ValueError``. Defaults to ``None``.

    Note:
        Heavy multi-writer scenarios are a known SQLite limitation. A Turso
        / libSQL migration is planned as a follow-up issue.
    """

    DEFAULT_DB_PATH = Path.home() / ".orcapod" / "file_hash_cache.db"

    def __init__(
        self,
        db_path: Path | None = None,
        *,
        read_only: bool = False,
        min_cache_size_bytes: int | None = None,
    ) -> None:
        if min_cache_size_bytes is not None and min_cache_size_bytes < 0:
            raise ValueError(
                f"min_cache_size_bytes must be None or a non-negative integer, "
                f"got {min_cache_size_bytes!r}"
            )
        self.db_path = Path(
            db_path
            or os.environ.get("ORCAPOD_HASH_CACHE_DB")
            or self.DEFAULT_DB_PATH
        )
        self._read_only = read_only
        self._min_cache_size_bytes = min_cache_size_bytes
        self._local = threading.local()
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """Create the cache table, enable WAL mode, and verify schema version.

        Uses a dedicated one-shot connection so schema setup happens once on
        construction, independent of the thread-local connection pool.

        Schema version is tracked via ``PRAGMA user_version``:

        * **V0** (default SQLite value) — legacy schema without ``cached_at``.
        * **V1** — current schema with ``cached_at``.

        If the database already contains a ``file_hash_cache`` table that is
        missing the ``cached_at`` column (V0), a ``ValueError`` is raised
        with instructions to run the bundled migration script.

        Raises:
            ValueError: If an existing database uses an outdated schema.
        """
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")

            # Create table with current schema (no-op if already exists).
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

            version = conn.execute("PRAGMA user_version").fetchone()[0]
            if version < _SQLITE_SCHEMA_VERSION:
                # Inspect actual columns in case the table pre-dates versioning.
                columns = {
                    row[1]
                    for row in conn.execute("PRAGMA table_info(file_hash_cache)")
                }
                if "cached_at" not in columns:
                    raise ValueError(
                        f"SQLite hash cache at '{self.db_path}' uses an outdated "
                        f"schema (version {version}, missing 'cached_at' column). "
                        f"Run the migration script to upgrade:\n\n"
                        f"    python -m orcapod.hashing.migrate_hash_cache "
                        f"{self.db_path}\n"
                    )
                # Table already has cached_at but version was never stamped
                # (created by code before versioning was introduced).  Stamp now.
                conn.execute(f"PRAGMA user_version = {_SQLITE_SCHEMA_VERSION}")

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

        No-ops silently when ``read_only=True`` or when ``key.size`` is below
        ``min_cache_size_bytes``. Uses ``INSERT OR REPLACE`` so writes are
        idempotent when they do proceed.

        Args:
            key: File hash cache key.
            value: ``ContentHash`` to store.
        """
        if self._read_only:
            return
        if self._min_cache_size_bytes is not None and self._min_cache_size_bytes > 0 and key.size < self._min_cache_size_bytes:
            return
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

    def __repr__(self) -> str:
        return (
            f"SqliteHashCacher("
            f"db_path={str(self.db_path)!r}, "
            f"read_only={self._read_only!r}, "
            f"min_cache_size_bytes={self._min_cache_size_bytes!r})"
        )

    def __enter__(self) -> "SqliteHashCacher":
        """Return self for use as a context manager."""
        return self

    def __exit__(self, *_: object) -> None:
        """Close the thread-local connection on exit."""
        self.close()
