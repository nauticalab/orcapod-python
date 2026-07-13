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

# Current SQLite schema version.  Stored in PRAGMA user_version.
# V0 (default) = legacy schema without the cached_at column.
# V1            = current schema: added cached_at column.
_SQLITE_SCHEMA_VERSION = 1


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
        match_mtime: When ``True`` (default), cache hits require
            ``path``, ``mtime_ns``, and ``size`` to all match. When
            ``False``, only ``path`` and ``size`` are compared; among
            multiple matching entries the one with the highest ``mtime_ns``
            is returned. The write path is unaffected — ``mtime_ns`` is
            always stored.
    """

    def __init__(
        self,
        *,
        read_only: bool = False,
        min_cache_size_bytes: int | None = None,
        match_mtime: bool = True,
    ) -> None:
        if min_cache_size_bytes is not None and min_cache_size_bytes < 0:
            raise ValueError(
                f"min_cache_size_bytes must be None or a non-negative integer, "
                f"got {min_cache_size_bytes!r}"
            )
        self._cache: dict[FileHashKey, ContentHash] = {}
        self._read_only = read_only
        self._min_cache_size_bytes = min_cache_size_bytes
        self._match_mtime = match_mtime

    def get(self, key: FileHashKey) -> ContentHash | None:
        """Return the cached ``ContentHash`` for ``key``, or ``None`` on miss.

        When ``match_mtime=True`` (default), all three key fields must match.
        When ``match_mtime=False``, only ``path`` and ``size`` are compared;
        among all matching entries the one with the highest ``mtime_ns`` is
        returned.

        Args:
            key: File hash cache key.

        Returns:
            Cached ``ContentHash``, or ``None`` if not found.
        """
        if self._match_mtime:
            return self._cache.get(key)
        best_key: FileHashKey | None = None
        best_value: ContentHash | None = None
        for cached_key, value in self._cache.items():
            if cached_key.path == key.path and cached_key.size == key.size:
                if best_key is None or cached_key.mtime_ns > best_key.mtime_ns:
                    best_key = cached_key
                    best_value = value
        return best_value

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
            f"min_cache_size_bytes={self._min_cache_size_bytes!r}, "
            f"match_mtime={self._match_mtime!r})"
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
        match_mtime: When ``True`` (default), cache hits require
            ``path``, ``mtime_ns``, and ``size`` to all match. When
            ``False``, only ``path`` and ``size`` are compared; among
            multiple matching entries the one with the highest ``mtime_ns``
            is returned. The write path is unaffected — ``mtime_ns`` is
            always stored.

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
        match_mtime: bool = True,
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
        self._match_mtime = match_mtime
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

            # Always validate the cached_at column is present, regardless of
            # user_version.  A manually-bumped user_version without the matching
            # schema change would otherwise cause a cryptic INSERT failure in
            # put() rather than a clear diagnostic here.
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

            if version < _SQLITE_SCHEMA_VERSION:
                # Version stamp is missing or outdated — stamp it now.
                # (Reaches here when the table was created by code before
                # schema versioning was introduced, so cached_at exists
                # but user_version is still 0.)
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

        When ``match_mtime=True`` (default), the query filters on
        ``path``, ``mtime_ns``, and ``size``. When ``match_mtime=False``,
        only ``path`` and ``size`` are filtered and results are ordered
        by ``mtime_ns DESC`` so the most recent entry is returned.

        Args:
            key: File hash cache key.

        Returns:
            Cached ``ContentHash``, or ``None`` if not found.
        """
        conn = self._connection()
        if self._match_mtime:
            cursor = conn.execute(
                "SELECT hash FROM file_hash_cache WHERE path=? AND mtime_ns=? AND size=?",
                (str(key.path), key.mtime_ns, key.size),
            )
        else:
            cursor = conn.execute(
                "SELECT hash FROM file_hash_cache "
                "WHERE path=? AND size=? ORDER BY mtime_ns DESC LIMIT 1",
                (str(key.path), key.size),
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
            INSERT OR REPLACE INTO file_hash_cache (path, mtime_ns, size, hash, cached_at)
            VALUES (?, ?, ?, ?, strftime('%s', 'now'))
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
            f"min_cache_size_bytes={self._min_cache_size_bytes!r}, "
            f"match_mtime={self._match_mtime!r})"
        )

    def __enter__(self) -> "SqliteHashCacher":
        """Return self for use as a context manager."""
        return self

    def __exit__(self, *_: object) -> None:
        """Close the thread-local connection on exit."""
        self.close()
