"""PostgreSQL-backed file hash cacher.

Provides ``PostgresHashCacher`` — a network-accessible, concurrent-safe
implementation of ``CacherProtocol[FileHashKey, ContentHash]``. Requires
the optional ``psycopg`` driver (``pip install 'orcapod[postgresql]'``).
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING

from orcapod.hashing.file_hashers import FileHashKey
from orcapod.types import ContentHash

if TYPE_CHECKING:
    import psycopg as _psycopg


class PostgresHashCacher:
    """PostgreSQL-backed file hash cacher.

    Stores file hashes keyed on ``(path, mtime_ns, size)`` in a shared
    PostgreSQL database. Uses thread-local connections for thread safety
    and ``INSERT ... ON CONFLICT DO NOTHING`` for concurrent-insert safety.

    The hash is stored as a BYTEA in ``{method}:{raw_digest}`` format via
    ``ContentHash.to_prefixed_digest()``.

    Requires ``psycopg[binary]>=3.0`` (install with
    ``pip install 'orcapod[postgresql]'``).

    Args:
        conninfo: psycopg3 connection string, e.g.
            ``"postgresql://user:pass@host:5432/dbname"`` or keyword DSN
            ``"host=myhost dbname=mydb user=myuser password=mypass"``.
        read_only: When ``True``, all ``put()`` calls are silent no-ops.
            ``get()`` still works normally. Defaults to ``False``.
        min_cache_size_bytes: When set to a positive integer, files whose
            ``key.size`` is strictly below this threshold are not inserted.
            ``None`` and ``0`` disable the threshold (default behaviour).
            Negative values raise ``ValueError``. Defaults to ``None``.
    """

    def __init__(
        self,
        conninfo: str,
        *,
        read_only: bool = False,
        min_cache_size_bytes: int | None = None,
    ) -> None:
        try:
            import psycopg  # noqa: F401
        except ImportError:  # pragma: no cover
            raise ImportError(
                "PostgresHashCacher requires psycopg. "
                "Install it with: pip install 'orcapod[postgresql]'"
            ) from None
        if min_cache_size_bytes is not None and min_cache_size_bytes < 0:
            raise ValueError(
                f"min_cache_size_bytes must be None or a non-negative integer, "
                f"got {min_cache_size_bytes!r}"
            )
        self._conninfo = conninfo
        self._read_only = read_only
        self._min_cache_size_bytes = min_cache_size_bytes
        self._local = threading.local()
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """Create the cache table if it does not exist.

        Uses a dedicated one-shot connection so schema setup happens once
        on construction, independent of the thread-local connection pool.
        """
        import psycopg

        with psycopg.connect(self._conninfo) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS file_hash_cache (
                    path      TEXT   NOT NULL,
                    mtime_ns  BIGINT NOT NULL,
                    size      BIGINT NOT NULL,
                    hash      BYTEA  NOT NULL,
                    cached_at BIGINT NOT NULL
                        DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT,
                    PRIMARY KEY (path, mtime_ns, size)
                )
                """
            )

    def _connection(self) -> "_psycopg.Connection[tuple[object, ...]]":
        """Return this thread's connection, opening it on first use."""
        import psycopg

        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = psycopg.connect(self._conninfo)
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
        row = conn.execute(
            "SELECT hash FROM file_hash_cache "
            "WHERE path=%s AND mtime_ns=%s AND size=%s",
            (str(key.path), key.mtime_ns, key.size),
        ).fetchone()
        if row is None:
            return None
        blob: bytes = bytes(row[0])
        method_bytes, digest = blob.split(b":", 1)
        return ContentHash(method=method_bytes.decode("ascii"), digest=digest)

    def put(self, key: FileHashKey, value: ContentHash) -> None:
        """Store ``value`` under ``key``.

        No-ops silently when ``read_only=True`` or when ``key.size`` is below
        ``min_cache_size_bytes``. Uses ``INSERT ... ON CONFLICT DO NOTHING``
        so concurrent inserts of the same key from multiple workers are safe.

        Args:
            key: File hash cache key.
            value: ``ContentHash`` to store.
        """
        if self._read_only:
            return
        if (
            self._min_cache_size_bytes is not None
            and self._min_cache_size_bytes > 0
            and key.size < self._min_cache_size_bytes
        ):
            return
        conn = self._connection()
        conn.execute(
            """
            INSERT INTO file_hash_cache (path, mtime_ns, size, hash)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (path, mtime_ns, size) DO NOTHING
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

    def __enter__(self) -> "PostgresHashCacher":
        """Return self for use as a context manager."""
        return self

    def __exit__(self, *_: object) -> None:
        """Close the thread-local connection on exit."""
        self.close()

    def __repr__(self) -> str:
        return (
            f"PostgresHashCacher("
            f"conninfo={self._conninfo!r}, "
            f"read_only={self._read_only!r}, "
            f"min_cache_size_bytes={self._min_cache_size_bytes!r})"
        )
