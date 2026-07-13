"""PostgreSQL-backed file hash cacher.

Provides ``PostgresHashCacher`` — a network-accessible, concurrent-safe
implementation of ``CacherProtocol[FileHashKey, ContentHash]``. Requires
the optional ``psycopg`` driver (``pip install 'orcapod[postgresql]'``).
"""

from __future__ import annotations

import re
import threading

from orcapod.hashing.file_hashers import FileHashKey
from orcapod.types import ContentHash

try:
    import psycopg
except ImportError as _exc:  # pragma: no cover
    raise ImportError(
        "PostgresHashCacher requires psycopg. "
        "Install it with: pip install 'orcapod[postgresql]'"
    ) from _exc

# Current schema version.  Increment when the DDL changes.
_SCHEMA_VERSION = 1


def _redact_conninfo(conninfo: str) -> str:
    """Return ``conninfo`` with any password value replaced by ``***``.

    Handles both URL form (``postgresql://user:pass@host/db``) and
    keyword DSN form (``host=... password=secret ...``).

    Args:
        conninfo: Raw psycopg3 connection string.

    Returns:
        Connection string safe for logging, with the password redacted.
    """
    # URL form: postgresql://user:pass@host → postgresql://user:***@host
    # Capture group 1: "://user:", group 2: "@" — replace the password between them.
    redacted = re.sub(r"(://[^:@]*:)[^@]+(@)", r"\1***\2", conninfo)
    # Keyword form: password=value or password='value with spaces'
    redacted = re.sub(
        r"(?i)\bpassword\s*=\s*(?:'[^']*'|\S+)", "password=***", redacted
    )
    return redacted


class PostgresHashCacher:
    """PostgreSQL-backed file hash cacher.

    Stores file hashes keyed on ``(path, mtime_ns, size)`` in a shared
    PostgreSQL database. Uses thread-local connections for thread safety
    and ``INSERT ... ON CONFLICT DO NOTHING`` for concurrent-insert safety.

    The hash is stored as a BYTEA in ``{method}:{raw_digest}`` format via
    ``ContentHash.to_prefixed_digest()``.

    Schema versioning is tracked in a companion ``file_hash_cache_meta``
    table. If an existing database is detected with an old schema (e.g.
    missing the ``cached_at`` column), a ``ValueError`` is raised explaining
    the required migration DDL.

    Requires ``psycopg[binary]>=3.0`` (install with
    ``pip install 'orcapod[postgresql]'``).
    Minimum supported PostgreSQL version: **14**.

    Args:
        conninfo: psycopg3 connection string, e.g.
            ``"postgresql://user:pass@host:5432/dbname"`` or keyword DSN
            ``"host=myhost dbname=mydb user=myuser password=mypass"``.
            Any password in the conninfo is redacted in ``__repr__``.
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

    Raises:
        ValueError: If ``min_cache_size_bytes`` is negative, or if the
            target database contains a ``file_hash_cache`` table with an
            outdated schema (missing ``cached_at``).
    """

    def __init__(
        self,
        conninfo: str,
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
        self._conninfo = conninfo
        self._read_only = read_only
        self._min_cache_size_bytes = min_cache_size_bytes
        self._match_mtime = match_mtime
        self._local = threading.local()
        self._ensure_schema()

    def _ensure_schema(self) -> None:
        """Create cache tables and verify the schema version.

        Uses a dedicated one-shot connection so schema setup happens once on
        construction, independent of the thread-local connection pool.

        The companion ``file_hash_cache_meta`` table stores a
        ``('schema_version', N)`` row.  If the ``file_hash_cache`` table
        already exists but is missing the ``cached_at`` column, a
        ``ValueError`` is raised with the migration DDL to apply.
        """
        with psycopg.connect(self._conninfo) as conn:
            # Main cache table (idempotent).
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
            # Supporting index for match_mtime=False lookups: WHERE path=%s AND size=%s
            # ORDER BY mtime_ns DESC.  The PK (path, mtime_ns, size) cannot serve this
            # efficiently because size is not a leftmost prefix.
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_file_hash_cache_path_size_mtime
                ON file_hash_cache (path, size, mtime_ns DESC)
                """
            )
            # Schema-version metadata table (idempotent).
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS file_hash_cache_meta (
                    key   TEXT NOT NULL PRIMARY KEY,
                    value TEXT NOT NULL
                )
                """
            )

            # Detect old schema: file_hash_cache exists but lacks cached_at.
            # Constrain table_schema to the current search_path schemas so that
            # a same-named table in a different schema does not shadow the check.
            row = conn.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'file_hash_cache'
                  AND column_name = 'cached_at'
                  AND table_schema = ANY(current_schemas(false))
                """
            ).fetchone()
            if row is None:
                raise ValueError(
                    "PostgreSQL hash cache table exists but uses an outdated schema "
                    "(missing the 'cached_at' column, schema version 0). "
                    "Apply the following migration and then retry:\n\n"
                    "    ALTER TABLE file_hash_cache\n"
                    "        ADD COLUMN cached_at BIGINT NOT NULL\n"
                    "        DEFAULT EXTRACT(EPOCH FROM NOW())::BIGINT;\n"
                    "    INSERT INTO file_hash_cache_meta (key, value)\n"
                    "        VALUES ('schema_version', '1')\n"
                    "        ON CONFLICT (key) DO UPDATE SET value = '1';\n"
                )

            # Record (or update) schema version so the meta table always
            # reflects what the running code has verified/initialized.
            conn.execute(
                """
                INSERT INTO file_hash_cache_meta (key, value)
                VALUES ('schema_version', %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """,
                (str(_SCHEMA_VERSION),),
            )

    def _connection(self) -> "psycopg.Connection[tuple[object, ...]]":
        """Return this thread's connection, opening it on first use."""
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = psycopg.connect(self._conninfo)
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
            row = conn.execute(
                "SELECT hash FROM file_hash_cache "
                "WHERE path=%s AND mtime_ns=%s AND size=%s",
                (str(key.path), key.mtime_ns, key.size),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT hash FROM file_hash_cache "
                "WHERE path=%s AND size=%s ORDER BY mtime_ns DESC LIMIT 1",
                (str(key.path), key.size),
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
            f"conninfo={_redact_conninfo(self._conninfo)!r}, "
            f"read_only={self._read_only!r}, "
            f"min_cache_size_bytes={self._min_cache_size_bytes!r}, "
            f"match_mtime={self._match_mtime!r})"
        )
