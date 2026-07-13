"""Tests for PostgresHashCacher using a real Postgres via testcontainers.

These tests are marked ``@pytest.mark.postgres`` and are **skipped by default**.
To run them, pass ``--postgres`` and ensure Docker is available::

    uv run pytest tests/test_hashing/test_postgres_hash_cacher.py --postgres

The ``pg_conninfo`` fixture uses ``testcontainers[postgres]`` to spin up a
``postgres:16`` container automatically; no manual Postgres setup is needed.
Tests in ``TestPostgresReprRedaction`` do **not** carry the postgres marker and
run unconditionally because they test the pure ``_redact_conninfo()`` helper.
"""

from __future__ import annotations

import threading

import pytest
from upath import UPath

psycopg = pytest.importorskip("psycopg")

from testcontainers.postgres import PostgresContainer  # noqa: E402

from orcapod.hashing.file_hashers import FileHashKey  # noqa: E402
from orcapod.hashing.postgres_hash_cacher import PostgresHashCacher  # noqa: E402
from orcapod.types import ContentHash  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_key(
    path: str = "/a/b.txt", mtime_ns: int = 1000, size: int = 100
) -> FileHashKey:
    return FileHashKey(path=UPath(path), mtime_ns=mtime_ns, size=size)


def make_hash(digest: bytes = b"\xab" * 32) -> ContentHash:
    return ContentHash(method="sha256", digest=digest)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def pg_conninfo():
    """Start one Postgres container for the entire module; yield a conninfo string."""
    with PostgresContainer("postgres:16") as pg:
        yield (
            f"host={pg.get_container_host_ip()} "
            f"port={pg.get_exposed_port(5432)} "
            f"dbname={pg.dbname} "
            f"user={pg.username} "
            f"password={pg.password}"
        )


@pytest.fixture()
def cacher(pg_conninfo):
    """Fresh PostgresHashCacher with a cleared table for each test."""
    c = PostgresHashCacher(pg_conninfo)
    c.clear()
    yield c
    c.close()


# ---------------------------------------------------------------------------
# Basic operations
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestPostgresHashCacherBasic:
    def test_miss_returns_none(self, cacher):
        assert cacher.get(make_key()) is None

    def test_put_then_get_returns_hit(self, cacher):
        key = make_key()
        value = make_hash()
        cacher.put(key, value)
        result = cacher.get(key)
        assert result is not None
        assert result.method == value.method
        assert result.digest == value.digest

    def test_different_path_is_miss(self, cacher):
        cacher.put(make_key("/a/b.txt"), make_hash(b"\xaa" * 32))
        assert cacher.get(make_key("/a/c.txt")) is None

    def test_different_mtime_ns_is_miss(self, cacher):
        cacher.put(make_key(mtime_ns=1000), make_hash(b"\xaa" * 32))
        assert cacher.get(make_key(mtime_ns=2000)) is None

    def test_different_size_is_miss(self, cacher):
        cacher.put(make_key(size=100), make_hash(b"\xaa" * 32))
        assert cacher.get(make_key(size=200)) is None

    def test_clear_empties_table(self, cacher):
        key = make_key()
        cacher.put(key, make_hash())
        cacher.clear()
        assert cacher.get(key) is None

    def test_persistence_across_instances(self, pg_conninfo):
        key = make_key("/persist.txt", mtime_ns=9999, size=512)
        value = make_hash(b"\xcc" * 32)

        cacher1 = PostgresHashCacher(pg_conninfo)
        cacher1.clear()
        cacher1.put(key, value)
        cacher1.close()

        cacher2 = PostgresHashCacher(pg_conninfo)
        result = cacher2.get(key)
        cacher2.close()

        assert result is not None
        assert result.method == value.method
        assert result.digest == value.digest

    def test_context_manager_closes_connection(self, pg_conninfo):
        key = make_key("/ctx.txt", mtime_ns=7777, size=64)
        value = make_hash(b"\xdd" * 32)

        with PostgresHashCacher(pg_conninfo) as c:
            c.clear()
            c.put(key, value)

        # Verify persistence (connection was closed but data committed)
        cacher2 = PostgresHashCacher(pg_conninfo)
        result = cacher2.get(key)
        cacher2.close()
        assert result is not None
        assert result.digest == value.digest


# ---------------------------------------------------------------------------
# Write guards
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestPostgresHashCacherReadOnly:
    def test_put_is_noop_when_read_only(self, pg_conninfo):
        key = make_key("/ro.txt")
        value = make_hash()

        cacher = PostgresHashCacher(pg_conninfo, read_only=True)
        cacher.clear()
        cacher.put(key, value)
        assert cacher.get(key) is None
        cacher.close()

    def test_read_only_can_read_preexisting(self, pg_conninfo):
        key = make_key("/ro_pre.txt", mtime_ns=5555, size=256)
        value = make_hash(b"\xee" * 32)

        writable = PostgresHashCacher(pg_conninfo)
        writable.clear()
        writable.put(key, value)
        writable.close()

        read_only = PostgresHashCacher(pg_conninfo, read_only=True)
        result = read_only.get(key)
        read_only.close()

        assert result is not None
        assert result.digest == value.digest

    def test_default_is_not_read_only(self, cacher):
        key = make_key()
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) is not None


@pytest.mark.postgres
class TestPostgresHashCacherThreshold:
    def test_small_file_not_cached(self, pg_conninfo):
        cacher = PostgresHashCacher(pg_conninfo, min_cache_size_bytes=100)
        cacher.clear()
        key = make_key(size=50)
        cacher.put(key, make_hash())
        assert cacher.get(key) is None
        cacher.close()

    def test_file_at_threshold_is_cached(self, pg_conninfo):
        cacher = PostgresHashCacher(pg_conninfo, min_cache_size_bytes=100)
        cacher.clear()
        key = make_key(size=100)
        value = make_hash()
        cacher.put(key, value)
        result = cacher.get(key)
        cacher.close()
        assert result is not None
        assert result.digest == value.digest

    def test_large_file_is_cached(self, pg_conninfo):
        cacher = PostgresHashCacher(pg_conninfo, min_cache_size_bytes=100)
        cacher.clear()
        key = make_key(size=200)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) is not None
        cacher.close()

    def test_none_threshold_caches_all(self, pg_conninfo):
        cacher = PostgresHashCacher(pg_conninfo, min_cache_size_bytes=None)
        cacher.clear()
        key = make_key(size=1)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) is not None
        cacher.close()

    def test_zero_threshold_caches_all(self, pg_conninfo):
        cacher = PostgresHashCacher(pg_conninfo, min_cache_size_bytes=0)
        cacher.clear()
        key = make_key(size=1)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) is not None
        cacher.close()


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestPostgresHashCacherConcurrency:
    def test_concurrent_insert_no_error_one_row(self, pg_conninfo):
        """Two threads inserting the same key simultaneously → no error, one row."""
        key = make_key("/concurrent.txt", mtime_ns=12345, size=999)
        value = make_hash(b"\xff" * 32)

        # Pre-clear using a dedicated cacher
        setup = PostgresHashCacher(pg_conninfo)
        setup.clear()
        setup.close()

        errors: list[Exception] = []

        def insert() -> None:
            try:
                c = PostgresHashCacher(pg_conninfo)
                c.put(key, value)
                c.close()
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=insert) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], f"Concurrent inserts raised errors: {errors}"

        # Verify exactly one row was written
        with psycopg.connect(pg_conninfo) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM file_hash_cache "
                "WHERE path=%s AND mtime_ns=%s AND size=%s",
                (str(key.path), key.mtime_ns, key.size),
            ).fetchone()[0]
        assert count == 1


# ---------------------------------------------------------------------------
# Parity with SqliteHashCacher
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestPostgresHashCacherParity:
    def test_parity_with_sqlite(self, pg_conninfo, tmp_path):
        """Same key/value → equivalent ContentHash from both backends."""
        from orcapod.hashing.hash_cachers import SqliteHashCacher

        key = make_key("/parity.txt", mtime_ns=111, size=222)
        value = make_hash(b"\x12" * 32)

        pg_cacher = PostgresHashCacher(pg_conninfo)
        pg_cacher.clear()
        pg_cacher.put(key, value)
        pg_result = pg_cacher.get(key)
        pg_cacher.close()

        sq_cacher = SqliteHashCacher(tmp_path / "parity.db")
        sq_cacher.put(key, value)
        sq_result = sq_cacher.get(key)

        assert pg_result is not None
        assert sq_result is not None
        assert pg_result.method == sq_result.method
        assert pg_result.digest == sq_result.digest


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestPostgresHashCacherValidation:
    def test_negative_min_cache_size_bytes_raises(self, pg_conninfo):
        with pytest.raises(ValueError, match="non-negative"):
            PostgresHashCacher(pg_conninfo, min_cache_size_bytes=-1)

    def test_repr_includes_conninfo_and_guards(self, pg_conninfo):
        cacher = PostgresHashCacher(
            pg_conninfo, read_only=True, min_cache_size_bytes=1024
        )
        r = repr(cacher)
        assert "PostgresHashCacher" in r
        assert "read_only=True" in r
        assert "min_cache_size_bytes=1024" in r
        cacher.close()


# ---------------------------------------------------------------------------
# enable_file_hash_caching(conninfo=...) integration
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


@pytest.mark.postgres
class TestEnableFileHashCachingPostgres:
    def test_conninfo_activates_postgres_cacher(
        self, restore_default_file_handler, pg_conninfo
    ):
        """enable_file_hash_caching(conninfo=...) wires up PostgresHashCacher."""
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.extension_types.file_type import File

        enable_file_hash_caching(conninfo=pg_conninfo)

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        assert isinstance(handler.file_hasher.cacher, PostgresHashCacher)

    def test_conninfo_passes_read_only(
        self, restore_default_file_handler, pg_conninfo
    ):
        """read_only=True is forwarded to PostgresHashCacher."""
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.extension_types.file_type import File

        enable_file_hash_caching(conninfo=pg_conninfo, read_only=True)

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        assert handler.file_hasher.cacher._read_only is True

    def test_conninfo_passes_min_cache_size_bytes(
        self, restore_default_file_handler, pg_conninfo
    ):
        """min_cache_size_bytes is forwarded to PostgresHashCacher."""
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.extension_types.file_type import File

        enable_file_hash_caching(conninfo=pg_conninfo, min_cache_size_bytes=2048)

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        handler = registry.get_handler_for_type(File)
        assert handler.file_hasher.cacher._min_cache_size_bytes == 2048


# ---------------------------------------------------------------------------
# Schema version and repr
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestPostgresSchemaVersion:
    def test_meta_table_contains_schema_version(self, pg_conninfo):
        """After construction, file_hash_cache_meta stores schema_version = 1."""
        PostgresHashCacher(pg_conninfo).close()

        with psycopg.connect(pg_conninfo) as conn:
            row = conn.execute(
                "SELECT value FROM file_hash_cache_meta WHERE key = 'schema_version'"
            ).fetchone()
        assert row is not None
        assert row[0] == "1"

    def test_old_schema_raises_with_migration_hint(self, pg_conninfo):
        """A pre-existing table missing cached_at raises ValueError with DDL hint."""
        # Drop and re-create without cached_at to simulate old schema.
        with psycopg.connect(pg_conninfo) as conn:
            conn.execute("DROP TABLE IF EXISTS file_hash_cache CASCADE")
            conn.execute("DROP TABLE IF EXISTS file_hash_cache_meta CASCADE")
            conn.execute(
                """
                CREATE TABLE file_hash_cache (
                    path     TEXT   NOT NULL,
                    mtime_ns BIGINT NOT NULL,
                    size     BIGINT NOT NULL,
                    hash     BYTEA  NOT NULL,
                    PRIMARY KEY (path, mtime_ns, size)
                )
                """
            )

        with pytest.raises(ValueError, match="cached_at"):
            PostgresHashCacher(pg_conninfo)

        # Restore clean state for subsequent tests.
        with psycopg.connect(pg_conninfo) as conn:
            conn.execute("DROP TABLE IF EXISTS file_hash_cache CASCADE")


# ---------------------------------------------------------------------------
# match_mtime flag
# ---------------------------------------------------------------------------


@pytest.mark.postgres
class TestPostgresHashCacherMatchMtime:
    def test_default_true_mtime_change_causes_miss(self, pg_conninfo):
        """Default (match_mtime=True): different mtime → cache miss."""
        cacher = PostgresHashCacher(pg_conninfo)
        cacher.clear()
        cacher.put(make_key(mtime_ns=1000, size=100), make_hash(b"\xaa" * 32))
        assert cacher.get(make_key(mtime_ns=2000, size=100)) is None
        cacher.close()

    def test_false_mtime_change_is_hit(self, pg_conninfo):
        """match_mtime=False: different mtime, same path+size → cache hit."""
        cacher = PostgresHashCacher(pg_conninfo, match_mtime=False)
        cacher.clear()
        value = make_hash(b"\xbb" * 32)
        cacher.put(make_key(mtime_ns=1000, size=100), value)
        result = cacher.get(make_key(mtime_ns=2000, size=100))
        assert result is not None
        assert result.digest == value.digest
        cacher.close()

    def test_false_size_change_is_miss(self, pg_conninfo):
        """match_mtime=False: different size → cache miss."""
        cacher = PostgresHashCacher(pg_conninfo, match_mtime=False)
        cacher.clear()
        cacher.put(make_key(mtime_ns=1000, size=100), make_hash())
        assert cacher.get(make_key(mtime_ns=2000, size=200)) is None
        cacher.close()

    def test_false_returns_latest_mtime_entry(self, pg_conninfo):
        """match_mtime=False: multiple entries for same path+size → returns highest mtime_ns."""
        cacher = PostgresHashCacher(pg_conninfo, match_mtime=False)
        cacher.clear()
        hash_old = make_hash(b"\xaa" * 32)
        hash_new = make_hash(b"\xbb" * 32)
        cacher.put(make_key(mtime_ns=1000, size=100), hash_old)
        cacher.put(make_key(mtime_ns=2000, size=100), hash_new)
        result = cacher.get(make_key(mtime_ns=3000, size=100))
        assert result is not None
        assert result.digest == hash_new.digest
        cacher.close()

    def test_repr_shows_match_mtime_true(self, pg_conninfo):
        """__repr__ includes match_mtime=True by default."""
        cacher = PostgresHashCacher(pg_conninfo)
        assert "match_mtime=True" in repr(cacher)
        cacher.close()

    def test_repr_shows_match_mtime_false(self, pg_conninfo):
        """__repr__ includes match_mtime=False."""
        cacher = PostgresHashCacher(pg_conninfo, match_mtime=False)
        assert "match_mtime=False" in repr(cacher)
        cacher.close()


class TestPostgresReprRedaction:
    def test_repr_redacts_url_password(self):
        """__repr__ replaces the password in a URL-form conninfo with ***."""
        from orcapod.hashing.postgres_hash_cacher import _redact_conninfo

        raw = "postgresql://alice:s3cr3t@db.example.com:5432/orcapod"
        redacted = _redact_conninfo(raw)
        assert "s3cr3t" not in redacted
        assert "alice" in redacted
        assert "***" in redacted

    def test_repr_redacts_keyword_password(self):
        """__repr__ replaces the password in a keyword-form conninfo with ***."""
        from orcapod.hashing.postgres_hash_cacher import _redact_conninfo

        raw = "host=db.example.com dbname=orcapod user=alice password=s3cr3t"
        redacted = _redact_conninfo(raw)
        assert "s3cr3t" not in redacted
        assert "***" in redacted

    def test_repr_no_password_unchanged(self):
        """__repr__ does not alter a conninfo string that has no password."""
        from orcapod.hashing.postgres_hash_cacher import _redact_conninfo

        raw = "host=localhost dbname=orcapod user=alice"
        assert _redact_conninfo(raw) == raw
