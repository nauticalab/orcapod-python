"""Tests for PostgresHashCacher using a real Postgres via testcontainers."""

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
