"""Tests for hash cacher write guards: read_only and min_cache_size_bytes.

Covers InMemoryHashCacher, SqliteHashCacher, CachedFileHasher integration,
and enable_file_hash_caching() pass-through — added by ITL-519.
"""

from __future__ import annotations

import pytest
from upath import UPath

from orcapod.hashing.file_hashers import CachedFileHasher, FileHashKey, FileHasher
from orcapod.hashing.hash_cachers import InMemoryHashCacher, SqliteHashCacher
from orcapod.types import ContentHash


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
# InMemoryHashCacher — read_only
# ---------------------------------------------------------------------------


class TestInMemoryHashCacherReadOnly:
    def test_put_is_noop_when_read_only(self):
        cacher = InMemoryHashCacher(read_only=True)
        key = make_key()
        cacher.put(key, make_hash())
        assert cacher.get(key) is None

    def test_get_returns_none_on_empty_read_only_cacher(self):
        cacher = InMemoryHashCacher(read_only=True)
        assert cacher.get(make_key()) is None

    def test_put_works_when_not_read_only(self):
        cacher = InMemoryHashCacher(read_only=False)
        key = make_key()
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) == value

    def test_default_is_not_read_only(self):
        cacher = InMemoryHashCacher()
        key = make_key()
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) == value


# ---------------------------------------------------------------------------
# InMemoryHashCacher — min_cache_size_bytes
# ---------------------------------------------------------------------------


class TestInMemoryHashCacherThreshold:
    def test_small_file_not_cached(self):
        cacher = InMemoryHashCacher(min_cache_size_bytes=100)
        key = make_key(size=50)
        cacher.put(key, make_hash())
        assert cacher.get(key) is None

    def test_file_at_threshold_is_cached(self):
        # Boundary is inclusive: size >= threshold → stored
        cacher = InMemoryHashCacher(min_cache_size_bytes=100)
        key = make_key(size=100)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) == value

    def test_large_file_is_cached(self):
        cacher = InMemoryHashCacher(min_cache_size_bytes=100)
        key = make_key(size=200)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) == value

    def test_none_threshold_caches_all(self):
        cacher = InMemoryHashCacher(min_cache_size_bytes=None)
        key = make_key(size=1)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) == value

    def test_zero_threshold_caches_all(self):
        # 0 is falsy → treated as "no threshold"
        cacher = InMemoryHashCacher(min_cache_size_bytes=0)
        key = make_key(size=1)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) == value


# ---------------------------------------------------------------------------
# InMemoryHashCacher — combined
# ---------------------------------------------------------------------------


class TestInMemoryHashCacherCombined:
    def test_read_only_wins_over_large_file(self):
        cacher = InMemoryHashCacher(read_only=True, min_cache_size_bytes=100)
        key = make_key(size=9999)
        cacher.put(key, make_hash())
        assert cacher.get(key) is None

    def test_writable_large_file_is_cached(self):
        cacher = InMemoryHashCacher(read_only=False, min_cache_size_bytes=100)
        key = make_key(size=200)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) == value

    def test_writable_small_file_not_cached(self):
        cacher = InMemoryHashCacher(read_only=False, min_cache_size_bytes=100)
        key = make_key(size=50)
        cacher.put(key, make_hash())
        assert cacher.get(key) is None


# ---------------------------------------------------------------------------
# InMemoryHashCacher — __repr__
# ---------------------------------------------------------------------------


class TestInMemoryHashCacherValidation:
    def test_negative_min_cache_size_bytes_raises(self):
        with pytest.raises(ValueError, match="non-negative"):
            InMemoryHashCacher(min_cache_size_bytes=-1)

    def test_zero_min_cache_size_bytes_is_allowed(self):
        cacher = InMemoryHashCacher(min_cache_size_bytes=0)
        assert cacher._min_cache_size_bytes == 0


class TestInMemoryHashCacherRepr:
    def test_repr_shows_read_only_false(self):
        cacher = InMemoryHashCacher()
        assert "read_only=False" in repr(cacher)

    def test_repr_shows_read_only_true(self):
        cacher = InMemoryHashCacher(read_only=True)
        assert "read_only=True" in repr(cacher)

    def test_repr_shows_min_cache_size_bytes_none(self):
        cacher = InMemoryHashCacher()
        assert "min_cache_size_bytes=None" in repr(cacher)

    def test_repr_shows_min_cache_size_bytes_value(self):
        cacher = InMemoryHashCacher(min_cache_size_bytes=1024)
        assert "min_cache_size_bytes=1024" in repr(cacher)


# ---------------------------------------------------------------------------
# SqliteHashCacher — read_only
# ---------------------------------------------------------------------------


class TestSqliteHashCacherReadOnly:
    def test_put_is_noop_when_read_only(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db", read_only=True)
        key = make_key()
        cacher.put(key, make_hash())
        assert cacher.get(key) is None

    def test_read_only_cacher_can_read_preexisting_entries(self, tmp_path):
        """A read-only SqliteHashCacher can still read entries written by a writable instance."""
        db = tmp_path / "cache.db"
        key = make_key()
        value = make_hash()
        # Pre-populate with a writable cacher
        writable = SqliteHashCacher(db)
        writable.put(key, value)
        writable.close()
        # Read-only cacher must see the pre-existing entry
        read_only = SqliteHashCacher(db, read_only=True)
        result = read_only.get(key)
        assert result is not None
        assert result.digest == value.digest

    def test_put_works_when_not_read_only(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db", read_only=False)
        key = make_key()
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) is not None

    def test_default_is_not_read_only(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db")
        key = make_key()
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) is not None


# ---------------------------------------------------------------------------
# SqliteHashCacher — min_cache_size_bytes
# ---------------------------------------------------------------------------


class TestSqliteHashCacherThreshold:
    def test_small_file_not_cached(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db", min_cache_size_bytes=100)
        key = make_key(size=50)
        cacher.put(key, make_hash())
        assert cacher.get(key) is None

    def test_file_at_threshold_is_cached(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db", min_cache_size_bytes=100)
        key = make_key(size=100)
        value = make_hash()
        cacher.put(key, value)
        result = cacher.get(key)
        assert result is not None
        assert result.digest == value.digest

    def test_large_file_is_cached(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db", min_cache_size_bytes=100)
        key = make_key(size=200)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) is not None

    def test_none_threshold_caches_all(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db", min_cache_size_bytes=None)
        key = make_key(size=1)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) is not None

    def test_zero_threshold_caches_all(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db", min_cache_size_bytes=0)
        key = make_key(size=1)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) is not None


# ---------------------------------------------------------------------------
# SqliteHashCacher — combined
# ---------------------------------------------------------------------------


class TestSqliteHashCacherCombined:
    def test_read_only_wins_over_large_file(self, tmp_path):
        cacher = SqliteHashCacher(
            tmp_path / "cache.db", read_only=True, min_cache_size_bytes=100
        )
        key = make_key(size=9999)
        cacher.put(key, make_hash())
        assert cacher.get(key) is None

    def test_writable_large_file_is_cached(self, tmp_path):
        cacher = SqliteHashCacher(
            tmp_path / "cache.db", read_only=False, min_cache_size_bytes=100
        )
        key = make_key(size=200)
        value = make_hash()
        cacher.put(key, value)
        assert cacher.get(key) is not None

    def test_writable_small_file_not_cached(self, tmp_path):
        cacher = SqliteHashCacher(
            tmp_path / "cache.db", read_only=False, min_cache_size_bytes=100
        )
        key = make_key(size=50)
        cacher.put(key, make_hash())
        assert cacher.get(key) is None


# ---------------------------------------------------------------------------
# SqliteHashCacher — __repr__
# ---------------------------------------------------------------------------


class TestSqliteHashCacherValidation:
    def test_negative_min_cache_size_bytes_raises(self, tmp_path):
        with pytest.raises(ValueError, match="non-negative"):
            SqliteHashCacher(tmp_path / "cache.db", min_cache_size_bytes=-1)

    def test_zero_min_cache_size_bytes_is_allowed(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db", min_cache_size_bytes=0)
        assert cacher._min_cache_size_bytes == 0


class TestSqliteHashCacherRepr:
    def test_repr_includes_db_path(self, tmp_path):
        db = tmp_path / "cache.db"
        cacher = SqliteHashCacher(db)
        assert str(db) in repr(cacher)

    def test_repr_shows_read_only_false(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db")
        assert "read_only=False" in repr(cacher)

    def test_repr_shows_read_only_true(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db", read_only=True)
        assert "read_only=True" in repr(cacher)

    def test_repr_shows_min_cache_size_bytes_none(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db")
        assert "min_cache_size_bytes=None" in repr(cacher)

    def test_repr_shows_min_cache_size_bytes_value(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db", min_cache_size_bytes=1024)
        assert "min_cache_size_bytes=1024" in repr(cacher)


# ---------------------------------------------------------------------------
# CachedFileHasher integration — read_only cacher (real files)
# ---------------------------------------------------------------------------


class TestCachedFileHasherWithReadOnlyCacher:
    def test_hash_file_returns_correct_hash_despite_read_only(self, tmp_path):
        """hash_file() computes and returns the hash even when the cacher is read-only."""
        f = tmp_path / "file.txt"
        f.write_text("hello world")

        inner = FileHasher(algorithm="sha256")
        cacher = InMemoryHashCacher(read_only=True)
        cached = CachedFileHasher(file_hasher=inner, cacher=cacher)

        result = cached.hash_file(f)
        expected = inner.hash_file(f)

        assert result.method == expected.method
        assert result.digest == expected.digest

    def test_hash_file_does_not_store_result_in_read_only_cacher(self, tmp_path):
        """hash_file() returns the correct hash but stores nothing in the cacher."""
        f = tmp_path / "file.txt"
        f.write_text("hello world")

        inner = FileHasher(algorithm="sha256")
        cacher = InMemoryHashCacher(read_only=True)
        cached = CachedFileHasher(file_hasher=inner, cacher=cacher)

        cached.hash_file(f)

        path = UPath(f).resolve()
        stat = path.stat()
        key = FileHashKey(path, stat.st_mtime_ns, stat.st_size)
        assert cacher.get(key) is None


# ---------------------------------------------------------------------------
# CachedFileHasher integration — threshold cacher (real files)
# ---------------------------------------------------------------------------


class TestCachedFileHasherWithThresholdCacher:
    def test_small_file_hashed_and_returned_but_not_stored(self, tmp_path):
        """Files below the threshold: hash is computed and returned, but not stored."""
        f = tmp_path / "small.bin"
        f.write_bytes(b"x" * 10)  # 10 bytes — below threshold of 100

        inner = FileHasher(algorithm="sha256")
        cacher = InMemoryHashCacher(min_cache_size_bytes=100)
        cached = CachedFileHasher(file_hasher=inner, cacher=cacher)

        result = cached.hash_file(f)
        expected = inner.hash_file(f)

        assert result.digest == expected.digest  # correct hash returned

        path = UPath(f).resolve()
        stat = path.stat()
        key = FileHashKey(path, stat.st_mtime_ns, stat.st_size)
        assert cacher.get(key) is None  # nothing stored

    def test_large_file_hashed_and_stored(self, tmp_path):
        """Files at or above the threshold: hash is computed, returned, and stored."""
        f = tmp_path / "large.bin"
        f.write_bytes(b"x" * 200)  # 200 bytes — at/above threshold of 100

        inner = FileHasher(algorithm="sha256")
        cacher = InMemoryHashCacher(min_cache_size_bytes=100)
        cached = CachedFileHasher(file_hasher=inner, cacher=cacher)

        result = cached.hash_file(f)

        path = UPath(f).resolve()
        stat = path.stat()
        key = FileHashKey(path, stat.st_mtime_ns, stat.st_size)
        stored = cacher.get(key)
        assert stored is not None
        assert stored.digest == result.digest
