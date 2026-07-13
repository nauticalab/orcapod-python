"""Tests for hash cacher match_mtime flag (ITL-522).

Covers InMemoryHashCacher and SqliteHashCacher for the match_mtime flag,
plus CachedFileHasher integration with real files.
"""
from __future__ import annotations

import os
from unittest.mock import patch

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
# InMemoryHashCacher — match_mtime
# ---------------------------------------------------------------------------


class TestInMemoryHashCacherMatchMtime:
    def test_default_true_mtime_change_causes_miss(self):
        """Default (match_mtime=True): different mtime → cache miss."""
        cacher = InMemoryHashCacher()
        cacher.put(make_key(mtime_ns=1000, size=100), make_hash())
        assert cacher.get(make_key(mtime_ns=2000, size=100)) is None

    def test_false_mtime_change_is_hit(self):
        """match_mtime=False: different mtime, same path+size → cache hit."""
        cacher = InMemoryHashCacher(match_mtime=False)
        value = make_hash()
        cacher.put(make_key(mtime_ns=1000, size=100), value)
        assert cacher.get(make_key(mtime_ns=2000, size=100)) == value

    def test_false_size_change_is_miss(self):
        """match_mtime=False: different size → cache miss (size guard still applies)."""
        cacher = InMemoryHashCacher(match_mtime=False)
        cacher.put(make_key(mtime_ns=1000, size=100), make_hash())
        assert cacher.get(make_key(mtime_ns=2000, size=200)) is None

    def test_false_different_path_is_miss(self):
        """match_mtime=False: different path → cache miss."""
        cacher = InMemoryHashCacher(match_mtime=False)
        cacher.put(make_key(path="/a/b.txt", size=100), make_hash())
        assert cacher.get(make_key(path="/a/c.txt", size=100)) is None

    def test_false_returns_latest_mtime_entry(self):
        """match_mtime=False: multiple entries for same path+size → returns highest mtime_ns."""
        cacher = InMemoryHashCacher(match_mtime=False)
        hash_old = make_hash(b"\xaa" * 32)
        hash_new = make_hash(b"\xbb" * 32)
        cacher.put(make_key(mtime_ns=1000, size=100), hash_old)
        cacher.put(make_key(mtime_ns=2000, size=100), hash_new)
        result = cacher.get(make_key(mtime_ns=3000, size=100))
        assert result == hash_new

    def test_false_no_entries_returns_none(self):
        """match_mtime=False: no matching path+size → None."""
        cacher = InMemoryHashCacher(match_mtime=False)
        assert cacher.get(make_key()) is None

    def test_repr_shows_match_mtime_true(self):
        assert "match_mtime=True" in repr(InMemoryHashCacher())

    def test_repr_shows_match_mtime_false(self):
        assert "match_mtime=False" in repr(InMemoryHashCacher(match_mtime=False))


# ---------------------------------------------------------------------------
# SqliteHashCacher — match_mtime
# ---------------------------------------------------------------------------


class TestSqliteHashCacherMatchMtime:
    def test_default_true_mtime_change_causes_miss(self, tmp_path):
        """Default (match_mtime=True): different mtime → cache miss."""
        cacher = SqliteHashCacher(tmp_path / "cache.db")
        cacher.put(make_key(mtime_ns=1000, size=100), make_hash())
        assert cacher.get(make_key(mtime_ns=2000, size=100)) is None

    def test_false_mtime_change_is_hit(self, tmp_path):
        """match_mtime=False: different mtime, same path+size → cache hit."""
        cacher = SqliteHashCacher(tmp_path / "cache.db", match_mtime=False)
        value = make_hash()
        cacher.put(make_key(mtime_ns=1000, size=100), value)
        result = cacher.get(make_key(mtime_ns=2000, size=100))
        assert result is not None
        assert result.digest == value.digest

    def test_false_size_change_is_miss(self, tmp_path):
        """match_mtime=False: different size → cache miss."""
        cacher = SqliteHashCacher(tmp_path / "cache.db", match_mtime=False)
        cacher.put(make_key(mtime_ns=1000, size=100), make_hash())
        assert cacher.get(make_key(mtime_ns=2000, size=200)) is None

    def test_false_different_path_is_miss(self, tmp_path):
        """match_mtime=False: different path → cache miss."""
        cacher = SqliteHashCacher(tmp_path / "cache.db", match_mtime=False)
        cacher.put(make_key(path="/a/b.txt", size=100), make_hash())
        assert cacher.get(make_key(path="/a/c.txt", size=100)) is None

    def test_false_returns_latest_mtime_entry(self, tmp_path):
        """match_mtime=False: multiple entries for same path+size → returns highest mtime_ns."""
        cacher = SqliteHashCacher(tmp_path / "cache.db", match_mtime=False)
        hash_old = make_hash(b"\xaa" * 32)
        hash_new = make_hash(b"\xbb" * 32)
        cacher.put(make_key(mtime_ns=1000, size=100), hash_old)
        cacher.put(make_key(mtime_ns=2000, size=100), hash_new)
        result = cacher.get(make_key(mtime_ns=3000, size=100))
        assert result is not None
        assert result.digest == hash_new.digest

    def test_false_no_entries_returns_none(self, tmp_path):
        """match_mtime=False: no matching path+size → None."""
        cacher = SqliteHashCacher(tmp_path / "cache.db", match_mtime=False)
        assert cacher.get(make_key()) is None

    def test_repr_shows_match_mtime_true(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db")
        assert "match_mtime=True" in repr(cacher)

    def test_repr_shows_match_mtime_false(self, tmp_path):
        cacher = SqliteHashCacher(tmp_path / "cache.db", match_mtime=False)
        assert "match_mtime=False" in repr(cacher)


# ---------------------------------------------------------------------------
# CachedFileHasher integration — match_mtime with real files
# ---------------------------------------------------------------------------


class TestCachedFileHasherMatchMtime:
    def test_t1_default_mtime_change_causes_miss(self, tmp_path):
        """T1: Default (match_mtime=True), mtime changed, size unchanged → cache miss → re-hash."""
        f = tmp_path / "file.bin"
        f.write_bytes(b"x" * 50)

        inner = FileHasher()
        cacher = InMemoryHashCacher(match_mtime=True)
        cached = CachedFileHasher(file_hasher=inner, cacher=cacher)

        with patch.object(inner, "hash_file", wraps=inner.hash_file) as spy:
            first_hash = cached.hash_file(f)
            assert spy.call_count == 1

            stat = f.stat()
            os.utime(f, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))

            # match_mtime=True: mtime change causes a cache miss; CachedFileHasher
            # re-hashes the file. Content is unchanged so the hash value is equal,
            # but spy.call_count == 2 proves the inner hasher was called again.
            second_hash = cached.hash_file(f)
            assert second_hash == first_hash
            assert spy.call_count == 2

    def test_t2_match_mtime_false_mtime_change_is_hit(self, tmp_path):
        """T2: match_mtime=False, mtime changed, size unchanged → cache hit."""
        f = tmp_path / "file.bin"
        f.write_bytes(b"x" * 50)

        inner = FileHasher()
        cacher = InMemoryHashCacher(match_mtime=False)
        cached = CachedFileHasher(file_hasher=inner, cacher=cacher)

        with patch.object(inner, "hash_file", wraps=inner.hash_file) as spy:
            first_hash = cached.hash_file(f)
            assert spy.call_count == 1

            stat = f.stat()
            os.utime(f, ns=(stat.st_atime_ns, stat.st_mtime_ns + 1_000_000_000))

            # match_mtime=False: mtime change is ignored; CachedFileHasher returns the
            # cached hash without calling the inner hasher again.
            second_hash = cached.hash_file(f)
            assert second_hash == first_hash
            assert spy.call_count == 1  # inner hasher NOT called again — confirms the cache hit

    def test_t3_match_mtime_false_size_change_is_miss(self, tmp_path):
        """T3: match_mtime=False, content and size changed → cache miss."""
        f = tmp_path / "file.bin"
        f.write_bytes(b"hello")  # 5 bytes

        cacher = InMemoryHashCacher(match_mtime=False)
        cached = CachedFileHasher(file_hasher=FileHasher(), cacher=cacher)
        first_hash = cached.hash_file(f)

        f.write_bytes(b"hello world")  # 11 bytes — different size

        second_hash = cached.hash_file(f)
        assert second_hash != first_hash

    def test_t4_match_mtime_false_same_size_content_flip_is_hit(self, tmp_path):
        """T4: match_mtime=False, content changed but size preserved → cache hit (known trade-off)."""
        f = tmp_path / "file.bin"
        f.write_bytes(b"aaaaa")  # 5 bytes

        cacher = InMemoryHashCacher(match_mtime=False)
        cached = CachedFileHasher(file_hasher=FileHasher(), cacher=cacher)
        first_hash = cached.hash_file(f)

        f.write_bytes(b"bbbbb")  # 5 bytes — same size, different content

        second_hash = cached.hash_file(f)
        assert second_hash == first_hash  # stale hit — known trade-off

    def test_t5_unchanged_file_both_modes_agree(self, tmp_path):
        """T5: Unchanged file — match_mtime=True and match_mtime=False return the same hash."""
        f = tmp_path / "file.bin"
        f.write_bytes(b"stable content")

        inner = FileHasher()
        hash_strict = CachedFileHasher(
            file_hasher=inner,
            cacher=InMemoryHashCacher(match_mtime=True),
        ).hash_file(f)
        hash_relaxed = CachedFileHasher(
            file_hasher=inner,
            cacher=InMemoryHashCacher(match_mtime=False),
        ).hash_file(f)
        assert hash_strict == hash_relaxed
