"""
Tests for file hashing return types and CachedFileHasher behavior.

These tests would have caught the pre-existing bug where hash_utils.hash_file()
returned raw bytes instead of ContentHash, and verify the CachedFileHasher
correctly round-trips ContentHash through the string cache.
"""

import pytest

from orcapod.hashing.file_hashers import BasicFileHasher, CachedFileHasher
from orcapod.hashing.hash_utils import hash_file
from orcapod.hashing.string_cachers import InMemoryCacher
from orcapod.types import ContentHash


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def sample_file(tmp_path):
    """Create a small sample file for hashing."""
    f = tmp_path / "sample.txt"
    f.write_text("hello world")
    return f


@pytest.fixture
def file_hasher():
    return BasicFileHasher(algorithm="sha256")


@pytest.fixture
def cached_file_hasher(file_hasher):
    cacher = InMemoryCacher()
    return CachedFileHasher(file_hasher=file_hasher, string_cacher=cacher)


# ---------------------------------------------------------------------------
# hash_utils.hash_file returns ContentHash
# ---------------------------------------------------------------------------


class TestHashFileReturnType:
    """Tests that would have caught hash_file returning raw bytes."""

    def test_hash_file_returns_content_hash(self, sample_file):
        result = hash_file(sample_file)
        assert isinstance(result, ContentHash), (
            f"hash_file should return ContentHash, got {type(result).__name__}"
        )

    def test_hash_file_has_method(self, sample_file):
        result = hash_file(sample_file)
        assert result.method == "sha256"

    def test_hash_file_has_digest_bytes(self, sample_file):
        result = hash_file(sample_file)
        assert isinstance(result.digest, bytes)
        assert len(result.digest) == 32  # SHA-256 produces 32 bytes

    def test_hash_file_xxh64_returns_content_hash(self, sample_file):
        result = hash_file(sample_file, algorithm="xxh64")
        assert isinstance(result, ContentHash)
        assert result.method == "xxh64"

    def test_hash_file_crc32_returns_content_hash(self, sample_file):
        result = hash_file(sample_file, algorithm="crc32")
        assert isinstance(result, ContentHash)
        assert result.method == "crc32"
        assert len(result.digest) == 4  # CRC32 produces 4 bytes

    def test_hash_file_hash_path_returns_content_hash(self, sample_file):
        result = hash_file(sample_file, algorithm="hash_path")
        assert isinstance(result, ContentHash)
        assert result.method == "hash_path"

    def test_hash_file_to_string_round_trips(self, sample_file):
        """ContentHash.to_string() / from_string() preserves method and digest."""
        original = hash_file(sample_file)
        serialized = original.to_string()
        restored = ContentHash.from_string(serialized)
        assert restored.method == original.method
        assert restored.digest == original.digest


# ---------------------------------------------------------------------------
# BasicFileHasher returns ContentHash
# ---------------------------------------------------------------------------


class TestBasicFileHasherReturnType:
    """Tests that would have caught BasicFileHasher returning raw bytes."""

    def test_returns_content_hash(self, file_hasher, sample_file):
        result = file_hasher.hash_file(sample_file)
        assert isinstance(result, ContentHash)

    def test_method_matches_algorithm(self, sample_file):
        for algo in ("sha256", "md5"):
            hasher = BasicFileHasher(algorithm=algo)
            result = hasher.hash_file(sample_file)
            assert result.method == algo

    def test_digest_is_bytes(self, file_hasher, sample_file):
        result = file_hasher.hash_file(sample_file)
        assert isinstance(result.digest, bytes)

    def test_deterministic(self, file_hasher, sample_file):
        h1 = file_hasher.hash_file(sample_file)
        h2 = file_hasher.hash_file(sample_file)
        assert h1 == h2

    def test_different_content_different_hash(self, file_hasher, tmp_path):
        f1 = tmp_path / "a.txt"
        f2 = tmp_path / "b.txt"
        f1.write_text("aaa")
        f2.write_text("bbb")
        assert file_hasher.hash_file(f1) != file_hasher.hash_file(f2)

    def test_same_content_different_paths_same_hash(self, file_hasher, tmp_path):
        f1 = tmp_path / "a.txt"
        f2 = tmp_path / "b.txt"
        f1.write_text("same")
        f2.write_text("same")
        assert file_hasher.hash_file(f1) == file_hasher.hash_file(f2)


# ---------------------------------------------------------------------------
# CachedFileHasher with InMemoryCacher
# ---------------------------------------------------------------------------


class TestCachedFileHasher:
    """Tests for CachedFileHasher caching behavior and ContentHash preservation."""

    def test_returns_content_hash(self, cached_file_hasher, sample_file):
        result = cached_file_hasher.hash_file(sample_file)
        assert isinstance(result, ContentHash)

    def test_cache_miss_delegates_to_inner_hasher(self, sample_file):
        """On cache miss, result must match the inner BasicFileHasher."""
        inner = BasicFileHasher(algorithm="sha256")
        cacher = InMemoryCacher()
        cached = CachedFileHasher(file_hasher=inner, string_cacher=cacher)

        expected = inner.hash_file(sample_file)
        actual = cached.hash_file(sample_file)

        assert actual.method == expected.method
        assert actual.digest == expected.digest

    def test_cache_hit_returns_correct_content_hash(self, sample_file):
        """On cache hit, the returned ContentHash must have correct method and digest."""
        inner = BasicFileHasher(algorithm="sha256")
        cacher = InMemoryCacher()
        cached = CachedFileHasher(file_hasher=inner, string_cacher=cacher)

        # First call populates cache
        first = cached.hash_file(sample_file)

        # Second call should hit cache
        second = cached.hash_file(sample_file)

        assert second.method == first.method
        assert second.digest == first.digest

    def test_cache_stores_to_string_format(self, sample_file):
        """The cache must store the full 'method:hex_digest' string."""
        import os

        inner = BasicFileHasher(algorithm="sha256")
        cacher = InMemoryCacher()
        cached = CachedFileHasher(file_hasher=inner, string_cacher=cacher)

        result = cached.hash_file(sample_file)

        # Inspect the raw cached value — key includes mtime+size
        stat = os.stat(sample_file)
        cache_key = f"file:{sample_file}:{stat.st_mtime_ns}:{stat.st_size}"
        cached_value = cacher.get_cached(cache_key)

        assert cached_value is not None
        assert cached_value == result.to_string()
        # Should be in "method:hex_digest" format
        assert ":" in cached_value
        method, hex_digest = cached_value.split(":", 1)
        assert method == "sha256"
        assert hex_digest == result.digest.hex()

    def test_cache_hit_preserves_method_not_cached(self, sample_file):
        """Cache hit must return the original method, not 'cached' or similar."""
        inner = BasicFileHasher(algorithm="sha256")
        cacher = InMemoryCacher()
        cached = CachedFileHasher(file_hasher=inner, string_cacher=cacher)

        # Populate cache
        cached.hash_file(sample_file)

        # Cache hit
        result = cached.hash_file(sample_file)
        assert result.method == "sha256", (
            f"Cache hit should preserve original method, got '{result.method}'"
        )

    def test_cache_round_trip_with_from_string(self, sample_file):
        """Manually verify the to_string / from_string round-trip used by the cache."""
        inner = BasicFileHasher(algorithm="sha256")
        original = inner.hash_file(sample_file)

        serialized = original.to_string()
        restored = ContentHash.from_string(serialized)

        assert restored.method == original.method
        assert restored.digest == original.digest

    def test_different_algorithms_share_cache_key(self, tmp_path):
        """Two CachedFileHashers with different algorithms but same path+mtime+size
        share the same cache key. The second hasher gets the first's cached result.

        This documents a known limitation: the cache key doesn't include the algorithm.
        """
        f = tmp_path / "file.txt"
        f.write_text("test content")

        cacher = InMemoryCacher()

        sha_inner = BasicFileHasher(algorithm="sha256")
        sha_cached = CachedFileHasher(file_hasher=sha_inner, string_cacher=cacher)
        sha_result = sha_cached.hash_file(f)

        md5_inner = BasicFileHasher(algorithm="md5")
        md5_cached = CachedFileHasher(file_hasher=md5_inner, string_cacher=cacher)
        md5_result = md5_cached.hash_file(f)

        # Same cache key (same file, same mtime+size), so md5 gets sha256 result
        assert md5_result.method == "sha256"
        assert md5_result.digest == sha_result.digest

    def test_modified_content_invalidates_cache(self, cached_file_hasher, tmp_path):
        """Cache is automatically invalidated when file content changes.

        CachedFileHasher includes mtime_ns and file size in the cache key,
        so writing new content produces a cache miss and a fresh hash.
        """
        f = tmp_path / "mutable.txt"
        f.write_text("short")
        first = cached_file_hasher.hash_file(f)

        # Use different-length content so file size changes even if mtime_ns
        # doesn't advance (can happen on fast filesystems).
        f.write_text("much longer content here")
        second = cached_file_hasher.hash_file(f)

        # Different because size changed → cache miss → rehashed
        assert first.digest != second.digest

    def test_clear_cache_forces_rehash(self, tmp_path):
        """After clearing the cache, a modified file produces a new hash."""
        inner = BasicFileHasher(algorithm="sha256")
        cacher = InMemoryCacher()
        cached = CachedFileHasher(file_hasher=inner, string_cacher=cacher)

        f = tmp_path / "mutable.txt"
        f.write_text("short")
        first = cached.hash_file(f)

        f.write_text("much longer content here")
        cacher.clear_cache()
        second = cached.hash_file(f)

        assert first.digest != second.digest
