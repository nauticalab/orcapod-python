"""Integration tests: op.Directory hashing with CachedFileHasher.

Verifies that BasicDirectoryHasher consults the file-hash cache at the
per-file level during Merkle tree traversal, and that
enable_file_hash_caching() wires the same CachedFileHasher instance into
both FileHandler and DirectoryHandler.
"""

from __future__ import annotations

import pytest

from orcapod.logical_types.directory_type import Directory
from orcapod.logical_types.file_type import File
from orcapod.hashing.directory_hashers import BasicDirectoryHasher
from orcapod.hashing.file_hashers import CachedFileHasher, FileHasher
from orcapod.hashing.hash_cachers import InMemoryHashCacher
from orcapod.protocols.hashing_protocols import FileContentHasherProtocol
from orcapod.types import ContentHash, PathLike


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class CountingFileHasher:
    """FileContentHasherProtocol wrapper that counts hash_file() calls."""

    def __init__(self, inner: FileContentHasherProtocol) -> None:
        self.inner = inner
        self.call_count = 0

    def hash_file(self, file_path: PathLike) -> ContentHash:
        self.call_count += 1
        return self.inner.hash_file(file_path)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def dir_with_files(tmp_path):
    """A directory containing three text files."""
    d = tmp_path / "mydir"
    d.mkdir()
    (d / "a.txt").write_bytes(b"content_a")
    (d / "b.txt").write_bytes(b"content_b")
    (d / "c.txt").write_bytes(b"content_c")
    return d


@pytest.fixture()
def restore_default_handlers():
    """Restore both FileHandler and DirectoryHandler after each test."""
    from orcapod.contexts import get_default_context

    context = get_default_context()
    registry = context.semantic_hasher.type_handler_registry
    original_file_handler = registry.get_handler_for_type(File)
    original_dir_handler = registry.get_handler_for_type(Directory)
    yield
    registry.register(File, original_file_handler)
    registry.register(Directory, original_dir_handler)


# ---------------------------------------------------------------------------
# Cache write / hit / invalidation
# ---------------------------------------------------------------------------


class TestDirectoryHashCaching:
    def test_cache_write_on_first_hash(self, dir_with_files):
        """First hash: underlying FileHasher called once per file; results stored."""
        counting = CountingFileHasher(FileHasher())
        cacher = InMemoryHashCacher()
        cached = CachedFileHasher(file_hasher=counting, cacher=cacher)
        hasher = BasicDirectoryHasher(file_hasher=cached)

        result = hasher.hash_directory(dir_with_files)

        assert isinstance(result, ContentHash)
        assert result.method == "merkle_sha256"
        assert counting.call_count == 3  # one per file; none cached yet

    def test_cache_hit_on_second_hash(self, dir_with_files):
        """Second hash of the same directory returns all results from cache."""
        counting = CountingFileHasher(FileHasher())
        cacher = InMemoryHashCacher()
        cached = CachedFileHasher(file_hasher=counting, cacher=cacher)
        hasher = BasicDirectoryHasher(file_hasher=cached)

        h1 = hasher.hash_directory(dir_with_files)
        count_after_first = counting.call_count  # 3 calls for 3 files

        h2 = hasher.hash_directory(dir_with_files)

        assert h1 == h2  # same directory content → same hash
        assert counting.call_count == count_after_first  # zero new calls; all from cache

    def test_cache_invalidated_on_file_change(self, dir_with_files):
        """Modifying a file (mtime_ns/size change) causes a cache miss for that file."""
        counting = CountingFileHasher(FileHasher())
        cacher = InMemoryHashCacher()
        cached = CachedFileHasher(file_hasher=counting, cacher=cacher)
        hasher = BasicDirectoryHasher(file_hasher=cached)

        hasher.hash_directory(dir_with_files)
        count_after_first = counting.call_count  # 3

        # Write different content with a different length → size change guarantees cache miss;
        # mtime_ns also changes on most filesystems but is not relied upon here
        (dir_with_files / "b.txt").write_bytes(b"modified_content_with_different_length")

        hasher.hash_directory(dir_with_files)

        # Only b.txt triggered a new hash call; a.txt and c.txt came from cache
        assert counting.call_count == count_after_first + 1

    def test_hash_value_unchanged_vs_uncached(self, dir_with_files):
        """Cached and uncached hashing produce identical ContentHash values."""
        plain = BasicDirectoryHasher(file_hasher=FileHasher())
        cached = BasicDirectoryHasher(
            file_hasher=CachedFileHasher(
                file_hasher=FileHasher(),
                cacher=InMemoryHashCacher(),
            )
        )

        assert plain.hash_directory(dir_with_files) == cached.hash_directory(dir_with_files)


# ---------------------------------------------------------------------------
# enable_file_hash_caching() wires DirectoryHandler
# ---------------------------------------------------------------------------


class TestEnableFileHashCachingWiresDirectory:
    """Verify enable_file_hash_caching() wires CachedFileHasher into DirectoryHandler.

    Note: Task 5 adds analogous tests to test_hash_cachers.py::TestEnableFileHashCaching.
    The duplication is intentional — this class tests the caching behavior in context
    (alongside the per-file cache tests above); test_hash_cachers.py tests the
    handler-patching function itself.
    """

    def test_directory_handler_uses_cached_file_hasher(
        self, restore_default_handlers, tmp_path
    ):
        """After enable_file_hash_caching(), DirectoryHandler uses a CachedFileHasher."""
        from orcapod.contexts import enable_file_hash_caching, get_default_context
        from orcapod.hashing.file_hashers import CachedFileHasher

        enable_file_hash_caching(db_path=tmp_path / "cache.db")

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry
        dir_handler = registry.get_handler_for_type(Directory)
        assert isinstance(dir_handler.directory_hasher.file_hasher, CachedFileHasher)

    def test_shared_cache_instance(self, restore_default_handlers, tmp_path):
        """FileHandler and DirectoryHandler share the same CachedFileHasher instance."""
        from orcapod.contexts import enable_file_hash_caching, get_default_context

        enable_file_hash_caching(db_path=tmp_path / "cache.db")

        context = get_default_context()
        registry = context.semantic_hasher.type_handler_registry

        file_hasher = registry.get_handler_for_type(File).file_hasher
        dir_file_hasher = registry.get_handler_for_type(Directory).directory_hasher.file_hasher

        assert file_hasher is dir_file_hasher  # exact same object
