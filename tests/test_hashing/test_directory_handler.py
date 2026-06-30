"""Tests for BasicDirectoryHasher and DirectoryHandler.

Note: TestDirectoryHandler is added in Task 5 when DirectoryHandler is implemented.
"""

from __future__ import annotations

import pytest

from orcapod.extension_types.directory_type import Directory
from orcapod.hashing.directory_hashers import BasicDirectoryHasher
from orcapod.hashing.semantic_hashing.builtin_handlers import (
    DirectoryHandler,
    register_builtin_python_type_handlers,
)
from orcapod.hashing.semantic_hashing.semantic_hasher import SemanticAwarePythonHasher
from orcapod.hashing.semantic_hashing.type_handler_registry import PythonTypeHandlerRegistry
from orcapod.types import ContentHash


class TestBasicDirectoryHasher:
    def test_empty_directory_returns_content_hash(self, tmp_path):
        empty = tmp_path / "empty"
        empty.mkdir()
        hasher = BasicDirectoryHasher()
        result = hasher.hash_directory(empty)
        assert isinstance(result, ContentHash)
        assert result.method == "merkle_sha256"

    def test_empty_directory_hash_is_stable(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        hasher = BasicDirectoryHasher()
        assert hasher.hash_directory(d1) == hasher.hash_directory(d2)

    def test_identical_content_same_hash(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "file.txt").write_bytes(b"hello")
        (d2 / "file.txt").write_bytes(b"hello")
        hasher = BasicDirectoryHasher()
        assert hasher.hash_directory(d1) == hasher.hash_directory(d2)

    def test_different_content_different_hash(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "file.txt").write_bytes(b"hello")
        (d2 / "file.txt").write_bytes(b"world")
        hasher = BasicDirectoryHasher()
        assert hasher.hash_directory(d1) != hasher.hash_directory(d2)

    def test_single_byte_change_in_nested_file_changes_hash(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        for base in (d1, d2):
            (base / "sub" / "deep").mkdir(parents=True)
            (base / "sub" / "deep" / "unchanged.txt").write_bytes(b"same content")
        (d1 / "sub" / "deep" / "target.txt").write_bytes(b"hello world")
        (d2 / "sub" / "deep" / "target.txt").write_bytes(b"hello World")  # capital W
        hasher = BasicDirectoryHasher()
        assert hasher.hash_directory(d1) != hasher.hash_directory(d2)

    def test_adding_file_changes_hash(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        (d / "a.txt").write_bytes(b"content")
        hasher = BasicDirectoryHasher()
        h1 = hasher.hash_directory(d)
        (d / "b.txt").write_bytes(b"new file")
        h2 = hasher.hash_directory(d)
        assert h1 != h2

    def test_removing_file_changes_hash(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        (d / "a.txt").write_bytes(b"content")
        (d / "b.txt").write_bytes(b"to remove")
        hasher = BasicDirectoryHasher()
        h1 = hasher.hash_directory(d)
        (d / "b.txt").unlink()
        h2 = hasher.hash_directory(d)
        assert h1 != h2

    def test_hidden_files_included_by_default(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "file.txt").write_bytes(b"same")
        (d2 / "file.txt").write_bytes(b"same")
        hasher = BasicDirectoryHasher()
        h1 = hasher.hash_directory(d1)
        (d2 / ".hidden").write_bytes(b"dotfile")
        h2 = hasher.hash_directory(d2)
        assert h1 != h2

    def test_ignore_glob_excludes_matching_files(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "app.py").write_bytes(b"code")
        (d2 / "app.py").write_bytes(b"code")
        (d2 / "app.pyc").write_bytes(b"compiled")  # extra .pyc, excluded
        hasher = BasicDirectoryHasher()
        assert hasher.hash_directory(d1) == hasher.hash_directory(d2, ignore=["*.pyc"])

    def test_ignore_callable_excludes_entries(self, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "app.py").write_bytes(b"code")
        (d2 / "app.py").write_bytes(b"code")
        (d2 / "excluded.txt").write_bytes(b"extra")
        hasher = BasicDirectoryHasher()
        h1 = hasher.hash_directory(d1)
        h2 = hasher.hash_directory(d2, ignore=lambda p: p.name == "excluded.txt")
        assert h1 == h2

    def test_symlink_recorded_not_followed(self, tmp_path):
        """Two dirs with identical symlinks to the same target → same hash."""
        real_file = tmp_path / "real.txt"
        real_file.write_bytes(b"content")
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "link").symlink_to(real_file)
        (d2 / "link").symlink_to(real_file)
        hasher = BasicDirectoryHasher()
        assert hasher.hash_directory(d1) == hasher.hash_directory(d2)

    def test_symlink_cycle_safe(self, tmp_path):
        """A symlink pointing to an ancestor directory must not cause infinite recursion."""
        d = tmp_path / "d"
        d.mkdir()
        (d / "self_link").symlink_to(d)  # points to its own parent
        hasher = BasicDirectoryHasher()
        result = hasher.hash_directory(d)  # must complete without error
        assert isinstance(result, ContentHash)

    def test_large_tree_smoke_test(self, tmp_path):
        """Hashing 200 files across 10 subdirectories must complete without error."""
        for i in range(10):
            sub = tmp_path / f"sub_{i:02d}"
            sub.mkdir()
            for j in range(20):
                (sub / f"file_{j:03d}.txt").write_bytes(f"content {i} {j}".encode())
        hasher = BasicDirectoryHasher()
        result = hasher.hash_directory(tmp_path)
        assert isinstance(result, ContentHash)

    def test_rename_changes_hash(self, tmp_path):
        """Same content, different name → different hash."""
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "original.txt").write_bytes(b"content")
        (d2 / "renamed.txt").write_bytes(b"content")  # same content, different name
        hasher = BasicDirectoryHasher()
        assert hasher.hash_directory(d1) != hasher.hash_directory(d2)

    def test_ignore_applied_recursively(self, tmp_path):
        """ignore filter is applied to entries inside nested subdirectories."""
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "sub").mkdir()
        (d2 / "sub").mkdir()
        (d1 / "sub" / "app.py").write_bytes(b"code")
        (d2 / "sub" / "app.py").write_bytes(b"code")
        (d2 / "sub" / "app.pyc").write_bytes(b"compiled")  # nested .pyc, should be excluded
        hasher = BasicDirectoryHasher()
        assert hasher.hash_directory(d1) == hasher.hash_directory(d2, ignore=["*.pyc"])


class TestDirectoryHandler:
    @pytest.fixture
    def handler(self):
        return DirectoryHandler(BasicDirectoryHasher())

    @pytest.fixture
    def hasher(self):
        registry = PythonTypeHandlerRegistry()
        register_builtin_python_type_handlers(registry)
        return SemanticAwarePythonHasher(
            hasher_id="test_directory_v0",
            type_handler_registry=registry,
        )

    def test_returns_content_hash(self, handler, hasher, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        (d / "file.txt").write_bytes(b"content")
        result = handler.handle(Directory(d), hasher)
        assert isinstance(result, ContentHash)

    def test_rejects_non_directory_object(self, handler, hasher):
        with pytest.raises(TypeError, match="DirectoryHandler"):
            handler.handle("not_a_directory", hasher)

    def test_same_content_same_hash(self, handler, hasher, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "a.txt").write_bytes(b"identical")
        (d2 / "a.txt").write_bytes(b"identical")
        h1 = handler.handle(Directory(d1), hasher)
        h2 = handler.handle(Directory(d2), hasher)
        assert h1 == h2

    def test_different_content_different_hash(self, handler, hasher, tmp_path):
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "a.txt").write_bytes(b"content A")
        (d2 / "a.txt").write_bytes(b"content B")
        h1 = handler.handle(Directory(d1), hasher)
        h2 = handler.handle(Directory(d2), hasher)
        assert h1 != h2

    def test_passes_ignore_to_hasher(self, handler, hasher, tmp_path):
        """ignore on the Directory instance is forwarded to the hasher."""
        d1 = tmp_path / "d1"
        d2 = tmp_path / "d2"
        d1.mkdir()
        d2.mkdir()
        (d1 / "app.py").write_bytes(b"code")
        (d2 / "app.py").write_bytes(b"code")
        (d2 / "app.pyc").write_bytes(b"compiled")  # extra .pyc, should be excluded
        h1 = handler.handle(Directory(d1), hasher)
        h2 = handler.handle(Directory(d2, ignore=["*.pyc"]), hasher)
        assert h1 == h2
