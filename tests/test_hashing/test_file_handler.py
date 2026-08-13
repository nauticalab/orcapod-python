"""Tests for FileHandler — content hashing for orcapod.File."""

from __future__ import annotations

import pytest

from orcapod.logical_types.file_type import File
from orcapod.hashing.file_hashers import FileHasher
from orcapod.hashing.semantic_hashing.builtin_handlers import FileHandler, register_builtin_python_type_handlers
from orcapod.hashing.semantic_hashing.semantic_hasher import SemanticAwarePythonHasher
from orcapod.hashing.semantic_hashing.type_handler_registry import PythonTypeHandlerRegistry
from orcapod.types import ContentHash


@pytest.fixture
def file_hasher():
    return FileHasher(algorithm="sha256")


@pytest.fixture
def handler(file_hasher):
    return FileHandler(file_hasher)


@pytest.fixture
def hasher():
    registry = PythonTypeHandlerRegistry()
    register_builtin_python_type_handlers(registry)
    return SemanticAwarePythonHasher(
        hasher_id="test_file_v0",
        type_handler_registry=registry,
    )


class TestFileHandler:
    def test_returns_content_hash(self, handler, hasher, tmp_path):
        p = tmp_path / "a.txt"
        p.write_text("hello")
        f = File(p)
        result = handler.handle(f, hasher)
        assert isinstance(result, ContentHash)

    def test_same_content_same_hash(self, handler, hasher, tmp_path):
        p1 = tmp_path / "a.txt"
        p2 = tmp_path / "b.txt"
        p1.write_bytes(b"identical")
        p2.write_bytes(b"identical")
        h1 = handler.handle(File(p1), hasher)
        h2 = handler.handle(File(p2), hasher)
        assert h1 == h2

    def test_different_content_different_hash(self, handler, hasher, tmp_path):
        p1 = tmp_path / "a.txt"
        p2 = tmp_path / "b.txt"
        p1.write_bytes(b"content A")
        p2.write_bytes(b"content B")
        h1 = handler.handle(File(p1), hasher)
        h2 = handler.handle(File(p2), hasher)
        assert h1 != h2

    def test_zero_byte_file_produces_hash(self, handler, hasher, tmp_path):
        p = tmp_path / "empty.txt"
        p.write_bytes(b"")
        f = File(p)
        result = handler.handle(f, hasher)
        assert isinstance(result, ContentHash)

    def test_zero_byte_file_hash_is_consistent(self, handler, hasher, tmp_path):
        p1 = tmp_path / "empty1.txt"
        p2 = tmp_path / "empty2.txt"
        p1.write_bytes(b"")
        p2.write_bytes(b"")
        h1 = handler.handle(File(p1), hasher)
        h2 = handler.handle(File(p2), hasher)
        assert h1 == h2

    def test_hash_matches_direct_sha256(self, handler, hasher, tmp_path):
        """FileHandler must produce the same digest as FileHasher(sha256) directly."""
        content = b"migration compatibility check"
        p = tmp_path / "compat.txt"
        p.write_bytes(content)
        f = File(p)
        handler_result = handler.handle(f, hasher)
        direct_result = FileHasher(algorithm="sha256").hash_file(p)
        assert handler_result == direct_result

    def test_rejects_non_file_object(self, handler, hasher):
        with pytest.raises(TypeError, match="FileHandler"):
            handler.handle("not_a_file", hasher)
