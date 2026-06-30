"""Tests for extension type column hashing via SemanticHashingVisitor."""

from __future__ import annotations

import pyarrow as pa
import pytest
from pathlib import Path

from orcapod.extension_types.file_type import File
from orcapod.hashing.visitors import SemanticHashingVisitor
from orcapod.contexts import get_default_context


@pytest.fixture
def ctx():
    return get_default_context()


class TestArrowTypeDataVisitorExtension:
    def test_visit_dispatches_to_visit_extension_for_extension_types(self, ctx, tmp_path):
        """visit() routes ExtensionType columns to visit_extension(), not visit_struct()."""
        # Create a real file so File can be constructed without errors
        real_file = tmp_path / "dummy.txt"
        real_file.write_text("dispatch test")

        arrow_type = ctx.type_converter.register_python_class(File)
        assert isinstance(arrow_type, pa.ExtensionType), (
            "File must be registered as an Arrow extension type"
        )
        storage_val = ctx.type_converter.python_to_storage(File(real_file), File)

        calls = []

        class TrackingVisitor(SemanticHashingVisitor):
            def visit_extension(self, ext_type, storage_value):
                calls.append("visit_extension")
                return super().visit_extension(ext_type, storage_value)

            def visit_struct(self, struct_type, data):
                calls.append("visit_struct")
                return super().visit_struct(struct_type, data)

        visitor = TrackingVisitor(ctx.type_converter, ctx.semantic_hasher)
        visitor.visit(arrow_type, storage_val)
        assert "visit_extension" in calls
        assert "visit_struct" not in calls


class TestSemanticHashingVisitorExtension:
    def test_file_column_hashed_to_large_binary(self, ctx, tmp_path):
        """File extension columns are replaced with pa.large_binary() hash tokens."""
        file = tmp_path / "test.txt"
        file.write_text("hello")

        arrow_type = ctx.type_converter.register_python_class(File)
        storage_val = ctx.type_converter.python_to_storage(File(file), File)

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        new_type, new_data = visitor.visit(arrow_type, storage_val)

        assert new_type == pa.large_binary()
        assert isinstance(new_data, bytes)

    def test_same_content_same_hash(self, ctx, tmp_path):
        """Two File objects pointing to files with identical content produce the same hash bytes."""
        file1 = tmp_path / "a.txt"
        file2 = tmp_path / "b.txt"
        file1.write_text("identical content")
        file2.write_text("identical content")

        arrow_type = ctx.type_converter.register_python_class(File)
        storage1 = ctx.type_converter.python_to_storage(File(file1), File)
        storage2 = ctx.type_converter.python_to_storage(File(file2), File)

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        _, hash1 = visitor.visit(arrow_type, storage1)
        _, hash2 = visitor.visit(arrow_type, storage2)

        assert hash1 == hash2

    def test_different_content_different_hash(self, ctx, tmp_path):
        """Files with different content produce different hash bytes."""
        file1 = tmp_path / "x.txt"
        file2 = tmp_path / "y.txt"
        file1.write_text("content A")
        file2.write_text("content B")

        arrow_type = ctx.type_converter.register_python_class(File)
        storage1 = ctx.type_converter.python_to_storage(File(file1), File)
        storage2 = ctx.type_converter.python_to_storage(File(file2), File)

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        _, hash1 = visitor.visit(arrow_type, storage1)
        _, hash2 = visitor.visit(arrow_type, storage2)

        assert hash1 != hash2

    def test_binary_encoding_format(self, ctx, tmp_path):
        """Hash bytes have format b'<type_name>::<method>:<digest>'."""
        file = tmp_path / "test.txt"
        file.write_text("test")

        arrow_type = ctx.type_converter.register_python_class(File)
        storage_val = ctx.type_converter.python_to_storage(File(file), File)

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        _, hash_bytes = visitor.visit(arrow_type, storage_val)

        assert b"::" in hash_bytes
        type_prefix, hash_part = hash_bytes.split(b"::", 1)
        # Extension name "orcapod.file" → dots replaced with colons
        assert type_prefix == b"orcapod:file"
        # hash_part should be "method:digest" — at least one colon
        assert b":" in hash_part

    def test_null_value_passthrough(self, ctx):
        """Null storage values pass through as-is."""
        arrow_type = ctx.type_converter.register_python_class(File)

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        new_type, new_data = visitor.visit(arrow_type, None)

        assert new_type == arrow_type
        assert new_data is None

    def test_unregistered_python_type_passes_through(self, ctx):
        """Extension types with no registered semantic hasher pass through unchanged."""
        import uuid
        from orcapod.hashing.semantic_hashing.type_handler_registry import PythonTypeHandlerRegistry
        from orcapod.hashing.semantic_hashing.semantic_hasher import SemanticAwarePythonHasher

        # Build a hasher with a registry that has NO entry for UUID
        empty_registry = PythonTypeHandlerRegistry()
        stripped_hasher = SemanticAwarePythonHasher(
            hasher_id="test_v0",
            type_handler_registry=empty_registry,
        )

        arrow_type = ctx.type_converter.register_python_class(uuid.UUID)
        storage_val = ctx.type_converter.python_to_storage(uuid.UUID("12345678-1234-5678-1234-567812345678"), uuid.UUID)

        visitor = SemanticHashingVisitor(ctx.type_converter, stripped_hasher)
        new_type, new_data = visitor.visit(arrow_type, storage_val)

        # Should be completely unchanged since UUID has no semantic hasher
        assert new_type == arrow_type
        assert new_data == storage_val

    def test_path_column_passthrough_when_no_handler_registered(self, ctx):
        """Path extension columns pass through unchanged (no file reading).

        Since PathHandler is not registered, the visitor has no semantic hasher
        for pathlib.Path. The column is returned as-is — the extension type and
        storage value are unchanged. A nonexistent path raises no error because
        no file content is read.
        """
        # Deliberately use a nonexistent path — proves no file IO happens
        nonexistent = Path("/nonexistent/path/that/does/not/exist.txt")

        arrow_type = ctx.type_converter.register_python_class(Path)
        storage_val = ctx.type_converter.python_to_storage(nonexistent, Path)

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        # Must not raise FileNotFoundError or any other error
        new_type, new_data = visitor.visit(arrow_type, storage_val)

        # Path has no handler → visitor passes through unchanged
        assert new_type == arrow_type
        assert new_data == storage_val


class TestCrossPathConsistency:
    """Verify that the Arrow visitor path and the direct Python hasher path produce
    identical hash tokens for the same underlying file content.

    The Arrow path (SemanticHashingVisitor.visit_extension) converts the extension
    storage value back to a Python object and calls semantic_hasher.hash_object —
    exactly the same call as the direct Python path. These tests make that
    structural guarantee explicit and regression-proof.

    Hash encoding:
    - Arrow path produces: b"<type_name>::<method>:<raw_digest>"
    - Python path produces: ContentHash with to_prefixed_digest() → b"<method>:<raw_digest>"
    Stripping the type-name prefix from the Arrow encoding yields an identical
    b"<method>:<raw_digest>" byte string.
    """

    def test_arrow_and_semantic_hash_same_file_content(self, ctx, tmp_path):
        """Arrow visitor path and direct Python hasher path embed the same digest."""
        file = tmp_path / "shared.txt"
        file.write_text("shared content for both paths")

        arrow_type = ctx.type_converter.register_python_class(File)
        storage_val = ctx.type_converter.python_to_storage(File(file), File)

        # Arrow path: visit_extension encodes as b"<type_name>::<method>:<digest>"
        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        _, arrow_hash_bytes = visitor.visit(arrow_type, storage_val)
        # Strip the "orcapod:file::" type prefix to get b"<method>:<digest>"
        prefixed_from_arrow = arrow_hash_bytes.split(b"::", 1)[1]

        # Python path: hash_object returns ContentHash directly
        python_content_hash = ctx.semantic_hasher.hash_object(File(file))
        prefixed_from_python = python_content_hash.to_prefixed_digest()

        assert prefixed_from_arrow == prefixed_from_python

    def test_same_content_two_files_cross_path(self, ctx, tmp_path):
        """Two files with identical content: Arrow path and Python path agree."""
        file_arrow = tmp_path / "file_arrow.txt"
        file_python = tmp_path / "file_python.txt"
        content = "same content for cross-path test"
        file_arrow.write_text(content)
        file_python.write_text(content)

        arrow_type = ctx.type_converter.register_python_class(File)
        storage_val = ctx.type_converter.python_to_storage(File(file_arrow), File)

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        _, arrow_hash_bytes = visitor.visit(arrow_type, storage_val)
        prefixed_from_arrow = arrow_hash_bytes.split(b"::", 1)[1]

        python_content_hash = ctx.semantic_hasher.hash_object(File(file_python))
        prefixed_from_python = python_content_hash.to_prefixed_digest()

        assert prefixed_from_arrow == prefixed_from_python
