"""Tests for extension type column hashing via SemanticHashingVisitor."""

from __future__ import annotations

import pyarrow as pa
import pytest
from pathlib import Path

from orcapod.hashing.visitors import SemanticHashingVisitor
from orcapod.contexts import get_default_context


@pytest.fixture
def ctx():
    return get_default_context()


class TestArrowTypeDataVisitorExtension:
    def test_visit_dispatches_to_visit_extension_for_extension_types(self, ctx, tmp_path):
        """visit() routes ExtensionType columns to visit_extension(), not visit_struct()."""
        # Create a real file so visit_extension can complete without errors
        real_file = tmp_path / "dummy.txt"
        real_file.write_text("dispatch test")

        arrow_type = ctx.type_converter.register_python_class(Path)
        assert isinstance(arrow_type, pa.ExtensionType), (
            "Path must be registered as an Arrow extension type"
        )
        storage_val = ctx.type_converter.python_to_storage(Path(real_file), Path)

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
    def test_path_column_hashed_to_large_binary(self, ctx, tmp_path):
        """Path extension columns are replaced with pa.large_binary() hash tokens."""
        file = tmp_path / "test.txt"
        file.write_text("hello")

        arrow_type = ctx.type_converter.register_python_class(Path)
        storage_val = ctx.type_converter.python_to_storage(Path(file), Path)

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        new_type, new_data = visitor.visit(arrow_type, storage_val)

        assert new_type == pa.large_binary()
        assert isinstance(new_data, bytes)

    def test_same_content_same_hash(self, ctx, tmp_path):
        """Two paths pointing to files with identical content produce the same hash bytes."""
        file1 = tmp_path / "a.txt"
        file2 = tmp_path / "b.txt"
        file1.write_text("identical content")
        file2.write_text("identical content")

        arrow_type = ctx.type_converter.register_python_class(Path)
        storage1 = ctx.type_converter.python_to_storage(Path(file1), Path)
        storage2 = ctx.type_converter.python_to_storage(Path(file2), Path)

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

        arrow_type = ctx.type_converter.register_python_class(Path)
        storage1 = ctx.type_converter.python_to_storage(Path(file1), Path)
        storage2 = ctx.type_converter.python_to_storage(Path(file2), Path)

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        _, hash1 = visitor.visit(arrow_type, storage1)
        _, hash2 = visitor.visit(arrow_type, storage2)

        assert hash1 != hash2

    def test_binary_encoding_format(self, ctx, tmp_path):
        """Hash bytes have format b'<type_name>::<method>:<digest>'."""
        file = tmp_path / "test.txt"
        file.write_text("test")

        arrow_type = ctx.type_converter.register_python_class(Path)
        storage_val = ctx.type_converter.python_to_storage(Path(file), Path)

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        _, hash_bytes = visitor.visit(arrow_type, storage_val)

        assert b"::" in hash_bytes
        type_prefix, hash_part = hash_bytes.split(b"::", 1)
        # Extension name "orcapod.path" → dots replaced with colons
        assert type_prefix == b"orcapod:path"
        # hash_part should be "method:digest" — at least one colon
        assert b":" in hash_part

    def test_null_value_passthrough(self, ctx):
        """Null storage values pass through as-is."""
        arrow_type = ctx.type_converter.register_python_class(Path)

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        new_type, new_data = visitor.visit(arrow_type, None)

        assert new_type == arrow_type
        assert new_data is None
