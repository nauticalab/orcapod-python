"""Tests for extension type column hashing via SemanticHashingVisitor."""

from __future__ import annotations

import uuid
from dataclasses import dataclass

import pyarrow as pa
import pytest
from pathlib import Path

from orcapod.logical_types.file_type import File
from orcapod.logical_types.builtin_logical_types import LogicalPath
from orcapod.logical_types.list_logical_type_factory import ListLogicalType
from orcapod.hashing.visitors import SemanticHashingVisitor
from orcapod.hashing.semantic_hashing.type_handler_registry import PythonTypeHandlerRegistry
from orcapod.hashing.semantic_hashing.semantic_hasher import SemanticAwarePythonHasher
from orcapod.contexts import get_default_context


# Module-level dataclass required for registration (local classes are rejected
# because they have no stable fully-qualified class name).
@dataclass
class FileBundle:
    """Test dataclass with a list[File] field for TestListExtensionHashing."""

    name: str
    files: list[File]


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


class TestListExtensionHashing:
    """Regression and contract tests for ITL-627 Defect 2.

    Before Fix 2, visit_extension short-circuited on `not isinstance(python_type, type)`
    for list[File] (a GenericAlias), returning the extension type unchanged — raw JSON
    path strings were hashed instead of file contents.
    """

    def _make_list_file_ext_type(self, ctx):
        """Return the ``extension<list[orcapod.file]>`` Arrow type via the type converter.

        Idempotent: ``register_python_class`` is a no-op if the type is already registered.
        """
        ctx.type_converter.register_python_class(list[File])
        return ctx.type_converter.python_type_to_arrow_type(list[File])

    def _make_scalar_file_ext_type(self, ctx):
        """Return the ``extension<orcapod.file>`` Arrow type.

        Idempotent: ``register_python_class`` is a no-op if the type is already registered.
        """
        return ctx.type_converter.register_python_class(File)

    def _file_storage(self, ctx, path):
        """Return the large_string storage value for a File."""
        return ctx.type_converter.python_to_storage(File(path), File)

    def test_list_file_extension_hashed_to_large_binary(self, ctx, tmp_path):
        """visit_extension for extension<list[orcapod.file]> must return
        (large_binary, bytes) — not the extension type unchanged.

        The result is a single bytes value combining the outer extension name and
        the per-element content hashes, mirroring the scalar encoding:
        ``b"<outer_type_name>::<elem0_hash>\\x00<elem1_hash>"``.

        With the original buggy code the isinstance guard exits immediately,
        returning (extension_type, storage_value). This assertion on new_type
        would fail.
        """
        f0 = tmp_path / "f0.txt"; f0.write_text("alpha")
        f1 = tmp_path / "f1.txt"; f1.write_text("beta")

        list_ext_type = self._make_list_file_ext_type(ctx)
        s0 = self._file_storage(ctx, f0)
        s1 = self._file_storage(ctx, f1)
        storage_value = [s0, s1]

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        new_type, new_data = visitor.visit(list_ext_type, storage_value)

        assert new_type == pa.large_binary(), (
            f"Expected large_binary(), got {new_type}. "
            "The list result is a single combined hash bytes value."
        )
        assert isinstance(new_data, bytes)
        # Combined format: b"<outer_type_name>::<elem_hashes>"
        assert b"::" in new_data
        type_prefix, _ = new_data.split(b"::", 1)
        assert type_prefix == b"list[orcapod:file]"

    def test_list_file_extension_embeds_per_element_scalar_hashes(self, ctx, tmp_path):
        """The combined list hash embeds each element's scalar hash.

        Combined format: b"<outer_type_name>::<h0>\\x00<h1>"
        where h0 and h1 are the scalar visit results for the same files.
        Splitting the suffix by \\x00 recovers individual element hashes.
        """
        f0 = tmp_path / "contract0.txt"; f0.write_text("content zero")
        f1 = tmp_path / "contract1.txt"; f1.write_text("content one")

        scalar_ext_type = self._make_scalar_file_ext_type(ctx)
        list_ext_type = self._make_list_file_ext_type(ctx)
        s0 = self._file_storage(ctx, f0)
        s1 = self._file_storage(ctx, f1)

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)

        # Scalar hashes — b"orcapod:file::<method>:<digest>" each
        _, h0_bytes = visitor.visit(scalar_ext_type, s0)
        _, h1_bytes = visitor.visit(scalar_ext_type, s1)

        # List hash — b"list[orcapod:file]::<h0>\x00<h1>"
        _, combined = visitor.visit(list_ext_type, [s0, s1])

        # Strip outer type prefix and split to recover per-element hashes
        suffix = combined.split(b"::", 1)[1]  # b"<h0>\x00<h1>"
        elem_hashes = suffix.split(b"\x00")
        assert elem_hashes[0] == h0_bytes, (
            "Embedded element 0 must equal the scalar hash of file 0"
        )
        assert elem_hashes[1] == h1_bytes, (
            "Embedded element 1 must equal the scalar hash of file 1"
        )

    def test_list_file_extension_content_change_changes_hash(self, ctx, tmp_path):
        """Changing file content changes the combined hash."""
        f = tmp_path / "mutable.txt"
        f.write_text("v1")

        list_ext_type = self._make_list_file_ext_type(ctx)
        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)

        s_v1 = self._file_storage(ctx, f)
        _, result_v1 = visitor.visit(list_ext_type, [s_v1])

        f.write_text("v2")
        s_v2 = ctx.type_converter.python_to_storage(File(f), File)
        _, result_v2 = visitor.visit(list_ext_type, [s_v2])

        assert result_v1 != result_v2, "Content change must change the combined hash"

    def test_list_file_extension_same_content_same_hash(self, ctx, tmp_path):
        """Two files with identical content produce identical combined hashes."""
        fa = tmp_path / "a.txt"; fa.write_text("identical")
        fb = tmp_path / "b.txt"; fb.write_text("identical")

        list_ext_type = self._make_list_file_ext_type(ctx)
        sa = self._file_storage(ctx, fa)
        sb = self._file_storage(ctx, fb)

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        _, result_a = visitor.visit(list_ext_type, [sa])
        _, result_b = visitor.visit(list_ext_type, [sb])

        assert result_a == result_b, (
            "Same content at different paths must produce the same combined hash"
        )

    def test_list_file_extension_passthrough_when_no_handler(self, ctx, tmp_path):
        """When the registry has no FileHandler, visit_extension must passthrough."""
        empty_registry = PythonTypeHandlerRegistry()
        stripped_hasher = SemanticAwarePythonHasher(
            hasher_id="test_v0",
            type_handler_registry=empty_registry,
        )

        f = tmp_path / "f.txt"; f.write_text("test")
        list_ext_type = self._make_list_file_ext_type(ctx)
        storage_value = [self._file_storage(ctx, f)]

        visitor = SemanticHashingVisitor(ctx.type_converter, stripped_hasher)
        new_type, new_data = visitor.visit(list_ext_type, storage_value)

        assert new_type == list_ext_type, "Must passthrough extension type when no handler"
        assert new_data == storage_value, "Must passthrough storage value when no handler"

    def test_list_path_extension_passthrough(self, ctx, tmp_path):
        """extension<list[orcapod.path]> must passthrough — Path has no content handler."""
        lt = ListLogicalType(LogicalPath(), is_set=False)
        list_ext_type = lt.get_arrow_extension_type()
        storage_value = ["/a.txt", "/b.txt"]

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        new_type, new_data = visitor.visit(list_ext_type, storage_value)

        assert new_type == list_ext_type, "Path has no handler — must passthrough"
        assert new_data == storage_value

    def test_set_file_extension_hashed_to_large_binary(self, ctx, tmp_path):
        """set[File] (extension<set[orcapod.file]>) also hashes to a combined large_binary.

        get_origin(set[File]) is `set`, covered by `in (list, set)` in the fix.
        The type must be registered with the converter before visiting so that
        ``arrow_type_to_python_type`` can resolve it.
        """
        f0 = tmp_path / "s0.txt"; f0.write_text("set alpha")
        f1 = tmp_path / "s1.txt"; f1.write_text("set beta")

        ctx.type_converter.register_python_class(set[File])
        set_ext_type = ctx.type_converter.python_type_to_arrow_type(set[File])
        s0 = self._file_storage(ctx, f0)
        s1 = self._file_storage(ctx, f1)
        storage_value = [s0, s1]

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        new_type, new_data = visitor.visit(set_ext_type, storage_value)

        assert new_type == pa.large_binary()
        assert isinstance(new_data, bytes)
        # Outer type name is "set[orcapod:file]" (dots replaced with colons)
        assert new_data.startswith(b"set[orcapod:file]::")

    def test_list_and_set_file_extension_produce_distinct_hashes(self, ctx, tmp_path):
        """list[File] and set[File] with identical contents must hash differently.

        Before the fix, the outer extension name was not included in the result,
        so ``extension<list[orcapod.file]>`` and ``extension<set[orcapod.file]>``
        tables holding the same file produced identical hashes — a silent hash
        collision that allows memoised records keyed on one to be served for the other.
        """
        f = tmp_path / "same.txt"; f.write_text("collision test")

        ctx.type_converter.register_python_class(list[File])
        ctx.type_converter.register_python_class(set[File])
        list_ext_type = ctx.type_converter.python_type_to_arrow_type(list[File])
        set_ext_type = ctx.type_converter.python_type_to_arrow_type(set[File])

        s = self._file_storage(ctx, f)
        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)

        _, list_hash = visitor.visit(list_ext_type, [s])
        _, set_hash = visitor.visit(set_ext_type, [s])

        assert list_hash != set_hash, (
            "list[File] and set[File] with identical content must produce distinct hashes"
        )

    def test_list_list_file_extension_hashed_recursively(self, ctx, tmp_path):
        """extension<list[list[orcapod.file]]> recurses to a single combined large_binary.

        Recursion: outer visit_extension delegates to _visit_list_elements with
        virtual_type=large_list(extension<list[orcapod.file]>), which calls
        visit(extension<list[orcapod.file]>, inner_list) for each element, which
        recurses back into visit_extension, each returning (large_binary, inner_bytes).
        The outer call then combines: b"<outer_name>::<inner0>\\x00<inner1>".

        Both the inner and outer list types must be registered with the converter so
        that ``arrow_type_to_python_type`` can resolve them.
        """
        f0 = tmp_path / "n0.txt"; f0.write_text("nested zero")
        f1 = tmp_path / "n1.txt"; f1.write_text("nested one")
        f2 = tmp_path / "n2.txt"; f2.write_text("nested two")

        # Register both inner and outer list types so the converter knows them.
        ctx.type_converter.register_python_class(list[File])
        ctx.type_converter.register_python_class(list[list[File]])
        outer_ext_type = ctx.type_converter.python_type_to_arrow_type(list[list[File]])

        s0 = self._file_storage(ctx, f0)
        s1 = self._file_storage(ctx, f1)
        s2 = self._file_storage(ctx, f2)
        # One row: [[s0, s1], [s2]]
        storage_value = [[s0, s1], [s2]]

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        new_type, new_data = visitor.visit(outer_ext_type, storage_value)

        assert new_type == pa.large_binary(), (
            f"Expected large_binary(), got {new_type}"
        )
        assert isinstance(new_data, bytes)
        # Outer type prefix encodes the nesting depth
        assert b"list[" in new_data[:30]
        assert b"::" in new_data

    def test_dataclass_with_list_file_field_hashed(self, ctx, tmp_path):
        """A struct with a list[File] extension field must hash per element.

        When visiting a struct type whose ``files`` field is
        ``extension<list[orcapod.file]>``, ``visit_struct`` recurses into each
        field via ``visit(field_type, field_data)``. Fix 2 handles the resulting
        ``visit_extension`` dispatch correctly.

        Note: Dataclasses registered via ``register_python_class`` are backed by
        Arrow extension types with struct storage.  Because ``FileBundle`` has no
        semantic handler, ``visit_extension`` passes through the extension type
        unchanged and does NOT recurse into the struct's fields. To exercise
        ``visit_struct`` → ``visit_extension(list[File])`` recursion we must
        visit the *storage struct type* directly.

        Note: Uses module-level ``FileBundle`` — local dataclasses are rejected
        because they have no stable fully-qualified class name.
        """
        # Register the dataclass to get its Arrow extension type.
        ctx.type_converter.register_python_class(FileBundle)
        ext_type = ctx.type_converter.python_type_to_arrow_type(FileBundle)
        # The storage is a struct<name: large_string, files: large_list<item: large_string>>.
        # We need to reconstruct the struct type with the list[File] extension type
        # for the `files` field so that visit_struct -> visit_extension(list[File]) fires.
        list_file_ext_type = self._make_list_file_ext_type(ctx)
        struct_type = pa.struct([
            pa.field("name", pa.large_utf8()),
            pa.field("files", list_file_ext_type),
        ])

        f0 = tmp_path / "dc0.txt"; f0.write_text("dc alpha")
        f1 = tmp_path / "dc1.txt"; f1.write_text("dc beta")

        s0 = ctx.type_converter.python_to_storage(File(f0), File)
        s1 = ctx.type_converter.python_to_storage(File(f1), File)
        storage_value = {"name": "bundle", "files": [s0, s1]}

        visitor = SemanticHashingVisitor(ctx.type_converter, ctx.semantic_hasher)
        new_type, new_data = visitor.visit(struct_type, storage_value)

        # The `files` field should be hashed to large_binary (single combined value)
        files_field_type = new_type.field("files").type
        assert files_field_type == pa.large_binary(), (
            f"files field must be large_binary(), got {files_field_type}"
        )
        files_combined = new_data["files"]
        assert isinstance(files_combined, bytes)
        assert files_combined.startswith(b"list[orcapod:file]::")
