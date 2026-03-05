"""
Integration tests verifying that file hashing is consistent across both paths:

1. **Arrow hasher path**: SemanticArrowHasher processes an Arrow table containing a
   path struct column → calls PathStructConverter.hash_struct_dict → file_hasher.
2. **Semantic hasher path**: BaseSemanticHasher hashes a Python Path object →
   calls PathContentHandler.handle → file_hasher.

Both paths must delegate to the same FileContentHasherProtocol so that identical
file content always produces identical hashes, regardless of entry point.
"""

from pathlib import Path

import pyarrow as pa
import pytest

from orcapod.hashing.arrow_hashers import SemanticArrowHasher
from orcapod.hashing.file_hashers import BasicFileHasher
from orcapod.hashing.semantic_hashing.builtin_handlers import (
    PathContentHandler,
    register_builtin_handlers,
)
from orcapod.hashing.semantic_hashing.semantic_hasher import BaseSemanticHasher
from orcapod.hashing.semantic_hashing.type_handler_registry import TypeHandlerRegistry
from orcapod.semantic_types.semantic_registry import SemanticTypeRegistry
from orcapod.semantic_types.semantic_struct_converters import PathStructConverter


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def file_hasher():
    """Single file hasher instance shared by both paths."""
    return BasicFileHasher(algorithm="sha256")


@pytest.fixture
def path_converter(file_hasher):
    return PathStructConverter(file_hasher=file_hasher)


@pytest.fixture
def arrow_hasher(path_converter):
    """SemanticArrowHasher wired with the shared file_hasher via PathStructConverter."""
    registry = SemanticTypeRegistry()
    registry.register_converter("path", path_converter)
    return SemanticArrowHasher(semantic_registry=registry)


@pytest.fixture
def semantic_hasher(file_hasher):
    """BaseSemanticHasher wired with the shared file_hasher via PathContentHandler."""
    registry = TypeHandlerRegistry()
    register_builtin_handlers(registry, file_hasher=file_hasher)
    return BaseSemanticHasher(
        hasher_id="test_v1", type_handler_registry=registry, strict=True
    )


# ---------------------------------------------------------------------------
# Arrow struct hasher: path column tests
# ---------------------------------------------------------------------------


class TestArrowStructPathHashing:
    """Tests for file hashing through the Arrow hasher path."""

    def test_same_content_different_paths_same_hash(
        self, arrow_hasher, tmp_path
    ):
        """Two distinct files with identical content produce the same table hash."""
        file1 = tmp_path / "a.txt"
        file2 = tmp_path / "b.txt"
        file1.write_text("identical content")
        file2.write_text("identical content")

        table1 = pa.table(
            {"file": [{"path": str(file1)}]},
            schema=pa.schema([pa.field("file", pa.struct([pa.field("path", pa.large_string())]))]),
        )
        table2 = pa.table(
            {"file": [{"path": str(file2)}]},
            schema=pa.schema([pa.field("file", pa.struct([pa.field("path", pa.large_string())]))]),
        )

        hash1 = arrow_hasher.hash_table(table1)
        hash2 = arrow_hasher.hash_table(table2)
        assert hash1.digest == hash2.digest

    def test_modified_content_different_hash(self, arrow_hasher, tmp_path):
        """Same path with modified content between hashes yields different hash."""
        file = tmp_path / "mutable.txt"
        file.write_text("version 1")

        schema = pa.schema([pa.field("file", pa.struct([pa.field("path", pa.large_string())]))])
        table_v1 = pa.table({"file": [{"path": str(file)}]}, schema=schema)
        hash1 = arrow_hasher.hash_table(table_v1)

        file.write_text("version 2")
        table_v2 = pa.table({"file": [{"path": str(file)}]}, schema=schema)
        hash2 = arrow_hasher.hash_table(table_v2)

        assert hash1.digest != hash2.digest

    def test_different_content_different_hash(self, arrow_hasher, tmp_path):
        """Two files with different content produce different table hashes."""
        file1 = tmp_path / "x.txt"
        file2 = tmp_path / "y.txt"
        file1.write_text("content A")
        file2.write_text("content B")

        schema = pa.schema([pa.field("file", pa.struct([pa.field("path", pa.large_string())]))])
        table1 = pa.table({"file": [{"path": str(file1)}]}, schema=schema)
        table2 = pa.table({"file": [{"path": str(file2)}]}, schema=schema)

        hash1 = arrow_hasher.hash_table(table1)
        hash2 = arrow_hasher.hash_table(table2)
        assert hash1.digest != hash2.digest


# ---------------------------------------------------------------------------
# Semantic hasher: Path object tests
# ---------------------------------------------------------------------------


class TestSemanticPathHashing:
    """Tests for file hashing through the semantic hasher path."""

    def test_same_content_different_paths_same_hash(
        self, semantic_hasher, tmp_path
    ):
        """Two distinct Path objects pointing to files with identical content."""
        file1 = tmp_path / "a.txt"
        file2 = tmp_path / "b.txt"
        file1.write_text("identical content")
        file2.write_text("identical content")

        hash1 = semantic_hasher.hash_object(Path(file1))
        hash2 = semantic_hasher.hash_object(Path(file2))
        assert hash1.digest == hash2.digest

    def test_modified_content_different_hash(self, semantic_hasher, tmp_path):
        """Same Path with modified content between hashes."""
        file = tmp_path / "mutable.txt"
        file.write_text("version 1")
        hash1 = semantic_hasher.hash_object(Path(file))

        file.write_text("version 2")
        hash2 = semantic_hasher.hash_object(Path(file))
        assert hash1.digest != hash2.digest

    def test_different_content_different_hash(self, semantic_hasher, tmp_path):
        """Two Paths pointing to different content produce different hashes."""
        file1 = tmp_path / "x.txt"
        file2 = tmp_path / "y.txt"
        file1.write_text("content A")
        file2.write_text("content B")

        hash1 = semantic_hasher.hash_object(Path(file1))
        hash2 = semantic_hasher.hash_object(Path(file2))
        assert hash1.digest != hash2.digest


# ---------------------------------------------------------------------------
# Cross-path consistency
# ---------------------------------------------------------------------------


class TestCrossPathConsistency:
    """Verify that the arrow hasher and semantic hasher use the same file_hasher
    and produce equivalent file content hashes for the same underlying file."""

    def test_arrow_and_semantic_hash_same_file_content(
        self, path_converter, semantic_hasher, file_hasher, tmp_path
    ):
        """The file content hash extracted by PathStructConverter.hash_struct_dict
        must match the ContentHash produced by PathContentHandler.handle (which
        the semantic hasher uses internally for Path objects).

        We compare at the file_hasher level: both paths ultimately call
        file_hasher.hash_file(path), so the raw digest must be identical.
        """
        file = tmp_path / "shared.txt"
        file.write_text("shared content for both paths")

        # Arrow path: PathStructConverter.hash_struct_dict (no prefix)
        arrow_hash_hex = path_converter.hash_struct_dict({"path": str(file)})

        # Semantic path: file_hasher.hash_file directly (same as PathContentHandler)
        semantic_content_hash = file_hasher.hash_file(file)

        assert arrow_hash_hex == semantic_content_hash.digest.hex()

    def test_arrow_and_semantic_same_content_two_files(
        self, path_converter, file_hasher, tmp_path
    ):
        """Two files with identical content: arrow struct hash_struct_dict and
        direct file_hasher.hash_file produce the same digest."""
        file1 = tmp_path / "file_arrow.txt"
        file2 = tmp_path / "file_semantic.txt"
        content = "same content for cross-path test"
        file1.write_text(content)
        file2.write_text(content)

        arrow_hex = path_converter.hash_struct_dict({"path": str(file1)})
        semantic_hex = file_hasher.hash_file(file2).digest.hex()

        assert arrow_hex == semantic_hex
