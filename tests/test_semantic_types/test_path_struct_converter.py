from pathlib import Path
from typing import cast

import pytest

from orcapod.hashing.file_hashers import BasicFileHasher
from orcapod.semantic_types.semantic_struct_converters import PathStructConverter


@pytest.fixture
def file_hasher():
    return BasicFileHasher(algorithm="sha256")


@pytest.fixture
def converter(file_hasher):
    return PathStructConverter(file_hasher=file_hasher)


def test_path_to_struct_and_back(converter):
    path_obj = Path("/tmp/test.txt")
    struct_dict = converter.python_to_struct_dict(path_obj)
    assert struct_dict["path"] == str(path_obj)
    restored = converter.struct_dict_to_python(struct_dict)
    assert restored == path_obj


def test_path_to_struct_invalid_type(converter):
    with pytest.raises(TypeError):
        converter.python_to_struct_dict("not_a_path")  # type: ignore


def test_struct_to_python_missing_field(converter):
    with pytest.raises(ValueError):
        converter.struct_dict_to_python({})


def test_can_handle_python_type(converter):
    assert converter.can_handle_python_type(Path)
    assert not converter.can_handle_python_type(str)


def test_can_handle_struct_type(converter):
    struct_type = converter.arrow_struct_type
    assert converter.can_handle_struct_type(struct_type)

    # Should fail for wrong fields
    class FakeField:
        def __init__(self, name, type):
            self.name = name
            self.type = type

    class FakeStructType(list):
        @property
        def names(self):
            return [f.name for f in self]

        pass

    import pyarrow as pa

    fake_struct = cast(
        pa.StructType, FakeStructType([FakeField("wrong", struct_type[0].type)])
    )
    assert not converter.can_handle_struct_type(fake_struct)


def test_is_semantic_struct(converter):
    assert converter.is_semantic_struct({"path": "/tmp/test.txt"})
    assert not converter.is_semantic_struct({"not_path": "value"})
    assert not converter.is_semantic_struct({"path": 123})


def test_hash_struct_dict_file_not_found(converter, tmp_path):
    struct_dict = {"path": str(tmp_path / "does_not_exist.txt")}
    with pytest.raises(FileNotFoundError):
        converter.hash_struct_dict(struct_dict)


def test_hash_struct_dict_is_directory(converter, tmp_path):
    struct_dict = {"path": str(tmp_path)}
    with pytest.raises(IsADirectoryError):
        converter.hash_struct_dict(struct_dict)


def test_hash_struct_dict_content_based(converter, tmp_path):
    """Two distinct files with identical content produce the same hash."""
    file1 = tmp_path / "file1.txt"
    file2 = tmp_path / "file2.txt"
    content = "identical content"
    file1.write_text(content)
    file2.write_text(content)
    hash1 = converter.hash_struct_dict({"path": str(file1)})
    hash2 = converter.hash_struct_dict({"path": str(file2)})
    assert hash1 == hash2


def test_hash_path_objects_content_based(converter, tmp_path):
    """Round-trip through python_to_struct_dict then hash_struct_dict."""
    file1 = tmp_path / "fileA.txt"
    file2 = tmp_path / "fileB.txt"
    content = "same file content"
    file1.write_text(content)
    file2.write_text(content)
    struct_dict1 = converter.python_to_struct_dict(Path(file1))
    struct_dict2 = converter.python_to_struct_dict(Path(file2))
    hash1 = converter.hash_struct_dict(struct_dict1)
    hash2 = converter.hash_struct_dict(struct_dict2)
    assert hash1 == hash2


def test_hash_struct_dict_with_prefix(converter, tmp_path):
    """Prefixed hash starts with 'path:sha256:'."""
    file = tmp_path / "file.txt"
    file.write_text("hello")
    hash_str = converter.hash_struct_dict({"path": str(file)}, add_prefix=True)
    assert hash_str.startswith("path:sha256:")


def test_hash_struct_dict_different_content(converter, tmp_path):
    """Same path with modified content yields a different hash."""
    file = tmp_path / "mutable.txt"
    file.write_text("version 1")
    hash1 = converter.hash_struct_dict({"path": str(file)})
    file.write_text("version 2")
    hash2 = converter.hash_struct_dict({"path": str(file)})
    assert hash1 != hash2


def test_hash_struct_dict_missing_path_field(converter):
    with pytest.raises(ValueError, match="Missing 'path' field"):
        converter.hash_struct_dict({})
