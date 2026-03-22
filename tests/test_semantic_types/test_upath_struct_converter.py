from pathlib import Path
from typing import cast

import pytest
from upath import UPath

from orcapod.hashing.file_hashers import BasicFileHasher
from orcapod.semantic_types.semantic_struct_converters import UPathStructConverter


@pytest.fixture
def file_hasher():
    return BasicFileHasher(algorithm="sha256")


@pytest.fixture
def converter(file_hasher):
    return UPathStructConverter(file_hasher=file_hasher)


def test_upath_to_struct_and_back(converter):
    path_obj = UPath("/tmp/test.txt")
    struct_dict = converter.python_to_struct_dict(path_obj)
    assert struct_dict["upath"] == str(path_obj)
    restored = converter.struct_dict_to_python(struct_dict)
    assert isinstance(restored, UPath)
    assert str(restored) == str(path_obj)


def test_upath_to_struct_invalid_type(converter):
    with pytest.raises(TypeError):
        converter.python_to_struct_dict(Path("/tmp/test.txt"))  # type: ignore


def test_struct_to_python_missing_field(converter):
    with pytest.raises(ValueError):
        converter.struct_dict_to_python({})


def test_can_handle_python_type(converter):
    assert converter.can_handle_python_type(UPath)
    assert not converter.can_handle_python_type(str)
    assert not converter.can_handle_python_type(Path)


def test_can_handle_struct_type(converter):
    struct_type = converter.arrow_struct_type
    assert converter.can_handle_struct_type(struct_type)


def test_is_semantic_struct(converter):
    assert converter.is_semantic_struct({"upath": "/tmp/test.txt"})
    assert not converter.is_semantic_struct({"path": "/tmp/test.txt"})
    assert not converter.is_semantic_struct({"upath": 123})


def test_hash_struct_dict_file_not_found(converter, tmp_path):
    struct_dict = {"upath": str(tmp_path / "does_not_exist.txt")}
    with pytest.raises(FileNotFoundError):
        converter.hash_struct_dict(struct_dict)


def test_hash_struct_dict_is_directory(converter, tmp_path):
    struct_dict = {"upath": str(tmp_path)}
    with pytest.raises(IsADirectoryError):
        converter.hash_struct_dict(struct_dict)


def test_hash_struct_dict_content_based(converter, tmp_path):
    """Two distinct files with identical content produce the same hash."""
    file1 = tmp_path / "file1.txt"
    file2 = tmp_path / "file2.txt"
    content = "identical content"
    file1.write_text(content)
    file2.write_text(content)
    hash1 = converter.hash_struct_dict({"upath": str(file1)})
    hash2 = converter.hash_struct_dict({"upath": str(file2)})
    assert hash1 == hash2


def test_hash_struct_dict_with_prefix(converter, tmp_path):
    """Prefixed hash starts with 'upath:sha256:'."""
    file = tmp_path / "file.txt"
    file.write_text("hello")
    hash_str = converter.hash_struct_dict({"upath": str(file)}, add_prefix=True)
    assert hash_str.startswith("upath:sha256:")


def test_hash_struct_dict_different_content(converter, tmp_path):
    """Same path with modified content yields a different hash."""
    file = tmp_path / "mutable.txt"
    file.write_text("version 1")
    hash1 = converter.hash_struct_dict({"upath": str(file)})
    file.write_text("version 2")
    hash2 = converter.hash_struct_dict({"upath": str(file)})
    assert hash1 != hash2


def test_hash_struct_dict_missing_field(converter):
    with pytest.raises(ValueError, match="Missing 'upath' field"):
        converter.hash_struct_dict({})


def test_upath_arrow_struct_type(converter):
    """The Arrow struct type has a single 'upath' field of large_string."""
    import pyarrow as pa

    struct_type = converter.arrow_struct_type
    assert isinstance(struct_type, pa.StructType)
    assert len(struct_type) == 1
    assert struct_type[0].name == "upath"
    assert struct_type[0].type == pa.large_string()


def test_path_and_upath_struct_types_differ():
    """Path and UPath converters produce distinct Arrow struct types."""
    from orcapod.semantic_types.semantic_struct_converters import PathStructConverter

    file_hasher = BasicFileHasher(algorithm="sha256")
    path_conv = PathStructConverter(file_hasher=file_hasher)
    upath_conv = UPathStructConverter(file_hasher=file_hasher)

    assert path_conv.arrow_struct_type != upath_conv.arrow_struct_type
    assert path_conv.arrow_struct_type[0].name == "path"
    assert upath_conv.arrow_struct_type[0].name == "upath"
