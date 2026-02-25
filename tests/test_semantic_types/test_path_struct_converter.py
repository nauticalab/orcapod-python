from pathlib import Path
from typing import cast
from unittest.mock import patch

import pytest

from orcapod.semantic_types.semantic_struct_converters import PathStructConverter


def test_path_to_struct_and_back():
    converter = PathStructConverter()
    path_obj = Path("/tmp/test.txt")
    struct_dict = converter.python_to_struct_dict(path_obj)
    assert struct_dict["path"] == str(path_obj)
    restored = converter.struct_dict_to_python(struct_dict)
    assert restored == path_obj


def test_path_to_struct_invalid_type():
    converter = PathStructConverter()
    with pytest.raises(TypeError):
        converter.python_to_struct_dict("not_a_path")  # type: ignore


def test_struct_to_python_missing_field():
    converter = PathStructConverter()
    with pytest.raises(ValueError):
        converter.struct_dict_to_python({})


def test_can_handle_python_type():
    converter = PathStructConverter()
    assert converter.can_handle_python_type(Path)
    assert not converter.can_handle_python_type(str)


def test_can_handle_struct_type():
    converter = PathStructConverter()
    struct_type = converter.arrow_struct_type
    assert converter.can_handle_struct_type(struct_type)

    # Should fail for wrong fields
    class FakeField:
        def __init__(self, name, type):
            self.name = name
            self.type = type

    class FakeStructType(list):
        pass

    import pyarrow as pa

    fake_struct = cast(
        pa.StructType, FakeStructType([FakeField("wrong", struct_type[0].type)])
    )
    assert not converter.can_handle_struct_type(fake_struct)


def test_is_semantic_struct():
    converter = PathStructConverter()
    assert converter.is_semantic_struct({"path": "/tmp/test.txt"})
    assert not converter.is_semantic_struct({"not_path": "value"})
    assert not converter.is_semantic_struct({"path": 123})


def test_hash_struct_dict_file_not_found(tmp_path):
    converter = PathStructConverter()
    struct_dict = {"path": str(tmp_path / "does_not_exist.txt")}
    with pytest.raises(FileNotFoundError):
        converter.hash_struct_dict(struct_dict)


def test_hash_struct_dict_permission_error(tmp_path):
    converter = PathStructConverter()
    file_path = tmp_path / "file.txt"
    file_path.write_text("data")
    with patch("pathlib.Path.read_bytes", side_effect=PermissionError):
        struct_dict = {"path": str(file_path)}
        with pytest.raises(PermissionError):
            converter.hash_struct_dict(struct_dict)


def test_hash_struct_dict_is_directory(tmp_path):
    converter = PathStructConverter()
    struct_dict = {"path": str(tmp_path)}
    with pytest.raises(ValueError):
        converter.hash_struct_dict(struct_dict)


def test_hash_struct_dict_content_based(tmp_path):
    converter = PathStructConverter()
    file1 = tmp_path / "file1.txt"
    file2 = tmp_path / "file2.txt"
    content = "identical content"
    file1.write_text(content)
    file2.write_text(content)
    struct_dict1 = {"path": str(file1)}
    struct_dict2 = {"path": str(file2)}
    hash1 = converter.hash_struct_dict(struct_dict1)
    hash2 = converter.hash_struct_dict(struct_dict2)
    assert hash1 == hash2


def test_hash_path_objects_content_based(tmp_path):
    converter = PathStructConverter()
    file1 = tmp_path / "fileA.txt"
    file2 = tmp_path / "fileB.txt"
    content = "same file content"
    file1.write_text(content)
    file2.write_text(content)
    path_obj1 = Path(file1)
    path_obj2 = Path(file2)
    struct_dict1 = converter.python_to_struct_dict(path_obj1)
    struct_dict2 = converter.python_to_struct_dict(path_obj2)
    hash1 = converter.hash_struct_dict(struct_dict1)
    hash2 = converter.hash_struct_dict(struct_dict2)
    assert hash1 == hash2
