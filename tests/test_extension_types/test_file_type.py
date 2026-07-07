"""Tests for orcapod.extension_types.file_type.File and LogicalFile."""

from __future__ import annotations

import json
import os
import pathlib

import fsspec
import pytest
import pyarrow as pa
from upath import UPath

from orcapod.extension_types.file_type import File, LogicalFile


class TestFileConstructor:
    def test_rejects_nonexistent_path(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            File(tmp_path / "does_not_exist.txt")

    def test_rejects_directory(self, tmp_path):
        with pytest.raises(IsADirectoryError):
            File(tmp_path)

    def test_rejects_symlink_to_directory_when_following(self, tmp_path):
        target_dir = tmp_path / "real_dir"
        target_dir.mkdir()
        link = tmp_path / "link_to_dir"
        link.symlink_to(target_dir)
        with pytest.raises(IsADirectoryError):
            File(link)  # follow_symlinks=True default, target is a dir

    def test_rejects_symlink_when_follow_symlinks_false(self, tmp_path):
        real_file = tmp_path / "real.txt"
        real_file.write_text("content")
        link = tmp_path / "link_to_file"
        link.symlink_to(real_file)
        with pytest.raises(ValueError, match="symlink"):
            File(link, follow_symlinks=False)

    def test_accepts_symlink_to_file_when_following(self, tmp_path):
        real_file = tmp_path / "real.txt"
        real_file.write_text("content")
        link = tmp_path / "link_to_file"
        link.symlink_to(real_file)
        f = File(link)  # follow_symlinks=True default
        assert str(f) == str(link)

    def test_accepts_zero_byte_file(self, tmp_path):
        empty = tmp_path / "empty.txt"
        empty.write_bytes(b"")
        f = File(empty)
        assert str(f) == str(empty)

    def test_accepts_regular_file(self, tmp_path):
        regular = tmp_path / "regular.txt"
        regular.write_text("hello")
        f = File(regular)
        assert str(f) == str(regular)

    def test_str_returns_path_string(self, tmp_path):
        p = tmp_path / "file.txt"
        p.write_text("data")
        f = File(p)
        assert str(f) == str(p)


class TestLogicalFile:
    def test_logical_type_name(self):
        lt = LogicalFile()
        assert lt.logical_type_name == "orcapod.file"

    def test_python_type(self):
        lt = LogicalFile()
        assert lt.python_type is File

    def test_arrow_ext_name(self):
        lt = LogicalFile()
        assert lt.get_arrow_extension_type().extension_name == "orcapod.file"

    def test_arrow_ext_storage_type(self):
        lt = LogicalFile()
        assert lt.get_arrow_extension_type().storage_type == pa.large_string()

    def test_python_to_storage_returns_json_string(self, tmp_path):
        p = tmp_path / "f.txt"
        p.write_text("x")
        f = File(p)
        lt = LogicalFile()
        result = lt.python_to_storage(f)
        data = json.loads(result)
        assert data == {"path": str(p)}

    def test_storage_to_python_accepts_json_string(self, tmp_path):
        p = tmp_path / "f.txt"
        p.write_text("x")
        lt = LogicalFile()
        result = lt.storage_to_python(json.dumps({"path": str(p)}))
        assert isinstance(result, File)
        assert str(result) == str(p)

    def test_round_trip_preserves_path(self, tmp_path):
        p = tmp_path / "f.txt"
        p.write_text("round trip")
        f = File(p)
        lt = LogicalFile()
        storage = lt.python_to_storage(f)
        recovered = lt.storage_to_python(storage)
        assert str(recovered) == str(f)

    def test_storage_to_python_raises_if_file_missing(self, tmp_path):
        lt = LogicalFile()
        with pytest.raises(FileNotFoundError):
            lt.storage_to_python(json.dumps({"path": str(tmp_path / "gone.txt")}))

    def test_arrow_extension_type_is_cached(self):
        lt = LogicalFile()
        assert lt.get_arrow_extension_type() is lt.get_arrow_extension_type()

    def test_storage_to_python_raises_value_error_on_bad_json(self):
        lt = LogicalFile()
        with pytest.raises(ValueError, match="LogicalFile"):
            lt.storage_to_python("not-json-at-all")

    def test_storage_to_python_raises_value_error_on_missing_path_key(self):
        lt = LogicalFile()
        with pytest.raises(ValueError, match="LogicalFile"):
            lt.storage_to_python(json.dumps({"wrong_key": "/some/file.txt"}))


class TestFilePathLike:
    def test_isinstance_pathlike(self, tmp_path):
        p = tmp_path / "file.txt"
        p.write_text("data")
        f = File(p)
        assert isinstance(f, os.PathLike)

    def test_fspath_returns_path_string(self, tmp_path):
        p = tmp_path / "file.txt"
        p.write_text("data")
        f = File(p)
        assert os.fspath(f) == str(p)

    def test_open_accepts_file(self, tmp_path):
        p = tmp_path / "file.txt"
        p.write_text("hello")
        f = File(p)
        with open(f) as fh:
            assert fh.read() == "hello"

    def test_pathlib_path_accepts_file(self, tmp_path):
        p = tmp_path / "file.txt"
        p.write_text("data")
        f = File(p)
        assert pathlib.Path(f) == p

    def test_remote_backed_fspath_raises(self):
        remote = File._from_upath(UPath("s3://bucket/key.csv"))
        with pytest.raises(TypeError):
            os.fspath(remote)

    def test_plain_proxy_upath_subclass_not_pathlike(self):
        from upath.extensions import ProxyUPath

        class _Stub(ProxyUPath):
            pass

        assert not issubclass(_Stub, os.PathLike)


class TestURLFormIdentity:
    """Regression tests: op.File preserves URL-form identity for non-local protocols.

    Uses ``memory://`` as a stable, always-available stand-in for ``engm://``.
    The same invariants hold for any non-local fsspec protocol.
    """

    @pytest.fixture(autouse=True)
    def memory_file(self):
        """Create ``memory://ns/x.bin`` with known content; clean up after."""
        fs = fsspec.filesystem("memory")
        # Clean up any pre-existing state
        if fs.exists("/ns/x.bin"):
            fs.rm("/ns/x.bin")
        try:
            fs.rmdir("/ns")
        except (FileNotFoundError, OSError):
            pass
        # Create directory and file
        fs.mkdir("/ns", create_parents=True)
        with fs.open("/ns/x.bin", "wb") as fh:
            fh.write(b"url-identity-test-content")
        yield
        # Clean up after test
        if fs.exists("/ns/x.bin"):
            fs.rm("/ns/x.bin")
        try:
            fs.rmdir("/ns")
        except (FileNotFoundError, OSError):
            pass

    def test_str_preserves_url_form(self):
        f = File("memory://ns/x.bin")
        assert str(f) == "memory://ns/x.bin", (
            f"Expected URL form 'memory://ns/x.bin', got {str(f)!r}"
        )

    def test_hash_is_stable(self):
        h1 = hash(File("memory://ns/x.bin"))
        h2 = hash(File("memory://ns/x.bin"))
        assert h1 == h2, "hash() must be identical across two constructions of the same URL"

    def test_hash_equals_upath_protocol_tuple(self):
        # File.__hash__ delegates to ProxyUPath.__hash__ → UPath.__hash__.
        # Derive the expected hash from UPath directly so the test stays focused
        # on verifying that File follows the same hash contract as its wrapped UPath,
        # rather than hard-coding the internal (protocol, vfspath) representation.
        ref = UPath("memory://ns/x.bin")
        expected = hash(ref)
        actual = hash(File("memory://ns/x.bin"))
        assert actual == expected, (
            f"hash(File('memory://ns/x.bin')) should equal hash(UPath('memory://ns/x.bin')), "
            f"got {actual} vs {expected}"
        )

    def test_logical_file_storage_encodes_url(self):
        f = File("memory://ns/x.bin")
        lt = LogicalFile()
        storage = lt.python_to_storage(f)
        data = json.loads(storage)
        assert data["path"] == "memory://ns/x.bin", (
            f"python_to_storage must encode URL form; got path={data['path']!r}"
        )

    def test_logical_file_round_trip_preserves_url(self):
        f = File("memory://ns/x.bin")
        lt = LogicalFile()
        recovered = lt.storage_to_python(lt.python_to_storage(f))
        assert str(recovered) == "memory://ns/x.bin", (
            f"storage_to_python(python_to_storage(f)) must preserve URL form; "
            f"got {str(recovered)!r}"
        )
