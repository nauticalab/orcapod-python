"""Tests for orcapod.extension_types.directory_type.Directory and LogicalDirectory."""

from __future__ import annotations

import json
import os
import pathlib

import pytest
import pyarrow as pa
from upath import UPath

from orcapod.extension_types.directory_type import Directory, LogicalDirectory, _try_import_callable


class TestDirectoryConstructor:
    def test_rejects_nonexistent_path(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            Directory(tmp_path / "does_not_exist")

    def test_rejects_non_directory_path(self, tmp_path):
        p = tmp_path / "file.txt"
        p.write_text("content")
        with pytest.raises(NotADirectoryError):
            Directory(p)

    def test_accepts_empty_directory(self, tmp_path):
        d = tmp_path / "empty"
        d.mkdir()
        obj = Directory(d)
        assert str(obj) == str(d)

    def test_accepts_non_empty_directory(self, tmp_path):
        d = tmp_path / "nonempty"
        d.mkdir()
        (d / "file.txt").write_text("hello")
        obj = Directory(d)
        assert str(obj) == str(d)

    def test_str_returns_path_string(self, tmp_path):
        d = tmp_path / "mydir"
        d.mkdir()
        obj = Directory(d)
        assert str(obj) == str(d)

    def test_ignore_stored_on_instance(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        patterns = ["*.pyc", ".git"]
        obj = Directory(d, ignore=patterns)
        assert obj._ignore == patterns

    def test_ignore_none_by_default(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        obj = Directory(d)
        assert obj._ignore is None

    @pytest.mark.skipif(
        not hasattr(os, "getuid") or os.getuid() == 0,
        reason="root bypasses permission checks",
    )
    def test_permission_error_on_untraversable_directory(self, tmp_path):
        d = tmp_path / "locked"
        d.mkdir()
        d.chmod(0o000)
        try:
            with pytest.raises(PermissionError):
                Directory(d)
        finally:
            d.chmod(0o755)

    def test_from_upath_creates_directory_via_parent(self, tmp_path):
        d = tmp_path / "child"
        d.mkdir()
        obj = Directory(d)
        parent = obj.parent  # ProxyUPath invokes _from_upath for derived paths
        assert isinstance(parent, Directory)
        assert parent._ignore is None


class TestLogicalDirectory:
    def test_logical_type_name(self):
        lt = LogicalDirectory()
        assert lt.logical_type_name == "orcapod.directory"

    def test_python_type(self):
        lt = LogicalDirectory()
        assert lt.python_type is Directory

    def test_arrow_ext_name(self):
        lt = LogicalDirectory()
        assert lt.get_arrow_extension_type().extension_name == "orcapod.directory"

    def test_arrow_ext_storage_type(self):
        lt = LogicalDirectory()
        assert lt.get_arrow_extension_type().storage_type == pa.large_string()

    def test_arrow_extension_type_is_cached(self):
        lt = LogicalDirectory()
        assert lt.get_arrow_extension_type() is lt.get_arrow_extension_type()

    def test_polars_extension_type_is_cached(self):
        lt = LogicalDirectory()
        assert lt.get_polars_extension_type() is lt.get_polars_extension_type()

    def test_python_to_storage_no_ignore(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        obj = Directory(d)
        lt = LogicalDirectory()
        storage = lt.python_to_storage(obj)
        data = json.loads(storage)
        assert data == {"path": str(d)}

    def test_python_to_storage_with_glob_ignore(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        obj = Directory(d, ignore=["*.pyc", ".git"])
        lt = LogicalDirectory()
        storage = lt.python_to_storage(obj)
        data = json.loads(storage)
        # patterns stored sorted
        assert data == {"path": str(d), "ignore": [".git", "*.pyc"]}

    def test_python_to_storage_with_lambda_warns_and_drops_ignore(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        fn = lambda p: p.name.endswith(".pyc")  # noqa: E731
        obj = Directory(d, ignore=fn)
        lt = LogicalDirectory()
        with pytest.warns(UserWarning, match="lambda"):
            storage = lt.python_to_storage(obj)
        data = json.loads(storage)
        assert data == {"path": str(d)}

    def test_python_to_storage_with_named_callable_stores_qualname(self, tmp_path):
        import json as _json_mod
        d = tmp_path / "d"
        d.mkdir()
        # json.dumps: __module__="json", __qualname__="dumps" — stable and importable
        obj = Directory(d, ignore=_json_mod.dumps)
        lt = LogicalDirectory()
        storage = lt.python_to_storage(obj)
        data = json.loads(storage)
        assert data.get("ignore_callable") == "json:dumps"

    def test_storage_to_python_no_ignore(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        lt = LogicalDirectory()
        result = lt.storage_to_python(json.dumps({"path": str(d)}))
        assert isinstance(result, Directory)
        assert str(result) == str(d)
        assert result._ignore is None

    def test_storage_to_python_with_glob_ignore(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        lt = LogicalDirectory()
        result = lt.storage_to_python(json.dumps({"path": str(d), "ignore": ["*.pyc"]}))
        assert isinstance(result, Directory)
        assert result._ignore == ["*.pyc"]

    def test_round_trip_no_ignore(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        obj = Directory(d)
        lt = LogicalDirectory()
        recovered = lt.storage_to_python(lt.python_to_storage(obj))
        assert str(recovered) == str(obj)
        assert recovered._ignore is None

    def test_round_trip_glob_ignore(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        obj = Directory(d, ignore=["*.pyc", ".git"])
        lt = LogicalDirectory()
        recovered = lt.storage_to_python(lt.python_to_storage(obj))
        assert str(recovered) == str(obj)
        assert recovered._ignore == [".git", "*.pyc"]  # sorted on storage

    def test_round_trip_named_callable_recovered(self, tmp_path):
        import json as _json_mod
        d = tmp_path / "d"
        d.mkdir()
        obj = Directory(d, ignore=_json_mod.dumps)
        lt = LogicalDirectory()
        storage = lt.python_to_storage(obj)
        recovered = lt.storage_to_python(storage)
        assert recovered._ignore is _json_mod.dumps

    def test_storage_to_python_raises_if_directory_missing(self, tmp_path):
        lt = LogicalDirectory()
        with pytest.raises(FileNotFoundError):
            lt.storage_to_python(json.dumps({"path": str(tmp_path / "gone")}))

    def test_storage_to_python_raises_value_error_on_bad_json(self):
        lt = LogicalDirectory()
        with pytest.raises(ValueError, match="LogicalDirectory"):
            lt.storage_to_python("not-json-at-all")

    def test_storage_to_python_raises_value_error_on_missing_path_key(self):
        lt = LogicalDirectory()
        with pytest.raises(ValueError, match="LogicalDirectory"):
            lt.storage_to_python(json.dumps({"wrong_key": "/some/dir"}))

    def test_python_to_storage_with_tuple_ignore(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        obj = Directory(d, ignore=("*.pyc", ".git"))
        lt = LogicalDirectory()
        storage = lt.python_to_storage(obj)
        data = json.loads(storage)
        assert data == {"path": str(d), "ignore": [".git", "*.pyc"]}

    def test_round_trip_tuple_ignore(self, tmp_path):
        d = tmp_path / "d"
        d.mkdir()
        obj = Directory(d, ignore=("*.pyc", ".git"))
        lt = LogicalDirectory()
        recovered = lt.storage_to_python(lt.python_to_storage(obj))
        assert str(recovered) == str(obj)
        assert recovered._ignore == [".git", "*.pyc"]  # sorted, stored as list


class TestTryImportCallable:
    def test_imports_known_function(self):
        import json as _json_mod
        result = _try_import_callable("json:dumps")
        assert result is _json_mod.dumps

    def test_returns_none_on_bad_module(self):
        with pytest.warns(UserWarning):
            result = _try_import_callable("nonexistent_module_xyz:some_fn")
        assert result is None

    def test_returns_none_on_bad_attribute(self):
        with pytest.warns(UserWarning):
            result = _try_import_callable("json:nonexistent_fn_xyz")
        assert result is None

    def test_returns_none_on_bad_format(self):
        with pytest.warns(UserWarning):
            result = _try_import_callable("no_colon_separator")
        assert result is None

    def test_returns_none_when_attribute_is_not_callable(self):
        # json.encoder.INFINITY is a float attribute — not callable
        with pytest.warns(UserWarning, match="not callable"):
            result = _try_import_callable("json.encoder:INFINITY")
        assert result is None


class TestDirectoryPathLike:
    def test_isinstance_pathlike(self, tmp_path):
        d = Directory(tmp_path)
        assert isinstance(d, os.PathLike)

    def test_fspath_returns_path_string(self, tmp_path):
        d = Directory(tmp_path)
        assert os.fspath(d) == str(tmp_path)

    def test_pathlib_path_accepts_directory(self, tmp_path):
        d = Directory(tmp_path)
        assert pathlib.Path(d) == tmp_path

    def test_remote_backed_fspath_raises(self):
        remote = Directory._from_upath(UPath("s3://bucket/prefix/"))
        with pytest.raises(TypeError):
            os.fspath(remote)
