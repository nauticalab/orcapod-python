"""Tests for built-in LogicalType implementations (LogicalPath, LogicalUPath, LogicalUUID)."""

from __future__ import annotations

import pathlib
import uuid as uuid_module
import warnings

import polars as pl
import pyarrow as pa
import pytest
from upath import UPath

from orcapod.extension_types.protocols import LogicalType
from orcapod.extension_types.registry import LogicalTypeRegistry


# ---------------------------------------------------------------------------
# LogicalPath tests
# ---------------------------------------------------------------------------


def test_logical_path_isinstance_logical_type():
    """LogicalPath() satisfies the LogicalType runtime-checkable protocol."""
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    assert isinstance(LogicalPath(), LogicalType)


def test_logical_path_logical_type_name():
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    assert LogicalPath().logical_type_name == "pathlib.Path"


def test_logical_path_python_type():
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    assert LogicalPath().python_type is pathlib.Path


def test_logical_path_arrow_ext_name():
    """get_arrow_extension_type().extension_name is 'pathlib.Path'."""
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    assert LogicalPath().get_arrow_extension_type().extension_name == "pathlib.Path"


def test_logical_path_arrow_ext_storage_type():
    """Arrow extension storage type is pa.large_string()."""
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    assert LogicalPath().get_arrow_extension_type().storage_type == pa.large_string()


def test_logical_path_get_arrow_extension_type_is_cached():
    """get_arrow_extension_type() returns the same object on repeated calls."""
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    lt = LogicalPath()
    assert lt.get_arrow_extension_type() is lt.get_arrow_extension_type()


def test_logical_path_get_polars_extension_type_is_cached():
    """get_polars_extension_type() returns the same object on repeated calls."""
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    lt = LogicalPath()
    assert lt.get_polars_extension_type() is lt.get_polars_extension_type()


def test_logical_path_round_trip():
    """Path -> python_to_storage -> storage_to_python -> Path is identity."""
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    lt = LogicalPath()
    p = pathlib.Path("/tmp/foo/bar.txt")
    assert lt.storage_to_python(lt.python_to_storage(p)) == p


def test_logical_path_python_to_storage_returns_string():
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    lt = LogicalPath()
    result = lt.python_to_storage(pathlib.Path("/tmp/test"))
    assert isinstance(result, str)
    assert result == "/tmp/test"


# ---------------------------------------------------------------------------
# LogicalUPath tests
# ---------------------------------------------------------------------------


def test_logical_upath_isinstance_logical_type():
    """LogicalUPath() satisfies the LogicalType runtime-checkable protocol."""
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    assert isinstance(LogicalUPath(), LogicalType)


def test_logical_upath_logical_type_name():
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    assert LogicalUPath().logical_type_name == "upath.UPath"


def test_logical_upath_python_type():
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    assert LogicalUPath().python_type is UPath


def test_logical_upath_arrow_ext_name():
    """get_arrow_extension_type().extension_name is 'upath.UPath'."""
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    assert LogicalUPath().get_arrow_extension_type().extension_name == "upath.UPath"


def test_logical_upath_arrow_ext_storage_type():
    """Arrow extension storage type is pa.large_string()."""
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    assert LogicalUPath().get_arrow_extension_type().storage_type == pa.large_string()


def test_logical_upath_get_arrow_extension_type_is_cached():
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    lt = LogicalUPath()
    assert lt.get_arrow_extension_type() is lt.get_arrow_extension_type()


def test_logical_upath_get_polars_extension_type_is_cached():
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    lt = LogicalUPath()
    assert lt.get_polars_extension_type() is lt.get_polars_extension_type()


def test_logical_upath_round_trip():
    """UPath -> python_to_storage -> storage_to_python -> UPath is identity."""
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    lt = LogicalUPath()
    up = UPath("s3://bucket/key/file.txt")
    assert lt.storage_to_python(lt.python_to_storage(up)) == up


def test_logical_upath_python_to_storage_returns_string():
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    lt = LogicalUPath()
    result = lt.python_to_storage(UPath("s3://bucket/key"))
    assert isinstance(result, str)
    assert result == "s3://bucket/key"
