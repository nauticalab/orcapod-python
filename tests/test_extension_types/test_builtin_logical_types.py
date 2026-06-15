"""Tests for built-in LogicalType implementations (LogicalPath, LogicalUPath, LogicalUUID)."""

from __future__ import annotations

import pathlib
import uuid as uuid_module

import polars as pl
import pyarrow as pa
from upath import UPath

from orcapod.extension_types.protocols import LogicalTypeProtocol
from orcapod.extension_types.registry import LogicalTypeRegistry


# ---------------------------------------------------------------------------
# LogicalPath tests
# ---------------------------------------------------------------------------


def test_logical_path_isinstance_logical_type():
    """LogicalPath() satisfies the LogicalType runtime-checkable protocol."""
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    assert isinstance(LogicalPath(), LogicalTypeProtocol)


def test_logical_path_logical_type_name():
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    assert LogicalPath().logical_type_name == "orcapod.path"


def test_logical_path_python_type():
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    assert LogicalPath().python_type is pathlib.Path


def test_logical_path_arrow_ext_name():
    """get_arrow_extension_type().extension_name is 'orcapod.path'."""
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    assert LogicalPath().get_arrow_extension_type().extension_name == "orcapod.path"


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

    assert isinstance(LogicalUPath(), LogicalTypeProtocol)


def test_logical_upath_logical_type_name():
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    assert LogicalUPath().logical_type_name == "orcapod.upath"


def test_logical_upath_python_type():
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    assert LogicalUPath().python_type is UPath


def test_logical_upath_arrow_ext_name():
    """get_arrow_extension_type().extension_name is 'orcapod.upath'."""
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    assert LogicalUPath().get_arrow_extension_type().extension_name == "orcapod.upath"


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


# ---------------------------------------------------------------------------
# LogicalUUID tests
# ---------------------------------------------------------------------------


def test_logical_uuid_isinstance_logical_type():
    """LogicalUUID() satisfies the LogicalType runtime-checkable protocol."""
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    assert isinstance(LogicalUUID(), LogicalTypeProtocol)


def test_logical_uuid_logical_type_name():
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    assert LogicalUUID().logical_type_name == "orcapod.uuid"


def test_logical_uuid_python_type():
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    assert LogicalUUID().python_type is uuid_module.UUID


def test_logical_uuid_arrow_ext_name():
    """Arrow extension name is 'orcapod.uuid', matching logical_type_name."""
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    lt = LogicalUUID()
    assert lt.get_arrow_extension_type().extension_name == "orcapod.uuid"
    assert lt.get_arrow_extension_type().extension_name == lt.logical_type_name


def test_logical_uuid_arrow_ext_storage_type():
    """Arrow extension storage type is pa.large_binary()."""
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    assert LogicalUUID().get_arrow_extension_type().storage_type == pa.large_binary()


def test_logical_uuid_get_arrow_extension_type_is_cached():
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    lt = LogicalUUID()
    assert lt.get_arrow_extension_type() is lt.get_arrow_extension_type()


def test_logical_uuid_get_polars_extension_type_is_cached():
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    lt = LogicalUUID()
    assert lt.get_polars_extension_type() is lt.get_polars_extension_type()


def test_logical_uuid_round_trip():
    """UUID -> python_to_storage -> storage_to_python -> UUID is identity."""
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    lt = LogicalUUID()
    u = uuid_module.uuid4()
    assert lt.storage_to_python(lt.python_to_storage(u)) == u


def test_logical_uuid_python_to_storage_returns_bytes():
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    lt = LogicalUUID()
    u = uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    result = lt.python_to_storage(u)
    assert isinstance(result, bytes)
    assert len(result) == 16


def test_logical_uuid_storage_to_python_accepts_bytes():
    """storage_to_python works when storage_value is plain bytes."""
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    lt = LogicalUUID()
    u = uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    recovered = lt.storage_to_python(u.bytes)
    assert recovered == u


def test_logical_uuid_registration_does_not_raise():
    """Registering LogicalUUID succeeds and is reachable by both logical and arrow names."""
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    registry = LogicalTypeRegistry()
    lt = LogicalUUID()
    registry.register_logical_type(lt)  # should NOT raise
    assert registry.get_by_logical_name("orcapod.uuid") is lt
    assert registry.get_by_arrow_extension_name("orcapod.uuid") is lt


# ---------------------------------------------------------------------------
# Arrow and Polars end-to-end round-trip tests
# ---------------------------------------------------------------------------


def test_logical_path_arrow_round_trip():
    """Python -> Arrow extension array -> Python via LogicalPath."""
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    lt = LogicalPath()
    registry = LogicalTypeRegistry()
    registry.register_logical_type(lt)

    originals = [pathlib.Path("/tmp/foo"), pathlib.Path("/home/user/bar.txt")]
    storage_vals = [lt.python_to_storage(p) for p in originals]
    arrow_ext = lt.get_arrow_extension_type()
    ext_arr = pa.ExtensionArray.from_storage(arrow_ext, pa.array(storage_vals, type=arrow_ext.storage_type))

    recovered = [lt.storage_to_python(v.as_py()) for v in ext_arr.storage]
    assert recovered == originals


def test_logical_path_polars_round_trip():
    """Python -> Arrow extension array -> Polars series -> Arrow -> Python via LogicalPath."""
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    lt = LogicalPath()
    registry = LogicalTypeRegistry()
    registry.register_logical_type(lt)

    originals = [pathlib.Path("/tmp/foo"), pathlib.Path("/home/user/bar.txt")]
    storage_vals = [lt.python_to_storage(p) for p in originals]
    arrow_ext = lt.get_arrow_extension_type()
    ext_arr = pa.ExtensionArray.from_storage(arrow_ext, pa.array(storage_vals, type=arrow_ext.storage_type))

    pl_series = pl.from_arrow(ext_arr)
    arr_back = pl_series.to_arrow()
    recovered = [lt.storage_to_python(v.as_py()) for v in arr_back.storage]
    assert recovered == originals


def test_logical_upath_arrow_round_trip():
    """Python -> Arrow extension array -> Python via LogicalUPath."""
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    lt = LogicalUPath()
    registry = LogicalTypeRegistry()
    registry.register_logical_type(lt)

    originals = [UPath("s3://bucket/key"), UPath("gs://other/path/file.txt")]
    storage_vals = [lt.python_to_storage(p) for p in originals]
    arrow_ext = lt.get_arrow_extension_type()
    ext_arr = pa.ExtensionArray.from_storage(arrow_ext, pa.array(storage_vals, type=arrow_ext.storage_type))

    recovered = [lt.storage_to_python(v.as_py()) for v in ext_arr.storage]
    assert recovered == originals


def test_logical_upath_polars_round_trip():
    """Python -> Arrow extension array -> Polars series -> Arrow -> Python via LogicalUPath."""
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    lt = LogicalUPath()
    registry = LogicalTypeRegistry()
    registry.register_logical_type(lt)

    originals = [UPath("s3://bucket/key"), UPath("gs://other/path/file.txt")]
    storage_vals = [lt.python_to_storage(p) for p in originals]
    arrow_ext = lt.get_arrow_extension_type()
    ext_arr = pa.ExtensionArray.from_storage(arrow_ext, pa.array(storage_vals, type=arrow_ext.storage_type))

    pl_series = pl.from_arrow(ext_arr)
    arr_back = pl_series.to_arrow()
    recovered = [lt.storage_to_python(v.as_py()) for v in arr_back.storage]
    assert recovered == originals


def test_logical_uuid_arrow_round_trip():
    """Python -> Arrow extension array -> Python via LogicalUUID."""
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    lt = LogicalUUID()
    registry = LogicalTypeRegistry()
    registry.register_logical_type(lt)

    originals = [uuid_module.UUID("12345678-1234-5678-1234-567812345678"), uuid_module.uuid4()]
    storage_vals = [lt.python_to_storage(u) for u in originals]
    arrow_ext = lt.get_arrow_extension_type()
    ext_arr = pa.ExtensionArray.from_storage(arrow_ext, pa.array(storage_vals, type=arrow_ext.storage_type))

    recovered = [lt.storage_to_python(v.as_py()) for v in ext_arr.storage]
    assert recovered == originals


def test_logical_uuid_polars_round_trip():
    """Python -> Arrow extension array -> Polars series -> Arrow -> Python via LogicalUUID."""
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    lt = LogicalUUID()
    registry = LogicalTypeRegistry()
    registry.register_logical_type(lt)

    originals = [uuid_module.UUID("12345678-1234-5678-1234-567812345678"), uuid_module.uuid4()]
    storage_vals = [lt.python_to_storage(u) for u in originals]
    arrow_ext = lt.get_arrow_extension_type()
    ext_arr = pa.ExtensionArray.from_storage(arrow_ext, pa.array(storage_vals, type=arrow_ext.storage_type))

    pl_series = pl.from_arrow(ext_arr)
    arr_back = pl_series.to_arrow()
    recovered = [lt.storage_to_python(v.as_py()) for v in arr_back.storage]
    assert recovered == originals


# ---------------------------------------------------------------------------
# Default context integration tests
# ---------------------------------------------------------------------------


def test_default_context_has_logical_type_registry():
    """DataContext has a logical_type_registry attribute."""
    from orcapod.contexts import get_default_context

    ctx = get_default_context()
    assert hasattr(ctx, "logical_type_registry")


def test_default_context_registry_has_logical_path():
    """Default registry returns LogicalPath for 'pathlib.Path'."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    registry = get_default_context().logical_type_registry
    lt = registry.get_by_logical_name("orcapod.path")
    assert isinstance(lt, LogicalPath)


def test_default_context_registry_lookup_by_python_type_path():
    """Default registry routes pathlib.Path to LogicalPath."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    registry = get_default_context().logical_type_registry
    lt = registry.get_by_python_type(pathlib.Path)
    assert isinstance(lt, LogicalPath)


def test_default_context_registry_lookup_by_arrow_name_path():
    """Default registry routes 'pathlib.Path' arrow ext name to LogicalPath."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    registry = get_default_context().logical_type_registry
    lt = registry.get_by_arrow_extension_name("orcapod.path")
    assert isinstance(lt, LogicalPath)


def test_default_context_registry_has_logical_upath():
    """Default registry returns LogicalUPath for 'upath.UPath'."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    registry = get_default_context().logical_type_registry
    lt = registry.get_by_logical_name("orcapod.upath")
    assert isinstance(lt, LogicalUPath)


def test_default_context_registry_lookup_by_python_type_upath():
    """Default registry routes UPath to LogicalUPath."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    registry = get_default_context().logical_type_registry
    lt = registry.get_by_python_type(UPath)
    assert isinstance(lt, LogicalUPath)


def test_default_context_registry_has_logical_uuid():
    """Default registry returns LogicalUUID for 'uuid.UUID'."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    registry = get_default_context().logical_type_registry
    lt = registry.get_by_logical_name("orcapod.uuid")
    assert isinstance(lt, LogicalUUID)


def test_default_context_registry_lookup_by_arrow_name_uuid():
    """Default registry routes 'uuid.UUID' arrow ext name to LogicalUUID."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    registry = get_default_context().logical_type_registry
    lt = registry.get_by_arrow_extension_name("orcapod.uuid")
    assert isinstance(lt, LogicalUUID)


def test_get_default_logical_type_registry_returns_same_as_context():
    """get_default_logical_type_registry() is the same object as get_default_context().logical_type_registry."""
    from orcapod.contexts import get_default_context, get_default_logical_type_registry

    assert get_default_logical_type_registry() is get_default_context().logical_type_registry


def test_default_context_idempotent_registry():
    """Calling get_default_context() twice returns the same LogicalTypeRegistry instance."""
    from orcapod.contexts import get_default_context

    r1 = get_default_context().logical_type_registry
    r2 = get_default_context().logical_type_registry
    assert r1 is r2
