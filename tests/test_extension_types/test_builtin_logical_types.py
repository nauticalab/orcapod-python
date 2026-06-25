"""Tests for built-in LogicalType implementations (LogicalPath, LogicalUPath, LogicalUUID)."""

from __future__ import annotations

import pathlib
import uuid as uuid_module

import polars as pl
import pyarrow as pa
from upath import UPath

import orcapod

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
    """DataContext's type_converter has a _logical_type_registry attribute."""
    from orcapod.contexts import get_default_context

    ctx = get_default_context()
    assert hasattr(ctx.type_converter, "_logical_type_registry")
    assert ctx.type_converter._logical_type_registry is not None


def test_default_context_registry_has_logical_path():
    """Default registry returns LogicalPath for 'pathlib.Path'."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    registry = get_default_context().type_converter._logical_type_registry
    lt = registry.get_by_logical_name("orcapod.path")
    assert isinstance(lt, LogicalPath)


def test_default_context_registry_lookup_by_python_type_path():
    """Default registry routes pathlib.Path to LogicalPath."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    registry = get_default_context().type_converter._logical_type_registry
    lt = registry.get_by_python_type(pathlib.Path)
    assert isinstance(lt, LogicalPath)


def test_default_context_registry_lookup_by_arrow_name_path():
    """Default registry routes 'pathlib.Path' arrow ext name to LogicalPath."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    registry = get_default_context().type_converter._logical_type_registry
    lt = registry.get_by_arrow_extension_name("orcapod.path")
    assert isinstance(lt, LogicalPath)


def test_default_context_registry_has_logical_upath():
    """Default registry returns LogicalUPath for 'upath.UPath'."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    registry = get_default_context().type_converter._logical_type_registry
    lt = registry.get_by_logical_name("orcapod.upath")
    assert isinstance(lt, LogicalUPath)


def test_default_context_registry_lookup_by_python_type_upath():
    """Default registry routes UPath to LogicalUPath."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    registry = get_default_context().type_converter._logical_type_registry
    lt = registry.get_by_python_type(UPath)
    assert isinstance(lt, LogicalUPath)


def test_default_context_registry_has_logical_uuid():
    """Default registry returns LogicalUUID for 'uuid.UUID'."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    registry = get_default_context().type_converter._logical_type_registry
    lt = registry.get_by_logical_name("orcapod.uuid")
    assert isinstance(lt, LogicalUUID)


def test_default_context_registry_lookup_by_arrow_name_uuid():
    """Default registry routes 'uuid.UUID' arrow ext name to LogicalUUID."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    registry = get_default_context().type_converter._logical_type_registry
    lt = registry.get_by_arrow_extension_name("orcapod.uuid")
    assert isinstance(lt, LogicalUUID)


def test_default_type_converter_logical_registry_is_not_none():
    """The default context's type_converter has a non-None _logical_type_registry."""
    from orcapod.contexts import get_default_context

    ctx = get_default_context()
    assert ctx.type_converter._logical_type_registry is not None


def test_default_context_idempotent_registry():
    """Calling get_default_context() twice returns the same LogicalTypeRegistry instance."""
    from orcapod.contexts import get_default_context

    r1 = get_default_context().type_converter._logical_type_registry
    r2 = get_default_context().type_converter._logical_type_registry
    assert r1 is r2


# ---------------------------------------------------------------------------
# Top-level orcapod namespace alias tests
# ---------------------------------------------------------------------------


def test_orcapod_path_alias_is_pathlib_path():
    """orcapod.Path is the same object as pathlib.Path."""
    import pathlib

    assert orcapod.Path is pathlib.Path


def test_orcapod_upath_alias_is_upath_upath():
    """orcapod.UPath is the same object as upath.UPath."""
    from upath import UPath

    assert orcapod.UPath is UPath


def test_orcapod_uuid_alias_is_uuid_uuid():
    """orcapod.UUID is the same object as uuid.UUID."""
    import uuid

    assert orcapod.UUID is uuid.UUID


def test_orcapod_path_alias_in_all():
    """orcapod.Path appears in orcapod.__all__."""
    assert "Path" in orcapod.__all__


def test_orcapod_upath_alias_in_all():
    """orcapod.UPath appears in orcapod.__all__."""
    assert "UPath" in orcapod.__all__


def test_orcapod_uuid_alias_in_all():
    """orcapod.UUID appears in orcapod.__all__."""
    assert "UUID" in orcapod.__all__


# ---------------------------------------------------------------------------
# Alias round-trip tests: using the stdlib types directly still works
# ---------------------------------------------------------------------------
# These tests verify that orcapod.Path / orcapod.UPath / orcapod.UUID are true
# aliases, not wrappers.  Because e.g. orcapod.UUID is uuid.UUID, using
# uuid.UUID directly produces the same orcapod.uuid Arrow extension type, and
# the value recovered from Arrow is a uuid.UUID (i.e. also an orcapod.UUID).
# Each test asserts the identity precondition first so the contract is clear.
# ---------------------------------------------------------------------------


def test_pathlib_path_works_via_orcapod_path_alias_arrow_round_trip():
    """pathlib.Path values round-trip through Arrow with the orcapod.path extension type.

    This test is only valid because orcapod.Path is pathlib.Path — they are the same
    object.  Using pathlib.Path directly (rather than orcapod.Path) produces the same
    Arrow extension type (``"orcapod.path"``), and the recovered value is a
    pathlib.Path (i.e. orcapod.Path).
    """
    from orcapod.extension_types.builtin_logical_types import LogicalPath

    # Precondition: test is only meaningful if orcapod.Path is pathlib.Path
    assert orcapod.Path is pathlib.Path

    lt = LogicalPath()
    registry = LogicalTypeRegistry()
    registry.register_logical_type(lt)

    # Create value using stdlib pathlib directly (not orcapod.Path)
    p = pathlib.Path("/tmp/alias_test/foo.txt")

    # Registry can find LogicalPath via pathlib.Path since orcapod.Path is pathlib.Path
    found = registry.get_by_python_type(pathlib.Path)
    assert found is lt

    # Saving to Arrow produces "orcapod.path" extension type
    storage_val = lt.python_to_storage(p)
    arrow_ext = lt.get_arrow_extension_type()
    assert arrow_ext.extension_name == "orcapod.path"
    ext_arr = pa.ExtensionArray.from_storage(
        arrow_ext, pa.array([storage_val], type=arrow_ext.storage_type)
    )

    # Recovered value is a pathlib.Path (which is orcapod.Path)
    recovered = lt.storage_to_python(ext_arr.storage[0].as_py())
    assert recovered == p
    assert isinstance(recovered, orcapod.Path)  # valid because orcapod.Path is pathlib.Path
    assert isinstance(recovered, pathlib.Path)


def test_upath_upath_works_via_orcapod_upath_alias_arrow_round_trip():
    """upath.UPath values round-trip through Arrow with the orcapod.upath extension type.

    This test is only valid because orcapod.UPath is upath.UPath — they are the same
    object.  Using upath.UPath directly (rather than orcapod.UPath) produces the same
    Arrow extension type (``"orcapod.upath"``), and the recovered value is a
    upath.UPath (i.e. orcapod.UPath).
    """
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    # Precondition: test is only meaningful if orcapod.UPath is upath.UPath
    assert orcapod.UPath is UPath

    lt = LogicalUPath()
    registry = LogicalTypeRegistry()
    registry.register_logical_type(lt)

    # Create value using upath directly (not orcapod.UPath)
    up = UPath("s3://bucket/alias_test/key.txt")

    # Registry can find LogicalUPath via UPath since orcapod.UPath is upath.UPath
    found = registry.get_by_python_type(UPath)
    assert found is lt

    # Saving to Arrow produces "orcapod.upath" extension type
    storage_val = lt.python_to_storage(up)
    arrow_ext = lt.get_arrow_extension_type()
    assert arrow_ext.extension_name == "orcapod.upath"
    ext_arr = pa.ExtensionArray.from_storage(
        arrow_ext, pa.array([storage_val], type=arrow_ext.storage_type)
    )

    # Recovered value is a upath.UPath (which is orcapod.UPath)
    recovered = lt.storage_to_python(ext_arr.storage[0].as_py())
    assert recovered == up
    assert isinstance(recovered, orcapod.UPath)  # valid because orcapod.UPath is upath.UPath
    assert isinstance(recovered, UPath)


def test_uuid_uuid_works_via_orcapod_uuid_alias_arrow_round_trip():
    """uuid.UUID values round-trip through Arrow with the orcapod.uuid extension type.

    This test is only valid because orcapod.UUID is uuid.UUID — they are the same
    object.  Using uuid.UUID directly (rather than orcapod.UUID) produces the same
    Arrow extension type (``"orcapod.uuid"``), and the recovered value is a
    uuid.UUID (i.e. orcapod.UUID).
    """
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    # Precondition: test is only meaningful if orcapod.UUID is uuid.UUID
    assert orcapod.UUID is uuid_module.UUID

    lt = LogicalUUID()
    registry = LogicalTypeRegistry()
    registry.register_logical_type(lt)

    # Create value using stdlib uuid directly (not orcapod.UUID)
    u = uuid_module.UUID("12345678-1234-5678-1234-567812345678")

    # Registry can find LogicalUUID via uuid.UUID since orcapod.UUID is uuid.UUID
    found = registry.get_by_python_type(uuid_module.UUID)
    assert found is lt

    # Saving to Arrow produces "orcapod.uuid" extension type
    storage_val = lt.python_to_storage(u)
    arrow_ext = lt.get_arrow_extension_type()
    assert arrow_ext.extension_name == "orcapod.uuid"
    ext_arr = pa.ExtensionArray.from_storage(
        arrow_ext, pa.array([storage_val], type=arrow_ext.storage_type)
    )

    # Recovered value is a uuid.UUID (which is orcapod.UUID)
    recovered = lt.storage_to_python(ext_arr.storage[0].as_py())
    assert recovered == u
    assert isinstance(recovered, orcapod.UUID)  # valid because orcapod.UUID is uuid.UUID
    assert isinstance(recovered, uuid_module.UUID)


# ---------------------------------------------------------------------------
# Converter param acceptance tests (Task 2 — PLT-1705)
# ---------------------------------------------------------------------------


def test_logical_path_python_to_storage_accepts_converter():
    """python_to_storage now accepts a converter param (ignored)."""
    from orcapod.extension_types.builtin_logical_types import LogicalPath
    lt = LogicalPath()
    result = lt.python_to_storage(pathlib.Path("/tmp/foo"), converter=None)
    assert result == "/tmp/foo"


def test_logical_path_storage_to_python_accepts_converter():
    """storage_to_python now accepts a converter param (ignored)."""
    from orcapod.extension_types.builtin_logical_types import LogicalPath
    lt = LogicalPath()
    result = lt.storage_to_python("/tmp/foo", converter=None)
    assert result == pathlib.Path("/tmp/foo")


def test_logical_uuid_python_to_storage_accepts_converter():
    from orcapod.extension_types.builtin_logical_types import LogicalUUID
    lt = LogicalUUID()
    u = uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    result = lt.python_to_storage(u, converter=None)
    assert result == u.bytes


def test_logical_uuid_storage_to_python_accepts_converter():
    from orcapod.extension_types.builtin_logical_types import LogicalUUID
    lt = LogicalUUID()
    u = uuid_module.UUID("12345678-1234-5678-1234-567812345678")
    result = lt.storage_to_python(u.bytes, converter=None)
    assert result == u


def test_logical_upath_python_to_storage_accepts_converter():
    from orcapod.extension_types.builtin_logical_types import LogicalUPath
    lt = LogicalUPath()
    result = lt.python_to_storage(UPath("s3://bucket/key"), converter=None)
    assert result == "s3://bucket/key"


def test_logical_upath_storage_to_python_accepts_converter():
    from orcapod.extension_types.builtin_logical_types import LogicalUPath
    lt = LogicalUPath()
    result = lt.storage_to_python("s3://bucket/key", converter=None)
    assert isinstance(result, UPath)
