"""Tests for built-in LogicalType implementations (LogicalPath, LogicalUPath, LogicalUUID)."""

from __future__ import annotations

import pathlib
import uuid as uuid_module

import pyarrow as pa
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


# ---------------------------------------------------------------------------
# LogicalUUID tests
# ---------------------------------------------------------------------------


def test_logical_uuid_isinstance_logical_type():
    """LogicalUUID() satisfies the LogicalType runtime-checkable protocol."""
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    assert isinstance(LogicalUUID(), LogicalType)


def test_logical_uuid_logical_type_name():
    """logical_type_name is 'uuid.UUID', not the Arrow extension name."""
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    assert LogicalUUID().logical_type_name == "uuid.UUID"


def test_logical_uuid_python_type():
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    assert LogicalUUID().python_type is uuid_module.UUID


def test_logical_uuid_arrow_ext_name_is_arrow_uuid():
    """Arrow extension name is 'arrow.uuid', intentionally different from logical_type_name."""
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    lt = LogicalUUID()
    assert lt.get_arrow_extension_type().extension_name == "arrow.uuid"
    assert lt.logical_type_name != lt.get_arrow_extension_type().extension_name


def test_logical_uuid_get_arrow_extension_type_returns_pa_uuid():
    """get_arrow_extension_type() returns PyArrow's built-in pa.uuid() type."""
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    lt = LogicalUUID()
    assert lt.get_arrow_extension_type() == pa.uuid()


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
    """Registering LogicalUUID succeeds even though pa.uuid() is already in PyArrow's registry."""
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    registry = LogicalTypeRegistry()
    lt = LogicalUUID()
    registry.register(lt)  # should NOT raise
    assert registry.get_by_logical_name("uuid.UUID") is lt
    assert registry.get_by_arrow_extension_name("arrow.uuid") is lt


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
    lt = registry.get_by_logical_name("pathlib.Path")
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
    lt = registry.get_by_arrow_extension_name("pathlib.Path")
    assert isinstance(lt, LogicalPath)


def test_default_context_registry_has_logical_upath():
    """Default registry returns LogicalUPath for 'upath.UPath'."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.builtin_logical_types import LogicalUPath

    registry = get_default_context().logical_type_registry
    lt = registry.get_by_logical_name("upath.UPath")
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
    lt = registry.get_by_logical_name("uuid.UUID")
    assert isinstance(lt, LogicalUUID)


def test_default_context_registry_lookup_by_arrow_name_uuid():
    """Default registry routes 'arrow.uuid' arrow ext name to LogicalUUID."""
    from orcapod.contexts import get_default_context
    from orcapod.extension_types.builtin_logical_types import LogicalUUID

    registry = get_default_context().logical_type_registry
    lt = registry.get_by_arrow_extension_name("arrow.uuid")
    assert isinstance(lt, LogicalUUID)


def test_default_context_registry_uuid_logical_name_differs_from_arrow_name():
    """The same LogicalUUID instance is found by both 'uuid.UUID' and 'arrow.uuid'."""
    from orcapod.contexts import get_default_context

    registry = get_default_context().logical_type_registry
    by_logical = registry.get_by_logical_name("uuid.UUID")
    by_arrow = registry.get_by_arrow_extension_name("arrow.uuid")
    assert by_logical is by_arrow


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
