"""Tests for LogicalTypeRegistry and make_arrow_extension_type."""

from __future__ import annotations

import pathlib
import tempfile
import uuid
import warnings

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from orcapod.extension_types.protocols import LogicalType
from orcapod.extension_types.registry import LogicalTypeRegistry, make_arrow_extension_type


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _unique_name() -> str:
    """Unique extension name to avoid cross-test global-registry collisions."""
    return f"test.registry.{uuid.uuid4().hex[:8]}"


def _make_stub(
    arrow_name: str | None = None,
    logical_name: str | None = None,
    storage: pa.DataType | None = None,
    py_type: type = str,
) -> LogicalType:
    """Factory for minimal LogicalType conforming stubs.

    ``arrow_name`` defaults to ``logical_name`` (or a unique name if both are
    omitted) so that callers can pass a single name and get consistent arrow
    and logical names.
    """
    _arrow_name = arrow_name or logical_name or _unique_name()
    _logical_name = logical_name or _arrow_name
    _storage = storage if storage is not None else pa.large_utf8()

    ArrowExtClass = make_arrow_extension_type(_arrow_name, _storage)

    class _PolarsExt(pl.BaseExtension):
        def __init__(self):
            super().__init__(_arrow_name, pl.String, None)
        @classmethod
        def ext_from_params(cls, ext_name, storage_dtype, metadata_str):
            return cls()

    class _Stub:
        @property
        def logical_type_name(self) -> str:
            return _logical_name

        @property
        def python_type(self) -> type:
            return py_type

        def get_arrow_extension_type(self) -> pa.ExtensionType:
            return ArrowExtClass()

        def get_polars_extension_type(self) -> pl.BaseExtension:
            return _PolarsExt()

        def python_to_storage(self, value):
            return str(value)

        def storage_to_python(self, storage_value):
            return storage_value

    return _Stub()


# ---------------------------------------------------------------------------
# make_arrow_extension_type tests
# ---------------------------------------------------------------------------

def test_make_arrow_extension_type_returns_class():
    """make_arrow_extension_type returns a pa.ExtensionType subclass."""
    cls = make_arrow_extension_type("test.MakeExt", pa.large_utf8())
    assert issubclass(cls, pa.ExtensionType)


def test_make_arrow_extension_type_instance_has_correct_name():
    """Instantiating the returned class yields the correct extension_name."""
    name = _unique_name()
    cls = make_arrow_extension_type(name, pa.large_utf8())
    inst = cls()
    assert inst.extension_name == name


def test_make_arrow_extension_type_instance_has_correct_storage():
    """Instantiating the returned class yields the correct storage_type."""
    cls = make_arrow_extension_type(_unique_name(), pa.large_binary())
    inst = cls()
    assert inst.storage_type == pa.large_binary()


def test_make_arrow_extension_type_metadata_defaults_to_empty():
    """Without metadata, __arrow_ext_serialize__ returns empty bytes."""
    cls = make_arrow_extension_type(_unique_name(), pa.large_utf8())
    inst = cls()
    assert inst.__arrow_ext_serialize__() == b""


def test_make_arrow_extension_type_metadata_roundtrip():
    """With metadata, __arrow_ext_serialize__ returns the provided bytes."""
    meta = b"orcapod.test"
    cls = make_arrow_extension_type(_unique_name(), pa.large_utf8(), metadata=meta)
    inst = cls()
    assert inst.__arrow_ext_serialize__() == meta


# ---------------------------------------------------------------------------
# Pure-Python LogicalTypeRegistry tests (no PA/Polars global state required)
# ---------------------------------------------------------------------------

def test_register_stores_logical_type():
    registry = LogicalTypeRegistry()
    lt = _make_stub()
    registry.register(lt)
    assert registry.get_by_logical_name(lt.logical_type_name) is lt


def test_register_same_instance_twice_is_idempotent():
    """Re-registering the exact same instance does not raise."""
    registry = LogicalTypeRegistry()
    lt = _make_stub()
    registry.register(lt)
    registry.register(lt)  # should not raise
    assert registry.get_by_logical_name(lt.logical_type_name) is lt


def test_register_conflict_on_logical_name_raises():
    """Two different instances with the same logical_type_name raise ValueError."""
    registry = LogicalTypeRegistry()
    name = _unique_name()
    lt1 = _make_stub(logical_name=name, py_type=str)
    lt2 = _make_stub(logical_name=name, py_type=bytes)
    registry.register(lt1)
    with pytest.raises(ValueError, match="logical_type_name"):
        registry.register(lt2)


def test_register_conflict_on_arrow_name_raises():
    """Two different logical types sharing the same Arrow extension name raise ValueError."""
    registry = LogicalTypeRegistry()
    arrow_name = _unique_name()
    lt1 = _make_stub(arrow_name=arrow_name, logical_name=_unique_name(), py_type=str)
    lt2 = _make_stub(arrow_name=arrow_name, logical_name=_unique_name(), py_type=bytes)
    registry.register(lt1)
    with pytest.raises(ValueError, match="arrow_extension_name"):
        registry.register(lt2)


def test_register_conflict_on_python_type_raises():
    """Two different logical types sharing the same python_type raise ValueError."""
    registry = LogicalTypeRegistry()
    lt1 = _make_stub(py_type=float)
    lt2 = _make_stub(py_type=float)
    registry.register(lt1)
    with pytest.raises(ValueError, match="python_type"):
        registry.register(lt2)


def test_get_by_logical_name_miss():
    registry = LogicalTypeRegistry()
    assert registry.get_by_logical_name("does.not.exist") is None


def test_get_by_python_type_exact():
    registry = LogicalTypeRegistry()
    lt = _make_stub(py_type=bytes)
    registry.register(lt)
    assert registry.get_by_python_type(bytes) is lt


def test_get_by_python_type_subclass():
    class _Base:
        pass

    class _Child(_Base):
        pass

    registry = LogicalTypeRegistry()
    lt = _make_stub(py_type=_Base)
    registry.register(lt)
    assert registry.get_by_python_type(_Child) is lt


def test_get_by_python_type_miss():
    registry = LogicalTypeRegistry()
    assert registry.get_by_python_type(int) is None


def test_get_by_arrow_extension_name():
    registry = LogicalTypeRegistry()
    arrow_name = _unique_name()
    lt = _make_stub(arrow_name=arrow_name)
    registry.register(lt)
    assert registry.get_by_arrow_extension_name(arrow_name) is lt


def test_get_by_arrow_extension_name_miss():
    registry = LogicalTypeRegistry()
    assert registry.get_by_arrow_extension_name("does.not.exist") is None


# ---------------------------------------------------------------------------
# PyArrow global registry tests
# ---------------------------------------------------------------------------

def test_register_populates_arrow_registry():
    """After register(), PA global registry contains the extension type."""
    lt = _make_stub()
    registry = LogicalTypeRegistry()
    registry.register(lt)

    arrow_ext_name = lt.get_arrow_extension_type().extension_name

    # If the name is registered, attempting to re-register it raises ArrowKeyError.
    # This is the only stable public signal PyArrow provides.
    class _Probe(pa.ExtensionType):
        def __init__(self):
            pa.ExtensionType.__init__(self, pa.large_utf8(), arrow_ext_name)
        def __arrow_ext_serialize__(self):
            return b""
        @classmethod
        def __arrow_ext_deserialize__(cls, st, se):
            return cls()

    with pytest.raises(pa.lib.ArrowKeyError):
        pa.register_extension_type(_Probe())


def test_register_arrow_preexisting_external_accepted_silently():
    """A name already registered externally in PyArrow is accepted silently (no raise)."""
    name = _unique_name()

    class _External(pa.ExtensionType):
        def __init__(self):
            pa.ExtensionType.__init__(self, pa.large_utf8(), name)
        def __arrow_ext_serialize__(self):
            return b""
        @classmethod
        def __arrow_ext_deserialize__(cls, st, se):
            return cls()

    pa.register_extension_type(_External())  # bypass our registry

    # New semantics: pre-existing registrations are accepted silently.
    lt = _make_stub(arrow_name=name)
    registry = LogicalTypeRegistry()
    registry.register(lt)  # should NOT raise
    assert registry.get_by_logical_name(lt.logical_type_name) is lt


def test_register_same_instance_two_registries():
    """The same LogicalType instance can be registered in two different registry instances."""
    lt = _make_stub()
    r1 = LogicalTypeRegistry()
    r2 = LogicalTypeRegistry()
    r1.register(lt)
    r2.register(lt)  # should not raise (same instance, PA/Polars accept silently)


# ---------------------------------------------------------------------------
# Polars global registry tests
# ---------------------------------------------------------------------------

def test_register_populates_polars_registry():
    """After register(), Polars knows the extension type."""
    arrow_name = _unique_name()
    lt = _make_stub(arrow_name=arrow_name)
    registry = LogicalTypeRegistry()
    registry.register(lt)

    # Verify by attempting to create a Polars series from a PA extension array.
    ArrowExtClass = make_arrow_extension_type(arrow_name, pa.large_utf8())
    storage_arr = pa.array(["a", "b"], type=pa.large_utf8())
    ext_arr = storage_arr.cast(ArrowExtClass())

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        pl_series = pl.from_arrow(ext_arr)

    assert isinstance(pl_series.dtype, pl.BaseExtension)
    assert pl_series.dtype.ext_name() == arrow_name


def test_register_polars_preexisting_external_accepted_silently():
    """A name already registered externally in Polars is accepted silently."""
    name = _unique_name()

    class _ExternalPL(pl.BaseExtension):
        def __init__(self):
            super().__init__(name, pl.String, None)
        @classmethod
        def ext_from_params(cls, n, s, m):
            return cls()

    class _ExternalPA(pa.ExtensionType):
        def __init__(self):
            pa.ExtensionType.__init__(self, pa.large_utf8(), name)
        def __arrow_ext_serialize__(self):
            return b""
        @classmethod
        def __arrow_ext_deserialize__(cls, st, se):
            return cls()

    pa.register_extension_type(_ExternalPA())
    pl.register_extension_type(name, _ExternalPL)

    lt = _make_stub(arrow_name=name)
    registry = LogicalTypeRegistry()
    registry.register(lt)  # should NOT raise
    assert registry.get_by_logical_name(lt.logical_type_name) is lt


# ---------------------------------------------------------------------------
# End-to-end integration tests
# ---------------------------------------------------------------------------


class _Color:
    """Minimal Python class used to exercise the LogicalType contract end-to-end."""
    def __init__(self, hex_str: str) -> None:
        self.hex_str = hex_str
    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Color) and self.hex_str == other.hex_str
    def __repr__(self) -> str:
        return f"Color({self.hex_str!r})"


def _make_color_logical_type() -> LogicalType:
    """LogicalType for _Color, backed by pa.large_utf8() storage."""
    _name = _unique_name()
    _ArrowExtClass = make_arrow_extension_type(_name, pa.large_utf8(), metadata=b"test.color")

    class _PolarsExt(pl.BaseExtension):
        def __init__(self):
            super().__init__(_name, pl.String, "test.color")
        @classmethod
        def ext_from_params(cls, ext_name, storage_dtype, metadata_str):
            return cls()

    class _ColorLogicalType:
        @property
        def logical_type_name(self) -> str:
            return _name

        @property
        def python_type(self) -> type:
            return _Color

        def get_arrow_extension_type(self) -> pa.ExtensionType:
            return _ArrowExtClass()

        def get_polars_extension_type(self) -> pl.BaseExtension:
            return _PolarsExt()

        def python_to_storage(self, value: _Color) -> str:
            return value.hex_str

        def storage_to_python(self, storage_value: str) -> _Color:
            return _Color(storage_value)

    return _ColorLogicalType()


def _build_ext_array(
    lt: LogicalType,
    values: list,
) -> pa.Array:
    """Build a PA extension array from Python values using the logical type."""
    storage_values = [lt.python_to_storage(v) for v in values]
    arrow_ext = lt.get_arrow_extension_type()
    storage_arr = pa.array(storage_values, type=arrow_ext.storage_type)
    return storage_arr.cast(arrow_ext)


def test_python_class_round_trip():
    """Python objects -> Arrow extension array -> Python objects via logical type methods."""
    lt = _make_color_logical_type()
    registry = LogicalTypeRegistry()
    registry.register(lt)

    originals = [_Color("#ff0000"), _Color("#00ff00"), _Color("#0000ff")]
    ext_arr = _build_ext_array(lt, originals)

    recovered = [lt.storage_to_python(v.as_py()) for v in ext_arr.storage]
    assert recovered == originals


def test_arrow_polars_round_trip():
    """PA ext array -> pl.from_arrow -> to_arrow() preserves extension type and values."""
    lt = _make_color_logical_type()
    registry = LogicalTypeRegistry()
    registry.register(lt)

    originals = [_Color("#aabbcc"), _Color("#112233")]
    ext_arr = _build_ext_array(lt, originals)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        pl_series = pl.from_arrow(ext_arr)

    assert isinstance(pl_series.dtype, pl.BaseExtension)
    assert pl_series.dtype.ext_name() == lt.get_arrow_extension_type().extension_name

    arr_back = pl_series.to_arrow()
    assert arr_back.type.extension_name == lt.get_arrow_extension_type().extension_name

    recovered = [lt.storage_to_python(v.as_py()) for v in arr_back.storage]
    assert recovered == originals


def test_parquet_round_trip():
    """PA ext array -> Parquet -> read back via PyArrow; extension type and values preserved."""
    lt = _make_color_logical_type()
    registry = LogicalTypeRegistry()
    registry.register(lt)

    originals = [_Color("#deadbe"), _Color("#cafeba")]
    ext_arr = _build_ext_array(lt, originals)
    arrow_ext = lt.get_arrow_extension_type()
    schema = pa.schema([pa.field("color", arrow_ext), pa.field("id", pa.int32())])
    table = pa.table(
        {"color": ext_arr, "id": pa.array([1, 2], type=pa.int32())},
        schema=schema,
    )

    with tempfile.TemporaryDirectory() as tmp:
        path = pathlib.Path(tmp) / "test.parquet"
        pq.write_table(table, path)
        table_back = pq.read_table(path)

    assert table_back.schema.field("color").type.extension_name == arrow_ext.extension_name
    storage_arr = table_back.column("color").combine_chunks().storage
    recovered = [lt.storage_to_python(v.as_py()) for v in storage_arr]
    assert recovered == originals


# ---------------------------------------------------------------------------
# Module-level instance test
# ---------------------------------------------------------------------------

def test_logical_type_registry_module_instance():
    """extension_types.default_logical_type_registry is a LogicalTypeRegistry."""
    from orcapod import extension_types
    assert isinstance(extension_types.default_logical_type_registry, LogicalTypeRegistry)
