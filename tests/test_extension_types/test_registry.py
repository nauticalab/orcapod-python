"""Tests for ExtensionTypeRegistry."""

from __future__ import annotations

import pathlib
import re
import tempfile
import uuid
import warnings

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from orcapod.extension_types.protocols import ExtensionTypeConverter
from orcapod.extension_types.registry import ExtensionTypeRegistry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _unique_name() -> str:
    """Unique extension name to avoid cross-test global-registry collisions."""
    return f"test.registry.{uuid.uuid4().hex[:8]}"


def _make_stub(
    name: str | None = None,
    storage: pa.DataType | None = None,
    metadata: bytes | None = b"test.category",
    py_type: type = str,
) -> ExtensionTypeConverter:
    """Factory for minimal ExtensionTypeConverter conforming stubs."""
    _name = name or _unique_name()
    _storage = storage if storage is not None else pa.large_utf8()
    _metadata = metadata
    _py_type = py_type

    class _Stub:
        @property
        def extension_name(self) -> str:
            return _name

        @property
        def extension_metadata(self) -> bytes | None:
            return _metadata

        @property
        def storage_type(self) -> pa.DataType:
            return _storage

        @property
        def python_type(self) -> type:
            return _py_type

        def python_to_storage(self, value):
            return str(value)

        def storage_to_python(self, storage_value):
            return storage_value

    return _Stub()


# ---------------------------------------------------------------------------
# Pure-Python registry tests (no PA/Polars global state required)
# ---------------------------------------------------------------------------

def test_register_stores_converter():
    registry = ExtensionTypeRegistry()
    conv = _make_stub()
    registry.register(conv)
    assert registry.get_converter_for_name(conv.extension_name) is conv


def test_register_duplicate_raises():
    registry = ExtensionTypeRegistry()
    name = _unique_name()
    registry.register(_make_stub(name=name))
    with pytest.raises(ValueError, match=name):
        registry.register(_make_stub(name=name))


def test_get_converter_for_name_miss():
    registry = ExtensionTypeRegistry()
    assert registry.get_converter_for_name("does.not.exist") is None


def test_get_converter_for_python_type_exact():
    registry = ExtensionTypeRegistry()
    conv = _make_stub(py_type=bytes)
    registry.register(conv)
    assert registry.get_converter_for_python_type(bytes) is conv


def test_get_converter_for_python_type_subclass():
    class _Base:
        pass

    class _Child(_Base):
        pass

    registry = ExtensionTypeRegistry()
    conv = _make_stub(py_type=_Base)
    registry.register(conv)
    assert registry.get_converter_for_python_type(_Child) is conv


def test_get_converter_for_python_type_miss():
    registry = ExtensionTypeRegistry()
    assert registry.get_converter_for_python_type(int) is None


def test_has_extension_name():
    registry = ExtensionTypeRegistry()
    conv = _make_stub()
    assert not registry.has_extension_name(conv.extension_name)
    registry.register(conv)
    assert registry.has_extension_name(conv.extension_name)


def test_has_python_type():
    registry = ExtensionTypeRegistry()
    conv = _make_stub(py_type=float)
    assert not registry.has_python_type(float)
    registry.register(conv)
    assert registry.has_python_type(float)


def test_list_extension_names():
    registry = ExtensionTypeRegistry()
    a = _make_stub()
    b = _make_stub()
    registry.register(a)
    registry.register(b)
    assert registry.list_extension_names() == [a.extension_name, b.extension_name]


def test_list_python_types():
    registry = ExtensionTypeRegistry()
    a = _make_stub(py_type=bytes)
    b = _make_stub(py_type=float)
    registry.register(a)
    registry.register(b)
    assert registry.list_python_types() == [bytes, float]


# ---------------------------------------------------------------------------
# PyArrow global registry tests
# ---------------------------------------------------------------------------

def test_register_populates_arrow_registry():
    """After register(), PA global registry contains the extension type."""
    conv = _make_stub()
    registry = ExtensionTypeRegistry()
    registry.register(conv)

    # If the name is registered, attempting to re-register it raises ArrowKeyError.
    # This is the only stable public signal PyArrow provides.
    class _Probe(pa.ExtensionType):
        def __init__(self):
            pa.ExtensionType.__init__(self, pa.large_utf8(), conv.extension_name)
        def __arrow_ext_serialize__(self):
            return b""
        @classmethod
        def __arrow_ext_deserialize__(cls, st, se):
            return cls()

    with pytest.raises(pa.lib.ArrowKeyError):
        pa.register_extension_type(_Probe())


def test_register_arrow_global_collision_same_params_is_idempotent():
    """A second registry instance registering the same name+params succeeds silently."""
    name = _unique_name()
    conv = _make_stub(name=name, storage=pa.large_utf8(), metadata=b"cat")

    ExtensionTypeRegistry().register(conv)   # first — populates _ARROW_REGISTRY
    ExtensionTypeRegistry().register(conv)   # second — should not raise


def test_register_arrow_global_collision_different_storage_raises():
    """A second registry using the same name but different storage_type raises."""
    name = _unique_name()
    ExtensionTypeRegistry().register(_make_stub(name=name, storage=pa.large_utf8()))

    with pytest.raises(ValueError, match=name):
        ExtensionTypeRegistry().register(_make_stub(name=name, storage=pa.large_binary()))


def test_register_arrow_global_collision_different_metadata_raises():
    """A second registry using the same name but different metadata raises."""
    name = _unique_name()
    ExtensionTypeRegistry().register(_make_stub(name=name, metadata=b"original"))

    with pytest.raises(ValueError, match=name):
        ExtensionTypeRegistry().register(_make_stub(name=name, metadata=b"different"))


def test_register_arrow_external_registration_raises():
    """A name registered directly with PyArrow (bypassing our registry) raises on register()."""
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

    with pytest.raises(ValueError, match="external source"):
        ExtensionTypeRegistry().register(_make_stub(name=name))


# ---------------------------------------------------------------------------
# Polars global registry tests
# ---------------------------------------------------------------------------

def test_register_populates_polars_registry():
    """After register(), _POLARS_REGISTRY shadow dict contains the extension type."""
    conv = _make_stub(storage=pa.large_utf8())
    registry = ExtensionTypeRegistry()
    registry.register(conv)

    from orcapod.extension_types.registry import _POLARS_REGISTRY
    assert conv.extension_name in _POLARS_REGISTRY
    stored_storage, stored_meta = _POLARS_REGISTRY[conv.extension_name]
    assert stored_storage == pl.String
    assert stored_meta == "test.category"


def test_register_polars_global_collision_same_params_is_idempotent():
    """A second registry instance registering the same name+params succeeds silently."""
    name = _unique_name()
    conv = _make_stub(name=name, storage=pa.large_utf8(), metadata=b"cat")

    ExtensionTypeRegistry().register(conv)
    ExtensionTypeRegistry().register(conv)   # should not raise


def test_register_polars_global_collision_different_storage_raises():
    """A second registry using the same name but different storage_type raises."""
    name = _unique_name()
    ExtensionTypeRegistry().register(_make_stub(name=name, storage=pa.large_utf8()))

    with pytest.raises(ValueError, match=name):
        ExtensionTypeRegistry().register(_make_stub(name=name, storage=pa.large_binary()))


def test_register_polars_global_collision_different_metadata_raises():
    """A second registry using the same name but different metadata raises."""
    name = _unique_name()
    ExtensionTypeRegistry().register(_make_stub(name=name, metadata=b"original"))

    with pytest.raises(ValueError, match=name):
        ExtensionTypeRegistry().register(_make_stub(name=name, metadata=b"different"))


def test_register_polars_external_registration_raises():
    """A name registered directly with Polars (bypassing our registry) raises on register()."""
    name = _unique_name()

    class _ExternalPL(pl.BaseExtension):
        def __init__(self):
            super().__init__(name, pl.String, None)
        @classmethod
        def ext_from_params(cls, n, s, m):
            return cls()

    # Also register in PA first so we don't hit the PA external-registration error
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

    with pytest.raises(ValueError, match="external source"):
        ExtensionTypeRegistry().register(_make_stub(name=name))


# ---------------------------------------------------------------------------
# End-to-end integration tests
# ---------------------------------------------------------------------------


class _Color:
    """Minimal Python class used to exercise the converter contract end-to-end."""
    def __init__(self, hex_str: str) -> None:
        self.hex_str = hex_str
    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Color) and self.hex_str == other.hex_str
    def __repr__(self) -> str:
        return f"Color({self.hex_str!r})"


def _make_color_converter() -> ExtensionTypeConverter:
    """ExtensionTypeConverter for _Color, backed by pa.large_utf8() storage."""
    _name = _unique_name()

    class _ColorConverter:
        @property
        def extension_name(self) -> str:
            return _name
        @property
        def extension_metadata(self) -> bytes | None:
            return b"test.color"
        @property
        def storage_type(self) -> pa.DataType:
            return pa.large_utf8()
        @property
        def python_type(self) -> type:
            return _Color
        def python_to_storage(self, value: _Color) -> str:
            return value.hex_str
        def storage_to_python(self, storage_value: str) -> _Color:
            return _Color(storage_value)

    return _ColorConverter()


def _build_ext_array(
    converter: ExtensionTypeConverter,
    values: list,
) -> pa.Array:
    """Build a PA extension array from Python values using the converter."""
    storage_values = [converter.python_to_storage(v) for v in values]
    storage_arr = pa.array(storage_values, type=converter.storage_type)

    _name = converter.extension_name
    _storage = converter.storage_type
    _metadata = converter.extension_metadata or b""
    _sanitized = re.sub(r"[^A-Za-z0-9]", "_", _name)

    ArrowExtType = type(
        f"_ArrowExt_{_sanitized}_probe",
        (pa.ExtensionType,),
        {
            "__init__": lambda self: pa.ExtensionType.__init__(self, _storage, _name),
            "__arrow_ext_serialize__": lambda self: _metadata,
            "__arrow_ext_deserialize__": classmethod(lambda cls, st, se: cls()),
        },
    )
    ext_type_instance = ArrowExtType()
    return storage_arr.cast(ext_type_instance)


def test_python_class_round_trip():
    """Python objects -> Arrow extension array -> Python objects via converter methods."""
    conv = _make_color_converter()
    registry = ExtensionTypeRegistry()
    registry.register(conv)

    originals = [_Color("#ff0000"), _Color("#00ff00"), _Color("#0000ff")]
    ext_arr = _build_ext_array(conv, originals)

    # Decode back
    storage_back = ext_arr.cast(conv.storage_type)
    recovered = [conv.storage_to_python(v.as_py()) for v in storage_back]
    assert recovered == originals


def test_arrow_polars_round_trip():
    """PA ext array -> pl.from_arrow -> to_arrow() preserves extension type and values."""
    conv = _make_color_converter()
    registry = ExtensionTypeRegistry()
    registry.register(conv)

    originals = [_Color("#aabbcc"), _Color("#112233")]
    ext_arr = _build_ext_array(conv, originals)

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        pl_series = pl.from_arrow(ext_arr)

    assert isinstance(pl_series.dtype, pl.BaseExtension)
    assert pl_series.dtype.ext_name() == conv.extension_name

    arr_back = pl_series.to_arrow()
    assert arr_back.type.extension_name == conv.extension_name

    recovered = [conv.storage_to_python(v.as_py()) for v in arr_back.cast(conv.storage_type)]
    assert recovered == originals


def test_parquet_round_trip():
    """PA ext array -> Parquet -> read back via PyArrow; extension type and values preserved."""
    conv = _make_color_converter()
    registry = ExtensionTypeRegistry()
    registry.register(conv)

    originals = [_Color("#deadbe"), _Color("#cafeba")]
    ext_arr = _build_ext_array(conv, originals)
    schema = pa.schema([pa.field("color", ext_arr.type), pa.field("id", pa.int32())])
    table = pa.table(
        {"color": ext_arr, "id": pa.array([1, 2], type=pa.int32())},
        schema=schema,
    )

    with tempfile.TemporaryDirectory() as tmp:
        path = pathlib.Path(tmp) / "test.parquet"
        pq.write_table(table, path)
        table_back = pq.read_table(path)

    assert table_back.schema.field("color").type.extension_name == conv.extension_name
    recovered = [
        conv.storage_to_python(v.as_py())
        for v in table_back.column("color").cast(conv.storage_type)
    ]
    assert recovered == originals


# ---------------------------------------------------------------------------
# Module-level instance test
# ---------------------------------------------------------------------------

def test_extension_type_registry_module_instance():
    """extension_types.extension_type_registry is an ExtensionTypeRegistry, starts empty."""
    from orcapod import extension_types
    assert isinstance(extension_types.extension_type_registry, ExtensionTypeRegistry)
    # PLT-1653 scope: no built-in converters registered yet (that is PLT-1656)
    assert extension_types.extension_type_registry.list_extension_names() == []
