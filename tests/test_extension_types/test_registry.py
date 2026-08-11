"""Tests for LogicalTypeRegistry and make_arrow_extension_type."""

from __future__ import annotations

import json
import pathlib
import tempfile
import uuid
import warnings

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from orcapod.extension_types.protocols import LogicalTypeProtocol, LogicalTypeFactoryProtocol
from orcapod.extension_types.registry import LogicalTypeRegistry, make_arrow_extension_type, make_polars_extension_type, _canonical_storage


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
) -> LogicalTypeProtocol:
    """Factory for minimal LogicalTypeProtocol conforming stubs.

    ``arrow_name`` defaults to ``logical_name`` (or a unique name if both are
    omitted) so that callers can pass a single name and get consistent arrow
    and logical names.
    """
    _arrow_name = arrow_name or logical_name or _unique_name()
    _logical_name = logical_name or _arrow_name
    _storage = storage if storage is not None else pa.large_utf8()

    ArrowExtClass = make_arrow_extension_type(_arrow_name, _storage)

    _pl_storage = pl.from_arrow(pa.array([], type=_storage)).dtype

    class _PolarsExt(pl.BaseExtension):
        def __init__(self):
            super().__init__(_arrow_name, _pl_storage, None)
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


def _make_stub_factory(return_lt: LogicalTypeProtocol | None = None) -> LogicalTypeFactoryProtocol:
    """Factory for minimal LogicalTypeFactoryProtocol conforming stubs.

    If ``return_lt`` is given, ``reconstruct_from_arrow`` returns it; otherwise
    it creates a fresh stub using ``_make_stub`` keyed on the arrow name.
    ``calls`` records every invocation as ``(arrow_extension_name, storage_type, metadata)``.
    ``python_type_calls`` records every ``create_for_python_type`` invocation.
    """
    _return_lt = return_lt

    class _Factory:
        def __init__(self):
            self.calls: list[tuple] = []
            self.python_type_calls: list[type] = []

        def reconstruct_from_arrow(self, arrow_extension_name, storage_type, metadata):
            self.calls.append((arrow_extension_name, storage_type, metadata))
            if _return_lt is not None:
                return _return_lt
            return _make_stub(arrow_name=arrow_extension_name, storage=storage_type)

        def create_for_python_type(self, python_type):
            self.python_type_calls.append(python_type)
            if _return_lt is not None:
                return _return_lt
            return _make_stub(py_type=python_type)

    return _Factory()


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
# LogicalTypeRegistry unit tests
# Each test uses a fresh LogicalTypeRegistry() instance. Registering does
# touch the global PA/Polars registries, but unique extension names (via
# _unique_name()) prevent cross-test collisions.
# ---------------------------------------------------------------------------

def test_register_stores_logical_type():
    registry = LogicalTypeRegistry()
    lt = _make_stub()
    registry.register_logical_type(lt)
    assert registry.get_by_logical_name(lt.logical_type_name) is lt


def test_register_same_instance_twice_is_idempotent():
    """Re-registering the exact same instance does not raise."""
    registry = LogicalTypeRegistry()
    lt = _make_stub()
    registry.register_logical_type(lt)
    registry.register_logical_type(lt)  # should not raise
    assert registry.get_by_logical_name(lt.logical_type_name) is lt


def test_register_conflict_on_logical_name_raises():
    """Two different instances with the same logical_type_name raise ValueError."""
    registry = LogicalTypeRegistry()
    name = _unique_name()
    lt1 = _make_stub(logical_name=name, py_type=str)
    lt2 = _make_stub(logical_name=name, py_type=bytes)
    registry.register_logical_type(lt1)
    with pytest.raises(ValueError, match="logical_type_name"):
        registry.register_logical_type(lt2)


def test_register_conflict_on_arrow_name_raises():
    """Two different logical types sharing the same Arrow extension name raise ValueError."""
    registry = LogicalTypeRegistry()
    arrow_name = _unique_name()
    lt1 = _make_stub(arrow_name=arrow_name, logical_name=_unique_name(), py_type=str)
    lt2 = _make_stub(arrow_name=arrow_name, logical_name=_unique_name(), py_type=bytes)
    registry.register_logical_type(lt1)
    with pytest.raises(ValueError, match="arrow_extension_name"):
        registry.register_logical_type(lt2)


def test_register_conflict_on_python_type_raises():
    """Two different logical types sharing the same python_type raise ValueError."""
    registry = LogicalTypeRegistry()
    lt1 = _make_stub(py_type=float)
    lt2 = _make_stub(py_type=float)
    registry.register_logical_type(lt1)
    with pytest.raises(ValueError, match="python_type"):
        registry.register_logical_type(lt2)


def test_get_by_logical_name_miss():
    registry = LogicalTypeRegistry()
    assert registry.get_by_logical_name("does.not.exist") is None


def test_get_by_python_type_exact():
    registry = LogicalTypeRegistry()
    lt = _make_stub(py_type=bytes)
    registry.register_logical_type(lt)
    assert registry.get_by_python_type(bytes) is lt


def test_get_by_python_type_subclass():
    class _Base:
        pass

    class _Child(_Base):
        pass

    registry = LogicalTypeRegistry()
    lt = _make_stub(py_type=_Base)
    registry.register_logical_type(lt)
    assert registry.get_by_python_type(_Child) is lt


def test_get_by_python_type_miss():
    registry = LogicalTypeRegistry()
    assert registry.get_by_python_type(int) is None


def test_get_by_arrow_extension_name():
    registry = LogicalTypeRegistry()
    arrow_name = _unique_name()
    lt = _make_stub(arrow_name=arrow_name)
    registry.register_logical_type(lt)
    assert registry.get_by_arrow_extension_name(arrow_name) is lt


def test_get_by_arrow_extension_name_miss():
    registry = LogicalTypeRegistry()
    assert registry.get_by_arrow_extension_name("does.not.exist") is None


# ---------------------------------------------------------------------------
# LogicalTypeRegistry constructor logical_types param tests
# ---------------------------------------------------------------------------

def test_registry_init_with_logical_types_preregisters():
    """LogicalTypeRegistry(logical_types=[lt]) makes the type immediately retrievable."""
    lt = _make_stub()
    registry = LogicalTypeRegistry(logical_types=[lt])
    assert registry.get_by_logical_name(lt.logical_type_name) is lt
    assert registry.get_by_python_type(lt.python_type) is lt
    assert registry.get_by_arrow_extension_name(lt.get_arrow_extension_type().extension_name) is lt


def test_registry_init_with_none_is_empty():
    """LogicalTypeRegistry(logical_types=None) starts empty without error."""
    registry = LogicalTypeRegistry(logical_types=None)
    assert registry.get_by_logical_name("anything") is None


def test_registry_init_with_empty_list_is_empty():
    """LogicalTypeRegistry(logical_types=[]) starts empty without error."""
    registry = LogicalTypeRegistry(logical_types=[])
    assert registry.get_by_logical_name("anything") is None


def test_registry_init_with_multiple_logical_types():
    """LogicalTypeRegistry(logical_types=[lt1, lt2]) registers both."""
    lt1 = _make_stub(py_type=int)
    lt2 = _make_stub(py_type=float)
    registry = LogicalTypeRegistry(logical_types=[lt1, lt2])
    assert registry.get_by_logical_name(lt1.logical_type_name) is lt1
    assert registry.get_by_logical_name(lt2.logical_type_name) is lt2


# ---------------------------------------------------------------------------
# register_logical_type_factory tests
# ---------------------------------------------------------------------------

def test_register_logical_type_factory_no_error():
    """register_logical_type_factory completes without raising."""
    registry = LogicalTypeRegistry()
    factory = _make_stub_factory()
    registry.register_logical_type_factory(factory, category="TestCat")  # should not raise


def test_register_logical_type_factory_same_instance_idempotent():
    """Re-registering the same factory instance for the same category does not raise."""
    registry = LogicalTypeRegistry()
    factory = _make_stub_factory()
    registry.register_logical_type_factory(factory, category="Cat")
    registry.register_logical_type_factory(factory, category="Cat")  # should not raise


def test_register_duplicate_category_raises():
    """Registering a different factory for an already-registered category raises ValueError."""
    registry = LogicalTypeRegistry()
    f1 = _make_stub_factory()
    f2 = _make_stub_factory()
    registry.register_logical_type_factory(f1, category="Cat")
    with pytest.raises(ValueError, match="Cat"):
        registry.register_logical_type_factory(f2, category="Cat")


def test_register_logical_type_factory_keyword_category():
    """register_logical_type_factory accepts factory as first arg, category as keyword."""
    registry = LogicalTypeRegistry()
    factory = _make_stub_factory()
    registry.register_logical_type_factory(factory, category="TestCat")  # no error


def test_register_logical_type_factory_keyword_python_bases():
    """register_logical_type_factory accepts python_bases as keyword."""
    registry = LogicalTypeRegistry()
    factory = _make_stub_factory()
    registry.register_logical_type_factory(factory, python_bases=[str])  # no error


def test_register_logical_type_factory_both_axes():
    """register_logical_type_factory accepts both category and python_bases."""
    registry = LogicalTypeRegistry()
    factory = _make_stub_factory()
    registry.register_logical_type_factory(factory, category="Cat", python_bases=[str, int])


def test_register_logical_type_factory_no_axes_raises():
    """register_logical_type_factory raises ValueError when called with no axes."""
    registry = LogicalTypeRegistry()
    factory = _make_stub_factory()
    with pytest.raises(ValueError, match="At least one of"):
        registry.register_logical_type_factory(factory)


def test_register_logical_type_factory_python_base_duplicate_different_factory_raises():
    """Registering a different factory for the same python_base raises ValueError."""
    registry = LogicalTypeRegistry()
    f1 = _make_stub_factory()
    f2 = _make_stub_factory()
    registry.register_logical_type_factory(f1, python_bases=[str])
    with pytest.raises(ValueError, match="different factory"):
        registry.register_logical_type_factory(f2, python_bases=[str])


def test_register_logical_type_factory_python_base_same_factory_idempotent():
    """Registering the same factory twice for the same python_base is a no-op."""
    registry = LogicalTypeRegistry()
    factory = _make_stub_factory()
    registry.register_logical_type_factory(factory, python_bases=[str])
    registry.register_logical_type_factory(factory, python_bases=[str])  # no error

# ---------------------------------------------------------------------------
# PyArrow global registry tests
# ---------------------------------------------------------------------------

def test_register_populates_arrow_registry():
    """After register(), PA global registry contains the extension type."""
    lt = _make_stub()
    registry = LogicalTypeRegistry()
    registry.register_logical_type(lt)

    # If the name is registered, attempting to re-register the same type raises
    # ArrowKeyError. This is the only stable public signal PyArrow provides.
    with pytest.raises(pa.lib.ArrowKeyError):
        pa.register_extension_type(lt.get_arrow_extension_type())


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
    registry.register_logical_type(lt)  # should NOT raise
    assert registry.get_by_logical_name(lt.logical_type_name) is lt


def test_register_same_instance_two_registries():
    """The same LogicalTypeProtocol instance can be registered in two different registry instances."""
    lt = _make_stub()
    r1 = LogicalTypeRegistry()
    r2 = LogicalTypeRegistry()
    r1.register_logical_type(lt)
    r2.register_logical_type(lt)  # should not raise (same instance, PA/Polars accept silently)
    assert r2.get_by_logical_name(lt.logical_type_name) is lt


# ---------------------------------------------------------------------------
# Polars global registry tests
# ---------------------------------------------------------------------------

def test_register_populates_polars_registry():
    """After register(), Polars knows the extension type."""
    arrow_name = _unique_name()
    lt = _make_stub(arrow_name=arrow_name)
    registry = LogicalTypeRegistry()
    registry.register_logical_type(lt)

    # Verify by attempting to create a Polars series from a PA extension array.
    ext_type = lt.get_arrow_extension_type()
    storage_arr = pa.array(["a", "b"], type=ext_type.storage_type)
    ext_arr = storage_arr.cast(ext_type)

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
    registry.register_logical_type(lt)  # should NOT raise
    assert registry.get_by_logical_name(lt.logical_type_name) is lt


# ---------------------------------------------------------------------------
# End-to-end integration tests
# ---------------------------------------------------------------------------


class _Color:
    """Minimal Python class used to exercise the LogicalTypeProtocol contract end-to-end."""
    def __init__(self, hex_str: str) -> None:
        self.hex_str = hex_str
    def __eq__(self, other: object) -> bool:
        return isinstance(other, _Color) and self.hex_str == other.hex_str
    def __repr__(self) -> str:
        return f"Color({self.hex_str!r})"


def _make_color_logical_type() -> LogicalTypeProtocol:
    """LogicalTypeProtocol for _Color, backed by pa.large_utf8() storage."""
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
    lt: LogicalTypeProtocol,
    values: list,
) -> pa.Array:
    """Build a PA extension array from Python values using the logical type.

    Global registration (via ``registry.register_logical_type(lt)``) is NOT required for
    this helper — ``cast()`` works with any ``pa.ExtensionType`` instance.
    Registration is only needed for IPC/Parquet *deserialization*, where Arrow
    maps the ``extension_name`` string back to the registered Python type.
    """
    storage_values = [lt.python_to_storage(v) for v in values]
    arrow_ext = lt.get_arrow_extension_type()
    storage_arr = pa.array(storage_values, type=arrow_ext.storage_type)
    return storage_arr.cast(arrow_ext)


def test_python_class_round_trip():
    """Python objects -> Arrow extension array -> Python objects via logical type methods."""
    lt = _make_color_logical_type()
    registry = LogicalTypeRegistry()
    registry.register_logical_type(lt)

    originals = [_Color("#ff0000"), _Color("#00ff00"), _Color("#0000ff")]
    ext_arr = _build_ext_array(lt, originals)

    recovered = [lt.storage_to_python(v.as_py()) for v in ext_arr.storage]
    assert recovered == originals


def test_arrow_polars_round_trip():
    """PA ext array -> pl.from_arrow -> to_arrow() preserves extension type and values."""
    lt = _make_color_logical_type()
    registry = LogicalTypeRegistry()
    registry.register_logical_type(lt)

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
    registry.register_logical_type(lt)

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
# make_polars_extension_type tests
# ---------------------------------------------------------------------------


def test_make_polars_extension_type_returns_class():
    """make_polars_extension_type returns a pl.BaseExtension subclass."""
    cls = make_polars_extension_type("test.MakePolarsExt", pa.large_utf8())
    assert issubclass(cls, pl.BaseExtension)


def test_make_polars_extension_type_instance_has_correct_name():
    """Instantiating the returned class yields the correct ext_name."""
    name = _unique_name()
    cls = make_polars_extension_type(name, pa.large_utf8())
    inst = cls()
    assert inst.ext_name() == name


def test_make_polars_extension_type_ext_from_params_returns_instance():
    """ext_from_params classmethod returns an instance of the class."""
    name = _unique_name()
    cls = make_polars_extension_type(name, pa.large_utf8())
    inst = cls.ext_from_params(name, pl.String, None)
    assert isinstance(inst, cls)


def test_make_polars_extension_type_with_binary_storage():
    """make_polars_extension_type works with pa.binary(16) storage (UUID case)."""
    name = _unique_name()
    cls = make_polars_extension_type(name, pa.binary(16), None)
    inst = cls()
    assert inst.ext_name() == name


def test_make_polars_extension_type_with_metadata():
    """make_polars_extension_type captures metadata in the class."""
    name = _unique_name()
    cls = make_polars_extension_type(name, pa.large_utf8(), "test.metadata")
    # Instantiating should not raise; ext_name is correct.
    inst = cls()
    assert inst.ext_name() == name




def test_registry_does_not_expose_ensure_methods():
    """ensure_logical_type_for_python_class and ensure_extension_type are removed."""
    registry = LogicalTypeRegistry()
    assert not hasattr(registry, "ensure_logical_type_for_python_class")
    assert not hasattr(registry, "ensure_extension_type")


# ---------------------------------------------------------------------------
# _canonical_storage helper
# ---------------------------------------------------------------------------

class TestCanonicalStorage:
    """Unit tests for the _canonical_storage helper."""

    def test_string_maps_to_large_string(self):
        assert _canonical_storage(pa.string()) == pa.large_string()

    def test_utf8_maps_to_large_string(self):
        # pa.utf8() is an alias for pa.string()
        assert _canonical_storage(pa.utf8()) == pa.large_string()

    def test_string_view_maps_to_large_string(self):
        assert _canonical_storage(pa.string_view()) == pa.large_string()

    def test_large_string_is_identity(self):
        assert _canonical_storage(pa.large_string()) == pa.large_string()

    def test_binary_maps_to_large_binary(self):
        assert _canonical_storage(pa.binary()) == pa.large_binary()

    def test_binary_view_maps_to_large_binary(self):
        assert _canonical_storage(pa.binary_view()) == pa.large_binary()

    def test_large_binary_is_identity(self):
        assert _canonical_storage(pa.large_binary()) == pa.large_binary()

    def test_int64_is_identity(self):
        assert _canonical_storage(pa.int64()) == pa.int64()

    def test_fixed_binary_is_identity(self):
        assert _canonical_storage(pa.binary(16)) == pa.binary(16)


# ---------------------------------------------------------------------------
# __arrow_ext_deserialize__ storage-family tolerance (ITL-602)
# ---------------------------------------------------------------------------

def _make_large_string_ext(name: str) -> pa.ExtensionType:
    """Return an extension type instance backed by large_string."""
    cls = make_arrow_extension_type(name, pa.large_string())
    return cls()


def _make_large_binary_ext(name: str) -> pa.ExtensionType:
    """Return an extension type instance backed by large_binary."""
    cls = make_arrow_extension_type(name, pa.large_binary())
    return cls()


class TestDeserializeStorageFamilyTolerance:
    """Extension deserializer accepts equivalent string/binary layouts (ITL-602).

    Each test builds a minimal extension type registered with large_string or
    large_binary, then calls __arrow_ext_deserialize__ directly with a
    physically-different-but-logically-equivalent storage type and asserts the
    result is the canonical extension type instance.

    Cross-family mismatches must still raise ValueError.
    """

    def test_deserialize_accepts_string_for_large_string(self):
        """string physical type is accepted when large_string is registered."""
        name = _unique_name()
        ext = _make_large_string_ext(name)
        result = ext.__arrow_ext_deserialize__(pa.string(), b"")
        assert result.extension_name == name
        assert result.storage_type == pa.large_string()

    def test_deserialize_accepts_large_string_for_large_string(self):
        """large_string physical type (normal path) still works."""
        name = _unique_name()
        ext = _make_large_string_ext(name)
        result = ext.__arrow_ext_deserialize__(pa.large_string(), b"")
        assert result.extension_name == name
        assert result.storage_type == pa.large_string()

    def test_deserialize_accepts_string_view_for_large_string(self):
        """string_view physical type is accepted when large_string is registered."""
        name = _unique_name()
        ext = _make_large_string_ext(name)
        result = ext.__arrow_ext_deserialize__(pa.string_view(), b"")
        assert result.extension_name == name
        assert result.storage_type == pa.large_string()

    def test_deserialize_accepts_binary_for_large_binary(self):
        """binary physical type is accepted when large_binary is registered."""
        name = _unique_name()
        ext = _make_large_binary_ext(name)
        result = ext.__arrow_ext_deserialize__(pa.binary(), b"")
        assert result.extension_name == name
        assert result.storage_type == pa.large_binary()

    def test_deserialize_accepts_binary_view_for_large_binary(self):
        """binary_view physical type is accepted when large_binary is registered."""
        name = _unique_name()
        ext = _make_large_binary_ext(name)
        result = ext.__arrow_ext_deserialize__(pa.binary_view(), b"")
        assert result.extension_name == name
        assert result.storage_type == pa.large_binary()

    def test_deserialize_rejects_binary_for_large_string(self):
        """binary is rejected when large_string is registered (cross-family)."""
        name = _unique_name()
        ext = _make_large_string_ext(name)
        with pytest.raises(ValueError, match="expected storage_type"):
            ext.__arrow_ext_deserialize__(pa.binary(), b"")

    def test_deserialize_rejects_string_for_large_binary(self):
        """string is rejected when large_binary is registered (cross-family)."""
        name = _unique_name()
        ext = _make_large_binary_ext(name)
        with pytest.raises(ValueError, match="expected storage_type"):
            ext.__arrow_ext_deserialize__(pa.string(), b"")

    def test_non_canonical_storage_accepts_exact_match(self):
        """Extension registered with plain string (non-canonical) still reads its own data.

        Regression test for the Copilot review finding: the previous check
        _canonical_storage(storage_type) != _storage would map string → large_string and
        then compare large_string != string, incorrectly raising for a type that should
        read back cleanly.
        """
        name = _unique_name()
        cls = make_arrow_extension_type(name, pa.string())  # non-canonical storage
        ext = cls()
        result = ext.__arrow_ext_deserialize__(pa.string(), b"")
        assert result.extension_name == name
        assert result.storage_type == pa.string()

    def test_non_canonical_storage_rejects_large_variant(self):
        """Extension registered with plain string rejects large_string physical data.

        Accepting large_string → string would require silent offset narrowing (64-bit →
        32-bit), which can overflow for values > 2 GB. The strict check prevents this.
        """
        name = _unique_name()
        cls = make_arrow_extension_type(name, pa.string())  # non-canonical storage
        ext = cls()
        with pytest.raises(ValueError, match="expected storage_type"):
            ext.__arrow_ext_deserialize__(pa.large_string(), b"")

    def test_metadata_mismatch_raises_even_when_storage_family_matches(self):
        """Metadata check stays strict regardless of storage compatibility."""
        name = _unique_name()
        cls = make_arrow_extension_type(name, pa.large_string(), metadata=b"expected")
        ext = cls()
        with pytest.raises(ValueError, match="expected metadata"):
            ext.__arrow_ext_deserialize__(pa.string(), b"wrong")

    def test_metadata_mismatch_raises_for_canonical_storage_too(self):
        """Metadata check raises even when storage_type matches exactly."""
        name = _unique_name()
        cls = make_arrow_extension_type(name, pa.large_string(), metadata=b"expected")
        ext = cls()
        with pytest.raises(ValueError, match="expected metadata"):
            ext.__arrow_ext_deserialize__(pa.large_string(), b"wrong")


# ---------------------------------------------------------------------------
# Parquet roundtrip with non-canonical physical storage (ITL-602)
# ---------------------------------------------------------------------------

def _write_string_backed_parquet(path: str, ext_name: str) -> None:
    """Write a parquet file where the physical type is string but extension metadata
    records ext_name.  Simulates the output of delta-rs optimize.compact().
    """
    write_cls = make_arrow_extension_type(ext_name, pa.string())
    write_ext = write_cls()
    arr = pa.ExtensionArray.from_storage(write_ext, pa.array(["hello", "world"], type=pa.string()))
    pq.write_table(pa.table({"col": arr}), path)
    # Unregister write-side type so the read-side (large_string) handler takes over.
    try:
        pa.unregister_extension_type(ext_name)
    except Exception:
        pass


class TestParquetRoundtripNonCanonicalStorage:
    """Parquet files with string physical type + extension metadata read correctly
    after the ITL-602 fix.
    """

    def test_string_physical_reads_as_large_string_extension(self, tmp_path):
        """Parquet with string physical type + extension metadata deserialises to
        large_string storage without raising."""
        name = _unique_name()
        path = str(tmp_path / "test.parquet")
        _write_string_backed_parquet(path, name)

        # Register the canonical (large_string) handler — this is what orcapod does at startup.
        read_cls = make_arrow_extension_type(name, pa.large_string())
        pa.register_extension_type(read_cls())

        try:
            result = pq.read_table(path)
            col = result.column("col")
            chunk = col.chunk(0)
            assert chunk.storage.type == pa.large_string(), (
                f"Expected large_string storage after widening, got {chunk.storage.type}"
            )
            assert chunk.to_pylist() == ["hello", "world"]
        finally:
            try:
                pa.unregister_extension_type(name)
            except Exception:
                pass


def test_register_logical_type_conflict_error_uses_repr_for_generic_alias():
    """Conflict error message for GenericAlias python_type must not raise AttributeError."""
    import uuid
    import pyarrow as pa
    from orcapod.extension_types.registry import LogicalTypeRegistry, make_arrow_extension_type, make_polars_extension_type
    from orcapod.extension_types.base_logical_type import BaseLogicalType

    class _FakeListLT(BaseLogicalType):
        logical_type_name = "list[orcapod.uuid]"
        python_type = list[uuid.UUID]

        def get_arrow_extension_type(self):
            ext_cls = make_arrow_extension_type(
                "list[orcapod.uuid]", pa.large_list(pa.large_binary())
            )
            return ext_cls()

        def get_polars_extension_type(self):
            ext_cls = make_polars_extension_type(
                "list[orcapod.uuid]", pa.large_list(pa.large_binary())
            )
            return ext_cls()

        def python_to_storage(self, value, converter):
            return value

        def storage_to_python(self, storage_value, converter):
            return storage_value

    class _FakeListLT2(_FakeListLT):
        """Different instance, same keys — should raise ValueError, not AttributeError."""

    registry = LogicalTypeRegistry()
    lt1 = _FakeListLT()
    lt2 = _FakeListLT2()
    try:
        registry.register_logical_type(lt1)

        # The registry should raise ValueError (not AttributeError) even when python_type
        # is a GenericAlias (like list[uuid.UUID]) that lacks __qualname__.
        with pytest.raises(ValueError):
            registry.register_logical_type(lt2)
    finally:
        # Clean up PyArrow's global registry so subsequent tests can register
        # "list[orcapod.uuid]" with different metadata without conflict.
        try:
            pa.unregister_extension_type("list[orcapod.uuid]")
        except Exception:
            pass
