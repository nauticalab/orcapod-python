"""Tests for LogicalTypeProtocol, LogicalTypeFactoryProtocol, and TypeConverterProtocol."""

from __future__ import annotations

import pyarrow as pa
import polars as pl

from orcapod.logical_types.protocols import LogicalTypeProtocol
from orcapod.logical_types.registry import make_arrow_extension_type


class _StubLogicalType:
    """Minimal conforming implementation of LogicalTypeProtocol for use in tests."""

    _ArrowExtClass = make_arrow_extension_type("test.module.MyType", pa.large_string())

    @property
    def logical_type_name(self) -> str:
        return "test.module.MyType"

    @property
    def python_type(self) -> type:
        return str

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        return self._ArrowExtClass()

    def get_polars_extension_type(self) -> pl.BaseExtension:
        class _PolarsExt(pl.BaseExtension):
            def __init__(self):
                super().__init__("test.module.MyType", pl.String, None)
            @classmethod
            def ext_from_params(cls, ext_name, storage_dtype, metadata_str):
                return cls()
        return _PolarsExt()

    def python_to_storage(self, value, converter):  # converter param added
        return str(value)

    def storage_to_python(self, storage_value, converter):  # converter param added
        return storage_value

    def pick_field(self, key: str) -> type:
        raise NotImplementedError

    def index_element(self) -> type:
        raise NotImplementedError


class _StubFactory:
    """Minimal conforming implementation of LogicalTypeFactoryProtocol for use in tests."""

    def supports_class(self, python_type):  # new method
        return True

    def reconstruct_from_arrow(self, arrow_extension_name, storage_type, metadata, converter):
        return _StubLogicalType()

    def create_for_python_type(self, python_type, converter):
        return _StubLogicalType()


def test_type_converter_protocol_is_importable():
    from orcapod.logical_types.protocols import TypeConverterProtocol
    assert TypeConverterProtocol is not None


def test_factory_supports_class_method_required():
    """LogicalTypeFactoryProtocol requires supports_class."""
    from orcapod.logical_types.protocols import LogicalTypeFactoryProtocol

    class _BadFactory:
        def reconstruct_from_arrow(self, name, storage_type, metadata, converter):
            pass
        def create_for_python_type(self, python_type, converter):
            pass
        # Missing supports_class

    assert not isinstance(_BadFactory(), LogicalTypeFactoryProtocol)


def test_factory_with_supports_class_satisfies_protocol():
    from orcapod.logical_types.protocols import LogicalTypeFactoryProtocol

    class _GoodFactory:
        def supports_class(self, python_type):
            return True
        def reconstruct_from_arrow(self, name, storage_type, metadata, converter):
            pass
        def create_for_python_type(self, python_type, converter):
            pass

    assert isinstance(_GoodFactory(), LogicalTypeFactoryProtocol)


def test_logical_type_python_to_storage_accepts_converter():
    """LogicalTypeProtocol.python_to_storage now requires converter param."""
    from orcapod.logical_types.protocols import LogicalTypeProtocol

    class _GoodLT:
        @property
        def logical_type_name(self): return "test.lt"
        @property
        def python_type(self): return str
        def get_arrow_extension_type(self): pass
        def get_polars_extension_type(self): pass
        def python_to_storage(self, value, converter): return value
        def storage_to_python(self, storage_value, converter): return storage_value
        def pick_field(self, key: str) -> type: raise NotImplementedError
        def index_element(self) -> type: raise NotImplementedError

    assert isinstance(_GoodLT(), LogicalTypeProtocol)


def test_logical_type_factory_protocol_is_importable():
    """LogicalTypeFactoryProtocol can be imported from logical_types.protocols."""
    from orcapod.logical_types.protocols import LogicalTypeFactoryProtocol
    assert LogicalTypeFactoryProtocol is not None


def test_logical_type_factory_conforming_class_satisfies_protocol():
    """A conforming class is recognized as a LogicalTypeFactoryProtocol instance."""
    from orcapod.logical_types.protocols import LogicalTypeFactoryProtocol
    assert isinstance(_StubFactory(), LogicalTypeFactoryProtocol)


def test_logical_type_factory_create_returns_logical_type():
    """A conforming factory returns a LogicalTypeProtocol from reconstruct_from_arrow."""
    from orcapod.logical_types.protocols import LogicalTypeFactoryProtocol, LogicalTypeProtocol
    factory: LogicalTypeFactoryProtocol = _StubFactory()
    result = factory.reconstruct_from_arrow(
        "test.ext", pa.large_utf8(), {"category": "Test"}, converter=None
    )
    assert isinstance(result, LogicalTypeProtocol)


def test_protocol_is_importable():
    """LogicalTypeProtocol can be imported from logical_types.protocols."""
    assert LogicalTypeProtocol is not None


def test_protocol_defines_required_members():
    """A conforming class is recognized as a LogicalTypeProtocol instance."""
    assert isinstance(_StubLogicalType(), LogicalTypeProtocol)


def test_conforming_class_satisfies_protocol():
    """A class implementing all required members works correctly via the protocol interface."""
    lt: LogicalTypeProtocol = _StubLogicalType()
    assert lt.logical_type_name == "test.module.MyType"
    assert lt.python_type is str
    assert lt.get_arrow_extension_type().extension_name == "test.module.MyType"
    assert isinstance(lt.get_polars_extension_type(), pl.BaseExtension)
    assert lt.python_to_storage(42, None) == "42"   # pass converter=None
    assert lt.storage_to_python("hello", None) == "hello"  # pass converter=None


def test_factory_create_for_python_type_conformance():
    """A conforming factory implements create_for_python_type and returns LogicalTypeProtocol."""
    from orcapod.logical_types.protocols import LogicalTypeFactoryProtocol, LogicalTypeProtocol
    factory: LogicalTypeFactoryProtocol = _StubFactory()
    assert isinstance(factory, LogicalTypeFactoryProtocol)
    result = factory.create_for_python_type(str, converter=None)
    assert isinstance(result, LogicalTypeProtocol)


def test_type_converter_protocol_has_arrow_type_to_python_type():
    """TypeConverterProtocol must declare arrow_type_to_python_type."""
    from typing_extensions import get_protocol_members

    from orcapod.logical_types.protocols import TypeConverterProtocol

    # ``get_protocol_members`` rather than ``__protocol_attrs__``: the latter is a
    # CPython implementation detail that only exists on typing.Protocol from 3.12 on.
    assert "arrow_type_to_python_type" in get_protocol_members(TypeConverterProtocol)
