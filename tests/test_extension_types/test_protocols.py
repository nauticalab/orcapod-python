"""Tests for LogicalTypeProtocol and LogicalTypeFactoryProtocol."""

from __future__ import annotations

import pyarrow as pa
import polars as pl

from orcapod.extension_types.protocols import LogicalTypeProtocol
from orcapod.extension_types.registry import make_arrow_extension_type


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

    def python_to_storage(self, value):
        return str(value)

    def storage_to_python(self, storage_value):
        return storage_value


class _StubFactory:
    """Minimal conforming implementation of LogicalTypeFactoryProtocol for use in tests."""

    def reconstruct_from_arrow(self, arrow_extension_name, storage_type, metadata):
        return _StubLogicalType()

    def create_for_python_type(self, python_type):
        return _StubLogicalType()


def test_logical_type_factory_protocol_is_importable():
    """LogicalTypeFactoryProtocol can be imported from extension_types.protocols."""
    from orcapod.extension_types.protocols import LogicalTypeFactoryProtocol
    assert LogicalTypeFactoryProtocol is not None


def test_logical_type_factory_conforming_class_satisfies_protocol():
    """A conforming class is recognized as a LogicalTypeFactoryProtocol instance."""
    from orcapod.extension_types.protocols import LogicalTypeFactoryProtocol
    assert isinstance(_StubFactory(), LogicalTypeFactoryProtocol)


def test_logical_type_factory_create_returns_logical_type():
    """A conforming factory returns a LogicalTypeProtocol from reconstruct_from_arrow."""
    from orcapod.extension_types.protocols import LogicalTypeFactoryProtocol, LogicalTypeProtocol
    factory: LogicalTypeFactoryProtocol = _StubFactory()
    result = factory.reconstruct_from_arrow(
        "test.ext", pa.large_utf8(), {"category": "Test"}
    )
    assert isinstance(result, LogicalTypeProtocol)


def test_protocol_is_importable():
    """LogicalTypeProtocol can be imported from extension_types.protocols."""
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
    assert lt.python_to_storage(42) == "42"
    assert lt.storage_to_python("hello") == "hello"


def test_factory_create_for_python_type_conformance():
    """A conforming factory implements create_for_python_type and returns LogicalTypeProtocol."""
    from orcapod.extension_types.protocols import LogicalTypeFactoryProtocol, LogicalTypeProtocol
    factory: LogicalTypeFactoryProtocol = _StubFactory()
    assert isinstance(factory, LogicalTypeFactoryProtocol)
    result = factory.create_for_python_type(str)
    assert isinstance(result, LogicalTypeProtocol)
