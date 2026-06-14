"""Tests for LogicalType protocol."""

from __future__ import annotations

import pyarrow as pa
import polars as pl

from orcapod.extension_types.protocols import LogicalType
from orcapod.extension_types.registry import make_arrow_extension_type


class _StubLogicalType:
    """Minimal conforming implementation of LogicalType for use in tests."""

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


def test_protocol_is_importable():
    """LogicalType can be imported from extension_types.protocols."""
    assert LogicalType is not None


def test_protocol_defines_required_members():
    """A conforming class is recognized as a LogicalType instance."""
    assert isinstance(_StubLogicalType(), LogicalType)


def test_conforming_class_satisfies_protocol():
    """A class implementing all required members works correctly via the protocol interface."""
    lt: LogicalType = _StubLogicalType()
    assert lt.logical_type_name == "test.module.MyType"
    assert lt.python_type is str
    assert lt.get_arrow_extension_type().extension_name == "test.module.MyType"
    assert lt.python_to_storage(42) == "42"
    assert lt.storage_to_python("hello") == "hello"
