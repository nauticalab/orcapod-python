"""Specification-derived tests for semantic type conversion.

Tests the UniversalTypeConverter and SemanticTypeRegistry based on
documented behavior in protocols and design specification.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.contexts import get_default_type_converter
from orcapod.types import Schema


# ---------------------------------------------------------------------------
# UniversalTypeConverter — Python ↔ Arrow type conversion
# ---------------------------------------------------------------------------


class TestPythonToArrowType:
    """Per the TypeConverterProtocol, python_type_to_arrow_type converts
    Python type hints to Arrow types."""

    @pytest.fixture
    def converter(self):
        return get_default_type_converter()

    def test_int_to_int64(self, converter):
        result = converter.python_type_to_arrow_type(int)
        assert result == pa.int64()

    def test_float_to_float64(self, converter):
        result = converter.python_type_to_arrow_type(float)
        assert result == pa.float64()

    def test_str_to_large_string(self, converter):
        result = converter.python_type_to_arrow_type(str)
        assert result == pa.large_string()

    def test_bool_to_bool(self, converter):
        result = converter.python_type_to_arrow_type(bool)
        assert result == pa.bool_()

    def test_bytes_to_binary(self, converter):
        result = converter.python_type_to_arrow_type(bytes)
        # Could be large_binary or binary
        assert pa.types.is_binary(result) or pa.types.is_large_binary(result)

    def test_list_of_int(self, converter):
        result = converter.python_type_to_arrow_type(list[int])
        assert pa.types.is_list(result) or pa.types.is_large_list(result)


class TestArrowToPythonType:
    """Per the TypeConverterProtocol, arrow_type_to_python_type converts
    Arrow types back to Python type hints."""

    @pytest.fixture
    def converter(self):
        return get_default_type_converter()

    def test_int64_to_int(self, converter):
        result = converter.arrow_type_to_python_type(pa.int64())
        assert result is int

    def test_float64_to_float(self, converter):
        result = converter.arrow_type_to_python_type(pa.float64())
        assert result is float

    def test_bool_to_bool(self, converter):
        result = converter.arrow_type_to_python_type(pa.bool_())
        assert result is bool


class TestSchemaConversionRoundtrip:
    """Python Schema → Arrow Schema → Python Schema should preserve types."""

    @pytest.fixture
    def converter(self):
        return get_default_type_converter()

    def test_simple_schema_roundtrip(self, converter):
        python_schema = Schema({"x": int, "y": float, "name": str})
        arrow_schema = converter.python_schema_to_arrow_schema(python_schema)
        roundtripped = converter.arrow_schema_to_python_schema(arrow_schema)
        assert set(roundtripped.keys()) == set(python_schema.keys())
        for key in python_schema:
            assert roundtripped[key] == python_schema[key]


class TestPythonDictsToArrowTable:
    """Per protocol, python_dicts_to_arrow_table converts list of dicts to pa.Table."""

    @pytest.fixture
    def converter(self):
        return get_default_type_converter()

    def test_simple_conversion(self, converter):
        data = [{"x": 1, "y": 2.0}, {"x": 3, "y": 4.0}]
        schema = Schema({"x": int, "y": float})
        result = converter.python_dicts_to_arrow_table(data, python_schema=schema)
        assert isinstance(result, pa.Table)
        assert result.num_rows == 2
        assert "x" in result.column_names
        assert "y" in result.column_names


class TestArrowTableToPythonDicts:
    """Per protocol, arrow_table_to_python_dicts converts pa.Table to list of dicts."""

    @pytest.fixture
    def converter(self):
        return get_default_type_converter()

    def test_simple_conversion(self, converter):
        table = pa.table({"x": [1, 2], "y": [3.0, 4.0]})
        result = converter.arrow_table_to_python_dicts(table)
        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["x"] == 1
        assert result[1]["y"] == 4.0
