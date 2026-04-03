"""
Pre-computed column converters for efficient semantic type conversion.

This module provides a way to pre-analyze schemas and create optimized
conversion functions for each column, eliminating runtime schema parsing
and type detection overhead.
"""

from __future__ import annotations

from typing import Any, TYPE_CHECKING
from collections.abc import Callable
import pyarrow as pa
from orcapod.utils.lazy_module import LazyModule


from orcapod.semantic_types.universal_converter import UniversalTypeConverter

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class SemanticSchemaConverter:
    def __init__(
        self,
        universal_converter: UniversalTypeConverter,
        python_schema: dict[str, type] | None = None,
        arrow_schema: pa.Schema | None = None,
    ):
        """
        self.universal_converter = universal_converter
        """
        self.universal_converter = universal_converter

        if python_schema is not None:
            self.python_schema = python_schema
            self.arrow_schema = self.universal_converter.python_schema_to_arrow_schema(
                python_schema
            )
        elif arrow_schema is not None:
            self.arrow_schema = arrow_schema
            self.python_schema = self.universal_converter.arrow_schema_to_python_schema(
                arrow_schema
            )
        else:
            raise ValueError(
                "Either python_schema or arrow_schema must be provided to initialize SemanticSchemaConverter."
            )

        # Pre-compute converters for each field
        self._python_to_arrow_converters: dict[str, Callable[[Any], Any]] = {}
        self._arrow_to_python_converters: dict[str, Callable[[Any], Any]] = {}

        for field_name, python_type in self.python_schema.items():
            self._python_to_arrow_converters[field_name] = (
                self.universal_converter.get_python_to_arrow_converter(python_type)
            )

        for field in self.arrow_schema:
            self._arrow_to_python_converters[field.name] = (
                self.universal_converter.get_arrow_to_python_converter(field.type)
            )

    def python_dict_to_struct_dict(self, record: dict[str, Any]) -> dict[str, Any]:
        """
        Convert a single Python dictionary to an Arrow-compatible struct dictionary.
        """
        if not record:
            raise ValueError("Cannot convert empty record")

        return self.python_dicts_to_struct_dicts([record])[0]

    def python_dicts_to_struct_dicts(
        self, data: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Convert a list of Python dictionaries to a list of Arrow-compatible struct dictionaries.
        """
        if not data:
            raise ValueError("Cannot convert empty data list")

        converted_data = []
        for record in data:
            converted_record = {}
            for field_name, converter in self._python_to_arrow_converters.items():
                # TODO: test the case of None/missing value
                value = record.get(field_name)
                if value is None:
                    converted_record[field_name] = None
                else:
                    # Convert using the pre-computed converter
                    converted_record[field_name] = converter(value)
            converted_data.append(converted_record)

        return converted_data

    def struct_dicts_to_python_dicts(
        self, struct_dicts: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """
        Convert a list of Arrow-compatible struct dictionaries back to Python dictionaries.
        """
        if not struct_dicts:
            raise ValueError("Cannot convert empty struct dicts list")

        # TODO: benchmark which approach of conversion would be faster
        # 1) turn pylist to pydict and then convert each column at once
        # 2) convert each row separately
        converted_data = []
        for struct_dict in struct_dicts:
            converted_record = {}
            for field_name, converter in self._arrow_to_python_converters.items():
                value = struct_dict.get(field_name)
                if value is None:
                    converted_record[field_name] = None
                else:
                    # Convert using the pre-computed converter
                    converted_record[field_name] = converter(value)
            converted_data.append(converted_record)

        return converted_data

    def struct_dict_to_python_dict(self, struct_dict: dict[str, Any]) -> dict[str, Any]:
        """
        Convert a single Arrow-compatible struct dictionary back to a Python dictionary.
        """
        if not struct_dict:
            raise ValueError("Cannot convert empty struct dict")

        return self.struct_dicts_to_python_dicts([struct_dict])[0]

    def python_dicts_to_arrow_table(
        self,
        data: list[dict[str, Any]],
        arrow_schema: pa.Schema | None = None,
    ) -> "pa.Table":
        """
        Convert a list of Python dictionaries to an Arrow table using pre-computed converters.

        Args:
            data: List of Python dictionaries to convert.
            arrow_schema: Optional Arrow schema override. When provided it is
                used instead of the pre-computed ``self.arrow_schema``.  Useful
                when the caller needs to enforce specific field types or nullable
                flags that differ from what was inferred at construction time.
        """
        struct_dicts = self.python_dicts_to_struct_dicts(data)
        return pa.Table.from_pylist(struct_dicts, schema=arrow_schema or self.arrow_schema)

    def arrow_table_to_python_dicts(self, table: pa.Table) -> list[dict[str, Any]]:
        """
        Convert an Arrow table to a list of Python dictionaries using pre-computed converters.
        """
        if not table:
            raise ValueError("Cannot convert empty table")

        struct_dicts = table.to_pylist()
        return self.struct_dicts_to_python_dicts(struct_dicts)
