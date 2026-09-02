from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol

from orcapod.types import DataType, Schema, SchemaLike

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.logical_types.protocols import LogicalTypeProtocol


class TypeConverterProtocol(Protocol):
    def python_type_to_arrow_type(self, python_type: DataType) -> "pa.DataType": ...

    def python_schema_to_arrow_schema(
        self, python_schema: SchemaLike
    ) -> "pa.Schema": ...

    def arrow_type_to_python_type(self, arrow_type: "pa.DataType") -> DataType: ...

    def arrow_schema_to_python_schema(self, arrow_schema: "pa.Schema") -> Schema: ...

    def python_dicts_to_struct_dicts(
        self,
        python_dicts: list[dict[str, Any]],
        python_schema: SchemaLike | None = None,
    ) -> list[dict[str, Any]]: ...

    def struct_dicts_to_python_dicts(
        self,
        struct_dict: list[dict[str, Any]],
        arrow_schema: "pa.Schema",
    ) -> list[dict[str, Any]]: ...

    def python_dicts_to_arrow_table(
        self,
        python_dicts: list[dict[str, Any]],
        python_schema: SchemaLike | None = None,
        arrow_schema: "pa.Schema | None" = None,
    ) -> "pa.Table": ...

    def arrow_table_to_python_dicts(
        self, arrow_table: "pa.Table"
    ) -> list[dict[str, Any]]: ...

    def get_python_to_arrow_converter(
        self, python_type: type
    ) -> "Callable[[Any], Any]": ...

    def get_arrow_to_python_converter(
        self, arrow_type: "pa.DataType"
    ) -> "Callable[[Any], Any]": ...

    def ensure_types_registered_for_schemas(self, *schemas: Schema) -> None: ...

    def get_logical_type(self, python_type: type) -> "LogicalTypeProtocol | None": ...


