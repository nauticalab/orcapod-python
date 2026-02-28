from __future__ import annotations

from collections.abc import Collection, Mapping
from typing import TYPE_CHECKING, Any

from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.core.sources.base import RootSource
from orcapod.types import ColumnConfig, DataValue, Schema, SchemaLike
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class DictSource(RootSource):
    """
    A source backed by a collection of Python dictionaries.

    Each dict becomes one (tag, packet) pair in the stream.  The dicts are
    converted to an Arrow table via the data-context type converter, then
    handled by ``ArrowTableSource`` (including source-info and schema-hash
    annotation).
    """

    def __init__(
        self,
        data: Collection[Mapping[str, DataValue]],
        tag_columns: Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        source_name: str | None = None,
        data_schema: SchemaLike | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        arrow_table = self.data_context.type_converter.python_dicts_to_arrow_table(
            [dict(row) for row in data],
            python_schema=data_schema,
        )
        self._arrow_source = ArrowTableSource(
            table=arrow_table,
            tag_columns=tag_columns,
            system_tag_columns=system_tag_columns,
            source_name=source_name,
            data_context=self.data_context,
            config=self.orcapod_config,
        )

    def identity_structure(self) -> Any:
        return self._arrow_source.identity_structure()

    def output_schema(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        return self._arrow_source.output_schema(columns=columns, all_info=all_info)

    def keys(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        return self._arrow_source.keys(columns=columns, all_info=all_info)

    def iter_packets(self):
        return self._arrow_source.iter_packets()

    def as_table(
        self,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> "pa.Table":
        return self._arrow_source.as_table(columns=columns, all_info=all_info)
