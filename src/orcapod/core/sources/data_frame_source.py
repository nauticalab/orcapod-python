from __future__ import annotations

from collections.abc import Collection
from typing import TYPE_CHECKING, Any
import logging

from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.core.sources.base import RootSource
from orcapod.core.streams.table_stream import TableStream
from orcapod.protocols.core_protocols import Stream
from orcapod.types import ColumnConfig, Schema
from orcapod.utils import polars_data_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    from polars._typing import FrameInitTypes
else:
    pl = LazyModule("polars")

logger = logging.getLogger(__name__)


class DataFrameSource(RootSource):
    """
    A source backed by a Polars DataFrame (or any Polars-compatible data).

    The DataFrame is converted to an Arrow table and then handled identically
    to ``ArrowTableSource``, including source-info provenance annotation and
    schema-hash system tags.  Because the data is immutable after construction
    the same ``TableStream`` is returned from every ``process()`` call.
    """

    def __init__(
        self,
        data: "FrameInitTypes",
        tag_columns: str | Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        source_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)

        df = pl.DataFrame(data)

        # Convert any Object-dtype columns to Arrow-compatible types.
        object_columns = [c for c in df.columns if df[c].dtype == pl.Object]
        if object_columns:
            logger.info(
                f"Converting {len(object_columns)} object column(s) to Arrow format"
            )
            sub_table = self.data_context.type_converter.python_dicts_to_arrow_table(
                df.select(object_columns).to_dicts()
            )
            df = df.with_columns([pl.from_arrow(c) for c in sub_table])

        if isinstance(tag_columns, str):
            tag_columns = [tag_columns]
        tag_columns = list(tag_columns)

        df = polars_data_utils.drop_system_columns(df)

        missing = set(tag_columns) - set(df.columns)
        if missing:
            raise ValueError(f"Tag column(s) not found in data: {missing}")

        # Delegate all enrichment logic to ArrowTableSource.
        self._arrow_source = ArrowTableSource(
            table=df.to_arrow(),
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
        *streams: Stream,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        return self._arrow_source.output_schema(columns=columns, all_info=all_info)

    def process(self, *streams: Stream, label: str | None = None) -> TableStream:
        self.validate_inputs(*streams)
        return self._arrow_source.process()
