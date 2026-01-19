from collections.abc import Collection
from typing import TYPE_CHECKING, Any

from orcapod.core.streams import TableStream
from orcapod.protocols import core_protocols as cp
from orcapod.types import Schema
from orcapod.utils.lazy_module import LazyModule
from orcapod.contexts.system_constants import constants
from orcapod.core import polars_data_utils
from orcapod.core.sources.source_registry import GLOBAL_SOURCE_REGISTRY, SourceRegistry
import logging
from orcapod.core.sources.base import SourceBase

if TYPE_CHECKING:
    import pyarrow as pa
    import polars as pl
    from polars._typing import FrameInitTypes
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")


logger = logging.getLogger(__name__)


class DataFrameSource(SourceBase):
    """Construct source from a dataframe and any Polars dataframe compatible data structure"""

    SOURCE_ID = "polars"

    def __init__(
        self,
        data: "FrameInitTypes",
        tag_columns: str | Collection[str] = (),
        source_name: str | None = None,
        source_registry: SourceRegistry | None = None,
        auto_register: bool = True,
        preserve_system_columns: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)

        # clean the table, dropping any system columns
        # Initialize polars dataframe
        # TODO: work with LazyFrame
        df = pl.DataFrame(data)

        object_columns = [c for c in df.columns if df[c].dtype == pl.Object]
        if len(object_columns) > 0:
            logger.info(
                f"Converting {len(object_columns)}object columns to Arrow format"
            )
            sub_table = self.data_context.type_converter.python_dicts_to_arrow_table(
                df.select(object_columns).to_dicts()
            )
            df = df.with_columns([pl.from_arrow(c) for c in sub_table])

        if isinstance(tag_columns, str):
            tag_columns = [tag_columns]

        if not preserve_system_columns:
            df = polars_data_utils.drop_system_columns(df)

        non_system_columns = polars_data_utils.drop_system_columns(df)
        missing_columns = set(tag_columns) - set(non_system_columns.columns)
        if missing_columns:
            raise ValueError(
                f"Following tag columns not found in data: {missing_columns}"
            )
        tag_schema = non_system_columns.select(tag_columns).to_arrow().schema
        packet_schema = non_system_columns.drop(list(tag_columns)).to_arrow().schema
        self.tag_columns = tag_columns

        tag_python_schema = (
            self.data_context.type_converter.arrow_schema_to_python_schema(tag_schema)
        )
        packet_python_schema = (
            self.data_context.type_converter.arrow_schema_to_python_schema(
                packet_schema
            )
        )
        schema_hash = self.data_context.object_hasher.hash_object(
            (tag_python_schema, packet_python_schema)
        ).to_hex(char_count=self.orcapod_config.schema_hash_n_char)

        self.table_hash = self.data_context.arrow_hasher.hash_table(df.to_arrow())

        if source_name is None:
            # TODO: determine appropriate config name
            source_name = self.content_hash().to_hex(
                char_count=self.orcapod_config.path_hash_n_char
            )

        self._source_name = source_name

        row_index = list(range(df.height))

        source_info = [
            f"{self.source_id}{constants.BLOCK_SEPARATOR}row_{i}" for i in row_index
        ]

        # add source info
        df = polars_data_utils.add_source_info(
            df, source_info, exclude_columns=tag_columns
        )

        df = polars_data_utils.add_system_tag_column(
            df, f"source{constants.FIELD_SEPARATOR}{schema_hash}", source_info
        )

        self._df = df

        self._table_stream = TableStream(
            table=self._df.to_arrow(),
            tag_columns=self.tag_columns,
            source=self,
            upstreams=(),
        )

        # Auto-register with global registry
        if auto_register:
            registry = source_registry or GLOBAL_SOURCE_REGISTRY
            registry.register(self.source_id, self)

    @property
    def reference(self) -> tuple[str, ...]:
        return ("data_frame", f"source_{self._source_name}")

    @property
    def df(self) -> "pl.DataFrame":
        return self._df

    def source_identity_structure(self) -> Any:
        return (self.__class__.__name__, self.tag_columns, self.table_hash)

    def get_all_records(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        return self().as_table(include_source=include_system_columns)

    def forward(self, *streams: cp.Stream) -> cp.Stream:
        """
        Load data from file and return a static stream.

        This is called by forward() and creates a fresh snapshot each time.
        """
        return self._table_stream

    def source_output_types(
        self, include_system_tags: bool = False
    ) -> tuple[Schema, Schema]:
        """Return tag and packet types based on provided typespecs."""
        return self._table_stream.types(include_system_tags=include_system_tags)
