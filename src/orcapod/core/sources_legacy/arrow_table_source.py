from collections.abc import Collection
from typing import TYPE_CHECKING, Any


from orcapod.core.streams import TableStream
from orcapod.protocols import core_protocols as cp
from orcapod.types import Schema
from orcapod.utils.lazy_module import LazyModule
from orcapod.contexts.system_constants import constants
from orcapod.core import arrow_data_utils
from orcapod.core.sources.source_registry import GLOBAL_SOURCE_REGISTRY, SourceRegistry

from orcapod.core.sources.base import SourceBase

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class ArrowTableSource(SourceBase):
    """Construct source from a collection of dictionaries"""

    SOURCE_ID = "arrow"

    def __init__(
        self,
        arrow_table: "pa.Table",
        tag_columns: Collection[str] = (),
        source_name: str | None = None,
        source_registry: SourceRegistry | None = None,
        auto_register: bool = True,
        preserve_system_columns: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)

        # clean the table, dropping any system columns
        # TODO: consider special treatment of system columns if provided
        if not preserve_system_columns:
            arrow_table = arrow_data_utils.drop_system_columns(arrow_table)

        non_system_columns = arrow_data_utils.drop_system_columns(arrow_table)
        tag_schema = non_system_columns.select(tag_columns).schema
        # FIXME: ensure tag_columns are found among non system columns
        packet_schema = non_system_columns.drop(list(tag_columns)).schema

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

        self.tag_columns = [
            col for col in tag_columns if col in arrow_table.column_names
        ]

        self.table_hash = self.data_context.arrow_hasher.hash_table(arrow_table)

        if source_name is None:
            # TODO: determine appropriate config name
            source_name = self.content_hash().to_hex(
                char_count=self.orcapod_config.path_hash_n_char
            )

        self._source_name = source_name

        row_index = list(range(arrow_table.num_rows))

        source_info = [
            f"{self.source_id}{constants.BLOCK_SEPARATOR}row_{i}" for i in row_index
        ]

        # add source info
        arrow_table = arrow_data_utils.add_source_info(
            arrow_table, source_info, exclude_columns=tag_columns
        )

        arrow_table = arrow_data_utils.add_system_tag_column(
            arrow_table, f"source{constants.FIELD_SEPARATOR}{schema_hash}", source_info
        )

        self._table = arrow_table

        self._table_stream = TableStream(
            table=self._table,
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
        return ("arrow_table", f"source_{self._source_name}")

    @property
    def table(self) -> "pa.Table":
        return self._table

    def source_identity_structure(self) -> Any:
        return (self.__class__.__name__, self.tag_columns, self.table_hash)

    def get_all_records(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        return self().as_table(include_source=include_system_columns)

    def forward(self, *streams: cp.StreamProtocol) -> cp.StreamProtocol:
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
