from collections.abc import Collection, Mapping
from typing import TYPE_CHECKING, Any


from orcapod.protocols import core_protocols as cp
from orcapod.types import DataValue, Schema, SchemaLike
from orcapod.utils.lazy_module import LazyModule
from orcapod.contexts.system_constants import constants
from orcapod.core.sources.arrow_table_source import ArrowTableSource

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

from orcapod.core.sources.base import SourceBase


def add_source_field(
    record: dict[str, DataValue], source_info: str
) -> dict[str, DataValue]:
    """Add source information to a record."""
    # for all "regular" fields, add source info
    for key in record.keys():
        if not key.startswith(constants.META_PREFIX) and not key.startswith(
            constants.DATAGRAM_PREFIX
        ):
            record[f"{constants.SOURCE_PREFIX}{key}"] = f"{source_info}:{key}"
    return record


def split_fields_with_prefixes(
    record, prefixes: Collection[str]
) -> tuple[dict[str, DataValue], dict[str, DataValue]]:
    """Split fields in a record into two dictionaries based on prefixes."""
    matching = {}
    non_matching = {}
    for key, value in record.items():
        if any(key.startswith(prefix) for prefix in prefixes):
            matching[key] = value
        else:
            non_matching[key] = value
    return matching, non_matching


def split_system_columns(
    data: list[dict[str, DataValue]],
) -> tuple[list[dict[str, DataValue]], list[dict[str, DataValue]]]:
    system_columns: list[dict[str, DataValue]] = []
    non_system_columns: list[dict[str, DataValue]] = []
    for record in data:
        sys_cols, non_sys_cols = split_fields_with_prefixes(
            record, [constants.META_PREFIX, constants.DATAGRAM_PREFIX]
        )
        system_columns.append(sys_cols)
        non_system_columns.append(non_sys_cols)
    return system_columns, non_system_columns


class DictSource(SourceBase):
    """Construct source from a collection of dictionaries"""

    def __init__(
        self,
        data: Collection[Mapping[str, DataValue]],
        tag_columns: Collection[str] = (),
        system_tag_columns: Collection[str] = (),
        source_name: str | None = None,
        data_schema: SchemaLike | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        arrow_table = self.data_context.type_converter.python_dicts_to_arrow_table(
            [dict(e) for e in data], python_schema=data_schema
        )
        self._table_source = ArrowTableSource(
            arrow_table,
            tag_columns=tag_columns,
            source_name=source_name,
            system_tag_columns=system_tag_columns,
        )

    @property
    def reference(self) -> tuple[str, ...]:
        # TODO: provide more thorough implementation
        return ("dict",) + self._table_source.reference[1:]

    def source_identity_structure(self) -> Any:
        return self._table_source.source_identity_structure()

    def get_all_records(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        return self._table_source.get_all_records(
            include_system_columns=include_system_columns
        )

    def forward(self, *streams: cp.Stream) -> cp.Stream:
        """
        Load data from file and return a static stream.

        This is called by forward() and creates a fresh snapshot each time.
        """
        return self._table_source.forward(*streams)

    def source_output_types(
        self, include_system_tags: bool = False
    ) -> tuple[Schema, Schema]:
        """Return tag and packet types based on provided typespecs."""
        # TODO: add system tag
        return self._table_source.source_output_types(
            include_system_tags=include_system_tags
        )
