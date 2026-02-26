import logging
from collections.abc import Collection, Mapping
from typing import TYPE_CHECKING, Any

from orcapod.core.operators.base import UnaryOperator
from orcapod.core.streams import TableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import Stream
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, Schema
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)


class SelectTagColumns(UnaryOperator):
    """
    Operator that selects specified columns from a stream.
    """

    def __init__(self, columns: str | Collection[str], strict: bool = True, **kwargs):
        if isinstance(columns, str):
            columns = [columns]
        self.columns = columns
        self.strict = strict
        super().__init__(**kwargs)

    def unary_static_process(self, stream: Stream) -> Stream:
        tag_columns, packet_columns = stream.keys()
        tags_to_drop = [c for c in tag_columns if c not in self.columns]
        new_tag_columns = [c for c in tag_columns if c not in tags_to_drop]

        if len(new_tag_columns) == len(tag_columns):
            logger.info("All tag columns are selected. Returning stream unaltered.")
            return stream

        table = stream.as_table(
            columns={"source": True, "system_tags": True, "sort_by_tags": False}
        )

        modified_table = table.drop_columns(list(tags_to_drop))

        return TableStream(
            modified_table,
            tag_columns=new_tag_columns,
            source=self,
            upstreams=(stream,),
        )

    def validate_unary_input(self, stream: Stream) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """
        # TODO: remove redundant logic
        tag_columns, packet_columns = stream.keys()
        columns_to_select = self.columns
        missing_columns = set(columns_to_select) - set(tag_columns)
        if missing_columns and self.strict:
            raise InputValidationError(
                f"Missing tag columns: {missing_columns}. Make sure all specified columns to select are present or use strict=False to ignore missing columns"
            )

    def unary_output_schema(
        self,
        stream: Stream,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        tag_schema, packet_schema = stream.output_schema(
            columns=columns, all_info=all_info
        )
        tag_columns, _ = stream.keys()
        tags_to_drop = [tc for tc in tag_columns if tc not in self.columns]

        # this ensures all system tag columns are preserved
        new_tag_schema = {k: v for k, v in tag_schema.items() if k not in tags_to_drop}

        return new_tag_schema, packet_schema

    def op_identity_structure(self, stream: Stream | None = None) -> Any:
        return (
            self.__class__.__name__,
            self.columns,
            self.strict,
        ) + ((stream,) if stream is not None else ())


class SelectPacketColumns(UnaryOperator):
    """
    Operator that selects specified columns from a stream.
    """

    def __init__(self, columns: str | Collection[str], strict: bool = True, **kwargs):
        if isinstance(columns, str):
            columns = [columns]
        self.columns = columns
        self.strict = strict
        super().__init__(**kwargs)

    def unary_static_process(self, stream: Stream) -> Stream:
        tag_columns, packet_columns = stream.keys()
        packet_columns_to_drop = [c for c in packet_columns if c not in self.columns]
        new_packet_columns = [
            c for c in packet_columns if c not in packet_columns_to_drop
        ]

        if len(new_packet_columns) == len(packet_columns):
            logger.info("All packet columns are selected. Returning stream unaltered.")
            return stream

        table = stream.as_table(
            columns={"source": True, "system_tags": True, "sort_by_tags": False},
        )
        # make sure to drop associated source fields
        associated_source_fields = [
            f"{constants.SOURCE_PREFIX}{c}" for c in packet_columns_to_drop
        ]
        packet_columns_to_drop.extend(associated_source_fields)

        modified_table = table.drop_columns(packet_columns_to_drop)

        return TableStream(
            modified_table,
            tag_columns=tag_columns,
            source=self,
            upstreams=(stream,),
        )

    def validate_unary_input(self, stream: Stream) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """
        # TODO: remove redundant logic
        tag_columns, packet_columns = stream.keys()
        columns_to_select = self.columns
        missing_columns = set(columns_to_select) - set(packet_columns)
        if missing_columns and self.strict:
            raise InputValidationError(
                f"Missing packet columns: {missing_columns}. Make sure all specified columns to select are present or use strict=False to ignore missing columns"
            )

    def unary_output_schema(
        self,
        stream: Stream,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        tag_schema, packet_schema = stream.output_schema(
            columns=columns, all_info=all_info
        )
        _, packet_columns = stream.keys()
        packets_to_drop = [pc for pc in packet_columns if pc not in self.columns]

        # this ensures all system tag columns are preserved
        new_packet_schema = {
            k: v for k, v in packet_schema.items() if k not in packets_to_drop
        }

        return tag_schema, new_packet_schema

    def identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            self.columns,
            self.strict,
        )


class DropTagColumns(UnaryOperator):
    """
    Operator that drops specified columns from a stream.
    """

    def __init__(self, columns: str | Collection[str], strict: bool = True, **kwargs):
        if isinstance(columns, str):
            columns = [columns]
        self.columns = columns
        self.strict = strict
        super().__init__(**kwargs)

    def unary_static_process(self, stream: Stream) -> Stream:
        tag_columns, packet_columns = stream.keys()
        columns_to_drop = self.columns
        if not self.strict:
            columns_to_drop = [c for c in columns_to_drop if c in tag_columns]

        new_tag_columns = [c for c in tag_columns if c not in columns_to_drop]

        if len(columns_to_drop) == 0:
            logger.info("No tag columns to drop. Returning stream unaltered.")
            return stream

        table = stream.as_table(
            columns={"source": True, "system_tags": True, "sort_by_tags": False}
        )

        modified_table = table.drop_columns(list(columns_to_drop))

        return TableStream(
            modified_table,
            tag_columns=new_tag_columns,
            source=self,
            upstreams=(stream,),
        )

    def validate_unary_input(self, stream: Stream) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """
        # TODO: remove redundant logic
        tag_columns, packet_columns = stream.keys()
        columns_to_drop = self.columns
        missing_columns = set(columns_to_drop) - set(tag_columns)
        if missing_columns and self.strict:
            raise InputValidationError(
                f"Missing tag columns: {missing_columns}. Make sure all specified columns to drop are present or use strict=False to ignore missing columns"
            )

    def unary_output_schema(
        self,
        stream: Stream,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        tag_schema, packet_schema = stream.output_schema(
            columns=columns, all_info=all_info
        )
        tag_columns, _ = stream.keys()
        new_tag_columns = [c for c in tag_columns if c not in self.columns]

        new_tag_schema = {k: v for k, v in tag_schema.items() if k in new_tag_columns}

        return new_tag_schema, packet_schema

    def identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            self.columns,
            self.strict,
        )


class DropPacketColumns(UnaryOperator):
    """
    Operator that drops specified columns from a stream.
    """

    def __init__(self, columns: str | Collection[str], strict: bool = True, **kwargs):
        if isinstance(columns, str):
            columns = [columns]
        self.columns = columns
        self.strict = strict
        super().__init__(**kwargs)

    def unary_static_process(self, stream: Stream) -> Stream:
        tag_columns, packet_columns = stream.keys()
        columns_to_drop = list(self.columns)
        if not self.strict:
            columns_to_drop = [c for c in columns_to_drop if c in packet_columns]

        if len(columns_to_drop) == 0:
            logger.info("No packet columns to drop. Returning stream unaltered.")
            return stream

        # make sure all associated source columns are dropped too
        associated_source_columns = [
            f"{constants.SOURCE_PREFIX}{c}" for c in columns_to_drop
        ]
        columns_to_drop.extend(associated_source_columns)

        table = stream.as_table(
            columns={"source": True, "system_tags": True, "sort_by_tags": False}
        )

        modified_table = table.drop_columns(columns_to_drop)

        return TableStream(
            modified_table,
            tag_columns=tag_columns,
            source=self,
            upstreams=(stream,),
        )

    def validate_unary_input(self, stream: Stream) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """
        # TODO: remove redundant logic
        _, packet_columns = stream.keys()
        missing_columns = set(self.columns) - set(packet_columns)
        if missing_columns and self.strict:
            raise InputValidationError(
                f"Missing packet columns: {missing_columns}. Make sure all specified columns to drop are present or use strict=False to ignore missing columns"
            )

    def unary_output_schema(
        self,
        stream: Stream,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        tag_schema, packet_schema = stream.output_schema(
            columns=columns, all_info=all_info
        )

        new_packet_schema = {
            k: v for k, v in packet_schema.items() if k not in self.columns
        }

        return tag_schema, new_packet_schema

    def identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            self.columns,
            self.strict,
        )


class MapTags(UnaryOperator):
    """
    Operator that maps tags in a stream using a user-defined function.
    The function is applied to each tag in the stream, and the resulting tags
    are returned as a new stream.
    """

    def __init__(
        self, name_map: Mapping[str, str], drop_unmapped: bool = False, **kwargs
    ):
        self.name_map = dict(name_map)
        self.drop_unmapped = drop_unmapped
        super().__init__(**kwargs)

    def unary_execute(self, stream: Stream) -> Stream:
        tag_columns, packet_columns = stream.keys()
        missing_tags = set(tag_columns) - set(self.name_map.keys())

        if not any(n in tag_columns for n in self.name_map):
            # nothing to rename in the tags, return stream as is
            return stream

        table = stream.as_table(columns={"source": True, "system_tags": True})

        name_map = {
            tc: self.name_map.get(tc, tc) for tc in tag_columns
        }  # rename the tag as necessary
        new_tag_columns = [name_map[tc] for tc in tag_columns]
        for c in packet_columns:
            name_map[c] = c  # no renaming on packet columns

        renamed_table = table.rename_columns(name_map)

        if missing_tags and self.drop_unmapped:
            # drop any tags that are not in the name map
            renamed_table = renamed_table.drop_columns(list(missing_tags))

        return TableStream(
            renamed_table, tag_columns=new_tag_columns, source=self, upstreams=(stream,)
        )

    def validate_unary_input(self, stream: Stream) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """
        # verify that renamed value does NOT collide with other columns
        tag_columns, packet_columns = stream.keys()
        relevant_source = []
        relevant_target = []
        for source, target in self.name_map.items():
            if source in tag_columns:
                relevant_source.append(source)
                relevant_target.append(target)
        remaining_tag_columns = set(tag_columns) - set(relevant_source)
        overlapping_tag_columns = remaining_tag_columns.intersection(relevant_target)
        overlapping_packet_columns = set(packet_columns).intersection(relevant_target)

        if overlapping_tag_columns or overlapping_packet_columns:
            message = f"Renaming {self.name_map} would cause collisions with existing columns: "
            if overlapping_tag_columns:
                message += f"overlapping tag columns: {overlapping_tag_columns}."
            if overlapping_packet_columns:
                message += f"overlapping packet columns: {overlapping_packet_columns}."
            raise InputValidationError(message)

    def unary_output_schema(
        self,
        stream: Stream,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        tag_typespec, packet_typespec = stream.output_schema(
            columns=columns, all_info=all_info
        )

        # Create new packet typespec with renamed keys
        new_tag_typespec = {self.name_map.get(k, k): v for k, v in tag_typespec.items()}

        return new_tag_typespec, packet_typespec

    def identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            self.name_map,
            self.drop_unmapped,
        )
