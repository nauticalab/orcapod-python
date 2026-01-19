from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from orcapod.core.operators.base import UnaryOperator
from orcapod.core.streams import TableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import ColumnConfig, Stream
from orcapod.system_constants import constants
from orcapod.types import Schema
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class MapPackets(UnaryOperator):
    """
    Operator that maps packets in a stream using a user-defined function.
    The function is applied to each packet in the stream, and the resulting packets
    are returned as a new stream.
    """

    def __init__(
        self, name_map: Mapping[str, str], drop_unmapped: bool = False, **kwargs
    ):
        self.name_map = dict(name_map)
        self.drop_unmapped = drop_unmapped
        super().__init__(**kwargs)

    def unary_static_process(self, stream: Stream) -> Stream:
        tag_columns, packet_columns = stream.keys()
        unmapped_columns = set(packet_columns) - set(self.name_map.keys())

        if not any(n in packet_columns for n in self.name_map):
            # nothing to rename in the packet, return stream as is
            return stream

        table = stream.as_table(
            columns={"source": True, "system_tags": True, "sort_by_tags": False}
        )

        name_map = {
            c: c
            for c in table.column_names
            if c not in packet_columns and not c.startswith("")
        }
        name_map = {
            tc: tc
            for tc in table.column_names
            if tc not in packet_columns and not tc.startswith(constants.SOURCE_PREFIX)
        }  # no renaming on tag columns
        for c in packet_columns:
            if c in self.name_map:
                name_map[c] = self.name_map[c]
                name_map[f"{constants.SOURCE_PREFIX}{c}"] = (
                    f"{constants.SOURCE_PREFIX}{self.name_map[c]}"
                )
            else:
                name_map[c] = c

        renamed_table = table.rename_columns(name_map)

        if self.drop_unmapped and unmapped_columns:
            renamed_table = renamed_table.drop_columns(list(unmapped_columns))

        return TableStream(
            renamed_table, tag_columns=tag_columns, source=self, upstreams=(stream,)
        )

    def validate_unary_input(self, stream: Stream) -> None:
        # verify that renamed value does NOT collide with other columns
        tag_columns, packet_columns = stream.keys()
        relevant_source = []
        relevant_target = []
        for source, target in self.name_map.items():
            if source in packet_columns:
                relevant_source.append(source)
                relevant_target.append(target)
        remaining_packet_columns = set(packet_columns) - set(relevant_source)
        overlapping_packet_columns = remaining_packet_columns.intersection(
            relevant_target
        )
        overlapping_tag_columns = set(tag_columns).intersection(relevant_target)

        if overlapping_packet_columns or overlapping_tag_columns:
            message = f"Renaming {self.name_map} would cause collisions with existing columns: "
            if overlapping_packet_columns:
                message += f"overlapping packet columns: {overlapping_packet_columns}, "
            if overlapping_tag_columns:
                message += f"overlapping tag columns: {overlapping_tag_columns}."
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
        new_packet_typespec = {
            self.name_map.get(k, k): v
            for k, v in packet_typespec.items()
            if k in self.name_map or not self.drop_unmapped
        }

        return tag_typespec, new_packet_typespec

    def identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            self.name_map,
            self.drop_unmapped,
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

        table = stream.as_table(
            columns={"source": True, "system_tags": True, "sort_by_tags": False}
        )

        name_map = {
            tc: self.name_map.get(tc, tc)
            for tc in tag_columns
            if tc in self.name_map or not self.drop_unmapped
        }  # rename the tag as necessary
        new_tag_columns = list(name_map.values())
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
        include_system_tags: bool = False,
    ) -> tuple[Schema, Schema]:
        tag_typespec, packet_typespec = stream.output_schema(
            columns=columns, all_info=all_info
        )

        # Create new packet typespec with renamed keys
        new_tag_typespec = {self.name_map.get(k, k): v for k, v in tag_typespec.items()}

        # Create new packet typespec with renamed keys
        new_tag_typespec = {
            self.name_map.get(k, k): v
            for k, v in tag_typespec.items()
            if k in self.name_map or not self.drop_unmapped
        }

        return new_tag_typespec, packet_typespec

        return new_tag_typespec, packet_typespec

    def identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            self.name_map,
            self.drop_unmapped,
        )
