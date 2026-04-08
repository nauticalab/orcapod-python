import logging
from collections.abc import Collection, Mapping, Sequence
from typing import TYPE_CHECKING, Any

from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.core.operators.base import UnaryOperator
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import PacketProtocol, StreamProtocol, TagProtocol
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

    def to_config(self) -> dict[str, Any]:
        """Serialize this SelectTagColumns operator to a config dict.

        Returns:
            A dict with ``class_name``, ``module_path``, and ``config`` keys,
            where ``config`` contains ``columns`` and ``strict``.
        """
        config = super().to_config()
        config["config"] = {
            "columns": list(self.columns),
            "strict": self.strict,
        }
        return config

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
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

        return ArrowTableStream(
            modified_table,
            tag_columns=new_tag_columns,
        )

    def validate_unary_input(self, stream: StreamProtocol) -> None:
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
        stream: StreamProtocol,
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

        return Schema(new_tag_schema), packet_schema

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
        **kwargs: Any,
    ) -> None:
        """Streaming: select tag columns per row without materializing."""
        try:
            tags_to_drop: list[str] | None = None
            async for tag, packet in inputs[0]:
                if tags_to_drop is None:
                    tag_keys = tag.keys()
                    if self.strict:
                        missing = set(self.columns) - set(tag_keys)
                        if missing:
                            raise InputValidationError(
                                f"Missing tag columns: {missing}. Make sure all "
                                f"specified columns to select are present or use "
                                f"strict=False to ignore missing columns"
                            )
                    tags_to_drop = [c for c in tag_keys if c not in self.columns]
                if not tags_to_drop:
                    await output.send((tag, packet))
                else:
                    await output.send((tag.drop(*tags_to_drop), packet))
        finally:
            await output.close()

    def identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            self.columns,
            self.strict,
        )


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

    def to_config(self) -> dict[str, Any]:
        """Serialize this SelectPacketColumns operator to a config dict.

        Returns:
            A dict with ``class_name``, ``module_path``, and ``config`` keys,
            where ``config`` contains ``columns`` and ``strict``.
        """
        config = super().to_config()
        config["config"] = {
            "columns": list(self.columns),
            "strict": self.strict,
        }
        return config

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
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

        return ArrowTableStream(
            modified_table,
            tag_columns=tag_columns,
        )

    def validate_unary_input(self, stream: StreamProtocol) -> None:
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
        stream: StreamProtocol,
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

        return tag_schema, Schema(new_packet_schema)

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
        **kwargs: Any,
    ) -> None:
        """Streaming: select packet columns per row without materializing."""
        try:
            pkts_to_drop: list[str] | None = None
            async for tag, packet in inputs[0]:
                if pkts_to_drop is None:
                    pkt_keys = packet.keys()
                    if self.strict:
                        missing = set(self.columns) - set(pkt_keys)
                        if missing:
                            raise InputValidationError(
                                f"Missing packet columns: {missing}. Make sure all "
                                f"specified columns to select are present or use "
                                f"strict=False to ignore missing columns"
                            )
                    pkts_to_drop = [c for c in pkt_keys if c not in self.columns]
                if not pkts_to_drop:
                    await output.send((tag, packet))
                else:
                    await output.send((tag, packet.drop(*pkts_to_drop)))
        finally:
            await output.close()

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

    def to_config(self) -> dict[str, Any]:
        """Serialize this DropTagColumns operator to a config dict.

        Returns:
            A dict with ``class_name``, ``module_path``, and ``config`` keys,
            where ``config`` contains ``columns`` and ``strict``.
        """
        config = super().to_config()
        config["config"] = {
            "columns": list(self.columns),
            "strict": self.strict,
        }
        return config

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
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

        return ArrowTableStream(
            modified_table,
            tag_columns=new_tag_columns,
        )

    def validate_unary_input(self, stream: StreamProtocol) -> None:
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
        stream: StreamProtocol,
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

        return Schema(new_tag_schema), packet_schema

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
        **kwargs: Any,
    ) -> None:
        """Streaming: drop tag columns per row without materializing."""
        try:
            effective_drops: list[str] | None = None
            async for tag, packet in inputs[0]:
                if effective_drops is None:
                    tag_keys = tag.keys()
                    if self.strict:
                        missing = set(self.columns) - set(tag_keys)
                        if missing:
                            raise InputValidationError(
                                f"Missing tag columns: {missing}. Make sure all "
                                f"specified columns to drop are present or use "
                                f"strict=False to ignore missing columns"
                            )
                    effective_drops = (
                        list(self.columns)
                        if self.strict
                        else [c for c in self.columns if c in tag_keys]
                    )
                if not effective_drops:
                    await output.send((tag, packet))
                else:
                    await output.send((tag.drop(*effective_drops), packet))
        finally:
            await output.close()

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

    def to_config(self) -> dict[str, Any]:
        """Serialize this DropPacketColumns operator to a config dict.

        Returns:
            A dict with ``class_name``, ``module_path``, and ``config`` keys,
            where ``config`` contains ``columns`` and ``strict``.
        """
        config = super().to_config()
        config["config"] = {
            "columns": list(self.columns),
            "strict": self.strict,
        }
        return config

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
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

        return ArrowTableStream(
            modified_table,
            tag_columns=tag_columns,
        )

    def validate_unary_input(self, stream: StreamProtocol) -> None:
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
        stream: StreamProtocol,
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

        return tag_schema, Schema(new_packet_schema)

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
        **kwargs: Any,
    ) -> None:
        """Streaming: drop packet columns per row without materializing."""
        try:
            effective_drops: list[str] | None = None
            async for tag, packet in inputs[0]:
                if effective_drops is None:
                    pkt_keys = packet.keys()
                    if self.strict:
                        missing = set(self.columns) - set(pkt_keys)
                        if missing:
                            raise InputValidationError(
                                f"Missing packet columns: {missing}. Make sure all "
                                f"specified columns to drop are present or use "
                                f"strict=False to ignore missing columns"
                            )
                    effective_drops = (
                        list(self.columns)
                        if self.strict
                        else [c for c in self.columns if c in pkt_keys]
                    )
                if not effective_drops:
                    await output.send((tag, packet))
                else:
                    await output.send((tag, packet.drop(*effective_drops)))
        finally:
            await output.close()

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

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
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

        return ArrowTableStream(
            renamed_table,
            tag_columns=new_tag_columns,
        )

    def validate_unary_input(self, stream: StreamProtocol) -> None:
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
        stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        tag_schema, packet_schema = stream.output_schema(
            columns=columns, all_info=all_info
        )

        # Create new packet schema with renamed keys
        new_tag_schema = {self.name_map.get(k, k): v for k, v in tag_schema.items()}

        return Schema(new_tag_schema), packet_schema

    def identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            self.name_map,
            self.drop_unmapped,
        )
