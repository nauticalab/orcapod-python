from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING, Any

from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.core.operators.base import UnaryOperator
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, KeyProtocol
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, Schema
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class MapData(UnaryOperator):
    """
    Operator that maps data in a stream using a user-defined function.
    The function is applied to each data in the stream, and the resulting data
    are returned as a new stream.
    """

    def __init__(
        self, name_map: Mapping[str, str], drop_unmapped: bool = False, **kwargs
    ):
        self.name_map = dict(name_map)
        self.drop_unmapped = drop_unmapped
        super().__init__(**kwargs)

    def to_config(self) -> dict[str, Any]:
        """Serialize this MapData operator to a config dict.

        Returns:
            A dict with ``class_name``, ``module_path``, and ``config`` keys,
            where ``config`` contains ``name_map`` and ``drop_unmapped``.
        """
        config = super().to_config()
        config["config"] = {
            "name_map": dict(self.name_map),
            "drop_unmapped": self.drop_unmapped,
        }
        return config

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        key_columns, data_columns = stream.keys()
        unmapped_columns = set(data_columns) - set(self.name_map.keys())

        if not any(n in data_columns for n in self.name_map):
            # nothing to rename in the data, return stream as is
            return stream

        table = stream.as_table(
            columns={"source": True, "system_keys": True, "sort_by_keys": False}
        )

        name_map = {
            c: c
            for c in table.column_names
            if c not in data_columns and not c.startswith("")
        }
        name_map = {
            tc: tc
            for tc in table.column_names
            if tc not in data_columns and not tc.startswith(constants.SOURCE_PREFIX)
        }  # no renaming on key columns
        for c in data_columns:
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

        return ArrowTableStream(renamed_table, key_columns=key_columns)

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        # verify that renamed value does NOT collide with other columns
        key_columns, data_columns = stream.keys()
        relevant_source = []
        relevant_target = []
        for source, target in self.name_map.items():
            if source in data_columns:
                relevant_source.append(source)
                relevant_target.append(target)
        remaining_data_columns = set(data_columns) - set(relevant_source)
        overlapping_data_columns = remaining_data_columns.intersection(
            relevant_target
        )
        overlapping_key_columns = set(key_columns).intersection(relevant_target)

        if overlapping_data_columns or overlapping_key_columns:
            message = f"Renaming {self.name_map} would cause collisions with existing columns: "
            if overlapping_data_columns:
                message += f"overlapping data columns: {overlapping_data_columns}, "
            if overlapping_key_columns:
                message += f"overlapping key columns: {overlapping_key_columns}."
            raise InputValidationError(message)

    def unary_output_schema(
        self,
        stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        key_schema, data_schema = stream.output_schema(
            columns=columns, all_info=all_info
        )

        # Create new data schema with renamed keys
        new_data_schema = {
            self.name_map.get(k, k): v
            for k, v in data_schema.items()
            if k in self.name_map or not self.drop_unmapped
        }

        return key_schema, Schema(new_data_schema)

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[KeyProtocol, DataProtocol]]],
        output: WritableChannel[tuple[KeyProtocol, DataProtocol]],
        **kwargs: Any,
    ) -> None:
        """Streaming: rename data columns per row without materializing."""
        try:
            rename_map: dict[str, str] | None = None
            unmapped: list[str] | None = None
            async for key, data in inputs[0]:
                if rename_map is None:
                    pkt_keys = data.keys()
                    rename_map = {
                        k: self.name_map[k] for k in pkt_keys if k in self.name_map
                    }
                    if self.drop_unmapped:
                        unmapped = [k for k in pkt_keys if k not in self.name_map]
                if not rename_map:
                    await output.send((key, data))
                else:
                    new_pkt = data.rename(rename_map)
                    if unmapped:
                        new_pkt = new_pkt.drop(*unmapped)
                    await output.send((key, new_pkt))
        finally:
            await output.close()

    def identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            self.name_map,
            self.drop_unmapped,
        )


class MapKeys(UnaryOperator):
    """
    Operator that maps keys in a stream using a user-defined function.
    The function is applied to each key in the stream, and the resulting keys
    are returned as a new stream.
    """

    def __init__(
        self, name_map: Mapping[str, str], drop_unmapped: bool = False, **kwargs
    ):
        self.name_map = dict(name_map)
        self.drop_unmapped = drop_unmapped
        super().__init__(**kwargs)

    def to_config(self) -> dict[str, Any]:
        """Serialize this MapKeys operator to a config dict.

        Returns:
            A dict with ``class_name``, ``module_path``, and ``config`` keys,
            where ``config`` contains ``name_map`` and ``drop_unmapped``.
        """
        config = super().to_config()
        config["config"] = {
            "name_map": dict(self.name_map),
            "drop_unmapped": self.drop_unmapped,
        }
        return config

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        key_columns, data_columns = stream.keys()
        missing_keys = set(key_columns) - set(self.name_map.keys())

        if not any(n in key_columns for n in self.name_map):
            # nothing to rename in the keys, return stream as is
            return stream

        table = stream.as_table(
            columns={"source": True, "system_keys": True, "sort_by_keys": False}
        )

        name_map = {
            tc: self.name_map.get(tc, tc)
            for tc in key_columns
            if tc in self.name_map or not self.drop_unmapped
        }  # rename the key as necessary
        new_key_columns = list(name_map.values())
        for c in data_columns:
            name_map[c] = c  # no renaming on data columns

        renamed_table = table.rename_columns(name_map)

        if missing_keys and self.drop_unmapped:
            # drop any keys that are not in the name map
            renamed_table = renamed_table.drop_columns(list(missing_keys))

        return ArrowTableStream(
            renamed_table,
            key_columns=new_key_columns,
        )

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """
        # verify that renamed value does NOT collide with other columns
        key_columns, data_columns = stream.keys()
        relevant_source = []
        relevant_target = []
        for source, target in self.name_map.items():
            if source in key_columns:
                relevant_source.append(source)
                relevant_target.append(target)
        remaining_key_columns = set(key_columns) - set(relevant_source)
        overlapping_key_columns = remaining_key_columns.intersection(relevant_target)
        overlapping_data_columns = set(data_columns).intersection(relevant_target)

        if overlapping_key_columns or overlapping_data_columns:
            message = f"Renaming {self.name_map} would cause collisions with existing columns: "
            if overlapping_key_columns:
                message += f"overlapping key columns: {overlapping_key_columns}."
            if overlapping_data_columns:
                message += f"overlapping data columns: {overlapping_data_columns}."
            raise InputValidationError(message)

    def unary_output_schema(
        self,
        stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        key_schema, data_schema = stream.output_schema(
            columns=columns, all_info=all_info
        )

        new_key_schema = {
            self.name_map.get(k, k): v
            for k, v in key_schema.items()
            if k in self.name_map or not self.drop_unmapped
        }

        return Schema(new_key_schema), data_schema

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[KeyProtocol, DataProtocol]]],
        output: WritableChannel[tuple[KeyProtocol, DataProtocol]],
        **kwargs: Any,
    ) -> None:
        """Streaming: rename key columns per row without materializing."""
        try:
            rename_map: dict[str, str] | None = None
            unmapped: list[str] | None = None
            async for key, data in inputs[0]:
                if rename_map is None:
                    key_keys = key.keys()
                    rename_map = {
                        k: self.name_map[k] for k in key_keys if k in self.name_map
                    }
                    if self.drop_unmapped:
                        unmapped = [k for k in key_keys if k not in self.name_map]
                if not rename_map:
                    await output.send((key, data))
                else:
                    new_key = key.rename(rename_map)
                    if unmapped:
                        new_key = new_key.drop(*unmapped)
                    await output.send((new_key, data))
        finally:
            await output.close()

    def identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            self.name_map,
            self.drop_unmapped,
        )
