import logging
from collections.abc import Collection, Mapping, Sequence
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

logger = logging.getLogger(__name__)


class SelectKeyColumns(UnaryOperator):
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
        """Serialize this SelectKeyColumns operator to a config dict.

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
        key_columns, data_columns = stream.keys()
        keys_to_drop = [c for c in key_columns if c not in self.columns]
        new_key_columns = [c for c in key_columns if c not in keys_to_drop]

        if len(new_key_columns) == len(key_columns):
            logger.info("All key columns are selected. Returning stream unaltered.")
            return stream

        table = stream.as_table(
            columns={"source": True, "system_keys": True, "sort_by_keys": False}
        )

        modified_table = table.drop_columns(list(keys_to_drop))

        return ArrowTableStream(
            modified_table,
            key_columns=new_key_columns,
        )

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """
        # TODO: remove redundant logic
        key_columns, data_columns = stream.keys()
        columns_to_select = self.columns
        missing_columns = set(columns_to_select) - set(key_columns)
        if missing_columns and self.strict:
            raise InputValidationError(
                f"Missing key columns: {missing_columns}. Make sure all specified columns to select are present or use strict=False to ignore missing columns"
            )

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
        key_columns, _ = stream.keys()
        keys_to_drop = [tc for tc in key_columns if tc not in self.columns]

        # this ensures all system key columns are preserved
        new_key_schema = {k: v for k, v in key_schema.items() if k not in keys_to_drop}

        return Schema(new_key_schema), data_schema

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[KeyProtocol, DataProtocol]]],
        output: WritableChannel[tuple[KeyProtocol, DataProtocol]],
        **kwargs: Any,
    ) -> None:
        """Streaming: select key columns per row without materializing."""
        try:
            keys_to_drop: list[str] | None = None
            async for key, data in inputs[0]:
                if keys_to_drop is None:
                    key_keys = key.keys()
                    if self.strict:
                        missing = set(self.columns) - set(key_keys)
                        if missing:
                            raise InputValidationError(
                                f"Missing key columns: {missing}. Make sure all "
                                f"specified columns to select are present or use "
                                f"strict=False to ignore missing columns"
                            )
                    keys_to_drop = [c for c in key_keys if c not in self.columns]
                if not keys_to_drop:
                    await output.send((key, data))
                else:
                    await output.send((key.drop(*keys_to_drop), data))
        finally:
            await output.close()

    def identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            self.columns,
            self.strict,
        )


class SelectDataColumns(UnaryOperator):
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
        """Serialize this SelectDataColumns operator to a config dict.

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
        key_columns, data_columns = stream.keys()
        data_columns_to_drop = [c for c in data_columns if c not in self.columns]
        new_data_columns = [
            c for c in data_columns if c not in data_columns_to_drop
        ]

        if len(new_data_columns) == len(data_columns):
            logger.info("All data columns are selected. Returning stream unaltered.")
            return stream

        table = stream.as_table(
            columns={"source": True, "system_keys": True, "sort_by_keys": False},
        )
        # make sure to drop associated source fields
        associated_source_fields = [
            f"{constants.SOURCE_PREFIX}{c}" for c in data_columns_to_drop
        ]
        data_columns_to_drop.extend(associated_source_fields)

        modified_table = table.drop_columns(data_columns_to_drop)

        return ArrowTableStream(
            modified_table,
            key_columns=key_columns,
        )

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """
        # TODO: remove redundant logic
        key_columns, data_columns = stream.keys()
        columns_to_select = self.columns
        missing_columns = set(columns_to_select) - set(data_columns)
        if missing_columns and self.strict:
            raise InputValidationError(
                f"Missing data columns: {missing_columns}. Make sure all specified columns to select are present or use strict=False to ignore missing columns"
            )

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
        _, data_columns = stream.keys()
        data_to_drop = [pc for pc in data_columns if pc not in self.columns]

        # this ensures all system key columns are preserved
        new_data_schema = {
            k: v for k, v in data_schema.items() if k not in data_to_drop
        }

        return key_schema, Schema(new_data_schema)

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[KeyProtocol, DataProtocol]]],
        output: WritableChannel[tuple[KeyProtocol, DataProtocol]],
        **kwargs: Any,
    ) -> None:
        """Streaming: select data columns per row without materializing."""
        try:
            pkts_to_drop: list[str] | None = None
            async for key, data in inputs[0]:
                if pkts_to_drop is None:
                    pkt_keys = data.keys()
                    if self.strict:
                        missing = set(self.columns) - set(pkt_keys)
                        if missing:
                            raise InputValidationError(
                                f"Missing data columns: {missing}. Make sure all "
                                f"specified columns to select are present or use "
                                f"strict=False to ignore missing columns"
                            )
                    pkts_to_drop = [c for c in pkt_keys if c not in self.columns]
                if not pkts_to_drop:
                    await output.send((key, data))
                else:
                    await output.send((key, data.drop(*pkts_to_drop)))
        finally:
            await output.close()

    def identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            self.columns,
            self.strict,
        )


class DropKeyColumns(UnaryOperator):
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
        """Serialize this DropKeyColumns operator to a config dict.

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
        key_columns, data_columns = stream.keys()
        columns_to_drop = self.columns
        if not self.strict:
            columns_to_drop = [c for c in columns_to_drop if c in key_columns]

        new_key_columns = [c for c in key_columns if c not in columns_to_drop]

        if len(columns_to_drop) == 0:
            logger.info("No key columns to drop. Returning stream unaltered.")
            return stream

        table = stream.as_table(
            columns={"source": True, "system_keys": True, "sort_by_keys": False}
        )

        modified_table = table.drop_columns(list(columns_to_drop))

        return ArrowTableStream(
            modified_table,
            key_columns=new_key_columns,
        )

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """
        # TODO: remove redundant logic
        key_columns, data_columns = stream.keys()
        columns_to_drop = self.columns
        missing_columns = set(columns_to_drop) - set(key_columns)
        if missing_columns and self.strict:
            raise InputValidationError(
                f"Missing key columns: {missing_columns}. Make sure all specified columns to drop are present or use strict=False to ignore missing columns"
            )

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
        key_columns, _ = stream.keys()
        new_key_columns = [c for c in key_columns if c not in self.columns]

        new_key_schema = {k: v for k, v in key_schema.items() if k in new_key_columns}

        return Schema(new_key_schema), data_schema

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[KeyProtocol, DataProtocol]]],
        output: WritableChannel[tuple[KeyProtocol, DataProtocol]],
        **kwargs: Any,
    ) -> None:
        """Streaming: drop key columns per row without materializing."""
        try:
            effective_drops: list[str] | None = None
            async for key, data in inputs[0]:
                if effective_drops is None:
                    key_keys = key.keys()
                    if self.strict:
                        missing = set(self.columns) - set(key_keys)
                        if missing:
                            raise InputValidationError(
                                f"Missing key columns: {missing}. Make sure all "
                                f"specified columns to drop are present or use "
                                f"strict=False to ignore missing columns"
                            )
                    effective_drops = (
                        list(self.columns)
                        if self.strict
                        else [c for c in self.columns if c in key_keys]
                    )
                if not effective_drops:
                    await output.send((key, data))
                else:
                    await output.send((key.drop(*effective_drops), data))
        finally:
            await output.close()

    def identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            self.columns,
            self.strict,
        )


class DropDataColumns(UnaryOperator):
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
        """Serialize this DropDataColumns operator to a config dict.

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
        key_columns, data_columns = stream.keys()
        columns_to_drop = list(self.columns)
        if not self.strict:
            columns_to_drop = [c for c in columns_to_drop if c in data_columns]

        if len(columns_to_drop) == 0:
            logger.info("No data columns to drop. Returning stream unaltered.")
            return stream

        # make sure all associated source columns are dropped too
        associated_source_columns = [
            f"{constants.SOURCE_PREFIX}{c}" for c in columns_to_drop
        ]
        columns_to_drop.extend(associated_source_columns)

        table = stream.as_table(
            columns={"source": True, "system_keys": True, "sort_by_keys": False}
        )

        modified_table = table.drop_columns(columns_to_drop)

        return ArrowTableStream(
            modified_table,
            key_columns=key_columns,
        )

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """
        # TODO: remove redundant logic
        _, data_columns = stream.keys()
        missing_columns = set(self.columns) - set(data_columns)
        if missing_columns and self.strict:
            raise InputValidationError(
                f"Missing data columns: {missing_columns}. Make sure all specified columns to drop are present or use strict=False to ignore missing columns"
            )

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

        new_data_schema = {
            k: v for k, v in data_schema.items() if k not in self.columns
        }

        return key_schema, Schema(new_data_schema)

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[KeyProtocol, DataProtocol]]],
        output: WritableChannel[tuple[KeyProtocol, DataProtocol]],
        **kwargs: Any,
    ) -> None:
        """Streaming: drop data columns per row without materializing."""
        try:
            effective_drops: list[str] | None = None
            async for key, data in inputs[0]:
                if effective_drops is None:
                    pkt_keys = data.keys()
                    if self.strict:
                        missing = set(self.columns) - set(pkt_keys)
                        if missing:
                            raise InputValidationError(
                                f"Missing data columns: {missing}. Make sure all "
                                f"specified columns to drop are present or use "
                                f"strict=False to ignore missing columns"
                            )
                    effective_drops = (
                        list(self.columns)
                        if self.strict
                        else [c for c in self.columns if c in pkt_keys]
                    )
                if not effective_drops:
                    await output.send((key, data))
                else:
                    await output.send((key, data.drop(*effective_drops)))
        finally:
            await output.close()

    def identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            self.columns,
            self.strict,
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

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        key_columns, data_columns = stream.keys()
        missing_keys = set(key_columns) - set(self.name_map.keys())

        if not any(n in key_columns for n in self.name_map):
            # nothing to rename in the keys, return stream as is
            return stream

        table = stream.as_table(columns={"source": True, "system_keys": True})

        name_map = {
            tc: self.name_map.get(tc, tc) for tc in key_columns
        }  # rename the key as necessary
        new_key_columns = [name_map[tc] for tc in key_columns]
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

        # Create new data schema with renamed keys
        new_key_schema = {self.name_map.get(k, k): v for k, v in key_schema.items()}

        return Schema(new_key_schema), data_schema

    def identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            self.name_map,
            self.drop_unmapped,
        )
