import logging
from abc import ABC, abstractmethod
from collections.abc import Collection, Mapping, Sequence
from typing import TYPE_CHECKING, Any, ClassVar

from orcapod.core.operators.meta_columns import WithoutMetaWriter
from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.core.operators.base import UnaryOperator
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, ContentHash, Schema
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)


def _canonical_columns(columns: str | Collection[str]) -> tuple[str, ...]:
    """Return the column names an operator acts on, as a sorted tuple without repeats.

    Select and Drop operators act on a set of names: the output keeps the
    stream's column order whatever order the names were given in, and a repeated
    name changes nothing. Their identity is therefore that set, written as a
    sorted list, so a list, a tuple, a set or a single string naming the same
    columns give one hash; an already sorted list keeps the hash it had before
    (ITL-942 F). A tuple is stored, never the caller's own collection.
    """
    if isinstance(columns, str):
        return (columns,)
    return tuple(sorted(set(columns)))


def require_a_data_column(remaining: Collection[str]) -> None:
    """Refuse a column operation whose result would have no data column.

    A stream needs at least one data column (``ArrowTableStream`` refuses a table
    without one), so the sync path checks this when the operator is applied and
    the async path on the first row, with this one error. Every column operator
    (``SelectDataColumns``, ``DropDataColumns``, ``SelectColumns``,
    ``DropColumns``, ``MapData``) uses it. Meta (``__``) columns
    are not data: no operator carries them (ITL-1080).

    Args:
        remaining: The data columns the result would have.

    Raises:
        InputValidationError: If ``remaining`` has no data column.
    """
    if not [c for c in remaining if not c.startswith(constants.META_PREFIX)]:
        raise InputValidationError(
            "no data column would remain: a stream needs at least one data column"
        )


class SelectTagColumns(UnaryOperator):
    """
    Operator that selects specified columns from a stream.
    """

    def __init__(self, columns: str | Collection[str], strict: bool = True, **kwargs):
        self.columns = _canonical_columns(columns)
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
        tag_columns, data_columns = stream.keys()
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
        tag_columns, data_columns = stream.keys()
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
        tag_schema, data_schema = stream.output_schema(
            columns=columns, all_info=all_info
        )
        tag_columns, _ = stream.keys()
        tags_to_drop = [tc for tc in tag_columns if tc not in self.columns]

        # this ensures all system tag columns are preserved
        new_tag_schema = {k: v for k, v in tag_schema.items() if k not in tags_to_drop}

        return Schema(new_tag_schema), data_schema

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash],
    ) -> None:
        """Streaming: select tag columns per row without materializing.

        Args:
            inputs: The one readable input channel.
            output: Writable channel for downstream consumption.
            input_pipeline_hashes: Not used: this operator keeps system-tag
                names as they are, so no column is named after an input's
                pipeline hash.
        """
        try:
            output = WithoutMetaWriter(output)
            tags_to_drop: list[str] | None = None
            async for tag, data in inputs[0]:
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
                    await output.send((tag, data))
                else:
                    await output.send((tag.drop(*tags_to_drop), data))
        finally:
            await output.close()

    def identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            list(self.columns),
            self.strict,
        )


class SelectDataColumns(UnaryOperator):
    """
    Operator that selects specified columns from a stream.
    """

    def __init__(self, columns: str | Collection[str], strict: bool = True, **kwargs):
        self.columns = _canonical_columns(columns)
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
        tag_columns, data_columns = stream.keys()
        data_columns_to_drop = [c for c in data_columns if c not in self.columns]
        new_data_columns = [c for c in data_columns if c not in data_columns_to_drop]

        if len(new_data_columns) == len(data_columns):
            logger.info("All data columns are selected. Returning stream unaltered.")
            return stream

        table = stream.as_table(
            columns={"source": True, "system_tags": True, "sort_by_tags": False},
        )
        # make sure to drop associated source fields
        associated_source_fields = [
            f"{constants.SOURCE_PREFIX}{c}" for c in data_columns_to_drop
        ]
        data_columns_to_drop.extend(associated_source_fields)

        modified_table = table.drop_columns(data_columns_to_drop)

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
        tag_columns, listed_columns = stream.keys()
        # A raw stream lists its meta (``__``) columns as data; they are not data
        # columns to select, as on the async path (ITL-1080).
        data_columns = [
            c for c in listed_columns if not c.startswith(constants.META_PREFIX)
        ]
        columns_to_select = self.columns
        missing_columns = set(columns_to_select) - set(data_columns)
        if missing_columns and self.strict:
            raise InputValidationError(
                f"Missing data columns: {missing_columns}. Make sure all specified columns to select are present or use strict=False to ignore missing columns"
            )
        require_a_data_column([c for c in data_columns if c in columns_to_select])

    def unary_output_schema(
        self,
        stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        tag_schema, data_schema = stream.output_schema(
            columns=columns, all_info=all_info
        )
        _, data_columns = stream.keys()
        data_to_drop = [pc for pc in data_columns if pc not in self.columns]

        # this ensures all system tag columns are preserved
        new_data_schema = {
            k: v for k, v in data_schema.items() if k not in data_to_drop
        }

        return tag_schema, Schema(new_data_schema)

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash],
    ) -> None:
        """Streaming: select data columns per row without materializing.

        Args:
            inputs: The one readable input channel.
            output: Writable channel for downstream consumption.
            input_pipeline_hashes: Not used: this operator keeps system-tag
                names as they are, so no column is named after an input's
                pipeline hash.
        """
        try:
            output = WithoutMetaWriter(output)
            pkts_to_drop: list[str] | None = None
            async for tag, data in inputs[0]:
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
                    require_a_data_column([c for c in pkt_keys if c in self.columns])
                    pkts_to_drop = [c for c in pkt_keys if c not in self.columns]
                if not pkts_to_drop:
                    await output.send((tag, data))
                else:
                    await output.send((tag, data.drop(*pkts_to_drop)))
        finally:
            await output.close()

    def identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            list(self.columns),
            self.strict,
        )


class DropTagColumns(UnaryOperator):
    """
    Operator that drops specified columns from a stream.
    """

    def __init__(self, columns: str | Collection[str], strict: bool = True, **kwargs):
        self.columns = _canonical_columns(columns)
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
        tag_columns, data_columns = stream.keys()
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
        tag_columns, data_columns = stream.keys()
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
        tag_schema, data_schema = stream.output_schema(
            columns=columns, all_info=all_info
        )
        # Only the named user tags go: the system-tag columns stay, as in the
        # computed result (ITL-1135).
        new_tag_schema = {k: v for k, v in tag_schema.items() if k not in self.columns}

        return Schema(new_tag_schema), data_schema

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash],
    ) -> None:
        """Streaming: drop tag columns per row without materializing.

        Args:
            inputs: The one readable input channel.
            output: Writable channel for downstream consumption.
            input_pipeline_hashes: Not used: this operator keeps system-tag
                names as they are, so no column is named after an input's
                pipeline hash.
        """
        try:
            output = WithoutMetaWriter(output)
            effective_drops: list[str] | None = None
            async for tag, data in inputs[0]:
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
                    await output.send((tag, data))
                else:
                    await output.send((tag.drop(*effective_drops), data))
        finally:
            await output.close()

    def identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            list(self.columns),
            self.strict,
        )


class DropDataColumns(UnaryOperator):
    """
    Operator that drops specified columns from a stream.
    """

    def __init__(self, columns: str | Collection[str], strict: bool = True, **kwargs):
        self.columns = _canonical_columns(columns)
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
        tag_columns, data_columns = stream.keys()
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
        _, listed_columns = stream.keys()
        # A raw stream lists its meta (``__``) columns as data; they are not data
        # columns to drop, as on the async path (ITL-1080).
        data_columns = [
            c for c in listed_columns if not c.startswith(constants.META_PREFIX)
        ]
        missing_columns = set(self.columns) - set(data_columns)
        if missing_columns and self.strict:
            raise InputValidationError(
                f"Missing data columns: {missing_columns}. Make sure all specified columns to drop are present or use strict=False to ignore missing columns"
            )
        require_a_data_column([c for c in data_columns if c not in self.columns])

    def unary_output_schema(
        self,
        stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        tag_schema, data_schema = stream.output_schema(
            columns=columns, all_info=all_info
        )

        new_data_schema = {
            k: v for k, v in data_schema.items() if k not in self.columns
        }

        return tag_schema, Schema(new_data_schema)

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash],
    ) -> None:
        """Streaming: drop data columns per row without materializing.

        Args:
            inputs: The one readable input channel.
            output: Writable channel for downstream consumption.
            input_pipeline_hashes: Not used: this operator keeps system-tag
                names as they are, so no column is named after an input's
                pipeline hash.
        """
        try:
            output = WithoutMetaWriter(output)
            effective_drops: list[str] | None = None
            async for tag, data in inputs[0]:
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
                    require_a_data_column(
                        [c for c in pkt_keys if c not in self.columns]
                    )
                    effective_drops = (
                        list(self.columns)
                        if self.strict
                        else [c for c in self.columns if c in pkt_keys]
                    )
                if not effective_drops:
                    await output.send((tag, data))
                else:
                    await output.send((tag, data.drop(*effective_drops)))
        finally:
            await output.close()

    def identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            list(self.columns),
            self.strict,
        )


def _is_meta(name: str) -> bool:
    """Whether ``name`` is a meta column or a raw stream's source-info column for one."""
    return name.startswith(
        (constants.META_PREFIX, f"{constants.SOURCE_PREFIX}{constants.META_PREFIX}")
    )


class _TagAndDataColumnsOperator(UnaryOperator, ABC):
    """Base of the operators that act on named columns, tag or data alike (ITL-1084).

    A subclass says only which of a group's columns it removes (``_removed``);
    everything else is shared. ``DropColumns`` of one group's names gives the rows
    of ``DropTagColumns`` or ``DropDataColumns``; ``SelectColumns`` gives those of
    ``SelectTagColumns`` or ``SelectDataColumns`` when its names also include every
    column of the other group. System tags pass through, the source-info column of
    a removed data column goes with it, and the stream's column order is kept.
    Unlike those operators, no data meta (``__``) column of the input is carried,
    on either path, as ``Join`` does; and a result without a data column is refused
    when the operator is called, on both paths.
    """

    _verb: ClassVar[str] = ""

    def __init__(self, columns: str | Collection[str], strict: bool = True, **kwargs):
        self.columns: tuple[str, ...] = _canonical_columns(columns)
        self.strict: bool = strict
        super().__init__(**kwargs)

    @abstractmethod
    def _removed(self, columns: Collection[str]) -> list[str]:
        """Return the names among ``columns`` (one group, in order) this operator removes."""

    def to_config(self) -> dict[str, Any]:
        """Serialize this operator to a config dict.

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

    def identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            list(self.columns),
            self.strict,
        )

    def _removals(
        self, tag_columns: Collection[str], data_columns: Collection[str]
    ) -> tuple[list[str], list[str]]:
        """Return the tag columns and the user data columns this operator removes.

        Args:
            tag_columns: The input's tag columns, in order.
            data_columns: The input's user data columns, in order (no meta column).
        """
        return self._removed(tag_columns), self._removed(data_columns)

    def _validate_columns(
        self, tag_columns: Collection[str], data_columns: Collection[str]
    ) -> None:
        """Check the names against the input's columns and the result's data columns.

        Args:
            tag_columns: The input's tag columns.
            data_columns: The input's user data columns (no meta column).

        Raises:
            InputValidationError: When ``strict`` and a name is neither a tag nor a
                data column, or when the result would have no data column.
        """
        missing = set(self.columns) - set(tag_columns) - set(data_columns)
        if missing and self.strict:
            raise InputValidationError(
                f"Missing columns: {sorted(missing)}. Make sure all specified columns "
                f"to {self._verb} are tag or data columns of the stream, or use "
                f"strict=False to ignore missing columns"
            )
        _, data_removed = self._removals(tag_columns, data_columns)
        try:
            require_a_data_column([c for c in data_columns if c not in data_removed])
        except InputValidationError as error:
            raise InputValidationError(
                f"{self.__class__.__name__}({list(self.columns)}) would leave no data "
                f"column: {error} (it removes data columns {data_removed})"
            ) from error

    @staticmethod
    def _user_columns(stream: StreamProtocol) -> tuple[tuple[str, ...], list[str]]:
        """The stream's tag columns and its data columns without meta columns.

        A raw ``ArrowTableStream`` lists its meta (``__``) columns as data columns.
        """
        tag_columns, data_columns = stream.keys()
        return tag_columns, [c for c in data_columns if not _is_meta(c)]

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        """Validate the names against the input stream.

        Raises:
            InputValidationError: When ``strict`` and a name is neither a tag nor a
                data column, or when the result would have no data column.
        """
        self._validate_columns(*self._user_columns(stream))

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        tag_columns, data_columns = self._user_columns(stream)
        tags_removed, data_removed = self._removals(tag_columns, data_columns)
        table = stream.as_table(
            columns={"source": True, "system_tags": True, "sort_by_tags": False}
        )
        removed = {
            *tags_removed,
            *data_removed,
            *(f"{constants.SOURCE_PREFIX}{c}" for c in data_removed),
        }
        return ArrowTableStream(
            table.drop_columns(
                [
                    c
                    for c in table.column_names
                    # Only data meta goes; a tag column is never stripped as meta.
                    if c in removed or (_is_meta(c) and c not in tag_columns)
                ]
            ),
            tag_columns=[c for c in tag_columns if c not in tags_removed],
            data_context=stream.data_context_key,
        )

    def unary_output_schema(
        self,
        stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        tag_schema, data_schema = stream.output_schema(
            columns=columns, all_info=all_info
        )
        tags_removed, data_removed = self._removals(*self._user_columns(stream))
        # System tag columns are not among the removed names, so they are kept.
        return (
            Schema({k: v for k, v in tag_schema.items() if k not in tags_removed}),
            Schema(
                {
                    k: v
                    for k, v in data_schema.items()
                    if k not in data_removed and not _is_meta(k)
                }
            ),
        )

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash],
    ) -> None:
        """Streaming: remove the columns per row without materializing.

        The names are validated against the first row, as ``validate_unary_input``
        validates them against the stream.

        Args:
            inputs: The one readable input channel.
            output: Writable channel for downstream consumption.
            input_pipeline_hashes: Not used: this operator keeps system-tag
                names as they are, so no column is named after an input's
                pipeline hash.
        """
        try:
            output = WithoutMetaWriter(output)
            removals: tuple[list[str], list[str]] | None = None
            async for tag, data in inputs[0]:
                if removals is None:
                    tag_keys, data_keys = tag.keys(), data.keys()
                    self._validate_columns(tag_keys, data_keys)
                    removals = self._removals(tag_keys, data_keys)
                tags_removed, data_removed = removals
                if tags_removed:
                    tag = tag.drop(*tags_removed)
                if data_removed:
                    data = data.drop(*data_removed)
                await output.send((tag, data))
        finally:
            await output.close()


class DropColumns(_TagAndDataColumnsOperator):
    """Drop the named columns, whether each is a tag or a data column (ITL-1084).

    For tag names only it gives the rows of ``DropTagColumns``, for data names only
    those of ``DropDataColumns``. A dropped data column's source-info column goes
    with it; system tags are kept.

    Args:
        columns: One column name or a collection of names.
        strict: When ``True`` (the default), a name that is neither a tag nor a data
            column of the input raises ``InputValidationError``; when ``False`` it
            is ignored.

    Raises:
        InputValidationError: When called on a stream whose data columns would all
            be dropped: a stream needs at least one data column.
    """

    _verb: ClassVar[str] = "drop"

    def _removed(self, columns: Collection[str]) -> list[str]:
        return [c for c in columns if c in self.columns]


class SelectColumns(_TagAndDataColumnsOperator):
    """Keep exactly the named columns, tag or data; drop every other one (ITL-1084).

    Every user tag and data column not named is dropped, with the source-info column
    of each dropped data column; system tags are kept. Naming no tag is allowed (the
    result has no tag column).

    Args:
        columns: One column name or a collection of names.
        strict: When ``True`` (the default), a name that is neither a tag nor a data
            column of the input raises ``InputValidationError``; when ``False`` it
            is ignored.

    Raises:
        InputValidationError: When called on a stream none of whose data columns is
            named: a stream needs at least one data column.
    """

    _verb: ClassVar[str] = "select"

    def _removed(self, columns: Collection[str]) -> list[str]:
        return [c for c in columns if c not in self.columns]


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
        tag_columns, data_columns = stream.keys()
        missing_tags = set(tag_columns) - set(self.name_map.keys())

        if not any(n in tag_columns for n in self.name_map):
            # nothing to rename in the tags, return stream as is
            return stream

        table = stream.as_table(columns={"source": True, "system_tags": True})

        name_map = {
            tc: self.name_map.get(tc, tc) for tc in tag_columns
        }  # rename the tag as necessary
        new_tag_columns = [name_map[tc] for tc in tag_columns]
        for c in data_columns:
            name_map[c] = c  # no renaming on data columns

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
        tag_columns, data_columns = stream.keys()
        relevant_source = []
        relevant_target = []
        for source, target in self.name_map.items():
            if source in tag_columns:
                relevant_source.append(source)
                relevant_target.append(target)
        remaining_tag_columns = set(tag_columns) - set(relevant_source)
        overlapping_tag_columns = remaining_tag_columns.intersection(relevant_target)
        overlapping_data_columns = set(data_columns).intersection(relevant_target)

        if overlapping_tag_columns or overlapping_data_columns:
            message = f"Renaming {self.name_map} would cause collisions with existing columns: "
            if overlapping_tag_columns:
                message += f"overlapping tag columns: {overlapping_tag_columns}."
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
        tag_schema, data_schema = stream.output_schema(
            columns=columns, all_info=all_info
        )

        # Create new data schema with renamed keys
        new_tag_schema = {self.name_map.get(k, k): v for k, v in tag_schema.items()}

        return Schema(new_tag_schema), data_schema

    def identity_structure(self) -> Any:
        return (
            self.__class__.__name__,
            self.name_map,
            self.drop_unmapped,
        )
