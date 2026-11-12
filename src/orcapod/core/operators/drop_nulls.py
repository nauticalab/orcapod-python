"""DropNulls operator: drop rows with a null in the chosen columns, narrow ``T | None`` to ``T``."""

from __future__ import annotations

import types
import typing
from collections.abc import Collection, Sequence
from typing import TYPE_CHECKING, Any

from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.core.datagrams import Data, Tag
from orcapod.core.operators.base import UnaryOperator
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, DataType, Schema
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.compute as pc
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")


def _without_none(python_type: DataType) -> DataType:
    """Return ``python_type`` with ``None`` removed from a union.

    ``int | None`` gives ``int`` and ``int | str | None`` gives ``int | str``.
    A type that does not admit ``None`` is returned unchanged.

    Args:
        python_type: A column's Python type.

    Returns:
        The type without its ``None`` member.
    """
    if typing.get_origin(python_type) not in (typing.Union, types.UnionType):
        return python_type
    members = tuple(m for m in typing.get_args(python_type) if m is not type(None))
    if len(members) == len(typing.get_args(python_type)) or not members:
        return python_type
    return typing.Union[members]  # noqa: UP007  # built from a runtime tuple of members


class DropNulls(UnaryOperator):
    """Keep the rows where every named column is non-null; narrow those columns to ``T``.

    A row survives only if all the named columns hold a value. Afterwards each
    named column is non-nullable: ``T`` instead of ``T | None`` in
    ``output_schema()``, and a non-nullable field in the Arrow table. A pod
    whose parameter is a plain ``T`` can then consume the column.

    Without names (``DropNulls()``), the columns are every tag column and every
    data column of the input stream: a row with a null in any of them is
    dropped, and all of them are non-nullable afterwards. The names are taken
    from each input stream when the operator is applied and are never stored,
    so one ``DropNulls()`` works on any stream. System-tag, source-info,
    context and meta columns are never among them. On a stream whose columns
    are all nullable and legitimately empty in places, this form can drop most
    rows; naming the columns is the precise form.

    The names may be tag columns, data columns, or both. Only the validity of
    the top-level value is read, never the value itself: a null list or a null
    struct is null, while an empty list, a list holding nulls, a struct whose
    fields are null and a floating-point NaN are not. Nested fields keep their
    nullability.

    A named column that is already non-nullable is accepted and left as it is,
    so applying the operator twice equals applying it once.

    Everything else passes through unchanged: the other columns' types and
    nullability, system tags (names and values), source info and row order.
    Because nullability is part of a row's Arrow schema, a surviving row's
    ``Data`` content hash changes when a named data column was nullable, and
    its ``Tag`` content hash changes when a named tag column was nullable. The
    new hash is the one the row has in a stream that declared the column
    non-nullable from the start.

    Args:
        columns: One column name, a collection of names, or ``None`` (the
            default) for every tag and data column of the input stream.
            Duplicates and order are ignored: the operator's identity is the
            set of names, or one fixed value when no names are given. An empty
            collection is not "no argument" and is refused: a list of names
            that came out empty is never read as "every column".

    Raises:
        ValueError: If ``columns`` is an empty collection.
    """

    def __init__(
        self, columns: str | Collection[str] | None = None, **kwargs: Any
    ) -> None:
        self.columns: tuple[str, ...] | None
        if columns is None:
            # Every tag and data column of whichever stream the operator is
            # applied to; resolved per input, never stored.
            self.columns = None
        else:
            names = (columns,) if isinstance(columns, str) else tuple(columns)
            if not names:
                raise ValueError(
                    "DropNulls needs at least one column name; got an empty"
                    + " collection. Pass no argument to mean every tag and data"
                    + " column."
                )
            # A sorted tuple of unique names, so a list, a tuple and a set of the
            # same names (in any order) are one identity.
            self.columns = tuple(sorted(set(names)))
        super().__init__(**kwargs)

    def identity_structure(self) -> Any:
        """Return the operator's identity: its class name and the set of column names.

        Returns:
            A ``(class_name, columns)`` tuple, where ``columns`` is the sorted
            tuple of unique names, or ``None`` for the no-argument form (every
            tag and data column), whatever stream it is applied to.
        """
        return (self.__class__.__name__, self.columns)

    def to_config(self) -> dict[str, Any]:
        """Serialize this operator to a config dict.

        Returns:
            A dict with ``class_name``, ``module_path``, and ``config`` keys,
            where ``config`` contains ``columns``: the list of names, or
            ``None`` for the no-argument form.
        """
        config = super().to_config()
        config["config"] = {
            "columns": None if self.columns is None else list(self.columns)
        }
        return config

    def _target_columns(
        self, tag_columns: Collection[str], data_columns: Collection[str]
    ) -> tuple[str, ...]:
        """Return the columns to test and narrow for an input with these columns.

        Args:
            tag_columns: The input's tag column names.
            data_columns: The input's data column names.

        Returns:
            The operator's names, or every given tag and data column when the
            operator was built without names. Meta columns are left out: a
            stream built straight from a table lists them among its data
            columns (ITL-846), and they are not user columns.
        """
        if self.columns is not None:
            return self.columns
        return tuple(
            name
            for name in (*tag_columns, *data_columns)
            if not name.startswith(constants.META_PREFIX)
        )

    # ------------------------------------------------------------------
    # Validation and schema prediction
    # ------------------------------------------------------------------

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        """Check that every named column is a tag or data column of ``stream``.

        Args:
            stream: The upstream stream.

        Raises:
            InputValidationError: If a named column is not a tag or data column.
        """
        if self.columns is None:
            return
        tag_columns, data_columns = stream.keys()
        missing = [
            name
            for name in self.columns
            if name not in tag_columns and name not in data_columns
        ]
        if missing:
            raise InputValidationError(
                f"DropNulls: columns {missing} are not tag or data columns of the"
                + f" input stream. Tag columns: {list(tag_columns)}; data columns:"
                + f" {list(data_columns)}."
            )

    @staticmethod
    def _narrow_schema(schema: Schema, names: Collection[str]) -> Schema:
        """Return ``schema`` with ``None`` removed from the type of each column in ``names``."""
        return Schema(
            {
                name: _without_none(python_type) if name in names else python_type
                for name, python_type in schema.items()
            },
            optional_fields=schema.optional_fields,
        )

    def unary_output_schema(
        self,
        stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return the input schemas with the named columns narrowed to ``T``.

        Without names, every tag and data column of ``stream`` is narrowed;
        system-tag columns included by ``columns`` are not.

        Args:
            stream: The upstream stream.
            columns: Column inclusion config passed through to ``output_schema``.
            all_info: Whether to include all info columns.

        Returns:
            A ``(tag_schema, data_schema)`` tuple.
        """
        tag_schema, data_schema = stream.output_schema(
            columns=columns, all_info=all_info
        )
        names = self._target_columns(*stream.keys())
        return (
            self._narrow_schema(tag_schema, names),
            self._narrow_schema(data_schema, names),
        )

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    @staticmethod
    def _narrow_fields(table: pa.Table, names: Collection[str]) -> pa.Table:
        """Flag the columns in ``names`` that are present in ``table`` non-nullable.

        Every column takes the large-normalized form of its input type; no
        other type, flag or value changes. The named columns must hold no null.

        Args:
            table: A table whose named columns hold no null.
            names: The columns to flag.

        Returns:
            The same rows with the named fields non-nullable.
        """
        table = arrow_utils.restore_column_types(
            table,
            {
                field.name: arrow_utils.normalize_to_large_types(
                    field.type, keep_null=True
                )
                for field in table.schema
            },
        )
        schema = table.schema
        for name in names:
            position = schema.get_field_index(name)
            if position >= 0:
                schema = schema.set(  # pyright: ignore[reportUnknownMemberType]  # stubs leave Field unparametrized
                    position, schema.field(position).with_nullable(False)
                )
        # Same arrays under new field flags: no cast, so no value or type can change.
        return pa.Table.from_arrays(table.columns, schema=schema)

    @staticmethod
    def _narrow_table(table: pa.Table, names: Collection[str]) -> pa.Table:
        """Drop the rows with a null in a named column and flag those columns non-nullable.

        Only the columns in ``names`` that are present in ``table`` are
        considered, so the same rule serves a whole stream table and a single
        row's tag or data table.

        Args:
            table: A table holding some or all of the named columns.
            names: The columns to test and narrow.

        Returns:
            The surviving rows, in order, with the named fields non-nullable.
        """
        mask = None
        for name in names:
            if name in table.column_names:
                valid = pc.is_valid(table.column(name))
                mask = valid if mask is None else pc.and_(mask, valid)
        if mask is not None:
            table = table.filter(mask)
        return DropNulls._narrow_fields(table, names)

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        """Return the stream's rows whose named columns are all non-null.

        Args:
            stream: The upstream stream.

        Returns:
            A stream over the surviving rows, in input order, with the named
            columns non-nullable. A result with no rows has the same schema.
        """
        table = stream.as_table(
            columns={"source": True, "system_tags": True, "sort_by_tags": False}
        )
        tag_columns, data_columns = stream.keys()
        return ArrowTableStream(
            self._narrow_table(table, self._target_columns(tag_columns, data_columns)),
            tag_columns=tag_columns,
            data_context=stream.data_context,  # pyright: ignore[reportAttributeAccessIssue]  # StreamProtocol omits data_context, which every concrete stream provides (ITL-844)
        )

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        **kwargs: Any,
    ) -> None:
        """Streaming: test and narrow each row as it arrives, without materializing.

        Each row's tag and data Arrow tables go through the rule of the sync
        path (``_narrow_table``), and a surviving row is rebuilt from those
        tables with its own data context. Without names, the columns are the
        row's own tag and data keys.

        Args:
            inputs: Single-element sequence of readable channels.
            output: Channel to send surviving rows to.
            **kwargs: Ignored; present for protocol compliance.
        """
        try:
            async for tag, data in inputs[0]:
                names = self._target_columns(tag.keys(), data.keys())
                tag_table = self._narrow_table(
                    tag.as_table(columns={"system_tags": True}), names
                )
                data_table = self._narrow_table(
                    data.as_table(columns={"source": True, "meta": True}), names
                )
                # A table with no columns (a stream without tags) has no rows
                # to lose, so only a table that held a named column can drop the row.
                if (tag_table.num_columns and tag_table.num_rows == 0) or (
                    data_table.num_columns and data_table.num_rows == 0
                ):
                    continue
                new_tag = (
                    Tag(tag_table, data_context=tag.data_context_key)
                    if tag_table.num_columns
                    else tag
                )
                await output.send(
                    (new_tag, Data(data_table, data_context=data.data_context_key))
                )
        finally:
            await output.close()
