"""Index operator — positional projection into list-typed columns."""

from __future__ import annotations

import logging
import typing
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, cast

from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.core.operators.base import UnaryOperator
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol
from orcapod.types import ColumnConfig, ContentHash, Schema
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)


class Index(UnaryOperator):
    """Extract an element from a list-typed data column by position.

    The list length is per-packet data, so bounds are not checked at build
    time.  Out-of-bounds access causes the packet to be skipped (or an error
    when ``fail_on_miss=True``).  Negative indices follow Python semantics
    (``-1`` is the last element).

    The extracted column keeps the list's element Arrow type (``list<int8>``
    yields ``int8``). Its nullability follows the predicted output type, which
    is not Optional for list elements, so the column is ``NOT NULL``. An
    element that is ``None`` therefore raises ``ValueError`` (whatever
    ``fail_on_miss`` is), naming the operator, the column and the row's tag.

    Args:
        column: Name of the data column to project into.
        i: Position to extract.  Negative indices follow Python semantics.
        out: Output column name.  ``None`` (default) replaces ``column``
            in-place; a string adds a new column alongside the original.
        fail_on_miss: If ``True``, raise ``RuntimeError`` on out-of-bounds
            instead of skipping.  Excluded from ``identity_structure``.
            See ITL-439.
    """

    def __init__(
        self,
        column: str,
        i: int,
        out: str | None = None,
        fail_on_miss: bool = False,
        **kwargs: Any,
    ) -> None:
        self.column = column
        self.i = i
        self.out = out
        self.fail_on_miss = fail_on_miss
        self._output_type: type = None  # type: ignore[assignment]
        self._mode: str = ""
        # Fallback Arrow type of the element, resolved at validation from the
        # stream's context (``async_execute`` has no stream). ``None`` with a
        # reason when the stream's converter cannot map the element type.
        self._element_arrow_type_fallback: pa.DataType | None = None
        self._element_arrow_type_error: str = ""
        super().__init__(**kwargs)

    def identity_structure(self) -> Any:
        return (self.__class__.__name__, self.column, self.i, self.out)

    def _output_nullable(self) -> bool:
        """Return whether the projected column is nullable.

        Nullability follows the predicted output type: the column is nullable
        only when that type is Optional. The rule is the same for every row and
        for the all-skipped result, so the materialized schema always equals
        the predicted ``output_schema``.

        Returns:
            True when the predicted output type admits ``None``.
        """
        return type(None) in typing.get_args(self._output_type)

    def _element_arrow_type(self, column_type: pa.DataType) -> pa.DataType:
        """Return the Arrow type of one element of a column of ``column_type``.

        The element type is read from the input column's Arrow type, so a
        ``list<int8>`` column yields ``int8``. Only when the column is a
        logical (extension) type, whose storage drops the element's logical
        identity, is the fallback used: the Arrow type of the resolved Python
        element type, resolved at validation from the stream's context.

        Args:
            column_type: Arrow type of the indexed column.

        Returns:
            The Arrow type of the extracted element.

        Raises:
            ValueError: If the fallback is needed but the stream's context
                could not map the element type to an Arrow type.
        """
        if isinstance(
            column_type, (pa.ListType, pa.LargeListType, pa.FixedSizeListType)
        ):
            return cast("pa.DataType", column_type.value_type)
        if self._element_arrow_type_fallback is None:
            raise ValueError(
                "Index: cannot derive the Arrow type of the element of column"
                + f" {self.column!r}: output type {self._output_type!r} has no"
                + " Arrow type in the stream's context"
                + f" ({self._element_arrow_type_error or 'not validated'})."
            )
        return self._element_arrow_type_fallback

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate_unary_input(self, stream: StreamProtocol) -> None:
        """Validate input schema and resolve output element type.

        Args:
            stream: The upstream stream to validate.

        Raises:
            InputValidationError: If ``column`` is missing, ``out`` collides,
                or the column type is not a supported index target.
            NotImplementedError: If the column holds an extension type whose
                ``index_element`` is not yet implemented.
        """
        tag_schema, data_schema = stream.output_schema()
        data_columns = list(data_schema.keys())

        if self.column not in data_columns:
            raise InputValidationError(
                f"Index: column {self.column!r} not found in data schema. "
                f"Available data columns: {data_columns}"
            )

        if self.out is not None and self.out in tag_schema:
            raise InputValidationError(
                f"Index: out column {self.out!r} is a tag column of the input, "
                f"and a name cannot be both a tag and a data column. Choose a "
                f"different name, or rename the tag with map_tags."
            )

        if self.out is not None and self.out in data_columns:
            raise InputValidationError(
                f"Index: out column {self.out!r} already exists in data schema. "
                f"Choose a different name to avoid clobbering existing data."
            )

        # StreamProtocol omits data_context, which every concrete stream (StreamBase)
        # provides; the same debt is baselined in Batch and GroupBy (ITL-844).
        converter = stream.data_context.type_converter  # pyright: ignore[reportAttributeAccessIssue]
        col_type = data_schema[self.column]
        origin = typing.get_origin(col_type)

        if origin is list:
            args = typing.get_args(col_type)
            self._output_type = args[0] if args else Any  # type: ignore[assignment]
            self._mode = "list"
        else:
            # Extension type — delegate to logical type's index_element via the
            # stream's own type converter (not a global default).
            lt = converter.get_logical_type(col_type)
            if lt is None:
                raise InputValidationError(
                    f"Index: column {self.column!r} has type {col_type!r} which is "
                    f"not a supported index target (not list[T] and no registered "
                    f"logical type)."
                )
            # May raise NotImplementedError for types not yet implemented
            self._output_type = lt.index_element()
            self._mode = "extension"

        # Resolved now because the streaming path has no stream. A failure is
        # deferred to the point of use: most columns never need the fallback.
        try:
            self._element_arrow_type_fallback = converter.python_type_to_arrow_type(
                self._output_type
            )
            self._element_arrow_type_error = ""
        except (ValueError, TypeError, NotImplementedError) as exc:
            self._element_arrow_type_fallback = None
            self._element_arrow_type_error = str(exc)

    # ------------------------------------------------------------------
    # Schema prediction
    # ------------------------------------------------------------------

    def unary_output_schema(
        self,
        stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return the (tag, data) schemas for the output stream.

        Args:
            stream: The upstream stream.
            columns: Column inclusion config.
            all_info: Include all info columns.

        Returns:
            A ``(tag_schema, data_schema)`` tuple.
        """
        tag_schema, data_schema = stream.output_schema(
            columns=columns, all_info=all_info
        )
        data_dict = dict(data_schema)

        if self.out is None:
            data_dict[self.column] = self._output_type
        else:
            data_dict[self.out] = self._output_type

        return tag_schema, Schema(data_dict)

    # ------------------------------------------------------------------
    # Per-packet processing
    # ------------------------------------------------------------------

    def _process_one(
        self,
        tag: TagProtocol,
        data: DataProtocol,
    ) -> tuple[TagProtocol, DataProtocol] | None:
        """Process a single (tag, data) packet.

        Extracts the element at position ``self.i`` from the packet's
        ``self.column`` and returns a new (tag, data) pair with the projection
        applied.  Returns ``None`` if the index is out of bounds and
        ``fail_on_miss=False``.

        The output schema is already known at build time (``self._output_type``),
        so a new ``Data`` object is created with the correct type directly —
        no schema re-derivation needed at runtime.

        Args:
            tag: The tag for this packet.
            data: The data for this packet.

        Returns:
            A ``(tag, new_data)`` pair with the projected column, or ``None``
            if the packet was skipped.

        Raises:
            RuntimeError: If the index is out of bounds and ``fail_on_miss=True``.
            ValueError: If the extracted element is ``None`` but the predicted
                output type is not Optional.
        """
        col_val = data[self.column]
        length = len(col_val)
        effective_i = self.i if self.i >= 0 else length + self.i

        if effective_i < 0 or effective_i >= length:
            if self.fail_on_miss:
                raise RuntimeError(
                    f"Index: index {self.i} out of bounds for column "
                    f"{self.column!r} (length {length}, fail_on_miss=True). "
                    f"See ITL-439."
                )
            logger.warning(
                "Index: skipping packet — index %d out of bounds for column %r "
                "(length %d).",
                self.i,
                self.column,
                length,
            )
            return None

        extracted = col_val[self.i]
        nullable = self._output_nullable()
        if extracted is None and not nullable:
            raise ValueError(
                f"Index: element {self.i} of column {self.column!r} is None in"
                + f" the row with tag {tag}, but the output type"
                + f" {self._output_type} is not Optional."
            )

        old_src = data.source_info().get(self.column)
        new_src = f"{old_src}[{self.i}]" if old_src else None

        element_field = pa.field(
            self.out or self.column,
            self._element_arrow_type(
                arrow_utils.column_arrow_type(data.arrow_schema(), self.column)
            ),
            nullable=nullable,
        )
        if self.out is None:
            # Replace column in place: drop the old (with its stale type), re-add
            # it under the same name with the element's Arrow type, and restore
            # the original column order.
            column_order = list(data.keys())
            new_data = (
                data.drop(self.column)
                .with_columns(
                    column_types={self.column: element_field},
                    **{self.column: extracted},
                )
                .select(*column_order)
                .with_source_info(**{self.column: new_src})
            )
        else:
            # Add new column; original stays unchanged.
            new_data = data.with_columns(
                column_types={self.out: element_field},
                **{self.out: extracted},
            ).with_source_info(**{self.out: new_src})

        return tag, new_data

    # ------------------------------------------------------------------
    # Barrier-mode execution
    # ------------------------------------------------------------------

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        """Process the full stream packet by packet.

        Iterates ``stream.iter_data()``, applies ``_process_one`` per packet,
        and packs surviving packets back into a stream.  Skipped packets
        (out-of-bounds, ``fail_on_miss=False``) are silently dropped.

        Args:
            stream: The upstream stream.

        Returns:
            A new stream with the projection applied.  If all packets were
            skipped, an empty stream with the correct output schema is returned.

        Raises:
            RuntimeError: If ``fail_on_miss=True`` and any packet is
                out-of-bounds.
            ValueError: If an extracted element is ``None`` but the predicted
                output type is not Optional.
        """
        out_rows = []
        for tag, data in stream.iter_data():
            result = self._process_one(tag, data)
            if result is not None:
                out_rows.append(result)

        if not out_rows:
            # All packets were skipped (fail_on_miss=False).  Return an empty
            # stream with the correct output schema — this is not an error.
            # The schema is the input's Arrow schema (large-normalized, as for
            # a non-empty result) with only the projected column changed.
            tag_columns, _ = stream.keys()
            input_schema = arrow_utils.normalize_schema_to_large_types(
                stream.as_table().schema
            )
            element_field = pa.field(
                self.out or self.column,
                self._element_arrow_type(
                    arrow_utils.column_arrow_type(input_schema, self.column)
                ),
                nullable=self._output_nullable(),
            )
            if self.out is None:
                position = input_schema.get_field_index(self.column)
                output_schema = input_schema.set(position, element_field)  # pyright: ignore[reportUnknownMemberType]  # stubs leave Field unparametrized
            else:
                output_schema = input_schema.append(element_field)  # pyright: ignore[reportUnknownMemberType]  # stubs leave Field unparametrized
            return ArrowTableStream(
                output_schema.empty_table(),
                tag_columns=tag_columns,
                data_context=stream.data_context,
            )

        return self._materialize_to_stream(out_rows)

    # ------------------------------------------------------------------
    # Streaming execution
    # ------------------------------------------------------------------

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash],
    ) -> None:
        """Process packets one at a time as they arrive (true streaming).

        Each packet is processed independently via ``_process_one`` and
        forwarded immediately to the output channel — no buffering required.

        Args:
            inputs: Single-element sequence of readable channels.
            output: Channel to send transformed packets to.
            input_pipeline_hashes: Not used: this operator keeps system-tag
                names as they are, so no column is named after an input's
                pipeline hash.
        """
        try:
            async for tag, data in inputs[0]:
                result = self._process_one(tag, data)
                if result is not None:
                    await output.send(result)
        finally:
            await output.close()
