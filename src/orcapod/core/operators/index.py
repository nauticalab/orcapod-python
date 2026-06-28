"""Index operator — positional projection into list-typed columns."""
from __future__ import annotations

import logging
import typing
from collections.abc import Sequence
from typing import Any

from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.core.operators.base import UnaryOperator
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, ContentHash, Schema

logger = logging.getLogger(__name__)

_MISSING = object()


class Index(UnaryOperator):
    """Extract an element from a list-typed data column by position.

    The list length is per-packet data, so bounds are not checked at build
    time.  Out-of-bounds access causes the packet to be skipped (or an error
    when ``fail_on_miss=True``).  Negative indices follow Python semantics
    (``-1`` is the last element).

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
        self._output_type: type = _MISSING  # type: ignore[assignment]
        self._mode: str = ""
        super().__init__(**kwargs)

    def identity_structure(self) -> Any:
        return (self.__class__.__name__, self.column, self.i, self.out)

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
        _, data_schema = stream.output_schema()
        data_columns = list(data_schema.keys())

        if self.column not in data_columns:
            raise InputValidationError(
                f"Index: column {self.column!r} not found in data schema. "
                f"Available data columns: {data_columns}"
            )

        if self.out is not None and self.out in data_columns:
            raise InputValidationError(
                f"Index: out column {self.out!r} already exists in data schema. "
                f"Choose a different name to avoid clobbering existing data."
            )

        col_type = data_schema[self.column]
        origin = typing.get_origin(col_type)

        if origin is list:
            args = typing.get_args(col_type)
            self._output_type = args[0] if args else Any  # type: ignore[assignment]
            self._mode = "list"
        else:
            from orcapod.contexts import get_default_type_converter
            converter = get_default_type_converter()
            registry = getattr(converter, "_logical_type_registry", None)
            if registry is None:
                raise InputValidationError(
                    f"Index: cannot resolve logical type for column {self.column!r}."
                )
            lt = registry.get_by_python_type(col_type)
            if lt is None:
                raise InputValidationError(
                    f"Index: column {self.column!r} has type {col_type!r} which is "
                    f"not a supported index target (not list[T] and no registered "
                    f"logical type)."
                )
            self._output_type = lt.index_element()
            self._mode = "extension"

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
        tag_schema, data_schema = stream.output_schema(columns=columns, all_info=all_info)
        data_dict = dict(data_schema)

        if self.out is None:
            data_dict[self.column] = self._output_type
        else:
            data_dict[self.out] = self._output_type

        return tag_schema, Schema(data_dict)

    # ------------------------------------------------------------------
    # Barrier-mode execution
    # ------------------------------------------------------------------

    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        """Process the full stream in barrier mode.

        Builds the output table from Python-level dicts to avoid type conflicts
        when the projected column type differs from the original (e.g. list[int]
        → int).

        Args:
            stream: The fully materialised input stream.

        Returns:
            A new stream with the projection applied.

        Raises:
            RuntimeError: If ``fail_on_miss=True`` and any packet is
                out-of-bounds.
            ValueError: If all packets were skipped (empty output).
        """
        tag_columns, data_columns = stream.keys()
        src_col = f"{constants.SOURCE_PREFIX}{self.column}"
        out_col = self.out if self.out is not None else self.column
        out_src_col = f"{constants.SOURCE_PREFIX}{out_col}"

        full_table = stream.as_table(columns={"source": True, "system_tags": True})
        all_rows = stream.data_context.type_converter.arrow_table_to_python_dicts(full_table)

        skipped_count = 0
        out_rows: list[dict[str, Any]] = []

        for row in all_rows:
            col_val = row[self.column]
            length = len(col_val)
            effective_i = self.i if self.i >= 0 else length + self.i

            if effective_i < 0 or effective_i >= length:
                skipped_count += 1
                continue

            extracted = col_val[self.i]

            old_src = row.get(src_col)
            new_src = f"{old_src}[{self.i}]" if old_src else None

            new_row = dict(row)
            if self.out is None:
                new_row[self.column] = extracted
                new_row[src_col] = new_src
            else:
                new_row[self.out] = extracted
                new_row[out_src_col] = new_src

            out_rows.append(new_row)

        if skipped_count:
            if self.fail_on_miss:
                raise RuntimeError(
                    f"Index: {skipped_count} packet(s) out-of-bounds at index "
                    f"{self.i} in column {self.column!r} (fail_on_miss=True). "
                    f"See ITL-439."
                )
            logger.warning(
                "Index: %d packet(s) skipped — index %d out of bounds in column %r.",
                skipped_count,
                self.i,
                self.column,
            )

        if not out_rows:
            raise ValueError(
                f"Index operator produced an empty stream: all packets were skipped "
                f"(index {self.i} out of bounds for every packet in column "
                f"{self.column!r})."
            )

        # Build source_info from first row (all rows share same source tokens)
        first = out_rows[0]
        source_info = {}
        for col in data_columns:
            si_key = f"{constants.SOURCE_PREFIX}{col}"
            if si_key in first:
                source_info[col] = first[si_key]
        if self.out is not None and out_src_col in first:
            source_info[self.out] = first[out_src_col]

        # Strip source/system-tag prefixes from data rows
        sys_prefix = constants.SYSTEM_TAG_PREFIX
        src_prefix = constants.SOURCE_PREFIX
        data_rows = [
            {k: v for k, v in row.items()
             if not k.startswith(src_prefix) and not k.startswith(sys_prefix)}
            for row in out_rows
        ]

        # Build combined schema (tags + data with updated column type)
        tag_schema, data_schema = stream.output_schema()
        out_data_schema = dict(data_schema)
        if self.out is None:
            out_data_schema[self.column] = self._output_type
        else:
            out_data_schema[self.out] = self._output_type
        combined_schema = {**dict(tag_schema), **out_data_schema}

        output_table = stream.data_context.type_converter.python_dicts_to_arrow_table(
            data_rows,
            python_schema=combined_schema,
        )

        return ArrowTableStream(
            output_table,
            tag_columns=tag_columns,
            source_info=source_info,
        )

    # ------------------------------------------------------------------
    # Streaming execution
    # ------------------------------------------------------------------

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash] | None = None,
    ) -> None:
        """Barrier-mode: collect all input, run unary_static_process, emit.

        Args:
            inputs: Single-element sequence of readable channels.
            output: Channel to send transformed packets to.
            input_pipeline_hashes: Ignored; present for protocol compliance.
        """
        try:
            rows = await inputs[0].collect()
            if rows:
                stream = self._materialize_to_stream(rows)
                result = self.unary_static_process(stream)
                for tag, data in result.iter_data():
                    await output.send((tag, data))
        finally:
            await output.close()
