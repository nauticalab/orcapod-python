"""Index operator — positional projection into list-typed columns."""
from __future__ import annotations

import logging
import typing
from collections.abc import Sequence
from typing import Any

from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.core.operators.base import UnaryOperator
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol
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

        Args:
            stream: The fully materialised input stream.

        Returns:
            A new stream with the projection applied.

        Raises:
            RuntimeError: If ``fail_on_miss=True`` and any packet is
                out-of-bounds.
            ValueError: If all packets were skipped (empty output).
        """
        out_rows: list[tuple[Any, Any]] = []
        skipped_count = 0

        for tag, data in stream.iter_data():
            col_val = data[self.column]
            length = len(col_val)
            effective_i = self.i if self.i >= 0 else length + self.i

            if effective_i < 0 or effective_i >= length:
                skipped_count += 1
                continue

            extracted = col_val[self.i]

            src_token = data.source_info().get(self.column) or ""
            new_src = f"{src_token}[{self.i}]" if src_token else None

            if self.out is None:
                new_data = (
                    data.update(**{self.column: extracted})
                        .with_source_info(**{self.column: new_src})
                )
            else:
                new_data = (
                    data.with_columns(
                        column_types={self.out: self._output_type},
                        **{self.out: extracted},
                    ).with_source_info(**{self.out: new_src})
                )

            out_rows.append((tag, new_data))

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

        return self._materialize_to_stream(out_rows)

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
        """Process packets one at a time as they arrive.

        Args:
            inputs: Single-element sequence of readable channels.
            output: Channel to send transformed packets to.
            input_pipeline_hashes: Ignored; present for protocol compliance.
        """
        try:
            async for tag, data in inputs[0]:
                col_val = data[self.column]
                length = len(col_val)
                effective_i = self.i if self.i >= 0 else length + self.i

                if effective_i < 0 or effective_i >= length:
                    if self.fail_on_miss:
                        raise RuntimeError(
                            f"Index: index {self.i} out of bounds for column "
                            f"{self.column!r} (length {length}, "
                            f"fail_on_miss=True). See ITL-439."
                        )
                    logger.warning(
                        "Index: skipping packet — index %d out of bounds for "
                        "column %r (length %d).",
                        self.i,
                        self.column,
                        length,
                    )
                    continue

                extracted = col_val[self.i]

                src_token = data.source_info().get(self.column) or ""
                new_src = f"{src_token}[{self.i}]" if src_token else None

                if self.out is None:
                    new_data = (
                        data.update(**{self.column: extracted})
                            .with_source_info(**{self.column: new_src})
                    )
                else:
                    new_data = (
                        data.with_columns(
                            column_types={self.out: self._output_type},
                            **{self.out: extracted},
                        ).with_source_info(**{self.out: new_src})
                    )

                await output.send((tag, new_data))
        finally:
            await output.close()
