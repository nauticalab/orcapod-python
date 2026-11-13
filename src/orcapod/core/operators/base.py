from __future__ import annotations

import asyncio
from abc import abstractmethod
from collections.abc import Collection, Sequence
from typing import TYPE_CHECKING, Any

from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import (
    ArgumentGroup,
    DataProtocol,
    StreamProtocol,
    TagProtocol,
)
from orcapod.types import ColumnConfig, ContentHash, Schema
from orcapod.utils import arrow_utils

if TYPE_CHECKING:
    import pyarrow as pa


def require_shared_arrow_type(
    operator: str, name: str, left_type: pa.DataType, right_type: pa.DataType
) -> pa.DataType:
    """Return the Arrow type a column's two input types share, or raise.

    The rule is ``arrow_utils.shared_arrow_type``; this adds the error an
    operator raises when there is no shared type. Operators call it as soon as
    they have the input tables (or datagrams), before the join, because the
    disagreement cannot be seen at graph-build time: ``output_schema()``
    returns Python types, under which ``int8`` and ``int64``, or ``string`` and
    ``large_string``, are the same.

    Args:
        operator: The operator's name, for the error message.
        name: The column's name, for the error message.
        left_type: The column's Arrow type in one input.
        right_type: The column's Arrow type in the other input.

    Returns:
        The large-normalized type both inputs share.

    Raises:
        InputValidationError: If the types still differ after normalization to
            their large variants. The message names the operator, the column and
            both input types as given.
    """
    shared = arrow_utils.shared_arrow_type(left_type, right_type)
    if shared is None:
        raise InputValidationError(
            f"{operator} inputs disagree on the Arrow type of column {name!r}: "
            + f"{left_type} in one input and {right_type} in the other. {operator} "
            + "never widens or promotes a type; cast one input explicitly so both "
            + "have the same Arrow type."
        )
    return shared


class UnaryOperator(StaticOutputOperatorPod):
    """Base class for all unary operators."""

    @abstractmethod
    def validate_unary_input(self, stream: StreamProtocol) -> None:
        """Validate the single input stream.

        Raises:
            ValueError: If the input stream is not valid for this operator.
        """
        ...

    @abstractmethod
    def unary_static_process(self, stream: StreamProtocol) -> StreamProtocol:
        """Process a single input stream and return a new output stream."""
        ...

    @abstractmethod
    def unary_output_schema(
        self,
        stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """Return the (tag, data) output schemas for the given input stream."""
        ...

    def validate_inputs(self, *streams: StreamProtocol) -> None:
        if len(streams) != 1:
            raise ValueError("UnaryOperator requires exactly one input stream.")
        stream = streams[0]
        return self.validate_unary_input(stream)

    def static_process(self, *streams: StreamProtocol) -> StreamProtocol:
        """Forward to ``unary_static_process`` with the single input stream."""
        stream = streams[0]
        return self.unary_static_process(stream)

    def output_schema(
        self,
        *streams: StreamProtocol,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        stream = streams[0]
        return self.unary_output_schema(stream, columns=columns, all_info=all_info)

    def argument_symmetry(self, streams: Collection[StreamProtocol]) -> ArgumentGroup:
        # return single stream as a tuple
        return (tuple(streams)[0],)

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash],
    ) -> None:
        """Barrier-mode: collect single input, run unary_static_process, emit.

        An input with no rows emits no rows (ITL-865).

        Args:
            inputs: The one readable input channel.
            output: Writable channel for downstream consumption.
            input_pipeline_hashes: The input stream's pipeline hash, as a
                one-element sequence. The rebuilt input reports it, so
                system-tag names equal the sync path's (ITL-906).

        Raises:
            ValueError: If ``input_pipeline_hashes`` does not have one hash per input.
        """
        try:
            if len(input_pipeline_hashes) != len(inputs):
                raise ValueError(
                    f"input_pipeline_hashes length ({len(input_pipeline_hashes)}) "
                    f"must match inputs length ({len(inputs)})"
                )
            rows = await inputs[0].collect()
            if not rows:
                # No rows in, no rows out: an operator never synthesizes a row.
                # The typed empty stream the sync path returns needs a schema the
                # channel does not carry (ITL-858), so nothing is emitted (ITL-865).
                return
            stream = self._materialize_input_stream(rows, input_pipeline_hashes[0])
            result = self.static_process(stream)
            for tag, data in result.iter_data():
                await output.send((tag, data))
        finally:
            await output.close()


class BinaryOperator(StaticOutputOperatorPod):
    """Base class for all binary operators."""

    @abstractmethod
    def validate_binary_inputs(
        self, left_stream: StreamProtocol, right_stream: StreamProtocol
    ) -> None:
        """Validate the two input streams.

        Raises:
            ValueError: If the inputs are not valid for this operator.
        """
        ...

    @abstractmethod
    def binary_static_process(
        self, left_stream: StreamProtocol, right_stream: StreamProtocol
    ) -> StreamProtocol:
        """Process two input streams and return a new output stream."""
        ...

    @abstractmethod
    def binary_output_schema(
        self,
        left_stream: StreamProtocol,
        right_stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]: ...

    @abstractmethod
    def is_commutative(self) -> bool:
        """Return True if the operator is commutative (order of inputs does not matter)."""
        ...

    def static_process(self, *streams: StreamProtocol) -> StreamProtocol:
        """Forward to ``binary_static_process`` with two input streams."""
        left_stream, right_stream = streams
        return self.binary_static_process(left_stream, right_stream)

    def output_schema(
        self,
        *streams: StreamProtocol,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        left_stream, right_stream = streams
        return self.binary_output_schema(
            left_stream, right_stream, columns=columns, all_info=all_info
        )

    def validate_inputs(self, *streams: StreamProtocol) -> None:
        if len(streams) != 2:
            raise ValueError("BinaryOperator requires exactly two input streams.")
        left_stream, right_stream = streams
        self.validate_binary_inputs(left_stream, right_stream)

    def argument_symmetry(self, streams: Collection[StreamProtocol]) -> ArgumentGroup:
        if self.is_commutative():
            # return as symmetric group
            return frozenset(streams)
        else:
            # return as ordered group
            return tuple(streams)

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash],
    ) -> None:
        """Barrier-mode: collect both inputs concurrently, run binary_static_process, emit.

        An input with no rows emits no rows, as an inner join does (ITL-865).

        Args:
            inputs: The left and right readable input channels.
            output: Writable channel for downstream consumption.
            input_pipeline_hashes: The left and right input streams' pipeline
                hashes. Each rebuilt input reports its own, so system-tag names
                and canonical input order equal the sync path's (ITL-906).

        Raises:
            ValueError: If ``input_pipeline_hashes`` does not have one hash per input.
        """
        try:
            if len(input_pipeline_hashes) != len(inputs):
                raise ValueError(
                    f"input_pipeline_hashes length ({len(input_pipeline_hashes)}) "
                    f"must match inputs length ({len(inputs)})"
                )
            left_hash, right_hash = input_pipeline_hashes
            left_rows, right_rows = await asyncio.gather(
                inputs[0].collect(), inputs[1].collect()
            )
            if not left_rows or not right_rows:
                # The one barrier binary operator, MergeJoin, is an inner join: an
                # input with no rows gives no rows, as on the sync path. The typed
                # empty stream needs a schema the channels do not carry (ITL-858),
                # so nothing is emitted (ITL-865).
                return
            left_stream = self._materialize_input_stream(left_rows, left_hash)
            right_stream = self._materialize_input_stream(right_rows, right_hash)
            result = self.static_process(left_stream, right_stream)
            for tag, data in result.iter_data():
                await output.send((tag, data))
        finally:
            await output.close()


class NonZeroInputOperator(StaticOutputOperatorPod):
    """Base class for operators that require at least one input stream.

    Useful for operators that accept a variable number of input streams,
    such as joins and unions.
    """

    @abstractmethod
    def validate_nonzero_inputs(
        self,
        *streams: StreamProtocol,
    ) -> None:
        """Validate the input streams.

        Raises:
            ValueError: If the inputs are not valid for this operator.
        """
        ...

    def validate_inputs(self, *streams: StreamProtocol) -> None:
        if len(streams) == 0:
            raise ValueError(
                f"Operator {self.__class__.__name__} requires at least one input stream."
            )
        self.validate_nonzero_inputs(*streams)
