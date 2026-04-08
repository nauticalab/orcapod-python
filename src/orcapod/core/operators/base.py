from __future__ import annotations

import asyncio
from abc import abstractmethod
from collections.abc import Collection, Sequence
from typing import Any

from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.core.operators.static_output_pod import StaticOutputOperatorPod
from orcapod.protocols.core_protocols import (
    ArgumentGroup,
    PacketProtocol,
    StreamProtocol,
    TagProtocol,
)
from orcapod.types import ColumnConfig, ContentHash, Schema


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
        """Return the (tag, packet) output schemas for the given input stream."""
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
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash] | None = None,
    ) -> None:
        """Barrier-mode: collect single input, run unary_static_process, emit."""
        try:
            rows = await inputs[0].collect()
            stream = self._materialize_to_stream(rows)
            result = self.static_process(stream)
            for tag, packet in result.iter_packets():
                await output.send((tag, packet))
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
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash] | None = None,
    ) -> None:
        """Barrier-mode: collect both inputs concurrently, run binary_static_process, emit."""
        try:
            left_rows, right_rows = await asyncio.gather(
                inputs[0].collect(), inputs[1].collect()
            )
            left_stream = self._materialize_to_stream(left_rows)
            right_stream = self._materialize_to_stream(right_rows)
            result = self.static_process(left_stream, right_stream)
            for tag, packet in result.iter_packets():
                await output.send((tag, packet))
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
