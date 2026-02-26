from abc import abstractmethod
from collections.abc import Collection
from typing import Any

from orcapod.core.static_output_pod import StaticOutputPod
from orcapod.protocols.core_protocols import ArgumentGroup, Stream
from orcapod.types import ColumnConfig, Schema


class Operator(StaticOutputPod):
    """
    Base class for all operators.
    Operators are basic pods that can be used to perform operations on streams.

    They are defined as a callable that takes a (possibly empty) collection of streams as the input
    and returns a new stream as output.
    """

    def identity_structure(self) -> Any:
        return self.__class__.__name__


class UnaryOperator(Operator):
    """
    Base class for all unary operators.
    """

    @abstractmethod
    def validate_unary_input(self, stream: Stream) -> None:
        """
        This method should be implemented by subclasses to validate the inputs to the operator.
        It takes two streams as input and raises an error if the inputs are not valid.
        """
        ...

    @abstractmethod
    def unary_static_process(self, stream: Stream) -> Stream:
        """
        This method should be implemented by subclasses to define the specific behavior of the unary operator.
        It takes one stream as input and returns a new stream as output.
        """
        ...

    @abstractmethod
    def unary_output_schema(
        self,
        stream: Stream,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """
        This method should be implemented by subclasses to return the typespecs of the input and output streams.
        It takes two streams as input and returns a tuple of typespecs.
        """
        ...

    def validate_inputs(self, *streams: Stream) -> None:
        if len(streams) != 1:
            raise ValueError("UnaryOperator requires exactly one input stream.")
        stream = streams[0]
        return self.validate_unary_input(stream)

    def static_process(self, *streams: Stream) -> Stream:
        """
        Forward method for unary operators.
        It expects exactly one stream as input.
        """
        stream = streams[0]
        return self.unary_static_process(stream)

    def output_schema(
        self,
        *streams: Stream,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        stream = streams[0]
        return self.unary_output_schema(stream, columns=columns, all_info=all_info)

    def argument_symmetry(self, streams: Collection[Stream]) -> ArgumentGroup:
        # return single stream as a tuple
        return (tuple(streams)[0],)


class BinaryOperator(Operator):
    """
    Base class for all operators.
    """

    @abstractmethod
    def validate_binary_inputs(self, left_stream: Stream, right_stream: Stream) -> None:
        """
        Check that the inputs to the binary operator are valid.
        This method is called before the forward method to ensure that the inputs are valid.
        """
        ...

    @abstractmethod
    def binary_static_process(
        self, left_stream: Stream, right_stream: Stream
    ) -> Stream:
        """
        Forward method for binary operators.
        It expects exactly two streams as input.
        """
        ...

    @abstractmethod
    def binary_output_schema(
        self,
        left_stream: Stream,
        right_stream: Stream,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]: ...

    @abstractmethod
    def is_commutative(self) -> bool:
        """
        Return True if the operator is commutative (i.e., order of inputs does not matter).
        """
        ...

    def output_schema(
        self,
        *streams: Stream,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        left_stream, right_stream = streams
        return self.binary_output_schema(
            left_stream, right_stream, columns=columns, all_info=all_info
        )

    def validate_inputs(self, *streams: Stream) -> None:
        if len(streams) != 2:
            raise ValueError("BinaryOperator requires exactly two input streams.")
        left_stream, right_stream = streams
        self.validate_binary_inputs(left_stream, right_stream)

    def argument_symmetry(self, streams: Collection[Stream]) -> ArgumentGroup:
        if self.is_commutative():
            # return as symmetric group
            return frozenset(streams)
        else:
            # return as ordered group
            return tuple(streams)


class NonZeroInputOperator(Operator):
    """
    Operators that work with at least one input stream.
    This is useful for operators that can take a variable number of (but at least one ) input streams,
    such as joins, unions, etc.
    """

    @abstractmethod
    def validate_nonzero_inputs(
        self,
        *streams: Stream,
    ) -> None:
        """
        Check that the inputs to the variable inputs operator are valid.
        This method is called before the forward method to ensure that the inputs are valid.
        """
        ...

    def validate_inputs(self, *streams: Stream) -> None:
        if len(streams) == 0:
            raise ValueError(
                f"Operator {self.__class__.__name__} requires at least one input stream."
            )
        self.validate_nonzero_inputs(*streams)
