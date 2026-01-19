from typing import TYPE_CHECKING, Any

from orcapod.core.operators.base import BinaryOperator
from orcapod.core.streams import TableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import ColumnConfig, Stream
from orcapod.types import Schema
from orcapod.utils import schema_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")


class SemiJoin(BinaryOperator):
    """
    Binary operator that performs a semi-join between two streams.

    A semi-join returns only the entries from the left stream that have
    matching entries in the right stream, based on equality of values
    in overlapping columns (columns with the same name and compatible types).

    If there are no overlapping columns between the streams, the entire
    left stream is returned unchanged.

    The output stream preserves the schema of the left stream exactly.
    """

    def binary_static_process(
        self, left_stream: Stream, right_stream: Stream
    ) -> Stream:
        """
        Performs a semi-join between left and right streams.
        Returns entries from left stream that have matching entries in right stream.
        """
        left_tag_schema, left_packet_schema = left_stream.output_schema()
        right_tag_schema, right_packet_schema = right_stream.output_schema()

        # Find overlapping columns across all columns (tags + packets)
        left_all_typespec = schema_utils.union_typespecs(
            left_tag_schema, left_packet_schema
        )
        right_all_typespec = schema_utils.union_typespecs(
            right_tag_schema, right_packet_schema
        )

        common_keys = tuple(
            schema_utils.intersection_typespecs(
                left_all_typespec, right_all_typespec
            ).keys()
        )

        # If no overlapping columns, return the left stream unmodified
        if not common_keys:
            return left_stream

        # include source info for left stream
        left_table = left_stream.as_table(columns={"source": True})

        # Get the right table for matching
        right_table = right_stream.as_table()

        # Perform left semi join using PyArrow's built-in functionality
        semi_joined_table = left_table.join(
            right_table,
            keys=list(common_keys),
            join_type="left semi",
        )

        return TableStream(
            semi_joined_table,
            tag_columns=tuple(left_tag_schema.keys()),
            source=self,
            upstreams=(left_stream, right_stream),
        )

    def binary_output_schema(
        self,
        left_stream: Stream,
        right_stream: Stream,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        """
        Returns the output types for the semi-join operation.
        The output preserves the exact schema of the left stream.
        """
        # Semi-join preserves the left stream's schema exactly
        return left_stream.output_schema(columns=columns, all_info=all_info)

    def validate_binary_inputs(self, left_stream: Stream, right_stream: Stream) -> None:
        """
        Validates that the input streams are compatible for semi-join.
        Checks that overlapping columns have compatible types.
        """
        try:
            left_tag_typespec, left_packet_typespec = left_stream.output_schema()
            right_tag_typespec, right_packet_typespec = right_stream.output_schema()

            # Check that overlapping columns have compatible types across all columns
            left_all_typespec = schema_utils.union_typespecs(
                left_tag_typespec, left_packet_typespec
            )
            right_all_typespec = schema_utils.union_typespecs(
                right_tag_typespec, right_packet_typespec
            )

            # intersection_typespecs will raise an error if types are incompatible
            schema_utils.intersection_typespecs(left_all_typespec, right_all_typespec)

        except Exception as e:
            raise InputValidationError(
                f"Input streams are not compatible for semi-join: {e}"
            ) from e

    def identity_structure(self) -> Any:
        return self.__class__.__name__
