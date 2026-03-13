from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.core.operators.base import BinaryOperator
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import PacketProtocol, StreamProtocol, TagProtocol
from orcapod.types import ColumnConfig, Schema
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
        self, left_stream: StreamProtocol, right_stream: StreamProtocol
    ) -> StreamProtocol:
        """
        Performs a semi-join between left and right streams.
        Returns entries from left stream that have matching entries in right stream.
        """
        left_tag_schema, left_packet_schema = left_stream.output_schema()
        right_tag_schema, right_packet_schema = right_stream.output_schema()

        # Find overlapping columns across all columns (tags + packets)
        left_all_schema = schema_utils.union_schemas(
            left_tag_schema, left_packet_schema
        )
        right_all_schema = schema_utils.union_schemas(
            right_tag_schema, right_packet_schema
        )

        common_keys = tuple(
            schema_utils.intersection_schemas(left_all_schema, right_all_schema).keys()
        )

        # If no overlapping columns, return the left stream unmodified
        if not common_keys:
            return left_stream

        # include source info and system tags for left stream
        left_table = left_stream.as_table(columns={"source": True, "system_tags": True})

        # Get the right table for matching
        right_table = right_stream.as_table()

        # Perform left semi join using PyArrow's built-in functionality
        semi_joined_table = left_table.join(
            right_table,
            keys=list(common_keys),
            join_type="left semi",
        )

        return ArrowTableStream(
            semi_joined_table,
            tag_columns=tuple(left_tag_schema.keys()),
        )

    def binary_output_schema(
        self,
        left_stream: StreamProtocol,
        right_stream: StreamProtocol,
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

    def validate_binary_inputs(
        self, left_stream: StreamProtocol, right_stream: StreamProtocol
    ) -> None:
        """
        Validates that the input streams are compatible for semi-join.
        Checks that overlapping columns have compatible types.

        Stores the common keys so that ``async_execute`` can use them
        to determine the correct empty-right behavior without data.
        """
        try:
            left_tag_schema, left_packet_schema = left_stream.output_schema()
            right_tag_schema, right_packet_schema = right_stream.output_schema()

            # Check that overlapping columns have compatible types across all columns
            left_all_schema = schema_utils.union_schemas(
                left_tag_schema, left_packet_schema
            )
            right_all_schema = schema_utils.union_schemas(
                right_tag_schema, right_packet_schema
            )

            # intersection_schemas will raise an error if types are incompatible
            common = schema_utils.intersection_schemas(
                left_all_schema, right_all_schema
            )
            self._validated_common_keys: tuple[str, ...] = tuple(common.keys())

        except Exception as e:
            raise InputValidationError(
                f"Input streams are not compatible for semi-join: {e}"
            ) from e

    def is_commutative(self) -> bool:
        return False

    def _common_keys_from_schema(self) -> tuple[str, ...]:
        """Return the common keys computed during input validation.

        Falls back to an empty tuple if validation hasn't been called
        (shouldn't happen in normal pipeline execution).
        """
        return getattr(self, "_validated_common_keys", ())

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
        **kwargs: Any,
    ) -> None:
        """Build-probe: collect right input, then stream left through a hash lookup.

        Phase 1 — Build: collect all rows from the right (filter) channel and
        index them by the common-key values.
        Phase 2 — Probe: stream left rows one at a time; for each row whose
        common-key values appear in the right-side index, emit immediately.

        Falls back to barrier mode when the right input is empty (schema
        cannot be inferred from data) or when there are no common keys.
        """
        try:
            left_ch, right_ch = inputs[0], inputs[1]

            # Phase 1: Build right-side lookup
            right_rows = await right_ch.collect()

            if not right_rows:
                # Empty right: determine common keys from the validated
                # input schemas (set during __init__) to match sync semantics.
                # Common keys exist → empty result; no common keys → pass left through.
                common = self._common_keys_from_schema()
                if common:
                    # Drain left channel (discard) — result is empty
                    await left_ch.collect()
                    return
                # No common keys — pass all left rows through unchanged
                async for tag, packet in left_ch:
                    await output.send((tag, packet))
                return

            # Determine right-side keys from first row
            right_tag_keys = set(right_rows[0][0].keys())
            right_pkt_keys = set(right_rows[0][1].keys())
            right_all_keys = right_tag_keys | right_pkt_keys

            # Phase 2: Probe — stream left rows
            common_keys: tuple[str, ...] | None = None
            right_lookup: set[tuple] | None = None

            async for tag, packet in left_ch:
                if common_keys is None:
                    # First left row — determine common keys and build index
                    left_tag_keys = set(tag.keys())
                    left_pkt_keys = set(packet.keys())
                    left_all_keys = left_tag_keys | left_pkt_keys
                    common_keys = tuple(sorted(left_all_keys & right_all_keys))

                    if not common_keys:
                        # No common keys — pass all left rows through
                        await output.send((tag, packet))
                        async for t, p in left_ch:
                            await output.send((t, p))
                        return

                    # Build right-side lookup
                    right_lookup = set()
                    for rt, rp in right_rows:
                        rd = rt.as_dict()
                        rd.update(rp.as_dict())
                        right_lookup.add(tuple(rd[k] for k in common_keys))

                # Probe
                ld = tag.as_dict()
                ld.update(packet.as_dict())
                if tuple(ld[k] for k in common_keys) in right_lookup:  # type: ignore[arg-type]
                    await output.send((tag, packet))
        finally:
            await output.close()

    def identity_structure(self) -> Any:
        return self.__class__.__name__
