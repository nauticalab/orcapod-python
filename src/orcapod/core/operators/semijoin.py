import math
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, cast

from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.core.operators.base import BinaryOperator, require_shared_arrow_type
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import DataProtocol, StreamProtocol, TagProtocol
from orcapod.types import ColumnConfig, ContentHash, Schema
from orcapod.utils import arrow_utils, schema_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
    import pyarrow.compute as pc
else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")
    pl = LazyModule("polars")


# The frozen form of a NaN key value: equal to itself, as NaN is in Polars.
_NAN = object()


def _freeze(value: object) -> object:
    """Return a key value (from ``as_py``) in a hashable form.

    Structs (dicts) become tuples of their items in field order, lists become
    tuples, and NaN becomes one shared sentinel. Both inputs' key types are
    checked equal first, so frozen values of different types never meet.

    Args:
        value: A Python value of an Arrow key column.

    Returns:
        The hashable form of ``value``.
    """
    if isinstance(value, float) and math.isnan(value):
        return _NAN
    if isinstance(value, dict):
        items = cast("dict[str, object]", value)
        return tuple((name, _freeze(item)) for name, item in items.items())
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in cast("Sequence[object]", value))
    return value


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
        left_tag_schema, left_data_schema = left_stream.output_schema()
        right_tag_schema, right_data_schema = right_stream.output_schema()

        # Find overlapping columns across all columns (tags + data)
        left_all_schema = schema_utils.union_schemas(left_tag_schema, left_data_schema)
        right_all_schema = schema_utils.union_schemas(
            right_tag_schema, right_data_schema
        )

        common_keys = tuple(
            schema_utils.intersection_schemas(left_all_schema, right_all_schema).keys()
        )

        # If no overlapping columns, return the left stream unmodified
        if not common_keys:
            return left_stream

        # include source info and system tags for left stream
        left_table = left_stream.as_table(columns={"source": True, "system_tags": True})
        right_table = right_stream.as_table()

        # A key must have one Arrow type on both sides (up to large layouts);
        # otherwise Polars would silently unify, e.g., int8 with int16.
        for key in common_keys:
            _ = require_shared_arrow_type(
                "SemiJoin",
                key,
                arrow_utils.column_arrow_type(left_table.schema, key),
                arrow_utils.column_arrow_type(right_table.schema, key),
            )

        # Only the right side's keys take part. A null key never matches:
        # Polars already applies that to scalar keys but treats a null struct or
        # list as equal to another, so drop right rows with a null key.
        right_keys = right_table.select(list(common_keys))
        valid = right_keys.column(0).is_valid()
        for index in range(1, right_keys.num_columns):
            valid = pc.and_(valid, right_keys.column(index).is_valid())
        right_keys = right_keys.filter(valid)

        # Polars, unlike Arrow's hash join, accepts nested (list, struct,
        # struct-backed logical) columns as keys and as passed-through columns,
        # and matches a string key against a large_string one.
        semi_joined_table = (
            pl.DataFrame(left_table)
            .join(
                pl.DataFrame(right_keys),
                on=list(common_keys),
                how="semi",
                maintain_order="left",
            )
            .to_arrow()
        )
        # The round-trip yields large layouts, nullable fields at every depth and
        # Polars' own types; give every column back the left input's exact field
        # (type, top-level and nested nullability, logical type).
        semi_joined_table = semi_joined_table.cast(left_table.schema)

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
            left_tag_schema, left_data_schema = left_stream.output_schema()
            right_tag_schema, right_data_schema = right_stream.output_schema()

            # Check that overlapping columns have compatible types across all columns
            left_all_schema = schema_utils.union_schemas(
                left_tag_schema, left_data_schema
            )
            right_all_schema = schema_utils.union_schemas(
                right_tag_schema, right_data_schema
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
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
        output: WritableChannel[tuple[TagProtocol, DataProtocol]],
        *,
        input_pipeline_hashes: Sequence[ContentHash],
    ) -> None:
        """Build-probe: collect right input, then stream left through a hash lookup.

        Phase 1 — Build: collect all rows from the right (filter) channel and
        index them by the common-key values.
        Phase 2 — Probe: stream left rows one at a time; for each row whose
        common-key values appear in the right-side index, emit immediately.

        Falls back to barrier mode when the right input is empty (schema
        cannot be inferred from data) or when there are no common keys.

        Key types are checked (``require_shared_arrow_type``) on the first row
        of each input, since channels carry no schema. When either input is
        empty there is nothing to check, so a key type mismatch the sync path
        reports gives an empty result here (as in ``Join``'s async path).

        Args:
            inputs: The left and right readable input channels.
            output: Writable channel for downstream consumption.
            input_pipeline_hashes: Not used: the left rows pass through with
                their system-tag names, so no column is named after an
                input's pipeline hash.
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
                async for tag, data in left_ch:
                    await output.send((tag, data))
                return

            # Determine right-side keys from first row
            right_tag_keys = set(right_rows[0][0].keys())
            right_pkt_keys = set(right_rows[0][1].keys())
            right_all_keys = right_tag_keys | right_pkt_keys

            # Phase 2: Probe — stream left rows
            common_keys: tuple[str, ...] | None = None
            right_lookup: set[tuple[object, ...]] = set()

            async for tag, data in left_ch:
                if common_keys is None:
                    # First left row — determine common keys and build index
                    left_tag_keys = set(tag.keys())
                    left_pkt_keys = set(data.keys())
                    left_all_keys = left_tag_keys | left_pkt_keys
                    common_keys = tuple(sorted(left_all_keys & right_all_keys))

                    if not common_keys:
                        # No common keys — pass all left rows through
                        await output.send((tag, data))
                        async for t, p in left_ch:
                            await output.send((t, p))
                        return

                    right_tag, right_data = right_rows[0]
                    for key in common_keys:
                        _ = require_shared_arrow_type(
                            "SemiJoin",
                            key,
                            self._key_arrow_type(tag, data, key),
                            self._key_arrow_type(right_tag, right_data, key),
                        )

                    # Build right-side lookup; a row with a null key never matches.
                    for rt, rp in right_rows:
                        right_key = self._match_key(rt, rp, common_keys)
                        if right_key is not None:
                            right_lookup.add(right_key)

                # Probe
                left_key = self._match_key(tag, data, common_keys)
                if left_key is not None and left_key in right_lookup:
                    await output.send((tag, data))
        finally:
            await output.close()

    @staticmethod
    def _key_table(tag: TagProtocol, data: DataProtocol, key: str) -> "pa.Table":
        """Return the one-row Arrow table, the tag's or the data's, holding ``key``."""
        table = tag.as_table()
        return table if key in table.column_names else data.as_table()

    @staticmethod
    def _key_arrow_type(
        tag: TagProtocol, data: DataProtocol, key: str
    ) -> "pa.DataType":
        """Return the Arrow type of column ``key`` in a row's tag or data."""
        table = SemiJoin._key_table(tag, data, key)
        return arrow_utils.column_arrow_type(table.schema, key)

    @staticmethod
    def _match_key(
        tag: TagProtocol, data: DataProtocol, keys: Sequence[str]
    ) -> tuple[object, ...] | None:
        """Return a row's hashable key for the async lookup, or ``None`` if null.

        Values come from the row's Arrow columns (a logical column's storage),
        not from ``as_dict()``, and are frozen so that struct and list keys are
        hashable. The comparison matches the sync path's Polars join: a row
        whose key has a top-level null never matches, nulls inside a struct or
        list compare equal, and NaN equals NaN.

        Args:
            tag: The row's tag.
            data: The row's data.
            keys: The common key columns.

        Returns:
            The frozen key values in ``keys`` order, or ``None`` when any of
            them is null.
        """
        values: list[object] = []
        for key in keys:
            scalar = SemiJoin._key_table(tag, data, key).column(key)[0]
            if isinstance(scalar, pa.ExtensionScalar):
                scalar = scalar.value
            value: object = scalar.as_py() if scalar is not None else None
            if value is None:
                return None
            values.append(_freeze(value))
        return tuple(values)

    def identity_structure(self) -> Any:
        return self.__class__.__name__
