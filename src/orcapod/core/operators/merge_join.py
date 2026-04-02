from typing import TYPE_CHECKING, Any

from orcapod.core.operators.base import BinaryOperator
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, Schema
from orcapod.utils import arrow_data_utils, arrow_utils, schema_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")


class MergeJoin(BinaryOperator):
    """
    Binary operator that joins two streams, merging colliding packet columns
    into sorted lists.

    For packet columns that exist in both streams:
    - Values are combined into a list<T> and sorted independently per column.
    - Corresponding source columns are reordered to match the sort order of
      their packet column.

    For non-colliding columns, values are kept as scalars (same as regular Join).

    Tag columns use inner join on shared tags, with union of tag schemas.

    MergeJoin is commutative: MergeJoin(A, B) produces the same result as
    MergeJoin(B, A), achieved by sorting merged values and system tag values.
    """

    @property
    def kernel_id(self) -> tuple[str, ...]:
        return (f"{self.__class__.__name__}",)

    def is_commutative(self) -> bool:
        return True

    def validate_binary_inputs(
        self, left_stream: StreamProtocol, right_stream: StreamProtocol
    ) -> None:
        _, left_packet_schema = left_stream.output_schema()
        _, right_packet_schema = right_stream.output_schema()

        # Colliding packet columns must have identical types since they are
        # merged into list[T] — both sides must contribute the same T.
        colliding_keys = set(left_packet_schema.keys()) & set(
            right_packet_schema.keys()
        )
        for key in colliding_keys:
            left_type = left_packet_schema[key]
            right_type = right_packet_schema[key]
            if left_type != right_type:
                raise InputValidationError(
                    f"Colliding packet column '{key}' has incompatible types: "
                    f"{left_type} (left) vs {right_type} (right). "
                    f"MergeJoin requires colliding columns to have identical types."
                )

        try:
            self.binary_output_schema(left_stream, right_stream)
        except InputValidationError:
            raise
        except Exception as e:
            raise InputValidationError(
                f"Input streams are not compatible for merge join: {e}"
            ) from e

    def binary_output_schema(
        self,
        left_stream: StreamProtocol,
        right_stream: StreamProtocol,
        *,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        columns_config = ColumnConfig.handle_config(columns, all_info=all_info)

        # Always get input schemas WITHOUT system tags for the base computation.
        # System tags are computed separately because the join renames them.
        left_tag_schema, left_packet_schema = left_stream.output_schema()
        right_tag_schema, right_packet_schema = right_stream.output_schema()

        # Tag schema: union of both tag schemas
        tag_schema = schema_utils.union_schemas(left_tag_schema, right_tag_schema)

        # Packet schema: colliding columns become list[T], non-colliding stay scalar
        colliding_schema = schema_utils.intersection_schemas(
            left_packet_schema, right_packet_schema
        )

        merged_packet_schema = {}
        all_packet_keys = set(left_packet_schema.keys()) | set(
            right_packet_schema.keys()
        )
        for key in all_packet_keys:
            if key in colliding_schema:
                merged_packet_schema[key] = list[colliding_schema[key]]
            elif key in left_packet_schema:
                merged_packet_schema[key] = left_packet_schema[key]
            else:
                merged_packet_schema[key] = right_packet_schema[key]

        # Add system tag columns if requested
        if columns_config.system_tags:
            system_tag_schema = self._predict_system_tag_schema(
                left_stream, right_stream
            )
            tag_schema = schema_utils.union_schemas(tag_schema, system_tag_schema)

        return tag_schema, Schema(merged_packet_schema)

    def _canonical_order(
        self, left_stream: StreamProtocol, right_stream: StreamProtocol
    ) -> list[tuple[StreamProtocol, int]]:
        """
        Determine canonical ordering of the two input streams by stable-sorting
        on pipeline_hash. Returns list of (stream, original_index) tuples in
        canonical order.
        """
        streams_with_idx = [(left_stream, 0), (right_stream, 1)]
        # Python's sorted is stable, so equal pipeline_hashes preserve input order
        return sorted(streams_with_idx, key=lambda s: s[0].pipeline_hash().to_hex())

    def _predict_system_tag_schema(
        self, left_stream: StreamProtocol, right_stream: StreamProtocol
    ) -> Schema:
        """Predict the system tag columns that the join would produce.

        Each input stream's existing system tag columns get renamed by
        appending ::{pipeline_hash}:{canonical_position}. This method
        computes those output column names without performing the join.
        """
        n_char = self.orcapod_config.system_tag_hash_n_char
        canonical = self._canonical_order(left_stream, right_stream)

        system_tag_fields: dict[str, type] = {}
        for stream, orig_idx in canonical:
            canon_pos = canonical.index((stream, orig_idx))
            stream_tag_schema, _ = stream.output_schema(columns={"system_tags": True})
            for col_name in stream_tag_schema:
                if col_name.startswith(constants.SYSTEM_TAG_PREFIX):
                    new_name = (
                        f"{col_name}{constants.BLOCK_SEPARATOR}"
                        f"{stream.pipeline_hash().to_hex(n_char)}:{canon_pos}"
                    )
                    system_tag_fields[new_name] = str
        return Schema(system_tag_fields)

    def binary_static_process(
        self, left_stream: StreamProtocol, right_stream: StreamProtocol
    ) -> StreamProtocol:
        n_char = self.orcapod_config.system_tag_hash_n_char

        # Determine canonical ordering for system tag positions
        canonical = self._canonical_order(left_stream, right_stream)

        # Get tables with source + system_tags, append system tag blocks
        tables = {}
        for stream, orig_idx in canonical:
            canon_pos = canonical.index((stream, orig_idx))
            table = stream.as_table(columns={"source": True, "system_tags": True})
            table = arrow_data_utils.append_to_system_tags(
                table, f"{stream.pipeline_hash().to_hex(n_char)}:{canon_pos}"
            )
            tables[orig_idx] = table

        left_table = tables[0]
        right_table = tables[1]

        # Determine shared tag keys for inner join
        left_tag_keys, left_packet_keys = left_stream.keys()
        right_tag_keys, right_packet_keys = right_stream.keys()
        shared_tag_keys = set(left_tag_keys) & set(right_tag_keys)

        # Find colliding packet columns
        colliding_keys = set(left_packet_keys) & set(right_packet_keys)

        # Perform inner join via Polars on shared tag keys
        # Use a common key trick to ensure cartesian product if no shared tags
        COMMON_JOIN_KEY = "_common"
        left_table = left_table.add_column(
            0, COMMON_JOIN_KEY, pa.array([0] * len(left_table))
        )
        right_table = right_table.add_column(
            0, COMMON_JOIN_KEY, pa.array([0] * len(right_table))
        )

        join_keys = list(shared_tag_keys | {COMMON_JOIN_KEY})

        # Track which columns Polars will auto-suffix with _right
        # (right-table columns that collide with left, excluding join keys)
        left_col_set = set(left_table.column_names) - {COMMON_JOIN_KEY}
        right_col_set = set(right_table.column_names) - {COMMON_JOIN_KEY}
        join_key_set = set(join_keys) - {COMMON_JOIN_KEY}
        polars_suffixed_bases = (right_col_set & left_col_set) - join_key_set

        joined = (
            pl.DataFrame(left_table)
            .join(pl.DataFrame(right_table), on=join_keys, how="inner")
            .to_arrow()
        )
        joined = joined.drop(COMMON_JOIN_KEY)

        # Process colliding packet columns: merge into sorted lists
        for col in colliding_keys:
            left_col_name = col
            right_col_name = f"{col}_right"

            left_source_col = f"{constants.SOURCE_PREFIX}{col}"
            right_source_col = f"{left_source_col}_right"

            if right_col_name not in joined.column_names:
                continue

            left_vals = joined.column(left_col_name).to_pylist()
            right_vals = joined.column(right_col_name).to_pylist()

            # Also handle corresponding source columns
            has_source = (
                left_source_col in joined.column_names
                and right_source_col in joined.column_names
            )
            if has_source:
                left_sources = joined.column(left_source_col).to_pylist()
                right_sources = joined.column(right_source_col).to_pylist()

            merged_vals = []
            merged_sources = [] if has_source else None
            for i in range(len(left_vals)):
                lv, rv = left_vals[i], right_vals[i]
                if has_source:
                    ls, rs = left_sources[i], right_sources[i]
                    # Sort by packet value, carry source along
                    pairs = sorted(zip([lv, rv], [ls, rs]), key=lambda p: p[0])
                    merged_vals.append([p[0] for p in pairs])
                    merged_sources.append([p[1] for p in pairs])
                else:
                    merged_vals.append(sorted([lv, rv]))

            # Replace the left column with merged list, drop right column
            col_idx = joined.column_names.index(left_col_name)
            joined = joined.drop(left_col_name)
            joined = joined.drop(right_col_name)

            merged_array = pa.array(merged_vals)
            joined = joined.add_column(col_idx, left_col_name, merged_array)

            if has_source:
                source_idx = joined.column_names.index(left_source_col)
                joined = joined.drop(left_source_col)
                joined = joined.drop(right_source_col)
                source_array = pa.array(merged_sources)
                joined = joined.add_column(source_idx, left_source_col, source_array)

        # Handle remaining Polars-generated _right suffixed columns
        # (only from columns we know Polars auto-suffixed, not original names)
        for base_name in polars_suffixed_bases:
            suffixed_name = f"{base_name}_right"
            if suffixed_name not in joined.column_names:
                continue  # Already handled during colliding column processing
            if base_name not in joined.column_names:
                # Left version was removed, rename right to original
                idx = joined.column_names.index(suffixed_name)
                col_data = joined.column(suffixed_name)
                joined = joined.drop(suffixed_name)
                joined = joined.add_column(idx, base_name, col_data)
            else:
                # Both versions exist, drop the right one
                joined = joined.drop(suffixed_name)

        # Sort system tag values for same-pipeline-hash streams to ensure commutativity
        joined = arrow_data_utils.sort_system_tag_values(joined)

        # Reorder: tag columns first, then packet columns
        all_tag_keys = set(left_tag_keys) | set(right_tag_keys)
        tag_cols = [c for c in joined.column_names if c in all_tag_keys]
        other_cols = [c for c in joined.column_names if c not in all_tag_keys]
        joined = joined.select(tag_cols + other_cols)

        # Rebuild schema with nullable=False to avoid spurious T | None types.
        # pa.Table.cast() is not used here because list columns may fail validation.
        new_schema = arrow_utils.make_schema_non_nullable(joined.schema)
        joined = pa.Table.from_arrays(
            [joined.column(i) for i in range(joined.num_columns)],
            schema=new_schema,
        )
        return ArrowTableStream(
            joined,
            tag_columns=tuple(all_tag_keys),
        )

    def identity_structure(self) -> Any:
        return self.__class__.__name__

    def __repr__(self) -> str:
        return "MergeJoin()"
