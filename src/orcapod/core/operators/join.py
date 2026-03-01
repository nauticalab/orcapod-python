from collections.abc import Collection
from typing import TYPE_CHECKING, Any

from orcapod.core.operators.base import NonZeroInputOperator
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import ArgumentGroup, StreamProtocol
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig, Schema
from orcapod.utils import arrow_data_utils, schema_utils
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import polars as pl
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")


class Join(NonZeroInputOperator):
    @property
    def kernel_id(self) -> tuple[str, ...]:
        """
        Returns a unique identifier for the kernel.
        This is used to identify the kernel in the computational graph.
        """
        return (f"{self.__class__.__name__}",)

    def validate_nonzero_inputs(self, *streams: StreamProtocol) -> None:
        try:
            self.output_schema(*streams)
        except Exception as e:
            # raise InputValidationError(f"Input streams are not compatible: {e}") from e
            raise e

    def order_input_streams(self, *streams: StreamProtocol) -> list[StreamProtocol]:
        # Canonically order by pipeline_hash for deterministic operation.
        # pipeline_hash is structure-only, so streams with the same schema+topology
        # get the same ordering regardless of data content.
        return sorted(streams, key=lambda s: s.pipeline_hash().to_hex())

    def argument_symmetry(self, streams: Collection) -> ArgumentGroup:
        return frozenset(streams)

    def output_schema(
        self,
        *streams: StreamProtocol,
        columns: ColumnConfig | dict[str, Any] | None = None,
        all_info: bool = False,
    ) -> tuple[Schema, Schema]:
        columns_config = ColumnConfig.handle_config(columns, all_info=all_info)

        if len(streams) == 1:
            return streams[0].output_schema(columns=columns, all_info=all_info)

        # Always get input schemas WITHOUT system tags for the base computation.
        # System tags are computed separately because the join renames them.
        stream = streams[0]
        tag_schema, packet_schema = stream.output_schema()
        for other_stream in streams[1:]:
            other_tag_schema, other_packet_schema = other_stream.output_schema()
            tag_schema = schema_utils.union_schemas(tag_schema, other_tag_schema)
            intersection_packet_schema = schema_utils.intersection_schemas(
                packet_schema, other_packet_schema
            )
            packet_schema = schema_utils.union_schemas(
                packet_schema, other_packet_schema
            )
            if intersection_packet_schema:
                raise InputValidationError(
                    f"Packets should not have overlapping keys, but {packet_schema.keys()} found in {stream} and {other_stream}."
                )

        # Add system tag columns if requested
        if columns_config.system_tags:
            system_tag_schema = self._predict_system_tag_schema(*streams)
            tag_schema = schema_utils.union_schemas(tag_schema, system_tag_schema)

        return tag_schema, packet_schema

    def _predict_system_tag_schema(self, *streams: StreamProtocol) -> Schema:
        """Predict the system tag columns that the join would produce.

        Each input stream's existing system tag columns get renamed by
        appending ::{pipeline_hash}:{canonical_position}. This method
        computes those output column names without performing the join.
        """
        n_char = self.orcapod_config.system_tag_hash_n_char
        ordered_streams = self.order_input_streams(*streams)

        system_tag_fields: dict[str, type] = {}
        for idx, stream in enumerate(ordered_streams):
            stream_tag_schema, _ = stream.output_schema(columns={"system_tags": True})
            for col_name in stream_tag_schema:
                if col_name.startswith(constants.SYSTEM_TAG_PREFIX):
                    new_name = (
                        f"{col_name}{constants.BLOCK_SEPARATOR}"
                        f"{stream.pipeline_hash().to_hex(n_char)}:{idx}"
                    )
                    system_tag_fields[new_name] = str
        return Schema(system_tag_fields)

    def static_process(self, *streams: StreamProtocol) -> StreamProtocol:
        """
        Joins two streams together based on their tags.
        The resulting stream will contain all the tags from both streams.
        """
        if len(streams) == 1:
            return streams[0]

        # Canonically order streams by pipeline_hash for deterministic
        # system tag column names regardless of input order (Join is commutative)
        streams = self.order_input_streams(*streams)

        COMMON_JOIN_KEY = "_common"

        n_char = self.orcapod_config.system_tag_hash_n_char

        stream = streams[0]

        tag_keys, _ = [set(k) for k in stream.keys()]
        table = stream.as_table(columns={"source": True, "system_tags": True})
        # trick to get cartesian product
        table = table.add_column(0, COMMON_JOIN_KEY, pa.array([0] * len(table)))
        table = arrow_data_utils.append_to_system_tags(
            table,
            f"{stream.pipeline_hash().to_hex(n_char)}:0",
        )

        for idx, next_stream in enumerate(streams[1:], start=1):
            next_tag_keys, _ = next_stream.keys()
            next_table = next_stream.as_table(
                columns={"source": True, "system_tags": True}
            )
            next_table = arrow_data_utils.append_to_system_tags(
                next_table,
                f"{next_stream.pipeline_hash().to_hex(n_char)}:{idx}",
            )
            # trick to ensure that there will always be at least one shared key
            # this ensure that no overlap in keys lead to full caretesian product
            next_table = next_table.add_column(
                0, COMMON_JOIN_KEY, pa.array([0] * len(next_table))
            )
            common_tag_keys = tag_keys.intersection(next_tag_keys)
            common_tag_keys.add(COMMON_JOIN_KEY)

            table = (
                pl.DataFrame(table)
                .join(pl.DataFrame(next_table), on=list(common_tag_keys), how="inner")
                .to_arrow()
            )

            tag_keys.update(next_tag_keys)

        # reorder columns to bring tag columns to the front
        # TODO: come up with a better algorithm
        table = table.drop(COMMON_JOIN_KEY)

        # Sort system tag values for same-pipeline-hash streams to ensure commutativity
        table = arrow_data_utils.sort_system_tag_values(table)

        reordered_columns = [col for col in table.column_names if col in tag_keys]
        reordered_columns += [col for col in table.column_names if col not in tag_keys]

        return ArrowTableStream(
            table.select(reordered_columns),
            tag_columns=tuple(tag_keys),
        )

    def identity_structure(self) -> Any:
        return self.__class__.__name__

    def __repr__(self) -> str:
        return "Join()"
