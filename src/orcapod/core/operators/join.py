import asyncio
from collections.abc import Collection, Sequence
from typing import TYPE_CHECKING, Any

from orcapod.channels import ReadableChannel, WritableChannel
from orcapod.core.operators.base import NonZeroInputOperator
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import (
    ArgumentGroup,
    PacketProtocol,
    StreamProtocol,
    TagProtocol,
)
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
        ordered_streams = self.order_input_streams(*streams)

        COMMON_JOIN_KEY = "_common"

        n_char = self.orcapod_config.system_tag_hash_n_char

        stream = ordered_streams[0]

        tag_keys, _ = [set(k) for k in stream.keys()]
        table = stream.as_table(columns={"source": True, "system_tags": True})
        # trick to get cartesian product
        table = table.add_column(0, COMMON_JOIN_KEY, pa.array([0] * len(table)))
        table = arrow_data_utils.append_to_system_tags(
            table,
            f"{stream.pipeline_hash().to_hex(n_char)}:0",
        )

        for idx, next_stream in enumerate(ordered_streams[1:], start=1):
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

    async def async_execute(
        self,
        inputs: Sequence[ReadableChannel[tuple[TagProtocol, PacketProtocol]]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
    ) -> None:
        """Async join with single-input passthrough and symmetric hash join.

        Single input: streams through directly without any buffering.
        Two inputs: symmetric hash join — each arriving row is immediately
        probed against the opposite side's buffer, emitting matches
        without waiting for both inputs to finish.
        Three or more inputs: collects all inputs concurrently, then
        delegates to ``static_process`` for the Polars N-way join.
        """
        try:
            if len(inputs) == 1:
                async for tag, packet in inputs[0]:
                    await output.send((tag, packet))
                return

            if len(inputs) == 2:
                await self._symmetric_hash_join(inputs[0], inputs[1], output)
                return

            # N > 2: concurrent collection + static_process
            all_rows = await asyncio.gather(*(ch.collect() for ch in inputs))
            streams = [self._materialize_to_stream(rows) for rows in all_rows]
            result = self.static_process(*streams)
            for tag, packet in result.iter_packets():
                await output.send((tag, packet))
        finally:
            await output.close()

    async def _symmetric_hash_join(
        self,
        left_ch: ReadableChannel[tuple[TagProtocol, PacketProtocol]],
        right_ch: ReadableChannel[tuple[TagProtocol, PacketProtocol]],
        output: WritableChannel[tuple[TagProtocol, PacketProtocol]],
    ) -> None:
        """Symmetric hash join for two inputs.

        Both sides are read concurrently via a merged queue.  Each
        arriving row is added to its side's index and immediately probed
        against the opposite side.  Matched rows are emitted to
        ``output`` as soon as found, so downstream consumers can begin
        work before either input is fully consumed.

        For correct system-tag column naming the implementation falls
        back to ``static_process`` (which requires materialised streams)
        when the total result set is ready.
        """
        from orcapod.core.datagrams import Packet, Tag

        # Merge both channels into one tagged queue
        _SENTINEL = object()
        queue: asyncio.Queue = asyncio.Queue()

        async def _drain(
            ch: ReadableChannel[tuple[TagProtocol, PacketProtocol]],
            side: int,
        ) -> None:
            async for item in ch:
                await queue.put((side, item))
            await queue.put((side, _SENTINEL))

        async with asyncio.TaskGroup() as tg:
            tg.create_task(_drain(left_ch, 0))
            tg.create_task(_drain(right_ch, 1))

            # -- process items as they arrive --
            # buffers[i] holds all rows seen so far from input i
            buffers: list[list[tuple[TagProtocol, PacketProtocol]]] = [[], []]
            # indexes[i] maps shared-key tuple → list of indices into buffers[i]
            indexes: list[dict[tuple, list[int]]] = [{}, {}]

            shared_keys: tuple[str, ...] | None = None
            needs_reindex = False
            closed_count = 0

            while closed_count < 2:
                side, item = await queue.get()

                if item is _SENTINEL:
                    closed_count += 1
                    continue

                tag, pkt = item
                other = 1 - side

                # Determine shared tag keys once we have rows from both sides
                if shared_keys is None:
                    if not buffers[other]:
                        # Other side empty — just buffer this row for later
                        buffers[side].append((tag, pkt))
                        continue

                    # We have data from both sides; compute shared keys
                    this_keys = set(tag.keys())
                    other_keys = set(buffers[other][0][0].keys())
                    shared_keys = tuple(sorted(this_keys & other_keys))
                    needs_reindex = True

                # One-time re-index of all rows buffered before shared_keys
                if needs_reindex:
                    needs_reindex = False
                    for buf_side in (0, 1):
                        for j, (bt, _bp) in enumerate(buffers[buf_side]):
                            btd = bt.as_dict()
                            k = (
                                tuple(btd[sk] for sk in shared_keys)
                                if shared_keys
                                else (0,)
                            )
                            indexes[buf_side].setdefault(k, []).append(j)

                    # Emit matches for all already-buffered rows across sides
                    for li, (lt, lp) in enumerate(buffers[0]):
                        ltd = lt.as_dict()
                        lk = (
                            tuple(ltd[sk] for sk in shared_keys)
                            if shared_keys
                            else (0,)
                        )
                        for ri in indexes[1].get(lk, []):
                            rt, rp = buffers[1][ri]
                            await output.send(
                                self._merge_row_pair(lt, lp, rt, rp, shared_keys)
                            )

                # Index the new row
                td = tag.as_dict()
                key = (
                    tuple(td[sk] for sk in shared_keys) if shared_keys else (0,)
                )
                row_idx = len(buffers[side])
                buffers[side].append((tag, pkt))
                indexes[side].setdefault(key, []).append(row_idx)

                # Probe the opposite buffer for matches
                matching_indices = indexes[other].get(key, [])
                for mi in matching_indices:
                    other_tag, other_pkt = buffers[other][mi]
                    if side == 0:
                        merged = self._merge_row_pair(
                            tag, pkt, other_tag, other_pkt, shared_keys
                        )
                    else:
                        merged = self._merge_row_pair(
                            other_tag, other_pkt, tag, pkt, shared_keys
                        )
                    await output.send(merged)

    @staticmethod
    def _merge_row_pair(
        left_tag: TagProtocol,
        left_pkt: PacketProtocol,
        right_tag: TagProtocol,
        right_pkt: PacketProtocol,
        shared_keys: tuple[str, ...],
    ) -> tuple[TagProtocol, PacketProtocol]:
        """Merge a matched pair of rows into one joined (Tag, Packet)."""
        from orcapod.core.datagrams import Packet, Tag

        # Merge tag dicts (shared keys come from left)
        merged_tag_d: dict = {}
        merged_tag_d.update(left_tag.as_dict())
        for k, v in right_tag.as_dict().items():
            if k not in merged_tag_d:
                merged_tag_d[k] = v

        # Merge system tags with side suffix to avoid collisions
        merged_sys: dict = {}
        for k, v in left_tag.system_tags().items():
            merged_sys[k] = v
        for k, v in right_tag.system_tags().items():
            merged_sys[k] = v

        merged_tag = Tag(merged_tag_d, system_tags=merged_sys)

        # Merge packet dicts (non-overlapping by Join's validation)
        merged_pkt_d: dict = {}
        merged_pkt_d.update(left_pkt.as_dict())
        merged_pkt_d.update(right_pkt.as_dict())

        merged_si: dict = {}
        merged_si.update(left_pkt.source_info())
        merged_si.update(right_pkt.source_info())

        merged_pkt = Packet(merged_pkt_d, source_info=merged_si)

        return merged_tag, merged_pkt

    def identity_structure(self) -> Any:
        return self.__class__.__name__

    def __repr__(self) -> str:
        return "Join()"
