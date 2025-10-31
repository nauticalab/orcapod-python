import logging
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from orcapod.core.system_constants import constants
from orcapod.protocols import core_protocols as cp
from orcapod.types import PythonSchema
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule
from orcapod.core.streams.base import StreamBase
from orcapod.core.streams.table_stream import TableStream


if TYPE_CHECKING:
    import pyarrow as pa
    import pyarrow.compute as pc
    import polars as pl

else:
    pa = LazyModule("pyarrow")
    pc = LazyModule("pyarrow.compute")
    pl = LazyModule("polars")


# TODO: consider using this instead of making copy of dicts
# from types import MappingProxyType

logger = logging.getLogger(__name__)


class CachedPodStream(StreamBase):
    """
    A fixed stream that lazily processes packets from a prepared input stream.
    This is what Pod.process() returns - it's static/fixed but efficient.
    """

    # TODO: define interface for storage or pod storage
    def __init__(self, pod: cp.CachedPod, input_stream: cp.Stream, **kwargs):
        super().__init__(source=pod, upstreams=(input_stream,), **kwargs)
        self.pod = pod
        self.input_stream = input_stream
        self._set_modified_time()  # set modified time to when we obtain the iterator
        # capture the immutable iterator from the input stream

        self._prepared_stream_iterator = input_stream.iter_packets()

        # Packet-level caching (from your PodStream)
        self._cached_output_packets: list[tuple[cp.Tag, cp.Packet | None]] | None = None
        self._cached_output_table: pa.Table | None = None
        self._cached_content_hash_column: pa.Array | None = None

    def set_mode(self, mode: str) -> None:
        return self.pod.set_mode(mode)

    @property
    def mode(self) -> str:
        return self.pod.mode

    def test(self) -> cp.Stream:
        return self

    async def run_async(
        self,
        *args: Any,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Runs the stream, processing the input stream and preparing the output stream.
        This is typically called before iterating over the packets.
        """
        if self._cached_output_packets is None:
            cached_results = []

            # identify all entries in the input stream for which we still have not computed packets
            target_entries = self.input_stream.as_table(
                include_content_hash=constants.INPUT_PACKET_HASH,
                include_source=True,
                include_system_tags=True,
            )
            existing_entries = self.pod.get_all_cached_outputs(
                include_system_columns=True
            )
            if existing_entries is None or existing_entries.num_rows == 0:
                missing = target_entries.drop_columns([constants.INPUT_PACKET_HASH])
                existing = None
            else:
                all_results = target_entries.join(
                    existing_entries.append_column(
                        "_exists", pa.array([True] * len(existing_entries))
                    ),
                    keys=[constants.INPUT_PACKET_HASH],
                    join_type="left outer",
                    right_suffix="_right",
                )
                # grab all columns from target_entries first
                missing = (
                    all_results.filter(pc.is_null(pc.field("_exists")))
                    .select(target_entries.column_names)
                    .drop_columns([constants.INPUT_PACKET_HASH])
                )

                existing = (
                    all_results.filter(pc.is_valid(pc.field("_exists")))
                    .drop_columns(target_entries.column_names)
                    .drop_columns(["_exists"])
                )
                renamed = [
                    c.removesuffix("_right") if c.endswith("_right") else c
                    for c in existing.column_names
                ]
                existing = existing.rename_columns(renamed)

            tag_keys = self.input_stream.keys()[0]

            if existing is not None and existing.num_rows > 0:
                # If there are existing entries, we can cache them
                existing_stream = TableStream(existing, tag_columns=tag_keys)
                for tag, packet in existing_stream.iter_packets():
                    cached_results.append((tag, packet))

            pending_calls = []
            if missing is not None and missing.num_rows > 0:
                for tag, packet in TableStream(missing, tag_columns=tag_keys):
                    # Since these packets are known to be missing, skip the cache lookup
                    pending = self.pod.async_call(
                        tag,
                        packet,
                        skip_cache_lookup=True,
                        execution_engine=execution_engine or self.execution_engine,
                        execution_engine_opts=execution_engine_opts
                        or self._execution_engine_opts,
                    )
                    pending_calls.append(pending)
            import asyncio

            completed_calls = await asyncio.gather(*pending_calls)
            for result in completed_calls:
                cached_results.append(result)

            self._cached_output_packets = cached_results
            self._set_modified_time()

    def run(
        self,
        *args: Any,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        cached_results = []

        # identify all entries in the input stream for which we still have not computed packets
        target_entries = self.input_stream.as_table(
            include_system_tags=True,
            include_source=True,
            include_content_hash=constants.INPUT_PACKET_HASH,
            execution_engine=execution_engine,
        )
        existing_entries = self.pod.get_all_cached_outputs(include_system_columns=True)
        if (
            existing_entries is None
            or existing_entries.num_rows == 0
            or self.mode == "development"
        ):
            missing = target_entries.drop_columns([constants.INPUT_PACKET_HASH])
            existing = None
        else:
            # TODO: do more proper replacement operation
            target_df = pl.DataFrame(target_entries)
            existing_df = pl.DataFrame(
                existing_entries.append_column(
                    "_exists", pa.array([True] * len(existing_entries))
                )
            )
            all_results_df = target_df.join(
                existing_df,
                on=constants.INPUT_PACKET_HASH,
                how="left",
                suffix="_right",
            )
            all_results = all_results_df.to_arrow()

            missing = (
                all_results.filter(pc.is_null(pc.field("_exists")))
                .select(target_entries.column_names)
                .drop_columns([constants.INPUT_PACKET_HASH])
            )

            existing = all_results.filter(
                pc.is_valid(pc.field("_exists"))
            ).drop_columns(
                [
                    "_exists",
                    constants.INPUT_PACKET_HASH,
                    constants.PACKET_RECORD_ID,
                    *self.input_stream.keys()[1],  # remove the input packet keys
                ]
                # TODO: look into NOT fetching back the record ID
            )
            renamed = [
                c.removesuffix("_right") if c.endswith("_right") else c
                for c in existing.column_names
            ]
            existing = existing.rename_columns(renamed)

        tag_keys = self.input_stream.keys()[0]

        if existing is not None and existing.num_rows > 0:
            # If there are existing entries, we can cache them
            existing_stream = TableStream(existing, tag_columns=tag_keys)
            for tag, packet in existing_stream.iter_packets():
                cached_results.append((tag, packet))

        if missing is not None and missing.num_rows > 0:
            hash_to_output_lut: dict[str, cp.Packet | None] = {}
            for tag, packet in TableStream(missing, tag_columns=tag_keys):
                # Since these packets are known to be missing, skip the cache lookup
                packet_hash = packet.content_hash().to_string()
                if packet_hash in hash_to_output_lut:
                    output_packet = hash_to_output_lut[packet_hash]
                else:
                    tag, output_packet = self.pod.call(
                        tag,
                        packet,
                        skip_cache_lookup=True,
                        execution_engine=execution_engine or self.execution_engine,
                        execution_engine_opts=execution_engine_opts
                        or self._execution_engine_opts,
                    )
                    # TODO: use getter for execution engine opts
                    hash_to_output_lut[packet_hash] = output_packet
                cached_results.append((tag, output_packet))

        self._cached_output_packets = cached_results
        self._set_modified_time()

    def iter_packets(
        self,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> Iterator[tuple[cp.Tag, cp.Packet]]:
        """
        Processes the input stream and prepares the output stream.
        This is typically called before iterating over the packets.
        """
        if self._cached_output_packets is None:
            cached_results = []

            # identify all entries in the input stream for which we still have not computed packets
            target_entries = self.input_stream.as_table(
                include_system_tags=True,
                include_source=True,
                include_content_hash=constants.INPUT_PACKET_HASH,
                execution_engine=execution_engine or self.execution_engine,
                execution_engine_opts=execution_engine_opts
                or self._execution_engine_opts,
            )
            existing_entries = self.pod.get_all_cached_outputs(
                include_system_columns=True
            )
            if existing_entries is None or existing_entries.num_rows == 0:
                missing = target_entries.drop_columns([constants.INPUT_PACKET_HASH])
                existing = None
            else:
                # missing = target_entries.join(
                #     existing_entries,
                #     keys=[constants.INPUT_PACKET_HASH],
                #     join_type="left anti",
                # )
                # Single join that gives you both missing and existing
                # More efficient - only bring the key column from existing_entries
                # .select([constants.INPUT_PACKET_HASH]).append_column(
                #     "_exists", pa.array([True] * len(existing_entries))
                # ),

                # TODO: do more proper replacement operation
                target_df = pl.DataFrame(target_entries)
                existing_df = pl.DataFrame(
                    existing_entries.append_column(
                        "_exists", pa.array([True] * len(existing_entries))
                    )
                )
                all_results_df = target_df.join(
                    existing_df,
                    on=constants.INPUT_PACKET_HASH,
                    how="left",
                    suffix="_right",
                )
                all_results = all_results_df.to_arrow()
                # all_results = target_entries.join(
                #     existing_entries.append_column(
                #         "_exists", pa.array([True] * len(existing_entries))
                #     ),
                #     keys=[constants.INPUT_PACKET_HASH],
                #     join_type="left outer",
                #     right_suffix="_right",  # rename the existing records in case of collision of output packet keys with input packet keys
                # )
                # grab all columns from target_entries first
                missing = (
                    all_results.filter(pc.is_null(pc.field("_exists")))
                    .select(target_entries.column_names)
                    .drop_columns([constants.INPUT_PACKET_HASH])
                )

                existing = all_results.filter(
                    pc.is_valid(pc.field("_exists"))
                ).drop_columns(
                    [
                        "_exists",
                        constants.INPUT_PACKET_HASH,
                        constants.PACKET_RECORD_ID,
                        *self.input_stream.keys()[1],  # remove the input packet keys
                    ]
                    # TODO: look into NOT fetching back the record ID
                )
                renamed = [
                    c.removesuffix("_right") if c.endswith("_right") else c
                    for c in existing.column_names
                ]
                existing = existing.rename_columns(renamed)

            tag_keys = self.input_stream.keys()[0]

            if existing is not None and existing.num_rows > 0:
                # If there are existing entries, we can cache them
                existing_stream = TableStream(existing, tag_columns=tag_keys)
                for tag, packet in existing_stream.iter_packets():
                    cached_results.append((tag, packet))
                    yield tag, packet

            if missing is not None and missing.num_rows > 0:
                hash_to_output_lut: dict[str, cp.Packet | None] = {}
                for tag, packet in TableStream(missing, tag_columns=tag_keys):
                    # Since these packets are known to be missing, skip the cache lookup
                    packet_hash = packet.content_hash().to_string()
                    if packet_hash in hash_to_output_lut:
                        output_packet = hash_to_output_lut[packet_hash]
                    else:
                        tag, output_packet = self.pod.call(
                            tag,
                            packet,
                            skip_cache_lookup=True,
                            execution_engine=execution_engine or self.execution_engine,
                            execution_engine_opts=execution_engine_opts
                            or self._execution_engine_opts,
                        )
                        hash_to_output_lut[packet_hash] = output_packet
                    cached_results.append((tag, output_packet))
                    if output_packet is not None:
                        yield tag, output_packet

            self._cached_output_packets = cached_results
            self._set_modified_time()
        else:
            for tag, packet in self._cached_output_packets:
                if packet is not None:
                    yield tag, packet

    def keys(
        self, include_system_tags: bool = False
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """
        Returns the keys of the tag and packet columns in the stream.
        This is useful for accessing the columns in the stream.
        """

        tag_keys, _ = self.input_stream.keys(include_system_tags=include_system_tags)
        packet_keys = tuple(self.pod.output_packet_types().keys())
        return tag_keys, packet_keys

    def types(
        self, include_system_tags: bool = False
    ) -> tuple[PythonSchema, PythonSchema]:
        tag_typespec, _ = self.input_stream.types(
            include_system_tags=include_system_tags
        )
        # TODO: check if copying can be avoided
        packet_typespec = dict(self.pod.output_packet_types())
        return tag_typespec, packet_typespec

    def as_table(
        self,
        include_data_context: bool = False,
        include_source: bool = False,
        include_system_tags: bool = False,
        include_content_hash: bool | str = False,
        sort_by_tags: bool = True,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> "pa.Table":
        if self._cached_output_table is None:
            all_tags = []
            all_packets = []
            tag_schema, packet_schema = None, None
            for tag, packet in self.iter_packets(
                execution_engine=execution_engine or self.execution_engine,
                execution_engine_opts=execution_engine_opts
                or self._execution_engine_opts,
            ):
                if tag_schema is None:
                    tag_schema = tag.arrow_schema(include_system_tags=True)
                if packet_schema is None:
                    packet_schema = packet.arrow_schema(
                        include_context=True,
                        include_source=True,
                    )
                all_tags.append(tag.as_dict(include_system_tags=True))
                # FIXME: using in the pinch conversion to str from path
                # replace with an appropriate semantic converter-based approach!
                dict_patcket = packet.as_dict(include_context=True, include_source=True)
                all_packets.append(dict_patcket)

            converter = self.data_context.type_converter

            struct_packets = converter.python_dicts_to_struct_dicts(all_packets)
            all_tags_as_tables: pa.Table = pa.Table.from_pylist(
                all_tags, schema=tag_schema
            )
            all_packets_as_tables: pa.Table = pa.Table.from_pylist(
                struct_packets, schema=packet_schema
            )

            self._cached_output_table = arrow_utils.hstack_tables(
                all_tags_as_tables, all_packets_as_tables
            )
        assert self._cached_output_table is not None, (
            "_cached_output_table should not be None here."
        )

        drop_columns = []
        if not include_source:
            drop_columns.extend(f"{constants.SOURCE_PREFIX}{c}" for c in self.keys()[1])
        if not include_data_context:
            drop_columns.append(constants.CONTEXT_KEY)
        if not include_system_tags:
            # TODO: come up with a more efficient approach
            drop_columns.extend(
                [
                    c
                    for c in self._cached_output_table.column_names
                    if c.startswith(constants.SYSTEM_TAG_PREFIX)
                ]
            )

        output_table = self._cached_output_table.drop_columns(drop_columns)

        # lazily prepare content hash column if requested
        if include_content_hash:
            if self._cached_content_hash_column is None:
                content_hashes = []
                for tag, packet in self.iter_packets(execution_engine=execution_engine):
                    content_hashes.append(packet.content_hash().to_string())
                self._cached_content_hash_column = pa.array(
                    content_hashes, type=pa.large_string()
                )
            assert self._cached_content_hash_column is not None, (
                "_cached_content_hash_column should not be None here."
            )
            hash_column_name = (
                "_content_hash"
                if include_content_hash is True
                else include_content_hash
            )
            output_table = output_table.append_column(
                hash_column_name, self._cached_content_hash_column
            )

        if sort_by_tags:
            try:
                # TODO: consider having explicit tag/packet properties?
                output_table = output_table.sort_by(
                    [(column, "ascending") for column in self.keys()[0]]
                )
            except pa.ArrowTypeError:
                pass

        return output_table
