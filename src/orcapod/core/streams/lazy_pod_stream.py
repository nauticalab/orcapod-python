import logging
from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

from orcapod.core.system_constants import constants
from orcapod.protocols import core_protocols as cp
from orcapod.types import PythonSchema
from orcapod.utils import arrow_utils
from orcapod.utils.lazy_module import LazyModule
from orcapod.core.streams.base import StreamBase


if TYPE_CHECKING:
    import pyarrow as pa
    import polars as pl
    import asyncio
else:
    pa = LazyModule("pyarrow")
    pl = LazyModule("polars")
    asyncio = LazyModule("asyncio")


# TODO: consider using this instead of making copy of dicts
# from types import MappingProxyType

logger = logging.getLogger(__name__)


class LazyPodResultStream(StreamBase):
    """
    A fixed stream that lazily processes packets from a prepared input stream.
    This is what Pod.process() returns - it's static/fixed but efficient.
    """

    def __init__(self, pod: cp.Pod, prepared_stream: cp.Stream, **kwargs):
        super().__init__(source=pod, upstreams=(prepared_stream,), **kwargs)
        self.pod = pod
        self.prepared_stream = prepared_stream
        # capture the immutable iterator from the prepared stream
        self._prepared_stream_iterator = prepared_stream.iter_packets()
        self._set_modified_time()  # set modified time to AFTER we obtain the iterator
        # note that the invocation of iter_packets on upstream likely triggeres the modified time
        # to be updated on the usptream. Hence you want to set this stream's modified time after that.

        # Packet-level caching (from your PodStream)
        self._cached_output_packets: dict[int, tuple[cp.Tag, cp.Packet | None]] = {}
        self._cached_output_table: pa.Table | None = None
        self._cached_content_hash_column: pa.Array | None = None

    def iter_packets(
        self,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
    ) -> Iterator[tuple[cp.Tag, cp.Packet]]:
        if self._prepared_stream_iterator is not None:
            for i, (tag, packet) in enumerate(self._prepared_stream_iterator):
                if i in self._cached_output_packets:
                    # Use cached result
                    tag, packet = self._cached_output_packets[i]
                    if packet is not None:
                        yield tag, packet
                else:
                    # Process packet
                    processed = self.pod.call(
                        tag,
                        packet,
                        execution_engine=execution_engine or self.execution_engine,
                        execution_engine_opts=execution_engine_opts
                        or self._execution_engine_opts,
                    )
                    # TODO: verify the proper use of execution engine opts
                    if processed is not None:
                        # Update shared cache for future iterators (optimization)
                        self._cached_output_packets[i] = processed
                        tag, packet = processed
                        if packet is not None:
                            yield tag, packet

            # Mark completion by releasing the iterator
            self._prepared_stream_iterator = None
        else:
            # Yield from snapshot of complete cache
            for i in range(len(self._cached_output_packets)):
                tag, packet = self._cached_output_packets[i]
                if packet is not None:
                    yield tag, packet

    async def run_async(
        self,
        *args: Any,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        if self._prepared_stream_iterator is not None:
            pending_call_lut = {}
            for i, (tag, packet) in enumerate(self._prepared_stream_iterator):
                if i not in self._cached_output_packets:
                    # Process packet
                    pending_call_lut[i] = self.pod.async_call(
                        tag,
                        packet,
                        execution_engine=execution_engine or self.execution_engine,
                        execution_engine_opts=execution_engine_opts
                        or self._execution_engine_opts,
                    )

            indices = list(pending_call_lut.keys())
            pending_calls = [pending_call_lut[i] for i in indices]

            results = await asyncio.gather(*pending_calls)
            for i, result in zip(indices, results):
                self._cached_output_packets[i] = result

            # Mark completion by releasing the iterator
            self._prepared_stream_iterator = None

    def run(
        self,
        *args: Any,
        execution_engine: cp.ExecutionEngine | None = None,
        execution_engine_opts: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> None:
        # Fallback to synchronous run
        self.flow(
            execution_engine=execution_engine or self.execution_engine,
            execution_engine_opts=execution_engine_opts or self._execution_engine_opts,
        )

    def keys(
        self, include_system_tags: bool = False
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        """
        Returns the keys of the tag and packet columns in the stream.
        This is useful for accessing the columns in the stream.
        """

        tag_keys, _ = self.prepared_stream.keys(include_system_tags=include_system_tags)
        packet_keys = tuple(self.pod.output_packet_types().keys())
        return tag_keys, packet_keys

    def types(
        self, include_system_tags: bool = False
    ) -> tuple[PythonSchema, PythonSchema]:
        tag_typespec, _ = self.prepared_stream.types(
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

            # TODO: re-verify the implemetation of this conversion
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
        if not include_system_tags:
            # TODO: get system tags more effiicently
            drop_columns.extend(
                [
                    c
                    for c in self._cached_output_table.column_names
                    if c.startswith(constants.SYSTEM_TAG_PREFIX)
                ]
            )
        if not include_source:
            drop_columns.extend(f"{constants.SOURCE_PREFIX}{c}" for c in self.keys()[1])
        if not include_data_context:
            drop_columns.append(constants.CONTEXT_KEY)

        output_table = self._cached_output_table.drop(drop_columns)

        # lazily prepare content hash column if requested
        if include_content_hash:
            if self._cached_content_hash_column is None:
                content_hashes = []
                # TODO: verify that order will be preserved
                for tag, packet in self.iter_packets(
                    execution_engine=execution_engine or self.execution_engine,
                    execution_engine_opts=execution_engine_opts or self._execution_engine_opts,
                ):
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
            # TODO: reimplement using polars natively
            output_table = (
                pl.DataFrame(output_table)
                .sort(by=self.keys()[0], descending=False)
                .to_arrow()
            )
            # output_table = output_table.sort_by(
            #     [(column, "ascending") for column in self.keys()[0]]
            # )
        return output_table
