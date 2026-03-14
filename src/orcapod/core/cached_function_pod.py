"""CachedFunctionPod — pod-level caching wrapper that intercepts process_packet()."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from orcapod.core.function_pod import WrappedFunctionPod
from orcapod.protocols.core_protocols import (
    FunctionPodProtocol,
    PacketProtocol,
    StreamProtocol,
    TagProtocol,
)
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.system_constants import constants
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa
else:
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)


class CachedFunctionPod(WrappedFunctionPod):
    """Pod-level caching wrapper that intercepts ``process_packet()``.

    Unlike ``CachedPacketFunction`` (which caches at the ``call(packet)``
    level using only the packet content hash as the cache key), this
    wrapper operates at the ``process_packet(tag, packet)`` level and
    incorporates the tag (including system tags) and the packet content
    hash into a single cache entry hash.

    The cache entry hash is computed the same way as the pipeline record's
    ``entry_id``: ``arrow_hasher.hash_table(tag_with_system_tags + input_packet_hash)``.
    This ensures two rows with identical user tags but different system
    tags (reflecting different source entries) are cached separately.

    Storage format aligns with ``CachedPacketFunction``: each cached
    record includes function variation data, execution data, the cache
    entry hash, and a timestamp.
    """

    # Column storing the combined hash of tag + system tags + input packet hash
    CACHE_ENTRY_HASH_COL = f"{constants.META_PREFIX}cache_entry_hash"

    # Meta column indicating whether the result was freshly computed
    RESULT_COMPUTED_FLAG = f"{constants.META_PREFIX}computed"

    def __init__(
        self,
        function_pod: FunctionPodProtocol,
        result_database: ArrowDatabaseProtocol,
        record_path_prefix: tuple[str, ...] = (),
        auto_flush: bool = True,
        **kwargs,
    ) -> None:
        super().__init__(function_pod, **kwargs)
        self._result_database = result_database
        self._record_path_prefix = record_path_prefix
        self._auto_flush = auto_flush

    @property
    def record_path(self) -> tuple[str, ...]:
        """Return the path to the cached records in the result store."""
        return self._record_path_prefix + self.uri

    def _compute_entry_hash(self, tag: TagProtocol, packet: PacketProtocol) -> str:
        """Compute a cache entry hash from tag (with system tags) and packet.

        The hash includes user-facing tag columns, system tag columns, and
        the input packet content hash — matching the pipeline record's
        entry_id computation.

        Args:
            tag: The tag associated with the packet.
            packet: The input packet.

        Returns:
            A hash string uniquely identifying this (tag, system_tags, packet)
            combination.
        """
        tag_with_hash = tag.as_table(columns={"system_tags": True}).append_column(
            constants.INPUT_PACKET_HASH_COL,
            pa.array([packet.content_hash().to_string()], type=pa.large_string()),
        )
        return self.data_context.arrow_hasher.hash_table(tag_with_hash).to_string()

    def _lookup(self, entry_hash: str) -> PacketProtocol | None:
        """Look up a cached output packet by cache entry hash.

        Args:
            entry_hash: The combined tag+system_tags+packet hash.

        Returns:
            The cached output packet, or ``None`` if not found.
        """
        from orcapod.core.datagrams import Packet

        RECORD_ID_COL = "_record_id"

        result_table = self._result_database.get_records_with_column_value(
            self.record_path,
            {self.CACHE_ENTRY_HASH_COL: entry_hash},
            record_id_column=RECORD_ID_COL,
        )

        if result_table is None or result_table.num_rows == 0:
            return None

        if result_table.num_rows > 1:
            logger.info(
                "Pod-level cache: multiple records for entry hash, taking most recent"
            )
            result_table = result_table.sort_by(
                [(constants.POD_TIMESTAMP, "descending")]
            ).take([0])

        record_id = result_table.to_pylist()[0][RECORD_ID_COL]
        result_table = result_table.drop_columns(
            [RECORD_ID_COL, self.CACHE_ENTRY_HASH_COL]
        )

        return Packet(
            result_table,
            record_id=record_id,
            meta_info={self.RESULT_COMPUTED_FLAG: False},
        )

    def _store(
        self,
        entry_hash: str,
        input_packet: PacketProtocol,
        output_packet: PacketProtocol,
    ) -> None:
        """Store an output packet in the cache.

        Stores the output packet data alongside function variation data,
        execution data, the cache entry hash, and a timestamp — matching
        the column structure of ``CachedPacketFunction``.

        Args:
            entry_hash: The combined tag+system_tags+packet hash.
            input_packet: The input packet (used for its content hash).
            output_packet: The computed output packet to store.
        """
        data_table = output_packet.as_table(columns={"source": True, "context": True})

        pf = self._function_pod.packet_function

        # Add function variation data columns
        i = 0
        for k, v in pf.get_function_variation_data().items():
            data_table = data_table.add_column(
                i,
                f"{constants.PF_VARIATION_PREFIX}{k}",
                pa.array([v], type=pa.large_string()),
            )
            i += 1

        # Add execution data columns
        for k, v in pf.get_execution_data().items():
            data_table = data_table.add_column(
                i,
                f"{constants.PF_EXECUTION_PREFIX}{k}",
                pa.array([v], type=pa.large_string()),
            )
            i += 1

        # Add cache entry hash (position 0)
        data_table = data_table.add_column(
            0,
            self.CACHE_ENTRY_HASH_COL,
            pa.array([entry_hash], type=pa.large_string()),
        )

        # Append timestamp
        timestamp = datetime.now(timezone.utc)
        data_table = data_table.append_column(
            constants.POD_TIMESTAMP,
            pa.array([timestamp], type=pa.timestamp("us", tz="UTC")),
        )

        self._result_database.add_record(
            self.record_path,
            output_packet.datagram_id,
            data_table,
            skip_duplicates=False,
        )

        if self._auto_flush:
            self._result_database.flush()

    def process_packet(
        self, tag: TagProtocol, packet: PacketProtocol
    ) -> tuple[TagProtocol, PacketProtocol | None]:
        """Process a packet with pod-level caching.

        The cache entry hash incorporates tag columns, system tag columns,
        and the input packet content hash.  On a cache hit, the stored
        output packet is returned without invoking the inner pod's
        computation.  The output packet carries a ``RESULT_COMPUTED_FLAG``
        meta value: ``True`` if freshly computed, ``False`` if from cache.

        Args:
            tag: The tag associated with the packet.
            packet: The input packet to process.

        Returns:
            A ``(tag, output_packet)`` tuple; output_packet is ``None``
            if the inner function filters the packet out.
        """
        entry_hash = self._compute_entry_hash(tag, packet)
        cached = self._lookup(entry_hash)
        if cached is not None:
            logger.info("Pod-level cache hit")
            return tag, cached

        tag, output = self._function_pod.process_packet(tag, packet)
        if output is not None:
            self._store(entry_hash, packet, output)
            output = output.with_meta_columns(**{self.RESULT_COMPUTED_FLAG: True})
        return tag, output

    async def async_process_packet(
        self, tag: TagProtocol, packet: PacketProtocol
    ) -> tuple[TagProtocol, PacketProtocol | None]:
        """Async counterpart of ``process_packet``.

        DB lookup and store are synchronous (DB protocol is sync), but the
        actual computation uses the inner pod's ``async_process_packet``
        for true async execution.
        """
        entry_hash = self._compute_entry_hash(tag, packet)
        cached = self._lookup(entry_hash)
        if cached is not None:
            logger.info("Pod-level cache hit")
            return tag, cached

        tag, output = await self._function_pod.async_process_packet(tag, packet)
        if output is not None:
            self._store(entry_hash, packet, output)
            output = output.with_meta_columns(**{self.RESULT_COMPUTED_FLAG: True})
        return tag, output

    def get_all_cached_outputs(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        """Return all cached records from the result store for this pod.

        Args:
            include_system_columns: If True, include system columns
                (e.g. record_id) in the result.

        Returns:
            A PyArrow table of cached results, or ``None`` if empty.
        """
        record_id_column = (
            constants.PACKET_RECORD_ID if include_system_columns else None
        )
        result_table = self._result_database.get_all_records(
            self.record_path, record_id_column=record_id_column
        )
        if result_table is None or result_table.num_rows == 0:
            return None
        return result_table

    def process(
        self, *streams: StreamProtocol, label: str | None = None
    ) -> StreamProtocol:
        """Invoke the inner pod but with pod-level caching on process_packet.

        The stream returned uses *this* pod's ``process_packet`` (which
        includes caching) rather than the inner pod's.
        """
        from orcapod.core.function_pod import FunctionPodStream

        # Validate and prepare the input stream
        input_stream = self._function_pod.handle_input_streams(*streams)
        self._function_pod.validate_inputs(*streams)

        return FunctionPodStream(
            function_pod=self,
            input_stream=input_stream,
            label=label,
        )
