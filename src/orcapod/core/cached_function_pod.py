"""CachedFunctionPod — pod-level caching wrapper that intercepts process_packet()."""

from __future__ import annotations

import logging
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
    incorporates *both* the tag content hash and the packet content hash
    into the cache key.

    This is useful when the same packet data may produce different results
    depending on tag metadata, or when tag-level deduplication is desired.
    """

    def __init__(
        self,
        function_pod: FunctionPodProtocol,
        result_database: ArrowDatabaseProtocol,
        record_path_prefix: tuple[str, ...] = (),
        **kwargs,
    ) -> None:
        super().__init__(function_pod, **kwargs)
        self._result_database = result_database
        self._record_path_prefix = record_path_prefix

    @property
    def record_path(self) -> tuple[str, ...]:
        """Return the path to the cached records in the result store."""
        return self._record_path_prefix + self.uri

    def _compute_cache_key(self, tag: TagProtocol, packet: PacketProtocol) -> str:
        """Compute a cache key from tag and packet content hashes.

        Args:
            tag: The tag associated with the packet.
            packet: The input packet.

        Returns:
            A string combining tag and packet content hashes.
        """
        tag_hash = tag.content_hash().to_string()
        packet_hash = packet.content_hash().to_string()
        return f"{tag_hash}:{packet_hash}"

    def _lookup(self, cache_key: str) -> PacketProtocol | None:
        """Look up a cached output packet by cache key.

        Args:
            cache_key: The combined tag+packet hash key.

        Returns:
            The cached output packet, or ``None`` if not found.
        """
        from orcapod.core.datagrams import Packet

        CACHE_KEY_COL = f"{constants.META_PREFIX}pod_cache_key"
        RECORD_ID_COL = "_record_id"

        result_table = self._result_database.get_records_with_column_value(
            self.record_path,
            {CACHE_KEY_COL: cache_key},
            record_id_column=RECORD_ID_COL,
        )

        if result_table is None or result_table.num_rows == 0:
            return None

        if result_table.num_rows > 1:
            logger.info(
                "Pod-level cache: multiple records for key %s, taking most recent",
                cache_key,
            )
            result_table = result_table.sort_by(
                [(constants.POD_TIMESTAMP, "descending")]
            ).take([0])

        record_id = result_table.to_pylist()[0][RECORD_ID_COL]
        result_table = result_table.drop_columns([RECORD_ID_COL, CACHE_KEY_COL])

        return Packet(result_table, record_id=record_id)

    def _store(
        self,
        cache_key: str,
        output_packet: PacketProtocol,
    ) -> None:
        """Store an output packet in the cache.

        Args:
            cache_key: The combined tag+packet hash key.
            output_packet: The computed output packet to store.
        """
        from datetime import datetime, timezone

        CACHE_KEY_COL = f"{constants.META_PREFIX}pod_cache_key"

        data_table = output_packet.as_table(columns={"source": True, "context": True})

        # Prepend cache key column
        data_table = data_table.add_column(
            0,
            CACHE_KEY_COL,
            pa.array([cache_key], type=pa.large_string()),
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
        self._result_database.flush()

    def process_packet(
        self, tag: TagProtocol, packet: PacketProtocol
    ) -> tuple[TagProtocol, PacketProtocol | None]:
        """Process a packet with pod-level caching.

        The cache key incorporates both tag and packet content hashes.
        On a cache hit, the stored output packet is returned without
        invoking the inner pod's computation.

        Args:
            tag: The tag associated with the packet.
            packet: The input packet to process.

        Returns:
            A ``(tag, output_packet)`` tuple; output_packet is ``None``
            if the inner function filters the packet out.
        """
        cache_key = self._compute_cache_key(tag, packet)
        cached = self._lookup(cache_key)
        if cached is not None:
            logger.info("Pod-level cache hit for key %s", cache_key)
            return tag, cached

        tag, output = self._function_pod.process_packet(tag, packet)
        if output is not None:
            self._store(cache_key, output)
        return tag, output

    def process(
        self, *streams: StreamProtocol, label: str | None = None
    ) -> StreamProtocol:
        """Invoke the inner pod but with pod-level caching on process_packet.

        The stream returned uses *this* pod's ``process_packet`` (which
        includes caching) rather than the inner pod's.
        """
        from orcapod.core.function_pod import FunctionPod, FunctionPodStream

        # Validate and prepare the input stream
        input_stream = self._function_pod.handle_input_streams(*streams)
        self._function_pod.validate_inputs(*streams)

        return FunctionPodStream(
            function_pod=self,
            input_stream=input_stream,
            label=label,
        )
