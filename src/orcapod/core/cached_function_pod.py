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

    Caches at the ``process_packet(tag, packet)`` level using only the
    **input packet content hash** as the cache key — the output of a
    packet function depends solely on the packet, not the tag.

    Tag-level provenance tracking (tag + system tags + packet hash) is
    handled separately by ``FunctionNode.add_pipeline_record``.

    Storage format aligns with ``CachedPacketFunction``: each cached
    record includes function variation data, execution data, input packet
    hash, and a timestamp.
    """

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

    def _lookup(self, input_packet: PacketProtocol) -> PacketProtocol | None:
        """Look up a cached output packet by input packet content hash.

        Args:
            input_packet: The input packet whose content hash is used for lookup.

        Returns:
            The cached output packet, or ``None`` if not found.
        """
        from orcapod.core.datagrams import Packet

        RECORD_ID_COL = "_record_id"

        # TODO: add match based on match_tier if specified
        # TODO: implement matching policy/strategy (see DESIGN_ISSUES P6)
        constraints = {
            constants.INPUT_PACKET_HASH_COL: input_packet.content_hash().to_string(),
        }

        result_table = self._result_database.get_records_with_column_value(
            self.record_path,
            constraints,
            record_id_column=RECORD_ID_COL,
        )

        if result_table is None or result_table.num_rows == 0:
            return None

        if result_table.num_rows > 1:
            logger.info(
                "Pod-level cache: multiple records for input packet hash, "
                "taking most recent"
            )
            result_table = result_table.sort_by(
                [(constants.POD_TIMESTAMP, "descending")]
            ).take([0])

        record_id = result_table.to_pylist()[0][RECORD_ID_COL]
        result_table = result_table.drop_columns(
            [RECORD_ID_COL, constants.INPUT_PACKET_HASH_COL]
        )

        return Packet(
            result_table,
            record_id=record_id,
            meta_info={self.RESULT_COMPUTED_FLAG: False},
        )

    def _store(
        self,
        input_packet: PacketProtocol,
        output_packet: PacketProtocol,
    ) -> None:
        """Store an output packet in the cache.

        Stores the output packet data alongside function variation data,
        execution data, input packet hash, and timestamp — matching the
        column structure of ``CachedPacketFunction``.

        Args:
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

        # Add input packet hash (position 0, same as CachedPacketFunction)
        data_table = data_table.add_column(
            0,
            constants.INPUT_PACKET_HASH_COL,
            pa.array([input_packet.content_hash().to_string()], type=pa.large_string()),
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

        The cache key is the input packet content hash only — the function
        output depends solely on the packet, not the tag.  The output
        packet carries a ``RESULT_COMPUTED_FLAG`` meta value: ``True`` if
        freshly computed, ``False`` if retrieved from cache.

        Args:
            tag: The tag associated with the packet.
            packet: The input packet to process.

        Returns:
            A ``(tag, output_packet)`` tuple; output_packet is ``None``
            if the inner function filters the packet out.
        """
        cached = self._lookup(packet)
        if cached is not None:
            logger.info("Pod-level cache hit")
            return tag, cached

        tag, output = self._function_pod.process_packet(tag, packet)
        if output is not None:
            self._store(packet, output)
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
        cached = self._lookup(packet)
        if cached is not None:
            logger.info("Pod-level cache hit")
            return tag, cached

        tag, output = await self._function_pod.async_process_packet(tag, packet)
        if output is not None:
            self._store(packet, output)
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
