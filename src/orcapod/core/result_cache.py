"""ResultCache — shared result caching logic for CachedPacketFunction and CachedFunctionPod.

Owns the database, record path, lookup (with match strategy), store,
conflict resolution, and auto-flush behavior.  Both ``CachedPacketFunction``
and ``CachedFunctionPod`` delegate to a ``ResultCache`` instance.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from orcapod.protocols.core_protocols import PacketProtocol
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.system_constants import constants
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.protocols.core_protocols.datagrams import DatagramProtocol
else:
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)


class ResultCache:
    """Shared result caching backed by an ``ArrowDatabaseProtocol``.

    Provides lookup (by input packet hash + optional additional constraints),
    store (output data + variation/execution metadata + timestamp), conflict
    resolution (most-recent-timestamp wins), and auto-flush.

    The match strategy is extensible: the default lookup matches on
    ``INPUT_PACKET_HASH_COL`` only, but callers can supply additional
    constraints (e.g. function variation columns) to narrow the match.
    This is the hook for future match-tier support (see DESIGN_ISSUES P6).

    Args:
        result_database: The database to store/retrieve cached results.
        record_path: The record path tuple for scoping records in the database.
        auto_flush: If True, flush the database after each store operation.
    """

    # Meta column indicating whether the result was freshly computed
    RESULT_COMPUTED_FLAG = f"{constants.META_PREFIX}computed"

    def __init__(
        self,
        result_database: ArrowDatabaseProtocol,
        record_path: tuple[str, ...],
        auto_flush: bool = True,
    ) -> None:
        self._result_database = result_database
        self._record_path = record_path
        self._auto_flush = auto_flush

    @property
    def result_database(self) -> ArrowDatabaseProtocol:
        """The underlying database."""
        return self._result_database

    @property
    def record_path(self) -> tuple[str, ...]:
        """The record path for scoping records in the database."""
        return self._record_path

    def set_auto_flush(self, on: bool = True) -> None:
        """Set auto-flush behavior."""
        self._auto_flush = on

    def lookup(
        self,
        input_packet: PacketProtocol,
        additional_constraints: dict[str, str] | None = None,
    ) -> PacketProtocol | None:
        """Look up a cached output packet for *input_packet*.

        The default match is by ``INPUT_PACKET_HASH_COL`` only.
        *additional_constraints* can narrow the match further (e.g. by
        function variation hash for stricter cache invalidation).

        If multiple records match, the most recent (by timestamp) wins.

        Args:
            input_packet: The input packet whose content hash is the
                primary lookup key.
            additional_constraints: Optional extra column-value pairs to
                include in the lookup query.

        Returns:
            The cached output packet with ``RESULT_COMPUTED_FLAG: False``
            in its meta, or ``None`` if no match was found.
        """
        from orcapod.core.datagrams import Packet

        RECORD_ID_COL = "_record_id"

        constraints: dict[str, str] = {
            constants.INPUT_PACKET_HASH_COL: input_packet.content_hash().to_string(),
        }
        if additional_constraints:
            constraints.update(additional_constraints)

        result_table = self._result_database.get_records_with_column_value(
            self._record_path,
            constraints,
            record_id_column=RECORD_ID_COL,
        )

        if result_table is None or result_table.num_rows == 0:
            return None

        if result_table.num_rows > 1:
            logger.info(
                "Cache conflict resolution: %d records for constraints %s, "
                "taking most recent",
                result_table.num_rows,
                list(constraints.keys()),
            )
            result_table = result_table.sort_by(
                [(constants.POD_TIMESTAMP, "descending")]
            ).take([0])

        record_id = result_table.to_pylist()[0][RECORD_ID_COL]
        # Drop lookup columns from the returned packet
        drop_cols = [RECORD_ID_COL] + [
            c for c in constraints if c in result_table.column_names
        ]
        result_table = result_table.drop_columns(drop_cols)

        return Packet(
            result_table,
            record_id=record_id,
            meta_info={self.RESULT_COMPUTED_FLAG: False},
        )

    def store(
        self,
        input_packet: PacketProtocol,
        output_packet: PacketProtocol,
        variation_datagram: "DatagramProtocol",
        execution_datagram: "DatagramProtocol",
        skip_duplicates: bool = False,
    ) -> None:
        """Store an output packet in the cache.

        Stores the output packet data alongside function variation and
        execution metadata (as Datagrams), input packet hash, and a timestamp.

        Args:
            input_packet: The input packet (used for its content hash).
            output_packet: The computed output packet to store.
            variation_datagram: Function variation metadata as a Datagram.
            execution_datagram: Execution environment metadata as a Datagram.
            skip_duplicates: If True, silently skip if a record with the
                same ID already exists.
        """
        data_table = output_packet.as_table(columns={"source": True, "context": True})

        # Add variation and execution columns with prefixes.
        # Use a running counter for insertion position since add_column shifts indices.
        col_idx = 0
        var_table = variation_datagram.as_table()
        for name in var_table.column_names:
            data_table = data_table.add_column(
                col_idx,
                f"{constants.PF_VARIATION_PREFIX}{name}",
                var_table.column(name),
            )
            col_idx += 1

        exec_table = execution_datagram.as_table()
        for name in exec_table.column_names:
            data_table = data_table.add_column(
                col_idx,
                f"{constants.PF_EXECUTION_PREFIX}{name}",
                exec_table.column(name),
            )
            col_idx += 1

        # Add input packet hash (position 0)
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
            self._record_path,
            output_packet.datagram_id,
            data_table,
            skip_duplicates=skip_duplicates,
        )

        if self._auto_flush:
            self._result_database.flush()

    def get_all_records(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        """Return all cached records from the result store.

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
            self._record_path, record_id_column=record_id_column
        )
        if result_table is None or result_table.num_rows == 0:
            return None
        return result_table
