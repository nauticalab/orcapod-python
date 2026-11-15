"""ResultCache — shared result caching logic for CachedDataFunction and CachedFunctionPod.

Owns the database, record path, lookup (with match strategy), store,
conflict resolution, and auto-flush behavior.  Both ``CachedDataFunction``
and ``CachedFunctionPod`` delegate to a ``ResultCache`` instance.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from orcapod.errors import OrcapodDatabaseReadError, OrcapodDatabaseWriteError
from orcapod.contexts.stored import (
    StaleTables,
    keep_rows_of_data_context,
    require_same_data_context,
    account_for_stale_tables,
    find_stale_tables,
    report_rows_of_other_data_contexts,
    with_data_context,
)
from orcapod.protocols.core_protocols import DataProtocol
from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
from orcapod.system_constants import (
    OLD_RESULT_DB_SCHEMA_VERSIONS,
    RESULT_DB_SCHEMA_VERSION,
    constants,
)
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.protocols.core_protocols.datagrams import DatagramProtocol
else:
    pa = LazyModule("pyarrow")

logger = logging.getLogger(__name__)


class ResultCache:
    """Shared result caching backed by an ``ArrowDatabaseProtocol``.

    Provides lookup (by input data hash + optional additional constraints),
    store (output data + variation/execution metadata + timestamp), conflict
    resolution (most-recent-timestamp wins), and auto-flush.

    The match strategy is extensible: the default lookup matches on
    ``INPUT_DATA_HASH_COL`` only, but callers can supply additional
    constraints (e.g. function variation columns) to narrow the match.
    This is the hook for future match-tier support (see design_issues/ P6).

    Args:
        result_database: The database to store/retrieve cached results.
        record_path: The record path tuple for scoping records in the database.
        auto_flush: If True, flush the database after each database operation.
    """

    # Meta column indicating whether the result was freshly computed
    RESULT_COMPUTED_FLAG = f"{constants.META_PREFIX}computed"

    def __init__(
        self,
        result_database: ArrowDatabaseProtocol,
        record_path: tuple[str, ...],
        auto_flush: bool = True,
        delivers_side_effects: bool = False,
        name_path: tuple[str, ...] | None = None,
    ) -> None:
        """Create a result cache over one result table.

        Args:
            result_database: The store holding the result table.
            record_path: Path of the result table without its layout component.
            auto_flush: Whether to flush the database after each stored result.
            delivers_side_effects: Whether computing a result is an external action
                (a function pod that takes the invocation context). Such a cache
                fails, instead of computing again, when it finds results stored
                under another data context or at an older table layout.
            name_path: The beginning of ``record_path`` up to the function's name,
                the part that holds no hash. Result tables written before v0.3 are
                looked for under it, whatever hashes their paths hold (ITL-954).
                ``None`` looks at the paths computed from ``record_path`` only.
        """
        self._result_database = result_database
        self._record_path = record_path
        self._auto_flush = auto_flush
        self._delivers_side_effects = delivers_side_effects
        self._name_path = name_path
        # Set once a read has reported the stored rows it skipped.
        self._skipped_rows_reported = False
        # Set once stored results of other generations have been accounted for.
        self._stale_tables_checked = False

    @property
    def result_database(self) -> ArrowDatabaseProtocol:
        """The underlying database."""
        return self._result_database

    @property
    def record_path(self) -> tuple[str, ...]:
        """The versioned path where records are stored.

        Returns ``_record_path + (RESULT_DB_SCHEMA_VERSION,)``: the actual storage
        location of result records. The layout version is the last component.
        """
        return self._versioned_record_path

    @property
    def _versioned_record_path(self) -> tuple[str, ...]:
        """Result DB path with the current schema version suffix appended."""
        return self._record_path + (RESULT_DB_SCHEMA_VERSION,)

    def set_auto_flush(self, on: bool = True) -> None:
        """Set auto-flush behavior."""
        self._auto_flush = on

    def stale_tables(
        self, data_context_key: str, include_current: bool = True
    ) -> StaleTables:
        """Find stored results of other generations: rows this cache will not use.

        Result tables at an older layout (``rdb_v1``, or no layout component), at
        this cache's own path and anywhere under its function's name (ITL-954),
        and, for a cache whose function delivers side effects, rows of another data
        context in the current table (ITL-944). The tables are only read.

        Args:
            data_context_key: Key of the caller's data context.
            include_current: Whether to read the current table and count its rows
                of another data context.

        Returns:
            What ``find_stale_tables`` found; empty when there is nothing.
        """
        return find_stale_tables(
            self._result_database,
            # Orcapod v0.1 stored the table at the path without a layout component.
            tuple(
                self._record_path + (layout,)
                for layout in OLD_RESULT_DB_SCHEMA_VERSIONS
            )
            + (self._record_path,),
            current=(self._versioned_record_path, data_context_key)
            if self._delivers_side_effects and include_current
            else None,
            name_path=self._name_path,
        )

    def stale_tables_accounted_for(self) -> None:
        """Record that the owner of this cache accounted for what ``stale_tables`` found.

        A node that delivers side effects accounts for its result tables together
        with its pipeline table, in one error or one warning; this cache then does
        not report or refuse them a second time.
        """
        self._stale_tables_checked = True

    def _account_for_stale_tables(self, data_context_key: str) -> None:
        """Report or refuse, once per cache object, stored results of other generations.

        Raises:
            SideEffectLogDataContextError: If this cache's function delivers side
                effects and such results exist; nothing has been computed then.
        """
        if self._stale_tables_checked:
            return
        account_for_stale_tables(
            self.stale_tables(data_context_key),
            delivers_side_effects=self._delivers_side_effects,
            current_context=data_context_key,
        )
        self._stale_tables_checked = True

    def lookup(
        self,
        input_data: DataProtocol,
        additional_constraints: dict[str, Any] | None = None,
        *,
        data_context_key: str,
    ) -> DataProtocol | None:
        """Look up a cached output data for *input_data*.

        The default match is by ``INPUT_DATA_HASH_COL`` among the rows written under
        the data context ``data_context_key``: a result stored under another data
        context for the same input hash is not a match (ITL-944).
        *additional_constraints* can narrow the match further (e.g. by
        function variation hash for stricter cache invalidation).

        If multiple records match, the most recent (by timestamp) wins.

        Args:
            input_data: The input data whose content hash is the
                primary lookup key.
            additional_constraints: Optional extra column-value pairs to
                include in the lookup query. Values may be ``bytes`` (for
                binary hash columns) or other scalar types (e.g. ``str``
                for ``function_name``).
            data_context_key: Key of the caller's data context. Only rows written
                under it are matched, and the returned data carries it.

        Returns:
            The cached output data with ``RESULT_COMPUTED_FLAG: False``
            in its meta, or ``None`` if no match was found.
        """
        from orcapod.core.datagrams import Data

        require_same_data_context(input_data.data_context_key, data_context_key)
        self._account_for_stale_tables(data_context_key)

        RECORD_ID_COL = "_record_id"

        constraints: dict[str, Any] = {
            constants.INPUT_DATA_HASH_COL: input_data.content_hash().to_prefixed_digest(),
        }
        if additional_constraints:
            if constants.DATA_CONTEXT_COL in additional_constraints:
                raise ValueError(
                    f"additional_constraints must not constrain "
                    f"{constants.DATA_CONTEXT_COL!r}: a lookup always matches the "
                    "caller's data context only."
                )
            constraints.update(additional_constraints)
        constraints[constants.DATA_CONTEXT_COL] = data_context_key

        result_table = self._result_database.get_records_with_column_value(
            self._versioned_record_path,
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

        record_id_bytes = result_table.to_pylist()[0][RECORD_ID_COL]
        # Convert bytes back to uuid.UUID (stored as binary(16) in the DB)
        record_uuid = (
            uuid.UUID(bytes=bytes(record_id_bytes))
            if record_id_bytes is not None
            else None
        )
        # Drop lookup columns from the returned data
        drop_cols = [RECORD_ID_COL] + [
            c for c in constraints if c in result_table.column_names
        ]
        result_table = result_table.drop_columns(drop_cols)

        return Data(
            result_table,
            record_uuid=record_uuid,
            meta_info={self.RESULT_COMPUTED_FLAG: False},
            data_context=data_context_key,
        )

    def store(
        self,
        input_data: DataProtocol,
        output_data: DataProtocol,
        variation_datagram: "DatagramProtocol",
        execution_datagram: "DatagramProtocol",
        skip_duplicates: bool = False,
        *,
        data_context_key: str,
    ) -> None:
        """Store an output data in the cache.

        Stores the output data data alongside function variation and
        execution metadata (as Datagrams), input data hash, a timestamp and the
        key of the data context that wrote the row (``__data_context``).

        Args:
            input_data: The input data (used for its content hash).
            output_data: The computed output data to store.
            variation_datagram: Function variation metadata as a Datagram.
            execution_datagram: Execution environment metadata as a Datagram.
            skip_duplicates: If True, silently skip if a record with the
                same ID already exists.
            data_context_key: Key of the caller's data context, stored on the row.
        """
        require_same_data_context(input_data.data_context_key, data_context_key)
        require_same_data_context(output_data.data_context_key, data_context_key)
        self._account_for_stale_tables(data_context_key)

        data_table = output_data.as_table(columns={"source": True})

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

        # Add input data hash as large_binary at position 0 (v1 schema).
        data_table = data_table.add_column(
            0,
            constants.INPUT_DATA_HASH_COL,
            pa.array(
                [input_data.content_hash().to_prefixed_digest()], type=pa.large_binary()
            ),
        )

        data_table = data_table.append_column(
            constants.POD_TIMESTAMP,
            pa.array([datetime.now(timezone.utc)], type=pa.timestamp("us", tz="UTC")),
        )
        data_table = with_data_context(data_table, data_context_key)

        try:
            self._result_database.add_record(
                self._versioned_record_path,
                output_data.datagram_uuid.bytes,
                data_table,
                skip_duplicates=skip_duplicates,
            )
        except OrcapodDatabaseReadError:
            # The stored table could not be read: report the read failure itself.
            raise
        except Exception as exc:
            raise OrcapodDatabaseWriteError(self._versioned_record_path) from exc
        if self._auto_flush:
            try:
                self._result_database.flush()
            except OrcapodDatabaseReadError:
                # The stored table could not be read: report the read failure itself.
                raise
            except Exception as exc:
                # Views of one database share their pending writes, so the batch that
                # failed may be an earlier row's or another table's.
                raise OrcapodDatabaseWriteError(
                    self._result_database.base_path, flush=True
                ) from exc

    def get_all_records(
        self, data_context_key: str, include_system_columns: bool = False
    ) -> "pa.Table | None":
        """Return the cached records written under the data context ``data_context_key``.

        Rows written under another data context are not current entries; they stay
        in the table and are not returned (ITL-944). The ``__data_context`` column is
        not part of the returned table.

        Args:
            data_context_key: Key of the caller's data context.
            include_system_columns: If True, include system columns
                (e.g. record_id) in the result.

        Returns:
            A PyArrow table of cached results, or ``None`` if empty.
        """
        self._account_for_stale_tables(data_context_key)

        record_id_column = constants.DATA_RECORD_ID if include_system_columns else None
        result_table = self._result_database.get_all_records(
            self._versioned_record_path, record_id_column=record_id_column
        )
        if result_table is None:
            return None
        stored_rows = result_table.num_rows
        result_table = keep_rows_of_data_context(result_table, data_context_key)
        if result_table.num_rows < stored_rows and not self._skipped_rows_reported:
            self._skipped_rows_reported = True
            report_rows_of_other_data_contexts(
                self._result_database.base_path + self._versioned_record_path,
                stored_rows - result_table.num_rows,
            )
        if result_table.num_rows == 0:
            return None
        return result_table.drop([constants.DATA_CONTEXT_COL])
