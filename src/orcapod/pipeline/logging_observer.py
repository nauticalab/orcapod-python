"""Concrete logging observer for orcapod pipelines.

Provides ``LoggingObserver``, a drop-in observer that captures stdout,
stderr, Python logging, and tracebacks from every packet execution and writes
structured log rows to any ``ArrowDatabaseProtocol`` implementation
(in-memory, Delta Lake, etc.).

Example::

    from orcapod.pipeline.logging_observer import LoggingObserver
    from orcapod.pipeline import SyncPipelineOrchestrator
    from orcapod.databases import InMemoryArrowDatabase

    obs = LoggingObserver(log_database=InMemoryArrowDatabase())
    pipeline.run(observer=obs)

    # Inspect captured logs
    logs = obs.get_logs()           # pyarrow.Table
    logs.to_pandas()                # pandas DataFrame

Log schema (fixed columns):
    Fixed columns are prefixed with ``_log_`` to follow system column conventions
    and avoid collision with user-defined tag column names.

    - ``_log_id`` (large_utf8): UUID unique to this log entry.
    - ``_log_run_id`` (large_utf8): UUID of the pipeline run (from ``on_run_start``).
    - ``_log_stdout_log`` (large_utf8): Captured standard output.
    - ``_log_stderr_log`` (large_utf8): Captured standard error.
    - ``_log_python_logs`` (large_utf8): Python logging output captured during execution.
    - ``_log_traceback`` (large_utf8): Full traceback on failure; ``None`` on success.
    - ``_log_success`` (bool): ``True`` if the packet function returned normally.
    - ``_log_timestamp`` (large_utf8): ISO-8601 UTC timestamp when ``record()`` was called.

    In addition, each tag key from the packet's tag becomes a separate
    ``large_utf8`` column (queryable, not JSON-encoded).  Tag columns use
    bare names (no prefix), so they are always distinguishable from fixed
    columns.

Log storage:
    Logs are stored at ``DEFAULT_LOG_PATH`` within the node-identity-scoped
    database.  When ``contextualize(*identity_path)`` is called, the database
    is scoped via ``db.at(*identity_path)`` so each node writes to its own
    storage path.  Node identity is encoded in the path, not in column values.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from uuid_utils import uuid7

from orcapod.pipeline.logging_capture import install_capture_streams
from orcapod.protocols.core_protocols import PacketProtocol, TagProtocol

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol

logger = logging.getLogger(__name__)

# Default path (table name) within the database where log rows are stored.
DEFAULT_LOG_PATH: tuple[str, ...] = ("execution_logs",)


class PacketLogger:
    """Context-bound logger created by `_ContextualizedLoggingObserver` per packet.

    Holds all context needed to write a structured log row
    (run_id, tag data) so the caller only needs to pass the `CapturedLogs` payload.

    Tag data is stored as individual queryable columns (not JSON) alongside
    the fixed log columns.

    This class is not intended to be instantiated directly — use
    `_ContextualizedLoggingObserver.create_packet_logger` instead.
    """

    def __init__(
        self,
        db: ArrowDatabaseProtocol,
        log_path: tuple[str, ...],
        run_id: str,
        tag_data: dict[str, Any],
    ) -> None:
        self._db = db
        self._log_path = log_path
        self._run_id = run_id
        self._tag_data = tag_data

    def record(self, **kwargs: Any) -> None:
        """Write one log row to the database.

        Args:
            **kwargs: Captured execution fields (e.g. ``stdout_log``, ``stderr_log``,
                ``python_logs``, ``traceback``, ``success``).  Each field is
                stored as a ``_log_``-prefixed column in the log table.
        """
        import pyarrow as pa

        log_id = str(uuid7())
        timestamp = datetime.now(timezone.utc).isoformat()

        # Context columns — prefixed with "_log_" to follow system column conventions
        columns: dict[str, pa.Array] = {
            "_log_id":         pa.array([log_id],               type=pa.large_utf8()),
            "_log_run_id":     pa.array([self._run_id],          type=pa.large_utf8()),
            "_log_timestamp":  pa.array([timestamp],             type=pa.large_utf8()),
        }

        # Execution output columns from kwargs — prefixed with "_log_"
        for key, value in kwargs.items():
            col_name = f"_log_{key}"
            if isinstance(value, bool):
                columns[col_name] = pa.array([value], type=pa.bool_())
            else:
                columns[col_name] = pa.array(
                    [str(value) if value is not None else None],
                    type=pa.large_utf8(),
                )

        # Dynamic tag columns — each tag key becomes its own column (unprefixed)
        for key, value in self._tag_data.items():
            columns[key] = pa.array([str(value)], type=pa.large_utf8())

        row = pa.table(columns)
        try:
            self._db.add_record(self._log_path, log_id, row, flush=True)
        except Exception:
            logger.exception(
                "LoggingObserver: failed to write log row",
            )


class LoggingObserver:
    """Writes packet-level logs to a pre-scoped log database.

    Instantiate once, outside the pipeline, and pass to the orchestrator::

        obs = LoggingObserver(log_database=InMemoryArrowDatabase())
        pipeline.run(observer=obs)

        # After the run, read back captured logs:
        logs_table = obs.get_logs()   # pyarrow.Table

    For scoped storage (e.g. per-pipeline namespace), pass a pre-scoped
    database::

        obs = LoggingObserver(log_database=db.at("my_pipeline", "_log"))

    Args:
        log_database: Any ``ArrowDatabaseProtocol`` instance.  May be
            a root database or a scoped view created via ``db.at(...)``.

    Note:
        Construction calls `install_capture_streams`
        so that stdout/stderr tee-capture is active from the moment the observer
        is created.
    """

    def __init__(
        self,
        log_database: ArrowDatabaseProtocol,
    ) -> None:
        self._db = log_database
        self._current_run_id: str = ""
        # Activate tee-capture as soon as the observer is created.
        install_capture_streams()

    def contextualize(self, *identity_path: str) -> "_ContextualizedLoggingObserver":
        """Return a contextualized wrapper scoped to the given identity path."""
        if not identity_path:
            raise ValueError(
                "LoggingObserver.contextualize() requires a non-empty identity_path."
            )
        return _ContextualizedLoggingObserver(self._db.at(*identity_path), self._current_run_id)

    # -- lifecycle hooks --

    def on_run_start(
        self,
        run_id: str,
        pipeline_uri: str = "",
    ) -> None:
        self._current_run_id = run_id

    def on_run_end(self, run_id: str) -> None:
        pass

    def on_node_start(
        self, node_label: str, node_hash: str, tag_schema=None
    ) -> None:
        pass

    def on_node_end(
        self, node_label: str, node_hash: str
    ) -> None:
        pass

    def on_packet_start(self, node_label: str, tag: TagProtocol, packet: PacketProtocol) -> None:
        pass

    def on_packet_end(
        self,
        node_label: str,
        tag: TagProtocol,
        input_packet: PacketProtocol,
        output_packet: PacketProtocol | None,
        cached: bool,
    ) -> None:
        pass

    def on_packet_crash(
        self, node_label: str, tag: TagProtocol, packet: PacketProtocol, error: Exception
    ) -> None:
        pass

    def create_packet_logger(
        self,
        tag: TagProtocol,
        packet: PacketProtocol,
    ) -> PacketLogger:
        """Return a `PacketLogger` bound to *tag* context using the root DB."""
        return PacketLogger(db=self._db, log_path=DEFAULT_LOG_PATH, run_id=self._current_run_id, tag_data=dict(tag))

    # -- convenience --

    def get_logs(self) -> "pa.Table | None":
        """Read all log rows aggregated across all node sub-paths.

        Reads from all node-scoped sub-paths within this observer's database,
        concatenating results. Falls back to a direct read for DBs that don't
        support ``list_sources``.

        Returns ``None`` if no logs have been written yet.
        """
        import pyarrow as pa

        try:
            sources = self._db.list_sources()
        except (NotImplementedError, Exception):
            return self._db.get_all_records(DEFAULT_LOG_PATH)

        parts = []
        for src in sources:
            if src[-len(DEFAULT_LOG_PATH):] == DEFAULT_LOG_PATH:
                tbl = self._db.get_all_records(src)
                if tbl is not None and tbl.num_rows > 0:
                    if "__record_id" in tbl.schema.names:
                        tbl = tbl.drop(["__record_id"])
                    parts.append(tbl)
        if not parts:
            return None
        if len(parts) == 1:
            return parts[0]
        return pa.concat_tables(parts, promote_options="default")

    # -- serialization --

    def to_config(self, db_registry: Any = None) -> dict:
        """Serialize this observer to a JSON-compatible config dict.

        Args:
            db_registry: Optional ``DatabaseRegistry`` instance.  When
                provided, the database config is registered and the
                emitted config uses the compact scoped reference format.

        Returns:
            A dict with ``"type": "logging"`` and a ``"database"`` key.
        """
        db_config = self._db.to_config(db_registry=db_registry)
        return {
            "type": "logging",
            "database": db_config,
        }

    @classmethod
    def from_config(
        cls, config: dict, db_registry: dict | None = None
    ) -> "LoggingObserver":
        """Reconstruct a ``LoggingObserver`` from a config dict.

        Args:
            config: Dict as produced by ``to_config``.
            db_registry: Optional plain dict (output of
                ``DatabaseRegistry.to_dict()``) used to resolve scoped
                database references.

        Returns:
            A new ``LoggingObserver`` instance.
        """
        from orcapod.pipeline.serialization import resolve_database_from_config

        db = resolve_database_from_config(
            config["database"], db_registry=db_registry
        )
        return cls(db)


class _ContextualizedLoggingObserver:
    """ExecutionObserverProtocol implementation bound to a specific node identity path.

    One instance per node per run, created via LoggingObserver.contextualize().
    Must NOT be shared across concurrent node executions.

    The database is pre-scoped to the node identity path so writes go to the
    correct per-node storage location automatically.
    """

    def __init__(
        self,
        db: ArrowDatabaseProtocol,
        run_id: str = "",
    ) -> None:
        self._db = db
        self._run_id = run_id

    def contextualize(self, *identity_path: str) -> "_ContextualizedLoggingObserver":
        """Re-contextualize with a new identity path (scopes the DB)."""
        return _ContextualizedLoggingObserver(self._db.at(*identity_path), self._run_id)

    def on_run_start(self, run_id: str, pipeline_uri: str = "") -> None:
        pass

    def on_run_end(self, run_id: str) -> None:
        pass

    def on_node_start(self, node_label: str, node_hash: str, tag_schema=None) -> None:
        pass

    def on_node_end(self, node_label: str, node_hash: str) -> None:
        pass

    def on_packet_start(self, node_label: str, tag: TagProtocol, packet: PacketProtocol) -> None:
        pass

    def on_packet_end(
        self,
        node_label: str,
        tag: TagProtocol,
        input_packet: PacketProtocol,
        output_packet: PacketProtocol | None,
        cached: bool,
    ) -> None:
        pass

    def on_packet_crash(
        self, node_label: str, tag: TagProtocol, packet: PacketProtocol, error: Exception
    ) -> None:
        pass

    def create_packet_logger(
        self,
        tag: TagProtocol,
        packet: PacketProtocol,
    ) -> PacketLogger:
        """Create a PacketLogger using context from this wrapper.

        Logs are written at ``DEFAULT_LOG_PATH`` within the scoped database.
        Node identity is encoded in the database path, not in column values.
        """
        tag_data = dict(tag)

        return PacketLogger(
            db=self._db,
            log_path=DEFAULT_LOG_PATH,
            run_id=self._run_id,
            tag_data=tag_data,
        )
