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
    pipeline.run(orchestrator=SyncPipelineOrchestrator(observer=obs))

    # Inspect captured logs
    logs = obs.get_logs()           # pyarrow.Table
    logs.to_pandas()                # pandas DataFrame

Log schema (fixed columns):
    Fixed columns are prefixed with ``_log_`` to follow system column conventions
    and avoid collision with user-defined tag column names.

    - ``_log_id`` (large_utf8): UUID unique to this log entry.
    - ``_log_run_id`` (large_utf8): UUID of the pipeline run (from ``on_run_start``).
    - ``_log_node_label`` (large_utf8): Label of the function node.
    - ``_log_node_hash`` (large_utf8): Content hash of the function node.
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
    All logs are written to a single flat table at ``DEFAULT_LOG_PATH``
    within the provided ``log_database``.  Pass a pre-scoped database
    (via ``db.at("pipeline_name", "_log")``) to namespace logs.
    Node identity is queryable via the ``_log_node_label`` column.
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
    (run_id, node_label, node_hash, tag data) so the caller only needs to
    pass the `CapturedLogs` payload.

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
        node_label: str = "",
        node_hash: str = "",
    ) -> None:
        self._db = db
        self._log_path = log_path
        self._run_id = run_id
        self._node_label = node_label
        self._node_hash = node_hash
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
            "_log_node_label": pa.array([self._node_label],      type=pa.large_utf8()),
            "_log_node_hash":  pa.array([self._node_hash],       type=pa.large_utf8()),
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
                "LoggingObserver: failed to write log row for node=%s",
                self._node_label,
            )


class LoggingObserver:
    """Writes packet-level logs to a pre-scoped log database.

    Instantiate once, outside the pipeline, and pass to the orchestrator::

        obs = LoggingObserver(log_database=InMemoryArrowDatabase())
        orch = SyncPipelineOrchestrator(observer=obs)
        pipeline.run(orchestrator=orch)

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
        """Return a contextualized wrapper bound to the given identity path."""
        return _ContextualizedLoggingObserver(self._db, identity_path, self._current_run_id)

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
        """Return a `PacketLogger` bound to *tag* context.

        Warning:
            This method **must** be called on a *contextualized* observer
            (the object returned by `contextualize`), not on the root
            ``LoggingObserver`` directly.  Calling it on the root observer
            produces a ``PacketLogger`` where ``_log_node_label`` and
            ``_log_node_hash`` are both written as ``""`` (empty string),
            because no node identity has been stamped yet.

            Correct usage (mirrors how ``FunctionNode`` calls it)::

                ctx_obs = observer.contextualize(node_hash, node_label)
                pkt_logger = ctx_obs.create_packet_logger(tag, packet)
        """
        tag_data = dict(tag)
        return PacketLogger(
            db=self._db,
            log_path=DEFAULT_LOG_PATH,
            run_id=self._current_run_id,
            tag_data=tag_data,
        )

    # -- convenience --

    def get_logs(self) -> pa.Table | None:
        """Read all log rows from the database as a `pyarrow.Table`.

        Returns all log rows for this observer's database, regardless of node.
        Node-specific logs can be filtered via the ``_log_node_label`` column.

        Returns ``None`` if no logs have been written yet.
        """
        return self._db.get_all_records(DEFAULT_LOG_PATH)

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
        return {
            "type": "logging",
            "database": self._db.to_config(db_registry=db_registry),
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
    """

    def __init__(
        self,
        db: ArrowDatabaseProtocol,
        identity_path: tuple[str, ...],
        run_id: str = "",
    ) -> None:
        if not identity_path:
            raise ValueError(
                "_ContextualizedLoggingObserver requires a non-empty identity_path. "
                "Call LoggingObserver.contextualize(*identity_path) with at least one component."
            )
        self._db = db
        self._identity_path = identity_path
        self._run_id = run_id

    def contextualize(self, *identity_path: str) -> "_ContextualizedLoggingObserver":
        """Re-contextualize with a new identity path."""
        return _ContextualizedLoggingObserver(self._db, identity_path, self._run_id)

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

        Logs are written at ``DEFAULT_LOG_PATH`` within the database.
        Node identity (hash and label) is extracted from ``self._identity_path``
        and stored as columns in each log row.
        """
        tag_data = dict(tag)

        # identity_path is (node_hash, node_label) as passed from FunctionNode
        node_hash = self._identity_path[0] if len(self._identity_path) > 0 else ""
        node_label = self._identity_path[1] if len(self._identity_path) > 1 else ""

        return PacketLogger(
            db=self._db,
            log_path=DEFAULT_LOG_PATH,
            run_id=self._run_id,
            node_label=node_label,
            node_hash=node_hash,
            tag_data=tag_data,
        )
