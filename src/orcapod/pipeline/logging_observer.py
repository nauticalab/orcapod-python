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
    Logs are stored at a pipeline-path-mirrored location:
    ``pipeline_path[:1] + ("logs",) + pipeline_path[1:]``.
    Each function node gets its own log table.  Use
    ``get_logs(pipeline_path=node.pipeline_path)`` to retrieve
    node-specific logs.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from uuid_utils import uuid7

from orcapod.pipeline.logging_capture import install_capture_streams
from orcapod.protocols.core_protocols import PacketProtocol, TagProtocol
from orcapod.types import SchemaLike

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol

logger = logging.getLogger(__name__)

# Default path (table name) within the database where log rows are stored.
DEFAULT_LOG_PATH: tuple[str, ...] = ("execution_logs",)


class PacketLogger:
    """Context-bound logger created by :class:`LoggingObserver` per packet.

    Holds all context needed to write a structured log row
    (run_id, node_label, node_hash, tag data) so the caller only needs to
    pass the :class:`~orcapod.pipeline.logging_capture.CapturedLogs` payload.

    Tag data is stored as individual queryable columns (not JSON) alongside
    the fixed log columns.

    This class is not intended to be instantiated directly — use
    :meth:`LoggingObserver.create_packet_logger` instead.
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


class _ContextualizedLoggingObserver:
    """Lightweight wrapper holding parent observer + node identity context.

    Created by :meth:`LoggingObserver.contextualize`. All lifecycle hooks
    and logger creation use the stamped ``node_hash`` and ``node_label``.
    """

    def __init__(
        self,
        parent: LoggingObserver,
        node_hash: str,
        node_label: str,
    ) -> None:
        self._parent = parent
        self._node_hash = node_hash
        self._node_label = node_label

    def contextualize(
        self, node_hash: str, node_label: str
    ) -> _ContextualizedLoggingObserver:
        """Re-contextualize (returns a new wrapper with updated identity)."""
        return _ContextualizedLoggingObserver(self._parent, node_hash, node_label)

    def on_run_start(self, run_id: str) -> None:
        self._parent.on_run_start(run_id)

    def on_run_end(self, run_id: str) -> None:
        self._parent.on_run_end(run_id)

    def on_node_start(
        self, node_label: str, node_hash: str, pipeline_path: tuple[str, ...] = (), tag_schema: SchemaLike | None = None
    ) -> None:
        self._parent.on_node_start(node_label, node_hash, pipeline_path=pipeline_path, tag_schema=tag_schema)

    def on_node_end(
        self, node_label: str, node_hash: str, pipeline_path: tuple[str, ...] = ()
    ) -> None:
        self._parent.on_node_end(node_label, node_hash, pipeline_path=pipeline_path)

    def on_packet_start(
        self, node_label: str, tag: TagProtocol, packet: PacketProtocol
    ) -> None:
        self._parent.on_packet_start(node_label, tag, packet)

    def on_packet_end(
        self,
        node_label: str,
        tag: TagProtocol,
        input_packet: PacketProtocol,
        output_packet: PacketProtocol | None,
        cached: bool,
    ) -> None:
        self._parent.on_packet_end(node_label, tag, input_packet, output_packet, cached)

    def on_packet_crash(
        self, node_label: str, tag: TagProtocol, packet: PacketProtocol, error: Exception
    ) -> None:
        self._parent.on_packet_crash(node_label, tag, packet, error)

    def create_packet_logger(
        self,
        tag: TagProtocol,
        packet: PacketProtocol,
        pipeline_path: tuple[str, ...] = (),
    ) -> PacketLogger:
        """Create a logger using context from this wrapper."""
        tag_data = dict(tag)

        # Compute mirrored log path
        if pipeline_path:
            log_path = pipeline_path[:1] + ("logs",) + pipeline_path[1:]
        else:
            log_path = self._parent._log_path

        return PacketLogger(
            db=self._parent._db,
            log_path=log_path,
            run_id=self._parent._current_run_id,
            node_label=self._node_label,
            node_hash=self._node_hash,
            tag_data=tag_data,
        )


class LoggingObserver:
    """Concrete observer that writes packet execution logs to a database.

    Instantiate once, outside the pipeline, and pass to the orchestrator::

        obs = LoggingObserver(log_database=InMemoryArrowDatabase())
        orch = SyncPipelineOrchestrator(observer=obs)
        pipeline.run(orchestrator=orch)

        # After the run, read back captured logs:
        logs_table = obs.get_logs()   # pyarrow.Table

    For async / Ray pipelines use :class:`~orcapod.pipeline.AsyncPipelineOrchestrator`
    with the same observer::

        orch = AsyncPipelineOrchestrator(observer=obs)
        pipeline.run(orchestrator=orch)

    Args:
        log_database: Any :class:`~orcapod.protocols.database_protocols.ArrowDatabaseProtocol`
            instance — :class:`~orcapod.databases.InMemoryArrowDatabase`,
            a Delta Lake database, etc.
        log_path: Tuple of strings identifying the table within the database.
            Defaults to ``("execution_logs",)``.

    Note:
        Construction calls :func:`~orcapod.pipeline.logging_capture.install_capture_streams`
        so that stdout/stderr tee-capture is active from the moment the observer
        is created.
    """

    def __init__(
        self,
        log_database: ArrowDatabaseProtocol,
        log_path: tuple[str, ...] | None = None,
    ) -> None:
        self._db = log_database
        self._log_path = log_path or DEFAULT_LOG_PATH
        self._current_run_id: str = ""
        # Activate tee-capture as soon as the observer is created.
        install_capture_streams()

    # -- contextualize --

    def contextualize(
        self, node_hash: str, node_label: str
    ) -> _ContextualizedLoggingObserver:
        """Return a contextualized wrapper stamped with node identity."""
        return _ContextualizedLoggingObserver(self, node_hash, node_label)

    # -- lifecycle hooks --

    def on_run_start(self, run_id: str) -> None:
        self._current_run_id = run_id

    def on_run_end(self, run_id: str) -> None:
        pass

    def on_node_start(
        self, node_label: str, node_hash: str, pipeline_path: tuple[str, ...] = (), tag_schema: SchemaLike | None = None
    ) -> None:
        pass

    def on_node_end(
        self, node_label: str, node_hash: str, pipeline_path: tuple[str, ...] = ()
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
        pipeline_path: tuple[str, ...] = (),
    ) -> PacketLogger:
        """Return a :class:`PacketLogger` bound to *tag* context.

        Log rows are stored at a pipeline-path-mirrored location:
        ``pipeline_path[:1] + ("logs",) + pipeline_path[1:]``.  This gives
        each function node its own log table in the database.

        Note:
            When called directly on ``LoggingObserver`` (not a contextualized
            wrapper), node_label and node_hash default to "unknown".  Prefer
            calling via a contextualized observer.
        """
        tag_data = dict(tag)

        # Compute mirrored log path
        if pipeline_path:
            log_path = pipeline_path[:1] + ("logs",) + pipeline_path[1:]
        else:
            log_path = self._log_path

        return PacketLogger(
            db=self._db,
            log_path=log_path,
            run_id=self._current_run_id,
            tag_data=tag_data,
        )

    # -- convenience --

    def get_logs(
        self, pipeline_path: tuple[str, ...] | None = None
    ) -> pa.Table | None:
        """Read log rows from the database as a :class:`pyarrow.Table`.

        Args:
            pipeline_path: If provided, reads logs for a specific node
                (mirrored path).  If ``None``, reads from the default
                log path.

        Returns ``None`` if no logs have been written yet.
        """
        if pipeline_path is not None:
            log_path = pipeline_path[:1] + ("logs",) + pipeline_path[1:]
        else:
            log_path = self._log_path
        return self._db.get_all_records(log_path)
