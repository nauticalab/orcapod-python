"""Concrete logging observer for orcapod pipelines.

Provides :class:`LoggingObserver`, a drop-in observer that captures stdout,
stderr, Python logging, and tracebacks from every packet execution and writes
structured log rows to any :class:`~orcapod.protocols.database_protocols.ArrowDatabaseProtocol`
(in-memory, Delta Lake, etc.).

Typical usage::

    from orcapod.pipeline.logging_observer import LoggingObserver
    from orcapod.pipeline import SyncPipelineOrchestrator
    from orcapod.databases import InMemoryArrowDatabase

    obs = LoggingObserver(log_database=InMemoryArrowDatabase())
    pipeline.run(orchestrator=SyncPipelineOrchestrator(observer=obs))

    # Inspect captured logs
    logs = obs.get_logs()           # pyarrow.Table
    logs.to_pandas()                # pandas DataFrame

Log schema (fixed columns)
--------------------------
.. list-table::
   :header-rows: 1

   * - Column
     - Type
     - Description
   * - ``log_id``
     - ``large_utf8``
     - UUID unique to this log entry
   * - ``run_id``
     - ``large_utf8``
     - UUID of the pipeline run (from ``on_run_start``)
   * - ``node_label``
     - ``large_utf8``
     - Label of the function node
   * - ``stdout``
     - ``large_utf8``
     - Captured standard output
   * - ``stderr``
     - ``large_utf8``
     - Captured standard error
   * - ``python_logs``
     - ``large_utf8``
     - Python ``logging`` output captured during execution
   * - ``traceback``
     - ``large_utf8``
     - Full traceback on failure; ``None`` on success
   * - ``success``
     - ``bool_``
     - ``True`` if the packet function returned normally
   * - ``timestamp``
     - ``large_utf8``
     - ISO-8601 UTC timestamp when ``record()`` was called

In addition, each tag key from the packet's tag becomes a separate
``large_utf8`` column (queryable, not JSON-encoded).

Log storage
-----------
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

from orcapod.pipeline.logging_capture import CapturedLogs, install_capture_streams

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol

logger = logging.getLogger(__name__)

# Default path (table name) within the database where log rows are stored.
DEFAULT_LOG_PATH: tuple[str, ...] = ("execution_logs",)


class PacketLogger:
    """Context-bound logger created by :class:`LoggingObserver` per packet.

    Holds all context needed to write a structured log row
    (run_id, node_label, tag data) so the caller only needs to pass the
    :class:`~orcapod.pipeline.logging_capture.CapturedLogs` payload.

    Tag data is stored as individual queryable columns (not JSON) alongside
    the fixed log columns.

    This class is not intended to be instantiated directly — use
    :meth:`LoggingObserver.create_packet_logger` instead.
    """

    def __init__(
        self,
        db: "ArrowDatabaseProtocol",
        log_path: tuple[str, ...],
        run_id: str,
        node_label: str,
        tag_data: dict[str, Any],
    ) -> None:
        self._db = db
        self._log_path = log_path
        self._run_id = run_id
        self._node_label = node_label
        self._tag_data = tag_data

    def record(self, captured: CapturedLogs) -> None:
        """Write one log row to the database."""
        import pyarrow as pa

        log_id = str(uuid7())
        timestamp = datetime.now(timezone.utc).isoformat()

        # Fixed columns
        columns: dict[str, pa.Array] = {
            "log_id":      pa.array([log_id],               type=pa.large_utf8()),
            "run_id":      pa.array([self._run_id],          type=pa.large_utf8()),
            "node_label":  pa.array([self._node_label],      type=pa.large_utf8()),
            "stdout":      pa.array([captured.stdout],        type=pa.large_utf8()),
            "stderr":      pa.array([captured.stderr],        type=pa.large_utf8()),
            "python_logs": pa.array([captured.python_logs],  type=pa.large_utf8()),
            "traceback":   pa.array([captured.traceback],    type=pa.large_utf8()),
            "success":     pa.array([captured.success],      type=pa.bool_()),
            "timestamp":   pa.array([timestamp],             type=pa.large_utf8()),
        }

        # Dynamic tag columns — each tag key becomes its own column
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
        log_database: "ArrowDatabaseProtocol",
        log_path: tuple[str, ...] | None = None,
    ) -> None:
        self._db = log_database
        self._log_path = log_path or DEFAULT_LOG_PATH
        self._current_run_id: str = ""
        # Activate tee-capture as soon as the observer is created.
        install_capture_streams()

    # -- lifecycle hooks --

    def on_run_start(self, run_id: str) -> None:
        self._current_run_id = run_id

    def on_run_end(self, run_id: str) -> None:
        pass

    def on_node_start(self, node: Any) -> None:
        pass

    def on_node_end(self, node: Any) -> None:
        pass

    def on_packet_start(self, node: Any, tag: Any, packet: Any) -> None:
        pass

    def on_packet_end(
        self,
        node: Any,
        tag: Any,
        input_packet: Any,
        output_packet: Any,
        cached: bool,
    ) -> None:
        pass

    def on_packet_crash(self, node: Any, tag: Any, packet: Any, error: Exception) -> None:
        pass

    def create_packet_logger(
        self,
        node: Any,
        tag: Any,
        packet: Any,
        pipeline_path: tuple[str, ...] = (),
    ) -> PacketLogger:
        """Return a :class:`PacketLogger` bound to *node* + *tag* context.

        Log rows are stored at a pipeline-path-mirrored location:
        ``pipeline_path[:1] + ("logs",) + pipeline_path[1:]``.  This gives
        each function node its own log table in the database.
        """
        node_label = getattr(node, "label", None) or getattr(node, "node_type", "unknown")
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
            node_label=node_label,
            tag_data=tag_data,
        )

    # -- convenience --

    def get_logs(
        self, pipeline_path: tuple[str, ...] | None = None
    ) -> "pa.Table | None":
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
