"""Run status observer for orcapod pipelines.

Provides ``StatusObserver``, a drop-in observer that records per-packet
execution state transitions (``RUNNING``, ``SUCCESS``, ``FAILED``) to any
``ArrowDatabaseProtocol`` implementation (in-memory, Delta Lake, etc.).

Example::

    from orcapod.pipeline.status_observer import StatusObserver
    from orcapod.pipeline import SyncPipelineOrchestrator
    from orcapod.databases import InMemoryArrowDatabase

    obs = StatusObserver(status_database=InMemoryArrowDatabase())
    pipeline.run(orchestrator=SyncPipelineOrchestrator(observer=obs))

    # Inspect run status for a specific node
    status = obs.get_status(pipeline_path=node.pipeline_path)  # pyarrow.Table
    status.to_pandas()                                         # pandas DataFrame

Status schema (fixed columns):
    Fixed columns are prefixed with ``_status_`` to follow system column
    conventions and avoid collision with user-defined tag column names.

    - ``_status_id`` (large_utf8): UUID7 unique to this status event.
    - ``_status_run_id`` (large_utf8): UUID of the pipeline run (from ``on_run_start``).
    - ``_status_pipeline_uri`` (large_utf8): Opaque URI identifying the pipeline version
      that produced this event (e.g. ``"my_pipeline@a1b2c3d4e5f6a1b2"``).  Set to ``""``
      when the pipeline URI is unknown (e.g. observer used outside a ``Pipeline.run()``
      context).
    - ``_status_node_label`` (large_utf8): Label of the function node.
    - ``_status_node_hash`` (large_utf8): Content hash of the function node.
    - ``_status_state`` (large_utf8): ``RUNNING``, ``SUCCESS``, ``FAILED``, or ``CACHED``.
    - ``_status_timestamp`` (large_utf8): ISO-8601 UTC timestamp.
    - ``_status_error_summary`` (large_utf8): Brief error on ``FAILED``; ``None`` otherwise.

    In addition, each tag key from the packet's tag becomes a separate
    ``large_utf8`` column (queryable, not JSON-encoded).  Tag columns use
    bare names (no prefix), so they are always distinguishable from fixed
    columns.

Status storage:
    Status events are stored at a pipeline-path-mirrored location:
    ``pipeline_path[:1] + ("status",) + pipeline_path[1:]``.
    Each function node gets its own status table.  Use
    ``get_status(pipeline_path=node.pipeline_path)`` to retrieve
    node-specific status.

Append-only:
    Each state transition is a new row.  Current state for a (node, tag)
    combination within a run is the row with the latest ``_status_timestamp``.
    If a ``RUNNING`` event has no subsequent terminal event for the same
    ``run_id``, the process crashed.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from uuid_utils import uuid7

from orcapod.pipeline.observer import NoOpLogger
from orcapod.protocols.core_protocols import PacketProtocol, TagProtocol
from orcapod.types import SchemaLike

if TYPE_CHECKING:
    import pyarrow as pa

    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol

logger = logging.getLogger(__name__)

# Default path within the database where status rows are stored.
DEFAULT_STATUS_PATH: tuple[str, ...] = ("execution_status",)

_NOOP_LOGGER = NoOpLogger()


class StatusObserver:
    """Concrete observer that writes packet execution status to a database.

    Instantiate once, outside the pipeline, and pass to the orchestrator
    (directly or via a ``CompositeObserver``)::

        obs = StatusObserver(status_database=InMemoryArrowDatabase())
        orch = SyncPipelineOrchestrator(observer=obs)
        pipeline.run(orchestrator=orch)

        # After the run, read back status:
        status_table = obs.get_status()   # pyarrow.Table

    Args:
        status_database: Any ``ArrowDatabaseProtocol`` instance.
        status_path: Tuple of strings identifying the default table within
            the database.  Defaults to ``("execution_status",)``.
    """

    def __init__(
        self,
        status_database: ArrowDatabaseProtocol,
        status_path: tuple[str, ...] | None = None,
    ) -> None:
        self._db = status_database
        self._status_path = status_path or DEFAULT_STATUS_PATH
        self._current_run_id: str = ""
        self._current_pipeline_uri: str = ""
        # Tracks (node_hash, pipeline_path, tag_schema) per node_label,
        # populated by on_node_start.  Allows packet-level hooks (which
        # only receive node_label) to look up the node's identity,
        # storage path, and tag schema.
        self._node_context: dict[str, tuple[str, tuple[str, ...], SchemaLike]] = {}

    # -- contextualize --

    def contextualize(
        self, node_hash: str, node_label: str
    ) -> _ContextualizedStatusObserver:
        """Return a contextualized wrapper stamped with node identity."""
        return _ContextualizedStatusObserver(self, node_hash, node_label)

    # -- lifecycle hooks --

    def on_run_start(
        self,
        run_id: str,
        pipeline_uri: str = "",
    ) -> None:
        self._current_run_id = run_id
        self._current_pipeline_uri = pipeline_uri
        self._node_context.clear()

    def on_run_end(self, run_id: str) -> None:
        self._node_context.clear()

    def on_node_start(
        self,
        node_label: str,
        node_hash: str,
        pipeline_path: tuple[str, ...] = (),
        tag_schema: SchemaLike | None = None,
    ) -> None:
        self._node_context[node_label] = (node_hash, pipeline_path, tag_schema or {})

    def on_node_end(
        self,
        node_label: str,
        node_hash: str,
        pipeline_path: tuple[str, ...] = (),
    ) -> None:
        self._node_context.pop(node_label, None)

    def on_packet_start(
        self, node_label: str, tag: TagProtocol, packet: PacketProtocol
    ) -> None:
        self._write_event(node_label, tag, state="RUNNING")

    def on_packet_end(
        self,
        node_label: str,
        tag: TagProtocol,
        input_packet: PacketProtocol,
        output_packet: PacketProtocol | None,
        cached: bool,
    ) -> None:
        self._write_event(node_label, tag, state="CACHED" if cached else "SUCCESS")

    def on_packet_crash(
        self, node_label: str, tag: TagProtocol, packet: PacketProtocol, error: Exception
    ) -> None:
        self._write_event(
            node_label, tag, state="FAILED", error=error
        )

    def create_packet_logger(
        self,
        tag: TagProtocol,
        packet: PacketProtocol,
        pipeline_path: tuple[str, ...] = (),
    ) -> NoOpLogger:
        """Return a no-op logger.

        Status events are written from observer hooks, not from the
        packet logger.
        """
        return _NOOP_LOGGER

    # -- convenience --

    def get_status(
        self, pipeline_path: tuple[str, ...] | None = None
    ) -> pa.Table | None:
        """Read status rows from the database as a ``pyarrow.Table``.

        Args:
            pipeline_path: If provided, reads status for a specific node
                (mirrored path).  If ``None``, reads from the default
                status path.

        Returns:
            ``None`` if no status events have been written yet.
        """
        if pipeline_path is not None:
            status_path = pipeline_path[:1] + ("status",) + pipeline_path[1:]
        else:
            status_path = self._status_path
        return self._db.get_all_records(status_path)

    # -- internal --

    def _write_event(
        self,
        node_label: str,
        tag: TagProtocol,
        state: str,
        error: Exception | None = None,
    ) -> None:
        """Build and write a single status event row."""
        import pyarrow as pa

        node_hash, pipeline_path, tag_schema = self._node_context.get(
            node_label, ("", (), {})
        )

        # Compute mirrored status path
        if pipeline_path:
            status_path = pipeline_path[:1] + ("status",) + pipeline_path[1:]
        else:
            status_path = self._status_path

        status_id = str(uuid7())
        timestamp = datetime.now(timezone.utc).isoformat()

        columns: dict[str, pa.Array] = {
            "_status_id":            pa.array([status_id],       type=pa.large_utf8()),
            "_status_run_id":        pa.array([self._current_run_id], type=pa.large_utf8()),
            "_status_pipeline_uri":  pa.array([self._current_pipeline_uri], type=pa.large_utf8()),
            "_status_node_label":    pa.array([node_label],      type=pa.large_utf8()),
            "_status_node_hash":     pa.array([node_hash],       type=pa.large_utf8()),
            "_status_state":         pa.array([state],           type=pa.large_utf8()),
            "_status_timestamp":     pa.array([timestamp],       type=pa.large_utf8()),
            "_status_error_summary": pa.array(
                [str(error) if error is not None else None],
                type=pa.large_utf8(),
            ),
        }

        # Tag columns — use statically-known schema from on_node_start
        for key in tag_schema:
            value = tag.get(key, None)
            columns[key] = pa.array(
                [str(value) if value is not None else None],
                type=pa.large_utf8(),
            )

        row = pa.table(columns)
        try:
            self._db.add_record(status_path, status_id, row, flush=True)
        except Exception:
            logger.exception(
                "StatusObserver: failed to write status event for node=%s state=%s",
                node_label,
                state,
            )


class _ContextualizedStatusObserver:
    """Lightweight wrapper holding parent observer + node identity context.

    Created by ``StatusObserver.contextualize()``.  All lifecycle hooks
    delegate to the parent.
    """

    def __init__(
        self,
        parent: StatusObserver,
        node_hash: str,
        node_label: str,
    ) -> None:
        self._parent = parent

    def contextualize(
        self, node_hash: str, node_label: str
    ) -> _ContextualizedStatusObserver:
        """Re-contextualize (returns a new wrapper with updated identity)."""
        return _ContextualizedStatusObserver(self._parent, node_hash, node_label)

    def on_run_start(
        self,
        run_id: str,
        pipeline_uri: str = "",
    ) -> None:
        self._parent.on_run_start(run_id, pipeline_uri=pipeline_uri)

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
        self._parent.on_packet_end(
            node_label, tag, input_packet, output_packet, cached
        )

    def on_packet_crash(
        self, node_label: str, tag: TagProtocol, packet: PacketProtocol, error: Exception
    ) -> None:
        self._parent.on_packet_crash(node_label, tag, packet, error)

    def create_packet_logger(
        self,
        tag: TagProtocol,
        packet: PacketProtocol,
        pipeline_path: tuple[str, ...] = (),
    ) -> NoOpLogger:
        return _NOOP_LOGGER
