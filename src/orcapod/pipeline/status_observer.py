"""Run status observer for orcapod pipelines.

Provides ``StatusObserver``, a drop-in observer that records per-packet
execution state transitions (``RUNNING``, ``SUCCESS``, ``FAILED``) to any
``ArrowDatabaseProtocol`` implementation (in-memory, Delta Lake, etc.).

Example::

    from orcapod.pipeline.status_observer import StatusObserver
    from orcapod.pipeline import SyncPipelineOrchestrator
    from orcapod.databases import InMemoryArrowDatabase

    obs = StatusObserver(status_database=InMemoryArrowDatabase())
    pipeline.run(observer=obs)

    # Inspect run status
    status = obs.get_status()  # pyarrow.Table
    status.to_pandas()         # pandas DataFrame

Status schema (fixed columns):
    Fixed columns are prefixed with ``_status_`` to follow system column
    conventions and avoid collision with user-defined tag column names.

    - ``_status_id`` (large_utf8): UUID7 unique to this status event.
    - ``_status_run_id`` (large_utf8): UUID of the pipeline run (from ``on_run_start``).
    - ``_status_pipeline_uri`` (large_utf8): Opaque URI identifying the pipeline version
      that produced this event (e.g. ``"my_pipeline@a1b2c3d4e5f6a1b2"``).  Set to ``""``
      when the pipeline URI is unknown (e.g. observer used outside a ``Pipeline.run()``
      context).
    - ``_status_state`` (large_utf8): ``RUNNING``, ``SUCCESS``, ``FAILED``, or ``CACHED``.
    - ``_status_timestamp`` (large_utf8): ISO-8601 UTC timestamp.
    - ``_status_error_summary`` (large_utf8): Brief error on ``FAILED``; ``None`` otherwise.

    In addition, each tag key from the packet's tag becomes a separate
    ``large_utf8`` column (queryable, not JSON-encoded).  Tag columns use
    bare names (no prefix), so they are always distinguishable from fixed
    columns.

Status storage:
    Status events are stored at ``DEFAULT_STATUS_PATH`` within the
    node-identity-scoped database.  When ``contextualize(*identity_path)``
    is called, the database is scoped via ``db.at(*identity_path)`` so each
    node writes to its own storage path.  Node identity is encoded in the
    path, not in column values.

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
    """Writes node execution status to a pre-scoped status database.

    Instantiate once, outside the pipeline, and pass to the orchestrator::

        obs = StatusObserver(status_database=InMemoryArrowDatabase())
        pipeline.run(observer=obs)

        # After the run, read back status:
        status_table = obs.get_status()   # pyarrow.Table

    For scoped storage (e.g. per-pipeline namespace), pass a pre-scoped
    database::

        obs = StatusObserver(status_database=db.at("my_pipeline", "_status"))

    Args:
        status_database: Any ``ArrowDatabaseProtocol`` instance.  May be
            a root database or a scoped view created via ``db.at(...)``.
    """

    def __init__(
        self,
        status_database: ArrowDatabaseProtocol,
    ) -> None:
        self._db = status_database
        self._current_run_id: str = ""
        self._current_pipeline_uri: str = ""
        # Maps node_label → tag_schema for in-flight nodes.
        # Populated by on_node_start; cleared by on_node_end.
        self._tag_schema_per_node: dict[str, SchemaLike] = {}

    # -- contextualize --

    def contextualize(self, *identity_path: str) -> "_ContextualizedStatusObserver":
        """Return a contextualized wrapper scoped to the given identity path."""
        if not identity_path:
            raise ValueError(
                "StatusObserver.contextualize() requires a non-empty identity_path."
            )
        ctx = _ContextualizedStatusObserver(self._db.at(*identity_path))
        ctx._current_run_id = self._current_run_id
        ctx._current_pipeline_uri = self._current_pipeline_uri
        return ctx

    # -- lifecycle hooks --

    def on_run_start(
        self,
        run_id: str,
        pipeline_uri: str = "",
    ) -> None:
        self._current_run_id = run_id
        self._current_pipeline_uri = pipeline_uri
        self._tag_schema_per_node.clear()

    def on_run_end(self, run_id: str) -> None:
        self._tag_schema_per_node.clear()

    def on_node_start(
        self,
        node_label: str,
        node_hash: str,
        tag_schema: SchemaLike | None = None,
    ) -> None:
        self._tag_schema_per_node[node_label] = tag_schema or {}

    def on_node_end(
        self,
        node_label: str,
        node_hash: str,
    ) -> None:
        self._tag_schema_per_node.pop(node_label, None)

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
        self._write_event(node_label, tag, state="FAILED", error=error)

    def create_packet_logger(
        self,
        tag: TagProtocol,
        packet: PacketProtocol,
    ) -> NoOpLogger:
        """Return a no-op logger.

        Status events are written from observer hooks, not from the
        packet logger.
        """
        return _NOOP_LOGGER

    # -- convenience --

    def get_status(self) -> "pa.Table | None":
        """Return all status records aggregated across all node sub-paths.

        Reads from all node-scoped sub-paths within this observer's database,
        concatenating results. Falls back to a direct read for DBs that don't
        support ``list_sources``.

        Returns:
            ``None`` if no status events have been written yet.
        """
        import pyarrow as pa

        try:
            sources = self._db.list_sources()
        except (AttributeError, NotImplementedError):
            # Fall back to direct read for DBs that don't implement list_sources
            return self._db.get_all_records(DEFAULT_STATUS_PATH)

        parts = []
        for src in sources:
            if src[-len(DEFAULT_STATUS_PATH):] == DEFAULT_STATUS_PATH:
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
            A dict with ``"type": "status"`` and a ``"database"`` key.
        """
        db_config = self._db.to_config(db_registry=db_registry)
        return {
            "type": "status",
            "database": db_config,
        }

    @classmethod
    def from_config(
        cls, config: dict, db_registry: dict | None = None
    ) -> "StatusObserver":
        """Reconstruct a ``StatusObserver`` from a config dict.

        Args:
            config: Dict as produced by ``to_config``.
            db_registry: Optional plain dict (output of
                ``DatabaseRegistry.to_dict()``) used to resolve scoped
                database references.

        Returns:
            A new ``StatusObserver`` instance.
        """
        from orcapod.pipeline.serialization import resolve_database_from_config

        db = resolve_database_from_config(
            config["database"], db_registry=db_registry
        )
        return cls(db)

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

        tag_schema = self._tag_schema_per_node.get(node_label, {})

        status_id = str(uuid7())
        timestamp = datetime.now(timezone.utc).isoformat()

        columns: dict[str, pa.Array] = {
            "_status_id":            pa.array([status_id],       type=pa.large_utf8()),
            "_status_run_id":        pa.array([self._current_run_id], type=pa.large_utf8()),
            "_status_pipeline_uri":  pa.array([self._current_pipeline_uri], type=pa.large_utf8()),
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
            self._db.add_record(DEFAULT_STATUS_PATH, status_id, row, flush=True)
        except Exception:
            logger.exception(
                "StatusObserver: failed to write status event for node=%s state=%s",
                node_label,
                state,
            )


class _ContextualizedStatusObserver:
    """ExecutionObserverProtocol implementation bound to a specific node identity path.

    One instance is created per node per run via StatusObserver.contextualize().
    Instances should NOT be shared across concurrent node executions — each node
    must get its own instance from contextualize().

    The database is pre-scoped to the node identity path, so writes go to the
    correct per-node storage location automatically.
    """

    def __init__(
        self,
        db: ArrowDatabaseProtocol,
    ) -> None:
        self._db = db
        self._current_run_id: str = ""
        self._current_pipeline_uri: str = ""
        self._tag_schema: SchemaLike = {}

    def contextualize(self, *identity_path: str) -> "_ContextualizedStatusObserver":
        """Re-contextualize with a new identity path (scopes the DB)."""
        return _ContextualizedStatusObserver(self._db.at(*identity_path))

    def on_run_start(
        self,
        run_id: str,
        pipeline_uri: str = "",
    ) -> None:
        self._current_run_id = run_id
        self._current_pipeline_uri = pipeline_uri

    def on_run_end(self, run_id: str) -> None:
        pass

    def on_node_start(
        self,
        node_label: str,
        node_hash: str,
        tag_schema: SchemaLike | None = None,
    ) -> None:
        self._tag_schema = tag_schema or {}

    def on_node_end(
        self,
        node_label: str,
        node_hash: str,
    ) -> None:
        self._tag_schema = {}

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
        self,
        node_label: str,
        tag: TagProtocol,
        packet: PacketProtocol,
        error: Exception,
    ) -> None:
        self._write_event(node_label, tag, state="FAILED", error=error)

    def create_packet_logger(
        self,
        tag: TagProtocol,
        packet: PacketProtocol,
    ) -> NoOpLogger:
        return _NOOP_LOGGER

    # -- internal --

    def _write_event(
        self,
        node_label: str,
        tag: TagProtocol,
        state: str,
        error: Exception | None = None,
    ) -> None:
        """Build and write a single status event row to the scoped database."""
        import pyarrow as pa

        status_id = str(uuid7())
        timestamp = datetime.now(timezone.utc).isoformat()

        columns: dict[str, pa.Array] = {
            "_status_id":            pa.array([status_id],               type=pa.large_utf8()),
            "_status_run_id":        pa.array([self._current_run_id],     type=pa.large_utf8()),
            "_status_pipeline_uri":  pa.array([self._current_pipeline_uri], type=pa.large_utf8()),
            "_status_state":         pa.array([state],                    type=pa.large_utf8()),
            "_status_timestamp":     pa.array([timestamp],                type=pa.large_utf8()),
            "_status_error_summary": pa.array(
                [str(error) if error is not None else None],
                type=pa.large_utf8(),
            ),
        }

        for key in self._tag_schema:
            value = tag.get(key, None)
            columns[key] = pa.array(
                [str(value) if value is not None else None],
                type=pa.large_utf8(),
            )

        row = pa.table(columns)
        # TODO: re-evaluate broad exception absorption — ideally narrow to DB-specific
        # errors (e.g. IOError, ArrowInvalid) once the full set of failure modes is known.
        try:
            self._db.add_record(DEFAULT_STATUS_PATH, status_id, row, flush=True)
        except Exception:
            logger.exception(
                "_ContextualizedStatusObserver: failed to write status event "
                "for node=%s state=%s",
                node_label,
                state,
            )
