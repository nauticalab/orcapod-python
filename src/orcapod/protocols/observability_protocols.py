"""Observability protocols for pipeline execution tracking and logging.

Defines:

* ``PacketExecutionLoggerProtocol`` — receives captured I/O from a single
  packet execution and persists it to a configured sink.
* ``ExecutionObserverProtocol`` — lifecycle hooks for pipeline/node/packet
  events, plus a factory method for creating context-bound loggers.

Both follow the same runtime-checkable Protocol pattern used throughout the
rest of the orcapod codebase.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from orcapod.protocols.core_protocols import PacketProtocol, TagProtocol
from orcapod.types import SchemaLike


@runtime_checkable
class PacketExecutionLoggerProtocol(Protocol):
    """Receives captured execution output and persists it.

    A logger is *bound* to a specific packet execution context (node, tag,
    packet) when created by the Observer.  It knows the destination (e.g. a
    Delta Lake table) but does not know how the logs were collected — that is
    the executor's responsibility.

    The ``record`` method accepts arbitrary keyword arguments so that
    different executor types can log different fields without the protocol
    being tied to a specific data structure.
    """

    def record(self, **kwargs: Any) -> None:
        """Persist captured execution output.

        Called after every packet execution (success or failure), except for
        cache hits when ``log_cache_hits=False`` (the default).

        Args:
            **kwargs: Arbitrary captured fields (e.g. ``stdout_log``, ``stderr_log``,
                ``python_logs``, ``traceback``, ``success``).  The logger
                implementation decides how to persist them.
        """
        ...


@runtime_checkable
class ExecutionObserverProtocol(Protocol):
    """Observer protocol for pipeline execution lifecycle events.

    Instantiated once outside the pipeline and injected into the orchestrator.
    Provides hooks for lifecycle events at the run, node, and packet level, and
    acts as a factory for context-specific loggers.

    ``on_packet_start`` / ``on_packet_end`` / ``on_packet_crash`` are invoked
    only for function nodes.  ``on_node_start`` / ``on_node_end`` are invoked
    for all node types.

    Observers are *contextualized* per node via ``contextualize()``, which
    returns a lightweight wrapper stamped with node identity. The contextualized
    observer is used for all hooks and logger creation within that node.
    """

    def contextualize(
        self, *identity_path: str
    ) -> ExecutionObserverProtocol:
        """Return a copy of this observer bound to the given node identity path.

        The returned observer is a wrapper that calls through to the same
        underlying observer but doesn't need pipeline_path on each hook call.

        Args:
            *identity_path: Variable-length sequence of strings that together
                identify the node (e.g. pod_uri, schema, instance).

        Returns:
            An observer (possibly a lightweight wrapper) that carries the
            identity_path context for all subsequent calls.
        """
        ...

    def on_run_start(
        self,
        run_id: str,
        pipeline_uri: str = "",
    ) -> None:
        """Called at the very start of an orchestrator ``run()`` call.

        Args:
            run_id: A UUID string unique to this execution run.  All loggers
                created during the run will be stamped with this ID.
            pipeline_uri: An opaque URI string that the pipeline formats to
                describe itself for this run.  ``Pipeline`` sets this to
                ``"<name>@<snapshot_hash>"`` by default (e.g.
                ``"my_pipeline@a1b2c3d4e5f6a1b2"``), but the observer may
                treat it as an arbitrary correlation token.  The snapshot hash
                component changes whenever nodes are added, removed, or
                modified; the name component remains stable.  Observers that
                need node-level storage scoping should use the identity path
                supplied via ``contextualize()`` before each node is processed.
        """
        ...

    def on_run_end(self, run_id: str) -> None:
        """Called at the very end of an orchestrator ``run()`` call.

        Args:
            run_id: The same UUID passed to ``on_run_start``.
        """
        ...

    def on_node_start(
        self,
        node_label: str,
        node_hash: str,
        tag_schema: SchemaLike | None = None,
    ) -> None:
        """Called before a node begins processing its packets.

        Args:
            node_label: Human-readable label of the node.
            node_hash: Content hash of the node.
            tag_schema: The tag schema (including system tags) for this
                node's input stream.
        """
        ...

    def on_node_end(
        self,
        node_label: str,
        node_hash: str,
    ) -> None:
        """Called after a node finishes processing all packets.

        Args:
            node_label: Human-readable label of the node.
            node_hash: Content hash of the node.
        """
        ...

    def on_packet_start(
        self,
        node_label: str,
        tag: TagProtocol,
        packet: PacketProtocol,
    ) -> None:
        """Called before a packet is processed by a function node."""
        ...

    def on_packet_end(
        self,
        node_label: str,
        tag: TagProtocol,
        input_packet: PacketProtocol,
        output_packet: PacketProtocol | None,
        cached: bool,
    ) -> None:
        """Called after a packet is successfully processed (or served from cache).

        Args:
            cached: ``True`` when the result came from a database cache and
                the user function was not executed.
        """
        ...

    def on_packet_crash(
        self,
        node_label: str,
        tag: TagProtocol,
        packet: PacketProtocol,
        error: Exception,
    ) -> None:
        """Called when a packet's execution fails.

        Covers both user-function exceptions (captured on the worker) and
        system-level crashes (e.g. ``WorkerCrashedError`` from Ray).  The
        pipeline continues processing remaining packets rather than aborting.
        """
        ...

    def create_packet_logger(
        self,
        tag: TagProtocol,
        packet: PacketProtocol,
    ) -> PacketExecutionLoggerProtocol:
        """Create a context-bound logger for a single packet execution.

        The returned logger is pre-stamped with the node label, run ID, and
        packet identity so every ``record()`` call writes the correct context
        without the executor needing to know anything about the pipeline.

        Args:
            tag: The tag for the packet being processed.
            packet: The input packet being processed.
        """
        ...
