# Protocols for pipeline and nodes
from __future__ import annotations

from collections.abc import Hashable
from typing import TYPE_CHECKING, Protocol, TypeVar, runtime_checkable

from orcapod.protocols import core_protocols as cp

if TYPE_CHECKING:
    import pyarrow as pa
    from orcapod.databases.in_memory_databases import InMemoryArrowDatabase
    from orcapod.pipeline.dag import GraphProtocol


NodeT = TypeVar("NodeT", bound=Hashable)


@runtime_checkable
class PipelineProtocol(Protocol[NodeT]):
    """Structural protocol for ``Pipeline`` and ``PipelineJob``.

    Both ``Pipeline`` (``NodeT=GraphNode``) and ``PipelineJob``
    (``NodeT=JobNode``) satisfy this protocol.  Callers that only need
    DAG introspection can accept ``PipelineProtocol[Any]`` rather than
    importing the concrete classes.

    Note:
        The ``dag`` return type is ``GraphProtocol[NodeT]`` here (the abstract
        protocol).  Callers using the concrete classes receive the more
        specific ``OrcaDAG[NodeT]`` type.
    """

    @property
    def name(self) -> tuple[str, ...]:
        """Pipeline name as a tuple of path components."""
        ...

    @property
    def nodes(self) -> dict[str, NodeT]:
        """Copy of the compiled label -> node mapping."""
        ...

    @property
    def source_pods(self) -> dict[str, NodeT]:
        """Copy of compiled nodes that are source nodes (label → node)."""
        ...

    @property
    def function_pods(self) -> dict[str, NodeT]:
        """Copy of compiled nodes that are function-pod nodes (label → node)."""
        ...

    @property
    def operator_pods(self) -> dict[str, NodeT]:
        """Copy of compiled nodes that are operator-pod nodes (label → node)."""
        ...

    @property
    def dag(self) -> "GraphProtocol[NodeT]":
        """Node-object DAG for topology traversal and introspection."""
        ...

    def set_ephemeral_store(self, store: "InMemoryArrowDatabase | None") -> None:
        """Assign or remove the ephemeral result store for all nodes.

        Propagates ``store`` to every node in the pipeline by calling
        ``node.set_ephemeral_store(store)`` on each. Nodes that do not
        support ephemeral storage (e.g. operator nodes in v1) treat this
        as a no-op.

        Pass ``None`` to detach the ephemeral store from all nodes,
        reverting them to persistent-only writes for subsequent runs.
        """
        ...


class NodeProtocol(cp.SourceProtocol, Protocol):
    # def record_pipeline_outputs(self):
    #     pass
    ...


@runtime_checkable
class PodNodeProtocol(cp.PodProtocol, Protocol):
    def get_all_records(
        self, include_system_columns: bool = False
    ) -> "pa.Table | None":
        """
        Retrieve all tag and data processed by this PodProtocol.

        This method returns a table containing all data processed by the PodProtocol,
        including metadata and system columns if requested. It is useful for:
        - Debugging and analysis
        - Auditing and data lineage tracking
        - Performance monitoring

        Args:
            include_system_columns: Whether to include system columns in the output

        Returns:
            pa.Table | None: A table containing all processed records, or None if no records are available
        """
        ...

    def flush(self):
        """
        Flush any in-memory data to persistent storage.

        This method ensures that all buffered data is written to the underlying
        storage system, making it durable and consistent. It is useful for:
        - Ensuring data integrity before shutdown or restart
        - Committing changes after a batch of operations
        - Reducing memory usage by clearing buffers

        """
        ...

    def add_pipeline_record(
        self,
        tag: cp.TagProtocol,
        input_data: cp.DataProtocol,
        data_record_id: str,
        retrieved: bool | None = None,
        skip_cache_lookup: bool = False,
    ) -> None: ...
