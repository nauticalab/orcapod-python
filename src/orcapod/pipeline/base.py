"""AbstractPipelineBase — shared recording mechanism for Pipeline and PipelineJob."""
from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import Hashable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from orcapod.core.tracker import AutoRegisteringContextBasedTracker
from orcapod.pipeline.dag import OrcaDAG
from orcapod.pipeline.pod_invocation import (
    FunctionInvocation,
    OperatorInvocation,
    PodInvocation,
)
from orcapod.protocols import core_protocols as cp
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import networkx as nx
else:
    nx = LazyModule("networkx")

logger = logging.getLogger(__name__)

NodeT = TypeVar("NodeT", bound=Hashable)


@dataclass(frozen=True)
class InvocationGraph:
    """Interchange value object between Pipeline and PipelineJob.

    Carries a topologically ordered tuple of ``PodInvocation`` objects and a
    mapping from content-hash string to the corresponding source stream.
    Both classes exchange representations by converting to this neutral form
    via ``to_invocations()`` and reconstructing via ``from_invocations()``.

    Args:
        invocations: Topologically ordered sequence of pod invocations
            (sources excluded).
        source_streams: Mapping of content-hash string → source stream
            (nodes whose invocation was not recorded in the pipeline).
    """

    invocations: tuple[PodInvocation, ...]
    source_streams: dict[str, Any]  # hash → StreamProtocol-compatible node


class AbstractPipelineBase(Generic[NodeT], AutoRegisteringContextBasedTracker, ABC):
    """Shared recording mechanism and graph state for Pipeline and PipelineJob.

    Manages the ``with``-block recording phase: accumulating invocations into
    ``_invocation_lut``, capturing raw source streams into ``_source_streams``,
    and building the topology in ``_hash_graph``.  On context exit, ``compile()``
    materialises the accumulated state into a frozen DAG of node objects.

    ``_invocation_lut`` and ``_source_streams`` are **additive** — they persist
    across multiple ``with`` blocks so that repeated recording sessions extend
    the same graph rather than replacing it.

    Args:
        name: Pipeline name (string or tuple). Used to scope database paths.
        tracker_manager: Optional tracker manager override.
    """

    def __init__(
        self,
        name: str | tuple[str, ...] = "pipeline",
        tracker_manager: cp.TrackerManagerProtocol | None = None,
    ) -> None:
        """Initialize shared pipeline state.

        Args:
            name: Pipeline name (string or tuple). Used to scope database paths.
                Stored internally as a tuple.
            tracker_manager: Optional tracker manager override. Uses the default
                tracker manager if ``None``.
        """
        super().__init__(tracker_manager=tracker_manager)
        self._name: tuple[str, ...] = (name,) if isinstance(name, str) else tuple(name)

        # --- Additive recording state (never cleared) -----------------
        # Maps content-hash-string → PodInvocation for each recorded invocation.
        self._invocation_lut: dict[str, PodInvocation] = {}
        # Maps content-hash-string → raw stream for each unregistered upstream.
        self._source_streams: dict[str, Any] = {}
        # Topology graph — vertices and edges are content-hash strings.
        # Additive: persists and grows across multiple with-blocks.
        self._hash_graph: "nx.DiGraph" = nx.DiGraph()

        # --- Compiled state (populated / replaced by compile()) --------
        self._persistent_node_map: dict[str, NodeT] = {}
        self._nodes: dict[str, NodeT] = {}
        self._node_graph: OrcaDAG[NodeT] | None = None
        self._compiled: bool = False

        # --- Legacy fields kept for _build_execution_graph() compat ----
        # Populated from _persistent_node_map / _hash_graph at end of compile().
        self._node_lut: dict[str, Any] = {}       # hash → non-source compiled node
        self._upstreams: dict[str, Any] = {}      # hash → source compiled node
        self._graph_edges: list[tuple[str, str]] = []  # edge list from _hash_graph

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def name(self) -> tuple[str, ...]:
        """Pipeline name tuple."""
        return self._name

    @property
    def graph(self) -> "nx.DiGraph":
        """Directed hash graph of accumulated pipeline structure."""
        return self._hash_graph

    @property
    def nodes(self) -> dict[str, Any]:
        """Copy of the compiled nodes dict (label → node)."""
        return self._nodes.copy()

    @property
    def dag(self) -> OrcaDAG[NodeT]:
        """Node-object DAG for this pipeline.

        Returns an ``OrcaDAG`` whose vertices are the compiled node objects
        (``GraphNode`` for ``Pipeline``, ``JobNode`` for ``PipelineJob``) and
        whose edges follow the data-flow topology.

        Raises:
            RuntimeError: If the pipeline has not been compiled yet.
        """
        if self._node_graph is None:
            raise RuntimeError(
                "Pipeline has not been compiled. "
                "Use 'with pipeline:' or call compile() first."
            )
        return self._node_graph

    # ------------------------------------------------------------------
    # Abstract — subclass node-factory declarations
    # ------------------------------------------------------------------

    @property
    @abstractmethod
    def source_node_class(self) -> type:
        """Node class to use for source (leaf) nodes — e.g. ``SourceNode``."""
        ...

    @property
    @abstractmethod
    def function_node_class(self) -> type:
        """Node class to use for function-pod invocations — e.g. ``FunctionNode``."""
        ...

    @property
    @abstractmethod
    def operator_node_class(self) -> type:
        """Node class to use for operator-pod invocations — e.g. ``OperatorNode``."""
        ...

    # ------------------------------------------------------------------
    # Recording
    # ------------------------------------------------------------------

    def record_function_pod_invocation(
        self,
        pod: "cp.FunctionPodProtocol",
        input_stream: "cp.StreamProtocol",
        label: str | None = None,
    ) -> None:
        """Record a function pod invocation into the graph.

        Args:
            pod: The function pod being invoked.
            input_stream: The upstream stream.
            label: Optional display label for the resulting compiled node.
        """
        self._record_invocation(
            FunctionInvocation(pod=pod, input_streams=(input_stream,), label=label)
        )

    def record_operator_pod_invocation(
        self,
        pod: "cp.OperatorPodProtocol",
        upstreams: "tuple[cp.StreamProtocol, ...]" = (),
        label: str | None = None,
    ) -> None:
        """Record an operator pod invocation into the graph.

        Args:
            pod: The operator pod being invoked.
            upstreams: Upstream streams for this operator.
            label: Optional display label for the resulting compiled node.
        """
        self._record_invocation(
            OperatorInvocation(pod=pod, input_streams=tuple(upstreams), label=label)
        )

    def _record_invocation(self, invocation: PodInvocation) -> None:
        """Store *invocation* and update the topology graph.

        Non-source nodes in ``_persistent_node_map`` are keyed by the
        corresponding ``PodInvocation.content_hash()`` string.  For
        ``PipelineJob`` (where ``source_node_class`` is ``SourceJobNode``),
        a bound ``SourceJobNode`` delegates its ``content_hash()`` to the
        underlying concrete source, so invocation and compiled-node hashes
        agree throughout the graph.

        For ``Pipeline`` (where ``source_node_class`` is ``SourceNode``), a
        concrete ``RootSource`` upstream is promoted to a ``SourceNode`` whose
        identity is ``("source_node", name, tag_schema, data_schema)`` — a
        different structure from the raw source.  As a result the compiled
        ``FunctionNode`` / ``OperatorNode``'s own ``content_hash()`` may
        diverge from the invocation hash used as its key.  This is intentional:
        the hash-graph topology is always built from raw stream hashes, and
        ``_persistent_node_map`` uses those same keys for consistent lookup.

        Args:
            invocation: The pod invocation to record.
        """
        key = invocation.content_hash().to_string()
        self._invocation_lut[key] = invocation
        self._hash_graph.add_node(key)
        for upstream in invocation.input_streams:
            upstream_hash = upstream.content_hash().to_string()
            self._hash_graph.add_edge(upstream_hash, key)
            if upstream_hash not in self._source_streams:
                self._source_streams[upstream_hash] = upstream

    # ------------------------------------------------------------------
    # reset() — no-op after refactor
    # ------------------------------------------------------------------

    def reset(self) -> None:
        """No-op — all recording state is additive and persists across with-blocks.

        Note:
            ``_invocation_lut``, ``_source_streams``, and ``_hash_graph``
            intentionally accumulate across ``with`` blocks.
            ``_persistent_node_map``, ``_node_lut``, ``_upstreams``, and
            ``_graph_edges`` are compiled artifacts overwritten by each
            ``compile()`` call.  Nothing needs to be cleared on re-entry.
        """

    def __exit__(self, exc_type=None, exc_value=None, traceback=None) -> None:
        """Exit the recording context, compiling if no exception occurred."""
        super().__exit__(exc_type, exc_value, traceback)
        if exc_type is None:
            self.compile()

    def __getattr__(self, item: str) -> Any:
        """Look up compiled nodes by label as attribute access."""
        if item.startswith("_"):
            raise AttributeError(item)
        # Use object.__getattribute__ to avoid recursion
        nodes = object.__getattribute__(self, "_nodes")
        if item in nodes:
            return nodes[item]
        raise AttributeError(
            f"{type(self).__name__!r} has no attribute {item!r}. "
            f"Available node labels: {sorted(nodes.keys())}"
        )

    # ------------------------------------------------------------------
    # compile() — single-pass, class-property driven
    # ------------------------------------------------------------------

    def compile(self) -> None:
        """Compile recorded invocations into a frozen DAG.

        Walks ``_hash_graph`` topologically.  Streams that appear as inputs
        but have no registered invocation are promoted to source nodes via
        ``self.source_node_class.from_stream()``.  Each ``PodInvocation`` in
        ``_invocation_lut`` becomes a ``function_node_class`` or
        ``operator_node_class`` instance.

        After compilation:

        - ``_persistent_node_map`` — hash → compiled node (all node types)
        - ``_nodes`` — label → compiled node (labelled nodes only)
        - ``_node_graph`` — OrcaDAG with node objects as vertices
        - Legacy fields ``_node_lut``, ``_upstreams``, ``_graph_edges``
          are repopulated for backward compat with ``_build_execution_graph()``.
        - ``_compiled`` is set to ``True``.

        This method always rebuilds from scratch; it does NOT perform
        incremental compilation.
        """
        import networkx as _nx

        source_node_cls = self.source_node_class

        # 1. Source hashes: inputs that have no registered invocation.
        source_hashes = set(self._source_streams.keys()) - set(self._invocation_lut.keys())

        # 2. Create source nodes keyed by the original stream hash.
        node_map: dict[str, Any] = {
            h: source_node_cls.from_stream(self._source_streams[h])
            for h in source_hashes
            if h in self._source_streams
        }

        # 2a. Validate source-node name uniqueness.
        # Source node names are identity-forming (used as bind() keys and
        # included in content_hash()).  Two distinct source nodes with the
        # same name indicate a source_id collision.  If their schemas also
        # differ the caller has assigned the same source_id to conceptually
        # different sources — raise an error.  If the schemas happen to match
        # the nodes are functionally identical (same name + same schema would
        # normally produce the same content_hash and be deduplicated at
        # recording time, so this branch is only reached in unusual edge cases
        # such as non-RootSource concrete streams with colliding label:hash
        # names), and we leave them as-is.
        name_to_hashes: dict[str, list[str]] = {}
        for h, node in node_map.items():
            name_to_hashes.setdefault(node._name, []).append(h)
        for node_name, hashes in name_to_hashes.items():
            if len(hashes) <= 1:
                continue
            nodes = [node_map[h] for h in hashes]
            first = nodes[0]
            schemas_differ = any(
                n._tag_schema != first._tag_schema or n._data_schema != first._data_schema
                for n in nodes[1:]
            )
            if schemas_differ:
                from orcapod.errors import InconsistentSourceError
                raise InconsistentSourceError(
                    f"Pipeline '{'.'.join(self._name)}' has {len(hashes)} source "
                    f"nodes all named {node_name!r} but with different schemas. "
                    f"Assign distinct source_id values to the conflicting sources "
                    f"so each slot has a unique, stable identity."
                )

        # 3. Topological pass — create function / operator nodes.
        for key in _nx.topological_sort(self._hash_graph):
            if key in node_map:
                continue  # already added as a source node in step 2
            if key not in self._invocation_lut:
                continue  # vertex with no invocation (e.g. pure source hash)
            inv = self._invocation_lut[key]
            upstream_nodes = [
                node_map[up.content_hash().to_string()]
                for up in inv.input_streams
            ]
            if isinstance(inv, FunctionInvocation):
                node_map[key] = self.function_node_class(
                    function_pod=inv.pod,
                    input_stream=upstream_nodes[0],
                    label=inv.label,
                )
            else:
                node_map[key] = self.operator_node_class(
                    operator=inv.pod,
                    input_streams=tuple(upstream_nodes),
                    label=inv.label,
                )

        self._persistent_node_map = node_map

        # 4. Label disambiguation (preserves existing Pipeline.compile() behavior).
        name_candidates: dict[str, list] = {}
        for node in node_map.values():
            name_candidates.setdefault(node.label, []).append(node)

        self._nodes.clear()
        for label, nodes in name_candidates.items():
            if len(nodes) > 1:
                sorted_nodes = sorted(nodes, key=lambda n: n.content_hash().to_string())
                for i, node in enumerate(sorted_nodes, start=1):
                    key = f"{label}_{i}"
                    self._nodes[key] = node
                    node._label = key
            else:
                self._nodes[label] = nodes[0]

        # 5. Build node_graph (OrcaDAG with node objects as vertices).
        node_dag: OrcaDAG[Any] = OrcaDAG()
        for up_hash, down_hash in self._hash_graph.edges():
            up_node = node_map.get(up_hash)
            down_node = node_map.get(down_hash)
            if up_node is not None and down_node is not None:
                node_dag.add_edge(up_node, down_node)
        for node in node_map.values():
            if node not in node_dag:
                node_dag.add_node(node)
        self._node_graph = node_dag  # type: ignore[assignment]

        # 6. Enrich hash_graph node attributes (used by GraphRenderer and serialization).
        for node_hash, node in node_map.items():
            if node_hash not in self._hash_graph:
                continue
            attrs = self._hash_graph.nodes[node_hash]
            if not attrs.get("node_type"):
                attrs["node_type"] = node.node_type
            if not attrs.get("label"):
                computed = node._label or (
                    node.computed_label() if hasattr(node, "computed_label") else None
                )
                if computed:
                    attrs["label"] = computed
            if not attrs.get("pipeline_hash"):
                attrs["pipeline_hash"] = node.pipeline_hash().to_string()

        # 7. Populate legacy fields for _build_execution_graph() backward compat.
        self._node_lut = {
            h: n for h, n in node_map.items()
            if not isinstance(n, source_node_cls)
        }
        self._upstreams = {
            h: n for h, n in node_map.items()
            if isinstance(n, source_node_cls)
        }
        self._graph_edges = list(self._hash_graph.edges())

        self._compiled = True

    # ------------------------------------------------------------------
    # InvocationGraph transitions
    # ------------------------------------------------------------------

    def to_invocations(self) -> InvocationGraph:
        """Extract an ``InvocationGraph`` from the compiled ``_persistent_node_map``.

        Reconstructs from ``_persistent_node_map`` (not raw ``_invocation_lut``) so
        that the in-memory path and the save/load path produce consistent results.
        Source nodes (leaves) go into ``source_streams``; function and operator
        nodes become ``FunctionInvocation`` / ``OperatorInvocation`` objects.

        Returns:
            An ``InvocationGraph`` with topologically ordered invocations and
            a mapping of hash → source node (which acts as its own stream).

        Raises:
            RuntimeError: If the pipeline has not been compiled.
        """
        import networkx as _nx

        if not self._compiled:
            raise RuntimeError(
                "Cannot call to_invocations() before compile(). "
                "Use 'with pipeline:' or call compile() first."
            )

        source_node_cls = self.source_node_class
        fn_node_cls = self.function_node_class
        source_streams: dict[str, Any] = {}
        # Map node_hash (from _persistent_node_map / _hash_graph) → invocation.
        # Using node_hash (not inv.content_hash()) so the topological sort keys
        # from _hash_graph align with inv_by_node_hash.
        inv_by_node_hash: dict[str, PodInvocation] = {}

        for node_hash, node in self._persistent_node_map.items():
            if isinstance(node, source_node_cls):
                source_streams[node.content_hash().to_string()] = node
            elif isinstance(node, fn_node_cls):
                if node._function_pod is None:
                    raise RuntimeError(
                        f"to_invocations() cannot serialise FunctionNode {node_hash!r}: "
                        "node._function_pod is None. This node was loaded without a live "
                        "function pod (read-only / stub mode). "
                        "Use PipelineJob.load() to work with loaded pipelines, or "
                        "ensure the function pod is available before calling "
                        "to_invocations() / from_pipeline()."
                    )
                inv_by_node_hash[node_hash] = FunctionInvocation(
                    pod=node._function_pod,
                    input_streams=(node.upstreams[0],),
                    label=node._label,
                )
            else:
                if node._operator is None:
                    raise RuntimeError(
                        f"to_invocations() cannot serialise OperatorNode {node_hash!r}: "
                        "node._operator is None. This node was loaded without a live "
                        "operator (read-only / stub mode). "
                        "Use PipelineJob.load() to work with loaded pipelines, or "
                        "ensure the operator is available before calling "
                        "to_invocations() / from_pipeline()."
                    )
                inv_by_node_hash[node_hash] = OperatorInvocation(
                    pod=node._operator,
                    input_streams=tuple(node.upstreams),
                    label=node._label,
                )

        # Sort topologically using _hash_graph (node_hash keys match _hash_graph).
        topo_keys = list(_nx.topological_sort(self._hash_graph))
        ordered = [inv_by_node_hash[k] for k in topo_keys if k in inv_by_node_hash]

        return InvocationGraph(
            invocations=tuple(ordered),
            source_streams=source_streams,
        )

    @classmethod
    def from_invocations(
        cls,
        graph: InvocationGraph,
        name: str | tuple[str, ...] = "pipeline",
    ) -> "AbstractPipelineBase":
        """Reconstruct a Pipeline or PipelineJob from an ``InvocationGraph``.

        Calls ``cls(name=name)`` to properly initialise all fields, then
        populates ``_invocation_lut``, ``_source_streams``, and ``_hash_graph``
        from *graph* before calling ``compile()``.

        Args:
            graph: The ``InvocationGraph`` to reconstruct from.
            name: Pipeline name for the new instance.

        Returns:
            A compiled instance of ``cls``.
        """
        instance = cls(name=name)
        instance._invocation_lut = {
            inv.content_hash().to_string(): inv
            for inv in graph.invocations
        }
        instance._source_streams = dict(graph.source_streams)
        for inv in graph.invocations:
            instance._hash_graph.add_node(inv.content_hash().to_string())
            for upstream in inv.input_streams:
                instance._hash_graph.add_edge(
                    upstream.content_hash().to_string(),
                    inv.content_hash().to_string(),
                )
        # Also add source hashes as isolated nodes (no incoming edges)
        for h in graph.source_streams:
            if h not in instance._hash_graph:
                instance._hash_graph.add_node(h)
        instance.compile()
        return instance
