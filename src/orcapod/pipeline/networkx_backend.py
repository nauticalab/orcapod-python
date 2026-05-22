"""Thin networkx adapter satisfying the GraphBackend protocol.

`NetworkxBackend` wraps `networkx.DiGraph` and exposes the same interface as
`OrcaDAG`, allowing callers to swap implementations via a config flag without
changing any call sites.

This module is scaffolding for the forthcoming ENG-494 migration.  It is not
yet wired into the pipeline — the active code still uses `networkx.DiGraph`
directly.  Once the pipeline is migrated, a single flag will choose between
`OrcaDAG` (the default) and `NetworkxBackend` (compatibility / debugging).

Behavioural notes vs. `OrcaDAG`:
- `topological_sort()` is implemented via `graphlib.TopologicalSorter` (not
  `nx.topological_sort`) so that cycles always raise `graphlib.CycleError` —
  the same exception type that `OrcaDAG.topological_sort()` raises.  Using
  the networkx native sort would raise `nx.NetworkXUnfeasible` instead.

Example:
    >>> backend: NetworkxBackend[str] = NetworkxBackend()
    >>> backend.add_edge("a", "b")
    >>> backend.topological_sort()
    ['a', 'b']
"""

from __future__ import annotations

import heapq
from collections.abc import Hashable
from graphlib import CycleError, TopologicalSorter
from typing import TYPE_CHECKING, Any, Generic, Iterable, Iterator, TypeVar

from orcapod.pipeline.dag import Comparable, ComparableNodeT
from orcapod.utils.lazy_module import LazyModule

if TYPE_CHECKING:
    import networkx as nx
else:
    nx = LazyModule("networkx")

__all__ = ["NetworkxBackend"]

NodeT = TypeVar("NodeT", bound=Hashable)


class NetworkxBackend(Generic[NodeT]):
    """Thin `networkx.DiGraph` adapter satisfying the `GraphBackend` protocol.

    Wraps an internal `nx.DiGraph` and provides the same twelve-method surface
    as `OrcaDAG`.  Intended for use as a drop-in during migration (ENG-494) and
    as a debugging aid when you want to inspect the graph with networkx tools
    (visualisation, path queries, etc.) without changing call sites.

    All mutation methods (`add_node`, `add_edge`) delegate directly to the
    wrapped `DiGraph`.  Query methods (`successors`, `predecessors`, `in_degree`,
    `node_attrs`) translate between networkx's attribute-dict style and the
    `GraphBackend` interface.

    Args:
        NodeT: The node type.  Must be hashable (same constraint as `DiGraph`).
            To call `topological_sort_deterministic`, NodeT must additionally
            satisfy `Comparable` (support `<` ordering).
    """

    def __init__(self) -> None:
        self._graph: nx.DiGraph = nx.DiGraph()

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def add_node(self, node: NodeT, **attrs: Any) -> None:
        """Add *node* if not already present, optionally setting attributes.

        Safe to call multiple times; subsequent calls with the same node do
        not overwrite existing attributes unless new ones are passed.

        Args:
            node: The node to add.
            **attrs: Attribute key/value pairs to store on the node.
        """
        if node not in self._graph:
            self._graph.add_node(node, **attrs)
        elif attrs:
            self._graph.nodes[node].update(attrs)

    def add_edge(self, u: NodeT, v: NodeT) -> None:
        """Add a directed edge from *u* to *v*.

        Both nodes are implicitly added if not already present.  Adding an
        edge that already exists is a no-op (idempotent).

        Args:
            u: Source node.
            v: Target node.
        """
        self._graph.add_edge(u, v)

    # ------------------------------------------------------------------
    # Node attribute access
    # ------------------------------------------------------------------

    def node_attrs(self, node: NodeT) -> dict[str, Any]:
        """Return the mutable attribute dict for *node*.

        The returned dict is live — mutations are reflected in the graph.

        Args:
            node: The node whose attributes to access.

        Returns:
            Mutable attribute dict for the node.

        Raises:
            KeyError: If *node* is not in the graph.
        """
        if node not in self._graph:
            raise KeyError(node)
        return self._graph.nodes[node]

    # ------------------------------------------------------------------
    # Membership and sizing
    # ------------------------------------------------------------------

    def __contains__(self, node: object) -> bool:
        return node in self._graph

    def __len__(self) -> int:
        return len(self._graph)

    def __iter__(self) -> Iterator[NodeT]:
        """Iterate over all nodes (insertion order, Python ≥ 3.7+)."""
        return iter(self._graph)

    # ------------------------------------------------------------------
    # Traversal
    # ------------------------------------------------------------------

    def nodes(self) -> Iterable[NodeT]:
        """Return an iterable over all nodes in insertion order.

        Returns:
            Iterable of all nodes.
        """
        return self._graph.nodes()

    def edges(self) -> Iterable[tuple[NodeT, NodeT]]:
        """Return an iterable over all (source, target) edge pairs.

        Returns:
            Iterable of (u, v) tuples for every directed edge in the graph.
        """
        return self._graph.edges()

    def successors(self, node: NodeT) -> frozenset[NodeT]:
        """Return the immediate successors (outgoing neighbours) of *node*.

        Returns a snapshot `frozenset` so callers cannot mutate internal graph
        state through the returned value.

        Args:
            node: The source node.

        Returns:
            Frozen set of nodes that *node* has a directed edge to.

        Raises:
            KeyError: If *node* is not in the graph.
        """
        if node not in self._graph:
            raise KeyError(node)
        return frozenset(self._graph.successors(node))

    def predecessors(self, node: NodeT) -> frozenset[NodeT]:
        """Return the immediate predecessors (incoming neighbours) of *node*.

        Returns a snapshot `frozenset` so callers cannot mutate internal graph
        state through the returned value.

        Args:
            node: The target node.

        Returns:
            Frozen set of nodes that have a directed edge to *node*.

        Raises:
            KeyError: If *node* is not in the graph.
        """
        if node not in self._graph:
            raise KeyError(node)
        return frozenset(self._graph.predecessors(node))

    def in_degree(self, node: NodeT) -> int:
        """Return the number of incoming edges for *node*.

        Args:
            node: The node to query.

        Returns:
            Count of edges pointing into *node*.

        Raises:
            KeyError: If *node* is not in the graph.
        """
        if node not in self._graph:
            raise KeyError(node)
        # nx.DiGraph.in_degree(node) returns an int when the node is present.
        return self._graph.in_degree(node)  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_predecessor_dict(self) -> dict[NodeT, set[NodeT]]:
        """Build the predecessor mapping required by `TopologicalSorter`.

        Returns:
            Mapping of each node to a fresh copy of its predecessor set.
        """
        return {node: set(self._graph.predecessors(node)) for node in self._graph}

    # ------------------------------------------------------------------
    # Ordering
    # ------------------------------------------------------------------

    def topological_sort(self) -> list[NodeT]:
        """Return nodes in a valid topological order.

        Uses `graphlib.TopologicalSorter` (Python stdlib) rather than
        `nx.topological_sort`, so that cycles raise `graphlib.CycleError`
        (the same exception as `OrcaDAG.topological_sort`).

        Returns:
            List of nodes in topological order (sources before dependents).

        Raises:
            graphlib.CycleError: If the graph contains a cycle.
        """
        ts: TopologicalSorter[NodeT] = TopologicalSorter(self._build_predecessor_dict())
        return list(ts.static_order())

    def topological_sort_deterministic(
        self: "NetworkxBackend[ComparableNodeT]",
    ) -> list[ComparableNodeT]:
        """Return nodes in a deterministic topological order.

        Implements Kahn's algorithm with a min-heap frontier so that the
        output ordering is stable across runs and Python versions.  Mirrors
        `OrcaDAG.topological_sort_deterministic` exactly.

        This method is type-gated: it is only callable on a `NetworkxBackend`
        whose node type satisfies `Comparable` (supports `<`).

        Returns:
            List of nodes in deterministic topological order.

        Raises:
            graphlib.CycleError: If the graph contains a cycle.
        """
        in_deg: dict[ComparableNodeT, int] = {
            node: self._graph.in_degree(node) for node in self._graph
        }
        frontier: list[ComparableNodeT] = [n for n, d in in_deg.items() if d == 0]
        heapq.heapify(frontier)
        ordered: list[ComparableNodeT] = []

        while frontier:
            node: ComparableNodeT = heapq.heappop(frontier)
            ordered.append(node)
            for successor in sorted(self._graph.successors(node)):
                in_deg[successor] -= 1
                if in_deg[successor] == 0:
                    heapq.heappush(frontier, successor)

        if len(ordered) != len(self._graph):
            # Delegate to TopologicalSorter so the CycleError has its .cycle
            # attribute populated with the offending nodes.
            list(
                TopologicalSorter(self._build_predecessor_dict()).static_order()
            )  # raises CycleError
            raise AssertionError("unreachable")  # pragma: no cover

        return ordered
