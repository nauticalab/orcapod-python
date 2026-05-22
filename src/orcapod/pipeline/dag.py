"""Lean directed acyclic graph for Orcapod pipeline topology.

Replaces `networkx.DiGraph` with a minimal, zero-dependency implementation
covering exactly the nine API shapes Orcapod requires. Backed by plain dicts
and `graphlib.TopologicalSorter` from the Python standard library.

See superpowers/specs/2026-05-21-networkx-replacement-design.md for the full
rationale and migration map.

Example:
    >>> dag: OrcaDAG[str] = OrcaDAG()
    >>> dag.add_edge("a", "b")
    >>> dag.add_edge("b", "c")
    >>> dag.topological_sort()
    ['a', 'b', 'c']
"""

from __future__ import annotations

import heapq
from collections.abc import Hashable
from graphlib import CycleError, TopologicalSorter
from typing import Any, Generic, Iterable, Iterator, Protocol, TypeVar

__all__ = ["Comparable", "OrcaDAG", "CycleError"]


class Comparable(Hashable, Protocol):
    """Protocol for node types that support both hashing and ordering.

    Nodes must be hashable to serve as dict keys and must support `<`
    for deterministic topological sort via `heapq`.

    Any type that implements `__hash__` (or inherits it from `object`)
    and `__lt__` satisfies this protocol — e.g. `str`, `int`, or a
    custom dataclass with those methods defined.
    """

    def __lt__(self, other: Any) -> bool: ...


# Class-level TypeVar: only requires hashability (usable as a dict key).
NodeT = TypeVar("NodeT", bound=Hashable)

# Method-level TypeVar for topological_sort_deterministic: additionally
# requires ordering so heapq operations are type-safe.
ComparableNodeT = TypeVar("ComparableNodeT", bound=Comparable)


class OrcaDAG(Generic[NodeT]):
    """Minimal directed acyclic graph for Orcapod pipeline topology.

    Covers exactly the operations Orcapod needs — DAG construction, node
    attribute storage, basic traversal, and topological sort.  No external
    dependencies; backed entirely by plain dicts and stdlib `graphlib`.

    Args:
        NodeT: The node type.  Must be hashable (used as a dict key).
            To call `topological_sort_deterministic`, NodeT must additionally
            satisfy `Comparable` (support `<` ordering).
    """

    def __init__(self) -> None:
        # node → mutable attribute dict
        self._attrs: dict[NodeT, dict[str, Any]] = {}
        # node → set of immediate successors (outgoing edges)
        self._successors: dict[NodeT, set[NodeT]] = {}
        # node → number of incoming edges
        self._in_degree: dict[NodeT, int] = {}

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
        if node not in self._attrs:
            self._attrs[node] = {}
            self._successors[node] = set()
            self._in_degree[node] = 0
        if attrs:
            self._attrs[node].update(attrs)

    def add_edge(self, u: NodeT, v: NodeT) -> None:
        """Add a directed edge from *u* to *v*.

        Both nodes are implicitly added if not already present.  Adding an
        edge that already exists is a no-op (idempotent).

        Args:
            u: Source node.
            v: Target node.
        """
        self.add_node(u)
        self.add_node(v)
        if v not in self._successors[u]:
            self._successors[u].add(v)
            self._in_degree[v] += 1

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
        return self._attrs[node]

    # ------------------------------------------------------------------
    # Membership and sizing
    # ------------------------------------------------------------------

    def __contains__(self, node: object) -> bool:
        return node in self._attrs

    def __len__(self) -> int:
        return len(self._attrs)

    def __iter__(self) -> Iterator[NodeT]:
        """Iterate over all nodes (insertion order)."""
        return iter(self._attrs)

    # ------------------------------------------------------------------
    # Traversal
    # ------------------------------------------------------------------

    def nodes(self) -> Iterable[NodeT]:
        """Return an iterable over all nodes in insertion order.

        Returns:
            Iterable of all nodes.
        """
        return self._attrs.keys()

    def edges(self) -> Iterable[tuple[NodeT, NodeT]]:
        """Return an iterable over all (source, target) edge pairs.

        Returns:
            Iterable of (u, v) tuples for every directed edge in the graph.
        """
        for u, successors in self._successors.items():
            for v in successors:
                yield u, v

    def successors(self, node: NodeT) -> frozenset[NodeT]:
        """Return the immediate successors (outgoing neighbours) of *node*.

        Returns a snapshot `frozenset` so callers cannot mutate internal
        graph state through the returned value.

        Args:
            node: The source node.

        Returns:
            Frozen set of nodes that *node* has a directed edge to.

        Raises:
            KeyError: If *node* is not in the graph.
        """
        return frozenset(self._successors[node])

    def in_degree(self, node: NodeT) -> int:
        """Return the number of incoming edges for *node*.

        Args:
            node: The node to query.

        Returns:
            Count of edges pointing into *node*.

        Raises:
            KeyError: If *node* is not in the graph.
        """
        return self._in_degree[node]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_predecessor_dict(self) -> dict[NodeT, set[NodeT]]:
        """Build the predecessor mapping required by TopologicalSorter.

        Inverts the internal successor representation into the predecessor
        form that `graphlib.TopologicalSorter` expects.

        Returns:
            Mapping of each node to the set of nodes that precede it.
        """
        pred: dict[NodeT, set[NodeT]] = {node: set() for node in self._attrs}
        for u, vs in self._successors.items():
            for v in vs:
                pred[v].add(u)
        return pred

    # ------------------------------------------------------------------
    # Ordering
    # ------------------------------------------------------------------

    def topological_sort(self) -> list[NodeT]:
        """Return nodes in a valid topological order.

        Uses `graphlib.TopologicalSorter` (Python stdlib).  The order is
        stable within a single run but is not guaranteed to be
        deterministic across Python versions or between runs.  Use
        `topological_sort_deterministic` when a stable, reproducible
        order is required (e.g. for content hashing).

        Returns:
            List of nodes in topological order (sources before dependents).

        Raises:
            graphlib.CycleError: If the graph contains a cycle.
        """
        ts: TopologicalSorter[NodeT] = TopologicalSorter(self._build_predecessor_dict())
        return list(ts.static_order())

    def topological_sort_deterministic(
        self: "OrcaDAG[ComparableNodeT]",
    ) -> list[ComparableNodeT]:
        """Return nodes in a deterministic topological order.

        Implements Kahn's algorithm with a min-heap frontier so that the
        output ordering is stable across runs and Python versions.

        This method is type-gated: it is only callable on an `OrcaDAG`
        whose node type satisfies `Comparable` (supports `<`).  Graphs
        whose nodes are only `Hashable` — e.g. `OrcaDAG[GraphNode]` —
        must use `topological_sort` instead.

        This is a direct port of the existing Kahn's implementation already
        present in `graph.py` (`_compute_pipeline_snapshot_hash`), moved
        here so it lives alongside the graph abstraction it operates on.

        Returns:
            List of nodes in deterministic topological order.

        Raises:
            graphlib.CycleError: If the graph contains a cycle.
        """
        in_deg: dict[ComparableNodeT, int] = dict(self._in_degree)
        frontier: list[ComparableNodeT] = [n for n, d in in_deg.items() if d == 0]
        heapq.heapify(frontier)
        ordered: list[ComparableNodeT] = []

        while frontier:
            node: ComparableNodeT = heapq.heappop(frontier)
            ordered.append(node)
            for successor in sorted(self._successors[node]):
                in_deg[successor] -= 1
                if in_deg[successor] == 0:
                    heapq.heappush(frontier, successor)

        if len(ordered) != len(self._attrs):
            # Kahn's detected a cycle (unprocessed nodes remain). Delegate to
            # TopologicalSorter so the raised CycleError has its .cycle
            # attribute (args[1]) populated with the offending nodes — the
            # same information callers get from topological_sort().
            list(
                TopologicalSorter(self._build_predecessor_dict()).static_order()
            )  # raises CycleError
            raise AssertionError("unreachable")  # pragma: no cover

        return ordered
