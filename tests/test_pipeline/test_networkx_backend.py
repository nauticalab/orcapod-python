"""Tests for NetworkxBackend — the thin networkx.DiGraph adapter.

Verifies that `NetworkxBackend` is behaviourally equivalent to `OrcaDAG` for
the full `GraphProtocol` surface, and that both classes satisfy the
`GraphProtocol` protocol at runtime (via `isinstance` with the
`@runtime_checkable` decorator).
"""

from __future__ import annotations

import pytest
from graphlib import CycleError

from orcapod.pipeline.dag import GraphProtocol, OrcaDAG
from orcapod.pipeline.networkx_backend import NetworkxBackend


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


class TestGraphProtocol:
    def test_orca_dag_satisfies_protocol(self) -> None:
        dag: OrcaDAG[str] = OrcaDAG()
        assert isinstance(dag, GraphProtocol)

    def test_networkx_backend_satisfies_protocol(self) -> None:
        backend: NetworkxBackend[str] = NetworkxBackend()
        assert isinstance(backend, GraphProtocol)

    def test_both_accept_same_call_sites(self) -> None:
        """Verify both backends can be used interchangeably via GraphProtocol."""

        def populate(g: GraphProtocol[str]) -> None:  # type: ignore[type-arg]
            g.add_edge("a", "b")
            g.add_edge("b", "c")

        orca: OrcaDAG[str] = OrcaDAG()
        nx_b: NetworkxBackend[str] = NetworkxBackend()
        populate(orca)
        populate(nx_b)
        assert set(orca.nodes()) == set(nx_b.nodes())
        assert set(orca.edges()) == set(nx_b.edges())


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------


class TestAddNode:
    def test_adds_node(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_node("a")
        assert "a" in b

    def test_add_node_idempotent(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_node("a")
        b.add_node("a")
        assert len(b) == 1

    def test_add_node_with_attrs(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_node("a", label="source")
        assert b.node_attrs("a")["label"] == "source"

    def test_subsequent_add_node_does_not_clear_existing_attrs(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_node("a", label="original")
        b.add_node("a")
        assert b.node_attrs("a")["label"] == "original"

    def test_subsequent_add_node_merges_new_attrs(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_node("a", label="original")
        b.add_node("a", node_type="source")
        assert b.node_attrs("a")["label"] == "original"
        assert b.node_attrs("a")["node_type"] == "source"

    def test_subsequent_add_node_overwrites_same_key(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_node("a", label="original")
        b.add_node("a", label="updated")
        assert b.node_attrs("a")["label"] == "updated"


class TestAddEdge:
    def test_adds_both_nodes_implicitly(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_edge("a", "b")
        assert "a" in b
        assert "b" in b

    def test_adds_edge(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_edge("a", "b")
        assert ("a", "b") in list(b.edges())

    def test_duplicate_edge_is_idempotent(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_edge("a", "b")
        b.add_edge("a", "b")
        assert list(b.edges()).count(("a", "b")) == 1


# ---------------------------------------------------------------------------
# Node attribute access
# ---------------------------------------------------------------------------


class TestNodeAttrs:
    def test_returns_mutable_dict(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_node("a")
        attrs = b.node_attrs("a")
        attrs["x"] = 42
        assert b.node_attrs("a")["x"] == 42

    def test_raises_key_error_for_missing_node(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        with pytest.raises(KeyError):
            b.node_attrs("nonexistent")


# ---------------------------------------------------------------------------
# Membership and sizing
# ---------------------------------------------------------------------------


class TestMembership:
    def test_contains_after_add_node(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_node("a")
        assert "a" in b
        assert "z" not in b

    def test_len_counts_nodes(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_edge("a", "b")
        assert len(b) == 2

    def test_iter_yields_nodes(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_node("a")
        b.add_node("b")
        assert set(b) == {"a", "b"}


# ---------------------------------------------------------------------------
# Traversal
# ---------------------------------------------------------------------------


class TestSuccessors:
    def test_returns_direct_successors(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_edge("a", "b")
        b.add_edge("a", "c")
        assert b.successors("a") == {"b", "c"}

    def test_leaf_node_has_no_successors(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_edge("a", "b")
        assert b.successors("b") == frozenset()

    def test_raises_key_error_for_missing_node(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        with pytest.raises(KeyError):
            b.successors("nonexistent")


class TestPredecessors:
    def test_returns_direct_predecessors(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_edge("a", "c")
        b.add_edge("b", "c")
        assert b.predecessors("c") == {"a", "b"}

    def test_root_node_has_no_predecessors(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_edge("a", "b")
        assert b.predecessors("a") == frozenset()

    def test_returns_frozenset_snapshot(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_edge("a", "b")
        snap = b.predecessors("b")
        assert isinstance(snap, frozenset)

    def test_raises_key_error_for_missing_node(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        with pytest.raises(KeyError):
            b.predecessors("nonexistent")


class TestInDegree:
    def test_source_node_has_in_degree_zero(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_edge("a", "b")
        assert b.in_degree("a") == 0

    def test_single_incoming_edge(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_edge("a", "b")
        assert b.in_degree("b") == 1

    def test_multiple_incoming_edges(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_edge("a", "c")
        b.add_edge("b", "c")
        assert b.in_degree("c") == 2

    def test_raises_key_error_for_missing_node(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        with pytest.raises(KeyError):
            b.in_degree("nonexistent")


# ---------------------------------------------------------------------------
# Topological sort
# ---------------------------------------------------------------------------


class TestTopologicalSort:
    def _is_valid_topo_order(
        self, backend: NetworkxBackend[str], order: list[str]
    ) -> bool:
        position = {node: idx for idx, node in enumerate(order)}
        for u, v in backend.edges():
            if position[u] >= position[v]:
                return False
        return True

    def test_linear_chain(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_edge("a", "b")
        b.add_edge("b", "c")
        order = b.topological_sort()
        assert self._is_valid_topo_order(b, order)
        assert set(order) == {"a", "b", "c"}

    def test_diamond_dag(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_edge("a", "b")
        b.add_edge("a", "c")
        b.add_edge("b", "d")
        b.add_edge("c", "d")
        order = b.topological_sort()
        assert self._is_valid_topo_order(b, order)
        assert set(order) == {"a", "b", "c", "d"}

    def test_raises_cycle_error(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_edge("a", "b")
        b.add_edge("b", "c")
        b.add_edge("c", "a")
        with pytest.raises(CycleError):
            b.topological_sort()

    def test_empty_graph(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        assert b.topological_sort() == []


class TestTopologicalSortDeterministic:
    def test_linear_chain(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_edge("a", "b")
        b.add_edge("b", "c")
        assert b.topological_sort_deterministic() == ["a", "b", "c"]

    def test_diamond_dag_deterministic(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_edge("a", "b")
        b.add_edge("a", "c")
        b.add_edge("b", "d")
        b.add_edge("c", "d")
        order = b.topological_sort_deterministic()
        # Kahn's + min-heap: "b" < "c" so b before c
        assert order.index("b") < order.index("c")

    def test_stable_across_repeated_calls(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_edge("z", "m")
        b.add_edge("z", "a")
        b.add_edge("m", "b")
        b.add_edge("a", "b")
        assert b.topological_sort_deterministic() == b.topological_sort_deterministic()

    def test_matches_orca_dag_output(self) -> None:
        """Both backends must produce identical deterministic order."""
        edges = [("z", "m"), ("z", "a"), ("m", "b"), ("a", "b")]

        dag: OrcaDAG[str] = OrcaDAG()
        backend: NetworkxBackend[str] = NetworkxBackend()
        for u, v in edges:
            dag.add_edge(u, v)
            backend.add_edge(u, v)

        assert dag.topological_sort_deterministic() == (
            backend.topological_sort_deterministic()
        )

    def test_raises_cycle_error(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        b.add_edge("a", "b")
        b.add_edge("b", "a")
        with pytest.raises(CycleError):
            b.topological_sort_deterministic()

    def test_empty_graph(self) -> None:
        b: NetworkxBackend[str] = NetworkxBackend()
        assert b.topological_sort_deterministic() == []


# ---------------------------------------------------------------------------
# ancestors()
# ---------------------------------------------------------------------------


class TestNetworkxBackendAncestors:
    def test_ancestors_returns_transitive_predecessors(self):
        backend = NetworkxBackend()
        backend.add_edge("a", "b")
        backend.add_edge("b", "c")
        assert backend.ancestors("c") == frozenset({"a", "b"})
        assert backend.ancestors("b") == frozenset({"a"})
        assert backend.ancestors("a") == frozenset()

    def test_ancestors_handles_diamond(self):
        backend = NetworkxBackend()
        backend.add_edge("a", "b")
        backend.add_edge("a", "c")
        backend.add_edge("b", "d")
        backend.add_edge("c", "d")
        assert backend.ancestors("d") == frozenset({"a", "b", "c"})
