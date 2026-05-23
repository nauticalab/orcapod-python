# Orcapod — networkx Replacement Design

**Issue:** ENG-492
**Date:** 2026-05-21
**Status:** Spike / Recommendation
**Author:** Kurodo (agent-kurodo[bot])

---

## Executive Summary

Orcapod should replace `networkx` with a lean, in-house `OrcaDAG` class
(`src/orcapod/pipeline/dag.py`, ~255 lines including full docstrings and type
annotations; core logic is under 80 lines). The case rests on
three converging facts:

1. Orcapod's networkx API surface is minimal — nine call shapes covering basic
   DAG construction, traversal, and topological sort.
2. Python's standard library (`graphlib.TopologicalSorter`, Python ≥ 3.9) already
   provides the only non-trivial algorithm needed; the rest is a thin dict wrapper.
3. networkx's release history shows two major migration guides (1.x → 2.0, 2.x →
   3.0) with documented breaking changes, and the dep is currently **unpinned** in
   `pyproject.toml` — a silent breakage risk on every `uv sync`.

The full migration (wiring `OrcaDAG` into `graph.py`, the orchestrators, and the
test suite) is scoped as a separate follow-on issue (ENG-494, see §7). This spike
delivers the prototype and the recommendation that justifies it.

---

## 1. networkx Stability — Fact-Check

The concern that networkx "has been unstable" is **directionally correct but
requires precision**. The evidence:

### What changed across major versions

| Migration | Key breaking changes | Relevant to Orcapod? |
|---|---|---|
| 1.x → 2.0 | `G.nodes()` changed from list to `NodeView`; `G.node` removed in favour of `G.nodes[n]`; `set_node_attributes` parameter order changed; several methods moved to main namespace | **Potentially** — `G.nodes[key]` is the new pattern, which orcapod already uses. Any code written against 1.x would break. |
| 2.x → 3.0 | `read_gpickle`, `write_gpickle`, `read_yaml`, `write_yaml` removed; `decorator` library dep removed | **No** — orcapod uses none of these. |

### Orcapod's specific API surface vs. breakage history

The nine API shapes orcapod uses (`DiGraph()`, `add_edge`, `add_node`,
`topological_sort`, `nodes[]`, `in_degree`, `successors`, `nodes()`, `edges()`)
are all **core, stable APIs** that survived both major version bumps intact. There
is no evidence these specific calls were broken in the 2.x or 3.x series.

### The real risk is not historical but structural

- The dep is **unpinned** (`"networkx"` with no version constraint in `pyproject.toml`).
- A hypothetical networkx 4.0 could change any of these APIs without warning.
- Every `uv sync` in CI or a developer environment can silently pull a new
  networkx release.
- The existing mismatch already shows this in practice: `uv.lock` pins `3.5`
  while the system venv has `3.4.2`.

**Verdict:** The instability concern is valid as a *forward risk*, not a documented
historical regression on orcapod's specific calls. The unpinned dep is the concrete
problem. Replacing networkx eliminates this class of risk entirely rather than
patching it with a version pin.

---

## 2. Dependency Footprint

### networkx alone

| Metric | Value |
|---|---|
| Wheel size | **2.1 MB** (networkx 3.6.1, pure Python) |
| Required transitive deps | **None** (removed in 3.0; numpy/scipy/matplotlib are optional) |
| Optional dep groups | `default`, `extra`, `developer`, `doc` (all optional) |

### What orcapod actually exercises

Orcapod uses **nine distinct API shapes** from networkx:

| API | Call sites | Purpose |
|---|---|---|
| `nx.DiGraph()` | 9 | Create directed graph |
| `.add_edge(u, v)` | 7 | Record a dependency edge |
| `.add_node(n)` | 4 | Add isolated node |
| `nx.topological_sort(g)` | 4 | Execution/compile ordering |
| `.nodes[key]` | 4 | Read/write per-node attributes |
| `.in_degree(n)` | 1 | Kahn's algorithm (custom impl already) |
| `.successors(n)` | 1 | Kahn's algorithm (custom impl already) |
| `.nodes()` | 4 | Iterate all nodes |
| `.edges()` | 4 | Iterate all edges |

The 2.1 MB import provides thousands of graph algorithms (shortest path, centrality,
clustering, network flow, etc.) that orcapod never calls. This is the legitimate
"too heavy for what we use" concern — not transitive deps, but the sheer ratio of
imported-but-unused code.

---

## 3. Alternatives Matrix

Four candidates were evaluated:

| Candidate | Dep footprint | Stability | Migration cost | License | Verdict |
|---|---|---|---|---|---|
| **networkx** (status quo) | 2.1 MB, no transitive | Good within a pinned version; unpinned is risky | — | BSD-3 | Keep only if pinned as a stopgap |
| **rustworkx** | Rust wheel est. 3–5 MB per platform (varies by arch); PyO3 bindings | Mature (IBM/Qiskit); active | Low (networkx-adjacent API) | Apache-2.0 | Good but still external; adds wheel-per-platform CI complexity |
| **python-igraph** | C extension, ~1.5 MB; libigraph ~5 MB native | Very mature; stable | Medium (different API) | GPL-2.0 | **GPL is a license concern** for an MIT library |
| **stdlib `graphlib`** | Zero (Python ≥ 3.9 stdlib) | Stable, maintained by CPython | Medium (only provides TopologicalSorter; no DiGraph container) | PSF (stdlib) | Good building block; not a standalone replacement |
| **In-house `OrcaDAG`** | Zero | Under orcapod's own control | Low (we write the interface to match usage exactly) | MIT (same as orcapod) | **Recommended** |

### Why not rustworkx?

`rustworkx` is an excellent library and would be the right call if orcapod's graph
operations were performance-sensitive or algorithmically complex. They are not.
Orcapod pipeline graphs have tens of nodes at most. At that scale, a pure-Python
`dict` is faster than any FFI boundary. Adding a Rust wheel also introduces per-
platform CI complexity (manylinux, macOS x86_64/arm64, Windows) that has zero
payoff at orcapod's graph sizes.

### Why not "reinventing the wheel"?

The "reinventing the wheel" anti-pattern applies to code with **genuine hidden
complexity**: correctness edge cases (cryptography), performance tuning
(numerics), or a large combinatorial test surface (HTTP). A DAG data structure
backed by dicts does not qualify:

- The only non-trivial algorithm — topological sort — is already in the Python
  standard library (`graphlib.TopologicalSorter`).
- Orcapod already **reimplemented the most complex variant** (deterministic
  topological sort via Kahn's + min-heap) in `graph.py` lines 559–571. The team
  has already proven it can own this code.
- The replacement is ~120 lines including type annotations and docstrings. This
  is smaller than the average orcapod test file.
- Ownership cost: near-zero. The only operations are dict mutations and one
  stdlib call.

Owning 120 lines of well-typed, well-tested dict manipulation is strictly better
than depending on a 2.1 MB library with its own release cadence, deprecation
policy, and scope.

---

## 4. Recommendation

**Build and adopt an in-house `OrcaDAG`** (`src/orcapod/pipeline/dag.py`).

- Zero new external dependencies.
- Eliminates the instability risk permanently (no external release cadence to track).
- The full API surface is trivially implementable using stdlib `graphlib` for
  topological sort and plain `dict` for everything else.
- Well-typed (`OrcaDAG[NodeT]`) so both usage patterns (hash-string nodes and
  `GraphNode` object nodes) work without casting.
- Covered by orcapod's existing MIT license.

The full migration (wiring `OrcaDAG` into the three call-site files and updating
tests) is tracked as a follow-on issue (ENG-494, created as part of this spike).

---

## 5. `OrcaDAG` Interface Design

### Public API

```python
from __future__ import annotations
from collections.abc import Hashable
from typing import Any, Generic, Iterable, Protocol, TypeVar

class Comparable(Hashable, Protocol):
    """Nodes must be hashable (dict keys) and support < (heapq / sorted)."""
    def __lt__(self, other: Any) -> bool: ...

NodeT = TypeVar("NodeT", bound=Comparable)

class OrcaDAG(Generic[NodeT]):
    """Minimal directed acyclic graph for Orcapod pipeline topology.

    Covers exactly the nine API shapes Orcapod needs; nothing more.
    Backed by plain dicts and stdlib graphlib. Zero external dependencies.
    """

    # Construction
    def add_node(self, node: NodeT, **attrs: Any) -> None: ...
    def add_edge(self, u: NodeT, v: NodeT) -> None: ...
        # Implicitly calls add_node for u and v if not already present.
        # Adding a duplicate edge is a no-op (idempotent).

    # Node attribute access — replaces nx.DiGraph.nodes[key]
    def node_attrs(self, node: NodeT) -> dict[str, Any]: ...
        # Returns the mutable attribute dict for node.
        # KeyError if node not present.

    # Membership
    def __contains__(self, node: object) -> bool: ...

    # Traversal
    def nodes(self) -> Iterable[NodeT]: ...
    def edges(self) -> Iterable[tuple[NodeT, NodeT]]: ...
    def successors(self, node: NodeT) -> frozenset[NodeT]: ...
        # Returns a snapshot frozenset — callers cannot corrupt _in_degree
        # by mutating the returned collection.
    def in_degree(self, node: NodeT) -> int: ...

    # Ordering
    def topological_sort(self) -> list[NodeT]: ...
        # Non-deterministic (insertion-order DFS via graphlib).
        # Raises CycleError if the graph contains a cycle.

    def topological_sort_deterministic(self) -> list[NodeT]: ...
        # Deterministic (Kahn's + min-heap). Type-safe because NodeT is
        # bounded to Comparable. Used for snapshot hash computation where
        # ordering must be stable across runs and Python versions.
```

### Internal representation

```
_attrs:      dict[NodeT, dict[str, Any]]   # node → attribute dict
_successors: dict[NodeT, set[NodeT]]       # node → set of outgoing neighbours
_in_degree:  dict[NodeT, int]              # node → count of incoming edges
```

All three structures are updated atomically in `add_node` and `add_edge`. No
networkx object is referenced anywhere in the implementation.

### Replacing `nx.DiGraph` in call sites

| networkx call | OrcaDAG equivalent |
|---|---|
| `nx.DiGraph()` | `OrcaDAG()` |
| `g.add_edge(u, v)` | `g.add_edge(u, v)` |
| `g.add_node(n)` | `g.add_node(n)` |
| `nx.topological_sort(g)` | `g.topological_sort()` |
| `g.nodes[key]` | `g.node_attrs(key)` |
| `g.nodes[key].get("x")` | `g.node_attrs(key).get("x")` |
| `g.nodes[key]["x"] = v` | `g.node_attrs(key)["x"] = v` |
| `for n in g` | `for n in g.nodes()` |
| `for n in g.nodes()` | `for n in g.nodes()` |
| `for u, v in g.edges()` | `for u, v in g.edges()` |
| `g.in_degree(n)` | `g.in_degree(n)` |
| `for s in g.successors(n)` | `for s in g.successors(n)` |
| `n not in g` | `n not in g` |

The only non-trivial translation is `g.nodes[key]` → `g.node_attrs(key)`, which
is a straightforward find-and-replace in `graph.py`.

---

## 6. Prototype (`dag.py`)

The working prototype is committed at `src/orcapod/pipeline/dag.py` as part of
this spike. It implements the full interface above and is covered by
`tests/test_pipeline/test_dag.py`.

The prototype is **self-contained** — it does not yet replace networkx in
`graph.py`, `sync_orchestrator.py`, or `async_orchestrator.py`. That wiring is
left to the follow-on issue (ENG-494) to keep this spike's diff reviewable.

---

## 7. Migration Surface Map (for follow-on issue ENG-494)

The full migration requires changes in exactly five places:

| File | Change required |
|---|---|
| `src/orcapod/pipeline/graph.py` | Replace all `nx.DiGraph()` with `OrcaDAG()`; replace `.nodes[key]` with `.node_attrs(key)`; replace `nx.topological_sort(g)` with `g.topological_sort()` or `g.topological_sort_deterministic()`; remove `LazyModule("networkx")` import; update type annotations |
| `src/orcapod/pipeline/sync_orchestrator.py` | Replace `nx.DiGraph` type annotation with `OrcaDAG`; replace `nx.topological_sort(graph)` with `graph.topological_sort()` |
| `src/orcapod/pipeline/async_orchestrator.py` | Same as sync_orchestrator |
| `tests/test_pipeline/test_graph_rendering.py` | Remove `import networkx as nx`; update type annotations; update any direct `nx.DiGraph` construction in test fixtures |
| `pyproject.toml` | Remove `"networkx"` from `dependencies` |

See **ENG-494**: https://linear.app/enigma-metamorphic/issue/ENG-494

Estimated call-site count: ~38 changes across these files (9 DiGraph
instantiations, 7 add_edge, 4 add_node, 4 topological_sort, 4 nodes[], 1
in_degree, 1 successors, 4 nodes(), 4 edges() — plus import and type annotation
cleanups).

---

## 8. Testing Plan for `OrcaDAG`

`tests/test_pipeline/test_dag.py` covers:

- `add_node` / `add_edge` — node and edge membership, implicit node creation
- `node_attrs` — read/write attributes, KeyError on missing node
- `nodes()` / `edges()` — correct enumeration
- `in_degree()` / `successors()` — correct values after edge additions
- `topological_sort()` — valid topological order on a representative DAG
- `topological_sort_deterministic()` — stable ordering across repeated calls
- `CycleError` on a graph with a cycle
- `__contains__` — membership check
- Generic usage — `OrcaDAG[str]` and `OrcaDAG[object]` both work

---

## References

- [ENG-492 Linear Issue](https://linear.app/enigma-metamorphic/issue/ENG-492)
- [networkx 1.x → 2.0 migration guide](https://networkx.org/documentation/stable/release/migration_guide_from_1.x_to_2.0.html)
- [networkx 2.x → 3.0 migration guide](https://networkx.org/documentation/stable/release/migration_guide_from_2.x_to_3.0.html)
- [networkx PyPI page](https://pypi.org/project/networkx/) (3.6.1: 2.1 MB wheel)
- [Python stdlib graphlib](https://docs.python.org/3/library/graphlib.html)
- Orcapod networkx call sites: `src/orcapod/pipeline/graph.py`, `sync_orchestrator.py`, `async_orchestrator.py`
