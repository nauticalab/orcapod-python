# ENG-517: PipelineJob Execution Graph Cleanup

**Date:** 2026-05-26
**Issue:** [ENG-517](https://linear.app/enigma-metamorphic/issue/ENG-517)
**Depends on:** ENG-515 (landed — `AbstractPipelineBase`, `InvocationGraph`, `PodInvocation` hierarchy)

---

## Overview

`PipelineJob._build_execution_graph()` currently clones the entire job on every `run()` call —
fresh `FunctionJobNode` / `OperatorJobNode` instances, a re-wired database set, a cloned
`exec_pipeline`, and a new `nx.DiGraph` with those cloned objects as vertices. The returned
`run()` value is a brand-new `PipelineJob` wrapping all of that.

This copy-on-run pattern is unnecessary. `PipelineJob` is the concrete, stateful instance meant
to be run. Databases are already wired via `_distribute_databases()` at `bind(store=...)` time.
Nodes in `_persistent_node_map` are already execution-ready. The cloning adds complexity without
benefit.

This spec covers:

1. Removing the copy-on-run pattern from `_build_execution_graph()` / `run()`.
2. Wiring the `GraphProtocol` / `OrcaDAG` interface into the execution path (replaces raw
   `nx.DiGraph` at the orchestrator boundary; adopts the common interface from
   `superpowers/specs/2026-05-21-networkx-replacement-design.md`).
3. Consolidating the fragmented `unbound_source_nodes()` + `_unresolved_specs` surface into a
   single `unbound_sources` property.
4. Cleaning up outdated `SourceSpec`-era terminology (`spec` → `source`).

---

## Goals & Success Criteria

- `run()` mutates `self` in place and returns `self` — no new `PipelineJob` constructed.
- `_build_execution_graph()` and `build_execution_graph()` are removed entirely; execution
  graph construction is inlined into `run()`.
- `exec_pipeline` concept eliminated — it was a carrier for exec nodes back to the caller
  and has no purpose once `run()` returns `self`.
- Orchestrators (`SyncPipelineOrchestrator`, `AsyncPipelineOrchestrator`) accept
  `GraphProtocol[JobNode]` instead of `nx.DiGraph`; three call-site substitutions cover all
  usage.
- `GraphProtocol` and `OrcaDAG` gain an `ancestors()` method; `GraphBackend` is renamed
  `GraphProtocol` throughout `dag.py` and `networkx_backend.py`.
- `unbound_source_nodes()` and `_unresolved_specs` / `unresolved_specs` are removed.
  A single live-computed `unbound_sources` property replaces both.
- `spec_names` local variable in `bind()` renamed to `source_names`.
- Serialised JSON key `"unresolved_specs"` updated to `"unbound_sources"` in `save()`; `load()`
  ignores this key entirely (pre-v0.1 greenfield — no backward-compat shims).
- No regressions in the existing pipeline and execution test suite.

---

## Design

### 1. `GraphProtocol` extension — `ancestors()`

Rename `GraphBackend` → `GraphProtocol` in `dag.py` and `networkx_backend.py`.

Add one method to the protocol, `OrcaDAG`, and `NetworkxBackend`:

```python
# Protocol
def ancestors(self, node: NodeT) -> frozenset[NodeT]: ...

# OrcaDAG — BFS over predecessors
def ancestors(self, node: NodeT) -> frozenset[NodeT]:
    visited: set[NodeT] = set()
    queue = list(self._predecessors.get(node, set()))
    while queue:
        n = queue.pop()
        if n not in visited:
            visited.add(n)
            queue.extend(self._predecessors.get(n, set()))
    return frozenset(visited)

# NetworkxBackend — thin delegation
def ancestors(self, node: NodeT) -> frozenset[NodeT]:
    import networkx as nx
    return frozenset(nx.ancestors(self._g, node))
```

### 2. `run()` — simplified, in-place, returns `self`

Execution graph construction is inlined. No private helper method remains.

```python
def run(self, observer=None) -> "PipelineJob":
    # --- build filtered OrcaDAG from existing nodes ---
    excluded_hashes: set[str] = set()

    # Build a hash-keyed OrcaDAG for ancestry lookups
    hash_dag: OrcaDAG[str] = OrcaDAG()
    for u, v in self._graph_edges:
        hash_dag.add_edge(u, v)

    for node_hash, node in self._persistent_node_map.items():
        if isinstance(node, SourceJobNode) and node.bound_source is None:
            excluded_hashes.add(node_hash)
            excluded_hashes.update(hash_dag.ancestors(node_hash))

    exec_dag: OrcaDAG[JobNode] = OrcaDAG()
    for node_hash, node in self._persistent_node_map.items():
        if node_hash not in excluded_hashes:
            exec_dag.add_node(node)
    for u_hash, v_hash in self._graph_edges:
        if u_hash not in excluded_hashes and v_hash not in excluded_hashes:
            exec_dag.add_edge(
                self._persistent_node_map[u_hash],
                self._persistent_node_map[v_hash],
            )

    # --- execute ---
    run_id = _generate_run_id()
    SyncPipelineOrchestrator().run(exec_dag, observer=observer, run_id=run_id)

    if self._store is not None:
        self._store.flush()

    # --- mutate self in place ---
    self._has_run = True
    self._run_id = run_id

    return self
```

Key points:
- `_persistent_node_map` nodes are used directly — no cloning.
- Databases are already wired; `_distribute_databases()` ran at `bind(store=...)` time.
- `unbound_sources` is live-computed from node state; `run()` does not track or store names.
- `exec_pipeline` is gone entirely.

### 3. Orchestrator interface

Both `SyncPipelineOrchestrator` and `AsyncPipelineOrchestrator` change their `run()` signature:

```python
# Before
def run(self, graph: nx.DiGraph, ...) -> OrchestratorResult:

# After
def run(self, graph: GraphProtocol[JobNode], ...) -> OrchestratorResult:
```

Three call-site substitutions (same method names, same semantics):

| Before | After |
|---|---|
| `nx.topological_sort(graph)` | `graph.topological_sort()` |
| `graph.predecessors(node)` | `graph.predecessors(node)` |
| `graph.edges()` | `graph.edges()` |

The `nx` lazy-import at the top of each orchestrator file is removed.

### 4. `unbound_sources` — single live-computed property

Replaces both `unbound_source_nodes()` and the stored `_unresolved_specs` / `unresolved_specs`.

```python
@property
def unbound_sources(self) -> list[str]:
    """Names of source slots not yet bound in this job.

    Returns:
        List of unbound source slot names, in order of appearance in the
        pipeline graph. Empty list if the job is not yet compiled.
    """
    from orcapod.core.nodes.source_node import SourceJobNode

    if not self._compiled:
        return []
    seen: set[str] = set()
    result: list[str] = []
    for node in (self._persistent_node_map or {}).values():
        if (
            isinstance(node, SourceJobNode)
            and node.bound_source is None
            and node.name not in seen
        ):
            result.append(node.name)
            seen.add(node.name)
    return result
```

`is_complete()` updates to:

```python
def is_complete(self) -> bool:
    return self._store is not None and not self.unbound_sources
```

The `_unresolved_specs` field, `unresolved_specs` property, and `unbound_source_nodes()` method
are all removed.

### 5. Terminology cleanup

- Local variable `spec_names` in `bind()` → `source_names` (two call sites).
- Serialised JSON key `"unresolved_specs"` → `"unbound_sources"` in `save()`.
- `load()` does not read this key at all — `unbound_sources` is live-computed from loaded
  `SourceJobNode` stubs (which naturally have `bound_source = None`). Update any test fixtures
  that assert on the saved JSON format directly.

---

## What Is Not Changing

- `bind()`, `_distribute_databases()`, save/load paths, recording, compilation — untouched.
- `_hash_graph` / `_node_graph` in `base.py` and `graph.py` — still `nx.DiGraph`; full
  networkx removal is ENG-494.
- `AsyncPipelineOrchestrator` internal channel-wiring logic — only the type annotation and
  the three call-site substitutions change.

---

## Test Migration

| Before | After |
|---|---|
| `result.pipeline.compiled_nodes["x"]` | `result.compiled_nodes["x"]` |
| `job.unbound_source_nodes()` | `job.unbound_sources` |
| `result.unresolved_specs` | `result.unbound_sources` |
| `exec_graph, specs, pipe = job.build_execution_graph()` | removed; use `job.run()` |

Tests that directly exercised `_build_execution_graph()` / `build_execution_graph()` cloning
behaviour are deleted (they tested implementation detail, not public contract).

---

## Out of Scope

- Full networkx removal from `pyproject.toml` and `base.py` / `graph.py` — ENG-494.
- Orchestrator internals beyond the interface boundary.
- Observer / status reporting implementations.
- Recording and compilation phases of `PipelineJob` — ENG-515 (already landed).
