# Pipeline & PipelineJob Refactor Design

**Issue:** ENG-515  
**Date:** 2026-05-23  
**Status:** Approved

---

## Overview

`Pipeline` and `PipelineJob` share the same logical recording phase (accumulate pod
invocations into a DAG) but currently implement it in parallel — duplicating recording
state, duplication logic in `record_function_pod_invocation` / `record_operator_pod_invocation`,
and maintaining two divergent compilation paths.

This refactor unifies invocation capture into a single minimal primitive (`PodInvocation`),
promotes the shared recording logic into `AbstractPipelineBase`, and replaces the
multi-step compilation walks with a single-pass class-property–driven `compile()`.

---

## Goals & Success Criteria

- A single `_record_invocation()` in `AbstractPipelineBase` handles both function and
  operator pod invocations for both `Pipeline` and `PipelineJob`.
- `Pipeline` and `PipelineJob` each declare three class-level node-factory properties
  (`source_node_class`, `function_node_class`, `operator_node_class`); the shared
  `compile()` in `AbstractPipelineBase` uses these to build the node graph in one pass.
- Duplicate fields (`_rec_node_lut`, `_rec_upstreams`, `_rec_graph_edges` in `PipelineJob`)
  are eliminated.
- All four representation transitions (LUT → compile, Pipeline → PipelineJob,
  PipelineJob → Pipeline, any → LUT) are clean compositions of two primitives:
  `to_invocations()` and `from_invocations()`.
- All existing tests pass; new tests cover the unified recording path and transitions.

---

## Scope & Boundaries

In scope:
- New `PodInvocation` hierarchy (`FunctionInvocation`, `OperatorInvocation`).
- `AbstractPipelineBase`: unified recording methods, `_record_invocation()`,
  `from_invocations()`, `to_invocations()`, class-property node factories, concrete `compile()`.
- `Pipeline`: remove recording overrides; add class-property node factories.
- `PipelineJob`: remove `_rec_*` fields, `_to_node_stream`, `_ensure_source_node`;
  add class-property node factories; thin `from_pipeline()` / `as_pipeline()`.
- `SourceJobNode.from_stream()`: three-way input-type logic.
- Tests landed **before** production code changes.

Out of scope:
- `_build_execution_graph()` internals in `PipelineJob`.
- `GraphRenderer` / `render_graph` in `graph.py`.
- `DynamicPodStream` or any other recording path outside Pipeline/PipelineJob.
- Backward-compatibility shims (pre-v0.1.0 project).

---

## Design

### 1. `PodInvocation` — the minimal recording primitive

**File:** `src/orcapod/pipeline/pod_invocation.py` (new)

```python
class PodInvocation(ContentIdentifiableBase):
    def __init__(self, pod, input_streams: tuple, label: str | None = None): ...
    def identity_structure(self) -> Any:
        return (self._pod, self._pod.argument_symmetry(self._input_streams))
    def pipeline_identity_structure(self) -> Any:
        return (self._pod, self._pod.argument_symmetry(self._input_streams))

class FunctionInvocation(PodInvocation):
    """pod is a FunctionPodProtocol; len(input_streams) == 1."""

class OperatorInvocation(PodInvocation):
    """pod is an OperatorPodProtocol; len(input_streams) >= 1."""
```

Key design choices:
- Two **concrete subclasses** instead of a `node_type` annotation, so downstream code can
  use cheap `isinstance(inv, FunctionInvocation)` rather than protocol isinstance checks.
- `identity_structure` and `pipeline_identity_structure` are intentionally identical —
  the full pod+symmetry tuple is the canonical identity at the invocation level.
- No node objects are created during recording; nodes are only materialised during
  `compile()`.
- `input_streams` are the raw streams passed in by the caller. Any stream whose own
  invocation was not registered in the current `with` block is promoted to a source node
  during `compile()` — this is the correct semantic boundary of a pipeline.
- An optional `label` field lets the caller name the resulting compiled node.

---

### 2. `AbstractPipelineBase` changes

**File:** `src/orcapod/pipeline/base.py`

#### 2a. Recording — concrete, unified

`record_function_pod_invocation` and `record_operator_pod_invocation` become **concrete**
methods that both delegate to a shared `_record_invocation()`:

```python
def record_function_pod_invocation(self, pod, input_stream, label=None):
    self._record_invocation(FunctionInvocation(pod, (input_stream,), label))

def record_operator_pod_invocation(self, pod, upstreams=(), label=None):
    self._record_invocation(OperatorInvocation(pod, tuple(upstreams), label))

def _record_invocation(self, invocation: PodInvocation) -> None:
    key = invocation.content_hash()
    self._node_lut[key] = invocation
    self._hash_graph.add_node(key)
    for upstream in invocation.input_streams:
        upstream_hash = upstream.content_hash()
        self._hash_graph.add_edge(upstream_hash, key)
        # capture the raw stream object for source promotion at compile time
        if upstream_hash not in self._source_streams:
            self._source_streams[upstream_hash] = upstream
```

Two fields are **removed** from the base class:
- `_upstreams` — upstreams are derivable from `invocation.input_streams` at compile time.
- `_graph_edges` — eliminated; `_hash_graph` (the nx.DiGraph) is built directly during
  recording and persists across `with` blocks.

`_node_lut` and `_source_streams` are **additive** — they are never cleared by `reset()`.
`reset()` becomes a no-op in the base class after this refactor (session-scoped state
no longer exists; all recording state accumulates until the object is discarded).

#### 2b. Class-property node factories (abstract)

Each subclass declares which node classes to use:

```python
@property
@abstractmethod
def source_node_class(self) -> type: ...

@property
@abstractmethod
def function_node_class(self) -> type: ...

@property
@abstractmethod
def operator_node_class(self) -> type: ...
```

#### 2c. Unified `compile()` — single pass

`compile()` becomes concrete in `AbstractPipelineBase`:

```python
def compile(self) -> None:
    # 1. Identify source hashes: inputs that have no registered invocation
    source_hashes = set(self._source_streams.keys()) - set(self._node_lut.keys())

    # 2. Create source nodes from raw captured streams
    node_map: dict[str, Any] = {
        h: self.source_node_class.from_stream(self._source_streams[h])
        for h in source_hashes
    }

    # 3. Topological pass — create FunctionNode / OperatorNode per invocation
    for key in nx.topological_sort(self._hash_graph):
        if key in node_map:
            continue  # already a source node
        if key not in self._node_lut:
            continue  # hash_graph may contain source hashes with no invocation
        inv = self._node_lut[key]
        upstream_nodes = [node_map[up.content_hash()] for up in inv.input_streams]
        label = inv.label or _default_label(inv)
        if isinstance(inv, FunctionInvocation):
            node_map[key] = self.function_node_class(
                inv.pod, upstream_nodes[0], label=label
            )
        else:
            node_map[key] = self.operator_node_class(
                inv.pod, tuple(upstream_nodes), label=label
            )

    self._persistent_node_map = node_map
    self._nodes = {
        inv.label: node_map[key]
        for key, inv in self._node_lut.items()
        if inv.label
    }
    self._compiled = True
```

`compile()` always rebuilds `_persistent_node_map` from scratch using the full
accumulated `_node_lut` and `_hash_graph`. It is called automatically on `__exit__`
(auto-compile default preserved) and explicitly by `from_invocations()`.

#### 2d. `InvocationGraph` value object

A lightweight value object that acts as the interchange format between Pipeline and
PipelineJob:

```python
@dataclass(frozen=True)
class InvocationGraph:
    invocations: tuple[PodInvocation, ...]   # topologically ordered
    source_streams: dict[str, StreamProtocol] # hash → stream
```

#### 2e. `to_invocations()` — extract LUT from compiled state

```python
def to_invocations(self) -> InvocationGraph:
    """Reconstruct an InvocationGraph from the compiled persistent_node_map.

    Must reconstruct from _persistent_node_map (not raw _node_lut) so that
    both the in-memory path and the save/load path produce consistent results.
    Specifically: _node_lut may contain raw concrete-source streams keyed by
    their original hash, but _persistent_node_map always contains the compiled
    node objects whose identity is stable.
    """
    source_streams: dict[str, StreamProtocol] = {}
    invocations: list[PodInvocation] = []

    for node_hash, node in self._persistent_node_map.items():
        if isinstance(node, self.source_node_class):
            source_streams[node_hash] = node  # source node acts as its own stream
        elif isinstance(node, self.function_node_class):
            inv = FunctionInvocation(
                pod=node.pod,
                input_streams=(node.upstream,),
                label=node.label,
            )
            invocations.append(inv)
        else:  # operator node
            inv = OperatorInvocation(
                pod=node.pod,
                input_streams=tuple(node.upstreams),
                label=node.label,
            )
            invocations.append(inv)

    # Sort topologically so from_invocations() can replay in order
    topo_keys = list(nx.topological_sort(self._hash_graph))
    invocations.sort(key=lambda inv: topo_keys.index(inv.content_hash()))
    return InvocationGraph(
        invocations=tuple(invocations),
        source_streams=source_streams,
    )
```

Note: `source_nodes` from `_persistent_node_map` implement `StreamProtocol` (they can
be used as upstreams in subsequent invocations), so they serve as the `source_streams`
in the `InvocationGraph` directly.

#### 2f. `from_invocations()` classmethod

```python
@classmethod
def from_invocations(
    cls,
    graph: InvocationGraph,
    name: str | tuple[str, ...] = "pipeline",
) -> "AbstractPipelineBase":
    instance = cls(name=name)   # calls __init__, initialises all fields cleanly
    # Load invocations into the LUT
    instance._node_lut = {inv.content_hash(): inv for inv in graph.invocations}
    # Load source streams
    instance._source_streams = dict(graph.source_streams)
    # Rebuild _hash_graph from the invocations so compile() can topologically sort
    for inv in graph.invocations:
        instance._hash_graph.add_node(inv.content_hash())
        for upstream in inv.input_streams:
            instance._hash_graph.add_edge(upstream.content_hash(), inv.content_hash())
    instance.compile()
    return instance
```

Calling `cls(name=name)` rather than `cls.__new__(cls)` ensures `__init__` fully
initialises all fields (including `_hash_graph`, `_compiled`, etc.) before they are
overwritten, avoiding fragile partial initialisation.

`from_invocations()` lives in `AbstractPipelineBase` to prevent either `Pipeline` or
`PipelineJob` from accessing the other's private state directly. Both transitions
(Pipeline → PipelineJob and PipelineJob → Pipeline) go through this classmethod.

---

### 3. `Pipeline` changes

**File:** `src/orcapod/pipeline/graph.py`

- **Remove** `record_function_pod_invocation` and `record_operator_pod_invocation`
  overrides (now inherited from base).
- **Remove** the custom `compile()` override (now inherited from base).
- **Add** class properties:
  ```python
  source_node_class = SourceNode
  function_node_class = FunctionNode
  operator_node_class = OperatorNode
  ```

`SourceNode.from_stream()` receives whatever stream the user passed in. It extracts the
stream's label and schema uniformly — no special-casing needed. If the stream is already
a `SourceNode`, it is returned unchanged.

**Cross-`with`-block reconnection for Pipeline:**  
After `compile()`, `SourceNode.identity_structure()` uses `(tag_schema, data_schema)` —
the original concrete stream hash is lost. This means that if the user opens a second
`with` block and passes the same concrete stream as input to a new invocation, there is
no guarantee the new source node will hash-match the source node from the first `with`
block. This is **by design and accepted**: Pipeline is a structural blueprint and
schema-normalization is irreversible.

---

### 4. `PipelineJob` changes

**File:** `src/orcapod/pipeline/job.py`

#### 4a. Remove duplicate recording state

Remove:
- `_rec_node_lut`, `_rec_upstreams`, `_rec_graph_edges`
- `_to_node_stream()`
- `_ensure_source_node()`

These are fully replaced by the unified base recording path.

#### 4b. Add class properties

```python
source_node_class = SourceJobNode
function_node_class = FunctionJobNode
operator_node_class = OperatorJobNode
```

#### 4c. `from_pipeline()` — thin composition

```python
@classmethod
def from_pipeline(cls, pipeline: Pipeline) -> "PipelineJob":
    return cls.from_invocations(pipeline.to_invocations())
```

#### 4d. `as_pipeline()` — thin composition

```python
def as_pipeline(self) -> Pipeline:
    return Pipeline.from_invocations(self.to_invocations())
```

#### 4e. `_compiled_pipeline` — lazy

```python
@property
def compiled_pipeline(self) -> Pipeline:
    if self._compiled_pipeline is None:
        self._compiled_pipeline = self.as_pipeline()
    return self._compiled_pipeline
```

`_build_execution_graph()` remains unchanged (out of scope for this refactor).

---

### 5. `SourceJobNode.from_stream()` — three-way logic

When `PipelineJob.compile()` calls `SourceJobNode.from_stream(stream)`, the method
must distinguish three input cases:

| Input type | Behavior |
|---|---|
| `SourceJobNode` | Copy bound_source; do NOT wrap SJN as the bound source of a SJN |
| `SourceNode` | Create unbound `SourceJobNode` (no data binding; schema preserved) |
| Any other stream (concrete source) | Create bound `SourceJobNode` with the stream as `bound_source` |

This three-way logic is necessary because `Pipeline.to_invocations()` produces
`SourceNode`-backed streams, and `PipelineJob.from_invocations()` must not lose the
distinction between "was this a concrete source?" and "was this a schema placeholder?".

---

### 6. `SourceJobNode.identity_structure()` — bound-state dependent (CRITICAL)

**This is already correctly implemented** and must not be changed:

```python
def identity_structure(self) -> Any:
    if self._bound_source is not None:
        return self._bound_source.identity_structure()
    return super().identity_structure()   # schema-based fallback
```

This property is what makes cross-`with`-block reconnection work for `PipelineJob`.
Because the `SourceJobNode`'s content hash is determined by the **bound source's hash**
(not just the schema), the same concrete source used in two separate `with` blocks will
produce the same node hash. This allows the compilation step to correctly stitch
invocations from different recording sessions together, and crucially survives
save/load cycles (the bound source's identity persists on disk).

For `Pipeline`, no analogous mechanism exists — `SourceNode.identity_structure()` is
always `("source_node", name, tag_schema, data_schema)`, so after compilation the
original concrete-stream hash is lost.

---

### 7. Cross-`with`-block reconnection semantics

| Class | Supported? | Mechanism |
|---|---|---|
| `PipelineJob` | **Yes** | `SourceJobNode.identity_structure()` delegates to `bound_source.identity_structure()` when bound; this hash is stable across saves/loads |
| `Pipeline` | **No, after compile** | `SourceNode.identity_structure()` is schema-based; original stream hash is discarded after `compile()`; there is no reliable way to reconnect across `with` blocks |

For `Pipeline`, users who need cross-`with`-block composition should use `PipelineJob`
or assemble multiple `Pipeline` objects explicitly.

---

### 8. Test plan

Tests are written **before** any production code is changed.

#### 8a. `PodInvocation` unit tests (`tests/test_pipeline/test_pod_invocation.py`)
- `FunctionInvocation.content_hash()` is stable for the same pod + stream.
- `OperatorInvocation.content_hash()` respects `argument_symmetry` (commutative
  operators: same hash regardless of input order; ordered operators: different hash).
- `FunctionInvocation` and `OperatorInvocation` are isinstance-distinguishable.

#### 8b. `AbstractPipelineBase` recording tests
- `_record_invocation()` adds to `_node_lut` and `_source_streams`.
- Calling `record_function_pod_invocation` and `record_operator_pod_invocation`
  both route through `_record_invocation()`.
- `_node_lut` is additive across multiple `with` blocks.
- `compile()` rebuilds `_persistent_node_map` from scratch each time.

#### 8c. `Pipeline` compile / transition tests
- `Pipeline.compile()` correctly identifies root streams and wraps them as `SourceNode`.
- `Pipeline.to_invocations()` returns an `InvocationGraph` consistent with the
  original recording.
- `Pipeline.from_invocations()` reconstructs an equivalent `Pipeline`.
- Schema-normalization: after compile, source nodes expose schema-based identity (not
  original stream hash).

#### 8d. `PipelineJob` compile / transition tests
- `PipelineJob.compile()` wraps concrete sources as bound `SourceJobNode`.
- `PipelineJob.to_invocations()` / `from_invocations()` round-trip is identity-preserving.
- `PipelineJob.from_pipeline()` produces a `PipelineJob` whose node graph matches
  what a direct recording of the same operations would produce.
- `PipelineJob.as_pipeline()` produces a `Pipeline` structurally equivalent to one
  recorded directly.

#### 8e. `SourceJobNode.from_stream()` tests
- `SJN → SJN`: bound source is copied; the input SJN is not used as a bound source.
- `SourceNode → SJN`: creates unbound SJN with matching schema.
- Concrete stream → SJN: creates bound SJN; `content_hash()` equals bound source hash.

#### 8f. Cross-`with`-block reconnection tests
- `PipelineJob`: same concrete source used in two separate `with` blocks produces
  the same source node hash, enabling correct graph stitching.
- `Pipeline`: source hash after compile does NOT equal original concrete stream hash
  (expected, documents the known limitation).

---

## Implementation order

1. **Tests** — write all tests in `tests/test_pipeline/` (they will fail initially).
2. **`pod_invocation.py`** — new file; tests 8a pass.
3. **`AbstractPipelineBase`** — add `_source_streams` field; make `_node_lut` and
   `_source_streams` additive (remove from `reset()`); remove `_upstreams` and
   `_graph_edges`; add `_record_invocation()`; add abstract class-property factories;
   add `InvocationGraph`; add `from_invocations()`, `to_invocations()`; make
   `record_function_pod_invocation` / `record_operator_pod_invocation` concrete;
   concrete `compile()`.
4. **`Pipeline`** — remove recording overrides and `compile()` override; add class
   properties.
5. **`SourceJobNode.from_stream()`** — three-way logic.
6. **`PipelineJob`** — remove `_rec_*` fields, `_to_node_stream`, `_ensure_source_node`;
   add class properties; thin `from_pipeline()` / `as_pipeline()`; lazy
   `compiled_pipeline` property.
7. Run full test suite; fix any regressions.

---

## Dependencies & Risks

- `ContentIdentifiableBase` must be importable in `pod_invocation.py` without circular
  imports — check import graph before writing the file.
- `SourceJobNode.from_stream()` three-way logic is subtle; the test for the
  SJN→SJN path is the guard against accidentally double-wrapping bound sources.
- The `_source_streams` collection in `_record_invocation()` must handle the case where
  the same stream appears as an input to multiple invocations (idempotent insert).
