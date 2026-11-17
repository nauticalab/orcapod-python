# Type-Specific Pod Access on Pipeline and PipelineJob

**Date:** 2026-06-11
**Linear issue:** PLT-420 (sub-task of PLT-422)
**Status:** Approved

---

## Overview

`AbstractPipelineBase.nodes` returns all compiled nodes — sources, function pods, and
operator pods — in a single `dict[str, Any]` keyed by label. There is no built-in way
to access only one type without filtering manually. This spec adds three read-only
properties — `source_pods`, `function_pods`, `operator_pods` — that each return a
`dict[str, Any]` containing only nodes of the matching type.

## Goals & Success Criteria

- `pipeline.function_pods` returns a `dict[str, Any]` containing only nodes whose
  `node_type == "function"`.
- `pipeline.source_pods` returns a `dict[str, Any]` containing only nodes whose
  `node_type == "source"`.
- `pipeline.operator_pods` returns a `dict[str, Any]` containing only nodes whose
  `node_type == "operator"`.
- Each property is a subset of `.nodes`: same keys and values, just filtered.
- All three properties are available on both `Pipeline` and `PipelineJob` (via
  `AbstractPipelineBase`).
- All three properties are declared on `PipelineProtocol` so callers that accept the
  protocol can use them without downcasting.
- The union of all three properties covers exactly the same set of entries as `.nodes`.

## Scope & Boundaries

In scope:
- Adding `source_pods`, `function_pods`, `operator_pods` properties to
  `AbstractPipelineBase`.
- Declaring the same three properties on `PipelineProtocol`.
- Unit tests covering all three properties on `Pipeline`.

Out of scope:
- Changing the return type of `.nodes`.
- Adding type-specific accessors to `OrcaDAG` or any other data structure.
- Filtering by node type in any other part of the system (serialization, rendering, etc.).
- Any changes to `PipelineJob`-specific logic (the properties are inherited from the
  base class and work the same way).

## Design

### `AbstractPipelineBase` (src/orcapod/pipeline/base.py)

Three new properties are added in the **Properties** section, immediately after the
existing `.nodes` property:

```python
@property
def source_pods(self) -> dict[str, Any]:
    """Copy of compiled nodes that are source nodes (label → node)."""
    return {k: v for k, v in self._nodes.items() if v.node_type == "source"}

@property
def function_pods(self) -> dict[str, Any]:
    """Copy of compiled nodes that are function-pod nodes (label → node)."""
    return {k: v for k, v in self._nodes.items() if v.node_type == "function"}

@property
def operator_pods(self) -> dict[str, Any]:
    """Copy of compiled nodes that are operator-pod nodes (label → node)."""
    return {k: v for k, v in self._nodes.items() if v.node_type == "operator"}
```

Each property performs a filtered dict comprehension over `self._nodes`, which is the
same backing store used by `.nodes`. Because `.nodes` already returns a copy,
these properties are consistent: callers receive a fresh dict on every access.

### `PipelineProtocol` (src/orcapod/protocols/pipeline_protocols.py)

Three property stubs are added to `PipelineProtocol`, mirroring the signature above:

```python
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
```

### Testing (tests/test_pipeline/test_pipeline.py or new file)

Tests to add:
- `test_function_pods_returns_only_function_nodes` — pipeline with one source + one
  function pod; `function_pods` has exactly one entry, that entry is the function node.
- `test_source_pods_returns_only_source_nodes` — same pipeline; `source_pods` has
  exactly one entry, that entry is the source node.
- `test_operator_pods_returns_only_operator_nodes` — pipeline with one source + one
  operator; `operator_pods` has exactly one entry.
- `test_type_pods_are_subsets_of_nodes` — union of all three covers exactly `.nodes`.
- `test_function_pods_empty_when_no_function_nodes` — pipeline with only a source;
  `function_pods` returns `{}`.
- `test_operator_pods_empty_when_no_operator_nodes` — pipeline with only a function pod;
  `operator_pods` returns `{}`.
