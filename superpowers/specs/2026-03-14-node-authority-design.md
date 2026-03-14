# Node Authority — Self-Validating, Self-Persisting Nodes

## Overview

Nodes become authoritative executors: they validate inputs, compute, persist, and cache
results internally. The orchestrator no longer calls `store_result` or accesses internal
pods directly. Instead, it calls `process_packet` or `process` on the node, and the node
handles everything.

This refactoring also introduces a clear **pod vs node** distinction:
- **Pod**: computation definition. `process() → StreamProtocol` (lazy, deferred).
- **Node**: computation executor. `process() → list[tuple[Tag, Packet]]` (eager,
  materialized, with schema validation + persistence + caching).

## Motivation

The previous design had the orchestrator reaching into `node.operator` to call the pod
directly, then telling the node to `store_result`. This broke encapsulation: the
orchestrator was acting as an intermediary between the node and its own pod. The node
should be the sole authority over its results — it should process input, decide if it's
valid, and handle all persistence internally.

**Schema validation** is the trust mechanism. Each node knows its expected input schema
(including system tags, which encode pipeline topology). If incoming data matches, the
node can treat the results as its own and persist them. If not, the input doesn't belong
to this node.

## Changes

### Remove from all node types

- `store_result()` — removed entirely. Persistence happens inside `process` / `process_packet`.

### Remove from protocols

- `store_result` from `SourceNodeProtocol`, `FunctionNodeProtocol`, `OperatorNodeProtocol`
- `operator` property from `OperatorNodeProtocol` (orchestrator no longer accesses it)

### FunctionNode

**`process_packet(tag, packet) → tuple[Tag, Packet | None]`**

Reverts to the original bundled behavior (pre-split), plus schema validation:

1. Validate tag + packet schema (including system tags) against expected input schema
   from `self._input_stream.output_schema()` / `self._input_stream.keys()`.
2. Compute via `CachedFunctionPod` (function-level memoization in result DB) or raw
   `FunctionPod` (no DB).
3. Write pipeline provenance record (via `add_pipeline_record`).
4. Cache result in `_cached_output_packets`.
5. Return `(tag_out, output_packet)`.

Validation checks: tag column names + types, packet column names + types, system tag
column names. System tags are critical because they encode pipeline topology — mismatched
system tags mean the data came from a different pipeline path.

**`process(input_stream) → list[tuple[Tag, Packet]]`** (NEW)

Bulk entry point that validates schema once against the stream, then iterates packets
using an internal unchecked path:

1. Validate stream schema (tag + packet + system tags) against expected input.
2. For each (tag, packet) in stream: call `_process_packet_internal(tag, packet)` —
   same as `process_packet` but skips per-packet schema validation.
3. Return materialized list of `(tag, output_packet)` pairs (excluding None outputs).

This is more efficient when per-packet observer hooks aren't needed.

**Internal structure:**

```
process_packet(tag, pkt)
  → _validate_input_schema(tag, pkt)
  → _process_packet_internal(tag, pkt)

process(input_stream)
  → _validate_stream_schema(input_stream)
  → for tag, pkt in input_stream.iter_packets():
      _process_packet_internal(tag, pkt)
  → return materialized results

_process_packet_internal(tag, pkt)
  → compute (CachedFunctionPod or FunctionPod)
  → write pipeline record (if DB attached)
  → cache in _cached_output_packets
  → return (tag_out, output_packet)
```

### OperatorNode

**`process(*input_streams) → list[tuple[Tag, Packet]]`** (NEW)

Replaces the orchestrator's direct access to `node.operator`:

1. Validate each input stream's schema (tag + system tags + packet) against expected
   upstream schemas from `self._input_streams`.
2. Compute via `self._operator.process(*input_streams)`.
3. Materialize results.
4. Persist to pipeline DB (if LOG mode, via existing `_store_output_stream`).
5. Cache in `_cached_output_stream`.
6. Return materialized list of `(tag, output_packet)` pairs.

**Remove**: public `operator` property (orchestrator no longer needs it).

### SourceNode

- Remove `store_result()`. The source is authoritative by definition — it produces its
  own data via `iter_packets()`.
- The orchestrator materializes the buffer from `iter_packets()`. If caching is needed
  for transient sources, `iter_packets()` can cache internally on first call (existing
  `_cached_results` field supports this — set during first iteration).

### Node Protocols (updated)

```python
class SourceNodeProtocol(Protocol):
    node_type: str
    def iter_packets(self) -> Iterator[tuple[TagProtocol, PacketProtocol]]: ...

class FunctionNodeProtocol(Protocol):
    node_type: str
    def get_cached_results(self, entry_ids: list[str]) -> dict[str, tuple[TagProtocol, PacketProtocol]]: ...
    def compute_pipeline_entry_id(self, tag: TagProtocol, packet: PacketProtocol) -> str: ...
    def process_packet(self, tag: TagProtocol, packet: PacketProtocol) -> tuple[TagProtocol, PacketProtocol | None]: ...
    def process(self, input_stream: StreamProtocol) -> list[tuple[TagProtocol, PacketProtocol]]: ...

class OperatorNodeProtocol(Protocol):
    node_type: str
    def get_cached_output(self) -> StreamProtocol | None: ...
    def process(self, *input_streams: StreamProtocol) -> list[tuple[TagProtocol, PacketProtocol]]: ...
```

### SyncPipelineOrchestrator (updated)

The orchestrator no longer calls `store_result` or accesses `node.operator`. Updated
execution paths:

**Source execution:**
```python
def _execute_source(self, node):
    self._observer.on_node_start(node)
    output = list(node.iter_packets())
    self._observer.on_node_end(node)
    return output
```

**Function execution (with observer hooks):**
```python
def _execute_function(self, node, upstream_buffer):
    self._observer.on_node_start(node)
    # Phase 1: pipeline-level cache lookup
    upstream_entries = [
        (tag, pkt, node.compute_pipeline_entry_id(tag, pkt))
        for tag, pkt in upstream_buffer
    ]
    cached = node.get_cached_results([eid for _, _, eid in upstream_entries])

    output = []
    for tag, pkt, entry_id in upstream_entries:
        self._observer.on_packet_start(node, tag, pkt)
        if entry_id in cached:
            tag_out, result = cached[entry_id]
            self._observer.on_packet_end(node, tag, pkt, result, cached=True)
            output.append((tag_out, result))
        else:
            tag_out, result = node.process_packet(tag, pkt)
            self._observer.on_packet_end(node, tag, pkt, result, cached=False)
            if result is not None:
                output.append((tag_out, result))

    self._observer.on_node_end(node)
    return output
```

**Operator execution:**
```python
def _execute_operator(self, node, upstream_buffers):
    self._observer.on_node_start(node)
    cached = node.get_cached_output()
    if cached is not None:
        output = list(cached.iter_packets())
    else:
        input_streams = [
            self._materialize_as_stream(buf, upstream_node)
            for buf, upstream_node in upstream_buffers
        ]
        output = node.process(*input_streams)  # node handles everything
    self._observer.on_node_end(node)
    return output
```

### Pipeline.run()

No changes needed beyond what's already there (previous refactoring already removed
`_apply_results`).

### Backward Compatibility

- `FunctionNode.run()` and `iter_packets()` continue to work for the non-orchestrated
  pull-based path. They use `_process_packet_internal` (same as the orchestrator path).
- `OperatorNode.run()` continues to work for the non-orchestrated path.
- The existing `_process_and_store_packet` on FunctionNode can be replaced by
  `_process_packet_internal` (same semantics).

## Schema Validation Details

Validation checks the following against the node's expected input:

1. **Tag column names** — must match exactly (user-defined tag columns).
2. **Tag column types** — must match the expected Arrow types.
3. **System tag column names** — must match (these encode pipeline topology via
   `_tag::source:...` naming with pipeline hash extensions).
4. **Packet column names** — must match.
5. **Packet column types** — must match.

Validation raises `InputValidationError` (or similar) with a clear message indicating
which columns/types don't match.

For `process_packet(tag, packet)`: validate from the Tag and Packet datagram objects
directly (they expose `keys()`, `arrow_schema()`, etc.).

For `process(input_stream)` / `process(*input_streams)`: validate from the stream's
`output_schema()` method, which is cheaper (once per stream, not per packet).

## Testing

- Update `process_packet` tests to verify it writes pipeline records again (revert the
  "purity" tests — `process_packet` is no longer pure).
- Add schema validation tests (valid schema passes, mismatched schema raises).
- Add `OperatorNode.process()` tests (basic computation, DB persistence, caching).
- Add `FunctionNode.process()` tests (bulk processing, single schema validation).
- Remove all `store_result` tests.
- Verify orchestrator integration tests still pass.
- Verify backward compat: `node.run()` and `iter_packets()` still work.
