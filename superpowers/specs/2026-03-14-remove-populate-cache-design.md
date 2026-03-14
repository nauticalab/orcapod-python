# Remove populate_cache — Self-Caching Nodes Design

## Overview

Remove the `populate_cache` method from all node types. Instead, nodes build their
in-memory cache as a natural side effect of the orchestrator's existing calls to
`get_cached_results`, `process_packet`, and `store_result`. This simplifies the
node protocol and eliminates the need for `Pipeline._apply_results()`.

## Motivation

The orchestrator already calls `get_cached_results`, `process_packet`, and `store_result`
on nodes during execution. Each of these is an opportunity for the node to build its
in-memory cache internally, rather than relying on an external `populate_cache` call.

The current flow has unnecessary indirection:
```
orchestrator.run() → returns OrchestratorResult
Pipeline._apply_results() → calls node.populate_cache() for each node
```

The simpler flow:
```
orchestrator.run() → nodes self-cache during execution → done
```

## Changes

### Node Protocol Changes

Remove `populate_cache` from all three protocols:
- `SourceNodeProtocol`: remove `populate_cache`
- `FunctionNodeProtocol`: remove `populate_cache`
- `OperatorNodeProtocol`: remove `populate_cache`

### SourceNode

- Remove `populate_cache()` method
- `store_result(results)`: now sets `self._cached_results = list(results)` in addition
  to any future DB persistence. This ensures `iter_packets()` returns from cache after
  orchestrated execution.

### FunctionNode

- Remove `populate_cache()` method
- `get_cached_results(entry_ids)`: after retrieving cached results, populates
  `_cached_output_packets` with the returned entries.
- `store_result(tag, input_packet, output_packet)`: after writing pipeline record,
  adds the result to `_cached_output_packets`. Also sets `_needs_iterator = False`
  and `_cached_input_iterator = None` to indicate iteration state is managed externally.

### OperatorNode

- Remove `populate_cache()` method
- `get_cached_output()`: already sets `_cached_output_stream` via `_replay_from_cache()`.
  No change needed.
- `store_result(results)`: now also sets `_cached_output_stream` from the materialized
  results (same as what `populate_cache` did). This ensures `iter_packets()` / `as_table()`
  work after orchestrated execution.

### Pipeline.run()

- Remove `_apply_results()` method (no longer needed)
- `run()` no longer calls `_apply_results()` — nodes are self-cached

### OrchestratorResult

- Keep as-is. The orchestrator still returns results for programmatic inspection.
  The caller may want to examine what was produced without going through node accessors.

### SyncPipelineOrchestrator

- No changes needed. It already calls `store_result` and `get_cached_results` on nodes.
  The orchestrator is unaware of caching — that's the node's concern.

## Testing

- Remove all `populate_cache` tests
- Update `store_result` tests to verify internal cache is populated
- Update `get_cached_results` tests to verify internal cache is populated
- Verify `iter_packets()` / `as_table()` work after orchestrated execution (existing
  integration tests already cover this via `Pipeline.run()`)
