# ITL-516: Unify FunctionPod.async_execute() and FunctionJobNode.async_execute() Concurrency Paths

**Date:** 2026-07-09
**Issue:** [ITL-516](https://linear.app/enigma-metamorphic/issue/ITL-516)

---

## Overview

`FunctionPod.async_execute()` and `FunctionJobNode.async_execute()` are currently two independent
implementations of the same async dispatch loop. Investigation revealed that `FunctionPod.async_execute()`
is an orphan method: it is not part of `FunctionPodProtocol`, never called by the orchestrator, and
has no sync counterpart on `FunctionPod` (which uses `process()` → `FunctionPodStream` for its own
execution model). The method was written alongside `FunctionJobNode.async_execute()` as the concurrency
pattern was being developed, leaving two divergent copies.

The correct model is the one OperatorNode already uses: `OperatorNode.async_execute()` delegates the
actual computation to `self._operator.async_execute()` and wraps only the DB-persistence concerns.
`FunctionJobNode` should do the same — delegate concurrent dispatch to `FunctionPod.async_execute()`
and own only the routing, caching, and pipeline-record concerns.

---

## Goals & Success Criteria

- `FunctionPod.async_execute()` is the single authoritative async dispatch loop, with observer support
- `FunctionJobNode.async_execute()` owns routing (cache hits vs misses), pipeline-record writing, and
  observer lifecycle — but never re-implements the concurrent dispatch loop
- The three stages (routing, computation, recording) run concurrently via `asyncio.TaskGroup`, so
  FunctionPod begins processing cache misses immediately as the router produces them
- Downstream nodes never receive tags carrying internal correlation metadata
- Existing behaviour is preserved: two-phase DB-backed execution for FunctionJobNode, standalone
  streaming for FunctionPod
- Tests cover the unified path

---

## Scope & Boundaries

**In scope:**

- Add `observer` parameter to `FunctionPod.async_execute()`
- Redesign `FunctionJobNode.async_execute()` as a 3-stage concurrent pipeline using
  `compute_channel`, `result_channel`, and a correlation-key tag-stamp mechanism
- Delete the orphan `FunctionPod.async_execute()` backpressure regression test; add equivalent
  coverage via `FunctionJobNode.async_execute()`
- Clean up any dead imports in `function_node.py` after removing the manual semaphore setup
  (e.g. `PipelineConfig` if it is no longer referenced outside `async_execute`)

**Out of scope:**

- `FunctionPod.process_data()` / `async_process_data()` — unchanged
- `FunctionJobNode.execute()` sync path — unchanged
- Operator node execution — unchanged
- `NodeConfig` / `PodConfig` semantics (ITL-512)
- Changes to synchronous execution paths (`process_data`, `_process_data_internal`)

---

## Design

### Why FunctionPod.async_execute() was an orphan

`FunctionPod` has no `execute()` method. Its sync model is `process(*streams)` → lazy
`FunctionPodStream`. `async_execute()` was written without a sync counterpart, without being
added to `FunctionPodProtocol`, and without being called by the orchestrator. The orchestrator
dispatches only to `SourceNodeProtocol`, `FunctionNodeProtocol`, and `OperatorNodeProtocol` —
FunctionPod is none of these. The method's sole caller was a single regression test.

### The OperatorNode analogy

`OperatorNode.async_execute()` delegates to `self._operator.async_execute()` in the no-DB path
and wraps it in a forwarding channel in the LOG path. It never reimplements the operator's logic.
This is the correct pattern: the pod owns computation, the node owns DB concerns.

For function execution, the same separation applies: `FunctionPod.async_execute()` owns concurrent
dispatch; `FunctionJobNode.async_execute()` owns routing, caching, and pipeline recording.

### async_execute() moves to _FunctionPodBase

Currently `async_execute()` is defined only on `FunctionPod`. `CachedFunctionPod` (used for
both persistent and ephemeral execution) inherits from `WrappedFunctionPod → _FunctionPodBase`
— not from `FunctionPod` — so it has no `async_execute()` today.

`_FunctionPodBase` already defines `async_process_data()`, which each subclass overrides
correctly (`FunctionPod` calls the data function, `CachedFunctionPod` checks the cache then
stores). Moving `async_execute()` to `_FunctionPodBase` gives all pod types the dispatch loop
for free via polymorphism: the loop calls `self.async_process_data()`, which resolves to the
right implementation at runtime.

`FunctionPod` retains no override — it inherits the loop from `_FunctionPodBase`.

Add an optional `observer` parameter (same pattern as all node `async_execute` signatures):

```python
# on _FunctionPodBase
async def async_execute(
    self,
    inputs: Sequence[ReadableChannel[tuple[TagProtocol, DataProtocol]]],
    output: WritableChannel[tuple[TagProtocol, DataProtocol]],
    pipeline_config: PipelineConfig | None = None,
    *,
    observer: ExecutionObserverProtocol | None = None,
) -> None:
```

When `observer` is provided, fire `on_data_start`, `on_data_end`, and `on_data_crash` per item.
When `None`, behaviour is unchanged (no-op). All pod types — `FunctionPod`, `CachedFunctionPod`
(persistent), `CachedFunctionPod` (ephemeral) — are now valid delegation targets from
`FunctionJobNode`.

### FunctionJobNode.async_execute() — 3-stage concurrent pipeline

#### No-DB path (simple delegation)

When `_cached_function_pod is None`, delegate directly — no routing, no recording:

```python
await self._function_pod.async_execute(
    [input_channel], output, observer=ctx_obs
)
```

Mirrors OperatorNode's no-DB delegation.

#### DB path (3-stage concurrent pipeline)

Before entering the TaskGroup, select the execution pod and `is_ephemeral` flag based on
`_node_config.is_result_ephemeral`:

```python
if self._node_config.is_result_ephemeral:
    execution_pod = self._ephemeral_cached_pod   # CachedFunctionPod → ephemeral DB
    is_ephemeral = True
else:
    execution_pod = self._cached_function_pod    # CachedFunctionPod → persistent DB
    is_ephemeral = False
```

Both are `CachedFunctionPod` instances that now inherit `async_execute()` from `_FunctionPodBase`.
Their `async_process_data()` overrides handle the correct store (ephemeral vs persistent) and
set `RESULT_COMPUTED_FLAG` on the output data. The 3-stage pipeline is identical for both;
only the pod and the flag differ.

Three coroutines run inside a single `asyncio.TaskGroup`:

```
input_channel
      │
      ▼  route_inputs() — task 1
      ├── cache hit → emit to output (observer: on_data_start + on_data_end cached=True)
      └── cache miss → stamp tag with correlation key → compute_channel
                                   │
                                   ▼  execution_pod.async_execute() — task 2
                                      (reads compute_channel, writes result_channel)
                              result_channel
                                   │
                                   ▼  record_and_forward() — task 3
                              read correlation key from output_tag
                              → look up (original_tag, input_data) in input_store
                              → add_pipeline_record(original_tag, input_data,
                                                    is_ephemeral=is_ephemeral, ...)
                              → strip correlation key from output_tag
                              → emit (clean_tag, output_data) to output
```

**Backpressure:** `compute_channel` and `result_channel` are bounded channels. If the execution
pod is slower than the router, `route_inputs` blocks on `compute_channel.writer.send()`; if the
recorder is slower than the pod, the pod blocks on `result_channel.writer.send()`.

**TaskGroup lifecycle:** The TaskGroup exits only when all three tasks complete. `route_inputs`
closes `compute_channel.writer` when the input is exhausted, signalling the pod to finish. The
pod closes `result_channel.writer` in its `finally`, signalling the recorder to finish.
`FunctionJobNode.async_execute()`'s own `finally` closes the final `output`.

### Correlation key mechanism

To give the recorder access to `(original_tag, input_data)` — needed by `add_pipeline_record` —
without changing `result_channel`'s type or FunctionPod's interface:

1. **Router** generates a correlation key (`bytes`, e.g. `uuid.uuid4().bytes`) per cache miss,
   stamps it into the tag as a private meta column (`_tag_node_input_ref`), stores
   `{key: (original_tag, input_data)}` in a local dict (`input_store`), then sends the
   stamped tag + data to `compute_channel`.

2. **FunctionPod** passes the tag through completely unchanged — the stamp travels to
   `result_channel` without modification.

3. **Recorder** reads `_tag_node_input_ref` from the output tag, retrieves
   `(original_tag, input_data)` from `input_store`, calls `add_pipeline_record`, then
   **strips the `_tag_node_input_ref` column** from the output tag before emitting to `output`.

Stripping the correlation key before downstream emission is a hard correctness requirement:
downstream nodes must never receive tags with internal bookkeeping columns.

The meta column name `_tag_node_input_ref` is a private constant defined in `function_node.py`.
It must not appear in any public schema or protocol.

### CACHE_ONLY and UNAVAILABLE modes

These are handled before the 3-stage pipeline, unchanged from the current implementation:

```python
status = self.load_status
if status == LoadStatus.CACHE_ONLY:
    await self._async_execute_cache_only(output, observer=observer)
    return
if status == LoadStatus.UNAVAILABLE:
    await output.close()
    raise RuntimeError(...)
```

### Observer responsibilities by stage

`on_node_start` is called once before the TaskGroup is entered. `on_node_end` is called once
after the TaskGroup exits. Per-item hooks are split across stages:

| Stage | Observer calls |
|---|---|
| **before TaskGroup** | `on_node_start` (once) |
| **route_inputs** | `on_data_start` + `on_data_end(cached=True)` for cache hits |
| **FunctionPod.async_execute** | `on_data_start` + `on_data_end(cached=False)` or `on_data_crash` for misses |
| **record_and_forward** | none |
| **after TaskGroup** | `on_node_end` (once) |

### Dead import cleanup in function_node.py

After the refactor, `FunctionJobNode.async_execute()` no longer calls `resolve_concurrency` or
creates semaphores directly — that logic moves into `FunctionPod.async_execute()`. If `PipelineConfig`
is imported in `function_node.py` solely for the `resolve_concurrency(pod_config, PipelineConfig())`
call in `async_execute`, it becomes dead and should be removed. Verify during implementation.

### Test changes

- **Remove** `TestAsyncExecuteBackpressure` from `test_regression_fixes.py` (tests the method
  in isolation, not through any real execution path)
- **Add** `tests/test_core/nodes/test_function_job_node_async_execute.py`:
  - All items processed via `FunctionJobNode.async_execute()` (no-DB and DB paths)
  - Concurrency limit respected: `max_concurrency=1` limits concurrent tasks to 1
  - Cache hits emitted directly, cache misses computed
  - Correlation key is absent from output tags
  - Semaphore released on computation failure
  - Observer callbacks fire correctly for hits and misses

---

## Key invariants

1. Output tags carry no `_tag_node_input_ref` column — verified by test
2. `add_pipeline_record` is called exactly once per cache miss
3. Cache hits bypass computation entirely
4. `_FunctionPodBase.async_execute()` is unaware of pipeline DB, base_entry_id, or correlation keys
5. The no-DB path delegates to `_function_pod`, not `_cached_function_pod` or `_ephemeral_cached_pod`
6. The ephemeral path uses `_ephemeral_cached_pod` as task 2 and passes `is_ephemeral=True` to `add_pipeline_record`
7. The persistent path uses `_cached_function_pod` as task 2 and passes `is_ephemeral=False` to `add_pipeline_record`
