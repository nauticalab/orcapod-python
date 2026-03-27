# Non-Active Node Semantics — Design Spec

**Date:** 2026-03-27
**Linear issue:** PLT-1182
**Branch:** `dev`
**Status:** Approved for implementation

---

## 1. Problem Statement

Iterating through any pipeline node's output must be a **read-only** operation. It must never trigger upstream computation. Only explicit calls to `node.run()` or `pipeline.run()` may drive computation.

Currently, `OperatorNode.iter_packets()` and `OperatorNode.as_table()` both call `self.run()` unconditionally:

```python
def iter_packets(self):
    self.run()           # ← triggers full computation
    assert self._cached_output_stream is not None
    return self._cached_output_stream.iter_packets()

def as_table(self, *, columns=None, all_info=False):
    self.run()           # ← triggers full computation
    assert self._cached_output_stream is not None
    return self._cached_output_stream.as_table(...)
```

Because `run()` calls `_compute_and_store()` → `operator.process(*input_streams)` → iterates every upstream node, a single call to `iter_packets()` on any `OperatorNode` cascades computation through the entire upstream DAG. This violates the intended semantics, where iteration is passive (read cached results) and `run()` is active (trigger computation).

`SourceNode` already demonstrates the correct pattern: its `iter_packets()` reads from `self.stream` or `self._cached_results` directly, and its `run()` is a no-op. `FunctionNode` is a generator-based iterator and does not eagerly invoke `run()`, so it is not directly affected by this bug — it will also benefit once `OperatorNode` is fixed, since `_ensure_iterator()` will no longer cascade into upstream computation.

---

## 2. Design Goals

1. **Iteration is read-only.** `iter_packets()` and `as_table()` on any node must never trigger computation.
2. **Graceful empty result.** When no cached data is available, iteration returns an empty iterator / empty table with the correct schema — not an error.
3. **`run()` remains the sole computation trigger.** The orchestrator (`SyncPipelineOrchestrator`, `AsyncPipelineOrchestrator`) drives execution by calling `node.run()` (via `node.execute()`) in topological order. This contract must not be bypassed.
4. **Minimal blast radius.** The fix is confined to `OperatorNode`. No changes to `FunctionNode`, `SourceNode`, the orchestrators, or the pipeline graph are required.
5. **No duplicate schema-building logic.** The empty-table construction that currently lives in `_replay_from_cache()` is extracted into a shared helper.

---

## 3. Architecture

### 3.1 Affected File

`src/orcapod/core/nodes/operator_node.py`

All other files remain unchanged.

### 3.2 New Private Helpers

**`_make_empty_table() -> pa.Table`**

Builds a zero-row PyArrow table whose columns match the node's full output schema (tags + packets). Uses `self.output_schema()` and `self.data_context.type_converter`. This is a pure, side-effect-free method.

**`_load_cached_stream_from_db() -> ArrowTableStream | None`**

Reads from the pipeline database without modifying any node state. Returns an `ArrowTableStream` if the DB has records (possibly zero rows), or `None` if no database is configured. Specifically:

- If `self._pipeline_database is None`: returns `None`
- Otherwise calls `self._pipeline_database.get_all_records(self.pipeline_path)`
  - If records exist: wraps in `ArrowTableStream` and returns it
  - If records are `None` (no rows yet): builds an empty table via `_make_empty_table()`, wraps in `ArrowTableStream`, and returns it

This helper is intentionally **state-free**: it does not assign to `self._cached_output_stream` and does not call `_update_modified_time()`.

### 3.3 Updated Methods

**`iter_packets()`** — remove `self.run()`, replace with passive read:

```
1. If self._cached_output_stream is not None → return its iter_packets()
2. Try _load_cached_stream_from_db() → if result is not None → return its iter_packets()
3. Otherwise → return iter([])   (empty iterator, no computation)
```

**`as_table()`** — remove `self.run()`, replace with passive read:

```
1. If self._cached_output_stream is not None → return its as_table(...)
2. Try _load_cached_stream_from_db() → if result is not None → return its as_table(...)
3. Otherwise → return self._make_empty_table()   (empty table, no computation)
```

**`_replay_from_cache()`** — refactor to call `_make_empty_table()` instead of
duplicating the schema-building logic inline. Behaviour is identical; the change
is purely internal deduplication.

### 3.4 Unchanged Behaviour

- `run()` logic is entirely unchanged. All three code paths (no DB / CacheMode.OFF+LOG / CacheMode.REPLAY) remain as-is.
- `execute()` (used by the orchestrators) is entirely unchanged.
- `_compute_and_store()` and `_store_output_stream()` are entirely unchanged.
- The `is_stale` / `clear_cache()` interaction inside `run()` is untouched.

---

## 4. Data Flow After the Fix

```
pipeline.run()
  └─ orchestrator.execute_nodes() (topological order)
       └─ node.execute()
            └─ node.run()
                 └─ _compute_and_store() or _replay_from_cache()
                      └─ populates self._cached_output_stream

user code: for tag, packet in operator_node:
  └─ iter_packets()          # read-only
       └─ reads self._cached_output_stream   # populated by run(), not here
```

Before `run()` is called, iteration yields an empty sequence. After `run()`, it
yields the computed results. This matches `SourceNode` behaviour exactly.

---

## 5. Error Handling

- **No `run()` called, no cache:** `iter_packets()` returns an empty iterator; `as_table()` returns a zero-row table. No exception is raised. This is intentional: calling `run()` first is the user's responsibility.
- **Schema availability:** `_make_empty_table()` calls `self.output_schema()`. If the output schema is unavailable (e.g., operator not yet connected to input streams), the exception propagates naturally — the same exception that `_replay_from_cache()` would raise today.
- **DB errors:** any database exception from `_load_cached_stream_from_db()` propagates unchanged.

---

## 6. Testing Plan

New test file: `tests/core/nodes/test_operator_node_non_active.py`

### 6.1 Core invariant tests

| Test | Description |
|------|-------------|
| `test_iter_packets_without_run_returns_empty` | Build a pipeline with two nodes. Iterate the downstream `OperatorNode` without calling `run()`. Assert zero packets returned and upstream `run()` was never called (use a mock/spy). |
| `test_as_table_without_run_returns_empty_table` | Same setup. Assert `as_table()` returns a zero-row PyArrow table with the correct schema (column names and types). |
| `test_iter_packets_after_run_returns_results` | Call `node.run()` first, then iterate. Assert the expected packets are returned. |
| `test_as_table_after_run_returns_results` | Call `node.run()` first, then `as_table()`. Assert non-empty table with expected data. |

### 6.2 Cascade isolation tests

| Test | Description |
|------|-------------|
| `test_single_node_run_does_not_cascade` | A → B → C chain. Call `B.run()` directly. Assert `A.run()` was NOT called (B's upstream is already a populated `SourceNode` stream, not another `OperatorNode`). |
| `test_pipeline_run_then_iterate` | Full pipeline `pipeline.run()` followed by iteration on every node. Assert correct results at each node and no redundant computation. |

### 6.3 CacheMode tests

| Test | Description |
|------|-------------|
| `test_iter_packets_replay_mode_returns_db_contents` | REPLAY mode: DB has pre-populated records. Assert `iter_packets()` without calling `run()` returns those records (loaded from DB via `_load_cached_stream_from_db`). |
| `test_iter_packets_replay_mode_no_records_returns_empty` | REPLAY mode: DB has no records for this node. Assert empty result. |
| `test_iter_packets_no_db_no_run_returns_empty` | No DB, no `run()` call. Assert empty. |

---

## 7. Out of Scope

- `FunctionNode` changes — not required; its `iter_packets()` is a generator and does not call `run()` eagerly.
- `SourceNode` changes — already correct.
- Changes to orchestrators, `pipeline/graph.py`, or any public API.
- Async (`async_iter_packets`) variants — follow-up if needed.
