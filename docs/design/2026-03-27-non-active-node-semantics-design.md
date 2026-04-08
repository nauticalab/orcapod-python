# Non-Active Node Semantics — Design Spec

**Date:** 2026-03-27
**Linear issue:** PLT-1182
**Branch:** `dev`
**Status:** Approved for implementation

---

## 1. Problem Statement

Iterating through any pipeline node's output must be a **read-only** operation. It must never trigger upstream computation. Only explicit calls to `node.run()`, `node.execute()`, or `pipeline.run()` may drive computation.

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

Because `run()` calls `_compute_and_store()` → `operator.process(*input_streams)` → iterates every upstream node, a single call to `iter_packets()` on any `OperatorNode` cascades computation through the entire upstream DAG.

`SourceNode` demonstrates the "no computation on iteration" principle: its `iter_packets()` reads from `self.stream` or `self._cached_results` directly and never calls `run()`. (Note: `SourceNode.iter_packets()` raises `RuntimeError` in read-only mode rather than returning empty — the new empty-fallback behaviour for `OperatorNode` is a deliberate extension beyond what `SourceNode` provides, appropriate because an `OperatorNode` always has a known output schema from `output_schema()`.)

**Transitively affected methods in `StreamBase`:**

- `__iter__` delegates to `iter_packets()`
- `flow()` calls `iter_packets()` in a list comprehension; its current docstring ("This will trigger any upstream computation") will be incorrect after the fix and must be updated
- `as_polars_df()` / `as_df()` / `as_lazy_frame()` / `as_pandas_df()` call `as_table()` transitively
- `_repr_html_()` and `view()` call `as_polars_df()` (used in Jupyter notebooks)

All of these will become passive after the fix — empty before `run()`, correct results after. For the display methods (`_repr_html_`, `view`), this is expected and correct: they are used to inspect results *after* running the pipeline.

**Note on `FunctionNode`:** `FunctionNode.iter_packets()` is a generator and does not call `run()`. It calls `_ensure_iterator()` → `self._input_stream.iter_packets()` on its wired input. With the old `OperatorNode` this cascaded upstream computation; after the fix, that cascade is eliminated. However, `FunctionNode.iter_packets()` is **not** fully passive — it still executes its own per-packet function for packets not already cached in the result DB (Phase 2 processing). This is intentional and unaffected by this change. What changes is only that the upstream `OperatorNode` cascade no longer occurs. No changes to `FunctionNode` are required.

---

## 2. Design Goals

1. **Iteration is read-only.** `iter_packets()`, `as_table()`, `__iter__()`, `flow()`, and all methods that delegate to them must never trigger upstream computation.
2. **Graceful empty result.** When no cached data is available, iteration returns an empty iterator / empty table with the correct schema — not an error.
3. **`run()` and `execute()` remain the sole computation triggers.** There are two distinct legitimate computation paths:
   - `node.run()` — direct/standalone computation by user code.
   - `node.execute(*input_streams, observer=...)` — used by the orchestrator (`SyncPipelineOrchestrator`, `AsyncPipelineOrchestrator`) in topological order; calls `_operator.process()` directly without going through `run()`.
   - `node.async_execute(...)` — async orchestrator path; in REPLAY mode calls `_replay_from_cache()` directly (stateful, correct for a computation path, not `_load_cached_stream_from_db`).
   All three paths remain entirely unchanged by this fix.
4. **Minimal blast radius.** The primary fix is in `OperatorNode`. One docstring in `StreamBase.flow()` also needs updating. No logic changes elsewhere.
5. **No duplicate schema-building logic.** The empty-table construction in `_replay_from_cache()` is extracted into `_make_empty_table()`. Both `_replay_from_cache()` and `_load_cached_stream_from_db()` will call `_make_empty_table()` for the zero-records case — this intentional sharing via the helper is the correct level of deduplication; the two callers are not merged because they have different contracts (stateful vs state-free).

---

## 3. Architecture

### 3.1 Affected Files

- **`src/orcapod/core/nodes/operator_node.py`** — primary fix (logic changes)
- **`src/orcapod/core/streams/base.py`** — one docstring update only (no logic changes)

### 3.2 New Private Helpers in `OperatorNode`

**`_make_empty_table() -> "pa.Table"`**

Builds a zero-row PyArrow table whose columns match the node's full output schema (tags + packets). Uses `self.output_schema()` and `self.data_context.type_converter`. This is a pure, side-effect-free method.

- The return type annotation must use a string literal (`"pa.Table"`) because `pa` is imported via `LazyModule` at runtime; the real type is only available under `TYPE_CHECKING`.
- `output_schema()` is safe on live and read-only deserialized nodes (uses `_stored_schema` when `_operator is None`).
- **`data_context` requires `_operator is not None`.** The `data_context` property calls `self._operator.data_context_key` with no guard for `_operator is None`. Therefore `_make_empty_table()` will raise `AttributeError` on read-only deserialized nodes. This is a pre-existing limitation: `_replay_from_cache()` has the same issue today. In practice, calling `_make_empty_table()` on a read-only node with no DB records is an invalid state — a read-only REPLAY node with no records means the LOG run that should have populated the DB never ran. Implementers should not add a workaround; the existing limitation is acceptable.

**`_load_cached_stream_from_db() -> "ArrowTableStream | None"`**

Reads from the pipeline database **only in `CacheMode.REPLAY` mode** without modifying any node state. Specifically:

Guards (return `None` immediately if any apply):
- `self._pipeline_database is None`
- `self._cache_mode != CacheMode.REPLAY`
  (In `LOG` and `OFF` modes the DB may contain historical records from prior runs. Reading them here would expose stale data.)

If all guards pass, call `self.pipeline_path` directly (no try/except). This is safe: by the time we reach this point, `_pipeline_database is not None`, and `pipeline_path` only raises `RuntimeError` when `_pipeline_database is None`. For live nodes `_pipeline_node_hash` is always set in `__init__`; for read-only deserialized nodes `_operator is None` causes `pipeline_path` to return `_stored_pipeline_path`.

Then call `self._pipeline_database.get_all_records(self.pipeline_path)`:
- If records are non-None (zero or more rows): wrap in `ArrowTableStream(records, tag_columns=self.keys()[0])` and return it.
  Note: the DB stores records with a `_record_hash` column added by `_store_output_stream`. `get_all_records` does not strip this column. `_load_cached_stream_from_db` inherits this behavior — it returns an `ArrowTableStream` that includes `_record_hash`. This matches the existing behavior of `_replay_from_cache`, which also does not strip `_record_hash`.
- If records are `None` (no prior LOG run has written to this path): build an empty table via `_make_empty_table()`, wrap in `ArrowTableStream`, and return it.

**Important semantics:** When `_pipeline_database is not None` and `_cache_mode == CacheMode.REPLAY`, this method **always returns non-None** (either real rows or a zero-row table). The step-3 empty fallback in `iter_packets` / `as_table` is therefore never reached when REPLAY+DB is active.

This helper is intentionally **state-free**: it does not assign to `self._cached_output_stream` and does not call `_update_modified_time()`. Consequently, repeated calls to `iter_packets()` or `as_table()` that reach step 2 will re-query the DB on every call — there is no in-memory caching via this path. Caching from DB results is the responsibility of the computation paths (`run()`, `execute()`).

### 3.3 Updated Methods in `OperatorNode`

**`iter_packets()`** — remove `self.run()`, replace with passive read:

```
1. If self._cached_output_stream is not None → return its iter_packets()
2. db_stream = _load_cached_stream_from_db()
   if db_stream is not None → return db_stream.iter_packets()
3. Otherwise → return iter([])
   (reachable only when: no DB, or CacheMode.OFF/LOG with no prior run)
```

**`as_table()`** — remove `self.run()`, replace with passive read:

```
1. If self._cached_output_stream is not None → return its as_table(...)
2. db_stream = _load_cached_stream_from_db()
   if db_stream is not None → return db_stream.as_table(...)
3. Otherwise → return self._make_empty_table()
   (reachable only when: no DB, or CacheMode.OFF/LOG with no prior run)
```

**`_replay_from_cache()`** — refactor the empty-table branch only:
- Replace the inline `empty_fields` loop with a call to `_make_empty_table()`.
- Everything else (assigning to `self._cached_output_stream`, calling `_update_modified_time()`) is **unchanged**.
- This method does **not** call `_load_cached_stream_from_db()` — they have different contracts. `_replay_from_cache` is stateful (computation path); `_load_cached_stream_from_db` is state-free (read path). Both independently call `_make_empty_table()` for the zero-records case; this is intentional, with `_make_empty_table()` as the only shared extraction.

### 3.4 Docstring Update in `StreamBase`

In `src/orcapod/core/streams/base.py`, update the `flow()` docstring:

**Before:** "This will trigger any upstream computation of the stream."

**After:** "Returns the entire collection of (TagProtocol, PacketProtocol) as a list. This is a read-only operation — results reflect whatever has been computed by a prior `run()` or `execute()` call. If no computation has been performed, returns an empty list."

No other changes to `base.py`.

### 3.5 Unchanged Behaviour

- `run()` logic is entirely unchanged. All three code paths (no DB / CacheMode.OFF+LOG / CacheMode.REPLAY) remain as-is.
- `execute()` is entirely unchanged.
- `async_execute()` is entirely unchanged, including its REPLAY branch which calls `_replay_from_cache()` directly (stateful, correct for a computation path).
- `_compute_and_store()` and `_store_output_stream()` are entirely unchanged.
- The `is_stale` / `clear_cache()` interaction inside `run()` is untouched.

### 3.6 Intentional Semantic Change: Staleness Check

The current `run()` checks `is_stale` and clears the cache if stale. After the fix, `iter_packets()` and `as_table()` read from `_cached_output_stream` without checking `is_stale`. This is **intentional**: iteration is now strictly passive; implicit cache invalidation is a side effect that violates the read-only contract.

Consequence: if an upstream is modified after `run()` was called, the user must call `run()` again to get fresh results. Calling `iter_packets()` or `as_table()` without an intermediate `run()` will silently return old cached data.

**User signal:** Users can call `node.is_stale` before iterating to check whether the cached data may be outdated. A `True` result indicates that an upstream or the source has a newer modification timestamp than the cached stream, and `run()` should be called before iterating.

---

## 4. Data Flow After the Fix

```
# Path A: orchestrator-driven (pipeline.run())
pipeline.run()
  └─ SyncPipelineOrchestrator.execute_nodes() (topological order)
       └─ node.execute(*input_streams, observer=...)
            ├─ get_cached_output() → _replay_from_cache()   [REPLAY mode]
            └─ _operator.process(*input_streams)             [OFF / LOG mode]
                 └─ populates self._cached_output_stream

# Path B: direct user call
node.run()
  └─ _replay_from_cache()    [REPLAY mode, with DB]
  └─ _compute_and_store()    [OFF / LOG mode, with DB]
  └─ _operator.process(...)  [no DB]
       └─ populates self._cached_output_stream

# Read path (this fix): never triggers upstream computation
for tag, packet in operator_node:     # __iter__ → iter_packets()
node.flow()                           # flow() → iter_packets()
node.iter_packets()
node.as_table()
node.as_polars_df() / as_df() / as_lazy_frame() / as_pandas_df()
node._repr_html_() / node.view()
  └─ reads _cached_output_stream (step 1), or DB in REPLAY mode (step 2), or empty (step 3)
```

Before any computation path is invoked, all read methods yield empty results.
After either computation path completes, they yield the computed results.

---

## 5. Error Handling

- **No computation called, no cache:** `iter_packets()` returns an empty iterator; `as_table()` / `_make_empty_table()` returns a zero-row table with correct schema. No exception is raised. This requires `output_schema()` to succeed, which it does for all valid `OperatorNode` constructions (live nodes have `_operator` set; read-only nodes have `_stored_schema`).
- **DB errors:** any database exception from `_load_cached_stream_from_db()` propagates unchanged to the caller.
- **Stale cache:** `iter_packets()` does not check `is_stale`. Users can inspect `node.is_stale` to detect whether cached data is outdated before iterating.
- **`async_iter_packets`:** `OperatorNode` does not override this; it continues to raise `NotImplementedError`. Out of scope for this change.

---

## 6. Testing Plan

### 6.1 Existing tests to update

**`tests/test_core/operators/test_operator_node.py`:**
- `test_iter_packets` — add `node.run()` before `list(node.iter_packets())`
- `test_as_table` — add `node.run()` before `node.as_table()`

**`tests/test_core/operators/test_operator_node_attach_db.py`:**
- `test_iter_packets_without_database` — add `node.run()` before `list(node.iter_packets())`
- `test_iter_packets_with_database` — add `node.run()` before `list(node.iter_packets())`

**`tests/test_core/operators/test_operator_node.py` — `test_replay_from_cache`:**
This test creates a LOG node, calls `node_log.run()` to populate the DB, then creates a REPLAY node and calls `node_replay.as_table()` without calling `run()`. Under the new semantics `as_table()` takes the `_load_cached_stream_from_db()` path (REPLAY mode + DB populated → non-None return) and returns the 3 rows from the DB. This test **passes unchanged** — no modification needed.

### 6.2 New test file

`tests/test_core/operators/test_operator_node_non_active.py`

#### 6.2.1 Core invariant tests

| Test | Description |
|------|-------------|
| `test_iter_packets_without_run_returns_empty` | `SourceNode → OperatorNode`. Iterate without `run()`. Assert zero packets and `_operator.process` never called (spy). |
| `test_as_table_without_run_returns_empty_table` | Same setup. Assert zero-row PyArrow table with correct schema (column names and types). |
| `test_dunder_iter_without_run_returns_empty` | Same setup. Assert `list(node)` returns `[]`. |
| `test_flow_without_run_returns_empty` | Same setup. Assert `node.flow()` returns `[]`. |
| `test_iter_packets_after_run_returns_results` | Call `node.run()` first, then iterate. Assert expected packets returned. |
| `test_as_table_after_run_returns_results` | Call `node.run()` first, then `as_table()`. Assert non-empty table with expected data. |

#### 6.2.2 Cascade isolation tests

| Test | Description |
|------|-------------|
| `test_iter_packets_does_not_cascade_upstream_operator` | Chain: `SourceNode → OperatorNode(A) → OperatorNode(B)`. Call `B.iter_packets()` without `run()` on any node. Assert `A._operator.process` was NOT called (spy on the operator's `process` method — this is the universal computation entry point for all `run()` code paths regardless of DB/CacheMode). Assert B returns empty. This is the key regression test for the bug. Note: spying only on `A._compute_and_store` is insufficient because no-DB nodes call `_operator.process()` directly in `run()` without going through `_compute_and_store`. |
| `test_pipeline_run_then_iterate` | Full `pipeline.run()` then iterate all nodes. Assert correct results and no redundant computation. |

#### 6.2.3 CacheMode tests

| Test | Description |
|------|-------------|
| `test_iter_packets_replay_mode_returns_db_contents` | REPLAY mode: use a prior LOG-mode `node.run()` to populate DB, then create fresh REPLAY node (no in-memory cache). Assert `iter_packets()` without `run()` returns DB records via `_load_cached_stream_from_db`. |
| `test_iter_packets_replay_mode_no_records_returns_empty` | REPLAY mode: DB exists but no records yet. Assert empty. |
| `test_iter_packets_no_db_no_run_returns_empty` | No DB, no `run()`. Assert empty. |
| `test_iter_packets_log_mode_no_run_returns_empty` | LOG mode: first do a LOG-mode `run()` to populate DB records. Confirm records exist at `pipeline_path` (to prove the guard is actually exercised). Create a fresh node with cleared in-memory cache. Assert `iter_packets()` without `run()` returns empty — not the stale DB records. |

---

## 7. Out of Scope

- `FunctionNode` changes — not required. No FunctionNode tests need to change.
- `SourceNode` changes — already correct for "no computation on iteration".
- Changes to orchestrators, `pipeline/graph.py`, or any public API (other than the `flow()` docstring).
- Stripping `_record_hash` from DB-read results — pre-existing behavior; `_replay_from_cache` and `_load_cached_stream_from_db` both return `ArrowTableStream` wrapping raw DB output including `_record_hash`. Normalising this is a separate concern.
- Async (`async_iter_packets`) variants — follow-up work tracked separately.
