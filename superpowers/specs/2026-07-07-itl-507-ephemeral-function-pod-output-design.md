# Design: Ephemeral Function-Pod Output — v1 (ITL-507)

**Date:** 2026-07-07
**Issue:** [ITL-507](https://linear.app/enigma-metamorphic/issue/ITL-507)
**Status:** Draft

---

## Overview

`PersistentFunctionNode` currently stores every computed result in a persistent result
database (Delta Lake, SQLite, etc.). This is the correct default for reproducible pipelines,
but it is the wrong choice when the output of a computation step is large, transient, or
otherwise undesirable to store permanently — for example, a large preprocessed recording
that is only needed by the immediately downstream step and will be recomputed cheaply on
the next run.

This design introduces an **ephemeral result store** for `PersistentFunctionNode`: when
`NodeConfig.ephemeral_result=True`, new computation results are written to a
pipeline-scoped `InMemoryArrowDatabase` instead of the persistent result database.
Provenance tracking (the tag table) is unaffected — the full tag entry including source
provenance and a `record_id` column is written persistently as before. The `record_id` for
ephemeral entries is prefixed with `"temp:"` so that the lookup path can route to the
correct store.

Existing persisted results remain fully accessible when `ephemeral_result=True` — the two
stores coexist and the lookup order is: ephemeral store first (for `"temp:"` prefixed
record IDs), persistent store second (for unprefixed record IDs).

---

## Goals & Success Criteria

1. `NodeConfig.ephemeral_result=True` routes new result writes to a pipeline-provided
   `InMemoryArrowDatabase` rather than the persistent result database.
2. Tag table entries are written identically to the current behaviour; the only difference
   is that `record_id` is prefixed with `"temp:"` for ephemeral results.
3. Existing persistent results are still served as cache hits when
   `ephemeral_result=True` — the node consults the persistent store for any record ID
   without the `"temp:"` prefix.
4. A `"temp:"` record ID found in the tag table but absent from the (empty or fresh)
   ephemeral store is treated as a cache miss — the computation is rerun and written to
   the current ephemeral store.
5. The ephemeral store is created by the pipeline and injected into nodes, following the
   same pass-in pattern as `result_database`.
6. `ephemeral_result=False` (the default) produces byte-for-byte identical behaviour to
   the current implementation.

---

## Scope & Boundaries

In scope:
- `NodeConfig` — new `ephemeral_result: bool = False` field
- `node_protocols.py` — `set_ephemeral_store(store)` added to `FunctionNodeProtocol`
  and `OperatorNodeProtocol`
- `pipeline_protocols.py` — `set_ephemeral_store(store)` added to `PipelineProtocol`
- `PersistentFunctionNode` — `ephemeral_result_store` slot, `set_ephemeral_store()`
  implementation, and two-store read/write logic
- All other node types — no-op `set_ephemeral_store()` satisfying the protocol
- `Pipeline.set_ephemeral_store(store)` — propagates to all nodes via
  `node.set_ephemeral_store(store)`
- `"temp:"` prefix convention for ephemeral record IDs
- Tests covering all read/write paths and the cross-session miss scenario

Out of scope (v1):
- `FunctionPodStream` — already fully in-memory; no result database to replace
- `OperatorNode` / `PersistentOperatorNode` — `set_ephemeral_store()` is defined (no-op)
  but no caching logic changes; full ephemeral support deferred
- Non-in-memory ephemeral backends (temp file, Redis, cloud object store) — v2+
- Explicit API to clear the ephemeral store mid-run — node replacement or pipeline
  reconstruction achieves this for v1
- `FunctionNode` (non-persistent) — has no result database to reroute

---

## Design

### 1. `NodeConfig`

One new field added to the existing frozen dataclass:

```python
@dataclass(frozen=True, slots=True)
class NodeConfig:
    max_concurrency: int | None = None
    ephemeral_result: bool = False   # NEW
```

`ephemeral_result=True` signals that this node should write new results to the ephemeral
store instead of the persistent result database. It has no effect when the node has no
`result_database` (the node is non-caching and already transient).

---

### 2. Node protocol and interface — `set_ephemeral_store()`

`set_ephemeral_store(store: InMemoryArrowDatabase | None)` is added to both
`FunctionNodeProtocol` and `OperatorNodeProtocol` in `node_protocols.py`.
Passing `None` explicitly removes (detaches) the ephemeral store from the node:

```python
# node_protocols.py — added to FunctionNodeProtocol and OperatorNodeProtocol
def set_ephemeral_store(self, store: InMemoryArrowDatabase | None) -> None:
    """Assign or remove the ephemeral result store for this node.

    Pass an ``InMemoryArrowDatabase`` to attach the store.
    Pass ``None`` to detach it — the node falls back to persistent-only
    behaviour for subsequent writes.
    No-op for node types that do not support ephemeral result storage.
    """
    ...
```

Concrete implementations:

- **`PersistentFunctionNode`** — stores or clears the value:
  ```python
  def set_ephemeral_store(self, store: InMemoryArrowDatabase | None) -> None:
      self.ephemeral_result_store = store
  ```
- **All other node types** (non-persistent function nodes, operator nodes) — implement
  the method as a no-op, satisfying the protocol without any behaviour change.

Adding the method to both protocols — rather than only `FunctionNodeProtocol` — avoids
any conditional branching in the pipeline's `set_ephemeral_store()` loop and keeps
operator nodes protocol-conformant for future versions.

### 3. `PersistentFunctionNode` — two-store model

`PersistentFunctionNode` gains a new optional slot:

```python
ephemeral_result_store: InMemoryArrowDatabase | None = None
```

This slot is `None` by default and is set via `set_ephemeral_store()` by the pipeline
before execution begins. The node never creates its own store.

Both stores can be active simultaneously:

| Store | Type | Lifetime | Receives new writes when |
|---|---|---|---|
| `result_database` | Any `ArrowDatabaseProtocol` | Persistent (across sessions) | `ephemeral_result=False` |
| `ephemeral_result_store` | `InMemoryArrowDatabase` | Pipeline-scoped (in-process) | `ephemeral_result=True` |

#### Record ID routing — the `"temp:"` prefix

The `record_id` column in the tag table acts as a routing key:

- Results written to `ephemeral_result_store` → `record_id = f"temp:{uuid7().hex}"`
- Results written to `result_database` → `record_id = uuid7().hex` (unchanged)

No other tag table columns change.

#### Phase 1 — available-results resolution (bulk, at `iter_data()` start)

At the start of `iter_data()`, before any computation begins, the node determines the
complete set of **currently available computed results** across both stores. This is a
bulk operation, not a per-tag lookup.

**Step 1 — fetch all tag table entries** for this node's pipeline path. This is the same
full scan performed today: retrieve every row in the tag table whose pipeline path matches
this node. Each row contains the input tag hash and its corresponding `record_id`.

**Step 2 — partition by `record_id` prefix:**

```
persistent_entries = {tag_hash: record_id
                      for tag_hash, record_id in tag_rows
                      if not record_id.startswith("temp:")}

ephemeral_entries  = {tag_hash: record_id.removeprefix("temp:")
                      for tag_hash, record_id in tag_rows
                      if record_id.startswith("temp:")}
```

**Step 3 — resolve each group against its store:**

- **Persistent group:** bulk join `persistent_entries` against `result_database` on
  `record_id`. Any `record_id` not found in the result database is dropped from the
  available set and treated as a cache miss, but a **`WARNING`-level log message is
  emitted** for each missing entry — a tag table entry pointing to a non-existent
  persistent record indicates unexpected data loss or external modification of the
  result database and should not pass silently.
- **Ephemeral group:** bulk join `ephemeral_entries` against `ephemeral_result_store` on
  the stripped `record_id`. Any `record_id` not found in the ephemeral store is silently
  dropped — no warning is emitted. This is the expected **cross-session miss** path: a
  prior run wrote a `"temp:"` tag entry, but the in-process store is fresh (or was never
  populated), so the result is simply absent and will be recomputed.

**Step 4 — union** the two resolved sets:

```
available_results = persistent_hits ∪ ephemeral_hits
```

`available_results` is keyed on input tag hash and maps to the fully reconstructed output
`(tag, data)` pair. This is the complete set of results the node can serve without
recomputation.

**Step 5 — determine what still needs computing:**

```
needs_computing = {tag for tag in current_inputs
                   if tag.hash() not in available_results}
```

Results in `available_results` are yielded immediately (cache hits). Inputs in
`needs_computing` proceed to Phase 2.

#### Phase 2 — compute and write

```python
output_data = self._data_function.call(input_data)

if self.node_config.ephemeral_result and self.ephemeral_result_store is not None:
    record_id = f"temp:{uuid7().hex}"
    self.ephemeral_result_store.add_result_record(record_id, input_data, output_data)
else:
    record_id = uuid7().hex
    self.result_database.add_result_record(record_id, input_data, output_data)

# skip_cache_lookup=True — always append the new pipeline record even if a
# stale entry for this entry_id already exists (see "Recompute-after-miss
# write strategy" below).
self._add_pipeline_record(tag, output_data, record_id, skip_cache_lookup=True)
```

If `ephemeral_result=True` but `ephemeral_result_store` is `None` (store not yet assigned
by the pipeline), raise `RuntimeError` at execution time with a clear message rather than
silently falling back to the persistent store.

#### Recompute-after-miss write strategy (v1 — known limitation)

When Phase 2 is triggered by a miss (either a persistent DB miss or a cross-session
ephemeral miss), the tag table may already contain a stale entry for the same
`entry_id` pointing to a `DATA_RECORD_ID` that no longer resolves. If
`add_pipeline_record` skips when an existing entry is found (the current default
behaviour), the new `DATA_RECORD_ID` is never written and the stale entry persists
indefinitely, causing every subsequent run to miss, warn, and recompute — an infinite
miss cycle.

**v1 strategy: always append, deduplication is implicit via the inner join.**

`add_pipeline_record` is called with `skip_cache_lookup=True` on all Phase 2 writes.
This bypasses the skip-if-exists guard and appends a new pipeline DB row alongside
the stale one. The database permits multiple rows per `entry_id`
(`skip_duplicates=False`).

At Phase 1 bulk resolution, the inner join between the tag table and the result store
is the natural deduplication filter:

- Stale rows (whose `DATA_RECORD_ID` is absent from the result store) find no join
  partner and are dropped silently.
- Valid rows (whose `DATA_RECORD_ID` is present) survive and are returned.

**Known race condition (v1 limitation):** under concurrent execution, two threads may
simultaneously detect the same miss and both proceed to Phase 2. Both will append a
new pipeline DB row, resulting in two valid rows for the same logical input. For
deterministic functions the results are semantically identical, but the duplicate rows
are wasteful and could cause double-delivery if not handled by the caller.

A proper concurrency-safe solution — an explicit **recomputation index** baked into
`entry_id` — is deferred to v0.2.0 (ITL-508). The v1 `skip_cache_lookup` approach is
acceptable for single-threaded and low-concurrency pipelines.

**Recovery scenario** — if a previously missing persistent result (R1) is later
restored to the result database while a replacement entry (R2) also exists:

- Both rows survive the join and map to the same logical input.
- For deterministic functions, R1 and R2 are semantically identical; either can be
  used. The implementation picks whichever join partner appears first.
- The recovery scenario is rare and the ambiguity is benign in v1.

**Ephemeral accumulation** — stale `"temp:"` entries accumulate across sessions (each
cross-session miss appends a new row). This is harmless: stale `"temp:"` rows never
match the ephemeral store and are silently dropped at join time. Cleanup is out of
scope for v1.

---

### 4. Pipeline — store creation and injection

`set_ephemeral_store(store: InMemoryArrowDatabase)` is added to `PipelineProtocol` in
`pipeline_protocols.py` and implemented on the concrete `Pipeline` class. It propagates
the store to every node by calling `node.set_ephemeral_store(store)` on each:

```python
# PipelineProtocol (pipeline_protocols.py) — new method signature
def set_ephemeral_store(self, store: InMemoryArrowDatabase | None) -> None: ...

# Pipeline (concrete implementation)
def set_ephemeral_store(self, store: InMemoryArrowDatabase | None) -> None:
    """Assign or remove the ephemeral result store for all nodes in the pipeline.

    Pass an ``InMemoryArrowDatabase`` to attach it to all nodes.
    Pass ``None`` to detach the ephemeral store from all nodes, reverting them
    to persistent-only behaviour for subsequent writes.
    Each node's ``set_ephemeral_store`` is called unconditionally; nodes that
    do not support ephemeral storage (e.g. operator nodes in v1) ignore the call.
    """
    for node in self._nodes.values():
        node.set_ephemeral_store(store)
```

Callers construct the store and hand it in — the pipeline does not create it:

```python
# During pipeline pre-run setup:
ephemeral_store = InMemoryArrowDatabase()
pipeline.set_ephemeral_store(ephemeral_store)
```

Calling `set_ephemeral_store` on every node (not only those with
`node_config.ephemeral_result=True`) is intentional: the method is a no-op for nodes
that don't use it, so the loop stays simple and uniform.

Sharing one `InMemoryArrowDatabase` instance across all ephemeral nodes is correct: each
node already scopes its own table within a shared database via its pipeline-hash-based
path, so no additional namespacing is needed.

**Lifetime decisions** are left to the caller:
- Passing a fresh `InMemoryArrowDatabase()` at each `run()` call gives clean
  within-run-only semantics.
- Reusing the same instance across `run()` calls gives cross-run in-memory caching
  (useful when the pipeline is invoked repeatedly in the same Python session).
  Both are valid; the choice is the caller's, not the pipeline's.

---

### 5. Supported node configurations

| `result_database` | `ephemeral_result` | Behaviour |
|---|---|---|
| `<persistent db>` | `False` | Current behaviour — all writes to persistent DB |
| `<persistent db>` | `True` | Persistent reads reused; new writes to ephemeral store |
| `None` | `True` | All writes to ephemeral store only; no persistent caching |
| `None` | `False` | No caching at all (existing non-caching node behaviour) |

---

## File Layout

```
src/orcapod/
└── types.py                          # NodeConfig.ephemeral_result field (new)

src/orcapod/protocols/
├── node_protocols.py                 # set_ephemeral_store() added to FunctionNodeProtocol
│                                     # and OperatorNodeProtocol
└── pipeline_protocols.py             # set_ephemeral_store() added to PipelineProtocol

src/orcapod/core/
└── nodes/
    ├── persistent_function_node.py   # set_ephemeral_store() override + ephemeral_result_store
    │                                 # slot + two-store read/write logic
    └── <other node types>            # no-op set_ephemeral_store() satisfying the protocol

src/orcapod/pipeline/
└── <pipeline module>                 # Pipeline.set_ephemeral_store() — propagates to all nodes

tests/test_core/function_pod/
└── test_ephemeral_result.py          # NEW — all ephemeral result tests
```

---

## Public API Additions

### `orcapod.types.NodeConfig`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `ephemeral_result` | `bool` | `False` | Route new writes to ephemeral in-memory store |

### `FunctionNodeProtocol` and `OperatorNodeProtocol` (`node_protocols.py`)

| Method | Signature | Behaviour |
|---|---|---|
| `set_ephemeral_store` | `(store: InMemoryArrowDatabase \| None) -> None` | Protocol-level declaration. Pass `None` to detach. Concrete implementations: `PersistentFunctionNode` stores or clears the value; all other node types no-op. |

### `PipelineProtocol` (`pipeline_protocols.py`)

| Method | Signature | Behaviour |
|---|---|---|
| `set_ephemeral_store` | `(store: InMemoryArrowDatabase \| None) -> None` | Protocol-level declaration. Pass `None` to detach from all nodes. Implemented on `Pipeline` — calls `node.set_ephemeral_store(store)` for every node. |

### `PersistentFunctionNode`

| Attribute | Type | Set by |
|---|---|---|
| `ephemeral_result_store` | `InMemoryArrowDatabase \| None` | `set_ephemeral_store()` called by pipeline |

| Method | Signature | Behaviour |
|---|---|---|
| `set_ephemeral_store` | `(store: InMemoryArrowDatabase \| None) -> None` | Assigns `self.ephemeral_result_store = store`. Pass `None` to detach. |

### `Pipeline`

| Method | Signature | Behaviour |
|---|---|---|
| `set_ephemeral_store` | `(store: InMemoryArrowDatabase) -> None` | Calls `node.set_ephemeral_store(store)` on every node in the pipeline |

---

## Testing Plan

All tests in `tests/test_core/function_pod/test_ephemeral_result.py`:

| Test | What it covers |
|---|---|
| `test_ephemeral_result_written_to_memory_not_persistent_db` | With `ephemeral_result=True`, persistent DB remains empty; in-memory store receives the result row |
| `test_ephemeral_record_id_has_temp_prefix` | Tag table entry for an ephemeral result has `record_id` starting with `"temp:"` |
| `test_persistent_hit_still_served_when_ephemeral_true` | A result pre-written to the persistent DB is returned as a cache hit even when `ephemeral_result=True` |
| `test_within_session_ephemeral_hit` | Same node, second call with identical input: result is served from in-memory store (no recomputation) |
| `test_cross_session_miss_recomputes` | Fresh `InMemoryArrowDatabase` (simulating a new session): `"temp:"` tag entry treated as cache miss, function is called again |
| `test_ephemeral_false_unchanged` | `ephemeral_result=False` is functionally identical to the existing implementation |
| `test_ephemeral_only_node` | `result_database=None, ephemeral_result=True`: end-to-end execution succeeds; results retrievable within session |
| `test_store_not_assigned_raises` | `ephemeral_result=True` but `ephemeral_result_store=None` at execution time raises `RuntimeError` with clear message |
| `test_pipeline_injects_shared_store` | Pipeline assigns the same `InMemoryArrowDatabase` instance to all ephemeral nodes |
| `test_bulk_resolution_mixed_stores` | Tag table contains both `"temp:"` and regular entries; bulk resolution yields hits from both stores and only recomputes entries absent from both |
| `test_bulk_resolution_ephemeral_miss_dropped_from_available` | Tag table has a `"temp:"` entry but ephemeral store is fresh; that entry is excluded from available results and the input falls into Phase 2 |
| `test_bulk_resolution_persistent_miss_warns_and_recomputes` | Tag table has a regular `record_id` entry but persistent DB has been trimmed; a `WARNING`-level log is emitted, the entry is excluded from available results, and the input falls into Phase 2 |
| `test_recompute_after_miss_appends_new_pipeline_record` | After a persistent miss triggers recomputation, the tag table contains two rows for the same entry_id — the stale one and the new valid one; subsequent lookup resolves correctly via the inner join without recomputing again |
| `test_recompute_after_ephemeral_miss_no_infinite_cycle` | Cross-session ephemeral miss triggers recomputation; the new `"temp:"` entry is appended and the result is served on the next call without triggering Phase 2 again |

---

## Out of Scope / Deferred

- **Non-in-memory ephemeral backends** (temp SQLite file, Redis, object store): the
  `InMemoryArrowDatabase` is the only supported ephemeral backend in v1. Future versions
  will introduce an `EphemeralStoreProtocol` and additional implementations.
- **`FunctionPodStream` ephemeral annotation**: `FunctionPodStream` has no result database;
  it is already fully transient. Any future design around provenance-skipping for
  `FunctionPodStream` outputs is a separate issue.
- **Operator ephemeral caching**: operators use a different three-tier caching model
  (off / log / replay); ephemeral support for operators is deferred.
- **Store clear / reset API**: no explicit method to clear `ephemeral_result_store` in v1.
  Callers control lifetime by passing a fresh store at the pipeline level.
- **Indexed recomputation (`entry_id` versioning, ITL-508 / v0.2.0)**: the v1 append
  strategy for recompute-after-miss has a known race window under concurrent execution.
  ITL-508 will incorporate an explicit recomputation index into `entry_id` (e.g.
  `hash(... + recomputation_index=N)`). Index 0 is the original computation; a miss
  increments to index 1. Insert uses an atomic insert-if-not-exists so that concurrent
  threads all attempting index 1 result in exactly one successful write. This eliminates
  both the race condition and unbounded stale-row accumulation.
