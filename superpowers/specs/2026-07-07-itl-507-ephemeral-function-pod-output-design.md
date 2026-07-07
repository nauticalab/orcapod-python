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
- `PersistentFunctionNode` — new `ephemeral_result_store` slot and two-store
  read/write logic
- Pipeline compilation — detect nodes with `ephemeral_result=True` and assign the
  shared `InMemoryArrowDatabase`
- `"temp:"` prefix convention for ephemeral record IDs
- Tests covering all read/write paths and the cross-session miss scenario

Out of scope (v1):
- `FunctionPodStream` — already fully in-memory; no result database to replace
- `OperatorNode` / `PersistentOperatorNode` — different caching model; deferred
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

### 2. `PersistentFunctionNode` — two-store model

`PersistentFunctionNode` gains a new optional slot:

```python
ephemeral_result_store: InMemoryArrowDatabase | None = None
```

This slot is `None` by default and is assigned by the pipeline after compilation — the
same pattern used for `result_database` and `tag_database`. The node never creates its own
store.

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

#### Phase 1 — cache lookup

```
tag table lookup(tag) → tag_entry?
  no  → cache miss → Phase 2
  yes → record_id starts with "temp:"?
          yes → ephemeral_result_store.lookup(record_id_without_prefix)
                  found  → yield (within-session hit)
                  not found → treat as cache miss → Phase 2
          no  → result_database.lookup(record_id)
                  found  → yield (persistent hit)
                  not found → treat as cache miss → Phase 2
```

The "not found in ephemeral store" branch covers the cross-session case: a previous run
wrote a `"temp:"` tag entry, the in-process store has since been cleared or replaced, so
the result is simply recomputed.

#### Phase 2 — compute and write

```python
output_data = self._data_function.call(input_data)

if self.node_config.ephemeral_result and self.ephemeral_result_store is not None:
    record_id = f"temp:{uuid7().hex}"
    self.ephemeral_result_store.add_result_record(record_id, input_data, output_data)
else:
    record_id = uuid7().hex
    self.result_database.add_result_record(record_id, input_data, output_data)

self._add_pipeline_record(tag, output_data, record_id)   # tag table — always persistent
```

If `ephemeral_result=True` but `ephemeral_result_store` is `None` (store not yet assigned
by the pipeline), raise `RuntimeError` at execution time with a clear message rather than
silently falling back to the persistent store.

---

### 3. Pipeline — store creation and injection

The pipeline creates one shared `InMemoryArrowDatabase` instance and injects it into all
nodes whose `NodeConfig.ephemeral_result=True` before execution begins:

```python
# During pipeline compilation / pre-run setup:
ephemeral_store = InMemoryArrowDatabase()
for node in self._nodes:
    if node.node_config.ephemeral_result:
        node.ephemeral_result_store = ephemeral_store
```

Sharing one instance across all ephemeral nodes is correct: each node already scopes its
own table within a shared database via its pipeline-hash-based path, so no additional
namespacing is needed.

**Lifetime decisions** are left to the pipeline:
- Passing a fresh `InMemoryArrowDatabase()` at each `run()` call gives clean
  within-run-only semantics.
- Reusing the same instance across `run()` calls gives cross-run in-memory caching
  (useful when the pipeline is invoked repeatedly in the same Python session).
  Both are valid; the choice is the pipeline's, not the node's.

---

### 4. Supported node configurations

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
└── types.py                  # NodeConfig.ephemeral_result field (new)

src/orcapod/core/
└── nodes/
    └── persistent_function_node.py   # ephemeral_result_store slot + two-store logic

src/orcapod/pipeline/
└── <compilation module>      # store creation and injection into nodes

tests/test_core/function_pod/
└── test_ephemeral_result.py  # NEW — all ephemeral result tests
```

---

## Public API Additions

### `orcapod.types.NodeConfig`

| Field | Type | Default | Meaning |
|---|---|---|---|
| `ephemeral_result` | `bool` | `False` | Route new writes to ephemeral in-memory store |

### `PersistentFunctionNode`

| Attribute | Type | Set by |
|---|---|---|
| `ephemeral_result_store` | `InMemoryArrowDatabase \| None` | Pipeline (post-construction) |

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
