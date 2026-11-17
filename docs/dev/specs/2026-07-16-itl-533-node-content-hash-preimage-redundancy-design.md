# ITL-533: Remove NODE_CONTENT_HASH_COL from record_id preimage

**Date:** 2026-07-16 (updated 2026-07-20 after ITL-534/532/535 merge)
**Linear issue:** ITL-533
**Scope:** `table_scope="pipeline_hash"` (the default) only

---

## Overview

`FunctionJobNode` and `SideEffectPod` compute a `record_id` (the DB primary key) by
hashing an Arrow preimage table. The preimage is currently:

**`base_entry_id`** (`__pipeline_base_entry_id`) — the recomputation-stable identifier:
```
system_tag columns          (tag.as_table(columns={"system_tags": True}))
__input_data_hash           (input_data.content_hash(), large_string)
__node_content_hash         (self.content_hash(), large_string)         ← redundant
```

**`record_id`** (`__record_id`, DB primary key) — extends the base:
```
system_tag columns
__input_data_hash
__node_content_hash                                                      ← redundant
__pipeline_recomputation_index   (int32)
```

**Side-effect tdb** (`_execute_side_effect_row`) — mirrors the pdb shape at index 0:
```
system_tag columns
__input_data_hash
__node_content_hash                                                      ← redundant
__pipeline_recomputation_index   = 0  (fixed; side effects never recompute)
```

`__node_content_hash` is `self.content_hash()` for the node — a hash of the function
identity and the upstream data content. This column is redundant because those two
pieces of information are already present elsewhere in every stored record:

* **Table path** — the pipeline DB table is stored at `node_identity_path`, which is
  scoped by `pipeline_hash`. `pipeline_hash` encodes the function identity and the
  upstream topology/schema. It already captures the same function-version information
  that `NODE_CONTENT_HASH_COL` carries.

* **System tags** — each record stores one or more `_tag_source_id::*` columns whose
  values default to `table_hash` (content-addressed). These already encode the upstream
  data content — the same information `NODE_CONTENT_HASH_COL` encodes via
  `upstream_content_hash`.

Since `node_content_hash = hash(pod_content_hash, upstream_content_hash)` and both
components are fully determined by `(table_path, system_tags)`, `NODE_CONTENT_HASH_COL`
can be derived from information already in the record. Storing it separately is
redundant.

After this change, the preimages become:

**`base_entry_id`** (new):
```
system_tag columns
__input_data_hash           (input_data.content_hash(), large_binary)
```

**`record_id`** (new):
```
system_tag columns
__input_data_hash           (large_binary)
__pipeline_recomputation_index   (int32)
```

This spec defines the changes required to remove `NODE_CONTENT_HASH_COL` from (a) the
hash preimage and (b) the `_filter_by_content_hash()` filtering mechanism that depended
on it.

**Note (post-implementation update):** the column is **retained** in stored pdb_v1 rows
for per-node isolation — `_fetch_joined_records()` filters rows by
`NODE_CONTENT_HASH_COL` when `table_scope='pipeline_hash'` to prevent cross-node
contamination in shared pipeline DB tables.  What changed is only its role in the
`record_id` preimage (removed) and its column-name prefix (from `DATAGRAM_PREFIX` `_`
to `SYSTEM_COLUMN_PREFIX` `__`, so `_node_content_hash` → `__node_content_hash`).
The pdb v0→v1 migration handles the rename and type conversion accordingly.

All schema changes in this project ship together in the upcoming v0.1.0 minor release.
The three DB types each get a single v0 → v1 migration step. ITL-533's pdb changes are
folded into **pdb v1** (not a new v2). The definitive pdb_v1 column layout retains
`__node_content_hash` (binary) for per-node isolation but excludes it from the record_id
preimage. ITL-533 also introduces **tdb v1**, the first formally versioned schema for the
side-effect tracking DB.

---

## Goals & Success Criteria

* `_build_entry_id_preimage()` returns only `system_tags + INPUT_DATA_HASH_COL`.
* `_execute_side_effect_row()` builds the same preimage shape (plus
  `__pipeline_recomputation_index`); the `node_content_hash_str` parameter is removed.
* `add_pipeline_record()` writes `__node_content_hash` to the DB for per-node isolation
  but does NOT include it in the `record_id` preimage.
* `_filter_by_content_hash()` is deleted; `_fetch_joined_records()` performs per-node
  isolation inline via Polars filter on `NODE_CONTENT_HASH_COL` when
  `table_scope='pipeline_hash'`.
* `get_all_records()` always drops `NODE_CONTENT_HASH_COL` from the user-facing table
  (it is an internal discriminator column, not user data).
* A shared private helper `_build_record_id_preimage(tag, data) -> pa.Table` unifies
  the preimage construction for `FunctionJobNode` and `SideEffectPod`.
* `NODE_CONTENT_HASH_COL` uses `SYSTEM_COLUMN_PREFIX` (`__`), giving column name
  `__node_content_hash`; v0→v1 migration renames `_node_content_hash` → `__node_content_hash`
  and converts the string value to binary.
* `PIPELINE_DB_SCHEMA_VERSION` in `system_constants.py` remains `"pdb_v1"`; pdb writes
  continue to go to `node_identity_path + ("pdb_v1",)`. The v0→v1 migration is extended
  to rename+convert `__node_content_hash` and recompute the preimage-based hashes.
* `TRACKING_DB_SCHEMA_VERSION = "tdb_v1"` added to `system_constants.py`; tracking DB
  writes go to `table_path + ("tdb_v1",)`. Presence of a tdb_v0 table at the old path
  is silently ignored (old log entries become orphaned; side effects re-deliver once).
* No tdb migration utility — tdb_v0 entries cannot be remapped (record_id preimage
  components are not stored in the log); orphaning is the intended behaviour.
* All existing tests pass; `test_node_content_hash_col_not_in_preimage_keys` passes.
* No backward-compatibility shims.

---

## Correctness Argument

### Why `NODE_CONTENT_HASH_COL` is redundant in the preimage

The `record_id` preimage is hashed to produce a unique identifier for each pipeline
record. The current preimage is:

```
preimage = system_tags + INPUT_DATA_HASH_COL + NODE_CONTENT_HASH_COL
```

**Structural argument.** `node_content_hash = hash(pod_content_hash, upstream_content_hash)`.

* `pod_content_hash` is already captured by the DB table path: every `FunctionJobNode`
  stores its records under `node_identity_path`, which is scoped by `pipeline_hash`.
  `pipeline_hash` is a hash of the function identity and upstream topology/schema —
  exactly what `pod_content_hash` encodes. Any two nodes that share the same
  `pipeline_hash` (and therefore the same DB table) also share the same
  `pod_content_hash`.

* `upstream_content_hash` is already captured by the system tags: each record stores
  `_tag_source_id::*` columns whose values default to `table_hash` — a content-addressed
  fingerprint of the upstream data. `ArrowTableSource.identity_structure()` =
  `(class_name, schema, source_id)`, so two sources with the same `source_id` have
  identical content hashes and produce identical `upstream_content_hash`.

Since `node_content_hash` is fully determined by `(pipeline_hash_in_table_path,
source_id_in_system_tags)`, and both of those are already encoded in every stored record,
`NODE_CONTENT_HASH_COL` carries no new information. This leads directly to the lockstep
property below.

**Lockstep property.** There is no scenario where two records in the same pipeline DB
table have the same `(system_tags, INPUT_DATA_HASH_COL)` but different
`NODE_CONTENT_HASH_COL`:

* Same table path → same `pipeline_hash` → same `pod_content_hash`.
* Same `system_tags` → same `source_id` → same `upstream_content_hash`.
* Therefore same `node_content_hash` → same `NODE_CONTENT_HASH_COL`.

Equivalently, different `NODE_CONTENT_HASH_COL` values can only arise from different
`system_tags`, which already produce a different `base_entry_id`:

> `NODE_CONTENT_HASH_COL` and `base_entry_id` are always in lockstep.
> One cannot differ without the other differing first.

Verified empirically in
`tests/test_core/function_pod/test_node_content_hash_redundancy.py`.

### Why `_filter_by_content_hash()` is a no-op

`_filter_by_content_hash()` filters the pipeline DB table to rows where
`NODE_CONTENT_HASH_COL = self.content_hash()`. Its purpose was to prevent one
`FunctionJobNode` from reading records written by a different node that happens to share
the same `pipeline_hash` (and therefore the same DB table).

This concern is already fully addressed by the `base_entry_id` filter in
`get_cached_results(base_entry_ids=[...])`: only records whose `base_entry_id` matches
the current node's inputs are returned. Since `base_entry_id` is derived from
`system_tags` (which includes the content-addressed `source_id`), records from a
different upstream source always have different `base_entry_ids` and are naturally
excluded.

Verified empirically: patching `_filter_by_content_hash()` to be a pass-through
produces identical call counts and output values in all scenarios.

---

## Schema Versioning

### Framework (established by ITL-535)

Schema versions are encoded as the last component of the storage path tuple:

```
pipeline DB:  node_identity_path + ("pdb_v1",)  ← current target
result DB:    pod_record_path    + ("rdb_v1",)  ← current target (ITL-535)
tracking DB:  table_path        + ("tdb_v1",)  ← introduced by ITL-533
```

All three DB types get their first and only v1 schema as part of the upcoming v0.1.0
minor release. A single migration step handles each type (v0 → v1). Future schema
changes will target v2 only in subsequent minor releases.

### pdb v1 — path: `node_identity_path + ("pdb_v1",)`

ITL-535 introduced pdb_v1 and defined its schema. ITL-533 refines that definition
before release: `__node_content_hash` is removed, and `__pipeline_base_entry_id` /
`__record_id` are recomputed using the smaller preimage. The v0→v1 migration
(implemented in ITL-535) is extended to also apply these transformations.

| Column | pdb v0 | pdb v1 |
|---|---|---|
| system_tag columns | `large_string` | `large_string` (unchanged) |
| source columns | as written | unchanged |
| `__data_id` | `large_binary(16)` | unchanged |
| `__node_content_hash` | `large_string` | **removed** |
| `__input_data_hash` | absent / `large_string` | `large_binary` |
| `__output_data_hash` | absent / `large_string` | `large_binary` (nullable) |
| `__input_data_context_key` | `large_string` | unchanged |
| `__computed` | `bool` | unchanged |
| `__is_ephemeral` | `bool` | unchanged |
| `__pipeline_base_entry_id` | `large_string` | `large_binary` (recomputed without `NODE_CONTENT_HASH_COL`) |
| `__pipeline_recomputation_index` | `int32` | unchanged |
| `__record_id` (DB key) | `large_string` | `large_binary` (recomputed without `NODE_CONTENT_HASH_COL`) |

### tdb v1 — path: `table_path + ("tdb_v1",)`

The side-effect invocation log (written by `_write_invocation_row`) gains formal
schema versioning. The stored columns are identical to tdb_v0:

| Column | Type | Notes |
|---|---|---|
| `record_id_hash` | `large_string` | Human-readable hash string for the completed invocation |
| `pipeline_run_id` | `large_string` | Run identifier (nullable) |
| `executed_at` | `timestamp(us, UTC)` | Delivery timestamp |

The only change is the hash used to compute the primary key (`record_id`): in tdb_v0 it
included `NODE_CONTENT_HASH_COL` in the preimage; in tdb_v1 it does not.

`TRACKING_DB_SCHEMA_VERSION = "tdb_v1"` is added to `system_constants.py`.
`SideEffectJobNode` uses this suffix when reading/writing. Presence of a tdb_v0 table
at the old path does **not** raise `SchemaVersionError` — old entries are simply
orphaned under the old path (new code uses the new path) and side effects re-deliver
once on next run.

---

## Architecture

### Shared preimage helper

A new private free function `_build_record_id_preimage(tag, data) -> pa.Table` is
extracted. It returns a single-row Arrow table with columns:

```
[system_tag columns..., INPUT_DATA_HASH_COL]
```

Used by both `FunctionJobNode._build_entry_id_preimage()` and `_execute_side_effect_row()`.

**Placement:** Module-level private function in `function_node.py`, importable by
`side_effects.py`.

### Changes per file

#### `src/orcapod/system_constants.py`

| Change | Detail |
|---|---|
| `PIPELINE_DB_SCHEMA_VERSION` | No change — remains `"pdb_v1"` |
| `TRACKING_DB_SCHEMA_VERSION` | Add new constant `"tdb_v1"` |

#### `src/orcapod/core/nodes/function_node.py`

| Location | Change |
|---|---|
| `_build_entry_id_preimage()` | Delegate to `_build_record_id_preimage(tag, data)` |
| `add_pipeline_record()` | Remove `__node_content_hash` column from written row |
| `_filter_by_content_hash()` | Delete method entirely |
| `_fetch_joined_records()` | Remove `_filter_by_content_hash()` call |
| `get_all_records()` | Remove `__node_content_hash` from always-drop list |
| `get_cached_results()` | Remove `__node_content_hash` exclusion |
| Docstrings | Update all references to `__node_content_hash` in preimage descriptions |

#### `src/orcapod/side_effects.py`

| Location | Change |
|---|---|
| `_execute_side_effect_row()` | Delegate preimage to `_build_record_id_preimage(tag, data)` + append recomputation index; remove `node_content_hash_str` parameter |
| `SideEffectPodStream.iter_data()` (line 248) | Remove `node_content_hash_str=...` |
| `SideEffectNode.iter_data()` (line 828) | Remove `node_content_hash_str=...` |
| `SideEffectJobNode.execute()` (line 966) | Remove `node_content_hash_str=...` |
| Async path (line 1026) | Remove `node_content_hash_str=...` |
| `SideEffectJobNode.attach_pipeline_database()` | Append `TRACKING_DB_SCHEMA_VERSION` suffix to `_table_path` |
| `_write_invocation_row()` | No column changes; path change is upstream |

#### `src/orcapod/migrations/pipeline_db.py`

Extend the existing `migrate_pipeline_v0_to_v1()` to also drop `__node_content_hash`
from the output and recompute `__pipeline_base_entry_id` and `__record_id` using the
new (smaller) preimage. No new migration functions are added.

#### `src/orcapod/migrations/` — no new files

No `migrate_pipeline_v1_to_v2()` and no new CLI command. The single v0→v1 migration
handles everything.

---

### Preimage shape before and after

| Column | pdb v0 / tdb v0 | pdb v1 / tdb v1 |
|---|---|---|
| `system_tag columns` | ✓ | ✓ |
| `INPUT_DATA_HASH_COL` | ✓ | ✓ |
| `NODE_CONTENT_HASH_COL` | ✓ | **removed** |
| `__pipeline_recomputation_index` | ✓ (pipeline_entry_id only) | ✓ (unchanged) |

---

## Migration

### pdb v0 → v1 (extended)

The existing `migrate_pipeline_v0_to_v1()` (introduced by ITL-535) is extended to
additionally:

1. Drop `__node_content_hash` from the output row entirely.
2. Recompute `__pipeline_base_entry_id` and `__record_id` using the new preimage
   (`system_tags + INPUT_DATA_HASH_COL` only, without `NODE_CONTENT_HASH_COL`).

**Why recomputation is feasible.** pdb_v0 stores system-tag columns and either
`__input_data_hash` directly or a fallback via the rdb (already handled by the existing
migration). The `__pipeline_recomputation_index` is also stored. All inputs to the new
preimage are therefore recoverable from data the migration already reads.

**Per-row transformation (updated):**

1. Convert `__node_content_hash`, `__input_data_hash`, `__output_data_hash` from
   `large_string` → `large_binary` (existing behavior, per ITL-535).
2. Backfill `__input_data_hash` from rdb when absent in v0 (existing behavior).
3. Extract system-tag columns and `__input_data_hash` (now guaranteed non-null or
   counted as unresolvable).
4. Reconstruct new preimage: `{system_tag_col: value, ..., INPUT_DATA_HASH_COL: value}`.
5. Compute `new_base_entry_id = arrow_hasher.hash_table(preimage).to_prefixed_digest()`.
6. Compute `new_record_id = arrow_hasher.hash_table(preimage_with_recomputation_index).to_prefixed_digest()`.
7. Replace `__pipeline_base_entry_id` with `new_base_entry_id`.
8. **Drop `__node_content_hash` column** from the output (not written to v1).
9. Write to `pipeline_path + ("pdb_v1",)`.

**Idempotency and skipped-row detection:** unchanged from ITL-535 implementation.

**Rows where `__input_data_hash` cannot be recovered** (pdb absent, rdb absent) remain
`rows_unresolvable`, written with `null` for all recomputed hash columns.

### tdb — no migration

The tdb invocation log stores only `record_id_hash`, `pipeline_run_id`, `executed_at`.
It does **not** store system_tags or `__input_data_hash`. The old `record_id` cannot be
recomputed under the new scheme.

**Strategy: orphan, don't migrate.** New code writes to `table_path + ("tdb_v1",)`.
Old log entries at the bare `table_path` are abandoned. No `SchemaVersionError` is
raised. On the next run, `track_completion` lookups use the new path → all previous
completions are not found → side effects re-deliver once. This is the minimum possible
migration cost.

---

## Scope & Boundaries

In scope:
* Remove `NODE_CONTENT_HASH_COL` from the `record_id` preimage in `FunctionJobNode`
* Remove `NODE_CONTENT_HASH_COL` from the `record_id` preimage in `SideEffectPod`
* Remove `NODE_CONTENT_HASH_COL` as a stored column in `add_pipeline_record()`
* Delete `_filter_by_content_hash()` and its single call site
* Extract shared `_build_record_id_preimage()` helper
* Update `get_all_records()` and `get_cached_results()` to stop referencing
  `NODE_CONTENT_HASH_COL`
* Update `system_constants.py`: add `TRACKING_DB_SCHEMA_VERSION = "tdb_v1"`
  (`PIPELINE_DB_SCHEMA_VERSION` stays `"pdb_v1"` — no change)
* Update `SideEffectJobNode` to use `TRACKING_DB_SCHEMA_VERSION` suffix in table path
* Extend `migrate_pipeline_v0_to_v1()` to drop `__node_content_hash` and recompute
  preimage-based hashes in the v1 output
* Update all affected docstrings

Out of scope:
* `table_scope="content_hash"` (non-default mode) — separate investigation needed
* Removing `NODE_CONTENT_HASH_COL` constant from `system_constants.py` (still needed
  in migration code for reading v0 rows)
* tdb migration utility (not feasible; orphaning is intentional)
* rdb changes (ITL-533 does not affect result DB schema)
* Any new migration CLI command (existing `orcapod migrate pipeline-db` is sufficient)

---

## Testing

### Existing tests (must all pass)

* `tests/test_core/function_pod/test_pipeline_hash_integration.py` — all tests,
  especially `test_shared_db_overlapping_inputs_avoids_recomputation` and
  `test_shared_db_all_inputs_pre_computed_zero_recomputation`
* All other `test_core/function_pod/` tests
* All `test_core/side_effect_pod/` and `test_core/side_effect_function/` tests
* `tests/test_migrations/test_pipeline_db.py` — existing v0→v1 tests updated to
  reflect the extended transformation

### Investigation artefact (forward-looking)

`tests/test_core/function_pod/test_node_content_hash_redundancy.py`:

* `TestPreimageShape.test_node_content_hash_col_not_in_preimage_keys` — currently
  **fails** (intentionally), will **pass** after the change.
* All other tests in that file already pass and must continue to pass.

### Extended migration tests

`tests/test_migrations/test_pipeline_db.py` (extended):

* `test_migrate_v0_to_v1_drops_node_content_hash` — v0 rows migrated to v1 must not
  contain `__node_content_hash`.
* `test_migrate_v0_to_v1_recomputes_base_entry_id` — `__pipeline_base_entry_id` in v1
  matches a fresh computation using only `(system_tags, INPUT_DATA_HASH_COL)`.
* `test_migrate_v0_to_v1_recomputes_record_id` — `__record_id` in v1 matches a fresh
  computation using only `(system_tags, INPUT_DATA_HASH_COL, recomputation_index)`.
* `test_migrate_v0_to_v1_extended_idempotent` — calling the extended migration twice
  produces identical v1 rows.
* `test_tdb_v1_path_used` — `SideEffectJobNode._table_path` ends with `"tdb_v1"`.
* `test_tdb_v0_entries_orphaned` — old tdb_v0 entries at bare path are not visible
  to the new code (no error, no lookup).

---

## Dependencies & Risks

* **Atomic deployment.** The `_filter_by_content_hash()` deletion and the
  `__node_content_hash` removal from `add_pipeline_record()` must ship together — the
  filter currently raises `ValueError` when the column is absent, so they cannot be
  split.
* **pdb_v1 redefinition.** Between ITL-535 merge and this change, any pdb_v1 rows
  written to disk would contain `__node_content_hash`. After ITL-533, the pdb_v1
  schema no longer includes that column. Since this is pre-v0.1.0, no compatibility
  handling is required — old rows will simply have an extra orphaned column that the
  new code ignores. Users who have run the v0→v1 migration before this change should
  re-run it to get correctly recomputed hash values.
* **tdb re-delivery.** Users with `track_completion=True` will see each side effect
  re-deliver once after deployment (old tdb_v0 entries are not read from the new path).
  This is intentional and should be documented in the release notes.
* **`NODE_CONTENT_HASH_COL` constant stays** — still needed in `migrate_pipeline_v0_to_v1()`
  to identify and drop the column from v0 input rows.
