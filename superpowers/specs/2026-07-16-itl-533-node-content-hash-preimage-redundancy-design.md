# ITL-533: Remove NODE_CONTENT_HASH_COL from record_id preimage

**Date:** 2026-07-16 (updated 2026-07-17 after ITL-534/ITL-532 merge)
**Linear issue:** ITL-533
**Scope:** `table_scope="pipeline_hash"` (the default) only

---

## Overview

`FunctionJobNode` and `SideEffectPod` compute a `record_id` by hashing a preimage
table. The preimage currently contains `NODE_CONTENT_HASH_COL` (`_node_content_hash`),
which is `self.content_hash()` for the node — a hash of the function identity and the
upstream data content. This column is redundant because those two pieces of information
are already present elsewhere in every stored record:

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

This spec defines the changes required to remove `NODE_CONTENT_HASH_COL` from (a) the
preimage, (b) the stored DB row, and (c) the `_filter_by_content_hash()` filtering
mechanism that depended on it, plus (d) a migration utility to update existing pipeline
DB tables to use the new hash scheme.

---

## Goals & Success Criteria

* `_build_entry_id_preimage()` returns only `system_tags + INPUT_DATA_HASH_COL`.
* `_execute_side_effect_row()` builds the same preimage shape (plus
  `__pipeline_recomputation_index`); the `node_content_hash_str` parameter is removed.
* `add_pipeline_record()` no longer writes `NODE_CONTENT_HASH_COL` to the DB.
* `_filter_by_content_hash()` is deleted; its single call site in
  `_fetch_joined_records()` is removed.
* `get_all_records()` no longer needs to strip `NODE_CONTENT_HASH_COL` from results.
* A shared private helper `_build_record_id_preimage(tag, data) -> pa.Table` unifies
  the preimage construction for `FunctionJobNode` and `SideEffectPod`.
* A `migrate_pipeline_records(node)` utility rewrites existing `FunctionJobNode` pipeline
  DB tables to use the new hash scheme (idempotent; skips records already migrated or
  lacking `INPUT_DATA_HASH_COL`).
* A `migrate_side_effect_records(pod_node)` utility drops existing side-effect invocation
  log entries (which cannot be recomputed from stored data) so side effects re-execute
  once after migration.
* All existing tests pass; `test_node_content_hash_col_not_in_preimage_keys` (currently
  an intentional fail) passes after the change.
* No backward-compatibility shims are added (pre-v0.1.0 project).

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

This concern is already fully addressed by the subsequent `base_entry_id` filter in
`get_cached_results(base_entry_ids=[...])`: only records whose `base_entry_id` matches
the current node's inputs are returned. Since `base_entry_id` is derived from
`system_tags` (which includes the content-addressed `source_id`), records from a
different upstream source always have different `base_entry_ids` and are naturally
excluded.

Verified empirically: patching `_filter_by_content_hash()` to be a pass-through
produces identical call counts and output values in all scenarios.

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
`side_effects.py`. This avoids coupling `SideEffectPod` to a `FunctionNode` class and
keeps the implementation as a plain function (no `self` needed).

### Changes per file

#### `src/orcapod/core/nodes/function_node.py`

| Location | Change |
|---|---|
| `_build_entry_id_preimage()` | Replace inline construction with call to `_build_record_id_preimage(tag, data)` |
| `add_pipeline_record()` | Remove `NODE_CONTENT_HASH_COL` column from the row written to DB |
| `_filter_by_content_hash()` | Delete method entirely |
| `_fetch_joined_records()` | Remove the `_filter_by_content_hash()` call (line 1860) |
| `get_all_records()` | Remove `NODE_CONTENT_HASH_COL` from the always-drop column list |
| `get_cached_results()` | Remove `NODE_CONTENT_HASH_COL` exclusion (line 2065, 2077) |
| `add_pipeline_record()` migration guard | Extend the ITL-508 guard to also warn if `NODE_CONTENT_HASH_COL` is present (unmigrated table) |
| Docstrings | Update all references to `NODE_CONTENT_HASH_COL` in preimage descriptions |
| New: `migrate_pipeline_records()` | Add migration method (see Migration section below) |

#### `src/orcapod/side_effects.py`

| Location | Change |
|---|---|
| `_execute_side_effect_row()` | Replace inline preimage construction with `_build_record_id_preimage(tag, data)` + append `__pipeline_recomputation_index`; remove `node_content_hash_str` parameter |
| Call site: `SideEffectPodStream.iter_data()` (line 248) | Remove `node_content_hash_str=...` argument |
| Call site: `SideEffectNode.iter_data()` (line 828) | Remove `node_content_hash_str=...` argument |
| Call site: `SideEffectJobNode.iter_data()` (line 966) | Remove `node_content_hash_str=...` argument |
| Call site: async path (line 1026) | Remove `node_content_hash_str=...` argument |
| New: `migrate_side_effect_records()` | Add migration function (see Migration section below) |

#### `src/orcapod/system_constants.py`

`NODE_CONTENT_HASH_COL` itself stays defined — other code outside this scope may
reference it, and removing a constant is a separate, lower-risk cleanup. No changes to
this file in this PR.

---

### Preimage shape before and after

| Column | Before | After |
|---|---|---|
| `system_tag columns` | ✓ | ✓ |
| `INPUT_DATA_HASH_COL` | ✓ | ✓ |
| `NODE_CONTENT_HASH_COL` | ✓ | **removed** |
| `__pipeline_recomputation_index` | ✓ (pipeline_entry_id only) | ✓ (unchanged) |

### DB row shape before and after (`add_pipeline_record`)

| Column | Before | After |
|---|---|---|
| system_tag columns | ✓ | ✓ |
| source columns | ✓ | ✓ |
| `DATA_RECORD_ID` | ✓ | ✓ |
| `NODE_CONTENT_HASH_COL` | ✓ | **removed** |
| `INPUT_DATA_HASH_COL` | ✓ (added ITL-534) | ✓ |
| `OUTPUT_DATA_HASH_COL` | ✓ (added ITL-534) | ✓ |
| `__input_data_context_key` | ✓ | ✓ |
| `__computed` | ✓ | ✓ |
| `IS_EPHEMERAL_COL` | ✓ | ✓ |
| `_PIPELINE_BASE_ENTRY_ID_COL` | ✓ | ✓ (recomputed by migration) |
| `_PIPELINE_RECOMPUTATION_INDEX_COL` | ✓ | ✓ |

### Impact on existing DB records

Removing `NODE_CONTENT_HASH_COL` from the preimage changes the hash function. Pipeline
DB records written before this change have stale `base_entry_id` and `pipeline_entry_id`
values (both include `NODE_CONTENT_HASH_COL` in their preimage). Those stale records
will never match the new `compute_base_entry_id()` results and will silently become dead
weight — the code remains correct, but prior computations are not reused.

The migration utility (below) restores reuse by rewriting those records with updated
hashes.

---

## Migration

### Why migration is now feasible

ITL-534 (merged just before this PR) added `INPUT_DATA_HASH_COL` as a stored column in
`add_pipeline_record()`. Every record written by the current codebase therefore contains
both the `system_tag` columns and `INPUT_DATA_HASH_COL`. Together, these are precisely
the columns needed to recompute the new preimage — so migration can be done entirely from
stored data, without access to the original input data.

### FunctionJobNode pipeline DB migration

**Function signature:**

```python
def migrate_pipeline_records(
    node: FunctionJobNode,
) -> MigrationReport:
    ...
```

A `MigrationReport` dataclass carries:

```python
@dataclasses.dataclass
class MigrationReport:
    migrated: int      # records successfully rewritten
    skipped: int       # records lacking INPUT_DATA_HASH_COL (pre-ITL-534; cannot migrate)
    already_done: int  # records already lacking NODE_CONTENT_HASH_COL (idempotency)
```

**Algorithm (per node):**

1. Read all existing records from `node.node_identity_path` via
   `node._pipeline_database.get_all_records(...)`.
2. If the table is `None` or empty, return early (`MigrationReport(0, 0, 0)`).
3. Check for `NODE_CONTENT_HASH_COL` in the table schema:
   - **Absent** → already migrated. Return `MigrationReport(0, 0, num_rows)`.
4. For each row:
   a. If `INPUT_DATA_HASH_COL` is absent or null → count as `skipped` and retain the
      row unchanged (it was written before ITL-534 and cannot be migrated).
   b. Otherwise:
      * Reconstruct the new preimage: a single-row table of
        `{system_tag_col: value, ..., INPUT_DATA_HASH_COL: value}`.
      * Compute `new_base_entry_id = arrow_hasher.hash_table(preimage).to_prefixed_digest()`.
      * Compute `new_pipeline_entry_id = arrow_hasher.hash_table(preimage + recomputation_index).to_prefixed_digest()`.
      * Replace `_PIPELINE_BASE_ENTRY_ID_COL` with `new_base_entry_id`.
      * Drop `NODE_CONTENT_HASH_COL` from the row.
      * Record the new primary key as `new_pipeline_entry_id`.
5. Build the migrated Arrow table (rows from step 4b, unmigrated rows from step 4a).
6. Write back to the DB using the backend's overwrite/replace mechanism.
7. Return `MigrationReport(migrated=len(step4b), skipped=len(step4a), already_done=0)`.

**Idempotency:** The outer check in step 3 makes repeated calls safe — if the column is
already gone, the function returns immediately.

**Write-back strategy:** `ArrowDatabaseProtocol` has no `replace_table` method. Two
concrete strategies:

* **`InMemoryArrowDatabase`:** Clear the internal record store for the path and
  re-add all rows using `add_records()`. Since InMemory has full access to its internal
  state, this is straightforward.
* **`DeltaTableDatabase`:** Use `deltalake.write_deltalake(table_uri, new_table,
  mode="overwrite")` directly, which is the same mechanism `flush()` already uses.
* **Other backends / no-op:** Log a warning and skip — the system remains correct (old
  records become orphaned) but prior computations are lost.

**Placement:** `migrate_pipeline_records` is a module-level function in a new file
`src/orcapod/migrations/itl_533.py`, accepting `node: FunctionJobNode`. It is not a
method on `FunctionJobNode` to keep the node class focused on runtime concerns.

### Side-effect invocation log migration

The side-effect invocation log (`_write_invocation_row`) stores only:
`record_id_hash`, `pipeline_run_id`, `executed_at`. It does **not** store `system_tags`
or `INPUT_DATA_HASH_COL`. There is therefore no way to recompute the new `record_id`
from the stored log entries.

**Strategy:** Drop all side-effect invocation log entries for a given node. The next run
will re-deliver all side effects once. This is acceptable because:

* `track_completion` is designed to prevent duplicate delivery within a pipeline run.
  One additional delivery per input after migration is the documented worst case.
* Side effects are by definition external actions; users of `track_completion` already
  accept the possibility of re-delivery (e.g., after a crash).

**Function signature:**

```python
def migrate_side_effect_records(
    pod_node: SideEffectNode | SideEffectJobNode,
) -> int:
    """Drop all side-effect invocation log entries. Returns count of dropped entries."""
    ...
```

**Write-back strategy:** Same backend-specific logic as the pipeline migration.

**Placement:** Same file — `src/orcapod/migrations/itl_533.py`.

---

## Scope & Boundaries

In scope:
* Remove `NODE_CONTENT_HASH_COL` from the `record_id` preimage in `FunctionJobNode`
* Remove `NODE_CONTENT_HASH_COL` from the `record_id` preimage in `SideEffectPod`
* Remove `NODE_CONTENT_HASH_COL` as a stored DB column in `add_pipeline_record()`
* Delete `_filter_by_content_hash()` and its single call site
* Extract shared `_build_record_id_preimage()` helper
* Update `get_all_records()` and `get_cached_results()` to stop referencing
  `NODE_CONTENT_HASH_COL`
* Update all affected docstrings
* Add `src/orcapod/migrations/itl_533.py` with `migrate_pipeline_records()` and
  `migrate_side_effect_records()`

Out of scope:
* `table_scope="content_hash"` (non-default mode) — separate investigation needed
* Removing `NODE_CONTENT_HASH_COL` from `system_constants.py` (may be used elsewhere)
* Adding `replace_table` to `ArrowDatabaseProtocol` (deferred to a follow-on issue)
* Any changes to pipeline_hash or content_hash computation logic

---

## Testing

### Existing tests (must all pass after the change)

* `tests/test_core/function_pod/test_pipeline_hash_integration.py` — all tests,
  especially `test_shared_db_overlapping_inputs_avoids_recomputation` and
  `test_shared_db_all_inputs_pre_computed_zero_recomputation`
* All other `test_core/function_pod/` tests
* All `test_core/side_effect_pod/` and `test_core/side_effect_function/` tests
* All `test_core/` operator and source tests

### Investigation artefact (forward-looking assertions)

`tests/test_core/function_pod/test_node_content_hash_redundancy.py` was created as
part of this investigation. After the fix:

* `TestPreimageShape.test_node_content_hash_col_not_in_preimage_keys` — currently
  **fails** (intentionally), will **pass** after the change. This is the primary
  regression guard.
* All other tests in that file already pass and must continue to pass.

### Migration tests

New test file `tests/test_core/function_pod/test_itl533_migration.py`:

* `test_migrate_pipeline_records_rewrites_hashes` — create a node, run it (writes old
  records with `NODE_CONTENT_HASH_COL`), call `migrate_pipeline_records()`, then
  instantiate a new node with the fixed code and assert results are served from cache
  (zero recomputation).
* `test_migrate_pipeline_records_idempotent` — calling migration twice produces the
  same result.
* `test_migrate_pipeline_records_skips_pre_itl534_rows` — rows without
  `INPUT_DATA_HASH_COL` are counted as `skipped` and left unchanged (they remain
  orphaned but don't corrupt the table).
* `test_migrate_side_effect_records_drops_entries` — create a side effect node, run it,
  call `migrate_side_effect_records()`, verify the log table is empty.
* `test_migrate_side_effect_records_idempotent` — second call on an already-empty log
  returns 0 without error.

---

## Dependencies & Risks

* **Code change and migration must be deployed together.** After the code change,
  `_filter_by_content_hash()` is gone, so old records (with `NODE_CONTENT_HASH_COL`)
  simply become orphans — no crash. Migration is optional for correctness but required
  to restore cache reuse of prior computations.
* **`_filter_by_content_hash()` currently raises `ValueError`** when
  `NODE_CONTENT_HASH_COL` is missing from a `pipeline_hash` table (added in this PR
  window, not the original spec). Deleting it is therefore strictly required alongside
  removing the column from `add_pipeline_record()` — they cannot be deployed
  independently.
* **`ArrowDatabaseProtocol` lacks `replace_table`.** Migration uses backend-specific
  overwrite (InMemory: clear + re-add; DeltaLake: `write_deltalake(mode="overwrite")`).
  Other backends silently skip with a warning. A follow-on issue should add
  `replace_table` to the protocol.
* **Side effects re-deliver once after migration.** Users with `track_completion=True`
  should be aware of and accept this one-time behaviour.
* **Pre-ITL-534 records (missing `INPUT_DATA_HASH_COL`) cannot be migrated.** They
  become orphaned and the node recomputes those inputs. Acceptable pre-v0.1.0.
* **`NODE_CONTENT_HASH_COL` constant stays** — removing the constant risks breaking
  non-in-scope usage. Leave it defined; a follow-on cleanup can remove it if unused.
