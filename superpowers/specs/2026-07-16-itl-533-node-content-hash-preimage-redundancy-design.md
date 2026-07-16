# ITL-533: Remove NODE_CONTENT_HASH_COL from record_id preimage

**Date:** 2026-07-16
**Linear issue:** ITL-533
**Scope:** `table_scope="pipeline_hash"` (the default) only

---

## Overview

`FunctionJobNode` and `SideEffectPod` compute a `record_id` by hashing a preimage
table. The preimage currently contains `NODE_CONTENT_HASH_COL` (`_node_content_hash`),
which is `self.content_hash()` for the node. This column is redundant: the combination
of `system_tags` and `INPUT_DATA_HASH_COL` already uniquely identifies every pipeline
record without it.

This spec defines the changes required to remove `NODE_CONTENT_HASH_COL` from (a) the
preimage, (b) the stored DB row, and (c) the `_filter_by_content_hash()` filtering
mechanism that depended on it.

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

There are exactly two cases:

**Case A — Different inputs.** Two rows with different upstream data have different
`system_tags` (because `ArrowTableSource.source_id` defaults to `table_hash`, so
different table content → different `source_id` → different `_tag_source_id::*` column
value). This alone makes `base_entry_id = hash(system_tags + INPUT_DATA_HASH_COL)`
distinct. `NODE_CONTENT_HASH_COL` adds nothing.

**Case B — Same inputs.** Same source content → same `source_id` → same `system_tags`
→ same `base_entry_id`. But the lockstep property (see below) means
`NODE_CONTENT_HASH_COL` is also identical in this case. The two rows represent the same
logical record, so sharing a `record_id` is correct (idempotency).

**Lockstep property.** `NODE_CONTENT_HASH_COL = self.content_hash()`, which for a
`FunctionJobNode` is `hash(content_hash(FunctionPod), content_hash(upstream))`. Because
`ArrowTableSource.identity_structure()` = `(class_name, schema, source_id)`, two nodes
with the same `source_id` and schema always produce the same `content_hash`. Therefore:

> There is no scenario where `base_entry_id` is the same but `NODE_CONTENT_HASH_COL`
> differs — or vice versa. They are always in lockstep.

This was verified empirically in
`tests/test_core/function_pod/test_node_content_hash_redundancy.py`.

### Why `_filter_by_content_hash()` is a no-op

`_filter_by_content_hash()` filters the pipeline DB table to rows where
`NODE_CONTENT_HASH_COL = self.content_hash()`. Its purpose was to prevent one
`FunctionJobNode` from reading records written by a different node that happens to share
the same `pipeline_hash` (and therefore the same DB table).

This concern is already fully addressed by the subsequent `base_entry_id` filter in
`get_cached_results(base_entry_ids=[...])`: only records whose `base_entry_id` matches
the current node's inputs are returned. Since the `base_entry_id` is derived from
`system_tags` (which includes the content-addressed `source_id`), records from a
different upstream source always have different `base_entry_ids` and are naturally
excluded.

This was verified empirically: patching `_filter_by_content_hash()` to be a no-op
(pass-through) produces identical call counts and output values in all scenarios.

---

## Architecture

### Shared preimage helper

A new private free function (or method) `_build_record_id_preimage(tag, data) -> pa.Table`
is extracted. It returns a single-row Arrow table with columns:

```
[system_tag columns..., INPUT_DATA_HASH_COL]
```

This helper is used by both `FunctionJobNode._build_entry_id_preimage()` and the
recomputation-index preimage step in `_execute_side_effect_row()`.

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
| `_fetch_joined_records()` | Remove the `_filter_by_content_hash()` call |
| `get_all_records()` | Remove `NODE_CONTENT_HASH_COL` from the always-drop column list |
| Docstrings | Update any references to `NODE_CONTENT_HASH_COL` in preimage descriptions |

#### `src/orcapod/side_effects.py`

| Location | Change |
|---|---|
| `_execute_side_effect_row()` | Replace inline preimage construction with `_build_record_id_preimage(tag, data)` + append `__pipeline_recomputation_index`; remove `node_content_hash_str` parameter |
| 4 call sites (lines 248, 799, 936, 995) | Remove `node_content_hash_str=self._pod.content_hash().to_string()` argument |

#### `src/orcapod/system_constants.py`

`NODE_CONTENT_HASH_COL` itself stays defined — other code outside this scope may
reference it, and removing a constant is a separate, lower-risk cleanup. No changes to
this file in this PR.

### Preimage shape before and after

| Column | Before | After |
|---|---|---|
| `system_tag columns` | ✓ | ✓ |
| `INPUT_DATA_HASH_COL` | ✓ | ✓ |
| `NODE_CONTENT_HASH_COL` | ✓ | **removed** |
| `__pipeline_recomputation_index` | ✓ (pipeline_entry_id only) | ✓ (unchanged) |

### DB row shape before and after

| Column | Before | After |
|---|---|---|
| `record_id` | ✓ | ✓ |
| `base_entry_id` | ✓ | ✓ |
| `pipeline_entry_id` | ✓ | ✓ |
| `NODE_CONTENT_HASH_COL` | ✓ | **removed** |
| data columns | ✓ | ✓ |

### Impact on existing DB records

Removing `NODE_CONTENT_HASH_COL` from the preimage changes the hash function. Any
pipeline DB records written before this change will have different `record_id` values
than records written after. This is acceptable because the project is pre-v0.1.0 and
no backward-compatibility guarantees exist. The existing code already has comments
acknowledging that preimage changes invalidate old records.

---

## Scope & Boundaries

In scope:
* Remove `NODE_CONTENT_HASH_COL` from the `record_id` preimage in `FunctionJobNode`
* Remove `NODE_CONTENT_HASH_COL` from the `record_id` preimage in `SideEffectPod`
* Remove `NODE_CONTENT_HASH_COL` as a stored DB column in `add_pipeline_record()`
* Delete `_filter_by_content_hash()` and its single call site
* Extract shared `_build_record_id_preimage()` helper
* Update `get_all_records()` to stop stripping `NODE_CONTENT_HASH_COL`
* Update all affected docstrings

Out of scope:
* `table_scope="content_hash"` (non-default mode) — separate investigation needed
* Removing `NODE_CONTENT_HASH_COL` from `system_constants.py` (may be used elsewhere)
* Any changes to pipeline_hash or content_hash computation logic
* Schema migration tooling for existing DB records

---

## Testing

### Existing tests (must all pass after the change)

* `tests/test_core/function_pod/test_pipeline_hash_integration.py` — all tests,
  especially `test_shared_db_overlapping_inputs_avoids_recomputation` and
  `test_shared_db_all_inputs_pre_computed_zero_recomputation`
* All other `test_core/function_pod/` tests
* All `test_core/` operator and source tests

### Investigation artefact (forward-looking assertions)

`tests/test_core/function_pod/test_node_content_hash_redundancy.py` was created as
part of this investigation. After the fix:

* `TestPreimageShape.test_node_content_hash_col_not_in_preimage_keys` — currently
  **fails** (intentionally), will **pass** after the change. This is the primary
  regression guard.
* All other tests in that file already pass and must continue to pass.

### No new tests required

The investigation tests already cover the key properties. The fix is a deletion, not
an addition, so test coverage naturally decreases (the filter code path disappears).

---

## Dependencies & Risks

* **Preimage hash change invalidates old DB records** — acceptable pre-v0.1.0.
* **`SideEffectPod` uses a free function** (`_execute_side_effect_row`) rather than a
  method, requiring the shared helper to be importable from `function_node.py` into
  `side_effects.py` (or placed in a shared utils module). Straightforward, no circular
  import risk since `side_effects.py` already imports from `function_node.py`.
* **`NODE_CONTENT_HASH_COL` constant stays** — removing the constant risks breaking
  non-in-scope usage. Leave it defined; a follow-on cleanup can remove it if unused.
