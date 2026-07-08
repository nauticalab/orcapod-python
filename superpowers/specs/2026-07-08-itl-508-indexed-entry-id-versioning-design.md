# ITL-508: Indexed `entry_id` Versioning for Concurrency-Safe Miss-Triggered Recomputation

**Date:** 2026-07-08
**Issue:** [ITL-508](https://linear.app/enigma-metamorphic/issue/ITL-508)
**Status:** Approved

---

## Background

The v1 ephemeral result store (ITL-507) introduced an append strategy for
miss-triggered Phase 2 writes: `add_pipeline_record` is called with
`skip_cache_lookup=True`, which bypasses the pipeline DB lookup and calls
`add_record(skip_duplicates=True)` using the existing `entry_id` as the key.

The problem is that `skip_duplicates=True` on the original `entry_id` is
blocked when a stale entry with that same key already exists in the pipeline
DB (either committed or pending). This means the new, valid `DATA_RECORD_ID`
is never written. Future Phase 1 loads always find the stale entry, miss the
join, fall through to the `CachedFunctionPod`'s secondary result-DB lookup
(which works, because the result IS in result_db), but never update the
pipeline record — accumulating a permanent spurious warning and preventing
the inner-join fast path from ever landing for that input.

Under concurrent execution (asyncio `TaskGroup`), multiple coroutines can all
observe a Phase 1 miss simultaneously, all enter Phase 2, and all compute the
same deterministic result. The first write lands; the rest are silently
no-oped by `skip_duplicates=True`. This is correct but wasteful and doesn't
update the pipeline record to point at the newly computed result.

The fix is to give each recomputation attempt a **distinct key** —
`versioned_entry_id = hash(tag, data, node, recomputation_index)`. Concurrent
threads competing to write index `N+1` all hash to the **same**
`versioned_entry_id` (deterministic); the DB's `skip_duplicates=True` ensures
exactly one write lands. Stale entry at index `N` no longer blocks the write
at index `N+1`.

---

## Design

### 1. New pipeline DB columns

Two columns are added to every pipeline DB record written by `FunctionJobNode`:

| Column constant | Arrow type | Description |
|---|---|---|
| `_PIPELINE_BASE_ENTRY_ID_COL` | `pa.large_binary()` | Hash of (tag + system_tags + input_data_hash + node_content_hash). Stable across all recomputation attempts for the same logical input. |
| `_PIPELINE_RECOMPUTATION_INDEX_COL` | `pa.int32()` | Position in the recomputation chain. `0` for the first computation, `N+1` for each miss-triggered recompute. |

The existing `_PIPELINE_ENTRY_ID_COL` column is retained as the DB primary
key; it now stores the **versioned** entry ID (see §2).

### 2. Entry ID computation

Two functions replace the current single `compute_pipeline_entry_id`:

**`compute_base_entry_id(tag, data) -> bytes`**

Identical to the current `compute_pipeline_entry_id` implementation: hashes
the Arrow table of (tag + system_tags + `INPUT_DATA_HASH_COL` +
`NODE_CONTENT_HASH_COL`). Returns `b"{method}:{digest}"`. This value is
stored in `_PIPELINE_BASE_ENTRY_ID_COL` and used as the in-memory cache key.

**`compute_pipeline_entry_id(tag, data, recomputation_index: int = 0) -> bytes`**

Extended signature. Appends a `_PIPELINE_RECOMPUTATION_INDEX_COL` column
(value: `recomputation_index`, type `pa.int32()`) to the table before
hashing. The resulting digest is the **versioned entry ID** stored in
`_PIPELINE_ENTRY_ID_COL` (the DB primary key).

Note: at `recomputation_index=0` this produces a hash that differs from the
old `compute_pipeline_entry_id` output (the index column is now part of the
preimage). Existing pipeline DB records written before this change are
implicitly invalidated. This is acceptable: the project is pre-v0.1.0 and
carries no backward-compatibility obligations.

### 3. In-memory cache re-keying

`_cached_output_datas` and all Phase 1 / Phase 2 code that reads or writes
it switches from `versioned_entry_id` to `base_entry_id` as the dict key.
`base_entry_id` is stable across recomputation cycles, so a Phase 1 hit on
index `N+1` correctly populates the cache for subsequent lookups of the same
logical input.

### 4. Phase 1 changes (cache loading)

**`execute()` and `async_execute()`**

`compute_base_entry_id(tag, data)` replaces `compute_pipeline_entry_id(tag, data)`
as the per-input identifier. All downstream calls that previously passed
`entry_id` lists now pass `base_entry_id` lists.

**`get_cached_results(base_entry_ids)`**

The `entry_ids` parameter is renamed `base_entry_ids`. The filtering inside
`_load_cached_entries` / `_fetch_joined_records` now targets
`_PIPELINE_BASE_ENTRY_ID_COL` rather than `_PIPELINE_ENTRY_ID_COL`.

**`_fetch_joined_records(base_entry_ids)`**

`get_all_records()` already returns every row for the node path, so all
recomputation-index versions of a given logical input are present in the
loaded table. The existing inner-join logic is unchanged: whichever versioned
row(s) have a `DATA_RECORD_ID` that resolves in the result DB survives; stale
rows drop out naturally.

After the join and anti-join merge, the returned dict (`_load_cached_entries`)
is keyed by `_PIPELINE_BASE_ENTRY_ID_COL` instead of `_PIPELINE_ENTRY_ID_COL`.
The `entry_ids` filter parameter is renamed to `base_entry_ids` and filters
on `_PIPELINE_BASE_ENTRY_ID_COL`.

### 5. Phase 2 changes (miss-triggered write)

**`add_pipeline_record` — redesigned**

The `skip_cache_lookup` parameter is removed; it was a v1 workaround that is
no longer needed. The new implementation always:

1. Computes `base_entry_id = compute_base_entry_id(tag, input_data)`.
2. Queries the pipeline DB for all existing rows with this `base_entry_id`:
   `get_records_with_column_value(node_path, {_PIPELINE_BASE_ENTRY_ID_COL: base_entry_id})`.
3. Determines `new_index`:
   - If no rows exist → `new_index = 0` (first-ever computation).
   - Otherwise → `new_index = max(row[_PIPELINE_RECOMPUTATION_INDEX_COL]) + 1`.
4. Computes `versioned_entry_id = compute_pipeline_entry_id(tag, input_data, new_index)`.
5. Builds the combined pipeline record (same columns as today, plus
   `_PIPELINE_BASE_ENTRY_ID_COL` and `_PIPELINE_RECOMPUTATION_INDEX_COL`).
6. Calls `add_record(node_path, versioned_entry_id, record, skip_duplicates=True)`.

Step 6 is the atomic insert-if-not-exists. For asyncio (the primary
concurrency model): steps 2–6 are all synchronous — no yield point exists
between "read max index" and "write". Concurrent coroutines competing for the
same `new_index` therefore serialise naturally: the first writer adds
`versioned_entry_id_{N+1}` to `_pending_record_ids`; subsequent writers find
it there and are silently no-oped by `skip_duplicates=True`.

**`_process_data_internal` and `_async_process_data_internal`**

Remove the `skip_cache_lookup=True` argument from all `add_pipeline_record`
calls (parameter removed). The in-memory cache write switches to:
```python
base_entry_id = self.compute_base_entry_id(tag, data)
self._cached_output_datas[base_entry_id] = (tag_out, output_data)
```

### 6. Recomputation index chain — single shared chain

The recomputation index is a single shared counter per `base_entry_id`,
regardless of `is_ephemeral`. A given index value is unique across all rows
sharing the same `base_entry_id` (whether ephemeral or persistent), so
`versioned_entry_id` (which hashes in the index) is always unique. The
existing anti-join merge in `_fetch_joined_records` continues to express
persistent-wins-over-ephemeral priority within a shared `base_entry_id`.

### 7. Known limitation — Python threads and multi-process (ITL-515)

For **asyncio** (cooperative multitasking): the "read max index → write at
max+1" sequence is atomic because both steps are synchronous. This is the
primary concurrency model and the guarantee is complete.

For **Python threads** sharing one `InMemoryArrowDatabase`: the check-then-set
in `_filter_existing_records` → `_pending_record_ids.update()` is not atomic
under the GIL. A `threading.Lock` around `add_records` would close this gap
but is deferred to ITL-515.

For **multiple processes** sharing a Delta Lake pipeline_db: a TOCTOU window
exists between "read max" and "write". Closing this requires
`insert_if_not_exists` backed by a Delta Lake ACID transaction, also deferred
to ITL-515.

---

## File-by-file change summary

| File | Change |
|---|---|
| `src/orcapod/core/nodes/function_node.py` | Add `_PIPELINE_BASE_ENTRY_ID_COL`, `_PIPELINE_RECOMPUTATION_INDEX_COL` constants. Add `compute_base_entry_id()`. Extend `compute_pipeline_entry_id()` signature. Redesign `add_pipeline_record()`. Update Phase 1 in `execute()`, `async_execute()`, `get_cached_results()`, `_fetch_joined_records()`, `_load_cached_entries()`. Update Phase 2 in `_process_data_internal()`, `_async_process_data_internal()`. |
| `src/orcapod/protocols/pipeline_protocols.py` | Remove `skip_cache_lookup` from the `add_pipeline_record` protocol definition. |
| `tests/test_core/function_pod/test_function_node_caching.py` | Update existing tests that call `compute_pipeline_entry_id` directly; remove `skip_cache_lookup` from `add_pipeline_record` calls. |
| `tests/test_core/function_pod/test_ephemeral_result.py` | Update existing miss-recomputation tests; add new concurrent-miss serialisation tests. |
| `tests/test_core/nodes/test_function_node_fetch_joined.py` | Update for renamed `entry_ids` → `base_entry_ids` parameter. |
| `tests/test_core/nodes/test_function_node_get_cached.py` | Replace `compute_pipeline_entry_id` calls with `compute_base_entry_id`; update `get_cached_results(entry_ids)` to `get_cached_results(base_entry_ids)`. |
| `tests/test_pipeline/test_node_protocols.py` | Update mock implementations of `get_cached_results` and `compute_pipeline_entry_id` to match new signatures. |

---

## Testing plan

### Single-threaded correctness
- Miss-triggered recompute writes a new row at `recomputation_index=1`.
- Pipeline DB contains two rows: stale at index 0, valid at index 1.
- Subsequent Phase 1 finds the valid row (inner join succeeds); no recompute.
- `call_count` does not increase in the third session.

### Concurrent-miss serialisation (asyncio)
- Two asyncio tasks both observe a Phase 1 miss for the same (tag, data).
- Both enter Phase 2 concurrently.
- After both complete, pipeline DB contains **at least one** valid row for
  the `base_entry_id`. Because `add_pipeline_record` is fully synchronous
  (no `await`), asyncio cooperative multitasking serialises the two writes:
  each coroutine reads the current `max_index` and writes at `max_index + 1`,
  so in practice both rows land (one at index 0, one at index 1). The
  `skip_duplicates=True` guard prevents a write from clobbering another only
  when two coroutines happen to compute the identical versioned entry ID, which
  cannot occur when `max_index` advances between reads. Asserting `>= 1` rows
  is therefore the correct lower bound; the exact count depends on scheduling.
- A subsequent Phase 1 lookup (new session, same DBs) finds a valid result
  and does NOT recompute.

### Ephemeral + persistent coexistence
- Ephemeral row at index 0 (stale) and persistent row at index 1 share the
  same `base_entry_id`.
- Phase 1 anti-join correctly surfaces the persistent result.

### Regression
- All existing tests in `test_function_node_caching.py`,
  `test_ephemeral_result.py`, and `test_function_node_fetch_joined.py` pass
  without modification (beyond call-site updates for removed parameter).
