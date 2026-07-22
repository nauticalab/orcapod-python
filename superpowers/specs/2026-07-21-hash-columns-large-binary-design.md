# Hash Columns as `large_binary` — Final Cleanup (ITL-539)

**Date:** 2026-07-21
**Issue:** [ITL-539](https://linear.app/metamorphic/issue/ITL-539)

## Overview

Most of ITL-539 was already implemented in earlier PRs (ITL-534, ITL-535): the pipeline DB and
result cache now write `INPUT_DATA_HASH_COL`, `OUTPUT_DATA_HASH_COL`, and `NODE_CONTENT_HASH_COL`
as `large_binary`, and full v0→v1 migration utilities exist for both databases.

This spec covers the remaining four gaps identified by a full audit:

1. `data_function.py` — variation hash fields still produced as `large_string`
2. `result_cache.py` — backward-compat conversion shim, no longer needed
3. `operator_node.py` — `NODE_CONTENT_HASH_COL` stored as `large_string`
4. `side_effects.py` — `record_id_hash` log column stored as `large_string`

## Audit Summary

### Already correct (no changes needed)

| Component | Column | Type |
|-----------|--------|------|
| `FunctionJobNode.add_pipeline_record()` | `INPUT_DATA_HASH_COL`, `OUTPUT_DATA_HASH_COL`, `NODE_CONTENT_HASH_COL` | `large_binary` ✅ |
| `ResultCache.store()` | `INPUT_DATA_HASH_COL` | `large_binary` ✅ |
| `ResultCache.lookup()` | query uses `to_prefixed_digest()` | ✅ |
| `ContentHash.from_prefixed_digest()` | exists in `types.py` | ✅ |
| v0→v1 migrations (pipeline DB + result DB) | all hash columns | ✅ |

### Non-DB string usages (intentional, out of scope)

- Path components: `f"schema:{hash.to_string()}"`, `f"instance:{hash.to_string()}"` — string path keys, not Arrow columns
- Observer calls: `on_node_start(label, hash_str)` — observer interface uses strings
- Python dict keys: `content_hash().to_string()` used as in-memory graph keys
- Source IDs: `f"...:{hash.to_string()}"` — string identifier, not stored in DB
- User-facing `_content_hash` column in `as_table()` — optional display column, not stored in DB
- Semantic hasher internals — hashing preimage computation

## Changes

### 1. `src/orcapod/core/data_function.py`

**Problem:** `_function_signature_hash` and `_function_content_hash` are computed and stored as
`str` via `to_string()`. The schema declares them as `str`, so `Datagram.as_table()` emits them
as `large_string` columns.

**Fix:**
- Change both `to_string()` calls to `to_prefixed_digest()` — the values become `bytes`.
- Change both schema entries from `str` to `bytes` — `Datagram.as_table()` will emit
  `large_binary` columns via the `bytes → pa.large_binary()` mapping in `universal_converter.py`.

```python
# Before
self._function_signature_hash = semantic_hasher.hash_object(
    get_function_signature(function)
).to_string()
self._function_content_hash = semantic_hasher.hash_object(
    get_function_components(self._function)
).to_string()

# After
self._function_signature_hash = semantic_hasher.hash_object(
    get_function_signature(function)
).to_prefixed_digest()
self._function_content_hash = semantic_hasher.hash_object(
    get_function_components(self._function)
).to_prefixed_digest()
```

```python
# Schema before
def get_function_variation_data_schema(self) -> Schema:
    return Schema({
        "function_name": str,
        "function_signature_hash": str,   # ❌
        "function_content_hash": str,     # ❌
        "git_hash": str,
    })

# Schema after
def get_function_variation_data_schema(self) -> Schema:
    return Schema({
        "function_name": str,
        "function_signature_hash": bytes,  # ✅
        "function_content_hash": bytes,    # ✅
        "git_hash": str,
    })
```

**No other callers** access `_function_signature_hash` or `_function_content_hash` directly —
both are only read through `get_function_variation_data()`.

### 2. `src/orcapod/core/result_cache.py`

**Problem:** The `_hash_val_to_binary()` helper and the `_HASH_VAR_COLS` conversion loop in
`store()` were added as a tolerance shim to handle string-typed variation hash columns. With
`PythonDataFunction` now producing bytes, the shim is dead code.

**Fix:** Remove `_hash_val_to_binary()` and the entire `_HASH_VAR_COLS` conversion loop from
`store()`. The variation datagram's `as_table()` now emits `large_binary` columns directly.

Note: The `constraints` dict in `lookup()` uses `bytes` values (via `to_prefixed_digest()`). Its
type annotation `dict[str, str]` is incorrect and should be corrected to `dict[str, bytes]`.

### 3. `src/orcapod/core/nodes/operator_node.py`

**Problem:** `_store_output_stream()` appends `NODE_CONTENT_HASH_COL` as `large_string` via
`.to_string()`. `_filter_by_content_hash()` reads it back with `.to_string()` and a string
`pc.equal` comparison.

**Fix:**
- Write: change `to_string()` / `large_string()` to `to_prefixed_digest()` / `large_binary()`.
- Read: change filter to compare against the binary digest.

```python
# Write — before
pa.repeat(self.content_hash().to_string(), n_rows).cast(pa.large_string())

# Write — after
pa.array(
    [self.content_hash().to_prefixed_digest()] * n_rows,
    type=pa.large_binary(),
)
```

```python
# Filter — before
own_hash = self.content_hash().to_string()
mask = pc.equal(table.column(col_name), own_hash)

# Filter — after
own_hash = self.content_hash().to_prefixed_digest()
mask = pc.equal(table.column(col_name), own_hash)
```

**Breaking change note:** `OperatorJobNode` has no schema versioning infrastructure. Any existing
operator node pipeline DB with the old `large_string` schema will fail on next write due to schema
mismatch. This is acceptable for a pre-v0.1.0 project. A DESIGN_ISSUES.md note will document the
migration gap.

### 4. `src/orcapod/side_effects.py`

**Problem:** `_write_invocation_row()` stores the `record_id_hash` column as `large_string` using
`.to_string()`. The binary `record_id` is already the primary key; `record_id_hash` is a display
column stored alongside it.

**Fix:**
- Change the function signature: `record_id_hash_str: str` → `record_id_hash_bytes: bytes`.
- Change the call site: pass `record_id_hash.to_prefixed_digest()` instead of `.to_string()`.
- Change the Arrow array: `pa.large_string()` → `pa.large_binary()`.

No migration needed — `record_id_hash` is not used for record lookup; each invocation writes a new
row and the old column value is never re-read by column comparison.

## Tests

### New tests needed

**`tests/test_pipeline/test_result_cache.py`** (or extend existing):
- Verify `ResultCache.store()` stores `function_signature_hash` and `function_content_hash` as
  `large_binary` columns when used with a `PythonDataFunction`.
- Verify the stored bytes decode correctly via `ContentHash.from_prefixed_digest()`.

**`tests/test_pipeline/test_operator_node_hash.py`** (or extend existing operator node tests):
- Verify `OperatorJobNode._store_output_stream()` writes `NODE_CONTENT_HASH_COL` as `large_binary`.
- Verify `_filter_by_content_hash()` correctly isolates rows by binary hash value.

**`tests/test_pipeline/test_side_effects.py`** (or extend existing):
- Verify `_write_invocation_row()` stores `record_id_hash` as `large_binary`.
- Verify the stored bytes decode correctly via `ContentHash.from_prefixed_digest()`.

### Existing tests

- All 4011 existing tests must continue to pass after the changes.
- `tests/test_migrations/` — no changes; existing migration tests cover string-to-binary
  conversion for the result DB and pipeline DB.

## Out of Scope

Per the original issue and confirmed in design:

- `_PIPELINE_BASE_ENTRY_ID_COL` — already `large_binary`
- `DATA_RECORD_ID` — already `large_binary`
- `NODE_CONTENT_HASH_COL` and `DATA_RECORD_ID` — out of scope per original issue (except
  the `OperatorJobNode` fix covered above, which was found during the audit)
- Backfilling old tag rows — tracked by ITL-535
- Non-DB string uses of `to_string()` (path components, observer calls, dict keys)
- User-facing `_content_hash` display column in `FunctionJobNode.as_table()`
