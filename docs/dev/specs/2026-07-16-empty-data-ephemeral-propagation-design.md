# EmptyData + Ephemeral Result Propagation Design

**Issue:** ITL-534  
**Date:** 2026-07-16  
**Status:** Approved

---

## Overview

Some Orcapod pods produce ephemeral results — data that may be absent by the time a
downstream pod tries to load it (pruned, expired, deliberately not persisted). Today,
a missing ephemeral result silently drops the pipeline row, blocking flow even in cases
where the downstream pod has already computed a result for that same input content hash.

This design introduces:

1. **`EmptyData`** — a `Data` subclass that represents missing data. Carries an optional
   cached content hash (the hash of the **upstream output** payload, which equals the
   downstream node's input hash) and an optional source-info field (data model only;
   reconstruction logic is deferred).
2. **Tag table schema extension** — `INPUT_DATA_HASH_COL` (upstream input hash) and
   `OUTPUT_DATA_HASH_COL` (upstream output hash) are now both persisted in every pipeline
   DB row so the read path can reconstruct a `ContentHash` for cache lookup. Hash columns
   are stored as `large_binary` using `ContentHash.to_prefixed_digest()`.
3. **Read path change** — `_fetch_joined_records()` emits `EmptyData` tokens for
   ephemeral miss rows instead of silently dropping them.  `cached_content_hash` is
   populated from `OUTPUT_DATA_HASH_COL` (the correct key for downstream cache lookup).
4. **Downstream handling** — `_process_data_internal()` performs a cache lookup via the
   `EmptyData`'s cached content hash; a cache miss raises `EphemeralResultMissingError`
   loudly.

---

## Goals & Success Criteria

- An ephemeral miss no longer silently drops a pipeline row.
- A downstream pod whose result store already has an entry for the same input hash
  produces its output without touching the missing intermediate data.
- A downstream pod with no cached result for the missing input fails loudly with a
  structured exception carrying enough context to debug.
- Old pipeline DB rows (lacking `OUTPUT_DATA_HASH_COL`) produce a WARNING log and
  continue to work in degraded mode (no flow-through, no forced migration).
- All non-ephemeral paths are unaffected.

---

## Scope & Boundaries

**In scope:**

- `EmptyData(Data)` subclass with `cached_content_hash` and `empty_source_info` fields.
- Exception types: `EmptyDataAccessError`, `EmptyDataHashMissingError`,
  `EphemeralResultMissingError`.
- `INPUT_DATA_HASH_COL` column added to `add_pipeline_record()`.
- `_fetch_joined_records()` emits `EmptyData` tokens for ephemeral miss rows.
- `_process_data_internal()` (sync and async) guards for `EmptyData` inputs.
- `_load_cached_entries()` merges normal rows and `EmptyData` tokens.
- `_JoinedRecords` extended with `empty_data_tokens` field.
- Warning log for old-format rows lacking `INPUT_DATA_HASH_COL`.
- Unit and integration tests for all new paths.

**Out of scope (follow-up issues):**

- Backfilling old tag rows — ITL-535.
- Configurable relaxation of strict `EmptyData` handling (options B/C) — follow-up.
- Rigorous upstream-ephemerality validation in `_process_data_internal()` — follow-up.
- Tag-row reconstruction from downstream cache evidence (writing rows with
  `record_id=None`) — follow-up.
- `empty_source_info` population (field defined here; write logic deferred) — follow-up.

---

## Architecture

```
┌────────────────────────────────────────────────────────┐
│ 1. Data model   EmptyData(Data) — new subclass         │
│                 empty_source_info field (model only)   │
│                 Exception types                        │
├────────────────────────────────────────────────────────┤
│ 2. Tag table    add INPUT_DATA_HASH_COL to pipeline DB │
│    write path   (add_pipeline_record)                  │
├────────────────────────────────────────────────────────┤
│ 3. Tag table    _fetch_joined_records emits EmptyData  │
│    read path    on ephemeral miss instead of dropping  │
├────────────────────────────────────────────────────────┤
│ 4. Downstream   _process_data_internal: cache lookup   │
│    handling     on EmptyData; loud fail on miss        │
└────────────────────────────────────────────────────────┘
```

### Ephemeral-miss data flow (after this change)

```
Upstream (is_result_ephemeral=True)
  → writes INPUT_DATA_HASH_COL (upstream input hash) into tag table row
  → writes OUTPUT_DATA_HASH_COL (upstream output hash) into tag table row
  → result expires from ephemeral store

Downstream _fetch_joined_records
  → sees ephemeral tag row, finds no result DB row
  → reads OUTPUT_DATA_HASH_COL from the tag row
  → emits EmptyData(cached_content_hash=<OUTPUT_DATA_HASH_COL value>)
    instead of silently dropping
    (OUTPUT_DATA_HASH_COL = upstream output = downstream input = correct cache key)

Downstream _process_data_internal(tag, EmptyData)
  → CachedFunctionPod.lookup_cached_data(empty_data)
      # uses empty_data.content_hash() = cached upstream output hash
  → Hit  → return cached output (normal flow)
  → Miss → raise EphemeralResultMissingError(tag, hash, node_identity_path)
```

---

## Component Design

### 1. `EmptyData` class

**File:** `src/orcapod/core/datagrams/tag_data.py`

`EmptyData` is a subclass of `Data`. It holds all normal datagram metadata (data context,
record UUID, python schema) but carries no data payload. Every method that would access
the payload raises `EmptyDataAccessError`.

**`content_hash()` override:**  
Returns `self._cached_content_hash` if set. Raises `EmptyDataHashMissingError` if
`cached_content_hash` is `None`. This makes `ResultCache.lookup(empty_data)` work
transparently — the cached hash is the upstream output hash (sourced from
`OUTPUT_DATA_HASH_COL`), which equals the downstream's input hash and therefore matches
the key used by the downstream's result cache.

**`empty_source_info` field:**  
An optional `dict[str, str | None]` structurally matching tag-row provenance columns,
with `record_id` allowed to be `None`. The field exists for the downstream reconstruction
follow-up; this PR defines the data model but does not populate it.

```python
class EmptyData(Data):
    def __init__(
        self,
        cached_content_hash: ContentHash | None = None,
        empty_source_info: dict[str, str | None] | None = None,
        python_schema: SchemaLike | None = None,
        data_context: str | DataContext | None = None,
        record_uuid: uuid.UUID | None = None,
    ) -> None: ...

    def content_hash(self, hasher=None) -> ContentHash:
        if self._cached_content_hash is None:
            raise EmptyDataHashMissingError(self)
        return self._cached_content_hash

    def identity_structure(self) -> Any:
        raise EmptyDataAccessError(self, "identity_structure")

    # All payload-access methods raise EmptyDataAccessError:
    # as_dict, as_table, keys, schema, arrow_schema

    @property
    def cached_content_hash(self) -> ContentHash | None: ...

    @property
    def empty_source_info(self) -> dict[str, str | None] | None: ...
```

### 2. Exception types

**File:** `src/orcapod/errors.py` (extend existing file)

| Exception | Raised when | Key fields |
|---|---|---|
| `EmptyDataAccessError` | Payload-access method called on `EmptyData` | `empty_data`, `method_name` |
| `EmptyDataHashMissingError` | `content_hash()` on `EmptyData` with no cached hash | `empty_data` |
| `EphemeralResultMissingError` | Downstream cache miss + input is `EmptyData` | `tag`, `cached_content_hash`, `node_identity_path`, `message` |

`EphemeralResultMissingError` must carry enough context to identify which input was
lost and from which node, to make debugging tractable.

### 3. Tag table write path

**File:** `src/orcapod/core/nodes/function_node.py`  
**Method:** `FunctionJobNode.add_pipeline_record()`

Add `INPUT_DATA_HASH_COL` and `OUTPUT_DATA_HASH_COL` to `meta_table`. Both are stored
as `large_string` using `ContentHash.to_string()` (format: `"{method}:{hex_digest}"`).

```python
meta_table = pa.table({
    constants.DATA_RECORD_ID: ...,
    constants.NODE_CONTENT_HASH_COL: ...,
    constants.INPUT_DATA_HASH_COL: pa.array(          # NEW — large_string
        [input_data.content_hash().to_string()],
        type=pa.large_string(),
    ),
    constants.OUTPUT_DATA_HASH_COL: pa.array(         # NEW — large_string
        [output_data_hash.to_string() if output_data_hash else None],
        type=pa.large_string(),
    ),
    f"{constants.META_PREFIX}input_data{constants.CONTEXT_KEY}": ...,
    f"{constants.META_PREFIX}computed": ...,
    constants.IS_EPHEMERAL_COL: ...,
    _PIPELINE_BASE_ENTRY_ID_COL: ...,
    _PIPELINE_RECOMPUTATION_INDEX_COL: ...,
})
```

`OUTPUT_DATA_HASH_COL` is the key column: it stores the upstream output hash, which is
the correct lookup key for the downstream result cache. `INPUT_DATA_HASH_COL` is also
persisted for provenance. The read path exclusively uses `OUTPUT_DATA_HASH_COL` for
`EmptyData` token construction.

### 4. Tag table read path

**File:** `src/orcapod/core/nodes/function_node.py`  
**Method:** `FunctionJobNode._fetch_joined_records()`

**`_JoinedRecords` namedtuple extended:**

```python
class _JoinedRecords(NamedTuple):
    table: pa.Table
    taginfo_columns: tuple[str, ...]
    empty_data_tokens: dict[bytes, EmptyData]    # NEW: base_entry_id → EmptyData
```

**Ephemeral miss handling (replacing the silent drop):**

After the ephemeral join, compute unmatched rows via anti-join and convert each to an
`EmptyData` token:

```python
unmatched_ephemeral_df = ephemeral_taginfo_df.join(
    pl.DataFrame(eph_results) if eph_results is not None else pl.DataFrame(),
    on=constants.DATA_RECORD_ID,
    how="anti",
)

empty_data_tokens: dict[bytes, EmptyData] = {}
for row in unmatched_ephemeral_df.iter_rows(named=True):
    raw_hash = row.get(constants.INPUT_DATA_HASH_COL)
    if raw_hash is None:
        logger.warning(
            "Pipeline DB row missing %r column — EmptyData will have no cached hash; "
            "flow-through unavailable for this row. base_entry_id: %s",
            constants.INPUT_DATA_HASH_COL,
            row.get(_PIPELINE_BASE_ENTRY_ID_COL),
        )
        cached_hash = None
    else:
        cached_hash = ContentHash.from_string(raw_hash)

    empty_data_tokens[row[_PIPELINE_BASE_ENTRY_ID_COL]] = EmptyData(
        cached_content_hash=cached_hash,
        data_context=self.data_context,
    )
```

**`_load_cached_entries()` updated:**

After loading normal rows from `_fetch_joined_records().table`, merge `empty_data_tokens`:
for each base entry ID present in tokens but absent from the normal rows, yield
`(tag, EmptyData)`. The tag is reconstructed from the taginfo row for that base entry ID
using the same column-exclusion logic as today: the tag columns are all taginfo columns
whose names do not start with `constants.META_PREFIX`, excluding the internal
discriminator columns (`_PIPELINE_ENTRY_ID_COL`, `_PIPELINE_BASE_ENTRY_ID_COL`,
`_PIPELINE_RECOMPUTATION_INDEX_COL`, `constants.DATA_RECORD_ID`,
`constants.NODE_CONTENT_HASH_COL`, `constants.IS_EPHEMERAL_COL`,
`constants.INPUT_DATA_HASH_COL`). System-tag columns (prefix
`constants.SYSTEM_TAG_PREFIX`) are passed to the `Tag` constructor via the
`system_tags` argument; remaining columns become the primary tag data.

### 5. Downstream handling

**File:** `src/orcapod/core/nodes/function_node.py`  
**Methods:** `_process_data_internal()` and `_async_process_data_internal()`

```python
def _process_data_internal(self, tag, data):
    # Cache lookup — works unchanged for both Data and EmptyData.
    # EmptyData.content_hash() returns cached_content_hash, which matches
    # the INPUT_DATA_HASH_COL written by the original upstream execution.
    tag_out, result = self._cached_function_pod.process_data(tag, data)

    if result is not None:
        return tag_out, result

    # Cache miss
    if isinstance(data, EmptyData):
        # Any EmptyData on cache miss is treated as a legitimate ephemeral miss.
        # Rigorous upstream-ephemerality validation is deferred (follow-up issue).
        raise EphemeralResultMissingError(
            tag=tag,
            cached_content_hash=data.cached_content_hash,
            node_identity_path=self.node_identity_path,
            message=(
                "Downstream cache miss for EmptyData input — "
                "ephemeral result is gone and downstream has not yet computed "
                "a result for this input hash."
            ),
        )

    # Normal computation path (data is a real Data instance)
    return self._compute_and_store(tag, data)
```

The async counterpart (`_async_process_data_internal()`) gets the identical guard.

### 6. Old-format row handling

Old pipeline DB rows lacking `OUTPUT_DATA_HASH_COL` (rows written before this change):

- Read path emits a `WARNING`-level log per unmatched row.
- `EmptyData` is constructed with `cached_content_hash=None`.
- Any downstream attempt to call `content_hash()` on this token raises
  `EmptyDataHashMissingError` loudly.
- No forced migration. Backfill is deferred to ITL-535.

---

## Files Changed

| File | Change |
|---|---|
| `src/orcapod/core/datagrams/tag_data.py` | Add `EmptyData(Data)` |
| `src/orcapod/errors.py` | Add three exception types |
| `src/orcapod/core/result_cache.py` | Store `INPUT_DATA_HASH_COL` as `large_string` |
| `src/orcapod/core/cached_function_pod.py` | Add `lookup_cached_data()` method |
| `src/orcapod/core/nodes/function_node.py` | Write path, read path, downstream guard |
| `src/orcapod/protocols/pipeline_protocols.py` | Update `add_pipeline_record` signature |
| `tests/test_core/test_empty_data.py` | New unit tests for `EmptyData` |
| `tests/test_core/test_result_cache.py` | Extended: lookup via `EmptyData` |
| `tests/test_core/nodes/test_function_node_empty_data.py` | New integration tests |

---

## Follow-up Issues

1. **Configurable relaxation of strict `EmptyData` handling** — allow per-pipeline opt-in
   for graceful treatment of `EmptyData` from non-ephemeral upstreams (axes B/C).
2. **Rigorous upstream-ephemerality validation** in `_process_data_internal()` — check
   that `EmptyData` only arrives from pods declared `is_result_ephemeral=True`.
3. **Tag-row reconstruction from downstream cache evidence** — populate
   `empty_source_info`, write reconstructed tag rows with `record_id=None` on downstream
   cache hit. Requires resolving extension axes 3–6 from ITL-534.
4. **Old-format tag row backfill** — ITL-535.
