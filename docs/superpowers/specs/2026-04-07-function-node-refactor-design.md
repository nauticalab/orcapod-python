# FunctionNode Refactor Design

**Issue:** ENG-379
**Date:** 2026-04-07
**Status:** Approved for implementation

---

## Problem Statement

`FunctionNode` has two intertwined problems:

1. **Redundant logic** — the DB join (taginfo + results → join on `PACKET_RECORD_ID` → filter by content hash → reconstruct stream) is duplicated in at least four places: `get_cached_results()`, `iter_packets()` Phase 1, `async_execute()` Phase 1, and `_load_all_cached_records()`.

2. **Incorrect iteration semantics** — `iter_packets()` currently triggers computation for any input packet not yet in the DB (Phase 2). Iteration should be a strictly read-only operation that yields only already-computed results.

---

## Goals

- `iter_packets()` is strictly read-only: never triggers computation
- Computation is only triggered by an explicit `run()` call (or the orchestrator calling `execute()` / `async_execute()`)
- If no results exist and no DB records are present, iteration yields nothing
- All duplicated DB join logic is consolidated into a single internal helper
- Every internal method has a clear, non-overlapping purpose
- Tests verify that iteration alone does not cause computation side effects

---

## Design

### 1. In-Memory Result Store

`_cached_output_packets` changes key type from `dict[int, tuple]` to `dict[str, tuple[TagProtocol, PacketProtocol]]` keyed by **entry_id** (the output of `compute_pipeline_entry_id(tag, packet)`).

**Properties:**
- **Add-only between `clear_cache()` calls**: entries are inserted and existing entries are never removed until `clear_cache()` is called. When `_load_cached_entries()` returns entries for `entry_id`s already in the store, `dict.update()` will technically overwrite them — but this is safe and intentional, because any in-memory result and its DB-serialised counterpart for the same `entry_id` are always semantically equivalent. No pre-update existence check is needed.
- **Cleared only** by `clear_cache()` (triggered on staleness detection) or `attach_databases()`
- O(1) membership test: `entry_id in self._cached_output_packets`
- Preserves insertion order (Python 3.7+ dict), so iteration order is stable between calls in the same session

The derived caches `_cached_output_table` and `_cached_content_hash_column` are invalidated (set to `None`) whenever `_cached_output_packets` is modified.

### 2. New Internal Helper: `_load_cached_entries()`

Single replacement for all four duplicated DB join sites:

```python
def _load_cached_entries(
    self,
    entry_ids: list[str] | None = None,
) -> dict[str, tuple[TagProtocol, PacketProtocol]]:
    """Load entries from the pipeline DB + result DB.

    entry_ids=None  — loads all records for this node
    entry_ids=[...] — loads only those specific entry IDs

    Returns a dict of entry_id → (tag, packet).
    Does NOT mutate _cached_output_packets; the caller merges via update().
    Returns {} if either _cached_function_pod or _pipeline_database is None.
    """
```

All callers merge results: `self._cached_output_packets.update(loaded)` — **no `clear()` before `update()`**.

**Tag key derivation inside `_load_cached_entries()`:**

Reconstructing `(tag, packet)` pairs from the joined Arrow table requires knowing which columns are tag columns. Two modes:

- **When `_input_stream` is available** (FULL / READ_ONLY load status): use `self._input_stream.keys()[0]` — the authoritative list of user-facing tag column names.
- **When `_input_stream` is not used** (CACHE_ONLY / deserialized node): derive from `taginfo.column_names` by excluding all known internal prefixes and sentinels: `META_PREFIX`, `SOURCE_PREFIX`, `SYSTEM_TAG_PREFIX`, `PIPELINE_ENTRY_ID_COL` (the local sentinel `"__pipeline_entry_id"`), and `NODE_CONTENT_HASH_COL`. This mirrors the existing logic in `_load_all_cached_records()`.

**Columns dropped from the joined table before reconstructing packets:**

Drop: columns starting with `META_PREFIX`, the `PIPELINE_ENTRY_ID_COL` sentinel, and `NODE_CONTENT_HASH_COL`. Source columns (`SOURCE_PREFIX`) are **not** dropped — `ArrowTableStream` requires them for packet identity. This matches the drop lists in the existing `get_cached_results()` (line 764) and `iter_packets()` Phase 1 (line 1233).

**Filtering to `entry_ids`:** when `entry_ids` is provided, apply a polars filter on `PIPELINE_ENTRY_ID_COL` in the joined table before reconstruction.

This encapsulates:
- Fetching taginfo from `_pipeline_database` with `PIPELINE_ENTRY_ID_COL`
- Fetching results from `_cached_function_pod._result_database`
- Joining on `PACKET_RECORD_ID` via polars
- Applying `_filter_by_content_hash()`
- Restoring schema nullability
- Deriving tag keys using the strategy above
- Dropping internal columns (per list above) from the joined table
- Reconstructing `(tag, packet)` pairs via `ArrowTableStream`
- Filtering to `entry_ids` when provided

**`get_all_records()`** also contains an inline DB join but returns a raw `pa.Table` with configurable column filtering. It is **out of scope** for this refactor — its join logic serves a different responsibility and will be addressed separately.

### 3. `iter_packets()` — Strictly Read-Only

```
UNAVAILABLE → raise RuntimeError (no pod, no DB)

CACHE_ONLY  → if _cached_output_packets is empty:
                  loaded = _load_cached_entries()  # load all from DB
                  _cached_output_packets.update(loaded)
              yield from _cached_output_packets.values()

FULL / READ_ONLY →
    if is_stale: clear_cache()
    if _cached_output_packets is empty AND DB is attached:
        loaded = _load_cached_entries()  # hot-load from DB
        _cached_output_packets.update(loaded)
    yield from _cached_output_packets.values()
    # No computation. No input stream traversal. No _process_packet_internal().
```

**Contract:** calling `iter_packets()` on a node where `run()` has never been called and the DB has no records yields nothing. This is correct and intentional.

**Iteration order stability:** the order of `_cached_output_packets.values()` reflects insertion order. When loaded from DB via `_load_cached_entries()`, the order follows the Polars join output order. When populated by `_process_packet_internal()`, it follows the order packets were processed in `execute()`. Two successive calls to `iter_packets()` in the same session yield the same order because the second call serves from the already-populated in-memory store without re-querying.

### 4. `run()` — User-Facing Computation Entry Point

`run()` checks `load_status` before delegating, mirroring the guard already present in `iter_packets()`:

```python
def run(self) -> None:
    """Eagerly compute all input packets and persist results."""
    from orcapod.pipeline.serialization import LoadStatus
    if self._load_status == LoadStatus.UNAVAILABLE:
        raise RuntimeError(
            f"FunctionNode {self.label!r} is unavailable: "
            "no function pod and no database attached."
        )
    if self._load_status == LoadStatus.CACHE_ONLY:
        # Upstream is unavailable (CACHE_ONLY); computation requires a live
        # input stream. Iteration will serve existing results from DB.
        return
    self.execute(self._input_stream)
```

### 5. `execute()` — Sync Computation (Orchestrator + `run()`)

```
1. Observer: on_node_start (with tag_schema from input_stream)
2. Collect all (tag, packet, entry_id) from input_stream.iter_packets()
3. Selective DB reload for missing entries only:
       missing = [eid for _, _, eid in upstream if eid not in _cached_output_packets]
       if missing and DB attached:
           loaded = _load_cached_entries(missing)
           _cached_output_packets.update(loaded)
4. Compute truly missing entries:
       to_compute = [(tag, pkt, eid) for tag, pkt, eid in upstream
                     if eid not in _cached_output_packets]
       if _executor_supports_concurrent(self._packet_function) and to_compute:
           inline concurrent gather (see below)
       else:
           for tag, pkt, eid in to_compute:
               try:
                   _process_packet_internal(tag, pkt)
               except Exception as exc:
                   log warning; on_packet_crash; re-raise if error_policy=="fail_fast"
5. Build and return output — fire observer hooks for all packets:
       output = []
       for tag, pkt, eid in upstream:
           on_packet_start(node_label, tag, pkt)
           if eid in _cached_output_packets:
               tag_out, result = _cached_output_packets[eid]
               if result is not None:
                   on_packet_end(..., cached=(eid not in to_compute_eids))
                   output.append((tag_out, result))
           # packets that failed and were not stored are silently skipped here
6. on_node_end
7. return output
```

**Observer hooks:** `on_packet_start` and `on_packet_end` fire for **every** packet including cache hits, preserving existing behavior. `cached=True` for DB-loaded results, `cached=False` for freshly computed ones.

**Concurrent path** — when `_executor_supports_concurrent(self._packet_function)` is True:

```python
async def _gather():
    return await asyncio.gather(
        *[self._async_process_packet_internal(tag, pkt) for tag, pkt, eid in to_compute],
        return_exceptions=True,  # collect errors per-packet instead of stopping all
    )

try:
    asyncio.get_running_loop()
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        results = pool.submit(asyncio.run, _gather()).result()
except RuntimeError:
    results = asyncio.run(_gather())

# Per-result error handling (mirrors sequential error_policy logic)
for (tag, pkt, eid), result in zip(to_compute, results):
    if isinstance(result, BaseException):
        log warning; on_packet_crash(node_label, tag, pkt, result)
        if error_policy == "fail_fast":
            on_node_end(node_label, node_hash)
            raise result
    # successful results are already stored in _cached_output_packets
    # by _async_process_packet_internal()
```

The `return_exceptions=True` flag ensures per-packet error handling consistent with the sequential path's `error_policy` behavior.

### 6. `async_execute()` — Async Computation (Async Orchestrator)

Phase 1 (cache pre-load) is simplified to a single `_load_cached_entries()` call:

```python
cached_by_entry_id: dict[str, tuple] = {}
if self._cached_function_pod is not None:
    loaded = self._load_cached_entries()  # load all existing records
    self._cached_output_packets.update(loaded)
    cached_by_entry_id = loaded
```

Phase 2 (per-packet: cached or compute) is unchanged in logic: uses `_async_execute_one_packet()` for cache misses. The `supports_concurrent_execution` flag is not consulted in `async_execute()` — the TaskGroup already provides per-packet concurrency.

`_async_execute_cache_only()` is simplified: replace the `_load_all_cached_records()` call with `_load_cached_entries()`, drop the `(tag_keys, data_table)` unpacking, and iterate over the returned dict values directly.

### 7. `_process_packet_internal()` — Simplified

- `cache_index: int | None` parameter is **removed**
- Computes `entry_id` internally and stores by it:
  ```python
  entry_id = self.compute_pipeline_entry_id(tag, packet)
  self._cached_output_packets[entry_id] = (tag_out, output_packet)
  self._cached_output_table = None
  self._cached_content_hash_column = None
  ```
- Lines clearing `_cached_input_iterator` and `_needs_iterator` are **removed**
- All other logic (routing through `_cached_function_pod` vs `_function_pod`, calling `add_pipeline_record()`) is unchanged

`execute_packet()` delegates to `_process_packet_internal()` without `cache_index`. No change to `execute_packet()`'s signature or callers. After the call, `_cached_output_packets` contains the result keyed by entry_id. Tests asserting `len(node._cached_output_packets) == N` continue to hold.

`_async_process_packet_internal()` is updated identically.

### 8. `get_cached_results()` — Simplified

Kept as a public method with its existing signature. Rewritten to delegate to `_load_cached_entries()` with add-only semantics:

```python
def get_cached_results(
    self, entry_ids: list[str]
) -> dict[str, tuple[TagProtocol, PacketProtocol]]:
    if not entry_ids or self._cached_function_pod is None:
        return {}
    missing = [eid for eid in entry_ids if eid not in self._cached_output_packets]
    if missing:
        loaded = self._load_cached_entries(missing)
        self._cached_output_packets.update(loaded)  # add-only, no clear()
    return {
        eid: self._cached_output_packets[eid]
        for eid in entry_ids
        if eid in self._cached_output_packets
    }
```

**Behavior change vs current implementation:** the existing implementation calls `self._cached_output_packets.clear()` before repopulating. The new version does not clear. This is intentional: existing in-memory results are preserved. The existing test `test_get_cached_results_populates_internal_cache` manually clears `_cached_output_packets` before calling `get_cached_results()`, so it continues to pass.

---

## Method Inventory: Before → After

| Method | Verdict | Notes |
|---|---|---|
| `_ensure_iterator()` | **REMOVE** | Only served the old compute-while-iterate model |
| `_iter_packets_sequential()` | **REMOVE** | Computation leaves `iter_packets()`; dissolves |
| `_iter_packets_concurrent()` | **REMOVE** | Concurrent logic moves inline into `execute()` |
| `_iter_all_from_database()` | **REMOVE** | `iter_packets()` calls `_load_cached_entries()` directly |
| `_load_all_cached_records()` | **REMOVE** | Absorbed by `_load_cached_entries()` |
| `_load_cached_entries()` | **ADD** | Single DB join helper replacing 4 duplicates |
| `_executor_supports_concurrent()` | **KEEP / RELOCATE** | Module-level helper; moves from serving `iter_packets()` to `execute()` |
| `clear_cache()` | **SIMPLIFY** | Remove `_cached_input_iterator` and `_needs_iterator` clearing |
| `_process_packet_internal()` | **SIMPLIFY** | Remove `cache_index`; store by entry_id; remove iterator field resets |
| `_async_process_packet_internal()` | **SIMPLIFY** | Same as above |
| `_async_execute_cache_only()` | **SIMPLIFY** | Use `_load_cached_entries()` instead of `_load_all_cached_records()` |
| `get_cached_results()` | **SIMPLIFY** | Delegate to `_load_cached_entries()`; drop `clear()` |
| `execute()` | **REFACTOR** | Selective reload + concurrent path; per-packet observer hooks |
| `async_execute()` | **REFACTOR** | Phase 1 via `_load_cached_entries()` |
| `run()` | **REWRITE** | Load-status guard + `execute(self._input_stream)` |
| `iter_packets()` | **REWRITE** | Strictly read-only |
| `get_all_records()` | **OUT OF SCOPE** | Contains duplicate join but returns `pa.Table`; different responsibility |
| `execute_packet()` | **KEEP** | Signature unchanged; inherits entry_id keying via `_process_packet_internal()` |
| `_async_execute_one_packet()` | **KEEP** | Clean async helper used by `async_execute()` |
| `_filter_by_content_hash()` | **KEEP** | Used inside `_load_cached_entries()` |
| `_require_pipeline_database()` | **KEEP** | Guard; still needed |
| `add_pipeline_record()` | **KEEP** | Core provenance responsibility; unchanged |
| `compute_pipeline_entry_id()` | **KEEP** | Now also called inside `_process_packet_internal()` |

**State removed from `__init__`, `clear_cache()`, and `from_descriptor()`:** `_cached_input_iterator`, `_needs_iterator`

---

## Testing

### New test file: `tests/test_core/nodes/test_function_node_iteration.py`

1. `iter_packets()` on a fresh node with no DB and no `run()` call yields nothing
2. `iter_packets()` on a node with DB records (prior session) hot-loads and yields without calling `_process_packet_internal()` (mock assertion)
3. `iter_packets()` does not call `_process_packet_internal()` under any non-compute path (mock assertion covers FULL, READ_ONLY, CACHE_ONLY modes)
4. After `run()`, `iter_packets()` yields from `_cached_output_packets` without an additional DB query
5. `iter_packets()` called twice in the same session returns the same results and the same order; DB is only queried on the first call
6. `_cached_output_packets` is keyed by entry_id strings (not ints) after `run()`
7. `as_table()` on a fresh node with no `run()` and empty DB returns an empty table (0 rows, valid schema) — no computation triggered
8. `run()` on a CACHE_ONLY node is a no-op (returns without error, no computation, no exception)
9. `run()` on an UNAVAILABLE node raises `RuntimeError`
10. `execute()` concurrent path: `on_packet_crash` fires per failing packet when `error_policy="continue"`; exception propagates on first failure when `error_policy="fail_fast"` (mirrors `test_regression_fixes.py::TestConcurrentFallbackInRunningLoop` for the new `execute()` path)

### Existing tests requiring full rewrite

The following tests **use `iter_packets()` as the sole computation trigger** and will break after the refactor. They must be updated to call `run()` or `execute()` first, then call `iter_packets()` (or inspect `_cached_output_packets`) to verify results:

- `tests/test_core/function_pod/test_function_pod_node_stream.py`:
  - All of `TestFunctionNodeStreamBasic` (`test_iter_packets_yields_correct_count`, `test_iter_packets_correct_values`, `test_iter_is_repeatable`, `test_dunder_iter_delegates_to_iter_packets`)
  - `TestIterPacketsMissingEntriesOnly::test_partial_fill_total_row_count_correct` and `test_partial_fill_all_values_correct` — these rely on Phase 2 computing missing entries during iteration; after the refactor only the 2 pre-existing DB entries are returned, not all 4
  - `TestFunctionNodeStaleness::test_clear_cache_resets_output_packets` and `test_iter_packets_auto_detects_stale_and_repopulates`

- `tests/test_core/function_pod/test_function_pod_node.py`:
  - `TestFunctionNodeStreamInterface::test_iter_packets_correct_values` and any other test calling `iter_packets()` without a prior `run()`

- `tests/test_core/function_pod/test_function_node_attach_db.py`:
  - `test_attach_databases_clears_caches` (calls `list(node.iter_packets())` and asserts `len > 0`)

- `tests/test_data/test_polars_nullability/test_function_node_nullability.py`:
  - Any test calling `node._iter_all_from_database()` directly — this method is removed; replace with `node._load_cached_entries()` or test via `iter_packets()` after DB population

- `tests/test_core/test_regression_fixes.py`:
  - `TestConcurrentFallbackInRunningLoop` — tests the removed `_iter_packets_concurrent` behavior; replace with a test targeting the concurrent path in `execute()` (covered by new test 10 above)

### Existing tests requiring minor updates (key type change only)

- `test_function_node_get_cached.py`: verify `_cached_output_packets` key type is `str`; `test_get_cached_results_populates_internal_cache` still passes with add-only semantics
- `test_node_execute.py`: size assertions hold; verify keys are entry_id strings
- `test_function_pod_node_stream.py` (tests not listed above): size assertions hold; key type changes
- Any test referencing `_cached_input_iterator`, `_needs_iterator`, `_ensure_iterator`, `_iter_packets_sequential`, `_iter_packets_concurrent`, or `_load_all_cached_records` — update or remove

---

## Out of Scope

- Concurrent execution control flag/argument for `execute()` → tracked in **ENG-381**
- Refactoring `get_all_records()` to use `_load_cached_entries()` (different return type / responsibility)
- Refactoring other node types (`OperatorNode`, `SourceNode`)
- Lazy evaluation / deferred computation patterns
