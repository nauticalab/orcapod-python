## `src/orcapod/core/cached_function_pod.py` / `src/orcapod/core/data_function.py`

### CFP1 — Extract shared result caching logic from CachedDataFunction and CachedFunctionPod
**Status:** resolved
**Severity:** medium

`CachedDataFunction` and `CachedFunctionPod` implement nearly identical result caching
logic: DB lookup by `INPUT_DATA_HASH_COL`, conflict resolution by most-recent timestamp,
record storage with variation/execution/timestamp columns, and a `RESULT_COMPUTED_FLAG`
meta column. The match tier / matching policy design (P6) will also need to apply to both.

**Fix:** Extracted `ResultCache` class (`src/orcapod/core/result_cache.py`) that owns the DB,
record path, lookup (with `additional_constraints` for future match tiers), store, conflict
resolution, and auto-flush logic. Both `CachedDataFunction` and `CachedFunctionPod` now
delegate to a `ResultCache` instance. The match tier strategy (P6) can be implemented once
on `ResultCache.lookup` via `additional_constraints`.
