## `src/orcapod/core/cached_function_pod.py` — pod-level result cache (ITL-988)

### CP1 — a pod-cached function pod was not recorded in a job, and its table showed cache state
**Status:** resolved
**Severity:** high
**Issue:** ITL-988

`CachedFunctionPod.process` never recorded the invocation with the tracker, so a pod created with `pod_cache_database=` and applied in a `PipelineJob` left the job with no nodes; it also joined its inputs twice. Its stream's `as_table()` carried `__computed` for computed rows and `__pf_var_*` / `__pf_exec_*` / `__pod_ts` for rows served from the cache, so meta columns leaked into the plain table and the columns depended on cache state.

**Fix:** `process` mirrors `FunctionPod.process` (join once, validate, record, return), `CachedFunctionPod.to_config` serializes the inner pod so a job saves as it does for an uncached pod, and the stream keeps only `__computed` and drops meta columns unless asked for. The pod cache database is not saved in a job, so a loaded job does not use it. `CachedFunctionPod` still has no `from_config` (it cannot be rebuilt without its cache database).
