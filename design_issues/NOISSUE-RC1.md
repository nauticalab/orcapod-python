## `src/orcapod/core/cached_function_pod.py` / `src/orcapod/core/result_cache.py`

### RC1 — `ResultCache.record_path` not overridable via public API
**Status:** open
**Severity:** medium

When loading a pipeline with a `DataFunctionProxy` (PLT-931), the
`CachedFunctionPod`'s `record_path` may differ from the original because the
pod-level URI is recomputed from a deserialized schema (whose semantic hash
may diverge from the original due to the `Python type → Arrow string → Python type`
round-trip). The workaround reaches into private fields:
`node._cached_function_pod._cache._record_path = stored_path`.

**Fix:** Add a public `override_record_path(path)` method to `CachedFunctionPod`
(or `ResultCache`) that validates and sets the record path. Alternatively, accept
an optional `record_path_override` in `CachedFunctionPod.__init__` that takes
precedence over the computed path when provided.
