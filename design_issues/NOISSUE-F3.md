## `src/orcapod/core/function_pod.py`

### F3 — Dual URI computation paths in the class hierarchy
**Status:** resolved
**Severity:** low
`TrackedDataFunctionPod.uri` assembles the URI from `self.data_function.*` with its own lazy
schema-hash cache. `WrappedFunctionPod.uri` simply delegates to `self._function_pod.uri`. These
should agree (and do, after the `data_function` fix), but having two independent implementations
makes future changes fragile.

**Fix:** Removed `_output_schema_hash` cache from `_FunctionPodBase.__init__` and replaced the
`uri` property body with `return self.data_function.uri`. `_FunctionPodBase.uri` now delegates
to `DataFunctionBase.uri`, which owns the canonical hash computation and caching via
`output_data_schema_hash`.
