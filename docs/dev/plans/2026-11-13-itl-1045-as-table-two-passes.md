# ITL-1045 plan: as_table never mixes two computations

Bug: `as_table` used `_cached_output_table` without a staleness check; the content-hash pass
(`iter_data`) cleared the stale cache and recomputed. Fix: `if self.is_stale: self.clear_cache()`
at the top of `FunctionPodStream.as_table` and `FunctionNodeBase.as_table` (the cached pod stream
inherits). Tests: tests/test_core/function_pod/test_as_table_stale_between_passes.py (pod
stream, cached pod stream, head/iter_data, job node latent). Job node: latent (results come from
the store; checked unfixed: aligned).
