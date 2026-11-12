# ITL-988 — pod-cached function pod in a PipelineJob; plain table independent of cache state

Branch `eywalker/itl-988-a-pod-cached-function-pod-is-not-recorded`. Only `src/orcapod/core/cached_function_pod.py` changes (plus tests, changelog fragment, design_issues file, docs sentence).

## Decisions (coordinator, 2026-11-12)
- Defect 1: `CachedFunctionPod.process` mirrors `FunctionPod.process`: join once, `_validate_input_schema`, `tracker_manager.record_function_pod_invocation(self, input_stream, label=label)`, return the stream. Record SELF (the pod cache is honoured: a row in the pod cache is not recomputed in the job). Job save needs `CachedFunctionPod.to_config` delegating to the inner pod's config (identical saved file to the uncached pod, apart from `run_id`). The pod cache database is not saved, so a loaded job does not use it.
- Defect 2: `process` returns a `FunctionPodStream` subclass in this file: it strips the stored `__pf_var_*`/`__pf_exec_*`/`__pod_ts` meta from served rows (keeps `__computed`), and `as_table` drops meta columns unless `meta` is requested. `process_data` is untouched (FunctionNode and hooks read `__computed` from it).
- #327 only changes `_FunctionPodBase.__call__`; process receives the already-narrowed stream. Re-check after it merges.

## Facts measured on main 25650c42
- Uncached node: ph a1b47334ae8c, ch d4bc20dfcee4; record SELF gives the same, same store paths; save of SELF fails (no `to_config`).
- Table columns: computed `[k,total,__computed]`; served `[k,total,__pf_*...,__pod_ts,__computed]`; uncached `[k,total]`.

## Conditions to test
(a) saved file equals uncached save except `run/run_id`; (b) save+load in memory and Delta, sync and async, loaded job reads stored results without pod cache; (c) docs/changelog state pod cache DB is not saved; (d) report from_config/proxy paths that cannot round-trip; (e) a row is computed once, appears in pod cache and job result table; second run computes nothing.
Fallback if any fails: record INNER plus info log; send facts to coordinator.

## Steps
1. Tests `tests/test_core/function_pod/test_cached_function_pod_job.py` (fail first).
2. Implement. 3. Full suite, ruff, basedpyright, golden. 4. changelog.d/ITL-988.md, design_issues/ITL-988-CP1.md, docs. 5. Merge origin/main, push, PR.
