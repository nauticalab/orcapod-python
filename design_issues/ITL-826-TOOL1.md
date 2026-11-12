## Tooling

### TOOL1 — Type-check debt recorded in the basedpyright baseline
**Status:** open
**Severity:** medium
**Issue:** ITL-826

The org required CI runs `basedpyright` in its default (`recommended`) mode. The code was
never type-checked, so ITL-826 recorded the existing errors in
`.basedpyright/baseline.json` instead of fixing them: the check passes and any new error
fails it. Baselined errors after the ITL-826 lint fixes and the ITL-825 merge: 1995 in
total (tests/ 1238, src/ 669, test-objective/ 84, examples/ 3, superpowers/ 1); the baseline
also records 30,279 warnings, which are reported but do not fail the check
(`failOnWarnings = false`). Largest error rules: `reportArgumentType`,
`reportMissingTypeArgument`, `reportAttributeAccessIssue`.

Pay it down directory by directory, starting with `src/`, and regenerate the baseline only
to remove fixed entries. Known items worth fixing first: `orcapod.hashing` exports nine
names from a `legacy_core` module that no longer exists, so they are always `None`
(`tests/test_hashing/generate_pathset_data_hashes.py` would call one); and 26 import
cycles reported in `src/`.

Protocols that lag their concrete classes. These baselined errors are not runtime bugs (the
concrete classes have the method or parameter), but the protocols misdescribe what callers
rely on. Paths are under `src/orcapod/`:
- `FunctionPodProtocol.process_data` / `async_process_data` lack the `logger` / `run_id`
  keyword arguments that callers pass: `core/cached_function_pod.py:138,189`,
  `core/nodes/function_node.py:1429,1566`.
- `StreamProtocol` lacks `data_context`, which operators use:
  `core/operators/batch.py:99`, `core/operators/group_by.py:217`,
  `core/operators/index.py:98`, `core/operators/pick.py:104`,
  `core/operators/merge_join.py:252`.
- `ArrowDatabaseProtocol` lacks `list_sources` (`pipeline/logging_observer.py:247`,
  `pipeline/status_observer.py:199`) and `to_config(db_registry=)`
  (`pipeline/logging_observer.py:278`, `pipeline/status_observer.py:231`).
- `DataFunctionProtocol` lacks `data_context`:
  `core/cached_function_pod.py:145,150,196,201`.
