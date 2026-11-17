## `src/orcapod/core/data_function.py`

### P7 — `PythonDataFunction.__init__` unconditionally extracts git info
**Status:** open
**Severity:** medium

`PythonDataFunction.__init__()` (line ~324) always calls `get_git_info()`, which runs git
subprocess commands. This fails or significantly slows initialization in non-git environments
(CI containers, notebooks, deployed services).

`# TODO: turn this into optional addition`

Fix: add an `include_git_info=True` parameter; skip extraction when `False`.
