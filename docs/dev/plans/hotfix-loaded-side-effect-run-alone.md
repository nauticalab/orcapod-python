# Hotfix: loaded side-effect node passes run_alone the unchecked source ids

Cause: #384 (ITL-1102) made `run_job_node`'s `run_alone` take the unchecked source ids; #381
(ITL-1094) `LoadedSideEffectJobNode.run` still passes a zero-argument lambda (basedpyright error,
TypeError if reached).

Plan:
1. Failing test: a `LoadedSideEffectJobNode` over a non-job stream (an `ArrowTableSource`
   stream) runs `run(load_only=True)`; this reaches `run_alone`.
2. Fix: `lambda _unchecked: ...`, as the live `SideEffectJobNode.run` does (the ids are unused).
3. basedpyright, ruff, targeted tests, changelog check; PR.
