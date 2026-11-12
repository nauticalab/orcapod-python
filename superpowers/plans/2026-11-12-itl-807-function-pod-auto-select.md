# Function pod `__call__` drops unused data columns (ITL-807, ITL-193) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `pod(a, b)` on a function pod drops the data columns the function does not take, by
inserting one automatic `SelectDataColumns` after the automatic join; `pod(..., strict=True)`
and `pod.process(...)` stay exact.

**Architecture:** One method, `_FunctionPodBase.__call__(*streams, label=None, strict=False)`.
It is graph construction only: it joins the inputs as `process` does today
(`handle_input_streams`), and when the joined stream carries data columns that are not
parameters it applies `SelectDataColumns(columns=<parameters present>)` (the select's default
`strict=True`, because the select's `strict` is in its identity) and hands the result to
`process`. `process` is not changed. No hashing code is touched.

**Tech Stack:** Python, pytest, `uv run`.

**Branch:** `eywalker/itl-807-docs-say-a-function-pod-extracts-the`. PR: `Fixes ITL-807`, `Fixes ITL-193`.

---

## Rulings (coordinator, relaying the developer, November 12, 2026)

1. Select goes AFTER the join: inputs -> automatic Join (several inputs) -> ONE automatic
   `SelectDataColumns` -> pod. This supersedes "per input before the join".
2. Any data column shared by two inputs fails at the Join as today, used or not ("that is on
   user"). No new handling or message.
3. If the function accepts arbitrary keywords (`**kwargs`), no automatic select. Today no
   data function can (`PythonDataFunction` rejects variadics at construction). Expressed as
   a property on the data function, `accepts_arbitrary_keywords` (base: `False`; wrapper
   delegates), read by the pod; no signature inspection in the pod. Do NOT add `**kwargs`
   support.
4. No select when the joined stream's data columns already match (including a parameter
   with a default that is absent). `pod(a, b)` with nothing to drop must have the same
   hashes as before this PR.
5. A stream passed for its tags: it is joined and its data columns are then dropped. Works
   with no new rule; test and document it.
6. `SideEffectPod` / `sink_pod` / `tap_pod`: unchanged. Operators: unchanged.
7. One info log per call naming the dropped columns of the joined stream. Not a warning.
8. Changelog: `### Changed` only, with a sentence that code relying on `pod(wide)` raising
   must use `strict=True` or `process`.
9. Docs / CLAUDE.md / .zed wording: "one automatic SelectDataColumns after the automatic join".
10. Out of scope, report only: `CachedFunctionPod.process` does not record the invocation
    with the tracker. Give a minimal reproduction in worker_done.

## Measured on main (25650c42)

- `SelectDataColumns` identity is `('SelectDataColumns', [sorted columns], strict)`.
- `SelectDataColumns(columns=[])(b)` -> `ValueError: No data columns found in the table`.
- Parameter with default: column present = used, absent = default.
- Hand-written `select -> pod` in a `PipelineJob`: nodes `{source, SelectDataColumns
  (OperatorJobNode), add (FunctionJobNode)}`; a changed unused column (source_id given) is a
  cache hit.

## Design of `__call__`

```python
def __call__(self, *streams, label=None, strict=False):
    # strict, or a function that takes arbitrary keywords (developer, Nov 12 2026): exact call
    if strict or self.data_function.accepts_arbitrary_keywords:
        return self.process(*streams, label=label)
    input_stream = self.handle_input_streams(*streams)      # the automatic join, as in process
    _, data_schema = input_stream.output_schema()
    parameters = self.input_data_schema                      # ctx argument already excluded
    dropped = [c for c in data_schema if c not in parameters]
    kept = [c for c in data_schema if c in parameters]
    if dropped and kept:
        # missing column / type errors are raised before a step is added
        self._validate_input_schema(Schema({c: data_schema[c] for c in kept}))
        logger.info(...)                                     # one message per call
        input_stream = SelectDataColumns(columns=kept)(input_stream)
    # nothing kept: no stream without data columns exists; process raises its exact error
    return self.process(input_stream, label=label)
```

`FunctionPod.__call__` stays as a typed override forwarding `strict`.
`process(input_stream)` with one stream does not join again, so the Join is recorded once.

## Tasks

### Task 1: failing tests
- [ ] Create `tests/test_core/function_pod/test_function_pod_auto_select.py` (public interface
  only). Cases: wide single input equals hand-written select (pipeline hash, content hash,
  `as_table(all_info=True)`); two inputs equal hand-written `Join` + select; nothing to drop:
  `pod(a, b)`, `pod(a, b, strict=True)`, `pod.process(a, b)` equal, hashes pinned to the values
  main gives; default parameter absent = no select; `strict=True` and `process` raise on an
  extra column; missing required column raises in all three modes; tags untouched;
  tags-only input works; shared data column (used and unused) raises at the Join; misspelt
  column for a defaulted parameter: dropped, default used, info log; exactly one INFO record
  per dropping call, none otherwise, no WARNING; ctx_arg pod; `CachedFunctionPod`;
  `WrappedFunctionPod`; `accepts_arbitrary_keywords` true -> no select (a test-local data
  function wrapper overriding the property; comment that it is about that property);
  `SideEffectPod.__call__` and operator `__call__` take no `strict`.
- [ ] PipelineJob cases: job with `pod(wide)` vs job with hand-written select over one
  database: same node labels and types, same function node hashes, same
  `db.list_table_paths()`, second job makes zero function calls; changed unused column =
  zero new calls; no select node when the input already matches; sync vs async orchestrator
  give equal tables; saved job files equal apart from run id.
- [ ] Run, confirm they fail (`TypeError: unexpected keyword 'strict'` / `ValueError`). Commit (WIP).

### Task 2: implementation
- [ ] `protocols/core_protocols/data_function.py`: `accepts_arbitrary_keywords` property.
- [ ] `core/data_function.py`: `DataFunctionBase.accepts_arbitrary_keywords -> False`;
  `DataFunctionWrapper` delegates.
- [ ] `core/function_pod.py`: `_FunctionPodBase.__call__` as above; `FunctionPod.__call__`
  forwards `strict`.
- [ ] Move `tests/test_core/test_newcomer_papercuts.py::test_extra_column_error_names_the_column_and_logs_nothing`
  to `pod.process(...)`; add a `strict=True` twin.
- [ ] Tests pass; ruff; basedpyright exit code 0. Commit.

### Task 3: docs and records
- [ ] `docs/concepts/function-pods.md`: tip and section "Input columns" rewritten; check
  `getting-started.md`, `operators.md`, `README.md`, `docs/index.md` for the old claim.
- [ ] `changelog.d/ITL-807.md` (`### Changed`), `design_issues/ITL-807-FP1.md` (resolved),
  CLAUDE.md + `.zed/rules` paragraph.
- [ ] Doc tests, mkdocs build, `scripts/design_issues.py --check`,
  `scripts/assemble_changelog.py --check`. Commit.

### Task 4: finish
- [ ] Full `uv run pytest tests/`, golden unchanged, ruff check/format, basedpyright.
- [ ] Merge origin/main if moved, push, PR, `orca linear attach`, `orca worktree set --pr`.
- [ ] CodeRabbit rounds (bodies too), CI green, worker_done with the CachedFunctionPod
  reproduction.

## Progress notes

- (keep updated)
