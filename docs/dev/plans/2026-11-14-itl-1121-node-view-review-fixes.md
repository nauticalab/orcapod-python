# ITL-1121: job node view review fixes, implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan
> task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the defects a full CodeRabbit review of PR #381 found in PR #380's code (the job
node current-run view, ITL-1095), in a PR stacked on #380, without new node-view behaviour and
without any hash, golden or stored-table change.

**Architecture:** Each fix goes where the defect lives: the function node's run paths (stamp
before `on_node_end`), the operator node's `run()` (lazy materialization inside the clearing
`try`), `RunSummary` (headline from what the run plan ran), the side-effect node's sync
load-only branch (clear on failure), and one load-only guard in `pipeline/node_run.py` that
refuses an input which delivers when read.

**Tech Stack:** Python 3.11+, pytest, `uv run`.

Branch: `eywalker/itl-1121-job-node-view-itl-1095-defects-found-by-a`, based on PR #380 head
`7de3512b`. Draft PR against `main`, top line "Stacked on #380: merge after it. Fixes ITL-1121".

Reproduction scripts: `superpowers/scripts/itl_1121/c1..c6_*.py` (committed).

## Coordinator decisions (November 14, 2026)

| # | Reproduced | Decision |
|---|---|---|
| 1 | yes, sync / async / load-only sync / load-only async | fix; observer test on all four paths + the node holds its rows after the run |
| 2 | yes (no DB, `CacheMode.OFF`) | fix |
| 3 | no (node.run commits per row; evidence c3) | no change; reply with evidence |
| 4 | yes, side-effect AND operator node cleared below a held function node | fix: `RunSummary(..., ran_nothing=...)`; tests: side-effect cleared, operator cleared, nothing ran, job with no function node; sync, async, node.run() |
| 5 | yes, through `execute()` only | fix (as async) |
| 6 | yes; general rule for outside streams | (a): a load-only run refuses, before reading, an input that would deliver (`SideEffectPodStream`), all three node kinds, direct input and cascade. A computing outside stream (FunctionPodStream) stays: write its reproduction + 3 lines for the coordinator |
| 7 | ITL-1097 | no fix; point to it |
| 8 | yes | docs: held node delivers nothing; `clear_cache()` first |
| 9 | - | recording function in `test-objective/integration/test_caching_flows.py` |

## Files

- Modify `src/orcapod/core/nodes/function_node.py`: `execute` (~2300-2322), `_execute_load_only`
  (~1731-1733), `_async_execute_load_only` (~1786-1788), `async_execute` (~4003-4007);
  `run_job_node` (~306-312, guard 6).
- Modify `src/orcapod/core/nodes/operator_node.py`: `run` (~1540-1580, fix 2 and guard 6).
- Modify `src/orcapod/pipeline/run_summary.py`: `RunSummary.__init__`, `_headline`.
- Modify `src/orcapod/pipeline/job.py`: `_finish_run` (`RunSummary(...)`).
- Modify `src/orcapod/pipeline/node_run.py`: `run_with_dependencies` (`RunSummary(...)`, guard 6),
  new `refuse_delivering_inputs(node)`.
- Modify `src/orcapod/side_effects.py`: `SideEffectJobNode.execute` load-only branch; `run` uses
  `run_job_node` (guard covered there).
- Modify `docs/concepts/function-pods.md` (~836-842).
- Modify `test-objective/integration/test_caching_flows.py` (`test_second_run_uses_cache`).
- Create `tests/test_pipeline/test_node_view_review_fixes.py` (all regression tests).
- Create `changelog.d/ITL-1121.md` (### Fixed), `design_issues/ITL-1121-NV1.md`.

## Task 1: stamp before `on_node_end` (comment 1)

- [ ] Test: observer subclassing `NoOpObserver` (contextualize returns self) reads
  `job.nodes[label].as_table().num_rows` in `on_node_end`; parametrize orchestrator
  sync/async and load_only False/True (load_only: a first job over the same store stores the
  results). Assert read == 3 in the hook, and after the run `as_table().num_rows == 3` and
  `holds_current_run(load_only=load_only)`.
- [ ] Run: fails (0 rows).
- [ ] Fix: in each of the four paths, call `self._update_modified_time()` right after
  `self._set_run_rows(...)` and before `ctx_obs.on_node_end(...)`; delete the later stamp. A
  raising observer still clears the node (the except branches call `clear_cache()`).
- [ ] Run the test and `tests/test_pipeline/test_node_staleness_after_run.py`,
  `test_job_node_current_run_view.py`, `test_observer.py`: pass. Commit `fix(function_node): ...`.

## Task 2: operator lazy materialization inside the clearing try (comment 2)

- [ ] Test: `OperatorJobNode(MapData({"y": "z"}), (flaky.pod(base),), pipeline_database=db,
  cache_mode=CacheMode.OFF)` for `db` None and `InMemoryArrowDatabase()`; the pod raises:
  `run()` raises; then `holds_current_run(load_only=False) is False`, `is_stale`, and
  `as_table().num_rows == 0`; after the pod recovers `run()` gives 2 rows.
- [ ] Fix: in `OperatorJobNode.run`, build the view inside the `try` for the branch that
  computed (`return self._run_view(computed=computed)` moved into the `try` when
  `computed`), keeping `except BaseException: self.clear_cache(); raise`.
- [ ] Run test + `tests/test_core/operators/test_operator_node*.py`. Commit.

## Task 3: summary headline from what ran (comment 4)

- [ ] Tests (sync, async; and `node.run()`):
  - function -> tap; run; `tap.clear_cache()`; run: delivered 3 and the headline is not
    "nothing to do" (`"Run of job 'j': all rows succeeded."`).
  - function -> select (operator); `op.clear_cache()`; run: headline not "nothing to do".
  - nothing ran: second run of an unchanged job says "nothing to do".
  - a job with no function node (source -> select): a second run says "nothing to do"; the
    first says "all rows succeeded".
- [ ] Fix: `RunSummary.__init__(..., ran_nothing: bool = False)` stored as `self.ran_nothing`
  (documented in Attributes and Args); `_headline` returns the "nothing to do" line iff
  `self.ran_nothing`. Callers: `job._finish_run` and `node_run.run_with_dependencies` pass
  `ran_nothing=not plan.to_run`.
- [ ] Run test + `tests/test_pipeline/test_job_run_state_rule.py test_job_run_summary.py
  test_node_run_cascade.py`. Commit.

## Task 4: sync load-only execute clears on failure (comment 5)

- [ ] Test: job `rowwise -> tap -> down`; `job.run()`; `tap.execute(failing, load_only=True)`
  raises; `tap.is_stale` and `down.is_stale`, both read 0 rows. (`execute` is the node's
  protocol entry point; this test is about it: one-line comment.)
- [ ] Fix: wrap the load-only branch in `try: ... except BaseException: self.clear_cache(); raise`.
- [ ] Commit.

## Task 5: a load-only run refuses an input that delivers (comment 6)

- [ ] Tests: standalone `SideEffectJobNode`, `FunctionJobNode`, `OperatorJobNode` over a
  `SideEffectPodStream`: `run(load_only=True)` raises `ValueError` and nothing is delivered;
  cascade: a job node over a standalone node over a `SideEffectPodStream`... (built as nodes
  outside a job, `FunctionJobNode(pod, SideEffectJobNode(...))`, only if constructible; else
  the direct cases plus the cascade through `run_with_dependencies` with a job node input).
  Full runs are unchanged (deliver as before).
- [ ] Fix: `node_run.refuse_delivering_inputs(node)` raises `ValueError` naming the node and
  the input when an upstream is a `SideEffectPodStream`. Called when `load_only` in
  `run_job_node` (alone path, before `run_alone`), in `OperatorJobNode.run` (alone path), and in
  `run_with_dependencies` for every node of `plan.to_run` before the orchestrator starts.
- [ ] Write c7 reproduction for FunctionPodStream computing in a load-only run (already in c6)
  and three lines for the coordinator.
- [ ] Commit.

## Task 6: docs (8), test-objective (9), changelog, design issue

- [ ] `function-pods.md`: the sample comment `node.run()  # holds a current run: delivers
  nothing`; the bullet: with `track_completion=False`, a run delivers every row, but a node
  that holds a current run is not run again: `clear_cache()` it first. Mention the load-only
  refusal sentence in the load-only paragraph if one exists.
- [ ] `test_caching_flows.py`: recording `_double`, assert no call during `node2.run()`.
- [ ] `changelog.d/ITL-1121.md` (### Fixed), `design_issues/ITL-1121-NV1.md` (resolved).
- [ ] `uv run python scripts/assemble_changelog.py --check`, `scripts/design_issues.py --check`,
  docs tests. Commit.

## Task 7: finish

- [ ] ruff format/check, basedpyright exit code, full `uv run pytest tests/` and
  `test-objective/`. Push, draft PR, `orca linear attach`, `orca worktree set --pr`.
- [ ] Reply on the nine CodeRabbit threads/review-body findings on #381 (signed).
- [ ] CI green, CodeRabbit round on the new PR addressed; worker_done.

## Progress notes

- 2026-11-14: reproduction table sent and approved (see decisions). Scripts committed (86902ec0).
- 2026-11-14 (session stopped at usage limit): DONE and pushed: fixes 1, 2, 4, 5, 6 (each its own
  commit, tests in tests/test_pipeline/test_node_view_review_fixes.py, 32 pass), docs 8, test 9.
  Fix 6 lives in node_run.plan_run (refuse_delivering_inputs) because plan_run reads outside
  streams as held rows; plus run_job_node / OperatorJobNode.run alone paths.
  Observation for the coordinator: a node.run() of a node holding a current run makes no summary;
  node.last_run stays the previous run's (existing #380 behaviour, not changed).
  LEFT: changelog.d/ITL-1121.md (### Fixed), design_issues/ITL-1121-NV1.md, the FunctionPodStream
  three lines for the coordinator (repro in superpowers/scripts/itl_1121/c6_*.py), basedpyright,
  full `uv run pytest tests/` + test-objective, draft PR (top line "Stacked on #380: merge after
  it. Fixes ITL-1121"), orca linear attach / worktree set --pr, signed replies on the nine #381
  findings (3: evidence c3; 7: ITL-1097), CI green, CodeRabbit round, worker_done.
- 2026-11-14 (resumed): changelog, design issue, draft PR #388, #381 replies (6 threads + 1 top-level),
  CodeRabbit round 1 on #388 fixed (C, D/F, H, I; 75324abc) and answered (A, B, E, G, J, K, L),
  KeyboardInterrupt log-commit fix (c9). Developer decision: a load-only run also refuses a
  computing outside stream (d06ffcf7, node_run.refuse_unloadable_inputs). main merged (#382, #383).
  LEFT: name the coordinator's follow-up issue in the reply to finding G (thread 4574191943);
  CI and a CodeRabbit review of the head; worker_done.

## Merge of main after #380 merged (task_f017ca8a173c)

#380 merged as e4b4319c (it had merged #375 ITL-912 typed zero-row tables and #387 ITL-1111
schema memo). Steps:
- [x] `git merge origin/main` (no rebase); resolve function_node.py, operator_node.py,
  side_effects.py, node_run.py, job.py, CLAUDE.md, .zed/rules keeping every #388 fix and main's
  #375 / #387 behaviour.
- [x] Doc fix: CLAUDE.md / .zed/rules "A failed end-of-run commit clears every node of the run"
  -> only the nodes that ran (`run_with_dependencies` -> `commit_run(stores, plan.to_run)`).
- [x] Re-run every script in superpowers/scripts/itl_1121/: each shows the fixed behaviour.
- [x] Tests: tests/test_pipeline, tests/test_core/{nodes,operators,function_pod,side_effect_pod},
  golden (3.12; async files also 3.11; one process).
- [ ] One push; CI green; one signed `@coderabbitai review`; plan to coordinator before code.
- 2026-11-14 (merge task): merged #380's final head 8c44199b first (1aa7ca2f, no conflicts:
  the true base), then origin/main e4b4319c (6b5db050; main's tree equals 8c44199b, so every
  conflict of the squash took this branch's side). Doc fix 8455c2f0. All itl_1121 scripts show
  the fixed behaviour. Tests: pipeline/nodes/operators/function_pod/side_effect_pod/golden on
  3.12 6434 passed 11 skipped 9 xfailed; async files + review-fix tests on 3.11 651 passed.
  LEFT: CI, one CodeRabbit review, plan to coordinator, worker_done.
- 2026-11-14: CodeRabbit 4575098786 (5dd8d6a3 round): #380 behaviour, also on main; declined with repro c11 (coordinator files it); _streams_read_by docstring corrected. CR review at 21:07Z rate limited; retries ~21:28Z, ~21:48Z.
- 2026-11-14: merged #390 (653c138e; one conflict in OperatorJobNode.run, kept _compute_output + _run_load_only) and #389 (810d1b03, clean). Scripts unchanged. 3.12 targets 9348 passed; async 3.11 651 passed. CR retry 1 at 21:38Z rate limited; last retry ~21:58Z.
- 2026-11-14: CodeRabbit round 2 (review 5907415238 of 9fc663d7): A 4575874462 (ArrowTableStream upstreams over-refused) and B 4575874471 (blueprint SideEffectNode delivered in a load-only run) reproduced (c12), approved, fixed in one commit with 13 tests. 3.12 targets 9438 passed; 3.11 async 664 passed.
