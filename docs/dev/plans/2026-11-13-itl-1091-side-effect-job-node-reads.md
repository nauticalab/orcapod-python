# ITL-1091: a read of a side-effect job node delivers nothing — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan
> task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** After `job.run()`, every read of a `SideEffectJobNode` (`iter_data`, `async_iter_data`,
`as_table`, the dataframe methods, `flow`, `head`, the preview, and a downstream node's read)
returns the rows the run passed through and delivers nothing; `node.run()` does the work
`job.run()` does for that node.

**Architecture:** `SideEffectJobNode` keeps the rows `execute` / `async_execute` passed on
(`_held_rows`, in memory, as `FunctionJobNode` keeps `_cached_output_datas`) and stamps itself
modified at the end of a run. Its `iter_data` yields those rows while it is not stale
(`StreamBase.is_stale`, the judgement function job nodes use) and nothing otherwise. `run()`
calls `execute(self._input_stream)` (log lookup and write as a job run does). `head()` shows the
held rows. The blueprint `SideEffectNode` and the standalone `SideEffectPodStream` are unchanged.

**Tech Stack:** Python, PyArrow, pytest, uv.

## Decisions (coordinator, November 13, 2026, developer option 1)

- (a) Rows held in memory by the run. Not rebuilt from the log (tdb_v2 holds no rows).
- (b1) No held rows (no run in this process, held rows stale, or the last run raised): a read
  delivers nothing and returns zero rows with the node's columns; `head()` stays
  `NOT_COMPUTED` with `SIDE_EFFECTS_NOTE`.
- (c) `node.run()` = `job.run()`'s work for this node: with `track_completion=True` it delivers
  only rows the log does not record; with `False` it delivers every row again. Docstring says so.
- (d) Held rows = `execute`'s return list (today's row set under `on_error="log"` and
  `drop_on_failure`); cleared at the start of a run, so a run that raises leaves none.
- (e) `head()`/preview after a run show the held rows (status `ROWS`, origin `held`).
- Conditions: stale test (upstream change after a run → zero rows, no delivery); downstream
  function node `down.run()` / `down.as_table()` → 0 new deliveries; async held rows are exactly
  this run's rows (ITL-1093 is fixing function/operator nodes after an async run); loaded-job
  finding (side-effect nodes vanish from `PipelineJob.load`) goes to worker_done only.
- No change to `_table_of_rows` (PR #375 touches it); no ordering dependency.

## Diagnosis (b4fde330), cumulative deliveries over 6 rows, same on both orchestrators

| track_completion | job.run | head | preview | iter_data | async_iter_data | as_table | as_polars_df | as_pandas_df | flow | node.run | job.run #2 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| False | 6 | 6 | 6 | 12 | 18 | 24 | 30 | 36 | 42 | 48 | 54 |
| True | 6 | 6 | 6 | 12 | 18 | 24 | 30 | 36 | 42 | 48 | 48 |

Downstream `down.run()` after `job.run()` delivered 6 more (track=True).

## Files

- Modify: `src/orcapod/side_effects.py` (`SideEffectJobNode`; `SideEffectNode.run` docstring)
- Modify: `src/orcapod/core/streams/base.py` (`StreamBase.run` docstring)
- Modify: `src/orcapod/core/streams/view.py` (`StreamView` docstring)
- Create: `tests/test_core/side_effect_pod/test_side_effect_job_node_reads.py`
- Modify: `tests/test_core/streams/test_run.py` (job-node case of the "delivers as as_table" test)
- Modify: `docs/concepts/streams.md`, `docs/concepts/function-pods.md`
- Modify: `CLAUDE.md`, `.zed/rules`
- Create: `changelog.d/ITL-1091.md`, `design_issues/ITL-1091-SE1.md`

### Task 1: failing tests

- [ ] Write `tests/test_core/side_effect_pod/test_side_effect_job_node_reads.py`: public
  interface only (`PipelineJob`, `SideEffectPod`, `job.nodes[...]`). Cases:
  every read after `job.run()` delivers nothing (param: orchestrator sync/async x
  track_completion True/False), returned rows equal the source rows (pass-through) and the
  downstream node's input tags; `node.run()` delivers 0 (True) / 6 (False); second `job.run()`
  unchanged; head after run shows rows; read before any run (fresh job on populated DB) → 0
  rows, 0 deliveries, head NOT_COMPUTED; on_error="log" drop True/False row sets; on_error raise
  → read returns 0 rows; stale after an upstream function node re-runs → 0 rows, 0 deliveries;
  downstream `down.run()` / `down.as_table()` → 0 deliveries; async run twice → held rows are 6.
- [ ] Run (py3.12) and see them fail.

### Task 2: implementation in `SideEffectJobNode`

- [ ] `_held_rows: list[(tag, data)] | None` in `__init__`.
- [ ] `execute`: clear at start, set to results at the end, `_update_modified_time()`.
- [ ] `async_execute`: clear at start, collect each row sent, set and stamp after the
  TaskGroup and before `output.close()` (ITL-1042 ordering).
- [ ] `_current_rows()`: drop held rows when `is_stale`; return them or `None`.
- [ ] `iter_data`: yield `_current_rows() or ()`; never delivers.
- [ ] `head`: held rows → `StreamView.of_rows(... as_table(columns=config).slice(0, n) ...,
  origin="held")`; else rowless NOT_COMPUTED + `SIDE_EFFECTS_NOTE`.
- [ ] `run`: `execute(self._input_stream)` then `_view_of_rows(self.as_table(), "held")`.
- [ ] Run tests (3.12 and 3.11 for async), plus `tests/test_core/side_effect_pod`,
  `tests/test_core/streams`, side-effect pipeline tests.

### Task 3: docs, CLAUDE.md/.zed/rules, changelog fragment, design issue entry

### Task 4: golden suite unchanged; ruff, basedpyright; full suite once before PR (CI runs it)

### Task 5: draft PR, CI, CodeRabbit rounds, worker_done

## Progress notes

- 2026-11-13: diagnosis sent, design approved by coordinator.
- 2026-11-13: implemented (Tasks 1-3 done); draft PR #377 opened (head 1e3d7904);
  waiting for CI, then ready-for-review, CodeRabbit rounds, worker_done.
- 2026-11-13: CodeRabbit round 1 (1e3d7904, 922fede6): 4 inline accepted (docs wording,
  node.run() flushes the log, send lock in async_execute, head() guard for an empty run whose
  schema needs a poll), 1 body style hint declined. Plan approved by coordinator.
