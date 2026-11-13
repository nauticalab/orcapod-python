# ITL-1001 crash-recovery flake after PR #347 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `test_crash_mid_batch_keeps_committed_batches[async]` proves that committed batches survive a crash mid-batch, with no assumption about the order in which rows finish and no wait that can expire under load.

**Architecture:** Test-only change in `tests/test_pipeline/test_job_crash_recovery.py`. The assertions compare sets of rows read from the store (rows with a committed pipeline record, rows with a committed result) with the rows the rerun computes. A new async case forces a row before 200 to finish after the second commit, using the same event as the crash row and no clock.

**Tech Stack:** pytest, subprocess job script, deltalake.

---

## Cause (established 2026-11-13)

CI job 131810636077 (`pytest (3.13)`, run 43403086940, PR #346 head `126ef033`) failed at
`test_job_crash_recovery.py:228`: `min(calls) >= 200` was false. Every earlier assertion
held: crash exit 3 (so the bounded wait of #347 did not time out), `records == 200`,
`200 <= results < 234`, rerun exit 0, `len(calls) == 250 - results`.

Category (b): the event was set and the counts were right. The failure is the test's
assumption that the two committed batches are input rows 0..199.

- `FunctionPod.async_execute` (`src/orcapod/core/function_pod.py`) starts one task per
  row. Each one runs the sync function on the default thread pool
  (`data_function.py`, `run_in_executor(None, ...)`), and sends its result on completion.
- `FunctionNode`'s record stage (`record_and_forward` → `_row_processed`) takes rows in
  completion order and commits every `commit_every_rows` rows.
- So the committed batches are the first 200 rows to finish. A row before 200 that
  finishes after 200 others (thread preemption on a loaded runner) is recorded after the
  second commit. Its result is still pending at the crash, and the rerun computes it.

The library guarantee holds. `CachedFunctionPod.async_process_data` stores the result
(pending) before the row reaches the record stage, and `_commit_pending_writes` commits
results before pipeline records. So every committed record has a committed result.
Nothing at or after the crash row is committed, and the rerun computes exactly the rows
without a committed result.

Deterministic repro (scratch script: row 199 waits on the same "second commit done" event
as the crash row): records 200, results 207, rerun `min(calls) == 199`. The current
assertion fails 3/3; the order-free check holds 3/3.

Latent off-by-one: `results < 234` should be `<= 234`. Rows 0..233 are 234 rows, and all
of them may have committed results.

## Decisions

- The in-child wait becomes unbounded (`Event.wait()`), and exit code 4 goes. It cannot
  deadlock. The executor queue is FIFO, so rows at or after the crash row start only after
  every earlier row has started. The default pool has at least 5 threads, so the one
  blocked late row leaves room. `run_job`'s `subprocess.run(timeout=600)` stays as the
  hang guard. It is not a timing assumption of the test.
- The late-row case runs on async only. On the sync path, rows run in order, so a waiting
  row 199 would block the second commit forever.
- No library change, no changelog fragment (test only, as #347), no design_issues entry.

## Out of scope (report in worker_done)

These are the six interrupt tests in `tests/test_pipeline/test_job_run_in_event_loop.py`
(`test_interrupt_while_running_inside_a_loop_stops_the_run`,
`test_second_interrupt_returns_and_a_late_function_writes_nothing[*]` and
`test_second_interrupt_names_an_async_function_blocking_the_loop[*]`). Their time bounds:
- `threading.Timer(1.0, _thread.interrupt_main)`: the second press assumes the first has
  been handled and the run is waiting for the stuck function within 1 s.
- `time.sleep(0.05)` per row with `calls_when_stopped < N_SLOW_ROWS`, then a
  `time.sleep(0.2)` quiet window.
- `stuck_release.wait(timeout=30)` and the 30 s deadline loops are hang guards only.

Making them event-driven needs an observable "run is waiting for the stuck function" state
from the library. That is not a few lines, so it is not in this PR.

### Task 1: Failing regression case (late row)

**Files:**
- Modify: `tests/test_pipeline/test_job_crash_recovery.py`

- [ ] Add a seventh script argument `late` ("none" or a row number). In `double`, when
  `crash_kind == "fn"` and `x == late`, wait on `earlier_batches_committed`. Add
  `late: int | None = None` to `run_job`, passed as `str(late)` or `"none"`.
- [ ] Parametrize the crash test over `sync`, `async`, `async-late-row` (async, late 199)
  with the old assertions. Run it: `async-late-row` fails at `min(calls) >= 200`.
- [ ] WIP commit.

### Task 2: Order-free assertions, unbounded wait

- [ ] Add `stored_rows(store, kind) -> set[int]`: `k` of the pipeline table, `y // 2` of
  the result table.
- [ ] The crash test asserts:
  - two whole batches: 200 pipeline records on 200 distinct rows, each with a committed
    result;
  - nothing at or after the crash row stored (`max(computed) < 234`);
  - sync: `recorded == computed == set(range(200))`;
  - late row: the late row is neither recorded nor computed;
  - the rerun computes exactly the rows without a committed result:
    `sorted(calls) == sorted(set(range(N_ROWS)) - computed)`;
  - `stable(store) == uninterrupted`.
- [ ] Make the wait `earlier_batches_committed.wait()`, and update the script comment and
  the module docstring.
- [ ] Run the file on 3.12 and 3.13. Loop: before (origin/main test, late row injected)
  vs after, ≤ 50 iterations, sequential.
- [ ] ruff format/check, basedpyright, commit, full suite, push, PR (`Refs ITL-1001`), Copilot review.

## Progress (2026-11-13)

- Task 1 done: `async-late-row` failed with the old assertions (`assert False` at the
  `min(calls) >= 200` line, as on CI); the other three cases passed.
- Task 2 done: 4/4 pass on 3.12 and 3.13; ruff clean; basedpyright exit 0, no new
  warnings.
- Before (origin/main test, row 199 late, scratch repro): old assertion failed 20/20;
  order-free check held 20/20 (records 200, results 207, rerun min 199).
- After: 25/25 sequential runs of the two async cases passed.
