# ITL-1001 / ITL-1029 — flaky async-run tests

Branch `eywalker/itl-1001-flaky-timing-test`. PR against `main`: `Fixes ITL-1029`, `Refs ITL-1001`
(coordinator, November 12, 2026: ITL-1001's original goal, `test_concurrent_with_io_bound_work`
and the wall-clock sweep, stays open).

## Findings

### Tests 1 and 2: library bug (ITL-1029)

`test_run_inside_running_loop_stores_what_a_script_run_stores[in_memory]` and
`test_await_run_async_stores_what_a_script_run_stores[in_memory]`.

CI logs (runs 43369849949 / job 131702869649 and 43356121100 / job 131658688707): the stored
records are equal. The difference is in `node.as_table(all_info=True)`: the `_content_hash`
column of the *script* run is rotated against its rows (r1 carries r2's output hash, ...).

Mechanism: `FunctionNodeBase._shape_table` (`src/orcapod/core/nodes/function_node.py:596-607`)
calls `arrow_utils.apply_column_config`, which sorts the rows by tag when `sort_by_tags` is on
(`all_info=True` turns it on), and only then appends the hash column, built from
`iter_data()` in `_cached_output_datas` order. The async node fills that dict in completion
order (`record_and_forward`), so when rows finish out of order the hashes land on other rows.
The sync path computes in input order, which is tag order in these tests, so it never shows.
`FunctionPodStream.as_table` and `ArrowTableStream.as_table` append the hash before sorting.

Deterministic repro: one row that sleeps makes it finish last: misaligned every time.

Other table-shaping paths checked: `OperatorNode.as_table` delegates to
`ArrowTableStream.as_table` (correct order); node `head()` refuses `sort_by_tags`, so its
hashes follow its rows; the run summary builds no table.

### Test 3: test assumption

`test_crash_mid_batch_keeps_committed_batches[async]`, CI `assert (100 == 200)` on `records`.

The async pod creates one task per input with no limit (`function_pod.py:644-648`,
`max_concurrency` unlimited by default), and each task hands the sync function to the thread
pool at once (`data_function.py:864`). The node records rows and commits every 100 in a
separate stage (`record_and_forward`, `_row_processed`, `function_node.py:1694`), on the loop
thread. So the thread pool can compute row 234 while fewer than 200 rows are recorded, most
easily while the loop is blocked in the 100-row Delta commit. The committed batches are intact
(100 records, 100+ results); nothing is lost. The library never promised that row x starts only
after the batches before it are committed.

Measured (scratch probe, uninterrupted async runs, 250 rows, 100 per commit): rows started
when the second node commit begins (flush 3): 223, 223, 223, 223, 239. So the thread pool
runs 24 to 40 rows ahead of recording (16-row result channel plus rows in flight); 234 is
inside that range. Natural rate on macOS: 0 / 510 under 3 processes (the CI failures were on
Linux). A slow first commit (scratch adversary, 0.3 s) did not change it (0 / 60): what matters
is the lag at the second commit, not the time the loop is blocked.

## Plan

1. Regression test for ITL-1029 (public interface): a job whose function makes the first row
   finish last, deterministically (threading events, no sleep), run with the async
   orchestrator; `node.as_table(all_info=True)` each row's `_content_hash` equals the hash
   of its own data (compare with `iter_data()` pairs keyed by tag). Show it fails.
2. Fix `_shape_table`: append the hash column to the full table, then `apply_column_config`.
3. Test 3: in `JOB_SCRIPT`, the function at the crash row waits on a `threading.Event` set by
   `CrashOnFlush.flush` after the inner flush returns, once the store has finished
   `1 + x // every` flushes (the run's first flush plus one per committed batch). Bounded wait;
   on timeout exit with a distinct code, which `run_job` rejects (test fails, not passes).
   Keep `records == 200` and `200 <= results < 234`.
4. Before/after rates: loop each test >= 500 times under load (<= 4 processes); for test 3 also
   an adversarial slow first commit (scratch only) to show the old form fails and the new does not.
5. design_issues entry `ITL-1029-FN2.md`, changelog fragment `changelog.d/ITL-1029.md`.
6. Full suite, ruff, basedpyright; PR; CodeRabbit.

## Status notes

- Setting ITL-1029 In Progress was denied by the auto-mode classifier; reported to coordinator.
