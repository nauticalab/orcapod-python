# ITL-1093: a function job node returns the rows of its run's inputs — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** After a run on either orchestrator, a `FunctionJobNode` returns exactly the rows of
that run's inputs (`as_table`, `iter_data`, `head`, `run()`, a consumer built over it), row for
row equal between sync and async. Nothing stored changes.

**Architecture:** `FunctionJobNode._cached_output_datas` stays a superset cache keyed by base
entry id. A new `_run_entry_ids: list[bytes] | None` records the base entry ids of the last
completed run's inputs in input order (`None` = no completed run in this process). One helper
`_held_pairs()` is the only reader of the held rows: with run ids it yields one row per run
input (not deduplicated, so held rows equal what downstream received), else every held value
(unchanged no-run behaviour). A run that raises defines no rows: the node clears its in-memory
rows and run marker (`clear_cache()`) and reads as a no-run node.

**Tech Stack:** Python 3.11+, PyArrow, pytest, `uv run`.

Coordinator decisions (November 13, 2026): proposal approved; no-run read unchanged (option A,
documented; B/C reported to the developer); duplicates not deduplicated; a raised run → no-run
read (option i); REPLAY union and async `fail_fast` (ITL-879) reported only.

---

## Cause (measured on main b4fde330)

- `async_execute` loads `self._load_cached_entries()` with no ids (every pipeline record under
  the node's content hash) into `_cached_output_datas`; every read yields every dict value.
- Sync `execute` loads only this run's ids (`get_cached_results(base_entry_ids)`), so it looked
  right only because a new job builds fresh node objects. Re-running the same job object with a
  rebound source accumulates on sync too (3/6/6/9).

## Files

- Modify: `src/orcapod/core/nodes/function_node.py` (FunctionJobNode: `__init__`, `clear_cache`,
  `execute`, `async_execute`, `iter_data`, `head`, new `_held_pairs`)
- Create: `tests/test_pipeline/test_job_node_rows_after_run.py`
- Create: `changelog.d/ITL-1093.md`, `design_issues/ITL-1093-JN1.md`
- Modify: `CLAUDE.md`, `.zed/rules` (Phase 1 sentence); `docs/concepts/` sentence if one exists.

## Tasks

### Task 1: Failing tests

- [ ] Write `tests/test_pipeline/test_job_node_rows_after_run.py`: pipeline `rowwise` (inc over
  DictSource source_id "s"), `downstream` (dbl over rowwise), `grouped` (total over
  `src.group_by("g")`). Sequence 3 / 6 / 3 again / 3 different rows.
  - fresh job per run, sync and async: each node's `as_table`, `iter_data`, `head(100)`,
    `run()` and a consumer `dbl.pod(job.nodes["rowwise"])` return the rows of the run's inputs;
    sync == async as sets of rows with system tags; the 6-row run reports 3 reused.
  - same job object, `job.bind(sources=...)` per run, sync and async: same expectations.
  - filtered inputs (function returns None) are in no read.
  - a raised run (sync fail_fast ValueError; sync and async StoreWriteError raised part-way)
    reads as a no-run node: equal to a fresh compiled job's read over the same store.
- [ ] Run (Python 3.12 and 3.11): `uv run pytest tests/test_pipeline/test_job_node_rows_after_run.py`
  — expect async and same-job cases to FAIL.
- [ ] WIP commit.

### Task 2: Fix

- [ ] `_run_entry_ids` class default `None`, set in `__init__`; `clear_cache()` resets it.
- [ ] `_held_pairs()` helper; `iter_data` (both branches) and `head`'s held branch read through it;
  `head` takes the held branch when `_run_entry_ids is not None` even with no held rows.
- [ ] `execute`: completed flag; at success set `_run_entry_ids = base_entry_ids` and drop the
  cached table/hash column; on any raise after processing began, `_commit_after_failure`
  (unchanged) then `clear_cache()`.
- [ ] `async_execute` DB path: `route_inputs` appends each input's base entry id to a local
  list; at success set `_run_entry_ids`; a raise (Exception or cancellation) → `clear_cache()`.
- [ ] Run new tests (3.12 + 3.11) and targeted node/pipeline tests; commit.

### Task 3: Docs and records

- [ ] Docstrings of `iter_data`, `as_table`, `head`, `run`, `execute`, `async_execute` state the rule.
- [ ] `changelog.d/ITL-1093.md` under `### Fixed`; check with
  `uv run python scripts/assemble_changelog.py --check`.
- [ ] `design_issues/ITL-1093-JN1.md` (resolved, Fix note); `uv run python scripts/design_issues.py --check`.
- [ ] CLAUDE.md + `.zed/rules` sentence → "After a run, a `FunctionJobNode` returns exactly the rows
  of that run's inputs, sync or async (a run that raised defines no rows). Read without a
  completed run in this process (fresh or loaded job), it returns every record stored under its
  content hash for the current data context. A run skips inputs whose entry is already stored."
- [ ] docs/concepts sentence if one states what a job node returns after a run.
- [ ] ruff, ruff format, basedpyright (exit code), golden suite, targeted suites; commit; push; draft PR.

## Notes for a successor

- Probes: scratchpad (not durable); measured tables are in the coordinator ask and the PR body.
- PR #359 (ITL-1063) edits the same `async_execute`/`execute` hunks; semantics compatible.
