# ITL-896: operator node reads back empty after an async job run — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** After `PipelineJob.run(orchestrator=AsyncPipelineOrchestrator())`, `job.nodes[<operator label>]`
returns the same rows from `as_table()` / `iter_data()` as after a sync run.

**Architecture:** `OperatorJobNode` keeps its result only in memory (`_cached_output_stream`).
`execute()` (sync orchestrator) always keeps it; `async_execute()` keeps it only in
`CacheMode.LOG`, and inside a job every operator is `CacheMode.OFF`. Fix (option A, approved by
the coordinator 2026-11-11): `async_execute` collects the rows it forwards in every mode
(OFF, LOG, and the no-database branch) and stores them on the node with the same
`StaticOutputOperatorPod._materialize_to_stream` the sync path uses, regardless of
`materialize_results`. Nothing new is written to any store; no hash, path or schema changes.

**Tech Stack:** Python, PyArrow, asyncio, pytest.

## Root cause (STEP 1, done)

- Reproduced through the public interface on `cf4f67c7` for select/drop/select-tag/map-data/
  map-tags/polars_filter(constraint)/Join/MergeJoin/SemiJoin/Batch/GroupBy and an
  operator→function-pod chain, in-memory and Delta: sync gives the rows, async gives 0 rows,
  no error. `get_all_records()` is `None` on both paths (operators in a job are
  `CacheMode.OFF`, nothing is stored).
- Script: scratchpad `repro.py` (not committed).
- `get_all_records()` stays `None` for an operator node that stored nothing (approved);
  document that operator nodes in a job keep results in memory only and correct the
  `PipelineJob.run` `materialize_results` docstring.

## Open question to coordinator

After `clear_cache()` an operator node returns a zero-row table on both paths today (the same
"not yet run" state as before `run()`). Coordinator asked to be told before changing it.

## Files

- Modify: `src/orcapod/core/nodes/operator_node.py` (`OperatorJobNode.async_execute`)
- Modify: `src/orcapod/pipeline/job.py` (`run` docstring)
- Create: `tests/test_pipeline/test_operator_node_async_readback.py`
- Modify: `docs/concepts/function-pods.md` (node read-back / `get_all_records` for operators)
- Modify: `CHANGELOG.md` (`[0.3.0] — unreleased`, Fixed)
- Modify: `DESIGN_ISSUES.md` (one entry appended at the end, resolved)

### Task 1: failing tests

- [ ] Write `tests/test_pipeline/test_operator_node_async_readback.py`: parametrize over every
  operator (built with the stream convenience methods / operator call, labelled `"op"`),
  store in {in-memory, Delta}, `materialize_results` in {True, False}. Run the same job twice
  (fresh store each) with sync and async orchestrators. Assert:
  - async `as_table(all_info=True)` sorted by tag columns equals sync (schema incl. nullability
    and column order, values, system tags);
  - `iter_data()` row count equal and per-row tag/data `content_hash()` multisets equal;
  - row count > 0 (guard against both being empty);
  - `get_all_records()` is `None` on both.
- [ ] Run, confirm async cases FAIL (0 rows) and sync cases pass.
- [ ] Commit WIP.

### Task 2: fix

- [ ] In `OperatorJobNode.async_execute`, no-DB branch: wrap the output writer to collect rows,
  then set `_cached_output_stream` + `_update_modified_time()`.
- [ ] DB branch OFF/LOG: always collect; after the TaskGroup, if rows were collected set
  `_cached_output_stream = _materialize_to_stream(collected)`; store only in LOG.
  `_update_modified_time()` as today.
- [ ] Run new tests + `tests/test_core/operators tests/test_pipeline tests/test_channels`.
- [ ] Commit.

### Task 3: docs, CHANGELOG, DESIGN_ISSUES

- [ ] function-pods.md: operator nodes keep their output in memory for the life of the job
  object, on both orchestrators; nothing is stored, so `get_all_records()` returns `None`.
- [ ] job.py `run` docstring: correct the `get_all_records()` claim.
- [ ] CHANGELOG Fixed entry; DESIGN_ISSUES entry (resolved, Fix note).
- [ ] `uv run pytest tests/test_docs` + mkdocs strict.

### Task 4: verify & ship

- [ ] ruff check/format, basedpyright exit code, full `uv run pytest tests/`, golden hashes unchanged.
- [ ] Merge origin/main, push, PR (Fixes ITL-896), link, CI, CodeRabbit (inline + review bodies).

## November 12 refresh (Codex takeover)

- [x] Preserve approved merge 69af724b: pre-forward context validation when LOG has
  a database, unconditional row retention, conditional persistence; retain both run
  docstring paragraphs. Changelog and OR1 now live in their per-entry files.
- [x] Both fragment checkers pass. Prior worker established 57 readback failures
  against main and passing targeted tests after the merge.
- [ ] Add named-column and no-argument DropNulls to CASES (two retained rows).
  Compare normal rows and all-info schemas; mark only the known ITL-976 metadata
  mismatch and stop for any other difference.
- [ ] Pin OFF, LOG with/without database, and populated REPLAY through public node
  run/async_execute and stream reads; assert persisted rows only for LOG/REPLAY.
  Keep existing test_stored_operator_node_raises_before_forwarding_rows_async as
  the pre-forward LOG refusal regression.
- [ ] Run focused readback/equivalence/stored-context tests and golden tests without
  changing fixtures; run full suite sequentially on 3.12, 3.11, 3.13, then ruff,
  format and basedpyright (exit zero).
- [ ] Fetch current main before final commit; merge if needed and revalidate.
  Update existing PR body, push once, inspect CI and all CodeRabbit review bodies
  and GraphQL threads on the final head, route findings through coordinator ask.

### Approved review follow-up

Coordinator approved November 12: fix singleton exception wrapping here; defer
data-less stream retention (sync rejects it too); clarify old LOG records remain
readable after switching to OFF. Add an Index fail_on_miss public-execution
regression for no database, OFF and LOG before editing library code. Catch the
TaskGroup's ExceptionGroup and re-raise its sole exception; leave groups with
multiple failures intact. Re-run focused tests and all three full suites after
this change. Report the data-less reproduction in the final worker report.

### Refresh verification checkpoint

- Both DropNulls forms pass all eight store/materialization combinations without
  a metadata exception. Cache-mode parity passes for all four requested cases.
- Exception regression: 3 failures before the fix, then all pass. A concurrent
  operator RuntimeError and output LookupError remain an ExceptionGroup of two.
- Disposable-process test using main's async_execute: no-database exception keeps
  its original RuntimeError; OFF/LOG wrap it. Main still loses readback in OFF
  and requested LOG without a database (4 failures, 3 passes across new cases).
- Affected readback/channels/stored-context tests: 542 passed, 5 known xfails.
- Focused equivalence and golden tests before the exception-only change:
  1386 passed, 14 known xfails. No golden files differ from main.
- Ruff check and format pass; basedpyright exits 0 (10370 existing warnings).
- Initial full Python 3.12 run: 9438 passed, 101 skipped, 23 xfailed; superseded
  by the fresh full run after the exception tests were added.
- Data-less reproduction (deferred by coordinator): one-row ArrowTableSource
  with id tag and x data; SelectDataColumns([]) rejects sync with ValueError,
  async forwards one row and then raises the same no-data-columns ValueError.
- All previous review bodies/threads audited; older nitpick docstrings already
  replied to. Three latest threads will receive approved dispositions after commit.

### Final local validation

- [x] Full suites completed sequentially on Python 3.12, 3.11, 3.13: each
  9442 passed, 101 skipped, 23 xfailed; all exit 0.
- [x] Ruff check/format pass. Basedpyright on Python 3.11 exits 0 with no errors
  (10370 existing warnings). Both fragment checkers pass.
- [x] Main remains b46549fd; no golden or CHANGELOG.md differences against main.
- [x] Approved review dispositions implemented, tested and checkpointed in 91a342cf.
- [ ] Push once and verify remote CI, CodeRabbit head coverage and thread replies.

Audit logs, skip names, review data and the coordinator report are under the
worktree-local gitignored `.untracked/` directory.
