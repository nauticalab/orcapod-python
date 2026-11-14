# ITL-1080 Sync and async give the same rows — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Sync and async runs give identical rows: the zero-row async operator node keeps the
sync path's columns and Arrow types, and (once the developer decides the meta rule) every
operator treats input `Data` meta columns alike on both paths.

**Architecture:** The zero-row fix lives in `OperatorJobNode.async_execute`: it taps each input
channel (first row, plus the first row with a `None` source-info token per column), and on a
run with no output rows rebuilds zero-row inputs exactly as the sync orchestrator materializes
its buffers (one shared helper, `StaticOutputOperatorPod._materialize_upstream`), then keeps
`operator.process(*inputs)` as the result, as `execute()` does. No channel protocol or
`async_execute` signature change. The meta fix (rule B, pending developer decision) drops
input meta in every operator on both paths.

**Tech Stack:** Python 3.12, PyArrow, pytest via `uv run`.

---

## Status / notes for a successor

- Diagnosis sent to coordinator 2026-11-13 (scratch notes in the worker transcript):
  - All 8 meta xfails: Data/Tag content hashes identical on both paths; only meta columns differ.
  - Main's Join DROPS meta (ITL-854/857), not keeps. Rule B (no operator carries input meta)
    recommended; **developer decided B (Nov 13): no operator carries input meta; carry-forward
    is ITL-867.** Implemented in `core/operators/meta_columns.py`.
  - Q2 (zero-row node) approved: implement now, test first, nothing stored for zero rows.
- Step-4 matrix (scratchpad `matrix.py`): source → fn pod → 17 operators → fn pod, sync vs async
  job, in-memory store: no difference. MergeJoin over a dict column raises TypeError on BOTH
  paths (report, out of scope).

## Task 1: Zero-row async operator node (approved)

**Files:**
- Modify: `src/orcapod/core/operators/static_output_pod.py` (add `_materialize_upstream`)
- Modify: `src/orcapod/pipeline/sync_orchestrator.py` (`_materialize_as_stream` delegates)
- Modify: `src/orcapod/core/nodes/operator_node.py` (`async_execute`: input taps, zero-row result)
- Test: `tests/test_pipeline/test_operator_node_async_readback.py`

- [ ] Remove the strict xfail on
  `test_operator_node_with_no_rows_keeps_columns_and_types_after_async_run`, parametrize it over
  zero-row cases (filter no match, DropNulls dropping all, Join/SemiJoin/MergeJoin no match,
  operator after an input that delivered no rows), comparing `as_table(all_info=True)` schema
  and rows and `as_table()` of sync vs async runs. Run: fails.
- [ ] Add `_materialize_upstream(rows, upstream, *, zero_rows)` beside `_rows_to_table`: the body
  of the sync orchestrator's `_materialize_as_stream`, optionally sliced to zero rows. Make the
  sync orchestrator call it (no behaviour change).
- [ ] In `OperatorJobNode.async_execute`, wrap each input reader in `_InputTap` (keeps the first
  row and, per source-info column, the first row whose token is `None`). With no output rows,
  `self._cached_output_stream = operator.process(*[_materialize_upstream(tap.rows, upstream,
  zero_rows=True) ...])`.
- [ ] Run targeted tests: readback, equivalence, sync orchestrator, golden. Commit.

## Task 2: Meta rule B (approved, done)

- [ ] Prepare failing tests over both meta origins (raw `ArrowTableStream` with `__` columns;
  `CachedFunctionPod` stream rows with `__computed`) for every operator, sync and async:
  output `Data.meta_columns == ()` and identical schemas on both paths.
- [ ] After approval: drop input meta in each operator (sync: input tables via the Join helper
  `_without_meta`, moved to the operator base; predictions drop `__` fields; async: per-row
  operators drop `Data` meta). Remove the 8 META_XFAIL marks; update
  `test_drop_nulls::test_no_argument_leaves_meta_columns_alone`, docs/concepts/operators.md.
- [ ] Golden suite unchanged.

## Task 3: Equivalence matrix coverage

- [ ] `CASES` in `test_async_sync_equivalence.py` already holds SemiJoin, MapTags, MapData, the
  four selections, DropNulls, Index, Pick. Add the automatic select in front of a function pod
  (sync vs async job, stored records) if not covered by `test_sync_async_stored_records.py`.

## Task 4: Finish

- [ ] `changelog.d/ITL-1080.md`, `design_issues/ITL-1080-SA1.md`.
- [ ] Merge origin/main, push, PR "Fixes ITL-1080", CodeRabbit, CI on 3.11/3.12/3.13.

## Progress (2026-11-13)

- Task 1 done (dcf5c69e). Task 2 done. Targeted suites (core, pipeline, hashing, docs): 9356
  passed, golden unchanged. Step-4 matrix variants plain/log/cached/cached+log/none+log: no
  sync/async difference; with an upstream that delivered no row, Join now raises on async as on
  sync (ITL-912), pinned by a test.
- Next: PR, CodeRabbit, CI.

## Merge of origin/main b4fde330 (#372, #374: ITL-829, ITL-1088)

- `join.py`: `_joined_user_schemas` keeps `without_meta_fields` (shared helper) and takes
  main's `reconcile_key_nullability`; main's `_shared_key_nullability` kept, main's copies of
  `_without_meta` / `_without_meta_fields` dropped (replaced by `meta_columns`).
- `semijoin.py`: main's `_match_keys` / `_checked_match_keys` taken whole. The PR's
  `without_meta_fields` calls are dead (keys are tags on both sides; a tag can never be `__`)
  and are removed; `WithoutMetaWriter` in `async_execute` stays. `_ignored_shared_names`
  (main) already skips `__` names, so a shared `__` data column does not warn: pinned by
  `test_shared_meta_column_is_neither_a_key_nor_warned_about` (sync, async validated, async
  unvalidated; mutation-checked).
- Equivalence test: main's new CASES kept; main's `META_XFAIL` (incl. its new
  `merge_join_nullable_key`) dropped, since the meta rule fixes every case.

## Merge of origin/main 5537291f (#367, ITL-1075)

- `operator_node.py`: main's `_discard` and this PR's `_InputTap` both kept. REPLAY branch is
  main's (drains the raw inputs in tasks); the computing branch taps (`_InputTap`). Taps work
  under SemiJoin's new concurrent left receive (`receive`, `__aiter__`, `collect`).
- `semijoin.py`: main's imports and `_collect_right_reading_left` / `_LeftRows` structure kept
  exactly; `WithoutMetaWriter(output)` is the first statement of the `try` (the `finally`
  closes the wrapped writer).
