# ITL-906: async system-tag names after a non-source input — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan
> task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The async path of `MergeJoin`, `Batch` and `GroupBy` names system-tag columns (and so
derives record ids) exactly as the sync path does when an input is not a root source.

**Architecture:** The barrier rebuild (`StaticOutputOperatorPod._materialize_to_stream`) returns a
producer-less `ArrowTableStream`, whose pipeline hash is the schema-only root hash. The fix gives
the rebuilt stream the pipeline hash the caller already passes through the existing
`input_pipeline_hashes` keyword of `async_execute` (the operator node passes
`[s.pipeline_hash() for s in self._input_streams]`). No public signature changes.

**Tech stack:** Python, PyArrow, pytest via `uv run`.

**Status:** measurement done, ask sent to the coordinator (no code before approval).
Linear: ITL-906 In Progress, project "Orcapod Python: v0.3 release".
Branch: `eywalker/itl-906-mergejoin-async-path-names-system-tag` (from origin/main 25650c42).

---

## Measurements (main 25650c42)

Scratch scripts (not committed): `measure.py` (operator level, every `CASES` entry of
`test_async_sync_equivalence.py`, over a root source and after a no-op `DropNulls("k")`),
`measure_job.py` (job level, Delta), `itl885.py` (ITL-885's own graph), `proto.py`
(monkeypatched prototype of the fix), `old_store.py` (pre-fix async store re-run post-fix).

Operator level, after a non-source input (system-tag names / values / tag content hash):

| Operator | async path | names | tag hash |
|---|---|---|---|
| Join (2, 3, rotated) | own, uses hashes | same | same |
| MergeJoin | `BinaryOperator` barrier | **differ** | same |
| SemiJoin | own (build/probe) | same | same |
| Batch (size 2, size 0) | own, `_materialize_to_stream` per batch | **differ** | same |
| GroupBy | `UnaryOperator` barrier | **differ** | same |
| PolarsFilter | `UnaryOperator` barrier | same (name-preserving) | same |
| MapTags, MapData, Select/Drop Tag/Data, Index, Pick, DropNulls | per row | same | same |

Job level (Delta store; function node downstream of the operator): sync and async write the
SAME `pdb_v2` path, with different system-tag column names and different `__record_id`s. Over a
root source the two agree. With the prototype fix: identical columns and `__record_id`s.

ITL-885 item 1 (its own graph `source -> double -> GroupBy -> first`, int control): sync then
async and async then sync FAIL on Delta (non-nullable `_tag_source_id::…` violation) and on the
in-memory store (schema mismatch). With the prototype fix all four orders pass on both stores with
no recomputation on the second run. Item 2 (GroupBy reads back empty after async) no longer
reproduces (ITL-896).

A store written by a PRE-fix async run of an affected graph, re-run POST-fix (async or sync):
the downstream function node's write FAILS (Delta: non-nullable violation; in-memory:
`ArrowInvalid` schema mismatch). Same failure as ITL-885 today.

Golden: `tests/test_hashing/golden/cases.py` runs MergeJoin (after `SelectDataColumns`), Batch
and GroupBy through `op(stream)` only (sync). No golden entry covers the async path; none moves.

## Tasks (after approval)

### Task 1: failing tests (public interface)

- `tests/test_core/operators/test_async_sync_equivalence.py`: add a `NON_SOURCE_CASES` matrix:
  every `CASES` entry with each input wrapped by a no-op operator (`DropNulls` on a non-nullable
  tag) and by a function pod; run through `assert_equivalent` (names, values, hashes). Covers
  every operator with `async_execute`. Expected to fail for merge_join, batch_2, batch_all,
  group_by only.
- `tests/test_pipeline/...`: un-xfail
  `test_merge_join_after_an_operator_reads_back_same_system_tags_after_async_run`; add Batch and
  GroupBy after an operator; add a mixed-orchestrator test (sync then async, async then sync, on
  in-memory and Delta) for ITL-885's graph, asserting no recomputation on the second run.
- Run, show failures, commit (WIP).

### Task 2: fix

- `static_output_pod.py`: `_materialize_to_stream(rows, pipeline_hash=None)` returns a private
  `ArrowTableStream` subclass whose `pipeline_hash()` returns the given hash (falls back to the
  schema-only hash when `None`). Default `StaticOutputOperatorPod.async_execute` passes
  `input_pipeline_hashes[i]`.
- `base.py`: `UnaryOperator.async_execute`, `BinaryOperator.async_execute` pass their hashes.
- `batch.py`: `Batch.async_execute` takes the same keyword the base classes have and passes it.
- Length mismatch between `inputs` and `input_pipeline_hashes` raises `ValueError` (as `Join`).
- Run tests, ruff, basedpyright, commit.

### Task 3: docs and records

- `changelog.d/ITL-906.md` under `### Breaking changes and migration`.
- `design_issues/ITL-906-<ID>.md`, status resolved with **Fix:**.
- Update test docstrings/xfail reasons; remove the stale "Ignored by single-input operators"
  docstring line.

### Task 4: finish

- Full `uv run pytest tests/`, ruff check/format, basedpyright exit code.
- Merge origin/main, push, PR "Fixes ITL-906" (+ ITL-885 per coordinator), CodeRabbit rounds.

## Coordinator decisions (2026-11-12)

- Fix approved as proposed; `Batch.async_execute`'s `**kwargs` becomes the keyword-only
  `input_pipeline_hashes`.
- PR says "Fixes ITL-906" and "Fixes ITL-885" (mixed-orchestrator test: source, operator and
  function nodes, in-memory and Delta, both orders). MergeJoin source-info tie finding: out of
  scope, minimal repro in worker_done.
- Changelog: only stores written by an async run of an unreleased v0.3 build are at risk.
- ITL-865 approved: same PR, its own commit(s), its own fragment `changelog.d/ITL-865.md`.

## Progress

- [x] ITL-906 tests (failing first), fix, changelog, design issue (commit bd450dd5).
- [ ] ITL-865: zero rows on every input / one input for every async operator (equivalence
  test), async PipelineJob with a no-match filter upstream of a barrier operator and of a
  function pod; fix: barrier path emits nothing when an input has no rows; docs + changelog
  (`### Fixed`, `### Known limitations`: no typed empty result on async until ITL-858).

## ITL-865 measurements (after the ITL-906 fix)

Operator level, zero rows: MergeJoin (either or both inputs empty), GroupBy, PolarsFilter
raise `ValueError: Cannot materialize an empty list of rows`; Batch, Join, the per-row
operators give 0 rows as sync. SemiJoin with an empty RIGHT input gives the left rows when
the operator object was never validated (`_validated_common_keys` is set by
`validate_inputs`, i.e. by `op(...)` / node construction): operator-state hazard, ITL-858.

Job level: a sync job whose upstream operator node produced zero rows feeds a binary operator
(Join/MergeJoin/SemiJoin) a widened table (ITL-912: int8 -> int64) and raises a type
disagreement; out of scope (ITL-912).

## Review round: developer comment 4557365620 ("what's the point of this helper function??")

Decision (developer "Go with A", coordinator approval 2026-11-12): REQUIRE the hashes and remove
`_pair_input_pipeline_hashes`. One rule for every operator:

- Every operator's `async_execute` declares `*, input_pipeline_hashes: Sequence[ContentHash]`,
  required, no default (replaces `| None = None` and `**kwargs`). Missing -> `TypeError`.
- The five that use it (UnaryOperator, BinaryOperator, StaticOutputOperatorPod default, Batch,
  Join) check `len(input_pipeline_hashes) != len(inputs)` inline and raise `ValueError` naming
  both numbers. Join's no-hash fallback (positional suffixes "0", "1") goes too.
- Per-row operators (MapTags, MapData, Select/Drop Tag/Data, DropNulls, Index, Pick) and SemiJoin
  take it and say it is not used (they preserve names).
- `_materialize_to_stream(rows)` loses its hash parameter (output holders: operator_node ~1150,
  ~1488, Index, Pick); barrier paths call a new `_materialize_input_stream(rows, pipeline_hash)`
  (required hash) returning `_RowsOfStream`.
- Protocols unchanged (no protocol declares an operator's `async_execute`).
- Changelog ITL-906: add Join's old fallback. PR body: signature uniform and why.

### Steps
- [x] Failing tests (`tests/test_core/operators/test_async_sync_equivalence.py`): each barrier
  operator + Join without hashes raises `TypeError`; wrong length raises `ValueError` naming both.
- [x] Implement; update direct test callers; design issue Fix note; changelog.
- [x] Full suite 3.12, 3.11, 3.13 (10167 passed, 109 skipped, 22 xfailed each); ruff; basedpyright exit 0; merged origin/main (290889e8).
- [ ] Push, PR body, reply on thread 4557365620, CodeRabbit on new head, CI green.
