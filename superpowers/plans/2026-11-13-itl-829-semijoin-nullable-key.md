# ITL-829 (second case) SemiJoin over a key nullable on one side only — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `SemiJoin(left, right)` accepts a shared tag key that is `T` on one input and
`T | None` on the other, by applying `DropNulls(<that key>)` to the nullable side first.

**Architecture:** Rule approved by the coordinator (developer decision, November 13, 2026):
for each name that is a TAG column on both inputs and whose Python type admits `None` on
exactly one side, SemiJoin narrows that side as `DropNulls` would, then runs today's semi
join. Keys nullable on both sides or on neither are untouched (byte-identical path). The
result equals `DropNulls(nullable-side keys)` on each input followed by `SemiJoin`, schema
included. Reuse `DropNulls._narrow_schema` / `DropNulls._narrow_table`; never copy them.

**Tech Stack:** pyarrow, polars, asyncio, pytest.

Branch: `eywalker/itl-829-semi_join-rejects-compatible-keys-string-vs` (the issue's
gitBranchName; it did not exist remotely: PR #291 used `eywalker/itl-847-...`), from
`origin/main` `512cece9`.

## Diagnosis (main, 512cece9)

- (left T, right T|None), (left T|None, right T), composite mixed keys: `InputValidationError`
  "Type conflict for key 'id': ..." from `validate_binary_inputs`
  (`schema_utils.intersection_schemas`), so a pipeline never reaches async. `async_execute`
  called directly gives the null-never-matches rows.
- Both T|None: works, schema `int | None`, null matches nothing (Join section of
  `docs/concepts/operators.md`; `core/operators/match_keys.py`).
- Join and MergeJoin raise the same mismatch at graph build (report only, PR body).
- A shared DATA column is a match key too today (report only, PR body).

## Files

- Modify: `src/orcapod/core/operators/semijoin.py`
- Create: `tests/test_core/operators/test_semijoin_nullable_keys.py`
- Modify: `tests/test_channels/test_node_async_execute.py` (remove strict xfail)
- Modify: `tests/test_core/operators/test_async_sync_equivalence.py` (cases)
- Docs: `docs/concepts/operators.md` (SemiJoin section, two sentences), `CLAUDE.md` +
  `.zed/rules` (one bullet), `changelog.d/ITL-829.md`, `design_issues/ITL-829-SJ2.md`

## Tasks

### Task 1: failing tests

`tests/test_core/operators/test_semijoin_nullable_keys.py`, public interface:
- for each case (left T/right T|None; left T|None/right T; composite a:T,b:T|None vs
  a:T|None,b:T): `SemiJoin()(left, right)` rows and Arrow schema equal
  `SemiJoin()(DropNulls(lnames)(left), DropNulls(rnames)(right))` (or the un-narrowed input
  when a side has no names); `output_schema()` equals
  `static_process(...).output_schema()` (comment: schema-prediction exception); left key
  type `int` when left was nullable.
- async rows (`async_execute` over channels, after `op(left, right)` validation) equal sync
  rows (`assert_equivalent`, `assert_same_provenance` from the equivalence module).
- both nullable: schema stays `int | None`, rows as today, Arrow field nullable.
- a shared DATA column with mismatched nullability still raises `InputValidationError`.
- `int` vs `str | None` tag key still raises.
- identity: `SemiJoin()(l, r).pipeline_hash()` on an all-non-nullable pair unchanged vs
  main (covered by golden tests; no new golden entries).
- [ ] Run, confirm they fail on main with the type conflict. WIP commit.

### Task 2: implement in `semijoin.py`

- `_nullable_key_names(left_tag_schema, right_tag_schema) -> (left_names, right_names)`:
  names in both tag schemas whose type differs in admitting `None` (`_without_none(t) != t`).
- `validate_binary_inputs`: narrow both tag schemas with `DropNulls._narrow_schema` before
  `intersection_schemas`.
- `binary_output_schema`: narrow the left output schemas by `left_names`.
- `binary_static_process`: `left_table = DropNulls._narrow_table(left_table, left_names)`
  and same for right, when the name sets are non-empty; `logger.info` naming them.
- `async_execute`: in the first-left-row block decide the names from the first left and
  first right row's Arrow tag field nullability; emitted left rows go through
  `DropNulls._narrow_table` (tag and data tables, as `DropNulls.async_execute`). Right rows
  need nothing (a null key never matches).
- [ ] Tests pass; remove the strict xfail in `test_node_async_execute.py`; add cases to
  `test_async_sync_equivalence.py`. Commit.

### Task 3: docs, changelog, design issue; ruff, basedpyright, targeted pytest; PR.

## Conflicts

PR #367 rewrites the async input reading (`_collect_right_reading_left`, `_LeftRows`); PR #369
adds `output = WithoutMetaWriter(output)`. Coordinator: merge main after both land.

## Progress notes

- [x] Diagnosis and proposal approved (coordinator, Nov 13).
- [x] SUPERSEDED (developer, Nov 13): no narrowing. New rule, approved by the coordinator:
  `Join`, `MergeJoin`, `SemiJoin` accept a shared TAG key differing only in nullability;
  Join/MergeJoin key is `T` if any input has `T`, `T | None` only if all nullable; SemiJoin
  output = left schema as is. Helper `core/operators/key_nullability.py`
  (`reconcile_key_nullability`, `key_nullable`). Data columns unchanged.
- [x] Implemented (commit c5571236); tests `test_join_key_nullability.py`, equivalence cases,
  `test_drop_nulls.py::test_narrowed_nullable_key_joins_with_a_non_nullable_key` updated
  (asserted the old Join error), golden entries `pipeline/join_key_nullability` (adds only).
- [x] Docs (operators.md null-key paragraph), CLAUDE.md/.zed bullet, changelog, design issue SJ2.
- [ ] Merge main after #367 and #369 land (coordinator tells); PR; CodeRabbit; CI.
