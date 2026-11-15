# ITL-1112 bind refuses a source whose column types differ — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `PipelineJob.bind(sources=...)` and the `SourceJobNode.bound_source` setter refuse a
source whose column types differ from the slot's declared schema, so an accepted bind never
moves a pipeline hash and never sends rows of another type to a stored path.

**Architecture:** `SourceNodeBase.validate` (the node owns the declared schema and its name)
compares, column by column, the source's `output_schema()` with the declared tag and data
schemas: names, role (tag/data), Python type. `bind` validates every slot before it changes
anything (sources and database); the setter calls the same `validate` before assigning.
`SourceSpecMismatchError` becomes a subclass of `InputValidationError` and `ValueError` (E1).

**Tech Stack:** Python, pytest, `uv run`.

---

## Decisions (coordinator, 2026-11-14)

- Developer: "Definitely refuse". No recompute of downstream identities.
- A1: compare names, roles, Python types (the pipeline-hash inputs); `optional_fields` and
  column order are not compared.
- E1: `SourceSpecMismatchError(InputValidationError, ValueError)` for name and type mismatches.
- Move `self._database = database` in `bind` after source validation.
- Setter calls `validate` (setting `None` unbinds, unchecked).
- **Nullability (T vs T | None): PENDING with the developer.** No tests or code may fix either
  answer until the coordinator replies. Tests use int vs str only.
- ITL-949 (int32 vs int64) out of scope.

## Reproduction (main a9b50350)

Scratchpad scripts `repro.py`, `repro3.py`: slot `src` (id:int, x:int) -> MapData(x->y) ->
`const(y: int) -> int`. Binding a str source is accepted by bind and the setter, fresh and
loaded job; pipeline hashes unchanged (src 70bf0c37, renamed e777d53b); a run sends str values
into the int-declared paths (`schema:f289a35f.../pdb_v2`, `_result/.../rdb_v2`).

## Files

- Modify: `src/orcapod/errors.py` (`SourceSpecMismatchError` bases)
- Modify: `src/orcapod/core/nodes/source_node.py` (`validate`, `bound_source` setter)
- Modify: `src/orcapod/pipeline/job.py` (`bind`: database assignment after validation; docstring)
- Test: `tests/test_pipeline/test_bind_type_check.py` (new)
- Add: `changelog.d/ITL-1112.md`, `design_issues/ITL-1112-BT1.md`
- Docs: `docs/concepts/function-pods.md` near line 783 (one sentence on bind)

## Tasks

- [ ] 1. Failing tests (`tests/test_pipeline/test_bind_type_check.py`), public interface:
  - bind with x: str in place of x: int raises `SourceSpecMismatchError`, which is an
    `InputValidationError`; message names slot `src`, column `x`, `int` and `str`.
  - same through `job.nodes["src"].bound_source = ...`.
  - both on a fresh job and on a job from `PipelineJob.load`.
  - after the refusal the previous binding is kept (`job.nodes["src"].bound_source is old`),
    and `bind(database=new, sources={bad})` leaves `job.database` unchanged.
  - a tag column given as str for a tag declared int is refused too.
  - after an accepted rebind (same types, other values/source id) every node's
    `pipeline_hash()` equals its value before the bind (fresh and loaded).
  - an accepted bind still works and the run stores rows.
  Run: `uv run pytest tests/test_pipeline/test_bind_type_check.py -v` -> FAIL.
- [ ] 2. Implement: errors.py bases; `validate` compares types after names; setter calls
  `validate`; `bind` moves database assignment. Run tests -> PASS. Commit.
- [ ] 3. Nullability: after the coordinator's reply, add tests and the rule.
- [ ] 4. Changelog fragment (Breaking changes and migration), design_issues entry, docs
  sentence; `uv run python scripts/assemble_changelog.py --check`,
  `uv run python scripts/design_issues.py --check`.
- [ ] 5. ruff, basedpyright, full `uv run pytest tests/`; merge origin/main; push; PR.

## Notes for a successor

- Conflicts: PR #384 (ITL-1102) edits the `bound_source` setter (adds `_update_modified_time()`);
  ITL-1111 adds a memo clear inside `bind`. Whoever merges second resolves.

## Decisions, round 2 (coordinator/developer, 2026-11-14)

- Nullability: a slot accepts a STRICTER source. Declared `T | None` accepts a source with `T`;
  declared `T` refuses `T | None`. Nothing else is compatible (no other union, subclass, width).
- The node presents the DECLARED schema: `output_schema()`/`keys()` and its rows. Rows are cast
  to the declared form at one helper: same arrays re-wrapped under nullable fields (no cast) and
  the two system-tag columns renamed from the source's schema-hash suffix to the declared one
  (N1; names from `system_tag_column_names` on both hashes). Row hashes, record ids, stored
  schema are those of a `T | None` source (the flag reaches the row hash today: measured).
- `content_hash` and `source_id` stay the source's own.
- A narrower source whose system-tag columns are not exactly the two of its schema hash is
  refused at bind (never mid-run).
- Entry points: `iter_data`, `async_iter_data` (per row: Tag/Data rebuilt with the row's data
  context), `as_table` and `head` (an `ArrowTableStream` over the cast table, so content-hash
  column, sort and column selection are computed on the cast rows), `output_schema`.
- L1: `SourceJobNode.__init__` binds through the setter; `PipelineJob.load` handles a rebuilt
  source the slot refuses like one that could not be rebuilt (warning, slot unbound).
- PR as draft; CodeRabbit reviews by itself (do not request).

## Tasks, round 2

- [ ] 6. Tests `tests/test_pipeline/test_bind_narrower_source.py`: accepted narrower bind (bind,
  setter; fresh, loaded), reverse refused, downstream schema and pipeline hashes unchanged,
  rows table-equal to a nullable-flag source (names, flags, row hashes, record ids), sync and
  async, same-path append (narrower run then nullable run with a null), zero rows, head.
  Loaded job: CSV changed type (warning, unbound, run fails as unbound, rebind works); CSV now
  narrower (bound, cast).
- [ ] 7. Implement `_DeclaredForm` + entry points + `__init__` through setter + load handling.

## Round 3 (coordinator, 2026-11-14): merge main (#375, ITL-912) + ITL-1126

Main is 52d67858 (#375 merged). Conflict: `source_node.py` imports only (both kept). Semantic
overlap: #375 added `SourceJobNode._typed_zero_row_table` returning the BOUND SOURCE's
zero-row table, which is not the declared form (source's flags, column order, system-tag names).
Downstream operator nodes type their zero-row tables (head before a run, `as_table` with nothing
held) from it, so they would differ from their populated tables.

- [ ] 8. Merge (import resolved). Failing test: zero-row table of a source node bound to a
  narrower / reordered / transformed (renamed system tags) source equals its populated table
  (`ZERO_ROW_INPUT_COLUMNS`), directly (`typed_zero_row_table(node)`) and through a downstream
  operator node (`as_table(all_info=True)` before vs after run). Fix: `_typed_zero_row_table`
  applies `self._declared_form` to the source's zero-row table.
- [ ] 9. ITL-1126 (developer: refuse for now; ITL-1127 is the follow-up): `_fit` refuses every
  source whose system-tag columns are not exactly one source-id and one record-id column,
  narrower or not; message names the slot and the columns found. Remove
  `_keeps_its_own_system_tags`; `_declared_form_for` loses its pass-through branch. Tests:
  equal-schema joined stream refused (bind, setter, `SourceJobNode(...bound_source=)`), raw
  `ArrowTableStream` with none refused, binding unchanged after refusal.
- [ ] 10. Docs sentence, CLAUDE.md + .zed/rules, changelog fragment (Breaking: the refusal; way
  around: make the join part of the pipeline, or store the joined stream and bind the stored
  source; ITL-1127), design_issues/ITL-1112-BT1.md, PR description (Fixes ITL-1112, Fixes
  ITL-1126; Known limits).
- [ ] 11. Tests: tests/test_pipeline, tests/test_core/nodes, tests/test_core/sources, golden
  (3.12; async files also 3.11); ruff, basedpyright; one push; CI; one '@coderabbitai review'.
