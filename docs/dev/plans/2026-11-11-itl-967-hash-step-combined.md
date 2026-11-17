# ITL-967 v0.3 Hash Step (combined PR) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this
> plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. A successor worker
> continues from the first unchecked step; the "Progress notes" section at the end is kept
> current.

**Goal:** One branch from `main` that holds the v0.3 hash-step pull requests (#309, #313,
#318, #314, #315, #319, and #301 only if it raises no design question), merged with history
kept, with one golden regeneration, opened as one PR against `main`.

**Architecture:** `git merge --no-ff` of each branch in the order given by the coordinator;
conflicts resolved according to `superpowers/specs/2026-11-11-itl-938-versioning-system-design.md`
(Parts 5 to 9). Golden-file conflicts take either side and are regenerated once at the end.
Each reported merge point gets a test. No squash, rebase or force-push.

**Tech Stack:** git, uv, pytest, ruff, basedpyright, mkdocs, gh, orca orchestration CLI.

Branch: `eywalker/itl-967-v03-hash-step-combine-the-hash-changing` (from `origin/main` `d8bba28e`).

---

## Branch tips to merge

| # | PR | Issue | Tip to merge | Why this tip |
|---|---|---|---|---|
| 1 | #309 | ITL-910 | local `eywalker/itl-910-adopt-starfix-05-hash-format-2-in-orcapod` `c4490d5a` | 1 ahead of origin `b88f7eb3`: changelog fragment commit |
| 2 | #313 | ITL-944 | local `eywalker/itl-944-data-context-v1-frozen-manifest` `81da9736` | 3 ahead of origin `8b050710` |
| 3 | #318 | ITL-954 | `origin/eywalker/itl-954-find-tables-written-before-v03-by-scanning` `8b291c9d` | fragment already on origin |
| 4 | #314 | ITL-945 | local `eywalker/itl-945-apply-the-hash-label-rule-one-format-labels` `3192a8a7` | 3 ahead of origin `40a71022` |
| 5 | #315 | ITL-942 | local `eywalker/itl-942-fix-the-wrong-hash-findings-of-the-hash` `6fb74c22` | 3 ahead of origin `a8538112` |
| 6 | #319 | ITL-935 | `origin/eywalker/itl-935-design-make-the-logical-type-version` `2369d75d` | fragment already on origin |
| 7 | #301 | ITL-905 | `origin/eywalker/itl-905-a-dict-values-insertion-order-changes-its` `66c6aac5` | only with the coordinator's go-ahead |

Re-check each local tip with `git rev-parse` immediately before merging it (the ITL-961
conversion worker may add commits).

## Dry-run conflicts (scratch branch, conflicts crudely resolved with "theirs" to continue)

| Merge | Conflicted files |
|---|---|
| 1 #309 | `DESIGN_ISSUES.md`, `src/orcapod/core/nodes/function_node.py` (1 hunk) |
| 2 #313 | none |
| 3 #318 | `core/cached_function_pod.py` (1), `databases/delta_lake_databases.py` (2), `pipeline/job.py` (1): all against main's ITL-919 / ITL-936 changes |
| 4 #314 | `DESIGN_ISSUES.md`, `changelog.d/ITL-910.md` (2), `docs/concepts/function-pods.md`, `docs/reference/hashing.md` (4), `contexts/data/schemas/context_schema.json` (2), `core/nodes/operator_node.py` (3), `core/sources/cached_source.py` (3), `errors.py`, `tests/test_arrays/test_array_v02_store.py`, `tests/test_hashing/test_starfix_arrow_hasher.py`, `tests/test_semantic_types/test_struct_layout_identity.py`, golden `pipeline.json`, `schema.json`; modify/delete (deleted by #313, modified by #314): `contexts/data/v0.1.json`, `hashing/stored_hash_labels.py`, `migrations/pipeline_db.py`, `tests/test_hashing/test_stored_hash_labels_helpers.py`, `tests/test_migrations/test_pipeline_db.py`, `tests/test_pipeline/test_stored_hash_labels.py` |
| 5 #315 | `DESIGN_ISSUES.md`, `hashing/semantic_hashing/builtin_handlers.py` (2), golden `arrow.json`, `pipeline.json`, `schema.json`, `semantic.json` |
| 6 #319 | `DESIGN_ISSUES.md`, `builtin_handlers.py` (1), `hashing/visitors.py` (1), `tests/test_arrays/test_array_value_format.py`, `tests/test_hashing/hash_samples/schema_hash_golden.json`, `tests/test_hashing/test_canonical_type_names.py`, `tests/test_hashing/test_extension_type_hashing.py`, golden `datagram.json`, `schema.json` |
| 7 #301 | `CHANGELOG.md`, `errors.py`, `semantic_types/universal_converter.py` (2), `tests/test_hashing/golden/cases.py`, golden `arrow.json`, `datagram.json`, `pipeline.json` |

## Resolution rules

- The design document decides. Anything it does not settle goes to the coordinator via `ask`.
- Files #313 deleted and #314 modified stay deleted (`v0.1.json`, `stored_hash_labels.py`,
  `migrations/`, their tests). What #314 wrote into `v0.1.json`'s changelog goes into
  `v1.json` `changes`.
- `v1.json` and `context_schema.json` lose `hasher_id` (merge point a).
- `operator_node._read_stored_records` and `cached_source._read_cached_records`: #313's
  version wins; #314's labelled row key is kept (merge point a).
- `visitors.py` scalar extension-cell token: `utf8("<name>;<hex metadata>") + b"::" + raw digest`
  (merge point b).
- `GenericAliasHandler` and `UnionTypeHandler` return `Encoded`; remove the strict-xfail on
  `test_hash_key_differs_from_its_string_spelling` (merge point c).
- Default source ids stay labelled, label `semantic_v1` (merge point e).
- Golden JSON conflicts: take either side; never judged until the final regeneration.
- Inline pinned values in tests and `hash_samples/*.json` that conflict: take either side
  during the merge, then re-record once after all merges, with the cause of each move noted.
- `DESIGN_ISSUES.md`: keep every branch's entries (union).
- `CLAUDE.md` / `.zed/rules`: main's text plus each branch's additions, both files in step.
- `CHANGELOG.md`: identical to main except #309's corrective edits to the ITL-888 text.

## Tasks

### Task 0: Plan approval

- [x] Read ITL-967, the design Parts 5 to 9, the seven PR bodies.
- [x] Dry-run the merges on a scratch branch (deleted afterwards).
- [ ] Send the merge plan through `ask`; wait for the reply.
- [ ] Commit this plan file.

### Task 1 to 6: one merge each (#309, #313, #318, #314, #315, #319)

For each merge:

- [ ] `git rev-parse <tip>`; `git merge --no-ff <tip>`.
- [ ] Resolve conflicts by the rules above; `git diff --check`; no conflict markers
      (`git grep -n '^<<<<<<<\|^>>>>>>>' -- . ':!*.md'` and the same for `.md`).
- [ ] `uv run ruff check . && uv run ruff format --check .`
- [ ] Targeted tests of the files touched by the resolution, excluding golden tests
      (`uv run pytest <paths> -q -p no:cacheprovider`), one pytest process at a time.
- [ ] Commit the merge (`Merge <branch> (#<n>, ITL-<id>) into ITL-967`), push, status message
      to the coordinator.
- [ ] Record the resolution in "Progress notes".

### Task 7: #301 (ITL-905)

- [ ] Only if the coordinator says to include it; otherwise record why it is left out.

### Task 8: merge-point tests and review findings

- [ ] For each merge point a to i: confirm a test exists (name it in the notes) or add one,
      failing first where it is a fix.
- [ ] Merge point h: evaluate each CodeRabbit finding against the combined code, send the
      revision plan through `ask`, fix the accepted ones (failing test first), one commit.
- [ ] Changelog: `uv run python scripts/assemble_changelog.py --check`; `git diff origin/main -- CHANGELOG.md`.
- [ ] `diff <(sed ... CLAUDE.md) .zed/rules` style comparison of the two instruction files.

### Task 9: one golden regeneration

- [ ] `uv run pytest tests/ -q -x --deselect tests/test_hashing/golden` first: everything
      but golden pins passes (or failures are pinned values only, listed).
- [ ] `uv run python tests/test_hashing/golden/generate_golden_hashes.py`
- [ ] Re-record the non-golden pins (`hash_samples`, inline pins) that moved; list them.
- [ ] Golden report against `origin/main`: per file and group, changed / added / removed /
      renamed, each attributed to an issue. An entry with no attributable cause: stop and ask.

### Task 10: verification

- [ ] ITL-954 re-verification: `uv run pytest tests/test_pipeline/test_pre_v03_tables.py tests/test_databases/test_list_table_paths.py -v`
- [ ] `uv run --python 3.11 pytest tests/`, then 3.12, then 3.13, never in parallel.
- [ ] `uv run --with ruff ruff check .`, `uv run --with ruff ruff format --check .`,
      `uv run --with basedpyright basedpyright` (exit code 0).
- [ ] Docs tests and `mkdocs build --strict` (in the suite; name them in the report).

### Task 11: PR and readiness

- [ ] Merge current `origin/main` if it moved (and any of #292, #316, #317 the coordinator
      announces); regenerate golden once more as the last step.
- [ ] Push; open the PR with the body the brief specifies; `orca linear attach`;
      `orca worktree set --worktree current --pr <n>`.
- [ ] CI green; CodeRabbit review on the latest head, inline threads and review-body findings
      answered; one push per round.
- [ ] `worker_done` with the PR URL.

## Progress notes

- Nov 11: plan approved by the coordinator. #301 (ITL-905) is LEFT OUT (two developer
  decisions still open: key-ordered in-memory dict copy, raising on NaN / colliding keys).
- Merge 1 #309 (c4490d5a): function_node.py imports (both kept), DESIGN_ISSUES.md (union).
- Merge 2 #313 (81da9736): function_node._add_pipeline_record keeps main's ITL-919 signature
  with #313's `_account_for_stale_tables()` in place of the removed `_ensure_pdb_schema()`.
- Merge 3 #318 (8b291c9d): three additive conflicts against main's ITL-919 code.
- Merge 4 #314 (3192a8a7): files #313 deleted stay deleted; `v1.json` and
  `context_schema.json` lose `hasher_id`, `v1.json` `changes` gains the label-rule entries;
  `operator_node._read_stored_records`, `cached_source._read_cached_records`, the
  `SideEffectLogDataContextError` docstring and the "older store" docs take #313/#318's text;
  the labelled row keys and bare path digests of #314 are in (auto-merged). Fragment
  `ITL-945.md` and `docs/reference/hashing.md` corrected for the combined result (`pdb_v2`;
  v0.2 tables are found by name). Tests adjusted for the layout component
  (`test_hash_label_rule.py`, `test_cached_source.py`).
- Merge 5 #315 (6fb74c22): GenericAlias/Union handlers return `Encoded` holding
  `ContentHash` objects; strict xfail removed.
- Merge 6 #319 (2369d75d): cell token = identifier text + `::` + raw digest.
- Option A (coordinator): `stream_builder` wrapper catches `ValueError` (starfix 0.5).
- Review findings (merge point h), plan approved: read-only operator node data context,
  side-effect check before every run, fresh-store wording, two nitpicks. Declined: event
  loop. Moot: two findings on removed files.
- Golden regenerated once (b14a56c7 + 1), attributed by regenerating at each merge commit
  (scratchpad scripts `attribute.py`, `entries.py`; result in
  `superpowers/plans/2026-11-12-itl-967-golden-entries.md`). Pins outside golden
  re-recorded: `hash_samples/schema_hash_golden.json`, `test_canonical_type_names.py`,
  `test_struct_layout_identity.py`, spiral fake `TestSourceId`.
- Nov 12: main (#316, ITL-955) merged, golden regenerated once more (6 group_by rows);
  design-issue entries moved to `design_issues/` (ITL-968).
- Verified at c4b1e4a7: Python 3.11, 3.12, 3.13 each 8525 passed, 101 skipped, 18 xfailed;
  ruff clean; basedpyright exit 0.
- PR #324 open against main. NEXT: CI green, CodeRabbit rounds (one push per round),
  answer review-body findings too, then `worker_done`.
