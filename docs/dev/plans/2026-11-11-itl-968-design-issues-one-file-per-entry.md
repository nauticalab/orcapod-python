# Design issues log: one file per entry (ITL-968) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the shared `DESIGN_ISSUES.md` with one file per entry in `design_issues/`,
so two pull requests that add or update different entries never conflict.

**Architecture:** A one-off, re-runnable migration script splits `DESIGN_ISSUES.md` into
`design_issues/<PREFIX>-<ENTRY-ID>.md` files and proves the split is lossless. A second
stdlib-only script reads the directory to print the log as a whole (index or full text) and to
validate every entry file; a test runs the validation in the normal suite. Model: PR #320
(`changelog.d/`, `scripts/assemble_changelog.py`, `tests/test_changelog_fragments.py`).

**Tech Stack:** Python standard library only, pytest.

---

## Facts found (origin/main d8bba28e)

- `DESIGN_ISSUES.md`: 2712 lines, 14-line preamble (title, severity guide), 103 entries under
  50 `## ` area headings. Between entries there is only glue: blank lines, `---` separators
  and `## ` area headings (no area-level prose). Headings inside code fences (`# TODO ...`)
  must be ignored, so the parser is fence-aware.
- 45 entries have an `**Issue:**` line, 58 have none. Nine `**Issue:**` values hold more than
  one id (`ITL-832 (with ITL-813; related: ITL-835, ITL-839)`); the first id is the primary.
- Entry ids repeat twice: `FN1` (one with ITL-615... one without an issue, so the file names
  differ) and `H1` (both without an issue: `hashing/semantic_hashing/` line 1010 and
  `hashing/` line 1195). `H1` is the only file-name collision.
- Status values: `open` 49, `resolved` 44, `in progress` 8, and two qualified ones:
  ``resolved (`tag_data.py` half), open (`arrow_utils.py` half)`` (U1) and
  `resolved (local stores); cloud stores remain exposed upstream` (DX1).
- `PF1` has no `**Severity:**` line (and no `**Issue:**` line).
- The area heading `` ## `src/orcapod/core/nodes/function_node.py` `` occurs twice,
  non-adjacent.
- Open PRs #311, #316, #317 already add `design_issues/ITL-943-DN1.md`,
  `ITL-955-GB1.md`, `ITL-939-TD1.md` in the fixed shape (area heading, blank line, `### `,
  Status/Severity/Issue, blank, body, one trailing newline).

## Decisions (sent to the coordinator through `ask`; see "Coordinator answers" below)

1. **Entries without a Linear issue:** fixed prefix `NOISSUE`, e.g.
   `design_issues/NOISSUE-B1.md`. Allowed only for a file whose entry has no `**Issue:**`
   line; an entry with an `**Issue:**` line must be named by one of the ids on that line (the
   migration uses the first).
2. **`H1` collision:** the second `H1` file needs a different name.
3. **What remains of `DESIGN_ISSUES.md`:** removed; `design_issues/README.md` is the landing
   page and `scripts/design_issues.py` prints the log as a whole. No committed generated
   index (it would be a shared file that conflicts again).
4. **Irregular legacy entries** (qualified status, `PF1` without severity): text stays
   unchanged; the checker accepts a status that starts with a valid status word, and has one
   named exemption for `PF1`'s missing severity.
5. **Migration script location:** `scripts/migrate_design_issues.py`, committed.

## Coordinator answers

Approved, all as recommended (coordinator, via `ask`): (1) `NOISSUE` prefix; (2) option A,
`NOISSUE-H1-2.md`, text unchanged, check allows an optional `-<n>` after the entry id;
(3) remove `DESIGN_ISSUES.md`, README landing page, `scripts/design_issues.py`, a test that
it is not recreated; (4) status must start with a valid value, one named exemption for
`PF1`'s missing severity; (5) migration script committed; (6) references as listed.

Two additions to the `CLAUDE.md` + `.zed/rules` edit: (a) correct "`GroupBy` is the only
operator that reduces row count many→one" (`Batch` also reduces); (b) Changelog section:
a category heading is followed by bullets or `####` sub-sections, not only "bullet lines".
The check must accept the files other PRs already add (`ITL-939-TD1.md`, `ITL-943-DN1.md`,
`ITL-955-GB1.md`, `ITL-881-UCI1.md`): verified, all four pass `--check`.

## File structure

- Create `scripts/migrate_design_issues.py` — split a `DESIGN_ISSUES.md` text into entry
  files; `--verify` proves the files plus glue reproduce the source byte for byte.
- Create `scripts/design_issues.py` — `--check` validates `design_issues/`; default prints the
  index (area, id, title, status, severity, issue, file); `--full` prints every entry's text
  grouped by area; `--status` filters.
- Create `design_issues/README.md` — the convention, severity guide, how to read the log.
- Create `design_issues/*.md` — 103 migrated entries (generated).
- Create `tests/test_design_issues.py` — drives `scripts/design_issues.py` as a command.
- Delete `DESIGN_ISSUES.md` (pending decision 3).
- Modify `CLAUDE.md`, `.zed/rules` ("Design issues log"), `AGENT_COORDINATION.md` (lines 52,
  107), `TESTING_PLAN.md:14`, and the `DESIGN_ISSUES` mentions in `src/` and
  `tests/test_core/operators/test_group_by.py` (comments, docstrings, error text).
- Not modified: `CHANGELOG.md` (rule), `superpowers/plans|specs/*`, `docs/metamorphic/plans/*`,
  `function-execution-improvements-plan.md` (historical records).
- No changelog fragment: nothing user-facing (pending coordinator view on the reworded error
  text that cites `ET1`/`ON1`).

## Entry file rules enforced by `--check`

- Name: `^(NOISSUE|[A-Z][A-Z0-9]*-[0-9]+)-([A-Z][A-Z0-9]*)\.md$`; `README.md` and dotfiles
  skipped; anything else in the directory is a problem.
- Line 1 is a `## ` area heading; the first `### ` line (outside code fences) is
  `### <ENTRY-ID> — <title>` with the id equal to the id in the file name; exactly one `### `
  entry heading and one `## ` heading per file (outside fences).
- The lines directly after the entry heading are `**Status:**`, `**Severity:**`, then
  optionally `**Issue:**`.
- Status starts with `open`, `in progress` or `resolved` (a qualification may follow).
- Severity is `critical`, `high`, `medium`, `low` or `trivial`.
- `NOISSUE` name ⇔ no `**Issue:**` line; otherwise the name's Linear id is one of the ids on
  the `**Issue:**` line.
- The entry has body text after the field lines; code fences are closed; file ends with a
  newline.

## Tasks

### Task 1: checker and index (`scripts/design_issues.py`) — TDD

- [ ] Write `tests/test_design_issues.py` (subprocess, `tmp_path` directories, as
      `tests/test_changelog_fragments.py` does): accepts a README-only directory; accepts a
      valid entry; parametrized rejections (wrong names; missing area heading; missing
      status; unknown status `done`; missing severity; unknown severity `urgent`; id in name
      differs from heading; `NOISSUE` name with an `**Issue:**` line; issue-named file whose
      `**Issue:**` line lacks that id; two entries in one file; unclosed fence; empty file);
      heading-like lines inside a fence ignored; every problem reported; index lists by
      area with id, title, status, severity; `--status open` filters; `--full` prints texts.
- [ ] Run `uv run pytest tests/test_design_issues.py` — expect failures (script missing).
- [ ] Implement `scripts/design_issues.py`; rerun — pass. Commit.

### Task 2: migration script

- [ ] Implement `scripts/migrate_design_issues.py`:
      `--source DESIGN_ISSUES.md --out design_issues [--verify]`. Tokenize the source
      (fence-aware) into preamble, glue lines (blank, `---`, `## ` heading) and entry blocks
      (from `### ` to the last non-blank line before the next glue or `### `). Each file is
      `<area heading>\n\n<entry block>\n`. `--verify` re-reads the files and checks that the
      source equals preamble + glue + file entry blocks in source order, and that each file's
      first line is the area heading in force at that point.
- [ ] Tests for it in `tests/test_design_issues.py` on a small synthetic log (split, names,
      duplicate-name rule, verify passes, verify fails after a file is edited).
- [ ] Commit.

### Task 3: migrate

- [ ] Run the migration and `--verify`; run `scripts/design_issues.py --check`; add the
      repository-level tests (`test_repository_entries_are_well_formed`, README exists,
      `DESIGN_ISSUES.md` absent). Delete `DESIGN_ISSUES.md`. Commit, push (WIP is fine).

### Task 4: docs and references

- [ ] `design_issues/README.md`; `CLAUDE.md` + `.zed/rules`; `AGENT_COORDINATION.md`;
      `TESTING_PLAN.md`; `src/` and test mentions. Run the affected tests (error-text
      assertions). Commit.

### Task 5: finish

- [ ] `uv run --with ruff ruff check .`, `ruff format --check .`,
      `uv run --with basedpyright basedpyright`, `uv run pytest tests/`.
- [ ] Merge `origin/main`; if `DESIGN_ISSUES.md` changed there, take main's version, re-run
      the migration and `--verify`, delete it again, rerun the suite.
- [ ] PR against `main` (`Fixes ITL-968`), with the verify output and re-run instructions in
      the body; `orca linear attach`; `orca worktree set --worktree current --pr <n>`.
- [ ] Readiness pipeline: CI green, CodeRabbit covers the latest head (one push per round).
- [ ] `worker_done`.

## Notes for a successor

- Tasks 1-4 done and pushed. `scripts/migrate_design_issues.py` stands alone (it does not
  import `scripts/design_issues.py`: the type checker resolves `design_issues` to the
  directory of that name).
- `.zed/rules` has no "only operator that reduces" sentence, so addition (a) changes
  `CLAUDE.md` only.
- To re-run after `main` moves: `git show origin/main:DESIGN_ISSUES.md > /tmp/x.md`, then
  `uv run python scripts/migrate_design_issues.py --source /tmp/x.md` and again with
  `--verify`; `uv run python scripts/design_issues.py --check`.
