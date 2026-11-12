# Changelog Fragments Implementation Plan (ITL-961)

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan
> task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A pull request adds `changelog.d/<ISSUE-ID>.md` instead of editing `CHANGELOG.md`; the
release PR folds the fragments into `CHANGELOG.md` with a standard-library script.

**Architecture:** One script, `scripts/assemble_changelog.py`, owns the fragment rules (file
name, category headings, non-empty) and the fold. Tests drive it as a command
(`python scripts/assemble_changelog.py ...`), and one test runs `--check` on the repository's
own `changelog.d/` so CI rejects a malformed fragment.

**Tech stack:** Python standard library only (`argparse`, `pathlib`, `re`). No new dependency.

---

## Decisions

- **Category headings** (exactly those `CHANGELOG.md` uses in `[0.3.0]` and `[0.2.0]`), in
  this canonical order: `### Breaking changes and migration`, `### Added`, `### Changed`,
  `### Fixed`, `### Known limitations`. The older `### New` / `### Breaking Changes` of
  `0.1.1` are not accepted.
- **Fragment name:** `<TEAM>-<number>.md` (`ITL-955.md`, `PLT-12.md`). `README.md` is the only
  other file allowed in `changelog.d/`.
- **Fragment body:** only `### <category>` headings, each followed by non-empty text (bullets,
  `####` sub-headings, paragraphs, as `CHANGELOG.md` has today). Text before the first heading,
  an unknown or repeated heading, an empty category, a `#`/`##` heading, or an empty file is an
  error. `#` lines inside fenced code blocks are not headings.
- **Fold:** fragments in order of (team key, issue number). For each category, the fragment
  text is appended at the end of that category in the release section; existing lines are
  never reordered or changed. A category missing from the section is inserted in canonical
  order (before the first existing later category, else at the end of the section). A missing
  release section is created above the first `## [` section as `## [X.Y.Z] — unreleased`.
  When the existing category ends in a bullet and the appended text starts with one, they are
  joined without a blank line (one list); otherwise one blank line separates them.
- **Release heading / date** stay a manual step in `RELEASING.md` (unchanged scope).
- **CLI:** `uv run python scripts/assemble_changelog.py --version 0.3.0` folds and deletes;
  `--check` validates only (no `--version` needed). `--changelog` and `--fragments-dir`
  default to the repository's paths. Exit 0 on success, 1 with one line per problem on stderr.
- **This PR's fragment:** none. The change is contributor process, not a user-visible change
  of the library, so it has no changelog text.
- **Docs/tests reading `CHANGELOG.md`:** none (`mkdocs.yml` and the doc-sample tests do not
  reference it; the sdist is `only-include`, so `changelog.d/` and `scripts/` are not shipped).

## Tasks

### Task 1: failing tests

- [ ] Create `tests/test_changelog_fragments.py` running the script with `subprocess`:
  check mode (valid, wrong name, unknown heading, empty, text before heading, empty category,
  heading inside a code fence), fold (appends to the right categories without reordering,
  order by issue number, inserts a missing category in canonical order, creates a missing
  section, deletes fragments, keeps `README.md`, leaves other sections byte-identical, refuses
  to fold and changes nothing when a fragment is malformed), and
  `test_repository_fragments_are_well_formed` on the real `changelog.d/`.
- [ ] `uv run pytest tests/test_changelog_fragments.py` fails (script missing).

### Task 2: script

- [ ] Create `scripts/assemble_changelog.py`; tests pass; ruff and basedpyright clean.

### Task 3: convention and docs

- [ ] `changelog.d/README.md`.
- [ ] "Changelog" section in `CLAUDE.md` and `.zed/rules`.
- [ ] `RELEASING.md` "Before You Cut" step 1: run the script, then set the heading and date.
- [ ] Commit, push.

### Task 4: finish

- [ ] Full `uv run pytest tests/`, ruff check, ruff format --check, basedpyright.
- [ ] PR against `main` (`Fixes ITL-961`), CodeRabbit rounds, CI green, `worker_done`.

## Notes for a successor

- PR #320 open. CodeRabbit round 1 (2 findings: fence matching, unclosed fence) accepted by coordinator and fixed.
