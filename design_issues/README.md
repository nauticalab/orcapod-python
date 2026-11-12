# Design issues log

The log of known design problems, bugs and code-quality issues: one file per entry
(developer, November 11, 2026; ITL-968). It replaces the single file `DESIGN_ISSUES.md`, so
two pull requests that add or update different entries never conflict.

## Reading the log

```
uv run python scripts/design_issues.py                 # index: every entry by area
uv run python scripts/design_issues.py --status open   # only open / in progress / resolved
uv run python scripts/design_issues.py --full          # the text of every entry, by area
```

The index gives each entry's id, status, severity, Linear issue, file name and title under
its area heading, and ends with a count by status. To find one entry, search this directory for its
id: `ls design_issues/*-GB1.md`.

## An entry file

The file is `design_issues/<LINEAR-ISSUE-ID>-<ENTRY-ID>.md`, for example
`design_issues/ITL-955-GB1.md`:

```markdown
## `src/orcapod/core/operators/group_by.py` — tags entering the data group (ITL-955)

### GB1 — `GroupBy` moved non-key tag values into data columns
**Status:** resolved
**Severity:** high
**Issue:** ITL-955

What is wrong, where, and what it causes.

**Fix:** What was changed, once it is resolved.
```

- Line 1 is the `## ` area heading the entry belongs under: usually the file or package it
  concerns. Entries whose area heading is the same text are listed together, so copy the
  heading of an existing entry when yours belongs to the same area.
- Then a blank line and `### <ENTRY-ID> — <title>` (an em dash). The entry id is a short
  tag in capital letters and digits, for example `GB1`; pick one that no other entry uses
  (`ls design_issues/`). It is the id in the file name.
- Directly under the heading: `**Status:**`, `**Severity:**`, `**Issue:**`, in that order.
  - Status is `open`, `in progress` or `resolved`. A qualification may follow the word
    (`resolved (local stores); cloud stores remain exposed upstream`).
  - Severity is one of the values in the guide below.
  - Issue names the Linear issue; the file name starts with an id from this line.
- Then a blank line and the body. One entry per file.

## Severity guide

- **critical** — Correctness bugs, silent data loss, or security issues.
- **high** — Broken or incomplete features that affect users or downstream consumers.
- **medium** — Performance, error-handling, or code-quality issues worth fixing in the
  normal course of development.
- **low** — Nice-to-haves, cosmetic improvements, or speculative refactors.
- **trivial** — Typos, dead comments, purely cosmetic.

## Adding an entry

Add one new file in the shape above. Never append an entry to another entry's file, and do
not bring back `DESIGN_ISSUES.md`.

## Updating an entry

Edit that entry's file in place: set `**Status:**` to `in progress` while working on it and
to `resolved` once done, and add a brief `**Fix:**` note. Keep the file name; it changes
only if the entry id or the issue it is named for changes.

## Checking

```
uv run python scripts/design_issues.py --check
```

fails if a file in this directory other than `README.md` is not named
`<LINEAR-ISSUE-ID>-<ENTRY-ID>.md`, is empty, does not start with one `## ` area heading
a blank line, and one `### <ENTRY-ID> — <title>` heading, has an entry id that differs from the
one in its name, lacks a valid `**Status:**` or `**Severity:**` line, is named for an issue
its `**Issue:**` line does not mention, has no body, or has a code fence that is never
closed. `tests/test_design_issues.py` runs the same check, so CI rejects a malformed entry.

## Entries older than this convention

The entries that were in `DESIGN_ISSUES.md` were moved here with their text unchanged by
`scripts/migrate_design_issues.py`. Three things in them are tolerated and are not for new
entries:

- **No Linear issue.** An entry with no `**Issue:**` line is named `NOISSUE-<ENTRY-ID>.md`.
  The check accepts `NOISSUE` only for a file with no `**Issue:**` line. When such an entry
  gets a Linear issue, add the line and rename the file.
- **A repeated entry id.** Two old entries are both `H1`; the second file is
  `NOISSUE-H1-2.md`. A `-<n>` after the entry id exists only to tell such files apart.
- **No severity.** `NOISSUE-PF1.md` has no `**Severity:**` line; it is the one file the
  check exempts.

A branch that still edits an entry in `DESIGN_ISSUES.md` conflicts once when it merges
`main` (the file is deleted there). While that merge is in progress, keep the branch's
copy, then:

```
uv run python scripts/migrate_design_issues.py --source DESIGN_ISSUES.md
git rm -f DESIGN_ISSUES.md
git diff MERGE_HEAD --stat -- design_issues/
```

The script prints `created:` for each entry the branch added and `updated:` for each entry
file whose text differs from the branch's copy. That copy is older than `main` for every
entry `main` changed after the branch started, so an `updated:` file is not always yours:
the last command must list only the entries your branch added or edited. Put every other
file back before committing the merge:

```
git checkout MERGE_HEAD -- design_issues/<file>
```
