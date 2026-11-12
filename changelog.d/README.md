# Changelog fragments

A pull request never edits `CHANGELOG.md` (developer, November 11, 2026; ITL-961). It adds
exactly one file here instead, so two open pull requests never conflict on the changelog.

## The rule

- The file is `changelog.d/<ISSUE-ID>.md`, for example `changelog.d/ITL-955.md`. A pull
  request that resolves several issues uses its primary issue's id.
- The file holds only the text that would have gone under the release heading in
  `CHANGELOG.md`: the same category headings `CHANGELOG.md` uses, each followed by its
  bullet lines. No version heading, no date.
- If your branch already edits `CHANGELOG.md`, restore `CHANGELOG.md` to its state on the
  branch's base and move that text, unchanged, into the fragment.
- A change with nothing to tell users (tests, internal tooling) adds no fragment.

The category headings, in the order they appear in a release section:

```markdown
### Breaking changes and migration
### Added
### Changed
### Fixed
### Known limitations
```

Use only the ones you need. Under a heading, write what `CHANGELOG.md` has today: bullet
lines, or a `####` sub-heading followed by paragraphs for a change that needs explaining.

## Example

`changelog.d/ITL-955.md`:

```markdown
### Added

- **`orcapod.errors.StoreError`** — the base of `StoreReadError` and `StoreWriteError`.
  (ITL-955 #301)

### Fixed

- `Join` no longer raises on an empty right-hand stream. (ITL-955 #301)
```

## Checking a fragment

```
uv run python scripts/assemble_changelog.py --check
```

fails if a file in this directory other than `README.md` is not named `<ISSUE-ID>.md`, is
empty, has text before its first category heading, has a heading that is not one of the
categories above (`####` and deeper are fine), repeats a category, leaves a category empty,
or has a code fence that is never closed. `tests/test_changelog_fragments.py` runs the same
check, so CI rejects a malformed fragment.

## At release

The release pull request folds the fragments into `CHANGELOG.md` and deletes them (see
`RELEASING.md`):

```
uv run python scripts/assemble_changelog.py --version 0.3.0
```

Each category's text is appended to the same category of the `## [0.3.0]` section, in order
of issue id (team key, then issue number). Lines already in `CHANGELOG.md` are not moved or
changed: entries that were in `CHANGELOG.md` on `main` before this convention stay there.
This file stays; only the fragments are deleted.
