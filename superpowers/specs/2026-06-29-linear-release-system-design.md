# Linear Release System for orcapod-python

**Date:** 2026-06-29  
**Issue:** ITL-448  
**Status:** Approved

## Overview

Configure Linear's native release system for `nauticalab/orcapod-python` so that when the
v0.1 release tag is pushed, all issues in "Merged" status whose PRs are included in the
release automatically transition to "Done".

The implementation mirrors the existing setup in `metamorphic-brain/axon` to keep release
tooling consistent across Metamorphic repos.

## Goals & Success Criteria

- `release-sync.yml` runs on every push to `main`, associating merged PRs with the open
  Linear release draft.
- On tag push, `linear-sync` (with version) runs in parallel with the publish pipeline,
  finalising which commits belong to the release.
- After `publish-pypi` succeeds (PyPI + GitHub Release created), `linear-complete` fires
  and transitions all Merged issues linked to that release to Done.
- `RELEASING.md` documents the Linear release system so the pattern can be replicated for
  other orcapod repos.

## Scope & Boundaries

In scope:
- Two new GitHub Actions workflow jobs/files wiring `linear/linear-release-action` into
  the existing publish pipeline.
- Update to `RELEASING.md`.

Out of scope:
- Changelog generation.
- Applying this to other orcapod repos (separate issues).
- Linear workspace configuration (release definition, status transitions) — this must be
  set up manually in the Linear UI by the workspace admin before the first release.

## Architecture

### Trigger: push to `main` — `release-sync.yml`

```
push to main → sync (no version)
```

Associates any PR merged to `main` with the currently-open Linear release draft.
Mirrors axon's `release-sync.yml` exactly.

### Trigger: tag push `v*` — `publish.yml`

```
linear-sync ─┐
             │
license-check → test → build → publish-testpypi → publish-pypi → linear-complete
```

- **`linear-sync`** (no dependencies, parallel): calls `linear-release-action sync
  --version <tag>`. Finalises the commit set for this release version in Linear.
- **`linear-complete`** (`needs: [publish-pypi]`): calls `linear-release-action complete
  --version <tag>`. Triggers the Merged → Done state transition in Linear.

### Why `linear-complete` is a separate job (not a step in `publish-pypi`)

The `pypi` GitHub environment has a required-reviewer protection rule (eywalker must
approve before `publish-pypi` runs). Adding non-publish logic inside that job would mix
concerns and make the gated job larger. A separate `linear-complete` job runs after
`publish-pypi` completes with no environment gate of its own, keeping `pypi` environment
focused on PyPI credentials only.

## Files

### New: `.github/workflows/release-sync.yml`

Exact mirror of `metamorphic-brain/axon`'s `release-sync.yml`:

```yaml
name: Linear release sync

on:
  push:
    branches:
      - main

jobs:
  sync:
    runs-on: ubuntu-latest
    timeout-minutes: 5
    permissions:
      contents: read
    steps:
      - uses: actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683  # v4.2.2
        with:
          fetch-depth: 0
      - uses: linear/linear-release-action@c0cb8354a362c24c6d3e0948f37fd66d07588e3f  # v0
        with:
          access_key: ${{ secrets.LINEAR_ACCESS_KEY }}
          command: sync
```

### Modified: `.github/workflows/publish.yml`

Two new jobs appended after the existing jobs:

```yaml
  linear-sync:
    runs-on: ubuntu-latest
    timeout-minutes: 5
    permissions:
      contents: read
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
      - uses: linear/linear-release-action@c0cb8354a362c24c6d3e0948f37fd66d07588e3f  # v0
        with:
          access_key: ${{ secrets.LINEAR_ACCESS_KEY }}
          command: sync
          version: ${{ github.ref_name }}

  linear-complete:
    needs: [publish-pypi]
    runs-on: ubuntu-latest
    timeout-minutes: 5
    permissions:
      contents: read
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0
      - uses: linear/linear-release-action@c0cb8354a362c24c6d3e0948f37fd66d07588e3f  # v0
        with:
          access_key: ${{ secrets.LINEAR_ACCESS_KEY }}
          command: complete
          version: ${{ github.ref_name }}
```

### Modified: `RELEASING.md`

New section: **Linear Release System** covering:
- What it does (Merged → Done on release cut)
- The `LINEAR_ACCESS_KEY` secret requirement
- The two-phase flow (push-to-main sync → tag-push sync + complete)
- How to replicate for other orcapod repos

## Manual Prerequisites

Before the first release, a workspace admin must:

1. **Set the `LINEAR_ACCESS_KEY` repo secret** in `nauticalab/orcapod-python` GitHub
   settings. Use the same Linear API key as `metamorphic-brain/axon`.
2. **Configure a Linear release** in the workspace for orcapod-python (release name,
   included statuses, target state for the Merged → Done transition). This is done via
   the Linear UI.

These steps are outside the scope of this PR but are required for the workflows to succeed.

## Deviations from axon

| Concern | axon | orcapod-python |
|---|---|---|
| Tag trigger pattern | `v*` | `v[0-9]*.[0-9]*.[0-9]*` (existing `publish.yml` pattern) |
| `complete` step location | inside `release` job | separate `linear-complete` job (due to `pypi` environment gate) |
| Binary build jobs | yes (Rust binaries) | no (Python wheel built by `uv build`) |
| Action pin style | pinned digest | pinned digest for Linear action; `@v4`/`@v5` for others (matches existing `publish.yml` style) |

## Replication Guide

To apply this pattern to another orcapod repo:

1. Copy `.github/workflows/release-sync.yml` verbatim.
2. Add `linear-sync` and `linear-complete` jobs to the repo's publish/release workflow,
   adjusting the `needs:` chain to point at whichever job creates the GitHub Release.
3. Set `LINEAR_ACCESS_KEY` repo secret.
4. Configure a Linear release in the workspace for the new repo.
