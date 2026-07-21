# Release Cutting: Optional Branch Argument

**Issue:** ITL-545
**Date:** 2026-07-21
**Status:** Approved

## Overview

The release workflow (`release.yml`) currently accepts only a `version` input and
implicitly cuts the release from `main` (via GitHub Actions' default dispatch ref). This
design adds an optional `branch` input that defaults to `main`, enabling release-cutting
from maintenance branches, hotfix branches, or any other named branch without merging to
`main` first.

## Goals & Success Criteria

- `release.yml` accepts a `branch` input alongside `version`; omitting it behaves
  identically to today (cuts from `main`)
- The release tag is created at the tip of the specified branch
- Tests run against the specified branch
- A typo in `branch` fails immediately (before tests run) with a clear error message
- `RELEASING.md` documents the new parameter with a hotfix example

## Design

### New `branch` input

Added to `workflow_dispatch.inputs`:

```yaml
branch:
  description: 'Branch to cut the release from (defaults to main)'
  required: false
  type: string
  default: main
```

When omitted, `${{ inputs.branch }}` evaluates to `"main"` throughout the workflow —
identical behavior to today.

### New `validate-branch` pre-flight job

A dedicated job that runs before `test` and `license-check`. Uses the built-in
`GITHUB_TOKEN` to query the GitHub REST API — no checkout required. Fails immediately
with a clear error message if the branch does not exist.

```yaml
validate-branch:
  name: Validate release branch
  runs-on: ubuntu-latest
  timeout-minutes: 5
  steps:
    - name: Check branch exists on origin
      env:
        GH_TOKEN: ${{ github.token }}
      run: |
        BRANCH="${{ inputs.branch }}"
        if ! gh api "repos/${{ github.repository }}/branches/${BRANCH}" --silent 2>/dev/null; then
          echo "::error::Branch '${BRANCH}' does not exist. Check for typos."
          exit 1
        fi
        echo "Branch '${BRANCH}' confirmed."
```

### Updated job graph

```
validate-branch ─┬─ test ──────────┐
                 └─ license-check ─┤─ build ─┬─ publish-testpypi ─ publish-pypi ─┐
                                             └─ linear-sync ─────────────────────┴─ linear-complete
```

### Per-job changes

| Job | Change |
|-----|--------|
| `validate-branch` | New job — validates branch exists via GitHub API |
| `test` | Add `needs: [validate-branch]`; add `ref: ${{ inputs.branch }}` to `actions/checkout` |
| `license-check` | Add `needs: [validate-branch]` |
| `build` | Add `ref: ${{ inputs.branch }}` to `actions/checkout` — tag is created at tip of release branch |
| `publish-testpypi` | No change (downloads pre-built artifact, no checkout) |
| `publish-pypi` | No change (downloads pre-built artifact, no checkout) |
| `linear-sync` | No change (operates on the version tag, branch-agnostic) |
| `linear-complete` | No change (operates on the version tag, branch-agnostic) |

### Branch validation approach

- **No branch-pattern gating** — any existing branch may be used; policy enforcement is
  out of scope for this ticket.
- Validation is purely existence-checking: if the branch exists on origin, the workflow
  proceeds. If not, it fails before a single test minute is consumed.

## Documentation Changes

`RELEASING.md` is updated in two places:

1. **Step 2** of "Cutting a Release" — mention the optional `Branch` field in the
   workflow dispatch UI.

2. **New "Cutting a Hotfix Release" section** — explains the `branch` parameter and
   walks through a concrete hotfix example (e.g. `hotfix/0.1.x` → `v0.1.1`).

## Out of Scope

- Branch-pattern protection (e.g. only `main`, `release/*`, `hotfix/*`)
- Surfacing the release branch in Slack announcements (ITL-521)
- Changes to `release-sync.yml` (it watches pushes to `main` independently)
- Changes to `_license-check.yml` (its checkout uses the dispatch ref, which is
  acceptable — the license check validates installed dependencies, not branch-specific
  source)
