# Release Workflow Design for orcapod-python

**Date:** 2026-06-30
**Issue:** ITL-449 — Add GitHub Actions release workflow to orcapod-python
**Reference:** `nauticalab/starfix-python` `.github/workflows/release.yml`

---

## Overview

Replace the existing tag-push-triggered `publish.yml` with a single `workflow_dispatch`-triggered
`release.yml` modeled on starfix-python's release workflow. The new workflow accepts a version
string as input, runs pre-flight validation, builds the package, creates and pushes the release
tag, publishes to TestPyPI and then PyPI, creates a GitHub Release, and closes out the Linear
release via the `linear/linear-release-action`.

---

## Goals & Success Criteria

- `orcapod-python` has `.github/workflows/release.yml` that replaces `publish.yml`.
- Releasing is triggered from the GitHub UI (Actions → Release → Run workflow) with a version
  input — no manual `git tag` step required.
- Workflow reliably produces: built wheel + sdist artifacts, a GitHub Release with the tag, and
  a published package on PyPI (staged through TestPyPI first).
- Linear release is synced and completed automatically after a successful PyPI publish.
- `RELEASING.md` documents the new workflow_dispatch process.

---

## Files Changed

| File | Action |
|---|---|
| `.github/workflows/release.yml` | Created (new) |
| `.github/workflows/publish.yml` | Deleted (replaced) |
| `RELEASING.md` | Updated |

`release-sync.yml` and `_license-check.yml` are **not changed**.

---

## Workflow: `release.yml`

### Trigger

```yaml
on:
  workflow_dispatch:
    inputs:
      version:
        description: 'Release version (e.g. 0.1.0 or v0.1.0 — leading v is stripped automatically)'
        required: true
        type: string
```

### Job Graph

```
[test (3.11)] ─┐
[test (3.12)] ─┤─ build ─ publish-testpypi ─ publish-pypi ─ linear-complete
[license-check]┘                                   │
                                           linear-sync ─────────────┘
```

All pre-flight jobs (`test`, `license-check`) run in parallel. `build` waits for all of them.
`publish-testpypi` waits for `build`. `publish-pypi` waits for `publish-testpypi`.
`linear-sync` runs after `build`. `linear-complete` needs both `publish-pypi` and `linear-sync`.

### Job: `test`

- Strategy matrix: Python `["3.11", "3.12"]` (`requires-python = ">=3.11"` rules out 3.10)
- `fail-fast: true`
- Installs system deps: `graphviz libgraphviz-dev` (required by orcapod)
- Syncs dev dependencies: `uv sync --locked --all-extras --dev --python <version>`
- Runs: `uv run --python <version> pytest -m "not postgres" --tb=short -q`

### Job: `license-check`

Calls the existing reusable workflow:

```yaml
license-check:
  uses: ./.github/workflows/_license-check.yml
```

### Job: `build`

- `needs: [test, license-check]`
- `permissions: contents: write` (required to push the release tag)
- Output: `version` (normalized, no leading `v`)

Steps:
1. **Normalize version** — strips leading `v` so `v0.1.0` and `0.1.0` both work; emits `version` output.
2. **Checkout** with `fetch-depth: 0` (required: `hatch-vcs` derives version from git tags).
3. **Configure git identity** — `github-actions[bot]`.
4. **Create local release tag** — `git tag "v<version>"` locally before building, so `hatch-vcs` can
   resolve the version during `uv build`. Tag is NOT pushed yet.
5. **Install uv** (pinned digest).
6. **Build** — `uv build` (produces wheel + sdist in `dist/`).
7. **Guard: check for duplicate tag** — fails clearly if `v<version>` already exists on origin.
8. **Push release tag** — `git push origin "v<version>"`.
9. **Upload artifact** — uploads `dist/` as artifact `dist`; `if-no-files-found: error`.

The tag is created locally before the build (so `hatch-vcs` can derive the version) but is pushed
to origin only after a successful build, preventing dangling remote tags on build failure.

### Job: `publish-testpypi`

- `needs: build`
- Environment: `testpypi` (URL: `https://test.pypi.org/p/orcapod`)
- `permissions: id-token: write` (OIDC trusted publishing)
- Downloads `dist` artifact; publishes via `uv publish --publish-url https://test.pypi.org/legacy/ dist/*`

### Job: `publish-pypi`

- `needs: [build, publish-testpypi]`
- Environment: `pypi` (URL: `https://pypi.org/p/orcapod`)
- `permissions: id-token: write, contents: write`
- Downloads `dist` artifact; publishes via `uv publish dist/*`
- Creates GitHub Release via `softprops/action-gh-release` (pinned digest):
  - `tag_name: "v${{ needs.build.outputs.version }}"`
  - `generate_release_notes: true`
  - `files: dist/*`

### Job: `linear-sync`

- `needs: build` (starts as soon as the tag is pushed; runs in parallel with the publish chain)
- `permissions: contents: read`
- Calls `linear/linear-release-action` (pinned digest) with `command: sync` and
  `version: "v${{ needs.build.outputs.version }}"` (version from build output, not `github.ref_name`)
- **Does not block PyPI publish** — nothing in the publish chain depends on this job

### Job: `linear-complete`

- `needs: [publish-pypi, linear-sync]`
- `permissions: contents: read`
- Calls `linear/linear-release-action` with `command: complete` and
  `version: "v${{ needs.build.outputs.version }}"`
- **Does not block anything** — a Linear failure after a successful PyPI publish is surfaced as
  a failed job but does not roll back the release

---

## Action Digest Pinning

All actions use pinned SHA digests with a `# vX` comment, matching starfix-python's convention:

| Action | Pinned to |
|---|---|
| `actions/checkout` | `34e114876b0b11c390a56381ad16ebd13914f8d5  # v4` |
| `astral-sh/setup-uv` | `e58605a9b6da7c637471fab8847a5e5a6b8df081  # v5` |
| `actions/upload-artifact` | `ea165f8d65b6e75b540449e92b4886f43607fa02  # v4.6.2` |
| `actions/download-artifact` | `d3f86a106a0bac45b974a628896c90dbdf5c8093  # v4.3.0` |
| `softprops/action-gh-release` | `3bb12739c298aeb8a4eeaf626c5b8d85266b0e65  # v2.6.2` |
| `linear/linear-release-action` | `c0cb8354a362c24c6d3e0948f37fd66d07588e3f  # v0` |

---

## Secrets & OIDC Prerequisites

| Requirement | Details |
|---|---|
| GitHub environment `testpypi` | Must exist; Trusted Publisher configured on test.pypi.org for this workflow file |
| GitHub environment `pypi` | Must exist; Trusted Publisher configured on pypi.org for this workflow file |
| `LINEAR_ACCESS_KEY` repo secret | Already set (from ITL-448); used by both linear jobs |

**Important:** If PyPI/TestPyPI Trusted Publisher configs still reference `publish.yml`, they must
be updated to reference `release.yml` before the first run.

---

## RELEASING.md Updates

The doc is updated to replace the manual tag-push flow with:

1. Go to **Actions → Release → Run workflow** in the GitHub UI.
2. Enter the version (e.g. `0.1.0`; a leading `v` is stripped automatically).
3. Click **Run workflow** — CI handles test, build, tag creation, PyPI publish, GitHub Release,
   and Linear release completion automatically.

The tag format table, pre-release guidance, and Linear release system explanation are retained.

---

## Out of Scope

- Inventing a new release flow distinct from starfix-python's pattern
- Adding a `workflow_dispatch` trigger to `publish.yml` instead of replacing it
- Multi-repo release coordination
- Backporting / hotfix workflows
