# Release Pipeline Design — PLT-1250

**Date:** 2026-04-08
**Issue:** [PLT-1250](https://linear.app/enigma-metamorphic/issue/PLT-1250/set-up-tag-triggered-cicd-pipeline-for-v010rc1-pypi-release)
**Status:** Approved

## Overview

Establish a fully automated, tag-triggered release pipeline for `orcapod-python` so that pushing a
version tag (e.g. `v0.1.0rc1`) builds, validates licenses, and publishes the package to PyPI.
The design mirrors the `starfix-python` release pipeline closely, extending it with orcapod-specific
requirements (system deps, Python version constraints, and existing CI jobs).

## Goals & Success Criteria

- Pushing a tag matching `v[0-9]+.[0-9]+.[0-9]+*` triggers the publish workflow
- The workflow runs tests, checks dependency licenses, builds artifacts, and publishes to PyPI
- `pip install orcapod==0.1.0rc1` works from PyPI after the workflow completes
- No copyleft (GPL/AGPL/LGPL) dependencies can reach PyPI — caught by license gate
- OIDC Trusted Publishing — no long-lived API tokens stored as secrets
- Release artifacts (wheel + sdist) attached to a GitHub Release with auto-generated notes
- The existing `run-tests.yml` CI gains the same license gates used in publish

## Architecture

### Build Backend Migration

Switch from `setuptools + setuptools-scm` to `hatchling + hatch-vcs` (mirrors starfix-python).
Behaviour is identical — git tag determines version automatically, written to
`src/orcapod/_version.py`. The migration requires updating `pyproject.toml` only; no source
code changes.

**Before:**
```toml
[build-system]
requires = ["setuptools>=64", "wheel", "setuptools-scm>=8"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
where = ["src"]

[tool.setuptools_scm]
version_file = "src/orcapod/_version.py"
```

**After:**
```toml
[build-system]
requires = ["hatchling", "hatch-vcs"]
build-backend = "hatchling.build"

[tool.hatch.version]
source = "vcs"

[tool.hatch.build.hooks.vcs]
version-file = "src/orcapod/_version.py"

[tool.hatch.build.targets.wheel]
packages = ["src/orcapod"]
```

`pip-licenses` is added to the `dev` dependency group to support the license-check job.

### Workflow: `.github/workflows/publish.yml` (new)

**Trigger:** tag push matching `v[0-9]+.[0-9]+.[0-9]+*`

The `*` suffix captures pre-release tags (e.g. `rc1`, `a1`, `b2`) in addition to stable releases.
All tags — stable and pre-release — flow through the full pipeline including real PyPI, so that
`pip install orcapod==0.1.0rc1` works without requiring `--index-url`.

**Job chain** (each job `needs` the previous):

```
test (Python 3.11 × 3.12)
  └─► license-check
        └─► build
              └─► publish-testpypi
                    └─► publish-pypi + GitHub Release
```

**Job details:**

| Job | Key steps | Notes |
|-----|-----------|-------|
| `test` | checkout (`fetch-depth: 0`), install graphviz, `uv sync --all-groups`, `pytest -m "not postgres"` | Matrix: 3.11, 3.12. `fetch-depth: 0` required for hatch-vcs to read tag |
| `license-check` | `uv sync --dev`, `pip-licenses --allow-only="..."` | Approved list: MIT, Apache-2.0, BSD-2-Clause, BSD-3-Clause, ISC, Python-2.0, PSF-2.0 |
| `build` | checkout (`fetch-depth: 0`), `uv build`, upload `dist/` as artifact | Single run, no matrix |
| `publish-testpypi` | download `dist/`, `uv publish --publish-url https://test.pypi.org/legacy/ dist/*` | Environment: `testpypi`; `permissions: id-token: write` |
| `publish-pypi` | download `dist/`, `uv publish dist/*`, create GitHub Release | Environment: `pypi`; `permissions: id-token: write, contents: write` |

### Workflow: `.github/workflows/run-tests.yml` (modified)

Add two new parallel jobs to the existing CI workflow, mirroring starfix's `ci.yml`:

- **`license-check`** — runs on every push and PR to `dev`/`main`. Same `pip-licenses` command as in publish.
- **`dependency-review`** — runs on PRs only. Uses `actions/dependency-review-action@v4` to deny
  GPL-2.0, GPL-3.0, AGPL-3.0, LGPL-2.0, LGPL-2.1, LGPL-3.0 (all `only` and `or-later` variants).

These jobs run independently of `test` (no `needs`), so they don't block or delay test results.

## Data Flow

```
git tag v0.1.0rc1
    │
    ▼
publish.yml triggered
    │
    ├─ test (3.11)  ─┐
    ├─ test (3.12)  ─┤ matrix, fail-fast: true
    │                │
    ▼                ▼
license-check (after test matrix passes)
    │
    ▼
build → dist/orcapod-0.1.0rc1-*.whl + dist/orcapod-0.1.0rc1.tar.gz
    │
    ▼
publish-testpypi (OIDC, testpypi environment)
    │
    ▼
publish-pypi (OIDC, pypi environment)
    │
    ▼
GitHub Release (auto-notes + dist/* attached)
```

## Error Handling

- **Test failure** — chain stops at `test`; nothing is built or published
- **License violation** — chain stops at `license-check`; build and publish blocked
- **Build failure** — chain stops at `build`; no artifacts uploaded, nothing published
- **TestPyPI failure** — chain stops; PyPI publish does not proceed (safety gate respected)
- **`fail-fast: true`** on test matrix — if one Python version fails, the other is cancelled immediately

## Files Changed

| File | Change |
|------|--------|
| `pyproject.toml` | Migrate build backend; add `pip-licenses` to dev deps |
| `uv.lock` | Regenerated after backend and dep changes |
| `.github/workflows/publish.yml` | New — tag-triggered release pipeline |
| `.github/workflows/run-tests.yml` | Add `license-check` and `dependency-review` jobs |

## Manual Setup Required (out of scope for this PR)

The following must be configured by a human in the GitHub and PyPI UIs before the first tag is pushed:

1. **PyPI Trusted Publisher** — at https://pypi.org/manage/account/publishing/:
   - Owner: `nauticalab`
   - Repository: `orcapod-python`
   - Workflow: `publish.yml`
   - Environment: `pypi`

2. **TestPyPI Trusted Publisher** — at https://test.pypi.org/manage/account/publishing/:
   - Same fields, environment: `testpypi`

3. **GitHub Environments** — in the repo Settings → Environments:
   - Create `pypi` and `testpypi` environments (can add optional protection rules / reviewers)

## References

- [starfix-python publish.yml](https://github.com/nauticalab/starfix-python/blob/main/.github/workflows/publish.yml)
- [starfix-python ci.yml](https://github.com/nauticalab/starfix-python/blob/main/.github/workflows/ci.yml)
- [PyPI Trusted Publishing docs](https://docs.pypi.org/trusted-publishers/)
- [hatch-vcs](https://github.com/ofek/hatch-vcs)
- [uv publish](https://docs.astral.sh/uv/guides/publish/)
