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
- No copyleft (GPL/AGPL/LGPL) dependencies can reach PyPI — caught by license gate before publish
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

The `[project.urls]` Homepage entry will also be corrected from `walkerlab/orcapod-python` to
`nauticalab/orcapod-python` as part of this change.

### Workflow: `.github/workflows/publish.yml` (new)

**Trigger:** tag push matching `v[0-9]+.[0-9]+.[0-9]+*`

The `*` suffix captures pre-release tags (e.g. `rc1`, `a1`, `b2`) in addition to stable releases.
All tags — stable and pre-release — flow through the full pipeline including real PyPI, so that
`pip install orcapod==0.1.0rc1` works without requiring `--index-url`.

**Job chain** (each job `needs` the previous):

```
test (Python 3.11 × 3.12, fail-fast: true)
  └─► license-check
        └─► build
              └─► publish-testpypi
                    └─► publish-pypi + GitHub Release
```

**TestPyPI gate rationale:** All releases (including pre-releases) publish to TestPyPI first as a
validation gate before the real PyPI publish. This is the pattern established by starfix-python.
Note: once a version is uploaded to TestPyPI it cannot be re-uploaded with the same version string
— if the TestPyPI step succeeds but PyPI subsequently fails, the release must be re-tagged with a
different version (e.g. `v0.1.0rc2`). This is acceptable for a pre-v1 project.

**Job details:**

| Job | Checkout | Key steps | Notes |
|-----|----------|-----------|-------|
| `test` | `fetch-depth: 0` | Install graphviz, `uv sync --all-groups`, `pytest -m "not postgres"` | Matrix: 3.11, 3.12. `fetch-depth: 0` required for hatch-vcs to resolve tag |
| `license-check` | `fetch-depth: 0` | `uv sync --dev`, `pip-licenses --allow-only="..." --partial-match` | `fetch-depth: 0` needed because `uv sync` installs the project via hatch-vcs |
| `build` | `fetch-depth: 0` | `uv build`, upload `dist/` as artifact | Single run, no matrix |
| `publish-testpypi` | none | Download `dist/`, `uv publish --publish-url https://test.pypi.org/legacy/ dist/*` | Environment: `testpypi`; `permissions: id-token: write` |
| `publish-pypi` | none | Download `dist/`, `uv publish dist/*`, create GitHub Release | Environment: `pypi`; `permissions: id-token: write, contents: write` |

**Approved license list for `pip-licenses`:**
```
MIT;Apache-2.0;BSD-2-Clause;BSD-3-Clause;ISC;Python-2.0;PSF-2.0
```
The `--partial-match` flag is required so that licenses reported by packages as free-form strings
(e.g. `"BSD License"`, `"Apache Software License"`) match against SPDX identifiers in the allowlist.

**GitHub Release creation** uses `softprops/action-gh-release@v2` with `generate_release_notes: true`
and `files: dist/*` — the same action used in starfix-python.

**`uv sync` scope in `license-check`:** Uses `--dev` (installs project + dev dependency group only).
This is sufficient because `pip-licenses` scans the entire installed environment including all
transitive runtime dependencies. The optional extras (`ray`, `redis`, `postgresql`, `spiraldb`) are
not installed in this job; if those extras introduce a dependency with a non-approved license, the
regular `test` job (which runs `--all-groups`) would surface build/import errors, but the license
gate itself would not catch it. This is an acceptable trade-off — adding `--all-extras` to the
license-check job would significantly increase its runtime.

### Workflow: `.github/workflows/run-tests.yml` (modified)

Add two new parallel jobs to the existing CI workflow, mirroring starfix's `ci.yml`. Both jobs run
independently (no `needs`) so they do not block or delay `test` or `spiral-integration`:

- **`license-check`** — runs on every push and PR to `dev`/`main`. Same `pip-licenses` invocation
  as in publish (with `--partial-match`). Requires checkout with `fetch-depth: 0`.
- **`dependency-review`** — runs on PRs only. Uses `actions/dependency-review-action@v4` to deny
  GPL-2.0, GPL-3.0, AGPL-3.0, LGPL-2.0, LGPL-2.1, LGPL-3.0 (all `only` and `or-later` variants).

The `spiral-integration` job is unaffected — it still only `needs: test`. A `license-check`
failure in `run-tests.yml` does not block `spiral-integration`; it is a separate blocking signal
for PRs and pushes independently.

## Data Flow

```
git tag v0.1.0rc1
    │
    ▼
publish.yml triggered
    │
    ├─ test (3.11)  ─┐
    ├─ test (3.12)  ─┘ matrix, fail-fast: true
    │
    ▼
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
GitHub Release via softprops/action-gh-release@v2 (auto-notes + dist/* attached)
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
| `pyproject.toml` | Migrate build backend to hatchling+hatch-vcs; fix Homepage URL; add `pip-licenses` to dev deps |
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
   - Create `pypi` and `testpypi` environments (can add optional protection rules / required reviewers)

## References

- [starfix-python publish.yml](https://github.com/nauticalab/starfix-python/blob/main/.github/workflows/publish.yml)
- [starfix-python ci.yml](https://github.com/nauticalab/starfix-python/blob/main/.github/workflows/ci.yml)
- [PyPI Trusted Publishing docs](https://docs.pypi.org/trusted-publishers/)
- [hatch-vcs](https://github.com/ofek/hatch-vcs)
- [uv publish](https://docs.astral.sh/uv/guides/publish/)
- [softprops/action-gh-release](https://github.com/softprops/action-gh-release)
