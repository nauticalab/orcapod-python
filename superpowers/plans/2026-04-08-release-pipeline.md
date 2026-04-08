# Release Pipeline Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a tag-triggered GitHub Actions pipeline that tests, validates dependency licenses, builds, and publishes `orcapod` to PyPI using OIDC Trusted Publishing — mirroring the `starfix-python` release pipeline.

**Architecture:** Migrate the build backend from `setuptools+setuptools-scm` to `hatchling+hatch-vcs` (identical auto-versioning from git tags). Add a new `publish.yml` workflow triggered by `v*` tags with a sequential job chain: `test → license-check → build → publish-testpypi → publish-pypi`. Extend the existing `run-tests.yml` CI with parallel `license-check` and `dependency-review` jobs.

**Tech Stack:** GitHub Actions, `uv` (build + publish), `hatchling`, `hatch-vcs`, `pip-licenses`, `softprops/action-gh-release@v2`, `actions/dependency-review-action@v4`

**Spec:** `superpowers/specs/2026-04-08-release-pipeline-design.md`

---

## File Map

| File | Action | What changes |
|------|--------|-------------|
| `pyproject.toml` | Modify | Build backend → hatchling+hatch-vcs; add `pip-licenses` to dev deps; fix Homepage URL |
| `uv.lock` | Regenerated | After pyproject.toml changes |
| `.github/workflows/publish.yml` | Create | Full tag-triggered release pipeline |
| `.github/workflows/run-tests.yml` | Modify | Add `license-check` and `dependency-review` jobs |

No source code changes. No new Python modules.

---

## Task 1: Migrate build backend in `pyproject.toml`

**Files:**
- Modify: `pyproject.toml`

The current file uses `setuptools+setuptools-scm`. Replace the build system config, package-find config, and scm config with their `hatchling+hatch-vcs` equivalents. Also add `pip-licenses` to the dev dependency group and fix the Homepage URL.

- [ ] **Step 1: Update `pyproject.toml` build system section**

Replace the `[build-system]` block:

```toml
# Remove this:
[build-system]
requires = ["setuptools>=64", "wheel", "setuptools-scm>=8"]
build-backend = "setuptools.build_meta"

# Replace with:
[build-system]
requires = ["hatchling", "hatch-vcs"]
build-backend = "hatchling.build"
```

- [ ] **Step 2: Replace setuptools package discovery and scm config**

Remove these two sections entirely from `pyproject.toml`:

```toml
[tool.setuptools.packages.find]
where = ["src"]

[tool.setuptools_scm]
version_file = "src/orcapod/_version.py"
```

Add these three sections in their place:

```toml
[tool.hatch.version]
source = "vcs"

[tool.hatch.build.hooks.vcs]
version-file = "src/orcapod/_version.py"

[tool.hatch.build.targets.wheel]
packages = ["src/orcapod"]
```

- [ ] **Step 3: Fix Homepage URL and add `pip-licenses` to dev deps**

In `[project.urls]`, change:
```toml
Homepage = "https://github.com/walkerlab/orcapod-python"
```
to:
```toml
Homepage = "https://github.com/nauticalab/orcapod-python"
```

In `[dependency-groups]` → `dev`, add `pip-licenses` anywhere in the list:
```toml
"pip-licenses>=5.0.0",
```

- [ ] **Step 4: Verify `uv build` works locally**

```bash
uv sync --all-groups
uv build
```

Expected: creates `dist/orcapod-*.whl` and `dist/orcapod-*.tar.gz` without errors.
The version will show as `0.0.0.dev0` or similar (no git tag on this branch — that's fine).
If hatch-vcs complains about missing tags, add `HATCH_VCS_PRETEND_VERSION=0.0.0` or run from within the repo with git history present.

- [ ] **Step 5: Verify pip-licenses passes**

```bash
uv run pip-licenses \
  --allow-only="MIT;Apache-2.0;BSD-2-Clause;BSD-3-Clause;ISC;Python-2.0;PSF-2.0" \
  --partial-match
```

Expected: exits 0 with a table of all dependency licenses. If any dependency triggers a violation, investigate and decide whether to expand the allowlist or remove the dependency.

- [ ] **Step 6: Run test suite to confirm no regressions from backend change**

```bash
uv run pytest -m "not postgres" --tb=short -q
```

Expected: same pass/skip counts as before (3190 passed, 10 skipped as of last run).

- [ ] **Step 7: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "chore(build): migrate build backend to hatchling+hatch-vcs"
```

---

## Task 2: Create `.github/workflows/publish.yml`

**Files:**
- Create: `.github/workflows/publish.yml`

This is the core release workflow. It triggers on tags matching `v[0-9]+.[0-9]+.[0-9]+*` (covers both stable `v0.1.0` and pre-release `v0.1.0rc1`). Five jobs run in sequence; failure at any stage stops the chain.

Critical detail: every job that runs `uv sync` or `uv build` must use `actions/checkout@v4` with `fetch-depth: 0`. Without the full git history, `hatch-vcs` cannot resolve the version from the tag and the build will fail.

- [ ] **Step 1: Create the workflow file**

Create `.github/workflows/publish.yml` with this exact content:

```yaml
name: Publish to PyPI

on:
  push:
    tags:
      - "v[0-9]+.[0-9]+.[0-9]+*"

jobs:
  test:
    name: Test (Python ${{ matrix.python-version }})
    runs-on: ubuntu-latest
    strategy:
      fail-fast: true
      matrix:
        python-version: ["3.11", "3.12"]
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0   # required: hatch-vcs needs full tag history

      - name: Install uv
        uses: astral-sh/setup-uv@v5

      - name: Set up Python ${{ matrix.python-version }}
        run: uv python install ${{ matrix.python-version }}

      - name: Install system dependencies
        run: sudo apt-get update && sudo apt-get install -y graphviz libgraphviz-dev

      - name: Install dependencies
        run: uv sync --locked --all-groups --python ${{ matrix.python-version }}

      - name: Run tests
        run: uv run --python ${{ matrix.python-version }} pytest -m "not postgres" --tb=short -q

  license-check:
    name: License check
    needs: test
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0   # required: hatch-vcs runs during uv sync

      - name: Install uv
        uses: astral-sh/setup-uv@v5

      - name: Install dependencies
        run: uv sync --dev

      - name: Check dependency licenses
        run: >-
          uv run pip-licenses
          --allow-only="MIT;Apache-2.0;BSD-2-Clause;BSD-3-Clause;ISC;Python-2.0;PSF-2.0"
          --partial-match

  build:
    name: Build distribution
    needs: license-check
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0   # required: hatch-vcs reads tag to set version

      - name: Install uv
        uses: astral-sh/setup-uv@v5

      - name: Build wheel and sdist
        run: uv build

      - name: Upload dist artifact
        uses: actions/upload-artifact@v4
        with:
          name: dist
          path: dist/
          if-no-files-found: error

  publish-testpypi:
    name: Publish → TestPyPI
    needs: build
    runs-on: ubuntu-latest
    environment:
      name: testpypi
      url: https://test.pypi.org/p/orcapod
    permissions:
      id-token: write   # required for OIDC Trusted Publishing
    steps:
      - name: Install uv
        uses: astral-sh/setup-uv@v5

      - name: Download dist artifact
        uses: actions/download-artifact@v4
        with:
          name: dist
          path: dist/

      - name: Publish to TestPyPI
        run: uv publish --publish-url https://test.pypi.org/legacy/ dist/*

  publish-pypi:
    name: Publish → PyPI
    needs: publish-testpypi
    runs-on: ubuntu-latest
    environment:
      name: pypi
      url: https://pypi.org/p/orcapod
    permissions:
      id-token: write   # required for OIDC Trusted Publishing
      contents: write   # required for creating GitHub Release
    steps:
      - name: Install uv
        uses: astral-sh/setup-uv@v5

      - name: Download dist artifact
        uses: actions/download-artifact@v4
        with:
          name: dist
          path: dist/

      - name: Publish to PyPI
        run: uv publish dist/*

      - name: Create GitHub Release
        uses: softprops/action-gh-release@v2
        with:
          generate_release_notes: true
          files: dist/*
```

- [ ] **Step 2: Validate YAML syntax**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/publish.yml'))" && echo "YAML OK"
```

Expected: `YAML OK`

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/publish.yml
git commit -m "ci: add tag-triggered publish workflow for PyPI release"
```

---

## Task 3: Add license and dependency-review jobs to `run-tests.yml`

**Files:**
- Modify: `.github/workflows/run-tests.yml`

Append two new jobs to the existing workflow. Both run independently (no `needs`) so they don't delay or block the existing `test` and `spiral-integration` jobs.

`license-check` — mirrors the same job in `publish.yml`, runs on every push and PR.
`dependency-review` — uses GitHub's built-in action to deny copyleft licenses on PRs; has no effect on push events.

- [ ] **Step 1: Append `license-check` job to `run-tests.yml`**

Add after the closing of the `spiral-integration` job:

```yaml
  license-check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with:
          fetch-depth: 0   # required: hatch-vcs runs during uv sync

      - name: Install uv
        uses: astral-sh/setup-uv@v5

      - name: Install dependencies
        run: uv sync --dev

      - name: Check dependency licenses
        run: >-
          uv run pip-licenses
          --allow-only="MIT;Apache-2.0;BSD-2-Clause;BSD-3-Clause;ISC;Python-2.0;PSF-2.0"
          --partial-match

  dependency-review:
    runs-on: ubuntu-latest
    if: github.event_name == 'pull_request'
    steps:
      - uses: actions/checkout@v4

      - name: Dependency review
        uses: actions/dependency-review-action@v4
        with:
          deny-licenses: >-
            GPL-2.0-only, GPL-2.0-or-later,
            GPL-3.0-only, GPL-3.0-or-later,
            AGPL-3.0-only, AGPL-3.0-or-later,
            LGPL-2.0-only, LGPL-2.0-or-later,
            LGPL-2.1-only, LGPL-2.1-or-later,
            LGPL-3.0-only, LGPL-3.0-or-later
```

- [ ] **Step 2: Validate YAML syntax**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/run-tests.yml'))" && echo "YAML OK"
```

Expected: `YAML OK`

- [ ] **Step 3: Run license check locally to confirm it passes with current deps**

```bash
uv run pip-licenses \
  --allow-only="MIT;Apache-2.0;BSD-2-Clause;BSD-3-Clause;ISC;Python-2.0;PSF-2.0" \
  --partial-match
```

Expected: exits 0. If any package fails, check its license and either add to allowlist (if permissive) or flag for removal.

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/run-tests.yml
git commit -m "ci: add license-check and dependency-review jobs to run-tests.yml"
```

---

## Task 4: Final verification and PR

**Files:** none (verification only)

- [ ] **Step 1: Run the full test suite one more time**

```bash
uv run pytest -m "not postgres" --tb=short -q
```

Expected: all tests pass, no regressions.

- [ ] **Step 2: Verify dist artifacts build cleanly from the current branch**

```bash
rm -rf dist/
uv build
ls dist/
```

Expected: two files — a `.whl` and a `.tar.gz`. Version will be a dev version (no tag on this branch), which is correct. The real version is only set when building from a tagged commit.

- [ ] **Step 3: Push branch and open PR against `dev`**

```bash
git push origin HEAD
```

Then create a PR targeting `dev`:

```bash
gh pr create \
  --base dev \
  --title "ci(PLT-1250): tag-triggered PyPI release pipeline" \
  --body "$(cat <<'EOF'
## Summary

- Migrates build backend from `setuptools+setuptools-scm` to `hatchling+hatch-vcs`
- Adds `publish.yml`: tag-triggered pipeline (`v*`) with test → license-check → build → TestPyPI → PyPI → GitHub Release
- Adds `license-check` and `dependency-review` jobs to `run-tests.yml` CI
- Fixes Homepage URL (`walkerlab` → `nauticalab`)

## Manual setup (already done)
- [x] PyPI Trusted Publisher configured (`publish.yml`, environment `pypi`)
- [x] TestPyPI Trusted Publisher configured (`publish.yml`, environment `testpypi`)
- [x] GitHub Environments `pypi` and `testpypi` created

## Test plan
- [ ] CI passes on this PR (test, license-check, dependency-review)
- [ ] After merge to `dev`, push a `v0.1.0rc1` tag and verify the publish workflow triggers and completes
- [ ] Confirm `pip install orcapod==0.1.0rc1` works from PyPI

Closes PLT-1250
EOF
)"
```

---

## Reference: Full `pyproject.toml` after changes

For completeness, the build-system related sections should look exactly like this after Task 1:

```toml
[build-system]
requires = ["hatchling", "hatch-vcs"]
build-backend = "hatchling.build"

[project]
name = "orcapod"
# ... (unchanged) ...

[project.urls]
Homepage = "https://github.com/nauticalab/orcapod-python"

# ... (optional-dependencies unchanged) ...

[tool.hatch.version]
source = "vcs"

[tool.hatch.build.hooks.vcs]
version-file = "src/orcapod/_version.py"

[tool.hatch.build.targets.wheel]
packages = ["src/orcapod"]

[dependency-groups]
dev = [
    # ... existing entries ...
    "pip-licenses>=5.0.0",
]
```

The removed sections (delete entirely — do not leave empty):
- `[tool.setuptools.packages.find]`
- `[tool.setuptools_scm]`
