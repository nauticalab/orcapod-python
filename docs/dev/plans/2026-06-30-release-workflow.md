# Release Workflow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the tag-push-triggered `publish.yml` with a `workflow_dispatch`-triggered `release.yml` that accepts a version input, runs pre-flight checks, builds the package, creates and pushes the release tag, publishes to TestPyPI then PyPI, creates a GitHub Release, and closes out the Linear release.

**Architecture:** A single `release.yml` workflow replaces `publish.yml`. Pre-flight jobs (`test` matrix + `license-check`) run in parallel; `build` waits for all of them to succeed before creating the tag; publish jobs run sequentially (TestPyPI → PyPI); Linear jobs run independently and do not block the publish chain.

**Tech Stack:** GitHub Actions, `uv`, `hatch-vcs`, OIDC Trusted Publishing, `linear/linear-release-action`

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `.github/workflows/release.yml` | **Create** | New `workflow_dispatch`-triggered release workflow |
| `.github/workflows/publish.yml` | **Delete** | Replaced by `release.yml` |
| `RELEASING.md` | **Modify** | Document the new `workflow_dispatch` release process |

`release-sync.yml` and `_license-check.yml` are **not touched**.

---

## Task 1: Create `release.yml`

**Files:**
- Create: `.github/workflows/release.yml`

- [ ] **Step 1: Write `release.yml`**

Create `.github/workflows/release.yml` with the following content:

```yaml
name: Release

on:
  workflow_dispatch:
    inputs:
      version:
        description: 'Release version (e.g. 0.1.0 or v0.1.0 — leading v is stripped automatically)'
        required: true
        type: string

jobs:
  test:
    name: Test (Python ${{ matrix.python-version }})
    runs-on: ubuntu-latest
    strategy:
      fail-fast: true
      matrix:
        python-version: ["3.11", "3.12"]
    steps:
      - uses: actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5  # v4
        with:
          fetch-depth: 0  # required: hatch-vcs needs full tag history

      - name: Install uv
        uses: astral-sh/setup-uv@e58605a9b6da7c637471fab8847a5e5a6b8df081  # v5

      - name: Install system dependencies
        run: sudo apt-get update && sudo apt-get install -y graphviz libgraphviz-dev

      - name: Install dependencies
        run: uv sync --locked --all-extras --dev --python ${{ matrix.python-version }}

      - name: Run tests
        run: uv run --python ${{ matrix.python-version }} pytest -m "not postgres" --tb=short -q

  license-check:
    uses: ./.github/workflows/_license-check.yml

  build:
    name: Build distribution
    needs: [test, license-check]
    runs-on: ubuntu-latest
    permissions:
      contents: write  # required to push the release tag
    outputs:
      version: ${{ steps.normalize.outputs.version }}
    steps:
      - name: Normalize version
        id: normalize
        run: |
          VERSION="${{ inputs.version }}"
          VERSION="${VERSION#v}"
          echo "version=${VERSION}" >> "$GITHUB_OUTPUT"

      - uses: actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5  # v4
        with:
          fetch-depth: 0  # required: hatch-vcs reads tag to set version

      - name: Configure git identity
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"

      - name: Create local release tag
        run: git tag "v${{ steps.normalize.outputs.version }}"

      - name: Install uv
        uses: astral-sh/setup-uv@e58605a9b6da7c637471fab8847a5e5a6b8df081  # v5

      - name: Build wheel and sdist
        run: uv build

      - name: Guard against duplicate tag
        run: |
          if git ls-remote --tags origin "refs/tags/v${{ steps.normalize.outputs.version }}" | grep -q .; then
            echo "::error::Tag v${{ steps.normalize.outputs.version }} already exists on origin. Aborting."
            exit 1
          fi

      - name: Push release tag
        run: git push origin "v${{ steps.normalize.outputs.version }}"

      - name: Upload dist artifact
        uses: actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02  # v4.6.2
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
      id-token: write  # required for OIDC Trusted Publishing
    steps:
      - name: Install uv
        uses: astral-sh/setup-uv@e58605a9b6da7c637471fab8847a5e5a6b8df081  # v5

      - name: Download dist artifact
        uses: actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093  # v4.3.0
        with:
          name: dist
          path: dist/

      - name: Publish to TestPyPI
        run: uv publish --publish-url https://test.pypi.org/legacy/ dist/*

  publish-pypi:
    name: Publish → PyPI
    needs: [build, publish-testpypi]
    runs-on: ubuntu-latest
    environment:
      name: pypi
      url: https://pypi.org/p/orcapod
    permissions:
      id-token: write   # required for OIDC Trusted Publishing
      contents: write   # required for creating GitHub Release
    steps:
      - name: Install uv
        uses: astral-sh/setup-uv@e58605a9b6da7c637471fab8847a5e5a6b8df081  # v5

      - name: Download dist artifact
        uses: actions/download-artifact@d3f86a106a0bac45b974a628896c90dbdf5c8093  # v4.3.0
        with:
          name: dist
          path: dist/

      - name: Publish to PyPI
        run: uv publish dist/*

      - name: Create GitHub Release
        uses: softprops/action-gh-release@3bb12739c298aeb8a4eeaf626c5b8d85266b0e65  # v2.6.2
        with:
          tag_name: "v${{ needs.build.outputs.version }}"
          generate_release_notes: true
          files: dist/*

  linear-sync:
    name: Sync Linear release
    needs: build
    runs-on: ubuntu-latest
    timeout-minutes: 5
    permissions:
      contents: read
    steps:
      - name: Checkout
        uses: actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5  # v4
        with:
          fetch-depth: 0
      - name: Sync Linear release
        uses: linear/linear-release-action@c0cb8354a362c24c6d3e0948f37fd66d07588e3f  # v0
        with:
          access_key: ${{ secrets.LINEAR_ACCESS_KEY }}
          command: sync
          version: "v${{ needs.build.outputs.version }}"

  linear-complete:
    name: Complete Linear release
    needs: [publish-pypi, linear-sync]
    runs-on: ubuntu-latest
    timeout-minutes: 5
    permissions:
      contents: read
    steps:
      - name: Checkout
        uses: actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5  # v4
        with:
          fetch-depth: 0
      - name: Complete Linear release
        uses: linear/linear-release-action@c0cb8354a362c24c6d3e0948f37fd66d07588e3f  # v0
        with:
          access_key: ${{ secrets.LINEAR_ACCESS_KEY }}
          command: complete
          version: "v${{ needs.build.outputs.version }}"
```

- [ ] **Step 2: Validate YAML syntax**

Run:
```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/release.yml')); print('YAML OK')"
```

Expected output:
```
YAML OK
```

If this fails, the file has a YAML syntax error — fix before continuing.

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/release.yml
git commit -m "ci: add workflow_dispatch release workflow (ITL-449)"
```

---

## Task 2: Delete `publish.yml`

**Files:**
- Delete: `.github/workflows/publish.yml`

- [ ] **Step 1: Delete the file**

```bash
git rm .github/workflows/publish.yml
```

- [ ] **Step 2: Verify it is gone**

```bash
ls .github/workflows/
```

Expected: `_license-check.yml`, `release-sync.yml`, `release.yml`, `run-objective-tests.yml`, `run-postgres-tests.yml`, `run-tests.yml`, `tests.yml` — no `publish.yml`.

- [ ] **Step 3: Commit**

```bash
git commit -m "ci: remove tag-push publish.yml (replaced by release.yml)"
```

---

## Task 3: Update `RELEASING.md`

**Files:**
- Modify: `RELEASING.md`

- [ ] **Step 1: Replace `RELEASING.md` with the updated content**

Replace the entire file with:

```markdown
# Releasing Orcapod

This document describes how to cut a release of `orcapod` to PyPI.

## Branching Model

- All development happens on feature branches off `main`.
- There are no long-lived branches (the historical `dev` branch has been retired).
- No back-merges are needed.

## Cutting a Release

Releases are triggered from the GitHub Actions UI — no manual `git tag` step required.

1. **Merge your branch into `main`** — open a PR, get it reviewed, merge it.

2. **Trigger the release workflow** — go to
   **[Actions → Release → Run workflow](https://github.com/nauticalab/orcapod-python/actions/workflows/release.yml)**
   in the GitHub UI, enter the version (e.g. `0.1.0`), and click **Run workflow**.

   A leading `v` is stripped automatically — `v0.1.0` and `0.1.0` both work.

3. **CI takes over** — the workflow runs the following jobs automatically:

   ```
   test (3.11) ─┐
   test (3.12) ─┤─ build ─ publish-testpypi ─ publish-pypi ─ linear-complete
   license-check┘                                  │
                                           linear-sync ────────────┘
   ```

   - Pre-flight: tests on Python 3.11 and 3.12, plus license check (in parallel)
   - Build: normalises version, creates local tag, builds wheel + sdist, pushes tag to origin
   - TestPyPI: publishes to test.pypi.org first as a staging step
   - PyPI: publishes to pypi.org and creates a GitHub Release with generated release notes
   - Linear: syncs and completes the Linear release (Merged → Done)

## Pre-releases

Release candidates (e.g. `0.1.0rc1`) follow exactly the same path — enter the version in the
workflow input. PyPI handles the stable vs pre-release distinction natively:

- `pip install orcapod` — installs the latest **stable** release only
- `pip install --pre orcapod` — installs the latest release including pre-releases

## Tag Format

| Release type | Tag format | Example |
|-------------|------------|---------|
| Stable | `vMAJOR.MINOR.PATCH` | `v0.1.0` |
| Release candidate | `vMAJOR.MINOR.PATCHrcN` | `v0.1.0rc1` |
| Alpha | `vMAJOR.MINOR.PATCHaN` | `v0.1.0a1` |
| Beta | `vMAJOR.MINOR.PATCHbN` | `v0.1.0b1` |

The workflow input accepts any of the above (with or without a leading `v`). The version is
derived from the git tag by `hatch-vcs` (`dynamic = ["version"]` in `pyproject.toml`) — no
manual version bump is needed.

## Linear Release System

When a release is triggered, the CI automatically transitions all Linear issues in
**"Merged"** status (whose PRs were included in the release) to **"Done"**. This is
handled by two jobs in `release.yml` that call `linear/linear-release-action`, plus a
separate `release-sync.yml` that runs on every push to `main`.

### How it works

| Trigger | Workflow / Job | Action | Effect |
|---------|---------------|--------|--------|
| Push to `main` | `release-sync.yml / sync` | `sync` (no version) | Associates the merged PR with the open Linear release draft |
| Release workflow `build` succeeds | `release.yml / linear-sync` | `sync --version <tag>` | Finalises the commit set for this release version in Linear |
| After `publish-pypi` succeeds | `release.yml / linear-complete` | `complete --version <tag>` | Marks the release done in Linear; triggers Merged → Done |

### Prerequisites

Before cutting the first release with this system active, a workspace admin must:

1. **Set the `LINEAR_ACCESS_KEY` repo secret** in `nauticalab/orcapod-python` GitHub
   settings (`Settings → Secrets and variables → Actions → New repository secret`).
   Use the same Linear API key as `metamorphic-brain/axon`.

2. **Configure a Linear release** in the workspace for orcapod-python via the Linear UI:
   set the release name, which statuses count as "included" (at minimum: Merged), and
   the target state for the transition (Done).

3. **Update PyPI/TestPyPI Trusted Publisher configs** — if the Trusted Publisher on
   pypi.org or test.pypi.org still references `publish.yml` as the workflow file, update
   it to `release.yml` before triggering the first release.

If `LINEAR_ACCESS_KEY` is not set: the `release-sync` workflow runs but the Linear action
may fail; the `linear-sync` and `linear-complete` jobs in `release.yml` fail but do not
block the PyPI publish (they have no dependents in the publish chain).

### Replicating for other orcapod repos

1. Copy `.github/workflows/release-sync.yml` verbatim.
2. Add `linear-sync` and `linear-complete` jobs to the repo's release workflow,
   adjusting `needs:` in `linear-complete` to point at whichever job creates the GitHub
   Release.
3. Set the `LINEAR_ACCESS_KEY` repo secret.
4. Configure a Linear release in the workspace for the new repo.
```

- [ ] **Step 2: Verify the file looks correct**

```bash
head -30 RELEASING.md
```

Expected: starts with `# Releasing Orcapod` and the new `workflow_dispatch`-based instructions.

- [ ] **Step 3: Commit**

```bash
git add RELEASING.md
git commit -m "docs: update RELEASING.md for workflow_dispatch release process"
```

---

## Self-Review

### Spec coverage

| Spec requirement | Task covering it |
|---|---|
| `release.yml` created, replaces `publish.yml` | Task 1, Task 2 |
| `workflow_dispatch` trigger with version input | Task 1, Step 1 |
| Pre-flight: test matrix (3.11, 3.12) + license-check in parallel | Task 1, Step 1 (`test` + `license-check` jobs, no `needs:` on either) |
| Build: normalize version, create local tag, build, push tag | Task 1, Step 1 (`build` job) |
| Duplicate tag guard before pushing | Task 1, Step 1 (guard step in `build`) |
| Publish to TestPyPI then PyPI | Task 1, Step 1 (`publish-testpypi`, `publish-pypi` jobs) |
| GitHub Release with generated notes | Task 1, Step 1 (`softprops/action-gh-release` in `publish-pypi`) |
| linear-sync after build (parallel to publish chain) | Task 1, Step 1 (`linear-sync` needs `build`) |
| linear-complete after publish-pypi and linear-sync | Task 1, Step 1 (`linear-complete` needs `[publish-pypi, linear-sync]`) |
| All actions pinned to SHA digests | Task 1, Step 1 (all `uses:` lines) |
| `publish.yml` deleted | Task 2 |
| `RELEASING.md` updated | Task 3 |
| PyPI Trusted Publisher note (rename publish.yml → release.yml) | Task 3, Step 1 |

All spec requirements covered. ✓

### Placeholder scan

No TBDs, TODOs, or incomplete sections. ✓

### Type/name consistency

- `steps.normalize.outputs.version` referenced in `build` job steps — defined by the `normalize` step with `id: normalize`. ✓
- `needs.build.outputs.version` used in `publish-pypi`, `linear-sync`, `linear-complete` — all have `needs: build` or transitively depend on it, and the `build` job declares `outputs: version`. ✓
- Action digest hashes are consistent across the plan. ✓
