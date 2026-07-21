# Release Branch Argument Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an optional `branch` input (default `main`) to the GitHub Actions release workflow so releases can be cut from any branch, with a pre-flight validation step that fails fast on invalid branch names.

**Architecture:** Two files change. `release.yml` gets a new `branch` workflow input, a new `validate-branch` pre-flight job, and updated checkout steps in `test` and `build`. `RELEASING.md` gets updated docs and a new hotfix-release example.

**Tech Stack:** GitHub Actions YAML, `gh` CLI (pre-installed on ubuntu-latest runners), PyYAML (for local syntax validation), Bash

---

## File Map

| Action | File | What changes |
|--------|------|--------------|
| Modify | `.github/workflows/release.yml` | Add `branch` input; add `validate-branch` job; update `test`, `license-check`, `build` jobs |
| Modify | `RELEASING.md` | Update Step 2; add "Cutting a Hotfix Release" section |

---

### Task 1: Create the feature branch

- [ ] **Step 1: Check out the feature branch**

```bash
git checkout -b eywalker/itl-545-release-cutting-allow-optional-branch-argument-defaults-to
```

- [ ] **Step 2: Verify you are on the correct branch**

```bash
git branch --show-current
```

Expected output:
```
eywalker/itl-545-release-cutting-allow-optional-branch-argument-defaults-to
```

---

### Task 2: Add `branch` input to `release.yml`

**Files:**
- Modify: `.github/workflows/release.yml:1-9`

The `workflow_dispatch.inputs` block currently has only `version`. Add `branch` immediately after it.

- [ ] **Step 1: Edit the `on:` block**

Replace the current `on:` block (lines 1–9):

```yaml
name: Release

on:
  workflow_dispatch:
    inputs:
      version:
        description: 'Release version (e.g. 0.1.0 or v0.1.0 — leading v is stripped automatically)'
        required: true
        type: string
```

With:

```yaml
name: Release

on:
  workflow_dispatch:
    inputs:
      version:
        description: 'Release version (e.g. 0.1.0 or v0.1.0 — leading v is stripped automatically)'
        required: true
        type: string
      branch:
        description: 'Branch to cut the release from (defaults to main)'
        required: false
        type: string
        default: main
```

- [ ] **Step 2: Validate YAML syntax**

```bash
uv run python -c "import yaml; yaml.safe_load(open('.github/workflows/release.yml')); print('YAML OK')"
```

Expected output:
```
YAML OK
```

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/release.yml
git commit -m "ci(release): add optional branch input (defaults to main)"
```

---

### Task 3: Add `validate-branch` pre-flight job

**Files:**
- Modify: `.github/workflows/release.yml` — insert new job before `test`

The new job uses the built-in `GITHUB_TOKEN` (exposed as `GH_TOKEN` env var) to query the GitHub API. No checkout needed. `gh` is pre-installed on all `ubuntu-latest` runners.

- [ ] **Step 1: Insert the `validate-branch` job**

In `.github/workflows/release.yml`, after the closing `type: string` line of the `branch` input and before the `jobs:` key, the file structure is:

```yaml
jobs:
  test:
    name: Test ...
```

Add the `validate-branch` job as the **first** job under `jobs:`:

```yaml
jobs:
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

  test:
    name: Test ...
```

- [ ] **Step 2: Validate YAML syntax**

```bash
uv run python -c "import yaml; yaml.safe_load(open('.github/workflows/release.yml')); print('YAML OK')"
```

Expected output:
```
YAML OK
```

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/release.yml
git commit -m "ci(release): add validate-branch pre-flight job"
```

---

### Task 4: Wire `branch` into `test` and `license-check` jobs

**Files:**
- Modify: `.github/workflows/release.yml` — `test` and `license-check` jobs

Both jobs must now depend on `validate-branch`. The `test` job's checkout must use `ref: ${{ inputs.branch }}` so tests run against the release branch.

- [ ] **Step 1: Update the `test` job**

Find the `test` job. It currently starts with:

```yaml
  test:
    name: Test (Python ${{ matrix.python-version }})
    runs-on: ubuntu-latest
```

And its checkout step is:

```yaml
      - uses: actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5  # v4
        with:
          fetch-depth: 0  # required: hatch-vcs needs full tag history
```

Make two changes:

1. Add `needs: [validate-branch]` between `name:` and `runs-on:`:

```yaml
  test:
    name: Test (Python ${{ matrix.python-version }})
    needs: [validate-branch]
    runs-on: ubuntu-latest
```

2. Add `ref: ${{ inputs.branch }}` to the checkout `with:` block:

```yaml
      - uses: actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5  # v4
        with:
          fetch-depth: 0  # required: hatch-vcs needs full tag history
          ref: ${{ inputs.branch }}
```

- [ ] **Step 2: Update the `license-check` job**

Find the `license-check` job. It currently reads:

```yaml
  license-check:
    uses: ./.github/workflows/_license-check.yml
```

Add `needs: [validate-branch]`:

```yaml
  license-check:
    needs: [validate-branch]
    uses: ./.github/workflows/_license-check.yml
```

- [ ] **Step 3: Validate YAML syntax**

```bash
uv run python -c "import yaml; yaml.safe_load(open('.github/workflows/release.yml')); print('YAML OK')"
```

Expected output:
```
YAML OK
```

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/release.yml
git commit -m "ci(release): wire branch input into test and license-check jobs"
```

---

### Task 5: Wire `branch` into `build` job

**Files:**
- Modify: `.github/workflows/release.yml` — `build` job's checkout step

The `build` job creates the git tag and pushes it. Its checkout must use `ref: ${{ inputs.branch }}` so the tag is created at the tip of the release branch, not at whatever commit triggered the dispatch.

- [ ] **Step 1: Update the `build` job checkout**

Find the checkout step inside `build`. It currently reads:

```yaml
      - uses: actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5  # v4
        with:
          fetch-depth: 0  # required: hatch-vcs reads tag to set version
```

Add `ref: ${{ inputs.branch }}`:

```yaml
      - uses: actions/checkout@34e114876b0b11c390a56381ad16ebd13914f8d5  # v4
        with:
          fetch-depth: 0  # required: hatch-vcs reads tag to set version
          ref: ${{ inputs.branch }}
```

- [ ] **Step 2: Validate YAML syntax**

```bash
uv run python -c "import yaml; yaml.safe_load(open('.github/workflows/release.yml')); print('YAML OK')"
```

Expected output:
```
YAML OK
```

- [ ] **Step 3: Verify the full job graph**

Confirm the structure of `release.yml` looks like this (just the job names and `needs:` lines — spot-check):

```bash
uv run python -c "
import yaml
wf = yaml.safe_load(open('.github/workflows/release.yml'))
for name, job in wf['jobs'].items():
    needs = job.get('needs', [])
    print(f'{name}: needs={needs}')
"
```

Expected output:
```
validate-branch: needs=[]
test: needs=['validate-branch']
license-check: needs=['validate-branch']
build: needs=['test', 'license-check']
publish-testpypi: needs=['build']
publish-pypi: needs=['build', 'publish-testpypi']
linear-sync: needs=['build']
linear-complete: needs=['build', 'publish-pypi', 'linear-sync']
```

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/release.yml
git commit -m "ci(release): wire branch input into build checkout"
```

---

### Task 6: Update `RELEASING.md`

**Files:**
- Modify: `RELEASING.md`

Two edits:

1. **Step 2 of "Cutting a Release"** — add a note about the optional branch field.
2. **New "Cutting a Hotfix Release" section** — insert after the existing "Pre-releases" section.

- [ ] **Step 1: Update Step 2 of "Cutting a Release"**

Find this paragraph in `RELEASING.md`:

```markdown
2. **Trigger the release workflow** — go to
   **[Actions → Release → Run workflow](https://github.com/nauticalab/orcapod-python/actions/workflows/release.yml)**
   in the GitHub UI, enter the version (e.g. `0.1.0`), and click **Run workflow**.

   A leading `v` is stripped automatically — `v0.1.0` and `0.1.0` both work.
```

Replace it with:

```markdown
2. **Trigger the release workflow** — go to
   **[Actions → Release → Run workflow](https://github.com/nauticalab/orcapod-python/actions/workflows/release.yml)**
   in the GitHub UI, enter the version (e.g. `0.1.0`), optionally enter a branch name
   (defaults to `main`), and click **Run workflow**.

   A leading `v` is stripped automatically — `v0.1.0` and `0.1.0` both work.
```

- [ ] **Step 2: Add "Cutting a Hotfix Release" section**

Find the "Pre-releases" section header in `RELEASING.md`:

```markdown
## Pre-releases
```

Insert the following new section **after** the entire "Pre-releases" section (i.e., after the last paragraph of "Pre-releases", before "## Tag Format"):

```markdown
## Cutting a Hotfix Release

To release from a branch other than `main` — for example, a maintenance branch carrying
a critical patch — enter the branch name in the **Branch** field when triggering the
workflow.

**Example:** cutting `v0.1.1` from `hotfix/0.1.x`:

1. **Trigger the release workflow** — go to
   **[Actions → Release → Run workflow](https://github.com/nauticalab/orcapod-python/actions/workflows/release.yml)**
2. Set **Version** to `0.1.1`
3. Set **Branch** to `hotfix/0.1.x`
4. Click **Run workflow**

The workflow will confirm the branch exists, run tests against it, create the `v0.1.1`
tag at its tip, and publish to PyPI — identical to a normal release.

```

- [ ] **Step 3: Commit**

```bash
git add RELEASING.md
git commit -m "docs(releasing): document optional branch argument and hotfix example"
```

---

### Task 7: Push branch and open PR

- [ ] **Step 1: Push the feature branch**

```bash
git push -u origin eywalker/itl-545-release-cutting-allow-optional-branch-argument-defaults-to
```

- [ ] **Step 2: Open the PR**

```bash
gh pr create \
  --title "ci(release): allow optional branch argument (defaults to main)" \
  --base main \
  --body "$(cat <<'EOF'
## Summary

- Adds an optional `branch` input to the `release.yml` workflow dispatch (defaults to `main`)
- Adds a `validate-branch` pre-flight job that fails fast with a clear error if the branch doesn't exist
- Wires `ref: ${{ inputs.branch }}` into the `test` and `build` checkout steps so tests and tag creation target the release branch
- Updates `RELEASING.md` with the new parameter and a hotfix release example

Closes ITL-545

## Test plan

- [ ] Trigger the release workflow without specifying a branch — confirm behavior is identical to today (cuts from `main`)
- [ ] Trigger the release workflow with a valid non-main branch name — confirm the tag is created at the tip of that branch
- [ ] Trigger the release workflow with a non-existent branch name — confirm `validate-branch` fails immediately with the error message `Branch '...' does not exist. Check for typos.` and no other jobs run
EOF
)"
```
