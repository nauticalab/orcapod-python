# Linear Release System Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire `linear/linear-release-action` into orcapod-python's CI so that cutting a release tag automatically transitions all Merged Linear issues to Done.

**Architecture:** Two new workflow jobs (`linear-sync`, `linear-complete`) are added to the existing `publish.yml`; a new `release-sync.yml` mirrors axon's push-to-main sync. No application code changes — this is CI infrastructure only.

**Tech Stack:** GitHub Actions, `linear/linear-release-action@c0cb8354a362c24c6d3e0948f37fd66d07588e3f` (v0), `actions/checkout@v4`

---

## File Map

| Action | Path | What it does |
|--------|------|--------------|
| Create | `.github/workflows/release-sync.yml` | Syncs merged PRs to the open Linear release draft on every push to `main` |
| Modify | `.github/workflows/publish.yml` | Adds `linear-sync` (parallel, tag push) and `linear-complete` (after `publish-pypi`) jobs |
| Modify | `RELEASING.md` | Documents the Linear release system for future maintainers |

---

## Task 1: Create the feature branch

- [ ] **Step 1.1: Check out the branch**

```bash
git checkout -b eywalker/itl-448-set-up-linear-release-system-for-orcapod-python-v01-release
```

- [ ] **Step 1.2: Verify you are on the correct branch**

```bash
git branch --show-current
```

Expected output:
```
eywalker/itl-448-set-up-linear-release-system-for-orcapod-python-v01-release
```

---

## Task 2: Create `release-sync.yml`

This file mirrors `metamorphic-brain/axon`'s `release-sync.yml` exactly. It runs on every push to `main` and calls `linear-release-action sync` (no `version` argument) to associate the merged PR with the currently-open Linear release draft.

**Files:**
- Create: `.github/workflows/release-sync.yml`

- [ ] **Step 2.1: Create the file**

Write `.github/workflows/release-sync.yml` with this exact content:

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
      - name: Checkout
        uses: actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683  # v4.2.2
        with:
          fetch-depth: 0
      - name: Sync Linear release
        uses: linear/linear-release-action@c0cb8354a362c24c6d3e0948f37fd66d07588e3f  # v0
        with:
          access_key: ${{ secrets.LINEAR_ACCESS_KEY }}
          command: sync
```

- [ ] **Step 2.2: Validate YAML syntax**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/release-sync.yml')); print('OK')"
```

Expected output: `OK`

- [ ] **Step 2.3: Commit**

```bash
git add .github/workflows/release-sync.yml
git commit -m "ci: add Linear release-sync workflow on push to main (ITL-448)"
```

---

## Task 3: Add `linear-sync` and `linear-complete` jobs to `publish.yml`

`publish.yml` currently ends with the `publish-pypi` job. We append two new jobs after it:

- **`linear-sync`**: no dependencies (runs in parallel with the existing pipeline), calls `sync` with `version: ${{ github.ref_name }}` to finalise which commits belong to this release in Linear.
- **`linear-complete`**: `needs: [publish-pypi]`, calls `complete` with `version: ${{ github.ref_name }}` to trigger the Merged → Done state transition in Linear.

**Files:**
- Modify: `.github/workflows/publish.yml`

- [ ] **Step 3.1: Append the two new jobs to `publish.yml`**

Open `.github/workflows/publish.yml`. After the closing of the `publish-pypi` job (the last job in the file), append the following at the same indentation level as the other jobs:

```yaml

  linear-sync:
    name: Sync Linear release
    runs-on: ubuntu-latest
    timeout-minutes: 5
    permissions:
      contents: read
    steps:
      - name: Checkout
        uses: actions/checkout@v4
        with:
          fetch-depth: 0
      - name: Sync Linear release
        uses: linear/linear-release-action@c0cb8354a362c24c6d3e0948f37fd66d07588e3f  # v0
        with:
          access_key: ${{ secrets.LINEAR_ACCESS_KEY }}
          command: sync
          version: ${{ github.ref_name }}

  linear-complete:
    name: Complete Linear release
    needs: [publish-pypi]
    runs-on: ubuntu-latest
    timeout-minutes: 5
    permissions:
      contents: read
    steps:
      - name: Checkout
        uses: actions/checkout@v4
        with:
          fetch-depth: 0
      - name: Complete Linear release
        uses: linear/linear-release-action@c0cb8354a362c24c6d3e0948f37fd66d07588e3f  # v0
        with:
          access_key: ${{ secrets.LINEAR_ACCESS_KEY }}
          command: complete
          version: ${{ github.ref_name }}
```

- [ ] **Step 3.2: Validate YAML syntax**

```bash
python3 -c "import yaml; yaml.safe_load(open('.github/workflows/publish.yml')); print('OK')"
```

Expected output: `OK`

- [ ] **Step 3.3: Verify job structure**

```bash
python3 -c "
import yaml
wf = yaml.safe_load(open('.github/workflows/publish.yml'))
jobs = list(wf['jobs'].keys())
print('Jobs:', jobs)
assert 'linear-sync' in jobs, 'linear-sync missing'
assert 'linear-complete' in jobs, 'linear-complete missing'
lc = wf['jobs']['linear-complete']
assert lc['needs'] == ['publish-pypi'], f'wrong needs: {lc[\"needs\"]}'
print('Assertions passed')
"
```

Expected output:
```
Jobs: ['license-check', 'test', 'build', 'publish-testpypi', 'publish-pypi', 'linear-sync', 'linear-complete']
Assertions passed
```

- [ ] **Step 3.4: Commit**

```bash
git add .github/workflows/publish.yml
git commit -m "ci: add Linear sync and complete jobs to publish workflow (ITL-448)"
```

---

## Task 4: Update `RELEASING.md`

Add a new section documenting the Linear release system so future maintainers understand the two-phase flow and know what manual steps are required before the first release.

**Files:**
- Modify: `RELEASING.md`

- [ ] **Step 4.1: Append the Linear release section to `RELEASING.md`**

Open `RELEASING.md` and append the following after the existing content:

```markdown

## Linear Release System

When a release tag is pushed, the CI automatically transitions all Linear issues in
**"Merged"** status (whose PRs were included in the release) to **"Done"**. This is
handled by two GitHub Actions jobs that call `linear/linear-release-action`:

### How it works

| Trigger | Job | Action | Effect |
|---------|-----|--------|--------|
| Push to `main` | `release-sync.yml / sync` | `sync` (no version) | Associates the merged PR with the open Linear release draft |
| Tag push `v*` | `publish.yml / linear-sync` | `sync --version <tag>` | Finalises the commit set for this release version in Linear |
| After `publish-pypi` succeeds | `publish.yml / linear-complete` | `complete --version <tag>` | Marks the release done in Linear; triggers Merged → Done |

### Prerequisites

Before cutting the first release with this system active, a workspace admin must:

1. **Set the `LINEAR_ACCESS_KEY` repo secret** in `nauticalab/orcapod-python` GitHub
   settings (`Settings → Secrets and variables → Actions → New repository secret`).
   Use the same Linear API key as `metamorphic-brain/axon`.

2. **Configure a Linear release** in the workspace for orcapod-python via the Linear UI:
   set the release name, which statuses count as "included" (at minimum: Merged), and
   the target state for the transition (Done).

If `LINEAR_ACCESS_KEY` is not set, the `linear-sync` and `linear-complete` jobs will
fail but will not block the PyPI publish (they have no dependents).

### Replicating for other orcapod repos

1. Copy `.github/workflows/release-sync.yml` verbatim.
2. Add `linear-sync` and `linear-complete` jobs to the repo's publish/release workflow,
   adjusting `needs:` in `linear-complete` to point at whichever job creates the GitHub
   Release.
3. Set the `LINEAR_ACCESS_KEY` repo secret.
4. Configure a Linear release in the workspace for the new repo.
```

- [ ] **Step 4.2: Verify the file ends correctly**

```bash
tail -10 RELEASING.md
```

Expected: the last few lines of the replication guide are visible with no truncation.

- [ ] **Step 4.3: Commit**

```bash
git add RELEASING.md
git commit -m "docs: document Linear release system in RELEASING.md (ITL-448)"
```

---

## Task 5: Push branch and open PR

- [ ] **Step 5.1: Re-authenticate (tokens expire after 1 hour)**

```bash
gh-app-token-generator nauticalab | gh auth login --with-token
```

- [ ] **Step 5.2: Push the branch**

```bash
git push -u origin eywalker/itl-448-set-up-linear-release-system-for-orcapod-python-v01-release
```

- [ ] **Step 5.3: Open the PR**

```bash
gh pr create \
  --title "ci: set up Linear release system for v0.1 release cut (ITL-448)" \
  --base main \
  --body "$(cat <<'EOF'
## Summary

- Adds `.github/workflows/release-sync.yml` — mirrors axon's setup; syncs merged PRs to the open Linear release draft on every push to `main`.
- Adds `linear-sync` job to `publish.yml` — runs in parallel on tag push; associates the release tag's commits with the Linear release.
- Adds `linear-complete` job to `publish.yml` — runs after `publish-pypi`; calls `complete` to transition all Merged issues linked to this release to Done in Linear.
- Updates `RELEASING.md` with a new **Linear Release System** section documenting the flow, prerequisites, and replication guide.

Closes ITL-448

## Manual prerequisites (before first release)

> These steps are **not** automated by this PR and must be completed by a workspace admin:
>
> 1. Set `LINEAR_ACCESS_KEY` as a repo secret in `nauticalab/orcapod-python` GitHub settings (same key as `metamorphic-brain/axon`).
> 2. Configure a Linear release in the workspace for orcapod-python via the Linear UI (release name, included statuses, Merged → Done transition).

## Test plan

- [ ] Verify YAML is valid: `python3 -c "import yaml; yaml.safe_load(open('.github/workflows/publish.yml'))"`
- [ ] After `LINEAR_ACCESS_KEY` secret is set: push a test tag (e.g. `v0.0.99-test`) and confirm `linear-sync` and `linear-complete` jobs run green in the Actions tab.
- [ ] Confirm a "Merged" Linear issue linked to a PR in that test release transitions to "Done".

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 5.4: Report the PR URL**

The `gh pr create` command above will print the PR URL. Copy it and share with the team.

---

## Post-PR checklist

- [ ] Remind eywalker to set `LINEAR_ACCESS_KEY` repo secret before cutting v0.1
- [ ] Confirm Linear workspace has a release configured for orcapod-python (release name, Merged → Done transition)
