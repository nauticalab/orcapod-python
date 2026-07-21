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
   in the GitHub UI, enter the version (e.g. `0.1.0`), optionally enter a branch name
   (defaults to `main`), and click **Run workflow**.

   A leading `v` is stripped automatically — `v0.1.0` and `0.1.0` both work.

3. **CI takes over** — the workflow runs the following jobs automatically:

   ```
   validate-branch ─┬─ test ──────────┐
                    └─ license-check ──┤─ build ─┬─ publish-testpypi ─ publish-pypi ─┐
                                                 └─ linear-sync ──────────────────────┴─ linear-complete
   ```

   - Pre-flight: branch validation, then tests on Python 3.11 and 3.12 and license check (parallel after validation)
   - Build: normalises version, creates local tag, builds wheel + sdist, pushes tag to origin
   - TestPyPI: publishes to test.pypi.org first as a staging step
   - PyPI: publishes to pypi.org and creates a GitHub Release with generated release notes
   - Linear: `linear-sync` starts immediately after build (parallel to publish); `linear-complete` runs after both PyPI publish and linear-sync succeed

## Pre-releases

Release candidates (e.g. `0.1.0rc1`) follow exactly the same path — enter the version in the
workflow input. PyPI handles the stable vs pre-release distinction natively:

- `pip install orcapod` — installs the latest **stable** release only
- `pip install --pre orcapod` — installs the latest release including pre-releases

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
