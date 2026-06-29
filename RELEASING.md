# Releasing Orcapod

This document describes how to cut a release of `orcapod` to PyPI.

## Branching Model

- All development happens on feature branches off `main`.
- There are no long-lived branches (the historical `dev` branch has been retired).
- No back-merges are needed.

## Cutting a Release

1. **Merge your branch into `main`** — open a PR, get it reviewed, merge it.

2. **Tag the commit on `main`** — the version is derived automatically from the git
   tag by `hatch-vcs` (`dynamic = ["version"]` in `pyproject.toml`). No manual
   version bump is needed.

   ```bash
   git checkout main
   git pull origin main
   git tag v0.1.0          # or v0.1.0rc1 for a pre-release
   git push origin v0.1.0
   ```

3. **CI takes over** — pushing the tag triggers the publish workflow
   (`.github/workflows/publish.yml`):

   ```
   license-check → test → build → publish to TestPyPI → publish to PyPI
   ```

   TestPyPI is always published first as a staging step before the final PyPI release.

## Pre-releases

Release candidates (e.g. `v0.1.0rc1`) follow exactly the same path. PyPI handles
the stable vs pre-release distinction natively:

- `pip install orcapod` — installs the latest **stable** release only
- `pip install --pre orcapod` — installs the latest release including pre-releases

## Tag Format

| Release type | Tag format | Example |
|-------------|------------|---------|
| Stable | `vMAJOR.MINOR.PATCH` | `v0.1.0` |
| Release candidate | `vMAJOR.MINOR.PATCHrcN` | `v0.1.0rc1` |
| Alpha | `vMAJOR.MINOR.PATCHaN` | `v0.1.0a1` |
| Beta | `vMAJOR.MINOR.PATCHbN` | `v0.1.0b1` |

All of the above trigger the publish workflow. PyPI classifies them automatically.

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
