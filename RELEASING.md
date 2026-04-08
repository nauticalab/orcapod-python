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
