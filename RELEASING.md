# Releasing Orcapod

This document describes how to cut a release of `orcapod`.

## Publish target

Orcapod is published on public PyPI as [`orcapod`](https://pypi.org/project/orcapod/), under
the MIT License (developer decision, 2026-11-10; the license is applied from `0.2.0`). Users
install it with `pip install orcapod` or `uv add orcapod`. `0.2.0` was cut as a tag only
(developer decision, 2026-11-10); `0.3.0` is the first release published to PyPI from this
repository.

The Release workflow (`.github/workflows/release.yml`) is the only publishing route. It runs
only when dispatched by hand, uploads to TestPyPI first and then to PyPI, and authenticates
to both through OIDC trusted publishing: the workflow uses no API token. That
requires, on each index, a trusted publisher for the `orcapod` project naming owner
`metamorphic-labs`, repository `orcapod-python`, workflow `release.yml` and environment
`testpypi` (TestPyPI) or `pypi` (PyPI); see *Prerequisites* below. Only an owner of the
`orcapod` project on each index can add one.

## Branching Model

- All development happens on feature branches off `main`.
- There are no long-lived branches (the historical `dev` branch has been retired).
- No back-merges are needed.

## Before You Cut

Do these on `main` before triggering the workflow:

1. **Update `CHANGELOG.md`** in a release pull request, the only kind of pull request that
   edits it. Feature pull requests add `changelog.d/<ISSUE-ID>.md` fragments instead (see
   `changelog.d/README.md`).
   1. Fold the fragments into the section of the version being released:

      ```
      uv run python scripts/assemble_changelog.py --version X.Y.Z
      ```

      This appends each fragment's text to the matching category of `## [X.Y.Z]` (in order
      of issue id, without moving the lines already there), creates the section or a
      category if it is missing, and deletes the fragments. It stops without changing
      anything if a fragment is malformed; `--check` reports the same problems without
      folding. Commit the changed `CHANGELOG.md` and the deleted fragments together;
      `changelog.d/README.md` stays.
   2. Set the section's heading to the version being released and its date
      (`## [X.Y.Z] — YYYY-MM-DD`), and make sure breaking changes carry migration notes.

   The GitHub Release body is generated from PR titles; `CHANGELOG.md` is the curated
   record.
2. **Check that `main` is green** — the `Run Tests` workflow on the commit you are
   releasing. The Release workflow re-runs the unit tests on Python 3.11, 3.12 and 3.13
   and the license check, but not the PostgreSQL or SpiralDB integration jobs.
3. **Check the version is free** — the tag must not exist on origin, and the version must
   not exist on PyPI or TestPyPI; neither index allows a version to be uploaded twice, even
   after it is deleted.

Per-release checklists live in `superpowers/plans/` (for example
`2026-11-09-v0.2-release-checklist.md`).

## Cutting a Release

Releases are triggered from the GitHub Actions UI — no manual `git tag` step required.
**Do not create the release tag by hand:** the `build` job aborts when the tag already exists
on origin, so a hand-made `vX.Y.Z` tag means that version can no longer be published by the
workflow until the tag is deleted.

1. **Merge your branch into `main`** — open a PR, get it reviewed, merge it.

2. **Trigger the release workflow** — go to
   **[Actions → Release → Run workflow](https://github.com/metamorphic-labs/orcapod-python/actions/workflows/release.yml)**
   in the GitHub UI, enter the version (e.g. `0.1.0`), optionally enter a branch name
   (defaults to `main`), and click **Run workflow**.

   A leading `v` is stripped automatically — `v0.1.0` and `0.1.0` both work.

3. **CI takes over** — the workflow runs the following jobs automatically:

   ```
   validate-branch ─┬─ test ──────────┐
                    └─ license-check ──┤─ build ─┬─ publish-testpypi ─ publish-pypi ─┐
                                                 └─ linear-sync ──────────────────────┴─ linear-complete
   ```

   - Pre-flight: branch validation, then tests on Python 3.11, 3.12 and 3.13 and license check (parallel after validation)
   - Build: normalises version (strips a leading `v`), creates the local tag, builds wheel + sdist with `uv build`, aborts if the tag already exists on origin, pushes the tag, uploads `dist/` as an artifact
   - TestPyPI: in the `testpypi` environment, `uv publish --publish-url https://test.pypi.org/legacy/ dist/*` with an OIDC token (`id-token: write`) — a staging upload
   - PyPI: in the `pypi` environment, after TestPyPI succeeds, `uv publish dist/*` with an OIDC token, then creates the GitHub Release for the tag with generated release notes and the dist files attached
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
   **[Actions → Release → Run workflow](https://github.com/metamorphic-labs/orcapod-python/actions/workflows/release.yml)**
2. In the **Use workflow from** dropdown, keep `main` selected (so the workflow version
   includes the `Branch` field — do not select `hotfix/0.1.x` here)
3. Set **Version** to `0.1.1`
4. Set **Branch** to `hotfix/0.1.x`
5. Click **Run workflow**

The workflow will confirm the branch exists, run tests against it, create the `v0.1.1`
tag at its tip, and publish to TestPyPI and PyPI — identical to a normal release.

## If a Run Fails Part-Way

The `build` job pushes the `vX.Y.Z` tag to origin *before* anything is published, so a
failure in a later job leaves the tag behind.

| Failed job | State left behind | Recovery |
|-----------|-------------------|----------|
| `test` or `license-check` | Nothing | Fix on `main`, trigger a new run |
| `build`, before "Push release tag" | Nothing | Fix, trigger a new run |
| `build`, after "Push release tag" | Tag on origin, no artifact | Delete the tag (`git push origin :refs/tags/vX.Y.Z`), then trigger a new run |
| `publish-testpypi` | Tag on origin | Use **Re-run failed jobs** on the same run — do not start a new run, which would fail creating a tag that already exists |
| `publish-pypi` | Tag on origin, version on TestPyPI | Use **Re-run failed jobs**. If the upload to PyPI itself partly succeeded, the version is burned: release the next patch version instead |
| `linear-sync` | Unknown: it needs only `build` and runs in parallel with publishing, so it can fail before anything is published | Check `publish-testpypi` and `publish-pypi` first and recover them by their rows above. Then re-run `linear-sync`; it does not affect publishing |
| `linear-complete` | Release is published (it runs only after `publish-pypi` and `linear-sync` succeed) | Re-run the failed job; it does not affect publishing |

Never delete a tag whose version has been published to any package index.

## Release Lineage

`v0.1.3` and `v0.1.4` were cut from a maintenance branch and are **not** ancestors of
`main`; `git describe` on `main` therefore reports a distance from `v0.1.2`. This does not
affect a release from `main`, because the workflow creates the release tag on the commit
being built and `hatch-vcs` reads that tag. The fix those two releases carried is on
`main`. The commit tagged `v0.1.4` is reachable only through its tag.

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

Before cutting the first release with this system active (items 1 and 4 need a repository
admin, item 2 a Linear workspace admin, item 3 an owner of the `orcapod` project on PyPI and
TestPyPI):

1. **Set the `LINEAR_ACCESS_KEY` repo secret** in `metamorphic-labs/orcapod-python` GitHub
   settings (`Settings → Secrets and variables → Actions → New repository secret`).
   Use the same Linear API key as `metamorphic-brain/axon`.

2. **Configure a Linear release** in the workspace for orcapod-python via the Linear UI:
   set the release name, which statuses count as "included" (at minimum: Merged), and
   the target state for the transition (Done).

3. **Point the PyPI and TestPyPI trusted publishers at this repository.** The repository
   moved from `nauticalab/orcapod-python` to `metamorphic-labs/orcapod-python`. On
   pypi.org and test.pypi.org, an owner of the `orcapod` project opens *Manage → Publishing*
   and adds a GitHub trusted publisher with owner `metamorphic-labs`, repository
   `orcapod-python`, workflow `release.yml` and environment `pypi` (PyPI) or `testpypi`
   (TestPyPI). An upload from a repository that no trusted publisher names is rejected, which fails `publish-testpypi` after the tag has been pushed.
4. **Create the `testpypi` and `pypi` environments** in the repository settings
   (`Settings → Environments`), if they do not exist.

If `LINEAR_ACCESS_KEY` is not set: the `release-sync` workflow runs but the Linear action
may fail; the `linear-sync` and `linear-complete` jobs in `release.yml` fail but do not
block publishing (they have no dependents in the publish chain).

### Replicating for other orcapod repos

1. Copy `.github/workflows/release-sync.yml` verbatim.
2. Add `linear-sync` and `linear-complete` jobs to the repo's release workflow,
   adjusting `needs:` in `linear-complete` to point at whichever job creates the GitHub
   Release.
3. Set the `LINEAR_ACCESS_KEY` repo secret.
4. Configure a Linear release in the workspace for the new repo.
