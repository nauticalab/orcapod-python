# Releasing Orcapod

This document describes how to cut a release of `orcapod`.

## Publish target and where things run

Orcapod is published on public PyPI as [`orcapod`](https://pypi.org/project/orcapod/), under
the MIT License (developer decision, 2026-11-10; the license is applied from `0.2.0`). Users
install it with `pip install orcapod` or `uv add orcapod`. `0.2.0` was cut as a tag only
(developer decision, 2026-11-10); `0.3.0` is the first release published to PyPI from this
repository.

Two repositories take part (developer decision, 2026-11-12, ITL-1007):

| Repository | Role |
|---|---|
| `metamorphic-labs/orcapod-python` | **All development**: branches, pull requests, review, CI, Linear integration, the design log. |
| `nauticalab/orcapod-python` (public) | **Releases only**: receives `main` by fast-forward push at a release point, and runs the Release workflow. |

The Release workflow (`.github/workflows/release.yml`) is the only publishing route. It runs
only when dispatched by hand **on `nauticalab/orcapod-python`**, uploads to TestPyPI first and then
to PyPI, and authenticates to both through OIDC trusted publishing: the workflow uses no API
token. That requires, on each index, a trusted publisher for the `orcapod` project naming owner
`nauticalab`, repository `orcapod-python`, workflow `release.yml` and environment `testpypi`
(TestPyPI) or `pypi` (PyPI); see *One-time setup* below.

## Branching Model

- All development happens on feature branches off `main`.
- There are no long-lived branches (the historical `dev` branch has been retired).
- No back-merges are needed.

## Before You Cut

Do these on `metamorphic-labs` `main` before anything is pushed:

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
2. **Record the release in the golden history**, in the same release pull request, as its
   last change (see *Golden hash values* below):

   ```
   uv run python tests/test_hashing/golden/generate_golden_hashes.py --release X.Y.Z
   ```

   It appends the release revision to the `golden` history of the current data context's
   manifest, writes its digest under `releases` in
   `src/orcapod/contexts/released_manifests.json`, and at the data context's first release
   sets `released_in` and the `manifest` pin. It computes no hash and refuses a version
   already released. Commit both files with the release pull request; the release is tagged
   on a commit that contains them. The Release workflow's build job checks this before it
   creates the tag (`scripts/check_golden_tags.py --release-version X.Y.Z`) and stops if the
   release is not recorded, or if a golden file was added after the release step (the
   release revision must be the last revision when the commit is tagged). It also runs the
   tag check (`scripts/check_golden_tags.py`) there, so a branch that changed what an earlier
   release of the data context shipped is not released. Tags of earlier data contexts are
   not checked, so the first release of a data context passes with nothing to check.
3. **Check that `main` is green** — the `Run Tests` workflow on the commit you are
   releasing. The Release workflow re-runs the unit tests on Python 3.11, 3.12 and 3.13
   and the license check, but not the PostgreSQL or SpiralDB integration jobs.
4. **Check the version is free** — the tag must not exist on origin, and the version must
   not exist on PyPI or TestPyPI; neither index allows a version to be uploaded twice, even
   after it is deleted.

Per-release checklists live in `superpowers/plans/` (for example
`2026-11-09-v0.2-release-checklist.md`).

## Pre-push checklist

All of these hold before the developer is asked for the go:

1. **The full suite is green**: `uv run pytest tests/` passes on the commit to be pushed.
2. **The release pull request is merged** on `metamorphic-labs` (`CHANGELOG.md` folded, section
   dated, `changelog.d/` fragments deleted).
3. **The golden release step is done**: `contexts/released_manifests.json` has the current
   data context's `releases` line for this version and a pinned `manifest`, and the golden
   hash tests and the data context tests pass unchanged.
4. **`main` is green** on `metamorphic-labs`, and `git status` is clean.
5. **The version is free**: the tag does not exist on `nauticalab`, and the version is on
   neither PyPI nor TestPyPI.

## Golden hash values

Developer decisions, November 15, 2026 (ITL-946); the mechanism and what each check protects
against are in `docs/reference/golden-hashes.md`.

- A golden file that a release listed never changes, and is never removed or renamed.
- New golden values can be added in any release, a patch included.
- Any change to a released golden value, a logical type's own identifier change included,
  needs a new data context version. A new data context version ships only in a minor release
  (before 1.0) or a major one, never in a patch.
- When a new data context starts, the old context's golden files and code move to
  `orcapod-sanctuary`; this repository carries only the current context's golden folder.
- Release tags are protected on both `metamorphic-labs/orcapod-python` and
  `nauticalab/orcapod-python`: the repository ruleset `protect-release-tags` (target
  `refs/tags/v*`; rules deletion, update and non-fast-forward; no bypass actors) is in place
  since 2026-11-15. A `v*` tag can be created, as the Release workflow does, but never moved
  or deleted. This is a precondition of the CI tag check (`scripts/check_golden_tags.py`),
  which compares HEAD with every release tag of the current data context. Agents never change
  repository settings.

## Cutting a Release

Releases are triggered from the GitHub Actions UI — no manual `git tag` step required.
**Do not create the release tag by hand:** the `build` job aborts when the tag already exists
on origin, so a hand-made `vX.Y.Z` tag means that version can no longer be published by the
workflow until the tag is deleted.

1. **Push `main` to `nauticalab` as a fast-forward, on the developer's explicit go only.**
   A push to a public repository cannot be undone, so no agent does this on its own.

   ```
   git remote add nauticalab https://github.com/nauticalab/orcapod-python.git   # once
   git fetch nauticalab
   git merge-base --is-ancestor nauticalab/main main && echo "fast-forward"       # must print it
   git push nauticalab main:main                                                  # no --force
   ```

   Never a squash, never a pull request: commit ids must stay identical in both repositories,
   because the version comes from the tag (`hatch-vcs`). Never push the `v0.2.0` tag (or any
   tag): the Release workflow creates the release tag on `nauticalab`. The push also replaces
   the workflows `nauticalab` carries (its old `tests.yml` is deleted by it, as `main` removed
   that file).

2. **Run Release on `nauticalab`** — go to
   **[Actions → Release → Run workflow](https://github.com/nauticalab/orcapod-python/actions/workflows/release.yml)**
   in the GitHub UI, enter the version (e.g. `0.3.0`), optionally enter a branch name
   (defaults to `main`), and click **Run workflow**. A leading `v` is stripped automatically.

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

4. **Fetch the tag back into `metamorphic-labs`** once the run has succeeded:

   ```
   git fetch nauticalab --tags
   git push origin vX.Y.Z
   ```

   `metamorphic-labs` then holds the same tag on the same commit.

## One-time setup (only the developer can do this)

On `nauticalab/orcapod-python` and the package indexes; no agent changes settings, secrets or
environments:

1. Create the environments `testpypi` and `pypi` (`Settings → Environments`).
2. On pypi.org and test.pypi.org, an owner of the `orcapod` project opens
   *Manage → Publishing* and adds a GitHub trusted publisher with owner `nauticalab`,
   repository `orcapod-python`, workflow `release.yml` and environment `pypi` (PyPI) or
   `testpypi` (TestPyPI). An upload from a repository that no trusted publisher names is
   rejected, which fails `publish-testpypi` after the tag has been pushed. Remove the entries
   that name `metamorphic-labs`, if any.
3. Protect `main` on `nauticalab` (no force push, no deletion). It is written only by the
   fast-forward push above.
4. Protect release tags on both `metamorphic-labs/orcapod-python` and
   `nauticalab/orcapod-python` with the ruleset `protect-release-tags` (target
   `refs/tags/v*`; deletion, update, non-fast-forward; no bypass actors), so the golden tag
   check has a fixed anchor (ITL-946). Done on 2026-11-15.
5. Optional: set `CODECOV_TOKEN` there. The Codecov, SpiralDB and Linear jobs run only on
   `metamorphic-labs` and are skipped on `nauticalab`.

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

0. **Make sure `hotfix/0.1.x` on `nauticalab` points at the commit to release.** The workflow
   resolves the **Branch** input only there and tags its tip. If the branch is missing or behind,
   fast-forward it (on the developer's explicit go only, never with `--force`):
   `git push nauticalab hotfix/0.1.x:hotfix/0.1.x`
1. **Trigger the release workflow** — go to
   **[Actions → Release → Run workflow](https://github.com/nauticalab/orcapod-python/actions/workflows/release.yml)**
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

Linear is wired to the development repository only. `release-sync.yml` and the `linear-sync` and
`linear-complete` jobs of `release.yml` run only when
`github.repository == 'metamorphic-labs/orcapod-python'`; on `nauticalab` they are skipped, which
does not block or fail the release (no publish job depends on them). They call
`linear/linear-release-action` and transition the issues in **"Merged"** status to **"Done"**.

| Trigger | Workflow / Job | Action | Effect |
|---------|---------------|--------|--------|
| Push to `main` (`metamorphic-labs`) | `release-sync.yml / sync` | `sync` (no version) | Associates the merged PR with the open Linear release draft |
| Release workflow `build` succeeds (`metamorphic-labs` only) | `release.yml / linear-sync` | `sync --version <tag>` | Finalises the commit set for the version |
| After `publish-pypi` succeeds (`metamorphic-labs` only) | `release.yml / linear-complete` | `complete --version <tag>` | Marks the release done in Linear |

Because the Release workflow runs on `nauticalab`, these jobs do not run for a release at all
today; completing the Linear release is a manual step until the developer decides otherwise
(the issue ITL-1007 notes this "needs more thought"). Setup on `metamorphic-labs`: the
`LINEAR_ACCESS_KEY` repo secret (the organisation's Linear access key) and a Linear release
configured in the workspace.
