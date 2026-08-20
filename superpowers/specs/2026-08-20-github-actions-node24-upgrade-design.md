# GitHub Actions Node 24 Upgrade — Design Spec

**Issue:** ITL-621
**Date:** 2026-08-20
**Status:** Approved

## Overview

Upgrade all five third-party GitHub Actions from their current Node 20 majors to their current
Node 24 majors. The GitHub runner is already shimming them onto Node 24; this upgrade removes
the deprecation warning and eliminates the risk of CI breaking when the shim is withdrawn.

## Goals & Success Criteria

- All five actions on their current major, pinned to full commit SHAs with accurate `# vX.Y.Z` comments
- No Node 20 deprecation warning in any workflow run
- Each breaking change explicitly checked against our usage, finding documented in the PR
- Full CI green across all workflows

## Scope & Boundaries

In scope:
- `actions/checkout` v4 → v7
- `astral-sh/setup-uv` v5 → v10
- `actions/setup-python` v5 → v7
- `codecov/codecov-action` v5 → v7
- `actions/dependency-review-action` v4 → v5

Out of scope:
- Other actions in `release.yml` (`upload-artifact`, `download-artifact`, `softprops/action-gh-release`, `linear/linear-release-action`) — not Node 20 actions
- `prune-cache: true` back-fill — accepted new default; monitoring deferred to follow-up issue

## Commit Plan

Single PR targeting `main`, five commits ordered by risk (smallest version jump first):

| # | Commit | Action | Old version | New SHA | New version | Files |
|---|---|---|---|---|---|---|
| 1 | `ci: upgrade dependency-review-action to v5.0.0` | `actions/dependency-review-action` | v4.9.0 | `a1d282b36b6f3519aa1f3fc636f609c47dddb294` | v5.0.0 | `run-tests.yml` |
| 2 | `ci: upgrade setup-python to v7.0.0` | `actions/setup-python` | v5.6.0 | `5fda3b95a4ea91299a34e894583c3862153e4b97` | v7.0.0 | `run-tests.yml`, `run-objective-tests.yml`, `run-postgres-tests.yml` |
| 3 | `ci: upgrade codecov-action to v7.0.0` | `codecov/codecov-action` | v5.5.5 | `fb8b3582c8e4def4969c97caa2f19720cb33a72f` | v7.0.0 | `run-tests.yml`, `run-objective-tests.yml`, `run-postgres-tests.yml` |
| 4 | `ci: upgrade actions/checkout to v7.0.0` | `actions/checkout` | v4.4.0 | `9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0` | v7.0.0 | all 6 workflow files |
| 5 | `ci: upgrade setup-uv to v10.0.1` | `astral-sh/setup-uv` | v5.4.2 | `20cfd1bf945f4377ade1205e4dbc17946fc9a30d` | v10.0.1 | all 6 workflow files |

## Breaking Change Findings

| Action | Breaking changes crossed | Finding |
|---|---|---|
| `actions/dependency-review-action` v4→v5 | Node 20→24 runtime only | ✅ No-op — `deny-licenses` list unchanged |
| `actions/setup-python` v5→v7 | v6: Node 20→24; v7: `pip-install` input removed | ✅ No-op — we don't use `pip-install`; uv handles all installs |
| `codecov/codecov-action` v5→v7 | v6: Node 24; v7: GPG signing key rotated (`codecovsecurity`→`codecovsecops`) | ✅ No-op — we don't verify Codecov signatures |
| `actions/checkout` v4→v7 | v5: Node 20→24; v6: credentials in separate file; v7: blocks fork PR head for `pull_request_target`/`workflow_run` | ✅ No-op — `release.yml` git operations work with new credential location; we only use `pull_request`, not `pull_request_target` |
| `astral-sh/setup-uv` v5→v10 | v6: input defaults changed; v7: Node 20→24, `server-url` removed; v8: immutable releases; v9: `prune-cache` default true→false; v10: `enable-cache: auto` disables cache for `pull_request_target`/`workflow_run` | ✅ Mostly no-op — none of the changed inputs are used; v10 cache change doesn't apply (we use `pull_request`/`push`/`workflow_dispatch`); `prune-cache` flip accepted, monitored via follow-up issue |

## prune-cache Decision

`setup-uv` v9 flipped `prune-cache` from `true` (default) to `false`. Decision: **accept the
new default** and monitor cache storage after the bump. If cache entries grow to an unacceptable
size, set `prune-cache: true` explicitly across all `setup-uv` call sites in a follow-up commit.

A follow-up Linear issue will be created to track this monitoring task.

## Follow-up

- Create Linear issue: "Monitor Actions cache size after setup-uv prune-cache default flip (ITL-621 follow-up)"
