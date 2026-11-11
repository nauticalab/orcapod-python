# ITL-875 public release readiness: implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give the developer an inventory to decide from before v0.3.0 goes to public PyPI and
the repository goes public, and land only the uncontroversial cleanups.

**Architecture:** Two deliverables on branch `eywalker/itl-875-public-release-readiness`
(PR body "Refs ITL-875"): (A) `superpowers/specs/2026-11-11-public-release-readiness.md`, an
inventory with options and consequences but no decisions; (B) a small cleanup in the same PR.
No library code, no `release.yml` behaviour change, no repository setting change.

**Tech Stack:** gitleaks / trufflehog (throwaway, via brew or `uvx`), `gh api`, `uv`,
`pip-licenses`.

---

## Findings already established (2026-11-11, `main` at `b197544e`)

- `RELEASING.md` has no `nauticalab` URL left (PR #269 fixed them); its one mention is the
  historical sentence "moved from `nauticalab/orcapod-python` to ...". Leave it.
- `README.md:35` and `CHANGELOG.md:342` link `nauticalab/orcapod-extension-spikeinterface`.
  That repository still exists, public, under `nauticalab` (and does not exist under
  `metamorphic-labs`), so the link is correct. Leave it.
- `LICENSE` / `tests/test_packaging.py` copyright holder "NauticaLab": leave (brief).
- `.github/workflows/_license-check.yml` is owned by this repository; it is called by
  `run-tests.yml` and `release.yml` with `uses: ./...`. The organization's required license
  check is a different workflow (`metamorphic-labs/shared-workflows`,
  `.github/workflows/ci-license-check.yml`, through ruleset `standard-python-project`'s
  `python-required-ci.yml`), whose allow-list also contains `LicenseRef-Metamorphic-Proprietary`
  (not ours to change; report it).

## Task 1: History secret scan

- [ ] Install gitleaks and trufflehog in a throwaway location (brew or release binary in the
      scratchpad). Never print secret values: use `--redact` (gitleaks) and post-process
      trufflehog JSON to drop `Raw`/`RawV2`/`Redacted`.
- [ ] `gitleaks git --redact --report-format json` over the full history (all refs).
- [ ] `trufflehog git file://<repo> --json --no-update` (all branches), with and without
      `--only-verified`.
- [ ] Triage each finding (commit, file, rule, true/false positive). Supplement with a pattern
      search (`git log -p --all -G`) for AWS keys, private keys, tokens, passwords in URLs,
      `.env` files.

## Task 2: Internal-material inventory

- [ ] Count and locate: `superpowers/`, `docs/metamorphic/`, `docs/design/`, `design/`,
      `AGENT_COORDINATION.md`, `CLAUDE.md`, `.zed/rules`, `DESIGN_ISSUES.md`, root plan files
      (`plan.md`, `function-execution-improvements-plan.md`, `TESTING_PLAN.md`,
      `orcapod-design.md`, `test-objective/`), `linear.app` links, SpiralDB / internal host
      names, emails, people's names, customer/dataset names.
- [ ] Separate what is on `main` today from what exists only in history.
- [ ] Options per category: leave / remove from `main` (stays in history) / rewrite history
      (with consequences: SHAs change, PR links, tags `v0.1.x`/`v0.2.0`, forks, open PRs).

## Task 3: Dependency licenses

- [ ] `uv export` the runtime set and each extra; run `pip-licenses` in throwaway envs
      (`uv run --no-dev --with pip-licenses`, and per extra) and record licenses.
- [ ] Check each is installable from public PyPI (pyspiral in particular).

## Task 4: Dependabot alerts

- [ ] `gh api repos/metamorphic-labs/orcapod-python/dependabot/alerts --paginate` (state
      open): count by severity, by scope (runtime / development), by package.

## Task 5: Release workflow walk-through for 0.3.0 and checklists

- [ ] Describe each job of `release.yml`; prerequisites (trusted publishers, environments,
      `LINEAR_ACCESS_KEY`); check what exists now with `gh api .../environments` (read only).
- [ ] Note: no TestPyPI-only mode; a hand-made tag blocks publishing that version.
- [ ] Release-day checklist and repository-going-public checklist.

## Task 6: Cleanup PR (part B)

- [ ] Verify that removing `LicenseRef-Metamorphic-Proprietary` from `_license-check.yml`
      passes: run the workflow's exact command locally with the shortened allow-list
      (`uv sync --locked --dev` environment). Expected: exit 0.
- [ ] If exit 0: remove the entry; commit
      `ci(license-check): drop the proprietary license from the allow-list (ITL-875)`.
- [ ] No `nauticalab` URL needs changing (see findings above); say so in the PR.
- [ ] CHANGELOG: none needed (CI-only change, not user-visible) unless the coordinator asks.

## Task 7: Write the spec, verify, PR

- [ ] Write `superpowers/specs/2026-11-11-public-release-readiness.md`.
- [ ] Ruff/format/basedpyright unaffected (no Python change); run `uv run pytest tests/` once.
- [ ] Merge origin/main, push, open PR against `main` (body "Refs ITL-875"), attach to
      Linear, `orca worktree set --pr`, handle CodeRabbit, then `worker_done`.

## Progress notes

- 2026-11-11: plan written; branch renamed and based on `origin/main` `b197544e`.
- 2026-11-11: Tasks 1-6 done. Secret scan (gitleaks 8.30.1, trufflehog 3.99.8, pattern search,
  GitHub text): no secrets, all hits triaged as false positives. Inventory written to
  `superpowers/specs/2026-11-11-public-release-readiness.md`. `_license-check.yml` entry
  removed after a local run of the exact command passed (commit `542344da`). No `nauticalab`
  URL needed changing. Remaining: full test suite, PR, CodeRabbit, worker_done.
