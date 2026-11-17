# GitHub Actions Node 24 Upgrade — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade all five third-party GitHub Actions from Node 20 majors to Node 24 majors, eliminating the deprecation warning and the risk of CI breaking when the GitHub runner shim is withdrawn.

**Architecture:** Pure workflow YAML edits — replace the pinned SHA and `# vX.Y.Z` comment for each action across all affected workflow files. No Python code changes. Five commits ordered by risk (smallest version jump first), all in one PR targeting `main`.

**Tech Stack:** GitHub Actions YAML, `sed`, `grep`, `python -c "import yaml"` for syntax validation.

---

## Files modified

| File | Actions touched |
|---|---|
| `.github/workflows/run-tests.yml` | all five |
| `.github/workflows/run-objective-tests.yml` | setup-python, codecov-action, checkout, setup-uv |
| `.github/workflows/run-postgres-tests.yml` | setup-python, codecov-action, checkout, setup-uv |
| `.github/workflows/tests.yml` | checkout, setup-uv |
| `.github/workflows/_license-check.yml` | checkout, setup-uv |
| `.github/workflows/release.yml` | checkout, setup-uv |
| `.github/workflows/release-sync.yml` | checkout |

---

## Task 0: Create the feature branch

**Files:** none

- [ ] **Step 1: Check out the branch**

```bash
git checkout -b eywalker/itl-621-upgrade-all-pinned-github-actions-to-current-major-versions
```

- [ ] **Step 2: Verify you are on the correct branch**

```bash
git branch --show-current
```

Expected output:
```
eywalker/itl-621-upgrade-all-pinned-github-actions-to-current-major-versions
```

---

## Task 1: Upgrade `actions/dependency-review-action` v4.9.0 → v5.0.0

**Files:**
- Modify: `.github/workflows/run-tests.yml`

**Breaking changes confirmed no-op:** Node 20→24 runtime only. The `deny-licenses` list is unchanged and carries over.

- [ ] **Step 1: Apply the replacement**

```bash
sed -i 's|actions/dependency-review-action@2031cfc080254a8a887f58cffee85186f0e49e48  # v4.9.0|actions/dependency-review-action@a1d282b36b6f3519aa1f3fc636f609c47dddb294  # v5.0.0|g' \
  .github/workflows/run-tests.yml
```

- [ ] **Step 2: Verify old SHA is gone**

```bash
grep -r "2031cfc080254a8a887f58cffee85186f0e49e48" .github/workflows/
```

Expected: no output (no matches).

- [ ] **Step 3: Verify new SHA is present**

```bash
grep -r "a1d282b36b6f3519aa1f3fc636f609c47dddb294" .github/workflows/
```

Expected:
```
.github/workflows/run-tests.yml:        uses: actions/dependency-review-action@a1d282b36b6f3519aa1f3fc636f609c47dddb294  # v5.0.0
```

- [ ] **Step 4: Validate YAML syntax**

```bash
python3 -c "import yaml, sys; yaml.safe_load(open('.github/workflows/run-tests.yml'))" && echo "OK"
```

Expected: `OK`

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/run-tests.yml
git commit -m "ci: upgrade dependency-review-action to v5.0.0 (ITL-621)

Node 20→24 runtime upgrade only. deny-licenses list unchanged — no-op."
```

---

## Task 2: Upgrade `actions/setup-python` v5.6.0 → v7.0.0

**Files:**
- Modify: `.github/workflows/run-tests.yml` (2 occurrences: test job, spiral-integration job)
- Modify: `.github/workflows/run-objective-tests.yml` (1 occurrence)
- Modify: `.github/workflows/run-postgres-tests.yml` (1 occurrence)

**Breaking changes confirmed no-op:**
- v6: Node 20→24.
- v7: `pip-install` input removed — we don't use it; uv handles all package installs.

- [ ] **Step 1: Apply the replacement across all three files**

```bash
sed -i 's|actions/setup-python@a26af69be951a213d495a4c3e4e4022e16d87065  # v5.6.0|actions/setup-python@5fda3b95a4ea91299a34e894583c3862153e4b97  # v7.0.0|g' \
  .github/workflows/run-tests.yml \
  .github/workflows/run-objective-tests.yml \
  .github/workflows/run-postgres-tests.yml
```

- [ ] **Step 2: Verify old SHA is gone**

```bash
grep -r "a26af69be951a213d495a4c3e4e4022e16d87065" .github/workflows/
```

Expected: no output.

- [ ] **Step 3: Verify new SHA appears in all three files**

```bash
grep -r "5fda3b95a4ea91299a34e894583c3862153e4b97" .github/workflows/
```

Expected (4 lines — 2 in run-tests.yml, 1 each in the other two):
```
.github/workflows/run-objective-tests.yml:      - uses: actions/setup-python@5fda3b95a4ea91299a34e894583c3862153e4b97  # v7.0.0
.github/workflows/run-postgres-tests.yml:      - uses: actions/setup-python@5fda3b95a4ea91299a34e894583c3862153e4b97  # v7.0.0
.github/workflows/run-tests.yml:      - uses: actions/setup-python@5fda3b95a4ea91299a34e894583c3862153e4b97  # v7.0.0
.github/workflows/run-tests.yml:        uses: actions/setup-python@5fda3b95a4ea91299a34e894583c3862153e4b97  # v7.0.0
```

- [ ] **Step 4: Validate YAML syntax for all three files**

```bash
for f in .github/workflows/run-tests.yml .github/workflows/run-objective-tests.yml .github/workflows/run-postgres-tests.yml; do
  python3 -c "import yaml; yaml.safe_load(open('$f'))" && echo "OK: $f"
done
```

Expected: three `OK` lines.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/run-tests.yml \
        .github/workflows/run-objective-tests.yml \
        .github/workflows/run-postgres-tests.yml
git commit -m "ci: upgrade setup-python to v7.0.0 (ITL-621)

v6: Node 20→24. v7: pip-install input removed — we don't use it (uv handles installs)."
```

---

## Task 3: Upgrade `codecov/codecov-action` v5.5.5 → v7.0.0

**Files:**
- Modify: `.github/workflows/run-tests.yml` (1 occurrence: test job)
- Modify: `.github/workflows/run-objective-tests.yml` (1 occurrence)
- Modify: `.github/workflows/run-postgres-tests.yml` (1 occurrence)

**Breaking changes confirmed no-op:**
- v6: Node 24 support.
- v7: GPG signing key rotated from `codecovsecurity` to `codecovsecops` — we don't verify Codecov signatures.

- [ ] **Step 1: Apply the replacement across all three files**

```bash
sed -i 's|codecov/codecov-action@0fb7174895f61a3b6b78fc075e0cd60383518dac  # v5.5.5|codecov/codecov-action@fb8b3582c8e4def4969c97caa2f19720cb33a72f  # v7.0.0|g' \
  .github/workflows/run-tests.yml \
  .github/workflows/run-objective-tests.yml \
  .github/workflows/run-postgres-tests.yml
```

- [ ] **Step 2: Verify old SHA is gone**

```bash
grep -r "0fb7174895f61a3b6b78fc075e0cd60383518dac" .github/workflows/
```

Expected: no output.

- [ ] **Step 3: Verify new SHA appears in all three files**

```bash
grep -r "fb8b3582c8e4def4969c97caa2f19720cb33a72f" .github/workflows/
```

Expected (3 lines, one per file):
```
.github/workflows/run-objective-tests.yml:        uses: codecov/codecov-action@fb8b3582c8e4def4969c97caa2f19720cb33a72f  # v7.0.0
.github/workflows/run-postgres-tests.yml:        uses: codecov/codecov-action@fb8b3582c8e4def4969c97caa2f19720cb33a72f  # v7.0.0
.github/workflows/run-tests.yml:        uses: codecov/codecov-action@fb8b3582c8e4def4969c97caa2f19720cb33a72f  # v7.0.0
```

- [ ] **Step 4: Validate YAML syntax for all three files**

```bash
for f in .github/workflows/run-tests.yml .github/workflows/run-objective-tests.yml .github/workflows/run-postgres-tests.yml; do
  python3 -c "import yaml; yaml.safe_load(open('$f'))" && echo "OK: $f"
done
```

Expected: three `OK` lines.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/run-tests.yml \
        .github/workflows/run-objective-tests.yml \
        .github/workflows/run-postgres-tests.yml
git commit -m "ci: upgrade codecov-action to v7.0.0 (ITL-621)

v6: Node 24 support. v7: GPG key rotation only — we don't verify Codecov signatures."
```

---

## Task 4: Upgrade `actions/checkout` v4.4.0 → v7.0.0

**Files:**
- Modify: `.github/workflows/run-tests.yml` (3 occurrences: test, spiral-integration, dependency-review jobs)
- Modify: `.github/workflows/run-objective-tests.yml` (1 occurrence)
- Modify: `.github/workflows/run-postgres-tests.yml` (1 occurrence)
- Modify: `.github/workflows/tests.yml` (1 occurrence)
- Modify: `.github/workflows/_license-check.yml` (1 occurrence)
- Modify: `.github/workflows/release.yml` (4 occurrences: test, build, linear-sync, linear-complete jobs)
- Modify: `.github/workflows/release-sync.yml` (1 occurrence: sync job)

**Breaking changes confirmed no-op:**
- v5: Node 20→24.
- v6: Credentials written to a separate file instead of `.git/config`. `release.yml` uses `git push` and `git tag` which both work correctly with the new credential location — nothing reads the extraheader out of `.git/config` directly.
- v7: Blocks checking out the fork PR head for `pull_request_target` and `workflow_run` events. We only use `pull_request`, `push`, and `workflow_dispatch` — no `pull_request_target` anywhere in our workflows.

- [ ] **Step 1: Apply the replacement across all seven files**

```bash
sed -i 's|actions/checkout@11d5960a326750d5838078e36cf38b85af677262  # v4.4.0|actions/checkout@9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0  # v7.0.0|g' \
  .github/workflows/run-tests.yml \
  .github/workflows/run-objective-tests.yml \
  .github/workflows/run-postgres-tests.yml \
  .github/workflows/tests.yml \
  .github/workflows/_license-check.yml \
  .github/workflows/release.yml \
  .github/workflows/release-sync.yml
```

- [ ] **Step 2: Verify old SHA is gone**

```bash
grep -r "11d5960a326750d5838078e36cf38b85af677262" .github/workflows/
```

Expected: no output.

- [ ] **Step 3: Verify new SHA count — expect 12 occurrences total**

```bash
grep -rc "9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0" .github/workflows/
```

Expected (files with non-zero counts only):
```
.github/workflows/_license-check.yml:1
.github/workflows/release-sync.yml:1
.github/workflows/release.yml:4
.github/workflows/run-objective-tests.yml:1
.github/workflows/run-postgres-tests.yml:1
.github/workflows/run-tests.yml:3
.github/workflows/tests.yml:1
```

- [ ] **Step 4: Confirm no `pull_request_target` or `workflow_run` triggers in any workflow**

```bash
grep -r "pull_request_target\|workflow_run" .github/workflows/
```

Expected: no output (confirms v7 fork-head block is a no-op for us).

- [ ] **Step 5: Validate YAML syntax for all seven files**

```bash
for f in .github/workflows/run-tests.yml \
          .github/workflows/run-objective-tests.yml \
          .github/workflows/run-postgres-tests.yml \
          .github/workflows/tests.yml \
          .github/workflows/_license-check.yml \
          .github/workflows/release.yml \
          .github/workflows/release-sync.yml; do
  python3 -c "import yaml; yaml.safe_load(open('$f'))" && echo "OK: $f"
done
```

Expected: seven `OK` lines.

- [ ] **Step 6: Commit**

```bash
git add .github/workflows/run-tests.yml \
        .github/workflows/run-objective-tests.yml \
        .github/workflows/run-postgres-tests.yml \
        .github/workflows/tests.yml \
        .github/workflows/_license-check.yml \
        .github/workflows/release.yml \
        .github/workflows/release-sync.yml
git commit -m "ci: upgrade actions/checkout to v7.0.0 (ITL-621)

v5: Node 20→24. v6: creds in separate file — git push/tag in release.yml unaffected.
v7: fork-head block for pull_request_target/workflow_run — we don't use either event."
```

---

## Task 5: Upgrade `astral-sh/setup-uv` v5.4.2 → v10.0.1

**Files:**
- Modify: `.github/workflows/run-tests.yml` (2 occurrences: test, spiral-integration jobs)
- Modify: `.github/workflows/run-objective-tests.yml` (1 occurrence)
- Modify: `.github/workflows/run-postgres-tests.yml` (1 occurrence)
- Modify: `.github/workflows/tests.yml` (1 occurrence)
- Modify: `.github/workflows/_license-check.yml` (1 occurrence)
- Modify: `.github/workflows/release.yml` (4 occurrences: test, build, publish-testpypi, publish-pypi jobs)

**Breaking changes confirmed no-op:**
- v6: `activate-environment`, `working-directory`, `cache-dependency-glob` defaults changed — we don't pass any of these inputs.
- v7: Node 20→24; `server-url` input removed — we don't use it.
- v8: Immutable releases; major/minor tags gone — we SHA-pin anyway, so no impact.
- v9: `prune-cache` default flips `true` → `false`. **Decision: accept the new default.** Cache entries will grow slightly; monitor after merge (tracked in follow-up issue).
- v10: `enable-cache: auto` disables cache for `pull_request_target`/`workflow_run`. We don't use either event. `tests.yml` uses `enable-cache: true` explicitly — this is unaffected.

- [ ] **Step 1: Apply the replacement across all six files**

```bash
sed -i 's|astral-sh/setup-uv@d4b2f3b6ecc6e67c4457f6d3e41ec42d3d0fcb86  # v5.4.2|astral-sh/setup-uv@20cfd1bf945f4377ade1205e4dbc17946fc9a30d  # v10.0.1|g' \
  .github/workflows/run-tests.yml \
  .github/workflows/run-objective-tests.yml \
  .github/workflows/run-postgres-tests.yml \
  .github/workflows/tests.yml \
  .github/workflows/_license-check.yml \
  .github/workflows/release.yml
```

- [ ] **Step 2: Verify old SHA is gone**

```bash
grep -r "d4b2f3b6ecc6e67c4457f6d3e41ec42d3d0fcb86" .github/workflows/
```

Expected: no output.

- [ ] **Step 3: Verify new SHA count — expect 10 occurrences total**

```bash
grep -rc "20cfd1bf945f4377ade1205e4dbc17946fc9a30d" .github/workflows/
```

Expected (files with non-zero counts only):
```
.github/workflows/_license-check.yml:1
.github/workflows/release.yml:4
.github/workflows/run-objective-tests.yml:1
.github/workflows/run-postgres-tests.yml:1
.github/workflows/run-tests.yml:2
.github/workflows/tests.yml:1
```

- [ ] **Step 4: Confirm `enable-cache: true` is still explicit in tests.yml**

```bash
grep "enable-cache" .github/workflows/tests.yml
```

Expected:
```
          enable-cache: true
```

- [ ] **Step 5: Validate YAML syntax for all six files**

```bash
for f in .github/workflows/run-tests.yml \
          .github/workflows/run-objective-tests.yml \
          .github/workflows/run-postgres-tests.yml \
          .github/workflows/tests.yml \
          .github/workflows/_license-check.yml \
          .github/workflows/release.yml; do
  python3 -c "import yaml; yaml.safe_load(open('$f'))" && echo "OK: $f"
done
```

Expected: six `OK` lines.

- [ ] **Step 6: Commit**

```bash
git add .github/workflows/run-tests.yml \
        .github/workflows/run-objective-tests.yml \
        .github/workflows/run-postgres-tests.yml \
        .github/workflows/tests.yml \
        .github/workflows/_license-check.yml \
        .github/workflows/release.yml
git commit -m "ci: upgrade setup-uv to v10.0.1 (ITL-621)

v7: Node 20→24. v8: immutable releases (SHA-pin unaffected). v9: prune-cache default
flips to false — accepted, monitoring via follow-up issue. v10: enable-cache: auto
change for pull_request_target/workflow_run — we don't use either event."
```

---

## Task 6: Final verification, PR, and follow-up issue

**Files:** none

- [ ] **Step 1: Confirm no Node-20 action SHAs remain anywhere in workflow files**

```bash
# Old SHAs that must all be absent:
for sha in \
  11d5960a326750d5838078e36cf38b85af677262 \
  d4b2f3b6ecc6e67c4457f6d3e41ec42d3d0fcb86 \
  a26af69be951a213d495a4c3e4e4022e16d87065 \
  0fb7174895f61a3b6b78fc075e0cd60383518dac \
  2031cfc080254a8a887f58cffee85186f0e49e48; do
  result=$(grep -r "$sha" .github/workflows/)
  if [ -n "$result" ]; then
    echo "STILL PRESENT: $sha"
    echo "$result"
  else
    echo "GONE: $sha"
  fi
done
```

Expected: five `GONE:` lines, no `STILL PRESENT:` lines.

- [ ] **Step 2: Confirm all five new SHAs are present**

```bash
for sha in \
  9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0 \
  20cfd1bf945f4377ade1205e4dbc17946fc9a30d \
  5fda3b95a4ea91299a34e894583c3862153e4b97 \
  fb8b3582c8e4def4969c97caa2f19720cb33a72f \
  a1d282b36b6f3519aa1f3fc636f609c47dddb294; do
  count=$(grep -rc "$sha" .github/workflows/ | awk -F: '{sum += $2} END {print sum}')
  echo "SHA $sha: $count occurrences"
done
```

Expected:
```
SHA 9c091bb21b7c1c1d1991bb908d89e4e9dddfe3e0: 12 occurrences   (checkout, 7 files)
SHA 20cfd1bf945f4377ade1205e4dbc17946fc9a30d: 10 occurrences   (setup-uv, 6 files)
SHA 5fda3b95a4ea91299a34e894583c3862153e4b97: 4 occurrences    (setup-python, 3 files)
SHA fb8b3582c8e4def4969c97caa2f19720cb33a72f: 3 occurrences    (codecov-action, 3 files)
SHA a1d282b36b6f3519aa1f3fc636f609c47dddb294: 1 occurrence     (dependency-review-action, 1 file)
```

- [ ] **Step 3: Push the branch**

```bash
git push -u origin eywalker/itl-621-upgrade-all-pinned-github-actions-to-current-major-versions
```

- [ ] **Step 4: Create the follow-up Linear issue**

Use the Linear MCP tool:

```
mcp__claude_ai_Linear__save_issue(
  title: "Monitor Actions cache size after setup-uv prune-cache default flip",
  team: "Tools",
  description: "## Overview\n\nITL-621 upgraded setup-uv from v5 to v10. v9.0.0 flipped the `prune-cache`\ndefault from `true` to `false`. We accepted the new default rather than\nback-filling `prune-cache: true` across all six setup-uv call sites.\n\nMonitor the GitHub Actions cache after this lands to determine if the\nlarger cache footprint is acceptable.\n\n## Goals & Success Criteria\n\n* Check Actions cache storage in the repository after a week of CI runs.\n* If cache entries exceed a reasonable threshold, add `prune-cache: true`\n  explicitly to all setup-uv call sites in a follow-up commit.\n* Close this issue once the cache behaviour is confirmed acceptable or\n  the explicit pin has been added.\n\n## Dependencies & Risks\n\n* Depends on ITL-621 merging first."
)
```

- [ ] **Step 5: Open the PR**

```bash
gh pr create \
  --title "ci: upgrade all pinned GitHub Actions to Node 24 majors (ITL-621)" \
  --base main \
  --body "$(cat <<'EOF'
## Summary

Upgrades all five third-party GitHub Actions from their Node 20 majors to current Node 24 majors, resolving the deprecation warning on every CI run.

Closes ITL-621

## Actions upgraded

| Action | Old | New |
|---|---|---|
| `actions/dependency-review-action` | v4.9.0 | v5.0.0 |
| `actions/setup-python` | v5.6.0 | v7.0.0 |
| `codecov/codecov-action` | v5.5.5 | v7.0.0 |
| `actions/checkout` | v4.4.0 | v7.0.0 |
| `astral-sh/setup-uv` | v5.4.2 | v10.0.1 |

## Breaking change audit

| Action | Breaking changes crossed | Finding |
|---|---|---|
| `dependency-review-action` v4→v5 | Node 20→24 runtime only | ✅ No-op — `deny-licenses` list unchanged |
| `setup-python` v5→v7 | v6: Node 20→24; v7: `pip-install` input removed | ✅ No-op — we don't use `pip-install`; uv handles all installs |
| `codecov-action` v5→v7 | v6: Node 24; v7: GPG key rotation | ✅ No-op — we don't verify Codecov signatures |
| `checkout` v4→v7 | v5: Node 20→24; v6: creds in separate file; v7: fork-head block for `pull_request_target`/`workflow_run` | ✅ No-op — `release.yml` git operations unaffected by new cred location; we don't use `pull_request_target` |
| `setup-uv` v5→v10 | v6–v8: input/tag changes; v9: `prune-cache` default flips true→false; v10: cache disabled for `pull_request_target`/`workflow_run` | ✅ Mostly no-op — we use none of the changed inputs; v10 cache change doesn't apply; `prune-cache` flip accepted (see below) |

## prune-cache note

`setup-uv` v9 flipped `prune-cache` from `true` to `false`. Decision: accept the new default and monitor cache growth. A follow-up issue has been created to revisit if storage becomes a concern.

## Commit structure

Five commits, one per action, ordered risk-ascending — bisectable if any action changes behaviour subtly.
EOF
)"
```

---

## Self-review checklist

After all tasks complete, verify against spec acceptance criteria:

- [ ] All five actions on current major, SHA-pinned with accurate `# vX.Y.Z` comments
- [ ] No `v4.4.0`, `v5.4.2`, `v5.6.0`, `v5.5.5`, or `v4.9.0` version comments remain
- [ ] Breaking change findings documented in PR body
- [ ] Follow-up Linear issue created for prune-cache monitoring
- [ ] PR targets `main`
