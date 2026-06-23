# pyspiral Upgrade (PLT-1773) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade `pyspiral` from the broken `0.11.7` to `0.14.9` so that all three
`TestSpiralDBConnectorIntegration` tests pass green in CI again.

**Architecture:** Three targeted edits — bump the minimum version constraint in
`pyproject.toml`, verify the `uv.lock` pin is at `0.14.9` (already updated during
investigation), and add breadcrumb documentation so the next person to hit a
pyspiral-related failure has context. No connector code changes needed: the API is
fully backward-compatible across this version range.

**Tech Stack:** Python, uv, pyspiral 0.14.9, pytest

---

### Task 1: Bump the `pyspiral` minimum in `pyproject.toml`

**Files:**
- Modify: `pyproject.toml:47-49`

- [ ] **Step 1: Update the constraint**

In `pyproject.toml`, change line 48 from:

```toml
[project.optional-dependencies]
redis = ["redis>=6.2.0"]
ray = ["ray[default]==2.48.0", "ipywidgets>=8.1.7"]
postgresql = ["psycopg[binary]>=3.0"]
spiraldb = [
    "pyspiral>=0.11.0",
]
```

to:

```toml
[project.optional-dependencies]
redis = ["redis>=6.2.0"]
ray = ["ray[default]==2.48.0", "ipywidgets>=8.1.7"]
postgresql = ["psycopg[binary]>=3.0"]
spiraldb = [
    "pyspiral>=0.14.0",
]
```

- [ ] **Step 2: Verify `uv.lock` is already pinned to 0.14.9**

```bash
grep -A 2 '^name = "pyspiral"' uv.lock
```

Expected output (version must be `0.14.9`):

```
name = "pyspiral"
version = "0.14.9"
source = { registry = "https://pypi.org/simple" }
```

If the version is still `0.11.7`, run:

```bash
uv lock --upgrade-package pyspiral
```

Then re-run the grep to confirm `0.14.9`.

- [ ] **Step 3: Re-sync the environment to confirm the constraint resolves cleanly**

```bash
uv sync --all-extras --dev
```

Expected: exits 0 with no resolution errors. The last few lines should show
`pyspiral-0.14.9` installed (or already satisfied).

- [ ] **Step 4: Run the full unit test suite to confirm no regressions**

```bash
uv run pytest tests/test_databases/test_spiraldb_connector.py -v
```

Expected: all tests PASS. The spiraldb connector unit tests mock the `spiral`
module entirely, so the version bump does not affect them.

- [ ] **Step 5: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "chore(deps): upgrade pyspiral 0.11.7 → 0.14.9 (PLT-1773)

t3.storage.dev began enforcing that all HTTP request headers must appear in
X-Amz-SignedHeaders around 2026-06-15. pyspiral 0.11.7's embedded Rust HTTP
client sent additional unsigned headers, causing every Vortex file read to
return AccessDenied. pyspiral 0.14.x rewrote the HTTP stack using Python-level
httpx, which resolves the issue.

Bumps the spiraldb extra floor from >=0.11.0 to >=0.14.0 to document the
effective minimum. No connector code changes required — full API compatibility
verified across 0.11.7 → 0.14.9 (see PLT-1773 spec).

Closes PLT-1773"
```

---

### Task 2: Add breadcrumb to integration test module docstring

**Files:**
- Modify: `tests/test_databases/test_spiraldb_connector_integration.py:1-19`

- [ ] **Step 1: Extend the module docstring**

Replace the existing module docstring (lines 1–19) with:

```python
"""Integration tests for SpiralDBConnector against the live dev project.

These tests are skipped unless the ``SPIRAL_INTEGRATION_TESTS=1`` env var is set.
They require a valid auth mechanism, either:

- Local dev: ``~/.config/pyspiral/auth.json`` (obtained via ``spiral login``)
- CI (GitHub Actions): ``SPIRAL_WORKLOAD_ID`` env var pointing to a workload
  that has editor access on the dev project and is bound to this repository via
  GitHub OIDC (see internal CI/runbook docs for the exact workload/policy IDs).

If the env var is set but credentials are absent, the tests fail rather than
skip — the operator is expected to ensure auth is in place when enabling
integration tests.

Project and server URL are configurable via env vars:

- ``SPIRAL_PROJECT_ID`` (default: ``test-orcapod-362211``)
- ``SPIRAL_SERVER_URL`` (default: ``http://api.spiraldb.dev``)

Version compatibility note (PLT-1773)
--------------------------------------
pyspiral is in active development and must be kept current. The pinned version
in ``uv.lock`` should be upgraded regularly (see PLT-1785 for the tracking issue).

History: around 2026-06-15, ``t3.storage.dev`` (SpiralDB's object-storage
backend) began strictly enforcing that every header in a presigned-URL request
must appear in ``X-Amz-SignedHeaders``. pyspiral 0.11.7's embedded Rust HTTP
client sent additional unsigned headers, causing all Vortex file reads to fail
with ``AccessDenied: There were headers present in the request which were not
signed``. Upgrading to pyspiral 0.14.x (which rewrote the HTTP stack in Python
using ``httpx``) resolved the issue. If these tests start failing with a similar
``AccessDenied`` / unsigned-headers error, upgrade pyspiral first.
"""
```

- [ ] **Step 2: Verify the file still imports cleanly**

```bash
uv run python -c "import tests.test_databases.test_spiraldb_connector_integration" 2>&1 || \
uv run python -c "
import importlib.util, sys
spec = importlib.util.spec_from_file_location(
    'test_mod',
    'tests/test_databases/test_spiraldb_connector_integration.py'
)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
print('OK')
"
```

Expected: `OK` (or a clean exit with no ImportError; the `pytest.mark.skipif`
at module level is fine to execute).

- [ ] **Step 3: Commit**

```bash
git add tests/test_databases/test_spiraldb_connector_integration.py
git commit -m "docs(spiraldb): add PLT-1773 breadcrumb to integration test module docstring

Explains the t3.storage.dev header-signing enforcement change (2026-06-15)
and the unsigned-headers AccessDenied failure pattern so future maintainers
know to check the pyspiral version first."
```

---

### Task 3: Add entry to `DESIGN_ISSUES.md`

**Files:**
- Modify: `DESIGN_ISSUES.md` (append new section)

- [ ] **Step 1: Add the new entry**

Append the following block at the end of `DESIGN_ISSUES.md` (after the last `---`
separator or at the very end of the file):

```markdown
---

## `pyspiral` dependency (SpiralDB integration)

### SP1 — pyspiral 0.11.7 broke against t3.storage.dev header-signing enforcement change
**Status:** resolved
**Severity:** high
**Issue:** PLT-1773

Around 2026-06-15, SpiralDB's object-storage backend (`t3.storage.dev`) began
strictly enforcing that every header present in a presigned S3-style GET request
must appear in `X-Amz-SignedHeaders`. pyspiral 0.11.7's embedded Rust HTTP
client sent additional unsigned headers alongside the presigned URL, causing all
Vortex file reads to return:

```
AccessDenied: There were headers present in the request which were not signed
```

All three `TestSpiralDBConnectorIntegration` tests failed on every CI push from
2026-06-15 onward. The runner image version was identical between the last green
and first red run, confirming this was a server-side enforcement change rather
than a runner regression.

**Fix:** Upgraded pyspiral from `0.11.7` to `0.14.9`. The 0.14.x line rewrote
the HTTP/auth stack to use Python-level `httpx` instead of the embedded Rust
client, eliminating the unsigned-header problem. The `pyproject.toml` minimum
was bumped from `>=0.11.0` to `>=0.14.0` to document the effective floor.
No connector code changes were needed — the public API is fully compatible
across this version range.

**Ongoing:** pyspiral releases frequently. See PLT-1785 for the tracking issue
covering routine version bumps.
```

- [ ] **Step 2: Run a quick smoke test to confirm no other tests broke**

```bash
uv run pytest tests/test_databases/ -v --ignore=tests/test_databases/test_spiraldb_connector_integration.py
```

Expected: all tests PASS (the integration tests are excluded since they require
live SpiralDB credentials).

- [ ] **Step 3: Commit**

```bash
git add DESIGN_ISSUES.md
git commit -m "docs(design-issues): log SP1 — pyspiral header-signing regression (PLT-1773)

Records the root cause (t3.storage.dev enforcement change), symptom
(AccessDenied on all Vortex reads), and resolution (pyspiral upgrade to
0.14.9) in DESIGN_ISSUES.md for future reference."
```

---

### Task 4: Push branch and verify CI

**Files:** none

- [ ] **Step 1: Push the branch**

```bash
git push -u origin eywalker/plt-1773-fix-ongoing-spiral-database-test-failure-in-ci
```

Expected: branch pushed without errors.

- [ ] **Step 2: Open a PR targeting `dev`**

```bash
gh pr create \
  --base dev \
  --title "fix(spiraldb): upgrade pyspiral 0.11.7 → 0.14.9 to fix CI header-signing failure (PLT-1773)" \
  --body "$(cat <<'EOF'
## Summary

- Upgrades `pyspiral` from `0.11.7` to `0.14.9` in `uv.lock`
- Bumps the `spiraldb` extra minimum in `pyproject.toml` from `>=0.11.0` to `>=0.14.0`
- Adds breadcrumb documentation to the integration test module docstring and `DESIGN_ISSUES.md`

## Root cause

Around 2026-06-15, `t3.storage.dev` (SpiralDB's object-storage backend) began strictly
enforcing that every header in a presigned-URL request must appear in
`X-Amz-SignedHeaders`. `pyspiral 0.11.7`'s embedded Rust HTTP client sent additional
unsigned headers, causing all Vortex file reads to fail with:

```
AccessDenied: There were headers present in the request which were not signed
```

`pyspiral 0.14.x` rewrites the HTTP stack using Python-level `httpx`, resolving the issue.
Full API compatibility was verified — no connector code changes were needed.

## Test plan

- [ ] Local: `uv run pytest tests/test_databases/test_spiraldb_connector.py -v` — all PASS
- [ ] CI: `spiral-integration` job on this push must go green (definitive end-to-end verification)

Fixes PLT-1773
Related: PLT-1785 (ongoing pyspiral version tracking)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

- [ ] **Step 3: Monitor the `spiral-integration` CI job**

The `spiral-integration` job runs on push (not on PRs), so it will fire when the
branch is pushed. Watch the Actions tab for the run. Expected outcome: all three
`TestSpiralDBConnectorIntegration` tests PASS.

If they fail with a different error (not the unsigned-headers `AccessDenied`), the
pyspiral 0.14.9 API may have a breaking change not caught by the static inspection.
In that case, read the new error, identify which API call broke, and update
`spiraldb_connector.py` accordingly.
