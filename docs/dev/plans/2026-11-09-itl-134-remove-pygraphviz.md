# Remove pygraphviz and Restore Green CI (ITL-134) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Drop the unused `pygraphviz` dependency so `uv sync` no longer compiles a C extension against Graphviz headers, which is what turned every CI job red when `ubuntu-latest` moved to ubuntu-26.04.

**Architecture:** No source change. `pygraphviz` leaves `pyproject.toml` and `uv.lock`; every workflow stops installing `libgraphviz-dev` but keeps the `graphviz` apt package, because `src/orcapod/pipeline/graph.py` uses the pure-Python `graphviz` package, which shells out to the `dot` binary. The MinIO job in `tests.yml` is a separate failure handled in Task 4 after the coordinator's decision.

**Tech Stack:** uv, GitHub Actions, pytest.

---

## Evidence gathered before planning

- `grep -rniE "pygraphviz|nx_agraph|to_agraph|graphviz_layout|agraph|nx_pydot|pydot"` over the repo (excluding `.git`, `.venv`, `uv.lock`) matches only `pyproject.toml`, the workflow apt lines (`libgraphviz-dev`), and historical `superpowers/` plans. Nothing in `src/`, `tests/`, `test-objective/`, `notebooks/`, `examples/`, `bench/`, `design/`, `docs/`, or `README.md`.
- networkx imports pygraphviz only inside `networkx.drawing.nx_agraph`; no `nx.drawing` / `nx_agraph` / `nx_pydot` reference exists in `src/`, `tests/`, or `test-objective/`.
- `src/orcapod/pipeline/graph.py:613,669` import `graphviz` (pure Python, needs `dot` at render time only).
- Local repro of the CI failure class: `uv sync` on a machine without Graphviz headers fails with `Failed to build pygraphviz==1.14 ... 'graphviz/cgraph.h' file not found`.
- CI: failing jobs run on `Image: ubuntu-26.04`; error is `graphviz_wrap.c:5480: passing argument 5 of 'gvRenderData' from incompatible pointer type`.
- README and docs contain no Graphviz header install instructions today.
- MinIO: Docker Hub `minio/minio` returns 404; `quay.io/minio/minio` returns 401 to anonymous pulls. `ghcr.io/coollabsio/minio:RELEASE.2025-10-15T17-29-55Z` exists (digest `sha256:69b55a1c1c5dc285ce04db96689f5b2102317fc77a50680a1874ca6efd1c87f9`).

## Files

- Create: `tests/test_packaging.py` (regression guard)
- Modify: `pyproject.toml` (remove one dependency line), `uv.lock` (regenerated)
- Modify: `.github/workflows/{run-tests,run-objective-tests,run-postgres-tests,_license-check,release}.yml`; delete `.github/workflows/tests.yml`
- Modify: `README.md`, `docs/index.md` (one sentence each on optional `dot`)
- Modify: `CHANGELOG.md` (one entry under `[Unreleased]` → `### Changed`)
- Modify: `DESIGN_ISSUES.md` (one entry appended at the end)

### Task 1: Regression test

- [ ] **Step 1: Write the failing test** in `tests/test_packaging.py`:

```python
"""Packaging guards for the dependency list in ``pyproject.toml``."""

from __future__ import annotations

import tomllib
from pathlib import Path

PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"


def test_pygraphviz_is_not_a_dependency():
    """``pygraphviz`` needs Graphviz C headers to build and nothing imports it (ITL-134)."""
    project = tomllib.loads(PYPROJECT.read_text())["project"]
    declared = list(project["dependencies"])
    for extra in project.get("optional-dependencies", {}).values():
        declared.extend(extra)
    assert not [d for d in declared if d.lower().startswith("pygraphviz")]
```

- [ ] **Step 2: Show it fails.** The project venv cannot be built before the fix, so run it outside the project:
  `uv run --no-project --with pytest pytest -c /dev/null --rootdir . --noconftest tests/test_packaging.py`
  Expected: FAIL, listing `pygraphviz>=1.14`.

### Task 2: Remove the dependency

- [ ] **Step 1:** Delete the line `    "pygraphviz>=1.14",` from `[project] dependencies` in `pyproject.toml`.
- [ ] **Step 2:** `uv lock`, then `git diff --stat uv.lock` and `git diff uv.lock`. Expected: only the three pygraphviz hunks removed (dependency entry, `requires-dist` entry, `[[package]]` block). If any other package version moved, restore `uv.lock` from git, delete those three hunks by hand, and confirm with `uv lock --check`.
- [ ] **Step 3:** `uv sync --locked --all-extras --dev` succeeds locally with no Graphviz headers installed.
- [ ] **Step 4:** `uv run pytest tests/test_packaging.py` passes.

### Task 3: CI, docs, changelog

- [ ] **Step 1:** In every workflow replace `apt-get install -y graphviz libgraphviz-dev` with `apt-get install -y graphviz` (`tests.yml` is removed in Task 4). Keep `runs-on: ubuntu-latest`; pin to `ubuntu-24.04` only if the PR's CI shows another image-related failure.
- [ ] **Step 2:** README "Running Tests Locally" and `docs/index.md` "Installation": add one sentence that the Graphviz `dot` binary is optional and only needed to render pipeline graphs; no development headers are required.
- [ ] **Step 3:** CHANGELOG `[Unreleased]` → `### Changed`: add a "Removed `pygraphviz` dependency (ITL-134)" entry.
- [ ] **Step 4:** Append a resolved entry to `DESIGN_ISSUES.md`.
- [ ] **Step 5:** `uv run pytest tests/` (full suite) and the license check command from `_license-check.yml`. Record failures and skips by name.
- [ ] **Step 6:** Commit `fix(deps): remove unused pygraphviz dependency (ITL-134)`, push, open the PR against `main` with `Fixes ITL-134`.

### Task 4: MinIO job (`tests.yml`)

Coordinator decision: remove the workflow in this PR. The official image no longer exists on
Docker Hub or quay.io, a third-party image is the developer's call, and the workflow is
otherwise redundant with `run-tests.yml`. S3 integration tests currently skip everywhere
(the testcontainers fallback uses the same missing image); restoring them is tracked as ITL-819.

- [ ] `git rm .github/workflows/tests.yml`. Do not touch `tests/test_databases/conftest.py`.

### Task 5: Verify on CI

- [ ] Wait for the PR's checks; report the actual conclusions of `test (3.11)`, `test (3.12)`, `objective-test`, `license-check`, and the MinIO job.
