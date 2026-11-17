# Packaging Cleanup for v0.2 (ITL-827) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship v0.2 under the Metamorphic Labs proprietary license, with `basedpyright` out of the runtime dependencies, an sdist that holds only what is needed to build the package, and `orcapod.__version__`.

**Architecture:** Packaging metadata only, plus a four-line `__version__` block in `src/orcapod/__init__.py`. Hatchling's sdist target is restricted with `only-include`; the wheel picks up `LICENSE` through hatchling's default `license-files`. Tests build the real distributions with `uv build` and pin their top-level contents.

**Tech Stack:** hatchling + hatch-vcs, uv 0.13, pip-licenses, pytest.

---

## Evidence gathered before planning

- `pyproject.toml` declares `license = { text = "MIT License" }` and the classifier `License :: OSI Approved :: MIT License`. No LICENSE file exists. README.md, docs/, mkdocs.yml and CHANGELOG.md contain no MIT or open-source claims (`grep -rniE '\bMIT\b|open[- ]source|licen[cs]e'`); the only MIT statements are the two lines in `pyproject.toml` (plus historical `superpowers/` plans, which are records and stay untouched).
- No vendored third-party code with its own license notice: `grep -rliE 'license|copyright' src/` is empty.
- Baseline build (`uv build`, version `0.1.3.dev30+g07e5a2ea3`): sdist 3,365,148 bytes containing every repo top-level entry (`tests/`, `superpowers/`, `docs/`, `notebooks/`, `CLAUDE.md`, `.github/`, `examples/`, `bench/`, `design/`, `uv.lock`, ...); wheel 592,529 bytes with top level `orcapod/` and `orcapod-<ver>.dist-info/`, no LICENSE.
- License checks: `pip-licenses` audits the installed `orcapod` itself (it reports `orcapod 0.1.3.dev30+g07e5a2ea3 MIT License`). The repo's `_license-check.yml` and the org's required `metamorphic-labs/shared-workflows` `ci-license-check.yml` both run `--allow-only ... --partial-match`. The org allow-list ends with `LicenseRef-Metamorphic-Proprietary`; a plain `Proprietary` would fail it.
- Coordinator decision (ask, 2026-11-09): declare `LicenseRef-Metamorphic-Proprietary` in the prevailing org form, keep the LICENSE text exactly as the template's, and add the identifier to the repo's own `--allow-only` list (one line). Prevailing form across org repos (`gh search code`): `license = {text = "LicenseRef-Metamorphic-Proprietary"}` in 6 of 9 repos (3 use the PEP 639 string form).
- `pip-licenses` defaults to `--from=mixed`, which prefers trove classifiers over the `License` metadata field. A `License :: Other/Proprietary License` classifier would therefore make pip-licenses report `Other/Proprietary License` and fail both allow-lists. Task 1 verifies this empirically; if confirmed, the classifier is not added (it "does not apply" because it would break the required check) and that is reported.
- Runtime dependencies with no import in `src/`: `basedpyright` (dev tool, moved), `pymongo` (nothing depends on it), `pyyaml`, `typing_extensions` (transitive anyway). `s3fs` (fsspec plugin UPath needs for `s3://`), `tzdata` (pandas needs it), `gitpython` and `typer` (imported) are genuine runtime deps. ~~Coordinator decision: keep `pymongo`, `pyyaml`, `typing_extensions`; report as follow-up.~~ Superseded by the developer decision in the addendum below: `pymongo` and `pyyaml` are removed, `typing_extensions` moves to the dev group.
- hatch-vcs generates `src/orcapod/_version.py` (gitignored) defining `__version__: str`.
- CI runs `uv run pytest -v -m "not postgres"`, with `uv` on PATH and `fetch-depth: 0`.

## Files

- Create: `LICENSE`
- Modify: `pyproject.toml` (`license`, classifiers, dependency lists, `[tool.hatch.build.targets.sdist]`)
- Modify: `uv.lock` (basedpyright move; per the addendum, also the `pymongo`/`pyyaml` removal and the `typing_extensions` move)
- Modify: `.github/workflows/_license-check.yml` (append the identifier to `--allow-only`)
- Modify: `src/orcapod/__init__.py` (`__version__` block and `__all__` entry)
- Modify: `tests/test_packaging.py` (new tests)
- Modify: `DESIGN_ISSUES.md` (one entry appended at the end)

### Task 1: License

- [ ] **Step 1: Failing tests** appended to `tests/test_packaging.py`:

```python
LICENSE_ID = "LicenseRef-Metamorphic-Proprietary"


def test_license_is_metamorphic_proprietary():
    """Orcapod is proprietary to Metamorphic Labs from v0.2 on (ITL-827)."""
    project = tomllib.loads(PYPROJECT.read_text())["project"]
    assert project["license"] == {"text": LICENSE_ID}
    assert not [c for c in project.get("classifiers", []) if "OSI Approved" in c]
    assert (REPO_ROOT / "LICENSE").read_text().startswith(
        "Copyright (c) Metamorphic Labs, Inc. All Rights Reserved."
    )
```

- [ ] **Step 2:** `uv run pytest tests/test_packaging.py -v` → FAIL (`MIT License` != ...).
- [ ] **Step 3:** Create `LICENSE` byte-identical to the template (`gh api repos/metamorphic-labs/python-project/contents/template-python/LICENSE -q .content | base64 -d > LICENSE`). In `pyproject.toml` set `license = { text = "LicenseRef-Metamorphic-Proprietary" }` and remove the MIT classifier. Empirically check whether adding `License :: Other/Proprietary License` changes what pip-licenses reports (re-sync, run `uv run pip-licenses --packages orcapod`); keep it only if both license checks still pass.
- [ ] **Step 4:** Append `;LicenseRef-Metamorphic-Proprietary` to the `--allow-only` value in `.github/workflows/_license-check.yml`.
- [ ] **Step 5:** Tests pass. Commit `feat(packaging): license Orcapod as Metamorphic Labs proprietary (ITL-827)`.

### Task 2: basedpyright to the dev group

- [ ] **Step 1: Failing test:**

```python
def test_basedpyright_is_a_dev_dependency_only():
    """A type checker is a development tool, not something users install (ITL-827)."""
    data = tomllib.loads(PYPROJECT.read_text())
    runtime = [d.lower() for d in data["project"]["dependencies"]]
    dev = [d.lower() for d in data["dependency-groups"]["dev"]]
    assert not [d for d in runtime if d.startswith("basedpyright")]
    assert [d for d in dev if d.startswith("basedpyright")]
```

- [ ] **Step 2:** FAIL. **Step 3:** move `"basedpyright>=1.38.1"` into `[dependency-groups] dev` (alphabetical slot after `httpie`? the list is roughly alphabetical; place it first, before `httpie`). Run `uv lock` (no `--upgrade`), then compare `git diff uv.lock`: keep only the hunks for the `orcapod` package's `dependencies` / `requires-dist` / `dev` group; hand-revert any lockfile `revision` bump or marker reordering; confirm `uv lock --check` exits 0.
- [ ] **Step 4:** Tests pass. Commit `fix(deps): move basedpyright from runtime to dev dependencies (ITL-827)`.

### Task 3: sdist contents

- [ ] **Step 1: Failing tests** (module-scoped fixture builds once; skip only if `uv` is absent):

```python
@pytest.fixture(scope="module")
def built_dists(tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, Path]:
    uv = shutil.which("uv")
    if uv is None:
        pytest.skip("uv is required to build the distributions")
    out_dir = tmp_path_factory.mktemp("dist")
    subprocess.run([uv, "build", "--out-dir", str(out_dir), str(REPO_ROOT)], check=True, capture_output=True)
    return next(out_dir.glob("*.tar.gz")), next(out_dir.glob("*.whl"))


def test_sdist_contains_only_build_inputs(built_dists): top-level names == {"LICENSE", "PKG-INFO", "README.md", "pyproject.toml", "src"}; src/ holds only orcapod/
def test_wheel_top_level(built_dists): top-level == {"orcapod", "orcapod-<ver>.dist-info"}
def test_license_shipped_in_sdist_and_wheel(built_dists): LICENSE bytes in both == repo LICENSE
```

(`uv build` builds the sdist and then the wheel *from the sdist*, so the wheel test also proves the trimmed sdist is buildable.)

- [ ] **Step 2:** FAIL (sdist holds tests/, superpowers/, ...; LICENSE absent before Task 1 is in the build).
- [ ] **Step 3:** add to `pyproject.toml`:

```toml
[tool.hatch.build.targets.sdist]
only-include = ["src/orcapod", "README.md", "LICENSE"]
```

- [ ] **Step 4:** Tests pass; record sizes. Commit `build(packaging): ship only package source, README and LICENSE in the sdist (ITL-827)`.

### Task 4: `orcapod.__version__`

- [ ] **Step 1: Failing tests** run in subprocesses (a meta-path finder blocks `orcapod._version`; no `sys.modules` manipulation): normal import returns `orcapod._version.__version__` and equals `importlib.metadata.version("orcapod")`; with `_version` blocked it falls back to the installed metadata version; with both unavailable it is `"0.0.0+unknown"`.
- [ ] **Step 2:** FAIL (`AttributeError: module 'orcapod' has no attribute '__version__'`).
- [ ] **Step 3:** top of `src/orcapod/__init__.py`:

```python
try:
    from ._version import __version__
except ImportError:  # source tree without the hatch-vcs generated _version.py
    from importlib.metadata import PackageNotFoundError, version

    try:
        __version__ = version("orcapod")
    except PackageNotFoundError:
        __version__ = "0.0.0+unknown"
```

and add `"__version__"` to `__all__`. Check ruff + basedpyright report nothing new on these lines.
- [ ] **Step 4:** Tests pass. Commit `feat: expose orcapod.__version__ (ITL-827)`.

### Task 5: Design issue, verification, PR

- [ ] Append one `DESIGN_ISSUES.md` entry (resolved) at the end.
- [ ] Full suite `uv run pytest tests/`.
- [ ] Build; clean venv `uv venv` + `uv pip install <wheel>` (no overrides); `uv pip check`; smoke pipeline (DictSource -> function pod) and print `orcapod.__version__`.
- [ ] License checks: repo command (`uv sync --locked --dev` + `_license-check.yml` command) and org command (`uv run --no-dev --with pip-licenses pip-licenses --allow-only=<org default> --partial-match --ignore-packages <org default>`), both exit 0; show the orcapod line.
- [ ] Rebase on origin/main, push, PR "Fixes ITL-827".

## Addendum: developer decision on unused runtime dependencies (2026-11-09)

The developer reversed the earlier "keep" decision: remove a runtime dependency that is unused
anywhere, and move one used only by tests to the dev group.

Evidence (imports, `LazyModule`/`import_module` strings, entry points and plugin loading over
`src/`, `tests/`, `test-objective/`, `examples/`, `bench/`, `notebooks/`, `docs/`, `design/`;
reverse dependencies from `uv tree --all-groups --invert`):

- `pymongo`: no reference anywhere; nothing else depends on it. Removed.
- `pyyaml`: imported only by two `notebooks/old_tutorials/` notebooks, which target a package
  that no longer exists; `ray` (extra) and dev tools (pre-commit, mkdocs, omegaconf) declare it
  themselves. Removed.
- `typing_extensions`: imported only by `tests/test_logical_types/test_protocols.py`
  (`get_protocol_members`, stdlib only from 3.13); `pydantic` and others still pull it in at
  runtime. Moved to the dev group.
- Kept, not clear-cut: `tzdata` (no import; time-zone database for `zoneinfo`/pyarrow on
  systems without one, also required by pandas) and `s3fs` (no import; the fsspec
  implementation `UPath` loads by name for `s3://` URIs).

### Task 6: drop and move the unused dependencies

- [ ] Failing tests `test_unused_package_is_not_a_dependency[pymongo|pyyaml]` and
  `test_typing_extensions_is_a_dev_dependency_only` (PEP 503 name normalization).
- [ ] Edit `pyproject.toml`; `uv lock`; trim the lock to the `orcapod` block plus the removed
  `pymongo` package block; `uv lock --check`.
- [ ] Re-run the clean install, smoke test and both license checks.
