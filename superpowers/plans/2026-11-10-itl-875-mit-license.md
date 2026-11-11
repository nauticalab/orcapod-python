# ITL-875 (license part): relicense Orcapod under MIT — Implementation Plan

**Goal:** From v0.2.0 Orcapod is MIT licensed, to be published on public PyPI as `orcapod`
from a public repository (developer decision, November 10, 2026). This PR applies only the
license: `LICENSE`, `pyproject.toml`, the packaging test, and every statement of the license or
of "internal registry only" outside the release docs. It reverses the proprietary license of
PR #277 (ITL-827). PR body says `Refs ITL-875` (the rest of the issue follows).

**Out of scope (owned elsewhere):** `release.yml`, `CHANGELOG.md`, `RELEASING.md`, and the
release-docs content of PR #269 (ITL-810). Sentences there that MIT / public PyPI invalidates
are listed in the worker report, not edited.

## Context

- `pyproject.toml` declares `license = { text = "LicenseRef-Metamorphic-Proprietary" }` with no
  license classifier. ITL-827 avoided a classifier because `pip-licenses --from=mixed` prefers
  trove classifiers over the `License` field, and `License :: Other/Proprietary License` fails
  the allow list.
- The build backend is `hatchling` (originally `hatchling>=1.21.0`, now `>=1.27.0`; not locked by `uv.lock`; `uv build`
  resolves the latest, which emits Metadata-Version 2.5). `pip-licenses` is locked at 5.5.5.
- `_license-check.yml` runs `uv run pip-licenses --allow-only="MIT;...;LicenseRef-Metamorphic-Proprietary" --partial-match`
  over the synced dev environment, which includes `orcapod` itself.

## Decision: the `license` declaration form

Preferred: PEP 639, `license = "MIT"` (SPDX expression), no license classifier (PEP 639
forbids combining an expression with license classifiers), `LICENSE` picked up by hatchling's
default `license-files`. Metadata then carries `License-Expression: MIT` and
`License-File: LICENSE`. The build floor is `hatchling>=1.27.0` (see the revision note
below); Task 2 also builds pinned to that floor (constraints file `hatchling==1.27.0`) to confirm it emits
Metadata-Version 2.4 with the declaration.

**Revised after review (CodeRabbit on #283, approved by the coordinator):** hatchling 1.21.0
writes `License-Expression` under `Metadata-Version: 2.1`, which does not define the field.
The build floor is raised to `hatchling>=1.27.0` (metadata 2.4), and the packaging test
asserts the floor, a built Metadata-Version of at least 2.4, and the exact MIT text.

Fallback (only if pip-licenses 5.5.5 does not report `MIT` for a `License-Expression`-only
distribution): `license = { text = "MIT" }` plus `License :: OSI Approved :: MIT License`.

Verified empirically in Task 2 before committing.

## Tasks

### Task 1: failing test first
- `tests/test_packaging.py`: replace `test_license_is_metamorphic_proprietary` with
  `test_license_is_mit`: `project.license == "MIT"`, no `License ::` classifier, and `LICENSE`
  is the standard MIT text with the single copyright line
  `Copyright (c) 2025-2026 NauticaLab` (holder decided by the developer, November 10, 2026; spelled `NauticaLab` per review comment 4534040178). Add `test_built_metadata_declares_mit` over the
  existing `built_dists` fixture: wheel METADATA has `License-Expression: MIT` and no
  `License:` field or license classifier; sdist PKG-INFO the same.
- Run; show both fail.

### Task 2: apply the license
- `LICENSE`: standard MIT text (SPDX `MIT` template), copyright line exactly once.
- `pyproject.toml`: `license = "MIT"`; build floor `hatchling>=1.27.0`.
- Verify: `uv build` twice, once with the latest hatchling and once at the floor (write
  `hatchling==1.27.0` to a constraints file, then `uv build --build-constraint <file>`); inspect wheel METADATA and sdist PKG-INFO, confirm LICENSE in both;
  `uv sync --locked --dev` then the `_license-check.yml` command verbatim, plus
  `pip-licenses --packages orcapod` showing `MIT`.
- Run the packaging tests; green.

### Task 3: statements of the license / distribution route
- `README.md` "Releasing": drop the internal package index sentence; say it is published on
  PyPI as `orcapod` under MIT; add a short `## License` section.
- `docs/index.md` "Installation": `pip install orcapod` / `uv add orcapod`, drop the
  "install command pending" note.
- `CLAUDE.md` / `.zed/rules`: only if they state the license (grep showed none).
- `DESIGN_ISSUES.md` DEP2 (ITL-827): append a dated note that the license decision was
  reversed to MIT on November 10, 2026 (ITL-875). History unchanged.
- `_license-check.yml` keeps its allow list unchanged (the brief does not ask to edit it);
  whether to drop `LicenseRef-Metamorphic-Proprietary` from it is reported.

### Task 4: verification and report
- Four CI commands: ruff check, ruff format --check, basedpyright (exit code), full pytest.
- Report-only investigations: pip-licenses for every runtime dependency (default install, and
  extras), non-MIT-compatible ones; scan of the tree for internal-only material (internal URLs,
  hostnames, tokens, customer/dataset names); PR #269 sentences needing change.
- Merge origin/main if moved, push, open PR (`Refs ITL-875`), link via orca.

### Task 5: review fix, holder spelling (comment 4534040178)
- The developer asked for the holder to be spelled `NauticaLab` (capital L).
- Failing test first: `COPYRIGHT_LINE` in `tests/test_packaging.py` becomes
  `Copyright (c) 2025-2026 NauticaLab`; run `test_license_is_mit`, show it fails.
- `LICENSE` line 3 becomes `Copyright (c) 2025-2026 NauticaLab`; test green.
- Lower-case `nauticalab` in URLs and repository slugs is an identifier and stays unchanged.
- Rebuild wheel and sdist; the shipped LICENSE carries the new spelling and METADATA still
  says `License-Expression: MIT`. Four CI commands, one commit, reply on the thread.
