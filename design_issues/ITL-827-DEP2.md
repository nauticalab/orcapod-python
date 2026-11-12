## `pyproject.toml`

### DEP2 — Packaging declared MIT, shipped a type checker to users and a 3.3 MB sdist
**Status:** resolved
**Severity:** high
**Issue:** ITL-827

`pyproject.toml` declared `MIT License` (with the OSI MIT classifier) although Orcapod is
proprietary to Metamorphic Labs, and no LICENSE file existed. `basedpyright` sat in the
runtime dependencies, so every `pip install orcapod` pulled in a type checker. The sdist
packed the whole repository (`tests/`, `superpowers/`, `docs/`, `notebooks/`, `CLAUDE.md`,
...), and `orcapod.__version__` did not exist.

**Fix:** Added the org template LICENSE and declared
`license = { text = "LicenseRef-Metamorphic-Proprietary" }`, the identifier the org license
check allows (and added it to the repo's own `_license-check.yml` allow-list). No license
classifier is declared: pip-licenses prefers classifiers, so `License :: Other/Proprietary
License` would fail both checks. Moved `basedpyright` and `typing_extensions` (only a test
imports it) to the dev group, dropped the unused `pymongo` and `pyyaml`, restricted the sdist
with `only-include` (3.37 MB to 0.47 MB), and added `orcapod.__version__`.
`tests/test_packaging.py` builds both distributions and pins their contents.

**Update (November 10, 2026, ITL-875):** the license decision was reversed: Orcapod is MIT
licensed from v0.2.0 on, published on public PyPI as `orcapod`. `LICENSE` holds the MIT text
and `pyproject.toml` declares the PEP 639 expression `license = "MIT"` (metadata
`License-Expression: MIT`, no license classifier).
