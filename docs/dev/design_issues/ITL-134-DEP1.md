## `pyproject.toml`

### DEP1 — Unused `pygraphviz` dependency breaks installs without Graphviz headers
**Status:** resolved
**Severity:** high
**Issue:** ITL-134

`pygraphviz>=1.14` was listed in `[project] dependencies` although nothing imports it
(pipeline graph rendering uses the pure-Python `graphviz` package). It ships only as an
sdist, so every `uv sync` compiled it against the system Graphviz headers; when
`ubuntu-latest` moved to ubuntu-26.04 its newer `libgraphviz-dev` broke the build and every
CI job failed.

**Fix:** Removed `pygraphviz` from `pyproject.toml` and `uv.lock`, and dropped
`libgraphviz-dev` from all workflows (the `graphviz` apt package stays for `dot`).
`tests/test_packaging.py` guards against re-adding it.
