# Polars value hashes carry a versioned label (ITL-578, PR #286) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `PolarsDataFrameHandler` / `PolarsSeriesHandler` return `ContentHash` methods
`polars_dataframe_v1` / `polars_series_v1`, and the version lives only in that label.

**Architecture:** Developer's decision (Nov 10, 2026): a per-type value hash carries a label
`<type>_v<N>` (as `ndarray_v1` in PR #289). The label names the whole definition (descriptor
grammar plus digest). The hashed descriptor loses its `"v"` key and
`POLARS_HASH_GRAMMAR_VERSION` goes away; the label constants replace it. Only the Polars golden
entries this PR added move (36 lines in `tests/test_hashing/golden/data`).

**Tech Stack:** Python, polars, starfix, pytest, `uv`.

---

### Task 1: Failing test for the labels

**Files:** Test: `tests/test_hashing/test_polars_dataframe_hashing.py`

- [ ] Add a test class that hashes a frame and a Series through the public semantic hasher
  (`get_default_context().semantic_hasher.hash_object(value)`) and through a datagram
  (`DictSource(...)` data `content_hash()` embeds the token) and asserts
  `.method == "polars_dataframe_v1"` / `"polars_series_v1"`, and that the descriptor no longer
  carries `"v"` (indirectly: label is the only version marker; a direct check of
  `polars_dtype_descriptor` is unaffected because it never carried `"v"`).

```python
class TestHashLabel:
    """The method label names the hash definition and is its only version marker."""

    def test_dataframe_label(self) -> None:
        hasher = get_default_context().semantic_hasher
        assert hasher.hash_object(pl.DataFrame({"a": [1]})).method == "polars_dataframe_v1"

    def test_series_label(self) -> None:
        hasher = get_default_context().semantic_hasher
        assert hasher.hash_object(pl.Series("a", [1])).method == "polars_series_v1"
```

- [ ] Run `uv run pytest tests/test_hashing/test_polars_dataframe_hashing.py -k Label -v`;
  expect FAIL (`'sha256' != 'polars_dataframe_v1'`).

### Task 2: Implement the labels

**Files:** Modify `src/orcapod/logical_types/polars_type.py`,
`src/orcapod/hashing/semantic_hashing/builtin_handlers.py` (docstrings only),
`src/orcapod/contexts/data/v0.1.json` (changelog wording).

- [ ] Replace `POLARS_HASH_GRAMMAR_VERSION = 1` with
  `POLARS_DATAFRAME_HASH_METHOD = "polars_dataframe_v1"` and
  `POLARS_SERIES_HASH_METHOD = "polars_series_v1"` (docstring: the label names the whole
  definition; never change the bytes under a label, a new definition gets a new label).
- [ ] `_content_hash(descriptor, df, method)` returns `ContentHash(method=method, ...)`.
- [ ] Drop `"v"` from both descriptors; update the module docstring grammar and the
  "versions the grammar" sentence; update "method sha256" in docstrings.
- [ ] Run Task 1 tests: PASS. Run the property tests: PASS (relative comparisons).

### Task 3: Regenerate golden entries deliberately

- [ ] `uv run python tests/test_hashing/golden/generate_golden_hashes.py`
- [ ] `git diff --stat tests/test_hashing/golden/data` and a script check: every changed line's
  key starts with a `polars_` case and the count is 36 changed entries; no line added/removed
  outside them; diff vs `origin/main` of `data/` remains additions only.
- [ ] Update the `cases.py` comment that names `POLARS_HASH_GRAMMAR_VERSION`.
- [ ] `uv run pytest tests/test_hashing -q`: PASS.

### Task 4: Docs and instructions

- [ ] `docs/reference/hashing.md` §1c: state the convention once, in general terms (label
  `<type>_v<N>`, bytes under an existing label never change, a new definition gets a new label so
  old and new hashes are told apart by inspection); fix the Polars formula/token example
  (`polars:dataframe::polars_dataframe_v1:…`), drop `"v"` from the grammar block.
- [ ] `CLAUDE.md` and `.zed/rules`: one bullet in "Golden hash values" with the same rule.
- [ ] `DESIGN_ISSUES.md` PL1 entry: mention the labels.
- [ ] `uv run pytest tests/test_docs -q`.

### Task 5: Verify, commit, push, PR

- [ ] `git fetch`; merge origin/main if it moved.
- [ ] ruff check, ruff format --check, basedpyright (exit 0), full suite on 3.11/3.12/3.13.
- [ ] Commits: `feat(hashing): label Polars value hashes polars_dataframe_v1 / polars_series_v1 (ITL-578)`
  and `docs(hashing): versioned per-type hash label convention (ITL-578)`.
- [ ] Push; update PR #286 description with an "Intended hash changes" section; wait for CI and
  CodeRabbit; worker_done with head SHA.
