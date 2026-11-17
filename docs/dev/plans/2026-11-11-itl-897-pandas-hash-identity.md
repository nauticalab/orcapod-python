# ITL-897 pandas value hash `v1` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** a pandas DataFrame / Series value hashes under `pandas_dataframe_v1` / `pandas_series_v1`,
distinguishing exactly what `assert_frame_equal` / `assert_series_equal` distinguish by default
(column order, MultiIndex level order, struct field order, label types, `columns.names`, index
names, pandas dtypes, categorical categories and `ordered`), except `flags` and `-0.0`.

**Architecture:** `hash = sha256(canonical_json(descriptor) + b"\x00" + ordered_digest)`. The
ordered digest is starfix 0.5 (hash format 2) `ArrowDigester.hash_table(table,
ordered_columns=True, ordered_struct_fields=True)` of `pa.Table.from_pandas(value,
preserve_index=True)` with fields renamed positionally and extension columns reduced to storage.
The descriptor is an Orcapod-owned grammar built from pandas objects (spec section (i)). The
starfix call lives behind one function, `ordered_arrow_digest`, in a shared helper module that
the `pa.Table` value PR (second PR) and Polars reuse.

**Tech Stack:** pandas 2.2, pyarrow 23, starfix-python 0.5.0 (unreleased; API from
`nauticalab/starfix-python` branch `eywalker/itl-898-starfix-optional-order-sensitive-hashing`).

Branch: `eywalker/itl-897-pandas-dataframe-hash-ignores-column-order` (merged `origin/main`
`cf4f67c7`). Coordinator task `task_25c74f4198f0`, dispatch `ctx_4af6270358e5`.
Spec: `superpowers/specs/2026-11-11-itl-897-pandas-hash-identity.md`.

## Status and decisions

- [x] Step 1 investigation, spec, scripts (`superpowers/scripts/itl_897/`).
- [x] Developer, Nov 11: "ITL 897 pandas: yes to all." (1) labels `pandas_dataframe_v1` /
      `pandas_series_v1`, every pandas hash moves once; (2) full descriptor, settles ITL-887;
      (3) `pa.Table` values: separation + `arrow_table_v1` (Table and RecordBatch share it), in a
      SECOND PR on the shared helper; (4) mechanism: starfix 0.5.0 format 2 options.
- [x] Coordinator: build from `main` after #286 merges; no PR until starfix 0.5.0 is on PyPI and
      #286 has merged. The starfix bump and `arrow_v0.1 -> arrow_v0.2` are a separate change.
- Waits: starfix 0.5.0 release (verification, golden regeneration), #286 merge (merge `main`
  then; docs §1c lives there).

## Files

- Create `src/orcapod/hashing/labelled_hash.py`: `canonical_json`, `labelled_content_hash`,
  `ordered_arrow_digest` (the only starfix 0.5 call site).
- Create `src/orcapod/hashing/pandas_value_hash.py`: grammar (`pandas_label_descriptor`,
  `pandas_dtype_descriptor`), `pandas_frame_content_hash`, `pandas_series_content_hash`, labels.
- Modify `src/orcapod/hashing/semantic_hashing/builtin_handlers.py`: pandas handlers delegate;
  they no longer take an Arrow hasher; registration and module docstring.
- Modify `src/orcapod/contexts/data/v0.1.json`: changelog line.
- Tests: `tests/test_hashing/test_pandas_value_identity.py` (extend),
  `tests/test_hashing/test_pandas_handlers.py` (update), `tests/test_hashing/test_labelled_hash.py`
  (new), `tests/test_core/function_pod/test_pandas_dataframe_order.py` (as is).
- Move `tests/test_hashing/test_arrow_table_value_identity.py` to the second PR's branch.
- `CHANGELOG.md` `[0.3.0] — unreleased`, `DESIGN_ISSUES.md` entry (appended at the end).
- After release: golden cases (additions: Series, reordered frame, categorical) and regenerated
  pandas entries; `docs/reference/hashing.md` pandas subsection under #286's §1c.

## Verification before starfix 0.5.0 exists

`superpowers/scripts/itl_897/starfix05_shim.py` replaces `ordered_arrow_digest` in its own
process with an emulation (format-1 digest plus the ordered field and struct-field names), then
runs pytest. It proves everything except the starfix call itself. CI on the branch fails until
the release (the starfix 0.4 `hash_table` rejects the keywords with `TypeError`), which is why
no PR is opened before then.

### Task 1: shared helper `labelled_hash.py`

**Files:** Create `src/orcapod/hashing/labelled_hash.py`; Test `tests/test_hashing/test_labelled_hash.py`.

- [ ] Tests (red: module missing): `canonical_json` sorts keys, uses `","`/`":"`, keeps
      non-ASCII; `labelled_content_hash("x_v1", {"a": 1}, b"d")` has method `x_v1` and digest
      `sha256(b'{"a":1}' + b"\x00" + b"d")`; `ordered_arrow_digest` differs for reordered columns
      and reordered struct fields and is equal for rechunked tables (needs starfix 0.5: run under
      the shim now).
- [ ] Implement:

```python
def canonical_json(descriptor: Any) -> bytes:
    return json.dumps(descriptor, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()

def labelled_content_hash(method: str, descriptor: Any, values_digest: bytes) -> ContentHash:
    return ContentHash(method, hashlib.sha256(canonical_json(descriptor) + b"\x00" + values_digest).digest())

def ordered_arrow_digest(table: pa.Table) -> bytes:
    return ArrowDigester.hash_table(
        normalize_extension_columns(table), ordered_columns=True, ordered_struct_fields=True
    )
```

- [ ] Run under the shim, ruff, basedpyright; commit `feat(hashing): shared labelled-hash helper ...`.

### Task 2: pandas grammar and content hashes `pandas_value_hash.py`

**Files:** Create `src/orcapod/hashing/pandas_value_hash.py`; Test `tests/test_hashing/test_pandas_value_identity.py`.

- [ ] Extend tests first (red on main): every non-order row of spec (i) that pandas'
      `assert_*_equal` distinguishes gives different hashes; RangeIndex vs Index[int64],
      `attrs`, dict key order in object columns give equal hashes; labels are
      `pandas_dataframe_v1` / `pandas_series_v1`; Timestamp labels, period and interval columns
      hash; an unsupported label or dtype raises `ValueError` naming it; descriptor pins
      (exact JSON for a small frame and Series, so the grammar cannot drift silently).
- [ ] Implement the grammar from `prototype_v1.py` (`pandas_label`, `pandas_dtype_descriptor`,
      `frame_descriptor`, `series_descriptor`), with the values table built as
      `pa.Table.from_pandas(df, preserve_index=True)` renamed to `c0..cN-1, i0..iM-1` (labels live
      in the descriptor; positional names avoid starfix 0.5's duplicate-path rejection for labels
      `1` and `"1"`). A Series is hashed as `series.to_frame(name="c0")`. `ArrowInvalid` from
      `from_pandas` is re-raised as `ValueError` (as today). Functions raise what they know; no
      parameter is passed only for an error message.
- [ ] Run under the shim; commit `feat(hashing): pandas_dataframe_v1 / pandas_series_v1 ...`.

### Task 3: handlers delegate

**Files:** Modify `builtin_handlers.py` (pandas handlers, registration, docstring),
`contexts/data/v0.1.json`; Test `tests/test_hashing/test_pandas_handlers.py`.

- [ ] Update tests: handlers constructed without an Arrow hasher; `method` is the v1 label; the
      reserved-name test goes (the sentinel only matters for storage, which keeps rejecting it).
- [ ] `PandasDataFrameHandler.handle` / `PandasSeriesHandler.handle` type-check and call
      `pandas_frame_content_hash` / `pandas_series_content_hash`; drop `__init__(arrow_hasher)` and
      `_get_arrow_hasher`; `register_builtin_python_type_handlers` registers `PandasDataFrameHandler()`.
- [ ] Full pandas tests under the shim, including the pipeline order tests (in-memory and Delta,
      sync and async); commit.

### Task 4: records

- [ ] `CHANGELOG.md`: `## [0.3.0] — unreleased` above `[0.2.0]` with a "Changed (hash)" entry
      naming the labels, what moves (every pandas value hash; row keys of rows holding pandas
      values; `source_id` and record IDs of sources holding pandas values without an explicit
      `source_id`), that stored values are unchanged and results recompute once.
- [ ] `DESIGN_ISSUES.md`: one entry appended at the end (status `in progress`).
- [ ] Move the table test file to the second branch (created from this one); commit; push.

### Task 5: after starfix 0.5.0 is on PyPI and #286 has merged (waits)

- [ ] Merge `origin/main` (starfix bump and `arrow_v0.2` change included).
- [ ] Remove the shim from the verification path; full `uv run pytest tests/` on 3.11 to 3.13.
- [ ] Golden: add cases (Series named and unnamed, reordered frame, categorical, MultiIndex) and
      regenerate; check the diff changes only `dataframe`/pandas entries and adds the new ones.
- [ ] `docs/reference/hashing.md`: pandas subsection under §1c (grammar, labels, what is out).
- [ ] PR via `sensei:create-pr` with `Fixes ITL-897`, `Fixes ITL-887`; CodeRabbit rounds.

## Revision (coordinator, Nov 11): one starfix call site with semantic preprocessing (option A)

A `pa.Table` value must get the same semantic preprocessing as every other Arrow hash (Path
columns by file content, extension identity kept); only the order options differ. So:

- [ ] `ArrowHasherProtocol` gains `hash_table_ordered(table) -> ContentHash`. `StarfixArrowHasher`
      implements `hash_table` and `hash_table_ordered` through one private
      `_digest(table, *, ordered)`: preprocessing, then the single
      `ArrowDigester.hash_table(..., include_metadata=..., ordered_columns=ordered,
      ordered_struct_fields=ordered)` call.
- [ ] `labelled_hash.ordered_arrow_digest` is removed; `labelled_hash` keeps `canonical_json`
      and `labelled_content_hash`. Its digest tests move to `test_starfix_arrow_hasher.py`.
- [ ] pandas routes through the same function: `pandas_frame_content_hash(df, arrow_hasher)`;
      the pandas handlers take the Arrow hasher again (lazy default context, as on `main`).
      Preprocessing is a no-op for pandas tables (`from_pandas` never yields Orcapod extension
      types; Path / UUID objects in object columns fail `from_pandas` today and after).
- [ ] Format-2 checks for the pandas projection: positional names avoid duplicate / aliased
      paths; `from_pandas` fields are nullable; all-null object columns, fixed-size lists and
      maps fail to hash on `main` already (`NotImplementedError`), so no regression.
- [ ] Second PR (`eywalker/itl-897-arrow-table-value-order-identity`): `ArrowTableHandler` ->
      `arrow_table_v1` via `hash_table_ordered`; `Datagram` / `ArrowTableStream` identities call
      `hash_table` explicitly.

## Progress (Nov 11, end of day)

- [x] Tasks 1-4 done on this branch, revised to option A: `StarfixArrowHasher._digest` is the one
      starfix table call (`hash_table` / `hash_table_ordered`); pandas routes through
      `hash_table_ordered`; `labelled_hash` keeps `canonical_json` / `labelled_content_hash`.
- [x] Verified with `superpowers/scripts/itl_897/starfix05_shim.py` (stand-in for 0.5's ordered
      keywords, also in child processes via a temporary `.pth` hook): full suite passes except the
      3 approved pandas golden entries and the cross-process golden check reporting the same 3.
- [x] Second PR branch `eywalker/itl-897-arrow-table-value-order-identity`: `arrow_table_v1`
      (`ArrowTableHandler` -> `hash_table_ordered`), `StreamTableIdentity` for `Datagram` and
      producer-less `ArrowTableStream` identities (lazy; `==` is `pa.Table.equals`, so MergeJoin's
      stream comparison still hashes nothing). Full suite under the stand-in: only the pandas golden
      entries. No golden entry of its own moves.
- [ ] Waits: starfix 0.5.0 on PyPI and the bump on `main` (then: real run, regenerate golden for the
      pandas entries plus new cases, the 2 basedpyright `ordered_*` errors clear); #286 merged
      (merge `main`; docs §1c subsection; Polars may move onto `hash_table_ordered`).

- [x] Label rule (developer, Nov 11): a label is bumped whenever any value's hash can change,
      including an Arrow hasher change; documented, and `test_labels_are_pinned_to_the_arrow_hasher`
      pins the labels to `arrow_v0.1` (update to `arrow_v0.2` in the merge with the starfix bump:
      the labels are unreleased, so the pin may move without a label bump then).
- [ ] When starfix 0.5.0 is installable: delete `superpowers/scripts/itl_897/starfix05_shim.py`
      (it refuses to run then) and the plan/spec references to it, before opening either PR.

## Superseded (November 12, 2026)

starfix 0.5.0 is on `main`; the stand-in script `starfix05_shim.py` named above was deleted and
the library calls starfix's ordered options directly. The label pin test and the "bump a label
whenever any hash can change" rule were replaced by the ITL-938 label rule. The refresh onto
`main` is planned in `2026-11-12-itl-897-refresh-onto-v03-hash-step.md`.
