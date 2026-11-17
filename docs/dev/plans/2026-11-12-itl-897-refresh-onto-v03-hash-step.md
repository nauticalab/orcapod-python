# ITL-897 Refresh onto main's v0.3 Hash Step — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this
> plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Land the pandas value hash (`pandas_dataframe_v1` / `pandas_series_v1`, column order
and the other `assert_frame_equal` properties as identity) on `origin/main` (c8e9eb21) before
the v0.3 tag, as one PR against `main`.

**Architecture:** One merge commit of `origin/main` (no rebase), then conformance commits. The
hash is `sha256(canonical_json(descriptor) + b"\x00" + D)`, where `D` is the data context Arrow
hasher's `hash_table_ordered` digest (starfix 0.5.0 `ordered_columns=True`,
`ordered_struct_fields=True`, called directly; the stand-in is deleted) of
`pa.Table.from_pandas(value, preserve_index=True)` with positional field names.

**Tech Stack:** pandas, pyarrow, starfix 0.5.0, pytest, ruff, basedpyright.

Coordinator task `task_ec9063abab16`, dispatch `ctx_4ceb763ec3e1`. Earlier plan and spec:
`superpowers/plans/2026-11-11-itl-897-pandas-hash-identity.md`,
`superpowers/specs/2026-11-11-itl-897-pandas-hash-identity.md`.

**Coordinator decisions (ask 1, November 12, 2026):**

- Merge resolutions 1-5 approved (Task 1).
- Option A stands: `ArrowHasherProtocol.hash_table_ordered`, one starfix table call site. Name
  it under a "Public interface" heading in the PR body and the changelog fragment.
- Do not touch `polars_type.py`, `arrow_ipc.py`, `pandas_type.py` (PR #286 overlap).
- D1 = A: full descriptor (developer, Nov 11: "ITL 897 pandas: yes to all"; column order
  confirmed again Nov 12). PR body: `Fixes ITL-887`, and a table of properties 1-11.
- D2 = A: the logical type identifiers stay `{"version":1}`; version 1 is what v0.3.0
  computes. Cost stated in the PR body: unreleased `main` builds miss the cache once for rows
  holding pandas values.
- Golden: exactly 3 entries move (`semantic.json` `value.dataframe.basic`; `datagram.json`
  `dataframe.row[{"k":"r1"}].data_hash`, `dataframe.row[{"k":"r2"}].data_hash`); 5 additions
  (`value.series.named`, `value.series.unnamed`, `value.dataframe.reordered`,
  `value.dataframe.categorical`, `value.dataframe.multiindex`). Anything else moving: stop
  and ask.
- No magic word for ITL-897: write "ITL-897 (pandas values; pa.Table values not included)".
  Nothing is done on the arrow-table branch. One push.

---

### Task 1: Merge origin/main

- [ ] `git merge origin/main --no-commit --no-ff`. 5 conflicts.
- [ ] `builtin_handlers.py`: main's docstring handler list, the two pandas lines reading
      `pandas_dataframe_v1` / `pandas_series_v1 (descriptor + ordered Arrow digest)`.
- [ ] `contexts/data/v0.1.json`: accept deletion. `DESIGN_ISSUES.md`: accept deletion; entry
      PV1 to `design_issues/ITL-897-PV1.md`. `CHANGELOG.md`: main's; branch text to
      `changelog.d/ITL-897.md` (rewritten to what is true on main in Task 4).
- [ ] `tests/test_hashing/test_pandas_handlers.py`: branch side.
- [ ] Commit the merge.

### Task 2: Conformance to main's rules

- [ ] Delete `superpowers/scripts/itl_897/starfix05_shim.py` and every reference (old plan,
      spec).
- [ ] Rename `PANDAS_DATAFRAME_HASH_METHOD` / `PANDAS_SERIES_HASH_METHOD` to
      `PANDAS_DATAFRAME_HASH_LABEL` / `PANDAS_SERIES_HASH_LABEL`; no label literal elsewhere in
      `src/`.
- [ ] Delete `TestLabels::test_labels_are_pinned_to_the_arrow_hasher`; reword the docstrings
      of `pandas_value_hash.py` and `labelled_hash.py` to the ITL-938 label rule (a label names
      its algorithm; it changes only when an existing input would get a different digest);
      `arrow_v0.1` mentions become `arrow_v1`.
- [ ] `tests/test_hashing/test_hash_label_rule.py`: add the two pandas labels to the value
      label test and to the format test's label set (and to the renaming fixture if labels
      are enumerated there).
- [ ] `StarfixArrowHasher` class docstring: `hash_table` keeps starfix's order options off;
      `hash_table_ordered` turns both on.

### Task 3: Tests through the public interface (already red on main)

Shown red on pure `origin/main` in the dry run (34 failures) and green on the merge:

- `tests/test_hashing/test_pandas_value_identity.py`: reordered columns hash differently, and
  every other property of the decision table.
- `tests/test_core/function_pod/test_pandas_dataframe_order.py`: a cached pod returning
  `df.columns[0]` recomputes for a reordered frame; storage keeps column order; in-memory and
  Delta, sync and async.
- [ ] Add the missing half: an EQUAL frame does not recompute (call counter on the pod; second
      run with an equal frame leaves the counter unchanged), same four store/orchestrator
      combinations.

### Task 4: Golden, docs, records

- [ ] `tests/test_hashing/golden/cases.py`: add the 5 value cases. Regenerate with
      `uv run python tests/test_hashing/golden/generate_golden_hashes.py`. The diff must
      change exactly the 3 approved lines and add the 5 new ones.
- [ ] `docs/reference/hashing.md`: pandas values are no longer hashed by
      `StarfixArrowHasher.hash_table`; label table rows; a pandas subsection (definition,
      grammar, identity table, what is not identity).
- [ ] `changelog.d/ITL-897.md` (`### Breaking changes and migration`, with a "Public
      interface" sub-heading), `design_issues/ITL-897-PV1.md` (status `resolved`, **Fix:**).
- [ ] `uv run python scripts/assemble_changelog.py --check`,
      `uv run python scripts/design_issues.py --check`.

### Task 5: Checks, PR, readiness

- [ ] Full suite on 3.12, then 3.11 and 3.13; `ruff check`, `ruff format --check`,
      basedpyright exit 0.
- [ ] Merge `origin/main` again if it moved; ONE push; PR against `main` via
      `sensei:create-pr`; `orca linear attach`; `orca worktree set --worktree current --pr <n>`.
- [ ] Readiness: CI green, CodeRabbit on the new head (review bodies and threads), every
      thread answered, `worker_done`.

## Progress notes

(appended while working)

- Nov 12: Tasks 1-4 done and committed locally (merge 18303b44, conformance, equal-frame
  cache test, golden: exactly the 3 approved entries moved and 5 added, docs section 10,
  changelog fragment, design issue PV1). ruff, format, basedpyright exit 0. NOT pushed.
- Nov 12 20:11Z: coordinator HOLD (developer reconsidering how numpy, torch, pandas and
  polars values are stored and hashed): no push, no golden regeneration, no GitHub posts
  until the coordinator writes again. Remaining when released: Task 5 (full suite on 3.12,
  3.11, 3.13; one push; PR; readiness). PR body draft is in this session's temp dir only;
  its content follows the coordinator decisions at the top of this plan.
- Nov 12 (later): first hold lifted (storage and hashing are separate), then the PUSH held
  again: what a hash ignores becomes configurable per type and is recorded in the extension
  metadata (shared mechanism: ITL-981; not built here). Added `PANDAS_IDENTITY_PROPERTIES`
  (one list in code, each entry checked by a test). Sent the coordinator the pandas spec
  (properties / hash / storage). Findings awaiting a decision, NOT implemented: `attrs`
  counting in the hash; `string[pyarrow]` and non-string `object` columns read back as
  another dtype, so hash(read(write(v))) != hash(v) today (to be fixed by describing them by
  what storage restores); `flags`, RangeIndex kind, index `freq`, unused MultiIndex levels
  are not restored by storage and are never hashed.
- Second merge of `origin/main` (25650c42; #299, #301, #303), no conflicts: 4bbe5ef0.
  Golden diff against `main`: the 3 approved entries changed, 5 added, nothing else.
  Full suite 3.12 / 3.11 / 3.13: 10111 passed, 109 skipped, 23 xfailed each; ruff, format,
  basedpyright exit 0; changelog and design-issue checks OK. Still NOT pushed; waiting for
  the coordinator's sign-off, then: PR body (draft content per the decisions above plus the
  storage/hash sentence), one push, PR, readiness.

## Phase 2 (developer sign-off relayed by the coordinator, November 12, 2026)

Decisions that apply to pandas:

- Storage and hashing are separate; the hash is strict by default. A property storage cannot
  restore never counts in the hash and has no option name.
- Names: `orcapod.pandas.dataframe` / `orcapod.pandas.series` (rename is in this PR), also as
  the descriptor `kind` (a type has one name everywhere). Drop `also_reads` and
  `_LEGACY_METADATA` from the two pandas types. Metadata stays `{"version":1}` until ITL-981
  adds the ignore list (`{"ignore":[...],"version":1}`, sorted keys).
- `attrs` counts by default. Options offered (via ITL-981's mechanism, not built here):
  `column_order`, `struct_field_order`, `series_name`, `attrs`.
- Never counted (storage does not restore them): `flags`, RangeIndex kind, index `freq`,
  unused MultiIndex levels, string storage (`string[python]` vs `string[pyarrow]`), the
  `object` dtype of a column whose values are not strings. The last two are FIXES: today the
  value read back hashes differently. Tests: `hash(read(write(v))) == hash(v)` through the
  public interface.
- Golden moves approved for the rename, on top of the 3: `arrow.json`
  `extension_no_values.{all_null,zero_rows}.{dataframe,series}`; `schema.json`
  `single_field.dataframe.{arrow_type,schema_hash}`; `semantic.json` `type.dataframe`;
  `datagram.json` `dataframe.{content_hash,pipeline_hash,system_tag_columns}` and the 4
  renamed `_tag_record_id::` / `_tag_source_id::` keys (values unchanged). The `value.*`
  entries move once more for `kind` and attrs. Anything outside pandas values / pandas-typed
  columns moving: stop and ask.
- Add a test: a v0.2-layout table with a `pandas.dataframe` column is counted, not read, not
  modified. The silent miss for stores of unreleased main builds is accepted (PR body).
- PR body: `Fixes ITL-887`; "ITL-897 (pandas values; pa.Table values not included)".
- NO PUSH until ITL-981's PR has merged into `main`. Before that: when the coordinator sends
  the sha of `eywalker/itl-981-library-value-types-native-storage-strict`, merge it locally
  and wire the pandas types to its `IgnoreListLogicalType` / `IgnoreListHandler` /
  `hash_ignore` config (design note: worktree `itl-981-library-value-types`,
  `superpowers/specs/`, section vi; read-only). After ITL-981 merges: merge `main`,
  regenerate the approved pandas entries only, full suites, ONE push, PR, readiness.

Tasks: (1) rename + `kind` + drop legacy reads, fix the 17 name-pin tests, docs; (2) attrs in
the descriptor; (3) string storage and object-column fixes with round-trip tests; (4)
`PANDAS_IDENTITY_PROPERTIES` gains the option flag and the never-counted entries; (5) v0.2
layout test; (6) golden (local), docs section 10, changelog, design issue, PR body.
- Phase 2 done locally (not pushed): rename, descriptor kind, legacy read dropped, attrs
  (as the JSON object storage keeps; `{}` when not JSON), string storage and object-column
  fixes, `PANDAS_IDENTITY_PROPERTIES` with options, v0.2-layout test, docs, changelog,
  design issue. ITL-981 mechanism commit 64c63bfe merged locally and wired:
  `LogicalPandasDataFrame` / `LogicalPandasSeries` are `IgnoreListLogicalType`s, the two
  handlers are `IgnoreListHandler`s, `hash_ignore` in `contexts/logical_types.json`,
  `hash_table_ordered(..., struct_field_order=True)`. Tests:
  `tests/test_hashing/test_pandas_ignore_list.py`. Full suites: only the 22 tests of
  `tests/test_arrays/test_array_strict_hash.py` fail (ITL-981's own array tests, whose
  implementation is not in 64c63bfe). Golden vs `main`: 13 values changed, 4 keys renamed,
  5 added, all pandas (list in the Phase 2 decisions).
- Remaining: wait for ITL-981's PR to merge into `main`; merge `main`; regenerate golden
  (pandas entries only); full suites; PR body (scratchpad draft is outdated: rewrite from
  the changelog fragment and this plan); ONE push; PR; readiness.
- Dry run against ITL-981's tip (PR #328, open): 6 one-hunk conflicts, all unions of both
  sides: `docs/concepts/logical-type-identifiers.md` (name table: their numpy row, our
  pandas rows and paragraph), `docs/reference/hashing.md` (label table: their `ndarray_v1`
  wording plus our two pandas rows), `contexts/logical_types.json` (`hash_ignore`: all
  three names), `tests/test_contexts/test_data_context_v1.py` (name list:
  `orcapod.numpy.ndarray` then the two pandas names), `tests/test_hashing/test_starfix_arrow_hasher.py`
  (keep both test classes), `tests/test_logical_types/test_logical_type_identifiers.py`
  (their `ALL_BUILTINS` / `BUILTIN_METADATA`; add the two pandas names to
  `BUILTIN_METADATA` or keep our `BUILTINS_WITH_IGNORE_LIST` test).

## Final merge of main (November 12, 2026)

- Merged `origin/main` 859106f7 (contains ITL-981 as squash commit 8608ed43, #328) in merge
  commit 6308943b. The branch history still holds 64c63bfe (the ITL-981 mechanism commit
  merged locally earlier); its files are identical to main's after this merge.
- Hand-resolved: `docs/concepts/logical-type-identifiers.md`, `docs/reference/hashing.md`,
  `src/orcapod/contexts/logical_types.json`, `tests/test_contexts/test_data_context_v1.py`,
  `tests/test_hashing/test_starfix_arrow_hasher.py`,
  `tests/test_logical_types/test_logical_type_identifiers.py`. ITL-981's own files
  (`ignore_list.py`, `registry.py`, its plan, spec and tests) taken from main.
- `logical_types.json` has one `hash_ignore` object (numpy, pandas dataframe, pandas series)
  and six references. Regenerating golden after the merge changes nothing; against main:
  13 values changed, 4 keys renamed, 5 added, all pandas.
