# ITL-910 follow-up — ignore rows hashed under another Arrow label Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the `StoreHashLabelError` refusal of PR #309 with a read-time filter: v0.3 writes
and uses `arrow_v0.2` rows in the same tables next to `arrow_v0.1` rows, never returns rows of
another label from a whole-table read, and leaves the old rows untouched and listable.

**Architecture:** `src/orcapod/hashing/stored_hash_labels.py` classifies each stored key
(`b"arrow_vX:<digest>"` gives the label; a 70-hex starfix digest gives its 6-hex format) and
offers one filter, `keep_rows_hashed_under(table, column, current)`, used by every whole-table
reader, plus `stored_hash_labels(database, record_path)` to list the labels a table holds. No
path, schema or hash value changes.

**Tech Stack:** pyarrow compute, pytest, uv.

Branch `eywalker/itl-910-adopt-starfix-05-hash-format-2-in-orcapod`, draft PR #309.

---

## Developer decision (November 11, 2026, ITL-938 session; reverses D1 = D)

- Do not raise on rows hashed under `arrow_v0.1`; store and use `arrow_v0.2` next to them.
- Old and new entries must stay clearly distinguishable; a later migration (ITL-913/914) must
  be possible.
- Only schema changes go into a table path; a value-only change stays in the same table.
- Not decided: whether v0.3 reports skipped rows (keep one hook, add no report).
- No new hash label conventions.

## Investigation (refusal disabled, `writing_as_v02()` store, then a v0.3 run)

| Reader | Returned (2 inputs) | Label carried by a row |
|---|---|---|
| function node `as_table` / `get_all_records` / `iter_data` / phase 1 (`_fetch_joined_records`) | 4; before the v0.3 run: the 2 v0.2 rows | `__pipeline_base_entry_id` = `b"<label>:..."` |
| `ResultCache.get_all_records` (`get_all_cached_outputs` of `CachedFunctionPod`, `CachedDataFunction`) | 4 | `__input_data_hash` = `b"<label>:..."` |
| loaded job (file saved by v0.2), function node | 4 | as above |
| operator node LOG `get_all_records`, REPLAY `as_table` / `iter_data` / run, loaded job | 4 | none; `_record_hash` (record id) = 70-hex starfix digest, format `000001` / `000002` |
| `DerivedSource` of a function / operator node | 4 | via the node |
| `CachedSource.as_table` / `get_all_records` | 4 | none; `_record_hash` as for operators |
| side-effect log | never read in full; key lookup (`get_record_by_id`) | `record_id_hash` = `b"<label>:..."` |
| `StatusObserver` / `LoggingObserver` tables | run history, no Arrow-keyed rows | not filtered |

The node content hash is the same under both labels when the source has an explicit
`source_id`, so `_filter_by_content_hash` does not separate generations.

Coordinator answers (ask, 2026-11-11): filter operator / cached-source tables by the starfix
format of `_record_hash` (exact for `arrow_v0.1` vs `arrow_v0.2`; a future label on the same
starfix format would need a stored label column, passed to the design session); keep rows
whose key carries no Arrow label; side effects: implement re-delivery (A) for now, easy to
switch, pinned by a test, developer being asked.

## Tasks

### Task 1: failing tests
- [ ] Rewrite `tests/test_pipeline/test_stored_hash_labels.py`: on a store holding both labels,
  every reader in the table above returns one row per input; before the first v0.3 run it
  returns none; a v0.3 run recomputes, a second run reuses; v0.2 rows are unchanged (snapshot
  subset, Delta files byte-identical); `stored_hash_labels` lists both labels per table; side
  effects on `arrow_v0.1` completions are delivered again once (pinned).
- [ ] Rewrite `tests/test_hashing/test_stored_hash_labels_helpers.py` for
  `hash_labels_of`, `keep_rows_hashed_under`, `stored_hash_labels`.
- [ ] Rewrite `tests/test_arrays/test_array_v02_store.py` (real v0.2.0 fixture): old rows
  ignored, every input recomputed, files kept, 3 rows per node.
- [ ] Run; confirm failures (`StoreHashLabelError` raised / import errors). WIP commit.

### Task 2: helpers (`hashing/stored_hash_labels.py`)
- [ ] `hash_labels_of(values) -> pa.Array` (label, 6-hex format, or null per value),
  `keep_rows_hashed_under(table, column, current) -> pa.Table` (the one drop site; comment marks
  where a report would go), `current_hash_format(hasher)` (kept),
  `stored_hash_labels(database, record_path) -> dict[str | None, int]`.
  Remove `foreign_arrow_label`, `foreign_hash_format`.

### Task 3: readers
- [ ] `FunctionJobNode._fetch_joined_records`: filter `taginfo` on
  `__pipeline_base_entry_id`; remove `check_stored_hash_labels` and its calls.
- [ ] `ResultCache.get_all_records(..., current_label=...)` filters on `__input_data_hash`;
  callers pass their data context's label; remove the check from `lookup` / `store`.
- [ ] `OperatorJobNode`: one `_read_stored_records()` (reads with the record id, filters by
  content hash and starfix format) for `_load_cached_stream_from_db`, `_replay_from_cache`,
  `get_all_records`; remove the check.
- [ ] `CachedSource`: `_read_cached_records()` for `_build_merged_stream` and
  `get_all_records`; remove the check.
- [ ] `SideEffectJobNode`: remove the check. `PipelineJob.run`: remove the pre-flight.
  `_ResultDatabaseReader`, `CachedFunctionPod`: remove `check_stored_hash_labels`.
- [ ] `orcapod.errors.StoreHashLabelError`: remove.

### Task 4: docs
- [ ] CHANGELOG (`[0.3.0] — unreleased`), `docs/reference/hashing.md`,
  `docs/concepts/function-pods.md`, `docs/concepts/arrays.md`, DESIGN_ISSUES HS2.

### Task 5: verification and PR
- [ ] Golden tests unchanged; full suite on 3.11, 3.12, 3.13 one at a time; ruff; basedpyright.
- [ ] origin/main already merged (0 commits behind at start); re-check before pushing.
- [ ] Push; PR stays a draft; update title and body; `worker_done`.

## Status log (for a successor)

- 2026-11-11: investigation and ask done; tasks 1-4 done and pushed (commits `34960c8c`,
  `66517c5a`, `92ffb2ed`). Red state before the filters: 39 failures (both generations
  returned). Golden files: no diff against `5363bfb8`. origin/main: 0 commits ahead of the
  branch, no merge needed.
- Side effects: re-delivery (option A) implemented as the interim behaviour and pinned by
  `TestSideEffects`; the developer's answer is pending with the coordinator. Switching to a
  failure for side-effect logs only = one check at the start of `SideEffectJobNode.execute` /
  `async_execute` (see the removed `check_stored_hash_labels` in commit `9bef94ee`).
- Report of skipped rows: not added. `keep_rows_hashed_under` is the one drop site; the count
  of skipped rows is one `pc.sum(other)` there (the table path would come from the caller).
- Found, not fixed, not filed: a `DeltaTableDatabase` object keeps the table version it read
  first, so a job object that read a table before a run does not see the rows the run added
  (v0.3-only store with 2 rows, run with 7 inputs, same-job `get_all_records()` gives 2; a
  newly built job gives 7). `tests/test_arrays/test_array_v02_store.py` reads through a newly
  built job for that reason.
- Developer decisions relayed by the coordinator (2026-11-11), implemented in `bcd905b9`,
  superseding the two notes above:
  - Side effects = B: a side-effect node whose invocation log holds deliveries logged under
    another Arrow label raises `orcapod.errors.SideEffectLogHashLabelError` before delivering
    anything (`SideEffectJobNode._refuse_log_of_another_label`). The developer wants a separate
    later discussion of side-effect logs.
  - Report = yes, totals only: `report_skipped_rows(record_path, skipped)` logs one WARNING
    per reader object and table (not per process: stored paths are relative to the store).
  - Old results looking absent before a v0.3 run is accepted.
  - A global hash-version identifier per row is being designed, not decided: not implemented.
- Verification: full suite 7755 passed / 96 skipped / 18 xfailed / 0 failed on each of 3.11,
  3.12, 3.13; ruff clean; basedpyright exit 0; golden files unchanged. PR #309 body and title
  updated; PR stays a draft.
