# ITL-928: function node and Delta store keep tag Arrow types — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A function node's `as_table()` returns the input's exact (large-normalized) Arrow types, and the Delta store gives back the Arrow types it was given, so joins after a function node work and a re-run on a Delta store writes no duplicate pipeline rows.

**Architecture:** Two independent defects (audit sent to the coordinator on 2026-11-12, `.untracked/ask1.md`).
Part A: `FunctionNodeBase.as_table` (`src/orcapod/core/nodes/function_node.py`) builds its
table from each row's Arrow tables instead of Python values plus a type inferred from the
Python output schema. Part B: `DeltaTableDatabase` records a column's original Arrow type in
top-level field metadata when Delta would store it differently, writes unsigned integers (and,
per Decision 1 (i), ns timestamps, date64, time, duration) as a same-width integer view, and
casts back on every read.

**Tech Stack:** pyarrow, deltalake (delta-rs), pytest, `uv run`.

## Status / decisions (keep updated for a successor)

- 2026-11-12: Part A APPROVED by the coordinator (own commits, do not push until told).
- Part B: with the developer (stored-format decision). Recommended: Decision 1 (i) integer
  view for ns / date64 / time / duration; Decision 2 (i) document that Delta stores written
  before the fix must be rebuilt. **Write no Delta code until the coordinator replies.** B's
  tests are prepared as strict xfails.
- 2026-11-12: Part A committed (43bb9375), plus FunctionPodStream.as_table logical-type
  tags (ecc879b7, approved by the coordinator as its own commit). Part B tests committed as
  strict xfails (`tests/test_databases/test_delta_arrow_type_roundtrip.py`,
  `test_store_read_back_keeps_tag_type`). Nothing pushed yet (coordinator: do not push).
- 2026-11-12: Part A PR opened: https://github.com/metamorphic-labs/orcapod-python/pull/335
  (body: ITL-928 part A, no magic word). Rows disagreeing on a column type raise
  InputValidationError (coordinator option 1). Part B will be a SECOND PR on top, after the
  developer's decision; still no Delta code. Next: CodeRabbit rounds + CI on #335.
- 2026-11-12: Part B APPROVED by the developer (decisions 1 (i), 2 (i)) and implemented on the
  same branch (`src/orcapod/databases/delta_arrow_types.py`); merged origin/main with #330;
  full suite 10352 passed. PR #335 now says Fixes ITL-928. Next: CodeRabbit rounds + CI.
- Audit scripts: `.untracked/measure.py` (tags, all read points, `[sync|async] [memory|delta|sqlite]`),
  `.untracked/measure2.py` (data columns, all-info columns, operator node, CachedSource),
  `.untracked/rerun_ids.py` (consequence 2), `.untracked/repro_flush_poison.py`.

## Files

- Modify: `src/orcapod/core/nodes/function_node.py` (`FunctionNodeBase.as_table`, ~l.451-503).
- Create: `tests/test_core/function_pod/test_function_node_arrow_types.py` (Part A, plus
  store read-back guards; Delta read-back cases are strict xfails until Part B).
- Part B (after approval): modify `src/orcapod/databases/delta_lake_databases.py` (write:
  record type + integer view; reads: cast back; append check compares converted types);
  create `tests/test_databases/test_delta_arrow_type_roundtrip.py`; loaded-job test in
  `tests/test_pipeline/loaded_job/`.
- `changelog.d/ITL-928.md`, `design_issues/ITL-928-FN1.md` (and a DT entry for Part B).

## The type matrix (tests)

int8, int16, int32, int64, uint8, uint16, uint32, uint64, float32, float64, bool,
timestamp s/ms/us/ns naive, timestamp us UTC, timestamp us America/New_York, date32, date64,
time32[s], time64[us], duration[s], duration[us], string, large_string, binary,
decimal128(10,2), uuid, Path. Expected type = `arrow_utils.normalize_to_large_types(source type)`.

Delta changes on main (Part B): every uint, timestamp s/ms/ns, timestamp with a non-UTC zone;
cannot write date64, time32/64, duration.

### Task 1: failing tests for Part A

**Files:** Create `tests/test_core/function_pod/test_function_node_arrow_types.py`

- [ ] Parametrize over the matrix x {in-memory, Delta} x {sync, async}: source with tag `k`
  (3 distinct values, non-null) and data `x: int64` -> `double.pod` in a `PipelineJob` -> run.
  Assert `job.nodes["double"].as_table().schema.field("k").type` is the expected type.
  Delta + (date64, time, duration): strict xfail (store cannot write them; Part B).
- [ ] `as_table(all_info=True)` of the node after the run has the same schema as the same
  pod's stream outside a job (`double.pod(src).as_table(all_info=True)`): `_source_y` and
  `_context_key` present (in-memory store, a few types).
- [ ] Join after a function node: `job.nodes["double"].join(other_source)` on a `uint32` and a
  `timestamp[us]` tag returns 3 rows, `k` typed as the source's.
- [ ] A node whose rows mix store-loaded and freshly computed entries (run with 2 rows, then
  a new job with 3 rows on the same in-memory store) returns 3 rows with the input types.
- [ ] Run: `uv run pytest tests/test_core/function_pod/test_function_node_arrow_types.py -q`;
  expected: failures for every type other than int64, float64, bool, ts[us,UTC], date32,
  string, large_string, binary; errors for time/duration/decimal/uuid/Path.

### Task 2: implement Part A

**Files:** `src/orcapod/core/nodes/function_node.py`

- [ ] Replace the non-empty branch of `FunctionNodeBase.as_table`:

```python
rows = list(self.iter_data())
if not rows:
    ...  # unchanged zero-row table from the declared output schema (reported, not fixed)
else:
    # Each row's own Arrow tables: the input's tag types and the data's stored types,
    # never a type inferred from Python values (ITL-928).
    tag_table = arrow_utils.concat_tables_by_name(
        [tag.as_table(all_info=True) for tag, _ in rows]
    )
    if constants.CONTEXT_KEY in tag_table.column_names:
        tag_table = tag_table.drop([constants.CONTEXT_KEY])
    data_table = arrow_utils.concat_tables_by_name(
        [data.as_table(all_info=True) for _, data in rows]
    )
    self._cached_output_table = arrow_utils.hstack_tables(tag_table, data_table)
```

  Keep nullability of `_source_*` fields consistent with `FunctionPodStream.as_table`
  (check with the all_info test; adjust field flags without a cast if needed).
- [ ] Run Task 1 tests: pass (except the strict xfails). Run
  `uv run pytest tests/test_core tests/test_pipeline -q -x`.
- [ ] Commit `fix(function_node): as_table keeps each row's Arrow types (ITL-928)`.

### Task 3: failing tests for Part B (strict xfail until approved)

**Files:** `tests/test_core/function_pod/test_function_node_arrow_types.py`,
`tests/test_databases/test_delta_arrow_type_roundtrip.py`

- [ ] Store read-back over the matrix x {in-memory, Delta} x {sync, async}: after the run,
  `get_all_records()` and a fresh job's `iter_data()` / `as_table()` on the same store keep
  the type. In-memory passes today (guard); Delta + changed types: strict xfail "ITL-928 part B".
- [ ] Re-run writes nothing: source(uint32 / ts[s] / ts[ns] / ts[us, NY] tag) -> pod -> pod,
  run twice on one store; a fresh job reads 3 rows from the second node and
  `get_all_records(all_info=True)` of each node still has 3 rows. Delta: strict xfail.
- [ ] `DeltaTableDatabase` round trip per type, a `uint32` value above int32 max, a
  `timestamp[ns]` with sub-microsecond digits; `OperatorJobNode` REPLAY and `CachedSource`
  on Delta keep types. Strict xfail.
- [ ] Saved-and-loaded job on a Delta store (separate process, as `loaded_job`) reads the
  input tag types. Strict xfail.
- [ ] Commit `test(delta): Arrow type round trip through the Delta store (ITL-928 part B, xfail)`.

### Task 4 (after approval only): implement Part B

- [ ] Write side (`add_records` / `_flush_by_key`): for each top-level column whose storage
  type the Delta writer converts (`_as_delta_writer_stores`), record the original type in
  field metadata (one key, IPC-serialized one-field schema), write unsigned and the types of
  Decision 1 as a same-width integer view; the append check compares converted types.
- [ ] Read side (every read method): cast/view each recorded column back, drop the key.
- [ ] Remove the xfail marks; full suite; commit `fix(delta): ...`.

### Task 5: finishing

- [ ] `design_issues/ITL-928-FN1.md` (+ DT entry), `changelog.d/ITL-928.md`.
- [ ] ruff format/check, basedpyright exit code, full `uv run pytest tests/`, golden tests unchanged.
- [ ] Merge origin/main, push once when the coordinator says, PR, readiness pipeline.
