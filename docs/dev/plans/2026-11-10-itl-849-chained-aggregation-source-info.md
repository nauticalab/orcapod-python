# ITL-849 Chained aggregation (nested source info) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `GroupBy` and `Batch` applied to a stream whose data and source-info columns are
already list-valued (after `MergeJoin`, `Batch` or `GroupBy`) produce a well-typed
list-of-lists result, on the sync and async paths, with no hash change for any pipeline that
worked before.

**Architecture:** The aggregation itself is already correct: `build_aggregated_table` types
`_source_x` as `large_list<large_list<large_string>>`. The crash is downstream of it.
`ArrowTableStream.iter_data` converts each row's source info to Python values and passes them
to `Data`. `Data` then rebuilds them in `arrow_utils.prepare_prefixed_columns`, whose
value-supplied branch hard-types every list token as `pa.list_(pa.large_string())`, so a
second level of nesting raises `ArrowTypeError: Expected bytes, got a 'list' object`. The fix
has two parts:

1. `iter_data` hands `Data` each row's source info as Arrow columns. These take the
   pass-through branch, so the types are kept and nothing is inferred.
2. The value branch types Python-supplied tokens with the documented source-info rule:
   `str`/`None` become `large_string` and lists become `large_list` of their element,
   recursively. Today that rule lives in `tag_data._source_info_arrow_type`; it moves to
   `arrow_utils.source_info_arrow_type` as the single definition. The async barrier path
   (`_materialize_to_stream`, which passes `rows[0].source_info()` as Python values) reaches
   this branch.

**Tech Stack:** Python, PyArrow, pytest (`uv run`).

**Approvals:** The coordinator approved this design on 2026-11-10. For the async per-row
source-info assertion it chose option A, a strict xfail citing ITL-845: `_materialize_to_stream`
broadcasts row 0's source info, which is ITL-845's bug in `static_output_pod.py`, a file this
PR must not edit. Whichever of the two PRs merges second removes the xfail.

---

## Root cause evidence (Phase 1, on `main` at ab544982)

- Reproduction: `GroupBy(["id"])(MergeJoin()(A, C)).as_table(all_info=True)` fails with
  `arrow_table_stream.py:223 as_table` → `:299 iter_data` → `tag_data.py:338 Data.__init__`
  → `arrow_utils.py:873 prepare_prefixed_columns` → `ArrowTypeError`.
- All five allowed chains fail at that frame, sync and async: MergeJoin→GroupBy,
  MergeJoin→Batch, Batch→Batch, GroupBy→Batch and GroupBy→GroupBy.
  `GroupBy(["g"])(Batch(...)(A))` is rejected with `InputValidationError`, as intended.
- Hypothesis test: patching only the value branch's list type in memory made every chain
  work. Types were exact (`large_list<large_list<int8>>`; `float32` kept;
  `Path` → `list[list[orcapod.path]]`), the prediction equalled `static_process` output, the
  zero-row chains worked, and hashes were computable.
- Hash baseline for 14 pipelines that already worked:
  `scratchpad/hash_snapshot.py` → `hash_before.json`. It is deterministic across two runs.

## File map

- Modify `src/orcapod/utils/arrow_utils.py`: add `source_info_arrow_type`, and make the
  `prepare_prefixed_columns` value branch use it.
- Modify `src/orcapod/core/datagrams/tag_data.py`: remove `_source_info_arrow_type` and use
  `arrow_utils.source_info_arrow_type`. `_source_info_python_type` stays and its docstring is
  updated.
- Modify `src/orcapod/core/streams/arrow_table_stream.py` (`iter_data`): batch the data
  columns together with the source-info columns and pass Arrow slices to `Data`.
- Create `tests/test_core/operators/test_chained_aggregation.py`: chain regressions (sync,
  async, zero-row, validation).
- Modify `tests/test_utils/test_arrow_utils.py`: value-branch unit test.
- Modify `tests/test_core/streams/test_iter_data_chunk_layouts.py`: nested source info
  across chunk layouts.
- Docs: `DESIGN_ISSUES.md` (one entry at the end), `CHANGELOG.md` (one entry in
  `[Unreleased]`, which is the v0.2.0 section), and the nested-list rule in `CLAUDE.md` and
  `.zed/rules`.

---

### Task 1: Failing chain regressions

**Files:** Create `tests/test_core/operators/test_chained_aggregation.py`.

- [ ] **Step 1: Write the tests.** Sources:
  - `A`: tags `id: int64`, `g: int64`; data `x: int8`, `y: float32`, `name: string`,
    `lname: large_string`, `lst: list<int8>`, `p: Path` (logical); all non-nullable.
  - `C`: tags `id`, `g`; data `x: int8`, `z: large_string`, `p: Path`.
  - `C0`: `C` with disjoint ids, used for zero-row inputs.

  Chains are `(name, upstream factory, operator factory)`:
  - `mergejoin_group_by`: `MergeJoin()(A, C)` → `GroupBy(["g"])`
  - `mergejoin_batch`: `MergeJoin()(A, C)` → `Batch(2)`
  - `batch_batch`: `Batch(2)(A)` → `Batch(2)`
  - `group_by_batch`: `GroupBy(["g"])(A)` → `Batch(2)`
  - `group_by_group_by`: `GroupBy(["g"])(A)` → `GroupBy(["g"])`

  Assertions, each case parametrized over the chains:
  - The issue's reproduction:
    `GroupBy(["id"])(MergeJoin()(A, C)).as_table(all_info=True)` succeeds.
  - Every data column has an exact expected Arrow type, written as a literal per chain
    (for example, MergeJoin→GroupBy: `x` is `large_list<large_list<int8>>`, `y` is
    `large_list<float32>`, `name` is `large_list<large_string>`, `lst` is
    `large_list<large_list<int8>>`, `p` is the converter's `list[list[Path]]` type). Every
    member column is non-nullable.
  - Every `_source_<col>` column is `large_list` of its incoming source type: one level
    deeper than upstream, and non-nullable.
  - `op.output_schema(up, columns=c) == op.unary_static_process(up).output_schema(columns=c)`
    for `c` in `({}, {"system_tags": True})`.
  - The values are exact for `x`, `y` and `lst`, and `p` round-trips to `Path` objects.
  - Per row, `data.source_info()[col]` equals that row's `_source_<col>` value in
    `as_table(columns={"source": True})`. It is a list for every row, and each member's tokens
    name that member's upstream rows.
  - `data.content_hash()`, `tag.content_hash()`, the record-ID preimage hash, the stream
    `content_hash()` and the stream `pipeline_hash()` all compute.
  - Async: feed the upstream rows into a `Channel`, run `op.async_execute`, then compare each
    row with the sync result. The data and tag content hashes must be equal, the
    `as_table(columns={"source": True})` Arrow types equal, and the values equal.
  - Async per-row source info equals sync. This assertion is marked
    `xfail(strict=True, reason="ITL-845: _materialize_to_stream broadcasts rows[0] source info")`.
  - Async three-stage chain: upstream `Batch(2)(GroupBy(["g"])(A))`, then `Batch(1)` async.
    This reaches the value branch with nested Python tokens; the types must match sync.
  - Zero-row: `MergeJoin()(A, C0)` → GroupBy, → Batch, and → Batch → Batch. There are zero
    rows, the types equal those of the non-empty chain, and the prediction matches.
  - `GroupBy(["g"])(Batch(2)(A))` raises `InputValidationError`.
- [ ] **Step 2: Run them and confirm they fail with `ArrowTypeError`.**
  `uv run pytest tests/test_core/operators/test_chained_aggregation.py -q`

### Task 2: Single source-info Arrow type rule, and value branch fix

**Files:** `src/orcapod/utils/arrow_utils.py`, `src/orcapod/core/datagrams/tag_data.py`,
`tests/test_utils/test_arrow_utils.py`.

- [ ] **Step 1: Write the failing unit test.**
  `prepare_prefixed_columns(table, {"_source_": {"v": [["a"], ["b", "c"]]}})` gives
  `_source_v: large_list<large_list<large_string>>` with that value in every row.
  `ArrowTableStream(..., source_info={"v": [["a"], ["b"]]})` iterates. A flat list still
  gives `large_list<large_string>`, a string gives `large_string`, and `None` gives a nullable
  `large_string`.
- [ ] **Step 2: Run it and confirm it fails.**
- [ ] **Step 3: Implement.** In `arrow_utils`:

  ```python
  def source_info_arrow_type(value: Any) -> pa.DataType:
      """Return the Arrow type of one source-info provenance token. ..."""
      if isinstance(value, list):
          if not value:
              return pa.large_list(pa.large_string())
          return pa.large_list(source_info_arrow_type(value[0]))
      return pa.large_string()
  ```

  Keep the TODO(NPIPE-204) comment on it. In the value branch, replace the
  `Collection`/`pa.list_` special case with
  `column_values = pa.array([value] * num_rows, type=source_info_arrow_type(value))`.
  In `tag_data.py`, delete `_source_info_arrow_type` and call
  `arrow_utils.source_info_arrow_type`.
- [ ] **Step 4: Run the unit test and confirm it passes.**

### Task 3: `iter_data` passes source info as Arrow columns

**Files:** `src/orcapod/core/streams/arrow_table_stream.py`,
`tests/test_core/streams/test_iter_data_chunk_layouts.py`.

- [ ] **Step 1: Write the failing test.** A stream whose `_source_x` column is
  `large_list<large_list<large_string>>`, chunked differently from the tag and data columns,
  yields the right nested source info for every row.
- [ ] **Step 2: Implement.** Build the batching table once, with the source-info columns
  appended to `self._table`, so `to_batches()` still cuts at the union of all chunk
  boundaries and every row's source info is indexed by its absolute row:

  ```python
  source_columns = list(self._source_info_table.column_names)
  batching_table = self._table
  for name in source_columns:
      batching_table = batching_table.append_column(
          self._source_info_table.schema.field(name), self._source_info_table.column(name)
      )
  for batch in batching_table.to_batches():
      tag_batch = batch.select(tag_columns)
      data_batch = batch.select(data_columns + source_columns)
      ...
      data = Data(data_batch.slice(i, 1), data_context=self.data_context)
  ```

  This removes `source_info_rows` and `row_offset`.
- [ ] **Step 3: Run the chunk-layout and chain tests and confirm they pass,** apart from the
  strict xfail.

### Task 4: Hash evidence and full verification

- [ ] Run `hash_snapshot.py` into `hash_after.json` and `diff` it against `hash_before.json`.
  The diff must be empty.
- [ ] `uv run pytest tests/test_hashing tests/test_pipeline/test_system_tag_run_stability.py tests/test_core/streams tests/test_core/operators -q`
- [ ] `uv run pytest tests/` on Python 3.11, 3.12 and 3.13 (`uv run --python 3.1x`).
- [ ] Run ruff check, ruff format --check and basedpyright, and record each exit code.
- [ ] Commit: `fix(streams): chained aggregation over list-valued source info (ITL-849)`.

### Task 5: Docs

- [ ] `DESIGN_ISSUES.md`: one self-contained entry at the end, status `resolved`, with a
  **Fix:** note.
- [ ] `CHANGELOG.md`: one `####` entry in `[Unreleased]`, before `### Breaking Changes`.
- [ ] `CLAUDE.md` and `.zed/rules`: an aggregated source-info column is `large_list` of its
  incoming source-info type (`large_list<large_string>` for scalar tokens, one level deeper
  for each further aggregation). Data source-info values may be nested lists.
  `prepare_prefixed_columns` types Python tokens only through `source_info_arrow_type`.
- [ ] Commit: `docs: record ITL-849 nested source-info rule`.

### Task 6: PR

- [ ] Fetch main, merge or rebase if it moved, and rerun the suite.
- [ ] Push, then open the PR with sensei:create-pr. The body is "Fixes ITL-849" and ends with
  the Claude Code line.
- [ ] Run `orca linear attach` and `orca worktree set --pr`.
- [ ] Run the CodeRabbit rounds (at most 3) and get CI green. Then send worker_done.
