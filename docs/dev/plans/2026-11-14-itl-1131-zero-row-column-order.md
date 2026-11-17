# ITL-1131: a zero-row table has its populated table's column order — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every operator's zero-row table (plain stream; job node before a run and after a run
whose input had no rows, sync and async) has exactly the populated table's schema: column
names, order, Arrow types, nullability.

**Architecture:** The row order of a node's rows (`_rows_to_table`: user tags, system tags,
data, source info) gets one owner, `StaticOutputOperatorPod._in_row_order`. The places that
restate it (`_zero_row_input`, `OperatorJobNode._typed_zero_row_table`) call it, and so do the
two places that keep a run's zero-row result (sync `execute()`, async `async_execute()` empty
branch), which today keep the operator's own order. `Index` and `Pick` build their empty result
with the input's system tags and source info, in row order, with the source-info flags a
row-built table has. The populated order is never changed (stored tables depend on it).

**Tech Stack:** Python, PyArrow, pytest, `uv run`.

---

## Findings (sweep on main 52d67858, before the fix)

Scripts: the worker's scratchpad `sweep.py`, `sweep_direct.py`, `latent.py` (copied into the
coordinator status message `msg_86fd0618208e`).

| Path | Differs | Where the order is decided |
|---|---|---|
| plain stream `op(stream)`, zero input, `as_table` / `head()` | `Index`, `Pick`: system tags missing; `Pick`: `_source_*` nullable | `Index`/`Pick.unary_static_process` empty branch (`stream.as_table(columns={"source": True})` / `stream.as_table()`) |
| job node before run | `Index`, `Pick` as above | `OperatorJobNode._typed_zero_row_table` (row order) over `zero_row_result` |
| job node after a zero-input run, sync and async | `Join`, `MergeJoin`: ORDER (data before system tags); `Index`, `Pick` as above | `OperatorJobNode.execute()` keeps `result_stream`; `async_execute` empty branch keeps `operator.process(...)`: the operator's own order. Populated keeps `_materialize_to_stream` (row order) |
| direct `OperatorJobNode(...).run()` (no job) | before-run table is row order, populated is operator order: every operator | OUT OF SCOPE (coordinator, option A): two populated orders per node; reported |
| function pod (stream and node) | none | — |

Coordinator decision (2026-11-14): approve a–d; item 4 (direct node) out of scope, reported.

## Files

- Modify: `src/orcapod/core/operators/static_output_pod.py` (`_in_row_order`, `_zero_row_input`, `_zero_rows_in_row_order`)
- Modify: `src/orcapod/core/nodes/operator_node.py` (`_typed_zero_row_table`, `execute`, `async_execute`)
- Modify: `src/orcapod/core/operators/index.py`, `src/orcapod/core/operators/pick.py` (empty branch)
- Create: `tests/test_pipeline/test_zero_row_column_order.py`
- Create: `changelog.d/ITL-1131.md`, `design_issues/ITL-1131-ZR2.md`

### Task 1: Failing sweep test

- [ ] Write `tests/test_pipeline/test_zero_row_column_order.py`: for every operator in
  `core/operators` and a function pod, inputs with 1 and 2 system-tag pairs, compare
  `as_table(all_info=True).schema` with `schema.equals`:
  plain stream over a zero-row input and its `head(all_info=True)` against the plain stream
  populated; job node before a run, and after a sync / async run whose input had no rows,
  against the job node populated by the same orchestrator. Also `Index`/`Pick` with `out=`.
- [ ] Run `uv run pytest tests/test_pipeline/test_zero_row_column_order.py -q`: expect the Join,
  MergeJoin, Index and Pick cases to fail, every other to pass.
- [ ] Commit `test(operators): failing zero-row column order sweep (ITL-1131)`.

### Task 2: One owner of the row order; node keeps a zero-row run in it

- [ ] `StaticOutputOperatorPod._in_row_order(table, tag_keys)`: select
  `[*tag_keys, *system tags, *data (remaining, table order), *source info]`.
- [ ] `_zero_row_input` and `OperatorJobNode._typed_zero_row_table` call it.
- [ ] `StaticOutputOperatorPod._zero_rows_in_row_order(result)`: `ArrowTableStream` of
  `_in_row_order(result.as_table(columns=ZERO_ROW_INPUT_COLUMNS), tag_keys)`, the result's data
  context. `execute()` (no rows) and the async empty branch keep it instead of the result stream.
- [ ] Run the sweep: Join/MergeJoin pass. Run `tests/test_pipeline tests/test_core/operators tests/test_channels`.
- [ ] Commit `fix(operator_node): a run with no rows keeps the row order of a run with rows (ITL-1131)`.

### Task 3: Index and Pick empty result

- [ ] Empty branch: schema from `stream.as_table(columns={"source": True, "system_tags": True})`
  (large-normalized), projected column (and token) replaced or appended as today, then
  `_in_row_order` and `_flag_source_info_nullability` on the empty table.
- [ ] Run the sweep and `tests/test_core/operators -k "index or pick"`.
- [ ] Commit `fix(index, pick): an empty result keeps the system tags and source flags (ITL-1131)`.

### Task 4: Docs, changelog, design issue, full suite, PR

- [ ] `changelog.d/ITL-1131.md` (`### Fixed`), `design_issues/ITL-1131-ZR2.md` (resolved, Fix note).
- [ ] `uv run --with ruff ruff check .`, `ruff format --check .`, basedpyright (exit code),
  `uv run pytest tests/`.
- [ ] Merge origin/main; after PR #380 merges, turn its strict xfail
  `test_a_join_with_no_input_rows_keeps_its_column_order` into a passing test.
- [ ] Draft PR against main, `Fixes ITL-1131`.

## Progress notes

- 2026-11-14: sweep done, coordinator approved a–d, item 4 out of scope.
- 2026-11-14: a-d done (commits on branch). Coordinator then moved ITL-1133 into this PR
  (developer: one stored column order): writer change (`_kept_output`, `_compute_output`)
  and option B (reorder on read, `_in_row_order_on_read`, own commit; developer approved B,
  no layout bump). Migration assessment: only `odb_v1` held two orders; `odb_v1` unreleased
  (introduced 2026-11-12, ITL-967). Merged main 0208dca4 (#387): predicted schema order
  equals the row order on every job node.
- Remaining: full suite, draft PR, CI, CodeRabbit; after PR #380 merges, flip its strict xfail.
- 2026-11-14: ITL-1135 (DropTagColumns prediction) added as its own commits (fix + golden,
  developer approved the golden move). PR #390 ready (developer). CodeRabbit round 1 (review
  5906873847 on 83ddb2c7): no actionable findings; signed reply posted. CI green.
- Remaining: when #380 merges (coordinator says when), merge main, turn its strict xfail
  `test_a_join_with_no_input_rows_keeps_its_column_order` into a passing test, push, CI, one
  signed '@coderabbitai review', worker_done.
