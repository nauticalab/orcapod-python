# ITL-854: async Join keeps Data meta columns — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `Join.async_execute` emits the same `Data` meta (`__`-prefixed) columns as `Join.static_process`: names, collision renames, Arrow types, nullability, values and order. The fix goes into PR #280 (ITL-837).

**Architecture:** Extract the collision-rename loop of `static_process` into one static helper, `Join._collision_renames`. `static_process` calls it unchanged, so the sync path keeps its exact behaviour. The async `_merge_data` reads each side with `as_table(columns={"meta": True})`, renames the right side's colliding columns with the same helper, using the right input's canonical index (1 for two inputs, i for the i-th binary join of the staggered chain), and hstacks the Arrow tables. `Data` splits the meta columns back out. No Python-value round-trip, no new operator state, no protocol change.

**Tech Stack:** pyarrow, pytest, uv.

**Decided with the coordinator (November 10, 2026, option A):** async meta order is canonical concatenation, which is the physical order sync produces. Sync moves columns that an input's `output_schema()` lists as data to the front, and only a raw `ArrowTableStream` over a table with `__` columns lists them. So async differs only for such inputs when their meta collides or is mixed with other stream kinds. `async_execute` never sees input schemas, so that order case is pinned with a strict xfail naming ITL-858. The pre-existing sync rename bug is pinned with its own strict xfail (ITL-857) and left unfixed: the rename does not check the next input's own unrenamed names, so `__x` renamed to `__x_1` can duplicate that input's own `__x_1`.

---

## Task 1: Failing tests (meta-column equivalence)

**Files:** Modify `tests/test_core/operators/test_async_sync_equivalence.py`

- [ ] Add `assert_same_meta(expected, actual)`. It compares `data.arrow_schema(columns={"meta": True})` (types, nullability, order), `data.meta_columns`, and the meta values from `data.as_table(columns={"meta": True}).select(meta_columns).to_pylist()`.
- [ ] Add `_with_meta(source, meta: dict[str, Column]) -> ArrowTableStream`. It builds a stream from `source.as_table(columns={"source": True, "system_tags": True})` with the meta columns appended, so every row's `Data` carries them.
- [ ] Add `META_CASES`: every `Case` with its sources wrapped by `_with_meta`, using distinct meta names per source. Types: non-null `int8`, nullable `string`, `list<int8>`. Add `test_async_meta_equals_sync` over `META_CASES`, through `process` (validation passes because nothing collides).
- [ ] Add `test_join_colliding_meta_async_equals_sync`, which runs sync through `static_process` (the only route that accepts colliding meta). Two inputs: `[__u_a, __m]` and `[__u_b, __m]`. Three inputs: `[__m]` each.
- [ ] Add a strict xfail `test_join_meta_order_schema_dependent` (ITL-858): three inputs `[__u_i, __m]` through `static_process`. Sync moves the unique names forward; async keeps concatenation order.
- [ ] Add a strict xfail `test_join_meta_rename_collides_with_own_column` (ITL-857): inputs whose canonical order makes `__x` rename to `__x_1` while that input already has `__x_1`. Sync raises Polars `DuplicateError`. The test asserts that both paths succeed and are equal.
- [ ] Run `uv run pytest tests/test_core/operators/test_async_sync_equivalence.py -q`. Expected: the Join meta cases FAIL (async meta `[]`). Record the results for the other operators.

## Task 2: Shared helper and async fix

**Files:** Modify `src/orcapod/core/operators/join.py`

- [ ] Add a static method `_collision_renames(existing, incoming, keep, index) -> dict[str, str]` holding exactly the loop now at `static_process` lines 207-216: a candidate `f"{col}_{index}"`, with the counter bumped while the candidate is in `existing` or already used.
- [ ] `static_process`: `rename_map = self._collision_renames(set(table.column_names), next_table.column_names, join_key_set, idx)`. No other sync change.
- [ ] `_merge_data(left, right, right_index)`: keep the data-overlap check on `as_table()`. Then:
  ```python
  left_full = left.as_table(columns={"meta": True})
  right_full = right.as_table(columns={"meta": True})
  renames = Join._collision_renames(set(left_full.column_names), right_full.column_names, (), right_index)
  if renames:
      right_full = right_full.rename_columns([renames.get(n, n) for n in right_full.column_names])
  return Data(arrow_utils.hstack_tables(left_full, right_full), source_info=..., data_context=left.data_context_key)
  ```
- [ ] `_merge_pair_rename` passes `right_index=1`. `_merge_pair_passthrough` gains `right_index`, and `_staggered_join` binds `i` per binary join with `functools.partial`.
- [ ] Run the equivalence file: the Join meta cases PASS, and the xfails stay xfailed.

## Task 3: Other operators

- [ ] For each other operator with its own async path whose meta case fails: if the fix is the same small local change, make it. Otherwise pin it with a strict xfail naming ITL-846 and report it.

## Task 4: Hash invariance and full verification

- [ ] `uv run pytest tests/` on 3.11, 3.12 and 3.13 (`uv run --python 3.1x`). The pinned sync hashes in `test_issue_example_types_and_pinned_hashes` and every existing hash test must stay unchanged.
- [ ] `uv run --with ruff ruff check .`, `uv run --with ruff ruff format --check .`, `uv run --with basedpyright basedpyright` (exit codes).

## Task 5: Docs and PR

- [ ] CHANGELOG ITL-837 entry: add one sentence on meta columns (ITL-854). DESIGN_ISSUES ITL-837 entry: add one sentence.
- [ ] Commit (Conventional Commits, Co-Authored-By trailer), make one push, and update the PR #280 body: add `Fixes ITL-854` next to `Fixes ITL-837`, and one plain sentence on the remaining order difference.
- [ ] Wait for green CI, request a CodeRabbit review (at most 2 rounds), then send worker_done.
