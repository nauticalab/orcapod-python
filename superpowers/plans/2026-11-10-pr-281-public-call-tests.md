# PR #281 review: drive chained-aggregation tests through the public call

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Address the developer's review comment 4530134335 on PR #281 by making
`tests/test_core/operators/test_chained_aggregation.py` exercise operators through
`op(stream)` instead of `op.unary_static_process(stream)`, except where the static
result is the thing under test.

**Architecture:** One commit: this test file plus the test convention in CLAUDE.md and
`.zed/rules` (Task 2). No source changes, no other test files.

**Tech Stack:** pytest, pyarrow, orcapod operators (`GroupBy`, `Batch`, `MergeJoin`).

---

### Task 1: Switch behavioural assertions to the public call

**Files:**
- Modify: `tests/test_core/operators/test_chained_aggregation.py`

- [ ] **Step 1:** `Chain.__init__`: `self.result = self.op(self.upstream)` (was
  `self.op.unary_static_process(self.upstream)`); update the class docstring.
- [ ] **Step 2:** `test_hashes_are_computable`: the last line already used
  `chain.op(chain.upstream)`; it now duplicates `chain.result`, so assert on
  `chain.result.as_table(all_info=True).num_rows > 0`.
- [ ] **Step 3:** `test_three_level_chain_matches_sync`:
  `sync_rows = list(op(upstream).iter_data())`.
- [ ] **Step 4:** `ZERO_ROW_CHAINS`: `GroupBy(["g"])(s)`, `Batch(batch_size=2)(s)`,
  `Batch(batch_size=2)(Batch(batch_size=2)(s))`.
- [ ] **Step 5:** `test_prediction_matches_static_process` keeps the only
  `unary_static_process` call, computed locally:

```python
        # Not ``chain.result``: ``op(stream)`` returns a stream whose
        # ``output_schema()`` delegates back to the operator's prediction, so
        # comparing against it would be circular (CLAUDE.md, "Important
        # implementation details"); ``as_table()`` legitimately carries columns
        # the schema omits.
        static = chain.op.unary_static_process(chain.upstream)
        assert chain.op.output_schema(
            chain.upstream, columns=columns
        ) == static.output_schema(columns=columns)
```

- [ ] **Step 6:** Run `uv run pytest tests/test_core/operators/test_chained_aggregation.py -v`.
  Expected: same pass/xfail counts as before (the ITL-845 strict xfail still xfails).
  If any assertion's outcome changes, STOP and `ask` the coordinator with evidence.
- [ ] **Step 7:** `grep -n unary_static_process` over the file shows exactly one use.
- [ ] **Step 8:** ruff check, ruff format --check, basedpyright (exit code), full
  `uv run pytest tests/` on 3.11, 3.12, 3.13.
- [ ] **Step 9:** Commit `test(operators): drive chained-aggregation tests through the public call`,
  push once, reply on thread 4530134335 (do not resolve), wait for CI, update PR status.

### Task 2: Record the test convention (coordinator redirect, developer approved)

**Files:**
- Modify: `CLAUDE.md`, `.zed/rules` (identical wording)

- [ ] **Step 1:** Add a self-contained `## Testing conventions` section right after the
  "Lint, format and type checks" section in both files: tests use the public interface
  (`op(stream)`); non-public entry points only when the test is about that entry point;
  the schema-prediction exception points to "Important implementation details".
- [ ] **Step 2:** Include both files in the same commit as Task 1; mention the
  convention in the thread reply and the PR description.
