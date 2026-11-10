# PR #272 review revision (ITL-817) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Address the developer's four inline comments on PR #272 with the revision plan the
developer approved: honest record-ID docs, one canonical column-order rule for commutative
joins, the order defined in one place, and an explicit pyright suppression.

**Architecture:** Column order follows the operator's declared argument symmetry. Commutative
operators (`Join`, `MergeJoin`) list tag and data columns by first appearance across their
inputs in canonical `pipeline_hash` order; `output_schema` / `binary_output_schema` is the only
place that computes it and `static_process` reads it back. Ordered operators (`SemiJoin`) keep
operand order (it already returns the left stream's schema and table unchanged).

**Tech Stack:** Python 3.11-3.13, PyArrow, Polars, pytest, uv, ruff, basedpyright.

**Review comments:** 4520619036 (docs wording), 4520632441 (which order), 4520639575 (derive from
output schema), 4520654070 (suppress type warning).

---

## Verified facts the plan relies on

- `ArrowHasher.hash_table`, the semantic hash of a `Schema`, and the semantic hash of a `dict`
  are all column/key-order-insensitive (checked with a scratch script). Record IDs
  (`_build_record_id_preimage` hashes `tag.as_table(system_tags=True)` + input data hash),
  `content_hash` and `pipeline_hash` therefore cannot change when column order changes.
- `ArrowTableStream.keys()` follows the `tag_columns` argument order, but
  `ArrowTableStream.output_schema()` and `as_table()` follow the **physical** table order. So the
  operator must physically reorder the table to the schema order, not just pass `tag_columns`.
- `Join.static_process` already joins in canonical order (`order_input_streams`);
  `Join.output_schema` unions in call order (the mismatch the developer flagged).
- `MergeJoin.binary_static_process` joins in **call** order, and `binary_output_schema` builds
  its data schema from a `set` (order depends on `PYTHONHASHSEED`).
- `Join` async: N>=3 already chains in canonical order; N=2 joins `inputs[0]` x `inputs[1]` in
  call order. `MergeJoin` async is barrier mode via `static_process`.
- Downstream identities key on system tags + input data hash, so a moved row under a positional
  record ID gets a new key: it is recomputed and appears as a new entry with the same user tag
  values and a different record ID; nothing is paired with the wrong row's result.

## Files

- Modify: `src/orcapod/core/operators/join.py` (`output_schema`, `static_process`, async N=2)
- Modify: `src/orcapod/core/operators/merge_join.py` (`binary_output_schema`, `binary_static_process`) — minimal
- Modify: `docs/concepts/sources.md`, `docs/concepts/operators.md`
- Modify: `src/orcapod/core/sources/arrow_table_source.py`, `src/orcapod/core/sources/stream_builder.py`
- Modify: `superpowers/specs/2026-11-09-itl-817-content-derived-record-ids.md` (same lineage claim)
- Modify: `DESIGN_ISSUES.md` (RS1, RS2 of this PR's entry)
- Test: `tests/test_core/operators/test_join_tag_order.py`

### Task 1: Failing tests for canonical column order

- [ ] Rewrite `test_join_tag_order.py`: a subprocess script (run for `PYTHONHASHSEED` 1..6) that,
  for `Join()` (left `{probe, x}`, right `{method, w}`) and `MergeJoin()` (left `{probe, x, y}`,
  right `{method, x, z}`), records for both `op(a, b)` and `op(b, a)`: `stream.keys()`,
  `op.output_schema(...)` keys with and without system tags, `as_table()` and
  `as_table(all_info=True)` column names, first-row tag/data keys, `op.static_process(...)
  .output_schema(...)` keys, and the rows of `as_table(all_info=True)` sorted.
- [ ] Assert: both operand orders give identical values for every field; prediction equals
  `static_process` output schema order (with and without system tags); every seed agrees.
- [ ] In-process async test for `Join` (2 inputs, with `input_pipeline_hashes`): first-row tag
  and data keys equal the sync stream's keys for both operand orders.
- [ ] Add the targeted `# pyright: ignore[<rule>]` on the `json.loads` return (rule name taken
  from basedpyright's own report).
- [ ] Run `uv run pytest tests/test_core/operators/test_join_tag_order.py -v`; expect FAIL.

### Task 2: Join

- [ ] `output_schema`: iterate `self.order_input_streams(*streams)` instead of `streams`.
- [ ] `static_process`: delete the `tag_order` loop and the `tag_keys`-based reorder; replace with

```python
tag_schema, data_schema = self.output_schema(*streams)
tag_order = list(tag_schema.keys())
data_order = list(data_schema.keys())
listed = {*tag_order, *data_order}
table = table.select(
    tag_order + data_order + [c for c in table.column_names if c not in listed]
)
return ArrowTableStream(table, tag_columns=tuple(tag_order))
```

- [ ] Async N=2: join `inputs` in canonical position order (shared helper with `_staggered_join`
  that reads the canonical position from the suffix), passing the permuted suffixes.
- [ ] Comment states the rule: commutative operator, canonical `pipeline_hash` order, first
  appearance; inputs with equal pipeline hashes keep call order (stable sort).

### Task 3: MergeJoin (minimal)

- [ ] `binary_output_schema`: iterate `self._canonical_order(...)`; tags by `union_schemas` of the
  canonical first then second; data keys by first appearance (first's keys, then second's new
  keys) instead of the `set`.
- [ ] `binary_static_process`: take `left_table`/`right_table` and their keys from the canonical
  order (type converter stays the call-order left stream's, unchanged); replace the final
  `all_tag_keys` reorder and `ordered_tag_keys` with the same schema-derived `select` as Join.

### Task 4: Docs and docstrings

- [ ] `docs/concepts/sources.md`, `ArrowTableSource`, `_make_provenance_token`, `DESIGN_ISSUES.md`
  RS2, and the spec: positional default record ID; a moved row gets a different record ID, is
  treated as a new entry (recomputed; tag table shows two entries with identical tag values and
  distinct record IDs); no result is paired with the wrong row; `record_id_column` + explicit
  stable `source_id` avoids the duplicates and the recomputation. Remove lineage wording and the
  swap parenthetical.
- [ ] `docs/concepts/operators.md`: one sentence stating the column-order rule and that callers
  should address columns by name; check the Join example output against a real run.
- [ ] `DESIGN_ISSUES.md` RS1 fix note: canonical order, single definition in `output_schema`.
- [ ] CLAUDE.md / `.zed/rules`: update only if they describe join column order (they do not at
  plan time; re-check).

### Task 5: Verify, commit, merge main, push, reply

- [ ] Targeted tests pass; hash-stability suites pass unchanged
  (`tests/test_hashing`, `tests/test_pipeline/test_system_tag_run_stability.py`, operator tests).
- [ ] One commit `fix(operators): ...` with Co-Authored-By trailer.
- [ ] `git merge origin/main` (merge commit), resolve `DESIGN_ISSUES.md` keeping both sides.
- [ ] ruff check, ruff format --check, basedpyright (exit code), pytest on 3.11/3.12/3.13,
  `pytest test-objective/`.
- [ ] One push; reply on each of the four threads (no resolve); update PR description; watch CI
  and CodeRabbit (max 3 rounds).
