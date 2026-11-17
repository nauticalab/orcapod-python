# ITL-837: Join's async path keeps Arrow types — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `Join.async_execute` yields rows with exactly the Arrow types, nullability, column
order, system tags and Tag/Data content hashes of `Join.static_process`, for two and for three
or more inputs; every other operator with its own async path is checked by the same test.

**Architecture:** The async merge helpers (`_merge_pair_rename`, `_merge_pair_passthrough`,
`_rename_sys_tags`) stop rebuilding `Tag` / `Data` from `as_dict()` Python values. They build
the merged row from the incoming datagrams' Arrow tables (`as_table()`), so no type is ever
inferred. The data context comes from the incoming datagrams (left/canonical-first datagram's
`data_context`); the operator gains no state and no protocol changes.

**Tech Stack:** Python, PyArrow, pytest, asyncio channels (`orcapod.channels.Channel`).

---

## Reproduction (before the fix, on origin/main ab544982)

Two `ArrowTableSource`s, tag `k: int16`, data `a: int8` / `b: int8`:

```
sync  tag: k: int16           | data: a: int8, b: int8
async tag: k: int64 not null  | data: a: int64 not null, b: int64 not null
Tag/Data content hashes differ for every row.
```

## Sync semantics to mirror (read from `static_process`)

* Column order: tag and data columns by first appearance across inputs in canonical
  (`pipeline_hash`) order. The async path already joins in canonical order (PR #272): left is
  canonically first, so "left columns, then right-only columns" reproduces it, also for the
  staggered `join(join(x, y), z)` chain.
* Shared tag (join-key) columns: `restore_schema_nullability(table, left_ref, right_ref)` —
  the last (right) reference wins, so a shared key takes the **right** input's field
  (nullability). Values are equal by construction (they matched).
* Data columns: left fields from left, right fields from right; no overlap (Join validates).
* System tags: Python dicts on the `Tag` in both paths (sync `iter_data` builds `Tag` from the
  table and extracts system tags via the converter), so passing the dict values through is
  already equivalent.

## Rules (developer, non-negotiable)

* No Arrow type inference from Python values when a type is known; no bare `pa.array` /
  `from_pylist` without a schema; converter only when nothing is known (not needed here).
* Large-normalized types; width, nullability, struct fields, logical types never change.
  Disagreement is an error: a shared tag key whose large-normalized Arrow types differ between
  the two sides raises `InputValidationError` in the async path. (Sync currently silently takes
  the right side's type — a pre-existing sync defect; pinned with a strict xfail and reported,
  not fixed here: changing it is a sync behaviour change.)
* Context from the datagrams, never `self.data_context` / `contexts.resolve_context`.

## File Structure

* Modify `src/orcapod/core/operators/join.py` — new static helpers `_merge_tag_tables`,
  `_merge_data_tables`, `_build_tag`; rewrite `_merge_pair_rename`, `_merge_pair_passthrough`,
  `_rename_sys_tags` to use them.
* Create `tests/test_core/operators/test_async_sync_equivalence.py` — the sync-vs-async
  equivalence matrix over Join (2, 3 inputs) and every operator with an async path, plus a
  pinned-hash test and the shared-key disagreement tests.
* Docs: `CHANGELOG.md` (one v0.2.0 entry), `DESIGN_ISSUES.md` (one entry appended at the
  end), `docs/concepts/operators.md` only if it describes async behaviour.

### Task 1: Failing equivalence test

**Files:** Create `tests/test_core/operators/test_async_sync_equivalence.py`

- [ ] Build three non-identical sources over shared tag `k: int16` (input A also has tag
  `ta: int8`, input C tag `tc: uint8`), with data columns spread across them: `int8`,
  `int16`, `int32`, `uint8`, `float32`, `string`, `large_string`, `binary`, `list<int8>`,
  `large_list<float32>`, `list<int8 not null>` (non-nullable items), a struct, a `Path`
  logical column (via the default type converter), a nullable column holding a null and
  non-nullable columns (cast `nullable=False`).
- [ ] `_sync_rows(op, *sources)` = `op.process(*sources).iter_data()`;
  `_async_rows(op, *sources)` feeds each source's `iter_data()` into a `Channel`, runs
  `op.async_execute(readers, out.writer, input_pipeline_hashes=[s.pipeline_hash() ...])`
  and collects.
- [ ] `_assert_equivalent(sync_rows, async_rows)`: match rows by a stable key (repr of sorted
  user-tag dict + sorted data dict); for each pair assert equal
  `tag.arrow_schema(columns={"system_tags": True})`, `data.arrow_schema(columns={"source": True})`
  (types AND nullability, field order), `list(tag.keys())`, `list(data.keys())`,
  `tag.content_hash()`, `data.content_hash()`, `list(tag.system_tags().items())`,
  `data.source_info()`, and the `arrow_hasher.hash_schema` of the combined row schema plus
  the `semantic_hasher.hash_object` of `(tag.schema(), data.schema())`.
- [ ] Cases: Join(A,B), Join(B,A), Join(A,B,C), Join(C,A,B); MergeJoin on two inputs with
  colliding non-struct columns; SemiJoin(A,B); Batch(2), Batch(0); GroupBy(["ta"]);
  PolarsFilter(constraints); MapTags; MapData; Select/Drop Tag/Data columns; Index on the
  `list<int8>` column; Pick on a `dict[str, int16]` column.
- [ ] Run `uv run pytest tests/test_core/operators/test_async_sync_equivalence.py -v`;
  expected: the Join cases FAIL (int64 vs int16/int8); record any other operator failures.

### Task 2: Fix Join's async merge

**Files:** Modify `src/orcapod/core/operators/join.py`

- [ ] Add helpers:

```python
@staticmethod
def _merge_tag_tables(left: TagProtocol, right: TagProtocol) -> "pa.Table | None":
    """Left tag columns then right-only ones; a shared key takes the right field."""
    left_table, right_table = left.as_table(), right.as_table()
    fields, arrays = [], []
    for name in left_table.column_names:
        if name in right_table.column_names:
            lf, rf = left_table.schema.field(name), right_table.schema.field(name)
            if arrow_utils.normalize_to_large_types(lf.type) != arrow_utils.normalize_to_large_types(rf.type):
                raise InputValidationError(...)
            fields.append(rf); arrays.append(right_table.column(name))
        else:
            fields.append(left_table.schema.field(name)); arrays.append(left_table.column(name))
    for name in right_table.column_names:
        if name not in left_table.column_names:
            fields.append(right_table.schema.field(name)); arrays.append(right_table.column(name))
    if not fields:
        return None  # tagless inputs: Tag({}).as_table() has no columns and no rows
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields))
```

  `_merge_data(left_data, right_data)` hstacks `left.as_table()` and `right.as_table()`
  (raising `InputValidationError` on overlapping names), merges `source_info()` dicts, and
  returns `Data(table, source_info=..., data_context=left_data.data_context)`.
  `_build_tag(table, system_tags, data_context)` returns `Tag(table, system_tags=...,
  data_context=...)`, or `Tag({}, ...)` when `table is None`.
- [ ] Rewrite `_merge_pair_rename`, `_merge_pair_passthrough` and `_rename_sys_tags` on these
  helpers (context: `left_tag.data_context` / `tag.data_context`).
- [ ] Run Task 1 tests: Join cases PASS.
- [ ] Commit `fix(join): async path builds merged rows from Arrow tables (ITL-837)`.

### Task 3: Other operators

- [ ] For each non-Join failure: same defect + small/local → fix here; otherwise mark
  `pytest.mark.xfail(strict=True, reason=...)` and record for worker_done.

### Task 4: Hash stability proof

- [ ] Pinned test: for the issue's example (k: int16, a/b: int8), async and sync per-row
  Tag/Data hashes equal the literal sync values recorded on main before the fix; and the sync
  stream's `content_hash()` / `pipeline_hash()` equal literals recorded on main.
- [ ] Run `uv run pytest tests/test_hashing tests/ -k "stab or hash"` and the full suite.

### Task 5: Docs and checks

- [ ] CHANGELOG v0.2.0 entry; DESIGN_ISSUES entry (resolved, with **Fix:**); operators.md if
  it describes async behaviour.
- [ ] `uv run --with ruff ruff check .`, `ruff format --check .`,
  `uv run --with basedpyright basedpyright` (exit codes), `uv run pytest tests/` on 3.11,
  3.12, 3.13.
- [ ] Rebase on origin/main, push, PR "Fixes ITL-837", CodeRabbit rounds, worker_done.
