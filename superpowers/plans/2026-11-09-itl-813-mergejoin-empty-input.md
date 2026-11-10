# ITL-813 MergeJoin Empty-Input Fix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `MergeJoin` with a zero-row input returns a zero-row table with the predicted joined schema instead of raising, and both `Join` and `MergeJoin` gain empty-input regression tests.

**Architecture:** Two null-typed-on-zero-rows constructs in `MergeJoin.binary_static_process` are given explicit Arrow types. Non-empty behaviour (Arrow types, hashes, stored schemas) is left byte-identical: explicit types are applied only where inference has no rows to infer from. No public API change.

**Tech Stack:** Python 3.12, PyArrow, Polars, pytest, `uv run --no-sync` (pygraphviz cannot build on this machine; see coordinator note).

---

## Root cause (verified on main 61d0f18e by probe, before any code change)

`pa.array([])` has Arrow type `null`. `MergeJoin.binary_static_process` hits that twice:

1. **Helper key** (`merge_join.py` lines 222, 225): `pa.array([0] * len(table))` is `null` for a
   zero-row side and `int64` for the other. Polars raises
   `SchemaError: datatypes of join keys don't match - _common: i64 on left does not match _common: null on right`.
   Reproduced for left-empty, right-empty, colliding and non-colliding.
2. **Merged list columns** (lines 299, 301, 308): `pa.array(merged_vals)` and
   `pa.array(merged_sources)` are `null` when the join yields zero rows. With both sides empty
   the helper keys agree (both `null`), the join succeeds, and the failure moves to schema
   reconstruction: `ValueError: A null type field may not be non-nullable`. Once (1) is fixed
   this path is reached for every empty-side join with a colliding column.

`Join` already uses `type=pa.int8()` (lines 137, 155) and passes; it has no tests for it.

## Probe results for the other operators (zero-row input)

| Operator | Zero-row result |
|---|---|
| Join | pass (already typed) |
| MergeJoin | **fails** (two constructs above) — fixed here |
| SemiJoin | pass (no helper key; Polars semi join on real tag keys) |
| Batch (size 0, size 2, drop_partial) | pass: 0 rows, no null-typed columns |
| GroupBy | pass: 0 rows, no null-typed columns |
| Select/Drop Tag/Data columns | pass |
| MapTags / MapData | pass |
| PolarsFilter (empty in; all rows dropped) | pass |
| Pick / Index | pass |

Not zero-row bugs, found by the probe, **reported only** (they reproduce on non-empty input):

- `PolarsFilter` output loses `nullable=False` (Polars round trip), so the materialized schema
  is `int | None` where `output_schema()` predicts `int`.
- `Batch`, `GroupBy`, `MergeJoin` predict `list[T | None]` for a nullable element column but
  materialize `list[T]`.
- `MergeJoin` materializes system-tag columns as non-nullable (null-count inference) when the
  inputs' system-tag fields are nullable (e.g. after a `PolarsFilter`), while the prediction
  keeps them nullable. The MergeJoin system-tag test therefore compares system-tag column
  names, not full schemas.
- `Pick` / `Index` reject a nullable `dict` / `list` column (`dict[str, int] | None` "is not a
  supported pick target").

## File structure

- Modify: `src/orcapod/core/operators/merge_join.py` — type the helper key; add module-level
  helper `_list_array` and use it for merged data/source columns.
- Create: `tests/test_core/operators/test_empty_inputs.py` — all zero-row operator tests.
- Modify: `DESIGN_ISSUES.md` — one entry appended at the end.

---

### Task 1: Failing tests

**Files:**
- Create: `tests/test_core/operators/test_empty_inputs.py`

- [ ] **Step 1: Write the tests.** Helpers:
  - `_stream(columns, tags, nullable=True)` builds an `ArrowTableStream` from a `pa.table`,
    optionally casting every field to `nullable=False`.
  - `_filtered_empty(columns, tags)` builds an `ArrowTableSource` with rows and runs
    `PolarsFilter(predicates=[pl.lit(False)]).static_process(...)` — the real fede shape: an
    empty stream that still carries system-tag and source-info columns.
  - `_assert_empty_result(op, streams, expected_columns)`:
    `op.validate_inputs(*streams)`; `result = op.static_process(*streams)`; for
    `cfg in ({}, {"system_tags": True})`: table has 0 rows, no field is `pa.null()`,
    and `result.output_schema(columns=cfg) == op.output_schema(*streams, columns=cfg)`
    (materialized stream vs prediction — never `process()`, which is circular). Default-config
    column names equal `expected_columns` as a set. Finally `len(op.process(*streams).as_table()) == 0`.

  Test classes:
  - `TestJoinEmptyInput`: issue snippet; two streams with empty at index 0 / 1 / both; three
    streams with empty at index 0 / 1 / 2 / all; a filtered-empty (system-tag-carrying) case.
  - `TestMergeJoinEmptyInput`: issue-correction snippet; parametrized
    `side in (left, right, both)` × `colliding in (False, True)`; the colliding case uses
    non-nullable columns, and asserts the data schema type is `list[int]` and the Arrow type
    is a list; a filtered-empty (system-tag-carrying) non-colliding case; a colliding case
    with source-info columns requested (`columns={"source": True}` has no null-typed field).
  - `TestOtherOperatorsEmptyInput`: parametrized over SemiJoin (3 shapes), Batch (3 configs),
    GroupBy, the four column-selection operators, MapTags, MapData, PolarsFilter (empty in,
    and all rows dropped), Pick, Index. Batch/GroupBy use non-nullable inputs and
    PolarsFilter nullable inputs, because of the reported non-zero-row nullability
    mismatches above (stated in the test docstring).

- [ ] **Step 2: Run, verify MergeJoin fails and everything else passes.**

Run: `uv run --no-sync pytest tests/test_core/operators/test_empty_inputs.py -q`
Expected: every `TestMergeJoinEmptyInput` test FAILS with `SchemaError` (`_common`) or
`ValueError: A null type field may not be non-nullable`; Join and other-operator tests PASS.

- [ ] **Step 3: Commit** `test(operators): add zero-row input tests for all operators (ITL-813)`

### Task 2: Fix MergeJoin

**Files:**
- Modify: `src/orcapod/core/operators/merge_join.py`

- [ ] **Step 1: Type the helper key** (both sites), same as `Join`:

```python
        # Use int8 explicitly so a zero-row table doesn't produce a null-typed
        # column that Polars rejects as a join key (same as Join).
        left_table = left_table.add_column(
            0, COMMON_JOIN_KEY, pa.array([0] * len(left_table), type=pa.int8())
        )
        right_table = right_table.add_column(
            0, COMMON_JOIN_KEY, pa.array([0] * len(right_table), type=pa.int8())
        )
```

- [ ] **Step 2: Add the module-level helper** after the imports:

```python
def _list_array(values: list[Any], element_type: "pa.DataType | None") -> "pa.Array":
    """Build the Arrow array for a merged (list-valued) column.

    With rows present the type is inferred from the values, exactly as before.
    With zero rows inference yields a ``null``-typed array, which cannot be a
    non-nullable field and does not match the predicted ``list[T]`` schema, so
    the list type is built from ``element_type`` instead.

    Args:
        values: One list per output row.
        element_type: Arrow type of the column's elements before merging, or
            ``None`` if unknown.

    Returns:
        The list array for the merged column.
    """
    if values or element_type is None:
        return pa.array(values)
    if isinstance(element_type, pa.ExtensionType):
        # Arrow cannot embed an extension type in a list value field.
        element_type = element_type.storage_type
    return pa.array(values, type=pa.list_(element_type))
```

- [ ] **Step 3: Use it** at the three untyped sites. Lines 299 and 301 become
  `merged_array = _list_array(merged_vals, elem_arrow_type)`. For the source column capture
  the element type before the column is dropped:

```python
            if has_source:
                source_idx = joined.column_names.index(left_source_col)
                source_elem_type = joined.schema.field(left_source_col).type
                joined = joined.drop(left_source_col)
                joined = joined.drop(right_source_col)
                source_array = _list_array(merged_sources, source_elem_type)
                joined = joined.add_column(source_idx, left_source_col, source_array)
```

- [ ] **Step 4: Run** `uv run --no-sync pytest tests/test_core/operators/ -q` — all PASS.

- [ ] **Step 5: Commit** `fix(merge_join): type helper key and merged columns for zero-row inputs (ITL-813)`

### Task 3: Log, verify, ship

- [ ] **Step 1:** Append one `DESIGN_ISSUES.md` entry (status `resolved`, with **Fix:** note).
- [ ] **Step 2:** `uv run --no-sync pytest tests/ -q` — full suite; record pre-existing
  failures and skips by name.
- [ ] **Step 3:** Commit `docs(design-issues): log MergeJoin zero-row fix (ITL-813)` together
  with this plan.
- [ ] **Step 4:** Rebase on `origin/main` if it moved, rerun the suite, push, open the PR
  against `main` (`Fixes ITL-813`), `orca linear attach`, `orca worktree set --pr`, send
  `worker_done`.

## Self-review

- Spec coverage: helper-key fix (T2.1); Join tests two/three streams, one/all empty (T1);
  MergeJoin left/right/both incl. colliding column (T1); schema == predicted (T1 helper);
  other operators checked (probe table) and pinned by tests (T1); same-kind fix for merged
  columns (T2.2–2.3); reportable findings listed above.
- No change to public API, documented behaviour, hash values, or stored schemas for
  non-empty inputs — no coordinator approval needed.
