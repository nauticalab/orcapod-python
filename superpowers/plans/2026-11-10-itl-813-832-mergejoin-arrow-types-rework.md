# MergeJoin zero-row inputs + input Arrow types (ITL-813, ITL-832) — rework

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this
> plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make PR #278 the single PR for ITL-813 and ITL-832, reworked to the developer's
review of #268: no Arrow inference from Python values in `MergeJoin`, merged columns typed
`large_list<input element type>` (or the logical list type of a logical element), and one
isolated rule for inputs whose Arrow types disagree.

**Architecture:** `arrow_utils.aggregated_list_type` is the one list-type construction for
`MergeJoin`, `Batch` and `GroupBy`; it returns `pa.large_list(T)` for a plain element and the
converter's `list[<element>]` extension type for a logical element, raising if the converter
cannot supply one. `MergeJoin` snapshots both input tables' Arrow types before the Polars
join, resolves every column present in both inputs through `_resolve_disagreeing_types`
(layout-only disagreement -> large variant, anything else -> `InputValidationError`), builds
merged columns with an explicit type and restores every pass-through column's type.

**Tech Stack:** PyArrow 23, Polars, pytest, uv.

**Decisions (developer review of #268, relayed by the coordinator; not re-asked):**
large variants, never non-large; reuse orcapod's type converter, no hand-written inference
table; a list of a logical type is the logical list type; inputs' Arrow types are respected.

---

## Already done before this plan

- Merged `origin/main` (365a5f74, PR #272 canonical column order) into the branch; resolved
  `DESIGN_ISSUES.md` keeping every entry once.
- Merged the #268 branch (0b8a8bb2) for its last test commit `d0678dbc` (merged source-info
  type parity); resolved `tests/test_core/operators/test_empty_inputs.py`.
- After those merges one pinned case of `TestDisagreeingArrowTypes` fails (main's canonical
  join order changed what the old "pre-ITL-832" behaviour produces); that class is replaced in
  Task 3 anyway.

## File map

- `src/orcapod/utils/arrow_utils.py` — `aggregated_list_type` (large_list, logical-list error),
  `list_array` (unchanged contract: explicit type), `build_aggregated_table` (passes column name).
- `src/orcapod/core/operators/merge_join.py` — delete `_inferred_type`, `_merged_list_array`'s
  inference branches; `_resolve_disagreeing_types` implements the rule; `_input_types` raises
  through it; every output column type comes from the inputs.
- `tests/test_utils/test_arrow_utils_list_types.py` — helper unit tests.
- `tests/test_core/operators/test_merge_join_arrow_types.py` — element types, zero-row parity,
  disagreeing types, hash pins.
- `tests/test_core/operators/test_empty_inputs.py` — expected merged type now `large_list`.
- New `tests/test_core/operators/test_aggregated_list_types.py` — Batch/GroupBy large_list,
  logical list preserved, hash equality across list/large_list.
- Docs: `docs/concepts/operators.md`, `DESIGN_ISSUES.md` (MJ1, MJ2), `CLAUDE.md` and
  `.zed/rules` (both).

## Task 1: `aggregated_list_type` returns `large_list`; logical element must map to a logical list

- [ ] Failing tests in `tests/test_utils/test_arrow_utils_list_types.py`:
  - `aggregated_list_type(T, tc, column="c") == pa.large_list(T)` for int8, float32, string,
    large_string, struct, list<int8>.
  - Path element -> `tc.python_type_to_arrow_type(list[Path])`, an `ExtensionType` named
    `list[orcapod.path]` with storage `large_list<large_string>`.
  - A foreign extension element (`pa.uuid()`; converter maps it to `Any` and returns a plain
    `large_list<null>`) raises `ValueError` naming the column and the type.
- [ ] Run, see failures (`list` != `large_list`, no error raised).
- [ ] Implement:

```python
def aggregated_list_type(element_type, type_converter, *, column: str) -> pa.DataType:
    if isinstance(element_type, pa.ExtensionType):
        element_python_type = type_converter.arrow_type_to_python_type(element_type)
        list_type = type_converter.python_type_to_arrow_type(list[element_python_type])
        if not isinstance(list_type, pa.ExtensionType):
            raise ValueError(
                f"Column {column!r}: cannot build a list of logical type {element_type}; "
                f"the type converter has no logical list type for it (got {list_type})."
            )
        return list_type
    return pa.large_list(element_type)
```

  `build_aggregated_table` passes `column=field.name`.
- [ ] Run helper tests; commit `fix(arrow_utils): build aggregated lists as large_list and require a logical list for logical elements (ITL-832)`.

## Task 2: Batch / GroupBy go through the helper (large_list, logical list preserved, hashes unchanged)

- [ ] New `tests/test_core/operators/test_aggregated_list_types.py`:
  - Batch and GroupBy over an `int8`/`large_string`/struct data column: output column
    `large_list<T>`; source-info column `large_list<large_string>`; prediction via
    `unary_static_process(stream).output_schema(...)` equals `op.output_schema(stream)`.
  - Path column: output type is the `list[orcapod.path]` extension, storage
    `large_list<large_string>`, values round-trip.
  - Hash pin: each row's tag/data `content_hash` is identical to the same output cast to the
    `list<...>` layout main produced (list/large_list hash the same).
- [ ] Run (fails on `large_list` assertions), confirm Task 1 makes them pass; fix any existing
  test that pinned `list<...>` for Batch/GroupBy.
- [ ] Commit `test(operators): pin large_list and logical list types for Batch and GroupBy (ITL-832)`.

## Task 3: MergeJoin — no inference, large_list merged columns, disagreement rule

- [ ] Rewrite tests in `tests/test_core/operators/test_merge_join_arrow_types.py`:
  - `TestMergedColumnElementType`: for every `ELEMENT_CASES` type and `rows in (0, 2)`, merged
    type `== pa.large_list(T)`; merged source-info type `== large_list<large_string>`; zero-row
    left only, right only, both (struct and Path included) give the same types as non-empty,
    match `static_process(...).output_schema(...)` vs `op.output_schema(...)`, and
    `pa.concat_tables` of empty + non-empty works.
  - Narrow never widens: `int8` input -> `large_list<int8>`, `float32` -> `large_list<float>`.
  - `TestDisagreeingArrowTypes` replaced:
    - layout-only pairs (string/large_string, binary/large_binary, list<int64>/large_list<int64>,
      struct<a: string>/struct<a: large_string>, list<string>/large_list<large_string>) for a
      colliding column -> `large_list<large variant>`, both argument orders, rows 0 and 2; for a
      shared tag key -> the large variant.
    - width/other pairs (int8/int64, float32/double, timestamp ms/us, decimal precisions,
      struct with different fields) -> `InputValidationError` naming the column and both
      types, for colliding column and shared tag, both orders, rows 0 and 2.
    - CSV (`string`) vs dict (`large_string`) label column merges to `large_list<large_string>`.
    - Direct unit tests of `_resolve_disagreeing_types`.
  - Hash pins: merged/pass-through rows hash equal to the `list` layout (layout-only change);
    the width case: int8 merged column hashes differently from the int64-widened layout main
    produced, while an int64 input hashes the same as main's layout.
- [ ] Run, see failures.
- [ ] Implement in `merge_join.py`:
  - delete `_inferred_type`; replace `_merged_list_array` with a call to
    `arrow_utils.list_array(values, arrow_utils.aggregated_list_type(t, tc, column=col))`;
  - `_resolve_disagreeing_types(name, left, right) -> pa.DataType` returns the pointwise large
    unification or raises `InputValidationError(f"MergeJoin: column {name!r} has Arrow type
    {left} in one input and {right} in the other ...")`;
  - `_input_types` always returns a type for every column (no `None`);
  - schema reconstruction uses `output_types[col]` (input/resolved type for pass-through,
    built list type for merged) — no fallback to whatever the join produced.
- [ ] Run tests; commit `fix(merge_join): type merged columns large_list<input type> and reject non-layout type disagreements (ITL-813, ITL-832)`.

## Task 4: Existing tests and docs

- [ ] `tests/test_core/operators/test_empty_inputs.py`: merged type `large_list<T>`.
- [ ] grep tests for `pa.list_(` assertions on Batch/GroupBy/MergeJoin outputs and update.
- [ ] `docs/concepts/operators.md`: Arrow type rules for MergeJoin/Batch/GroupBy.
- [ ] `DESIGN_ISSUES.md` MJ1 and MJ2 fix notes (MJ2 -> resolved).
- [ ] `CLAUDE.md` and `.zed/rules`: aggregated list columns are `large_list`; logical list
  rule; disagreement rule.
- [ ] Commit `docs: record large_list aggregated columns and the disagreement rule (ITL-813, ITL-832)`.

## Task 5: Verification and PR

- [ ] `grep -n "pa.array(\|from_pylist" src/orcapod/core/operators/merge_join.py` -> every hit
  has an explicit type.
- [ ] ruff check, ruff format --check, basedpyright (exit code), pytest on 3.11/3.12/3.13,
  `uv run pytest test-objective/`.
- [ ] fetch, merge main if moved, one push, retitle/rewrite PR #278, CI, CodeRabbit rounds.
