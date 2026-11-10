# ITL-832: MergeJoin keeps its inputs' Arrow types — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build every `MergeJoin` merged column (data and source-info) from the input columns' Arrow
element type, and keep the Arrow types of every pass-through column of `MergeJoin` and
`PolarsFilter`, so materialized output matches the inputs and the predicted schema with or without rows.

**Architecture:** One construction for aggregated list columns lives in `arrow_utils`
(`aggregated_list_type` + `list_array`), used by `build_aggregated_table` (Batch, GroupBy) and
`MergeJoin`. Pass-through columns get their input Arrow types back after the Polars round-trip via a
new type-only `arrow_utils.restore_column_types` (nullability is ITL-820's and stays untouched).

**Tech Stack:** pyarrow 23, polars, pytest, ruff, basedpyright; run everything with `uv run`.

**Base:** PR #268's branch (stacked). Branch `eywalker/itl-832-mergejoin-merged-list-columns-must-keep`.

---

## Decisions (coordinator, 2026-11-09, recorded from `ask`)

- **Rule (developer):** respect the Arrow types the inputs have; infer from Python values only when
  no Arrow type is known.
- **Q1 differing input layouts:** `MergeJoin.validate_binary_inputs` compares *Python* types, so
  `int8`/`int64`, `string`/`large_string`, `float32`/`double`, `timestamp[ms]`/`[us]` collide today.
  Use `pa.unify_schemas(..., promote_options="permissive")` for the element type when they differ;
  identical types are used as-is; no new rejections. Document in the `MergeJoin` docstring; test each
  promoted pair, including commutativity. The same promotion applies to shared tag keys.
- **Q2 outer layout:** `pa.list_(element)` (what `build_aggregated_table` already uses), through a
  helper factored out of `build_aggregated_table`. Extension elements keep the `ListLogicalType` path.
- **Q3:** also fix `MergeJoin` non-colliding columns and `PolarsFilter` pass-through columns (Polars
  turns `string`→`large_string`, `binary`→`large_binary`, `list`→`large_list`, struct child strings,
  `fixed_size_binary[16]` record ids→`large_binary`). Layout only; content hashes unchanged (verify).
- **Q4 report only:** `Datagram.with_columns` (Index/Pick), barrier materializer, SemiJoin on list
  columns.

### Revision (developer, 2026-11-09, supersedes Q1 above)

- **Q1 revised:** no promotion. Colliding data columns whose Arrow types differ (width,
  `string`/`large_string`, timestamp unit, decimal precision) raise `InputValidationError` naming
  the column and both types and telling the user to cast one side. Streams expose only Python
  schemas, so the check runs in `binary_static_process` (execution), not in validation; build-time
  validation still passes for such a pair.
- **Shared tag keys** whose Arrow types differ keep the joined type, as before (no promotion, no
  rejection); tracked under ITL-835 / ITL-829.
- **Q3 condition:** content hashes must not change; proved by
  `TestPassThroughContentHashes` and by the before/after snapshot against the base commit.
- **Q4:** Index/Pick/`Datagram.with_columns` is ITL-834 (another worker); findings handed over.

## Verified facts

- Arrow hasher is layout-insensitive (`string`==`large_string`, `list`==`large_list`) but
  width-sensitive (`int8`≠`int64`, `float`≠`double`, decimal precision).
- `ArrowTableStream` normalizes the source-info table to large types, so merged `_source_*` columns
  are `large_list<large_string>` before and after.
- `pipeline_hash` derives from Python schemas: unaffected.
- Before snapshot saved by the scratch script `snapshot.py` (`before.json`) for the before/after report.

## File map

- `src/orcapod/utils/arrow_utils.py` — add `aggregated_list_type`, `list_array`,
  `restore_column_types`; refactor `build_aggregated_table` to use `aggregated_list_type`.
- `src/orcapod/core/operators/merge_join.py` — delete `_inferred_type`, `_list_array`; snapshot input
  types; build merged columns with the helpers; restore pass-through types at schema reconstruction.
  Keep away from the `return ArrowTableStream(... tag_columns=...)` lines PR #272 edits.
- `src/orcapod/core/operators/filters.py` — `restore_column_types(filtered, table.schema)`.
- `tests/test_core/operators/test_merge_join_arrow_types.py` — new.
- `tests/test_core/operators/test_empty_inputs.py` — update PR #268's
  `test_colliding_column_type_matches_non_empty_result` to assert the input-derived type.
- `tests/test_utils/` (or wherever arrow_utils tests live) — helper tests.
- `CLAUDE.md`, `.zed/rules` — "Arrow types" note; `DESIGN_ISSUES.md` — one entry at the end.

### Task 1: arrow_utils helpers (TDD)

- [ ] Write failing tests for `aggregated_list_type` (primitive → `pa.list_(t)`, extension → the
      converter's `list[...]` extension type), `list_array` (empty and non-empty give the same type;
      extension wraps storage), `restore_column_types` (types restored, nullability untouched, unknown
      columns untouched).
- [ ] Run, see ImportError/AttributeError failures.
- [ ] Implement:

```python
def aggregated_list_type(element_type, type_converter) -> pa.DataType:
    if isinstance(element_type, pa.ExtensionType):
        element_python_type = type_converter.arrow_type_to_python_type(element_type)
        return type_converter.python_type_to_arrow_type(list[element_python_type])
    return pa.list_(element_type)

def list_array(values, list_type) -> pa.Array:
    if isinstance(list_type, pa.ExtensionType):
        storage = pa.array(values, type=list_type.storage_type)
        return pa.ExtensionArray.from_storage(list_type, storage)
    return pa.array(values, type=list_type)

def restore_column_types(table, reference_types: Mapping[str, pa.DataType]) -> pa.Table:
    fields = [f.with_type(reference_types[f.name]) if f.name in reference_types else f
              for f in table.schema]
    return table.cast(pa.schema(fields, metadata=table.schema.metadata))
```

- [ ] `build_aggregated_table` uses `aggregated_list_type`; Batch/GroupBy suites stay green.
- [ ] Commit `refactor(arrow_utils): share the aggregated list-type construction (ITL-832)`.

### Task 2: MergeJoin merged columns (TDD)

- [ ] New test module, parametrized over: int8, int16, int32, int64, uint8, float32, float64, string,
      large_string, binary, large_binary, bool, timestamp[ms], date32, list<int8>,
      large_list<int32>, decimal128(5,2), and a logical type (`Path` via a pod or the converter);
      rows ∈ {0, 2}. Assert `x` type == `pa.list_(input)` (logical: converter's `list[...]`), and
      `static_process(...).output_schema(columns=c) == op.output_schema(a, b, columns=c)` for
      `c in ({}, {"system_tags": True})`. Promoted pairs (Q1) in both argument orders → same type.
      Merged values still sorted, sources follow.
- [ ] Run: fails (e.g. `list<int64>` for int8).
- [ ] Implement in `binary_static_process`: snapshot `left_types`/`right_types` from `left_table`/
      `right_table` before the Polars join; `_merged_element_type(left, right)` = left if equal else
      permissive unify; merged data and source arrays via `aggregated_list_type` + `list_array`.
      Remove `_inferred_type`, `_list_array` and the inline ListLogicalType branch.
- [ ] Update PR #268's `test_colliding_column_type_matches_non_empty_result` to assert
      `pa.list_(arrow_type)` too.
- [ ] Run merge-join + empty-input + logical-type tests; commit
      `fix(merge_join): build merged columns from the inputs' Arrow element types (ITL-832)`.

### Task 3: pass-through columns of MergeJoin and PolarsFilter (TDD)

- [ ] Tests: non-colliding `string`, `binary`, `list<int8>`, `struct<x: int8, y: string>` data
      columns, string tag, and system tag columns keep the input type through `MergeJoin`; same for
      `PolarsFilter` (data, tag, system tags). Commutativity of shared tag key with
      `string`/`large_string`.
- [ ] Run: fails (`large_string`).
- [ ] MergeJoin: build `passthrough_types` = every non-colliding, non-`_source_<colliding>` column of
      the two input tables (shared names → permissive unify); in the final schema block use these
      types and `joined.cast(result_schema)`.
- [ ] PolarsFilter: `restore_column_types(filtered_table, {f.name: f.type for f in table.schema})`.
- [ ] Run operator suites; commit `fix(operators): keep pass-through Arrow types across Polars round-trips (ITL-832)`.

### Task 4: docs and logs

- [ ] "Arrow types" note in `CLAUDE.md` and `.zed/rules` (identical text).
- [ ] `DESIGN_ISSUES.md` entry at the end (resolved, Fix note).
- [ ] MergeJoin docstring: element type and promotion rule.
- [ ] Commit `docs: record the Arrow-type rule (ITL-832)`.

### Task 5: verification and PR

- [ ] Rerun `snapshot.py` → `after.json`; diff schemas / content hashes for the report.
- [ ] `uv run ruff check`, `uv run ruff format --check`, `uv run basedpyright` on changed files.
- [ ] Full suite on 3.11, 3.12, 3.13 (`uv run --python 3.x pytest tests/`).
- [ ] Rebase check vs origin/main (stacked on #268), push, PR (create-pr skill), link, worker_done.

### Revision 2 (developer HOLD, 2026-11-09, supersedes the Q1 revision and the tag-key extension)

- The rule for inputs whose Arrow types DISAGREE (colliding data columns and shared tag keys) is
  paused. No rejection, no promotion: behaviour stays exactly as on the base commit (verified
  identical on 64 cases), isolated in `merge_join._resolve_disagreeing_types` (TODO ITL-832), and
  pinned by `TestDisagreeingArrowTypes` (which also passes against the base commit's code).
- The rejection implementation is kept on the local-only branch
  `itl-832-local-reject-differing-arrow-types` (commit 71e707c2) so it can be restored.
- Everything undisputed stays: agreed types are respected for empty and non-empty results, the
  shared list-construction helper, pass-through type restoration with the content-hash proof, and
  the rule text.
