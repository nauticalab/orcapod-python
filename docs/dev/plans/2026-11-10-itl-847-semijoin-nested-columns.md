# SemiJoin over nested columns and string/large_string keys — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `SemiJoin` works when either input has list, large_list, struct or logical
(extension) columns, as non-key or key, and when a key is `string` on one side and
`large_string` on the other (ITL-847, first case of ITL-829), with sync and async paths
yielding the same rows.

**Architecture:** The sync path replaces pyarrow's Acero `left semi` join (which rejects
nested non-key fields and `string` vs `large_string` keys) with a Polars
`join(how="semi", maintain_order="left")` over the left table and the right table's key
columns, then casts the result back to the left table's exact Arrow schema (types,
top-level and nested nullability, extension types). Before the join every common key's
two Arrow types go through `require_shared_arrow_type` (large-normalized equality, else
`InputValidationError`), because Polars would silently unify `int8` with `int16`. The
async path does the same type check on the first row of each side and builds its
lookup from hashable Arrow storage values instead of `as_dict()` values (struct and list
keys are unhashable dicts and lists today).

**Tech Stack:** pyarrow 23, polars 1.41, pytest.

## Verified prototype facts (polars 1.41.2, pyarrow 23.0.1)

- Polars semi join keeps struct, list, large_list and orcapod extension (`Path`) payloads;
  `.cast(left_schema)` restores the left table exactly (`equals` is True), including
  `fixed_size_binary[16]` record ids and non-nullable fields.
- `string` vs `large_string` keys match.
- `int8` vs `int16` keys are silently unified by Polars: the explicit type check is
  required.
- Scalar null keys never match (same as Acero today). A top-level null **struct/list** key
  matches a null on the other side in Polars. To keep one rule ("a null key never
  matches"), drop right rows whose key has a top-level null before the join; the async
  path skips those rows too. Nulls nested inside a struct/list key compare equal in both
  paths. Polars treats NaN as equal to NaN; the async freeze maps NaN to a sentinel.

## Output types (decision)

The result keeps the left input's Arrow schema exactly — today's behaviour, Join's
behaviour (`restore_schema_nullability` restores the reference type), and what the async
path yields, which passes left datagrams through untouched. Inputs that already worked
therefore produce byte-identical tables. (Re-typing to large variants on the sync path
alone would make sync and async disagree.)

## Out of scope (report only)

- ITL-829 second case (non-nullable vs nullable key, raised by `schema_utils`
  `intersection_schemas` in `validate_binary_inputs`): unchanged; its strict xfail stays.
- Meta columns in SemiJoin (ITL-876). Join, MergeJoin.

## Files

- Modify: `src/orcapod/core/operators/semijoin.py` (sync + async).
- Modify: `tests/test_core/operators/test_async_sync_equivalence.py` (un-xfail
  `test_semijoin_with_nested_columns`; the semijoin case may use nested sources).
- Create: `tests/test_core/operators/test_semijoin_nested.py` (public-interface tests).
- Modify: `DESIGN_ISSUES.md` (one appended entry).

### Task 0: Before-dump

- [ ] Write `scratchpad/dump_semijoin.py` that runs every SemiJoin case that works on main
  (the `test_operators.py` fixtures, the equivalence-test `source_c`/`source_b(nested=False)`
  pair, scalar-only pairs with string keys of the same type, empty-left/empty-right,
  no-common-key) and dumps `content_hash`, `pipeline_hash`, `output_schema(all_info=True)`,
  `as_table(all_info=True)` schema + rows, and each row's Tag/Data `content_hash`, sync and
  async, to JSON. Run on main → `before.json`.

### Task 1: Failing tests

- [ ] Write `tests/test_core/operators/test_semijoin_nested.py`:
  - kept-side nested non-key: list, large_list, struct, dataclass, Path columns on left;
    result rows = filtered left rows in left order; `as_table()` schema equals the left's.
  - filtering-side nested non-key: same columns on the right.
  - struct and list keys (incl. nested null inside key, top-level null never matches).
  - Path key.
  - zero-row left, zero-row right (with nested columns).
  - `CSVSource` x `DictSource` string keys (ITL-829 case 1), both orders.
  - `int8` vs `int16` key raises `InputValidationError` naming the column and both types,
    sync and async.
  - predicted `output_schema` equals `binary_static_process(...).output_schema` (schema
    prediction exception) for nested inputs.
  - async equals sync for each nested scenario (reuse `assert_equivalent` helpers).
  - row order = left order with a right side in a different order.
- [ ] Run: `uv run pytest tests/test_core/operators/test_semijoin_nested.py -v` → FAIL
  (`ArrowInvalid ... not supported in join non-key field`, `Incompatible data types`).

### Task 2: Sync fix

- [ ] In `binary_static_process`: get `left_table` (source + system tags) and
  `right_table`; for each common key call `require_shared_arrow_type("SemiJoin", key, ...)`;
  `right_keys = right_table.select(common_keys)` with rows dropped where any key is null
  (`pc.is_valid` and-ed); `pl.DataFrame(left_table).join(pl.DataFrame(right_keys),
  on=common_keys, how="semi", maintain_order="left").to_arrow().cast(left_table.schema)`.

### Task 3: Async fix

- [ ] On the first left row, with right rows non-empty: compute common keys as today, call
  `require_shared_arrow_type` on the first left and first right row's Arrow schemas, build
  the lookup from `_row_key(tag, data, keys)` which reads each key from the datagram's
  Arrow table (extension → `.storage`), `to_pylist`, freezes lists/dicts/tuples into
  tuples and NaN into a sentinel, and returns `None` when any key is null (never matches).

### Task 4: Un-xfail and verify

- [ ] Remove the strict xfail on `test_semijoin_with_nested_columns`; switch the semijoin
  equivalence case to nested sources; remove `semijoin` from `SCALAR_META_ONLY` only if the
  meta test then passes (else leave, ITL-876).
- [ ] After-dump; `diff before.json after.json` must be empty.
- [ ] `uv run pytest tests/`, ruff check/format, basedpyright exit code.
- [ ] Confirm `test_binary_op_delegation` (ITL-829 case 2) still xfails strictly.

### Task 5: Finish

- [ ] DESIGN_ISSUES.md entry; commit; merge origin/main; push; PR (Fixes ITL-847, Refs
  ITL-829 case 1); link; worker_done.
