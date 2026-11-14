# ITL-1054: pandas values, refuse at write what storage cannot restore — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan
> task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A pandas `DataFrame` / `Series` that storage would change silently (MultiIndex levels
reordered) or could not read back is refused with `ValueError` naming the column or level, by the
value hash and by `python_to_storage`, through one shared check.

**Architecture:** One function in `orcapod/hashing/pandas_value_hash.py` (the module
`logical_types/pandas_type.py` already imports from) takes the Arrow schema storage would write and
the frame, returns the empty frame storage restores for that schema, and raises `ValueError` for
every value storage would not restore. `python_to_storage` (frame and Series) and the hash's
`_values_table` / `_in_canonical_column_order` both call it. No identifier, label, or golden value
changes.

**Tech stack:** pandas 2.2.3 (locked; `<3`), pyarrow `>=22.0.0` (locked 23.0.1).

**Status:** coordinator approved (Q1-Q5, all recommendations; then option B: remove the
`struct_field_order` ignore option). Implemented; draft PR next.

Decisions (coordinator, November 14, 2026): Q1 refuse case 1 and every case-2 shape; the
documented lossy read-backs stay accepted (#339). Q2 matrix: only first write may refuse; read
and second round trip never raise; hash stable; per-position check of columns and index
levels. Q3 the hash refuses everything storage refuses (one shared check). Q4 `pd.ArrowDtype`
decimal left as is; report exactly what is preserved (values exact incl. scale; dtype and
declared precision lost). Q5 write-time conversion errors wrapped into ValueError naming the
part. struct_field_order: removed from both OPTIONS sets and the identity list (B).

---

## Findings (reproduced November 14, 2026)

Scripts: the session scratchpad `repro/` (direct.py, pipeline.py, search.py, classify.py,
probe.py, emptycheck.py); not committed.

pandas 2.2.3; pyarrow 22.0.0, 23.0.1 (locked) and 26.0.1 (newest on PyPI; the lock allows any
`>=22`): **identical results on all three.**

### Case (1): written, read back changed, nothing raises

`pd.MultiIndex.from_tuples([("p", 7), ("q", 8)], names=[1, 0])` reads back as
`[(7, "p"), (8, "q")]`, names `["0", "1"]`: levels reordered. Frame and Series; logical type
directly and through a `PipelineJob` with `InMemoryArrowDatabase` and `DeltaTableDatabase`. The
hash is "stable" (`hash(v) == hash(read(write(v)))`) because the hash describes the restored
form; so `v` hashes equal to a genuinely different frame (the reordered one): a collision of an
identity property (`index_level_order`).

Root cause (pyarrow `pandas_compat._get_index_level_values`): pyarrow fetches level `i` with
`index.get_level_values(i)`, and pandas resolves an integer as a level NAME first. So any
MultiIndex level name equal (by `==`, so `1`, `1.0`, `True`) to another level's position
reorders. Over 1342 accepted name combinations (2 and 3 levels; names from ints, floats,
bools, text, None), 202 reorder; the check "level `i` fetched by `get_level_values(i)` is not
level `i`" predicts every one, with zero disagreements.

### Case (2): written, read raises

Through a `PipelineJob`: the result row is written to the result table (`rdb_v2`), the run's
read-back fails and is reported as a row failure (`RowFailuresWarning`), and the cached row
stays unreadable on every rerun.

Unreadable shapes found (frame and Series alike):

- column (or Series values) of `pd.ArrowDtype`: `list`, `large_list`, `fixed_size_list`,
  `list_view`, `struct`, `map`, `dictionary`, `run_end_encoded`, `dense_union`,
  `fixed_size_binary`;
- index level of `pd.ArrowDtype`: `list`, `large_list`, `fixed_size_list`, `struct`, `map`,
  `dictionary`, `fixed_size_binary`, `float16` ("float16 indexes are not supported");
- column axis (any level of it): categorical, period, timedelta (any unit), interval,
  `datetime.date` objects, `pd.ArrowDtype` timestamp / duration / bool / date32 / decimal.

`schema.empty_table().to_pandas()` (no rows converted) raises exactly when the full read
raises: 1122 of 1122 convertible values agree, on all three pyarrow versions.

### Search: matrix with only the first write allowed to refuse

1026 values (every value of `tests/test_hashing/test_pandas_storage_matrix.py`):

- refused at the first write: 10 (existing refusals);
- read fails: 9 (the column-axis and `ArrowDtype` shapes above);
- second round trip fails: 0; hash unstable: 0;
- read back differs from the original (`assert_frame_equal(check_exact=True)`): 726. Every one
  is a property PR #339 declared not identity because storage does not restore it (hash
  computed on the restored form): `RangeIndex` kind; index dtype (extension / object / string
  -> NumPy or object); index name type (`1` -> `"1"`); `object` column dtype (ints -> `int64`,
  `[1, None]` -> `float64`, datetimes -> `datetime64`); `object` list / dict / tuple / set
  elements (tuples and sets read back as arrays); categorical category dtype; mixed column
  labels -> text; fixed-offset tz class; string storage (`string[pyarrow]` -> `string[python]`);
  `ArrowDtype` string -> `string`; `ArrowDtype` timestamp index -> `DatetimeIndex`;
  `ArrowDtype` decimal -> `object` (precision lost); `attrs` JSON form. Only case (1) is an
  unaccounted change.

### Out of scope, reported (not fixed)

- `pd.ArrowDtype` `string_view`, `binary_view`, `month_day_nano_interval` columns: storage
  writes and reads them, the hash refuses them (`Hashing data of type ... is not supported`).
- An index level of `list_view` / `run_end_encoded` / union / view types raises
  `NotImplementedError` (not `ValueError`) at write; `complex128` raises
  `ArrowNotImplementedError`, sparse `TypeError`.
- `ArrowDtype` decimal precision lost by storage (developer note on ITL-1054: "decide here").

## Design

`require_restorable(schema, frame) -> pd.DataFrame` in `pandas_value_hash.py` (replaces
`pandas_type._require_restorable_axes` and the silent fallback in `_restored_axes`):

1. repeated field names -> `ValueError` (existing);
2. MultiIndex level `i` resolved by name by `frame.index.get_level_values(i)` (pyarrow's own
   call) -> `ValueError` naming the level, its name and the reason ("integer level names that are
   also level positions are read back in another order");
3. `schema.empty_table().to_pandas()` raises -> locate the culprit (column axis level, column,
   index level; each probed with a zero-row frame of that part alone) and raise `ValueError`
   naming it, its dtype and storage's error;
4. other number of columns / index levels -> `ValueError` (existing);
5. distinct labels that read back duplicated -> `ValueError` (existing).

Returns the restored empty frame, which `_restored_axes` reads its axes from.

The hash calls it in `_values_table` and `_in_canonical_column_order`; storage in both
`python_to_storage`s. The bare `"arrow"` descriptor term becomes unreachable (a nested,
dictionary or run-end `ArrowDtype` column is refused first); the grammar text says so. No
digest of any accepted value changes; identifiers and labels unchanged.

Tightened matrix test: only the first write may refuse; read and second round trip must not
raise; hash stable; plus a hash-independent positional check (see the ask).

## Tasks (after the coordinator's reply)

- [x] Task 1: failing tests: `tests/test_hashing/test_pandas_refused_at_write.py` (each shape,
  frame and Series, hash and `python_to_storage`, message names column/level), pipeline test
  (in-memory and Delta: the pod's row fails at write, nothing unreadable is stored), tightened
  matrix. Run, show failures.
- [x] Task 2: `require_restorable` + callers; run targeted tests.
- [x] Task 3: golden suite unchanged; identifier tests unchanged.
- [x] Task 4: docs (`docs/reference/hashing.md` section 10 "What is refused", executed sample),
  `changelog.d/ITL-1054.md`, `design_issues/ITL-1054-<id>.md`.
- [ ] Task 5: ruff, basedpyright, targeted pytest (one process, 3.12); push; draft PR.
