# ITL-834: Index and Pick must preserve input Arrow types — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan
> task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** After `Index` or `Pick`, every column the operator does not touch keeps the exact Arrow
type it had on input (tags and data, empty and non-empty results), and the extracted column's
Arrow type is derived from the input column's Arrow type instead of being inferred from Python
values.

**Rule (developer, 2026-11-09):** whatever Arrow types the inputs have are what is respected.
Inferring an Arrow type from Python types is reserved for when no Arrow type is known.

## Reproduction (main @ 07e5a2ea)

Source: tag `tg: int16`; data `i8: int8, i16: int16, f32: float32, s: string, ls: large_string,
bn: binary, l8: list<int8>, p: orcapod.path, lst: list<int8>, d: dict[str,int]` (non-nullable).

| Path | Result |
|---|---|
| `Index("lst", 0)` (in place) | untouched columns keep width; `lst` becomes `int64` (inferred from `int`), moved to last position |
| `Index("lst", 0, out="x")` | `i8/i16 -> int64`, `f32 -> double`, `l8/lst -> large_list<int64>`, `x: int64`; content hash of untouched data changes |
| `Index("lst", 5)` (all skipped) | tag `tg: int16 -> int64` and every data column widened |
| `Pick` | identical pattern for in place / `out=` / all skipped |

Layout-only normalization (`string -> large_string`, `binary -> large_binary`,
`list -> large_list`) happens in `Datagram._init_from_table` for every operator and is out of
scope per the issue (no hash change). Tests compare against the large-normalized input type.

## Root cause

1. `Datagram.with_columns` and `Datagram.update` (`src/orcapod/core/datagrams/datagram.py`)
   take the Arrow-native path only when the Arrow table is loaded **and** the dict cache is not.
   `Index`/`Pick` read `data[column]` first (which loads the dict cache), so `with_columns`
   falls into the dict path, which sets `_data_table = None` and later rebuilds the whole table
   from the Python schema — every narrow width is lost. `update` has the same defect.
2. The Arrow path of `with_columns` passes a partial `column_types` as the full `python_schema`,
   which silently drops new columns not named in `column_types` (latent; no src caller hits it).
3. `Index`/`Pick` pass the Python element type (`int`) for the extracted column, so it is inferred
   as `int64` even when the input is `list<int8>`.
4. The all-skipped path in `Index`/`Pick` builds the empty table from the predicted *Python*
   schema via `python_dicts_to_arrow_table`, so every column (tags included) is re-inferred.

## Design

- **Datagram (root fix):** `with_columns` and `update` operate on the Arrow table whenever it is
  loaded (regardless of the dict cache); existing columns keep their Arrow fields verbatim; only
  genuinely new columns get types, taken from `column_types` when given, otherwise inferred per
  column from the value. The dict cache is invalidated after the change. Dict-only datagrams keep
  their current Python-schema behaviour.
- **Extracted column type:** gate decision — see "Decision requested". Recommended: widen
  `with_columns(column_types=...)` to accept `pa.DataType` entries as well as Python types;
  Arrow entries are used verbatim.
- **Index/Pick:** derive the extracted Arrow type from the row's Arrow field
  (`list/large_list/fixed_size_list -> value_type`; `map -> item_type`;
  `list<struct<key,value>> -> value field type`; logical-type extension -> the element
  logical type's Arrow type). In-place replacement keeps the column's position (`select`
  original order after drop + add). The all-skipped path builds the empty table from the input
  stream's Arrow schema (large-normalized) with only the target column's type replaced.

## Decisions (coordinator, 2026-11-09)

- Option A approved: `column_types` accepts `pa.DataType` entries on `Datagram`, `Data` and
  `DatagramProtocol.with_columns` (protocol signature + docstring only).
- Gate #2 approved, each in its own commit with its own tests:
  - **Meta:** `copy(include_cache=False)` (used by every mutator) dropped the meta Arrow
    table, so narrow meta columns widened on any mutation. Carry it; meta mutators keep
    untouched meta fields verbatim.
  - **Stale Arrow schema:** dict-path `with_columns`/`select`/`drop`/`rename` copied the
    cached `_data_arrow_schema`, so `arrow_schema()` disagreed with `as_table()` (and
    `FunctionPodStream.as_table`, which builds tables from `arrow_schema()`, could drop a
    column). Reset it.

## Commits

1. `fix(datagram)`: reset cached Arrow schema in dict-path mutators.
2. `fix(datagram)`: `with_columns`/`update` keep known Arrow types; `column_types` accepts
   Arrow types.
3. `fix(datagram)`: keep meta Arrow types through every mutator.
4. `fix(operators)`: Index/Pick derive the extracted type, keep position, typed empty path.
5. `docs`: plan + DESIGN_ISSUES DG1.

## Tasks

- [ ] 1. Failing tests `tests/test_core/operators/test_index_pick_arrow_types.py`: Index and Pick,
      in place / `out=` / all skipped / `fail_on_miss=True` (raises) ; untouched tag+data Arrow
      types equal large-normalized input types; extracted type pinned (`int8` from `list<int8>`,
      value type from dict); per-row content hash of untouched data equals input's; materialized
      Python schema equals `output_schema()`; streaming (`async_execute`) path too.
- [ ] 2. Failing datagram tests `tests/test_core/datagrams/test_arrow_type_preservation.py`:
      `with_columns`/`update` on an Arrow-backed datagram after the dict cache is loaded keep
      `int8`/`float32`/`list<int8>`; partial `column_types` keeps all new columns.
- [ ] 3. Fix `Datagram.with_columns` / `update` (+ `Data.with_columns` signature).
- [ ] 4. Fix Index/Pick extracted type, in-place position, empty path.
- [ ] 5. Audit callers of datagram mutation methods across `src/`; record per caller.
- [ ] 6. Before/after probe (main vs branch): sources, FunctionPod, Join, Batch, GroupBy,
      PipelineJob round trip through a DB — compare schemas and content hashes.
- [ ] 7. Full suite on 3.11/3.12/3.13; ruff + basedpyright on changed files; DESIGN_ISSUES entry.
- [ ] 8. Rebase, PR ("Fixes ITL-834", breaking-change note), worker_done.

## Review round: operators use the stream's context (developer, 2026-11-10)

Developer comments on PR 279 (`index.py:67`, `index.py:138`): `Index`/`Pick` cached
`self._converter = contexts.resolve_context(stream.data_context_key).type_converter`,
re-implementing context resolution that `stream.data_context` already does. Approved rule:
operators use the stream's context only, and nothing in `Index`/`Pick` reads a converter or
`self.data_context` after validation.

`async_execute` has no stream, and `DataProtocol` exposes no converter. So the converter is used
once, in `validate_unary_input`, to resolve the fallback Arrow type of the extracted element (used
only for logical input columns such as `list[Path]` or `dict[str, Path]`). Only that resolved type
is cached, the same way `_output_type` is. If the eager conversion fails, validation does not
raise. The cached type stays `None`, and a `ValueError` naming the operator, column and output
type is raised only where the fallback is actually needed, which keeps the previous lazy
behaviour.

- [x] Failing tests: Pick on `dict[str, Path]` (logical column), sync and async; Index on
      `list[Path]`, async; deferred conversion error (validation passes, non-logical rows work,
      logical rows raise a clear `ValueError`); no converter/context held after validation, and
      the row-time code paths never touch a converter or `data_context`.
- [x] Refactor `Index` and `Pick`: drop `_converter`, its assert, `resolve_context` and the
      `contexts` / `TypeConverterProtocol` imports if unused.
- [x] ruff, basedpyright (exit code), suite on 3.11/3.12/3.13; one commit; one push; reply on
      both threads (do not resolve them).
