# DropNulls Operator Implementation Plan (ITL-943)

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A unary operator `DropNulls(columns)` and `stream.drop_nulls(columns)` that keeps
only rows where every named column is non-null and narrows each named column from `T | None`
to `T` (schema and Arrow field).

**Architecture:** One operator class in `src/orcapod/core/operators/drop_nulls.py`. Pure
Arrow (validity mask, `Table.filter`, re-wrap the same arrays under a schema whose named
fields are non-nullable); no Polars. A streaming `async_execute` applies the same
table-level narrowing to each datagram's Arrow table.

**Tech Stack:** pyarrow, pytest, `uv run`.

---

## Design (approved by the coordinator, November 11, 2026)

Coordinator's answer to the STEP 1 `ask`: (b)-(j) approved as proposed. (a) The NAME was the
developer's call; the developer chose **`DropNulls` / `drop_nulls`** on November 11, 2026, and
the operator (first built under a longer working name) was renamed everywhere, with no alias.

- **Argument:** `columns: str | Collection[str]`; a bare `str` is one name. Empty collection is
  `ValueError` at construction. Stored as `self.columns = tuple(sorted(set(names)))`.
- **Row rule:** a row survives only if every named column is non-null.
- **Missing column:** `InputValidationError` at call time (`validate_unary_input`), naming the
  missing names. No `strict` flag. System-tag, source-info, meta and context columns cannot be
  named (reported as missing).
- **Already non-nullable column:** accepted, no-op for that column (idempotent; postcondition
  semantics; a pipeline's validity must not depend on declared upstream nullability).
- **Tag vs data:** one operator; names may be tag or data columns, mixed.
- **Nested:** only top-level validity is tested (null list / null struct; not an empty list,
  list items, struct children, or NaN). Only the top-level field becomes non-nullable.
- **Identity:** `identity_structure() == ("DropNulls", self.columns)`; same for content and
  pipeline hash. `"x"`, `["x"]`, `("x",)`, `{"x"}`, `["x", "x"]` are one identity; order is
  irrelevant.
- **Hash effects (measured):** nullability is in the Arrow schema hash. A surviving row's
  `Data` content hash changes iff a named data column was nullable; its `Tag` content hash
  changes iff a named tag column was nullable. The new hash equals the hash the same row has in
  a stream that declared the column non-nullable from the start. System-tag names and values
  and source-info tokens are unchanged. No existing golden value moves.
- **System tags:** name-preserving. Source-info columns untouched. Row order preserved.
- **Schema prediction:** upstream schema with `None` removed from each named column's type.
  Verified against `unary_static_process(stream).output_schema(...)`. Zero-row results keep the
  narrowed types.
- **Async:** own streaming `async_execute`; nullness read from each datagram's Arrow table,
  surviving rows rebuilt from that table. Added to `test_async_sync_equivalence.py`.
- **Arrow:** every column large-normalized (`keep_null=True`), nothing else changed.
- **Save/load:** `to_config()["config"] == {"columns": [...]}`; registered in
  `pipeline/serialization.py`.
- **Boundary note:** the operator reads validity only, never values. Add one line to the
  operator table in `CLAUDE.md` and `.zed/rules`; put the wording in the PR for the developer.

Extra tests asked by the coordinator: NaN is not null; a nullable tag narrowed and then
joined; a Spiral-style source with nullable key columns narrowed to `T`, enabling a `Join`
with a source whose keys are `T`.

## Files

- Create: `src/orcapod/core/operators/drop_nulls.py` (the operator)
- Modify: `src/orcapod/core/operators/__init__.py` (export), `src/orcapod/operators/...`
  (public re-export, wherever `PolarsFilter` is listed)
- Modify: `src/orcapod/core/streams/base.py` (`drop_nulls` method),
  `src/orcapod/protocols/core_protocols/streams.py` (protocol method)
- Modify: `src/orcapod/pipeline/serialization.py` (operator registry)
- Create: `tests/test_core/operators/test_drop_nulls.py`
- Create: `tests/test_pipeline/test_drop_nulls_job.py` (Delta store, cache hits)
- Modify: `tests/test_core/operators/test_async_sync_equivalence.py` (two cases)
- Modify: `tests/test_core/operators/test_operator_config.py` (round trip)
- Modify: `tests/test_hashing/golden/cases.py` (new case `pipeline/drop_nulls`), then
  regenerate; the diff of `tests/test_hashing/golden/data/pipeline.json` must only add lines
- Modify: `docs/concepts/operators.md`, `CHANGELOG.md`, `DESIGN_ISSUES.md`, `CLAUDE.md`,
  `.zed/rules`

## Operator sketch

```python
class DropNulls(UnaryOperator):
    def __init__(self, columns: str | Collection[str], **kwargs: Any) -> None:
        names = (columns,) if isinstance(columns, str) else tuple(columns)
        if not names:
            raise ValueError("DropNulls needs at least one column name.")
        self.columns: tuple[str, ...] = tuple(sorted(set(names)))
        super().__init__(**kwargs)

    def _narrow_table(self, table: pa.Table) -> pa.Table:
        """Drop rows with a null in a named column present in ``table``; flag those
        columns non-nullable; normalize every column to its large type."""
        named = [n for n in self.columns if n in table.column_names]
        mask = None
        for name in named:
            valid = pc.is_valid(table.column(name))
            mask = valid if mask is None else pc.and_(mask, valid)
        if mask is not None:
            table = table.filter(mask)
        table = arrow_utils.normalize_table_to_large_types(table)   # keep_null behaviour checked
        schema = table.schema
        for name in named:
            i = schema.get_field_index(name)
            schema = schema.set(i, schema.field(i).with_nullable(False))
        return pa.Table.from_arrays(table.columns, schema=schema)   # same arrays, new flags
```

Sync: `_narrow_table(stream.as_table(columns={"source": True, "system_tags": True,
"sort_by_tags": False}))` wrapped in `ArrowTableStream(table, tag_columns=stream.keys()[0])`.

Async: per `(tag, data)`: narrow `tag.as_table(columns={"system_tags": True})` and
`data.as_table(columns={"source": True, "meta": True})`; if either comes back with zero rows
the row is dropped; otherwise emit `Tag(table, data_context=tag.data_context_key)` and
`Data(table, data_context=data.data_context_key)`. If row-by-row equivalence cannot be had,
fall back to the inherited barrier path and report it.

## Tasks

- [ ] **1. Tests first** (`tests/test_core/operators/test_drop_nulls.py`): rows with nulls
  dropped and order preserved; `output_schema` and `as_table` narrowed; other columns' types
  and nullability untouched; multiple columns; tag column; mixed tag and data; nested (list
  with null items, struct with null children survive; null list / struct dropped; nested
  flags untouched); NaN survives; already non-nullable (hashes untouched); missing column
  (`InputValidationError`), system/source column names refused; empty `columns`
  (`ValueError`); zero surviving rows keep the narrowed schema; logical types (`Path | None`,
  `np.ndarray | None`); identity (str/list/tuple/set/duplicates/order equal; different names
  differ); narrowed row hashes equal those of a source declared non-nullable; system tags and
  source info unchanged; schema prediction vs `unary_static_process(...).output_schema(...)`
  (comment at the non-public use); pod with a non-optional parameter refused before, runs
  after; `Join` after it; nullable tag narrowed then joined with a `T`-keyed source;
  `stream.drop_nulls` equals the operator. Run: all fail with `ImportError`.
- [ ] **2. More tests:** two `Case`s in `test_async_sync_equivalence.py` (`drop_nulls`
  on `nul`, `drop_nulls_tag_and_data`), config round trip in `test_operator_config.py`,
  `tests/test_pipeline/test_drop_nulls_job.py` (PipelineJob with `DeltaTableDatabase`:
  run, results; second job on the same store makes no new pod calls). Commit (WIP, failing).
- [ ] **3. Implement** the operator, exports, stream method, protocol method, registry.
  Run the new tests until green. Commit.
- [ ] **4. Golden:** add `@case("pipeline", "drop_nulls")` to `cases.py` (a source with a
  nullable data column; summaries of the operator on one and on two columns), regenerate,
  check `git diff --stat` and that the diff has no removed or changed line. Commit.
- [ ] **5. Docs:** `docs/concepts/operators.md` section with a runnable sample (drop null rows,
  then a pod with a non-optional parameter); convenience-method list; `CHANGELOG.md`
  `[0.3.0] — unreleased` Added; `DESIGN_ISSUES.md` entry appended; `CLAUDE.md` + `.zed/rules`
  (project layout line, operator boundary line). Run `tests/test_docs`. Commit.
- [ ] **6. Verify:** `uv run ruff format .`, `uv run ruff check .`,
  `uv run --with basedpyright basedpyright` (exit code), `uv run pytest tests/` (one Python
  version at a time, no xdist). Merge `origin/main` if it moved and rerun.
- [ ] **7. PR** against `main` (`Fixes ITL-943`), link with `orca linear attach` and
  `orca worktree set`, CodeRabbit round, `worker_done`.

## Notes / progress log

(Updated as work proceeds, for a successor.)

- November 11: tasks 1-5 done and pushed. Operator in
  `src/orcapod/core/operators/drop_nulls.py`; tests in
  `tests/test_core/operators/test_drop_nulls.py`,
  `tests/test_pipeline/test_drop_nulls_job.py`, two `Case`s plus a tagless test in
  `test_async_sync_equivalence.py`, `TestDropNullsConfig` in `test_operator_config.py`.
- Golden: cases `pipeline/drop_nulls` and `pipeline/drop_nulls_job` appended to
  `cases.py`; `pipeline.json` gained 128 lines, none removed or changed.
- The streaming async path is exactly equivalent row by row (including Data meta columns, so
  no xfail was needed); no fallback to the barrier path.
- Large normalization uses `normalize_to_large_types(..., keep_null=True)` per field with
  `restore_column_types` (the table-level helper maps `null` to `large_string`).
- NAME decided (developer, November 11, 2026): `DropNulls` / `drop_nulls`. Renamed class,
  module (`drop_nulls.py`), stream method, registry key, export, docs, CHANGELOG, DESIGN_ISSUES,
  CLAUDE.md, `.zed/rules`, tests (files too), golden case names and this plan file. The class
  name is in `identity_structure`, so the two golden cases this PR added were regenerated with
  it (additions only against main; no existing entry moved).
- Observation, not fixed (out of scope): a `PipelineJob` saved with an in-memory
  `ArrowTableSource` and loaded again gives operator nodes whose pipeline hash differs from
  the saved job's (same for `MapData`), because the source is not reconstructable.

