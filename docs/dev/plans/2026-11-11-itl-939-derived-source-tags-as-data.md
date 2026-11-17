# DerivedSource `tags_as_data` Implementation Plan (ITL-939)

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** `DerivedSource(origin, tags_as_data={tag: data_name})` — a root source derived from a
source or a job node that exposes chosen tag columns also as data columns.

**Architecture:** One new mode on the existing `DerivedSource` class, switched on by
`tags_as_data is not None`. In that mode the class reads the origin's rows, appends one Arrow
column per mapping entry (the tag's field renamed, so type, nullability and metadata are kept),
and runs the standard `SourceStreamBuilder`, so provenance is that of any source. The plain
mode (`node.as_source()` with no mapping) is untouched (ITL-956).

**Tech Stack:** PyArrow, existing `SourceStreamBuilder`, `pipeline/serialization.py` registry.

**Design:** `superpowers/specs/2026-11-11-itl-939-tag-and-data-same-name-design.md` section 8
(on branch `eywalker/itl-939-design-spike-let-a-tag-column-and-a-data`, PR #310), answers in 8.10.

**Branch:** `eywalker/itl-939-derived-source-tags-as-data` from `origin/main`. PR "Fixes ITL-939".

---

## Decisions fixed by the developer / coordinator (November 11, 2026)

- Default `source_id` = bare hex of `derivation_hash`, the content hash an operator node doing
  this derivation would have: `hash_object((("DerivedSource", tuple(sorted(tags_as_data.items()))), (origin,)))`,
  origin resolved through `origin.content_hash(hasher)`, hasher from the origin's data context.
  One function (`_derivation_hash`), so adding a readable prefix later is a one-line change.
- Explicit `source_id=` replaces the default everywhere.
- `content_hash`: the `RootSource` rule `(class name, output_schema, source_id)`.
- `pipeline_hash`: schema only (inherited).
- Provenance: fresh, by `SourceStreamBuilder` (own system tags, record ids, tokens).
- Store: like every source. Nothing saved by a job store; `derived.cached(db)` saves.
- Origins: any `RootSource`, or a job node (`get_all_records()`; zero rows before it has run).
- Plain mode: identity `(origin.content_hash(),)`, `source_id` as today. Do not touch.
- Do not touch `GroupBy` (ITL-955). `data_as_tags` is out of scope (a future operator).

## File structure

- Modify `src/orcapod/core/sources/derived_source.py`: the new mode, `to_config`, `from_config`.
- Modify `src/orcapod/core/nodes/function_node.py`, `operator_node.py`: `as_source(tags_as_data=None)`.
- Modify `src/orcapod/pipeline/serialization.py`: register `"derived": DerivedSource`.
- Create `tests/test_core/sources/test_derived_source_tags_as_data.py`.
- Modify `docs/concepts/sources.md` (executed example), `CHANGELOG.md` (`[0.3.0] — unreleased`,
  Added), `DESIGN_ISSUES.md` (TD1 entry), `CLAUDE.md` + `.zed/rules` (one implementation note).

## Behaviour to implement

```python
DerivedSource(
    origin,                      # RootSource | FunctionNode | OperatorNode
    source_id=None,
    tags_as_data=None,           # Mapping[str, str] | None: {tag column: new data column}
    record_id_column=None,       # only with tags_as_data
    **kwargs,                    # label, data_context (default: origin's), config
)
```

Validation at construction (`InputValidationError`, naming the column):
1. each key is a user tag column of the origin (`origin.keys()[0]`);
2. each new name is not a tag or data column of the origin;
3. new names are distinct;
4. a new name does not start with `constants.DATAGRAM_PREFIX` (`_`).
`record_id_column` without `tags_as_data` raises `ValueError` (plain mode has no record ids).

Rows (`_build_stream`): origin table = `origin.as_table()` for a `RootSource`, else
`origin.get_all_records()`; `None` gives the empty table of the origin's schema
(`arrow_utils.make_empty_table`) and is **not cached**, so the source fills in once the node
has run. For each `(tag, name)`: `table.append_column(table.schema.field(tag).with_name(name), table.column(tag))`.
Then `SourceStreamBuilder(self.data_context, self.orcapod_config).build(table, tag_columns=origin tag keys, source_id=self.source_id, record_id_column=...)`.
`keys`, `output_schema`, `iter_data`, `as_table` delegate to that stream.

Config: `to_config(db_registry=None)` returns `source_type: "derived"`, `tags_as_data`,
`record_id_column`, `source_id` (only when given explicitly, else `None`, so a reload recomputes
it from the reloaded origin), `origin` (the origin's `to_config(db_registry=...)`, only when the
origin is a `RootSource`), plus `_identity_config()`. `from_config` rebuilds the origin with
`resolve_source_from_config` and raises `NotImplementedError` when the config has no `origin`
(a node origin, or the plain mode): the job then loads with the source unbound, as today.

## Tasks

### Task 1: failing tests (public interface)

**Files:** Create `tests/test_core/sources/test_derived_source_tags_as_data.py`

- [x] Fixtures: EDI-shaped `session` and `experiment` Arrow tables (`session_start_datetime`
      `timestamp[us, tz=UTC]`, not null; `experiment_index` `uint32`), `ArrowTableSource`s.
- [x] Tests: columns and schema; copy equals tag; exact Arrow field (type, not-null, nullable,
      logical type `Path`, field metadata); fresh provenance (system-tag names differ from the
      origin's, source-id values, tokens); default `source_id` definition and its sensitivity;
      explicit `source_id`; `content_hash` / `pipeline_hash`; table equals the hand recipe's;
      origin untouched; elapsed-time pod + `Join` with `session` (5 rows); `Join` with the
      original; validation errors; sync and async `PipelineJob`; save/load with a file-backed
      `SQLiteTableSource` origin; not reloadable with an `ArrowTableSource` origin; node origin
      before and after run via `node.as_source(tags_as_data=...)`; zero-row origin; `.cached(db)`;
      plain `node.as_source()` identity unchanged; `record_id_column`.
- [x] Run `uv run pytest tests/test_core/sources/test_derived_source_tags_as_data.py -q`;
      expect failures (`TypeError: unexpected keyword argument 'tags_as_data'`). Commit.

### Task 2: implementation

- [x] `derived_source.py` as described; `as_source(tags_as_data=None)` on both nodes (pass the
      node-path `source_id` only in plain mode); registry entry.
- [x] Run the new tests, then `tests/test_core/sources tests/test_pipeline tests/test_hashing`. Commit.

### Task 3: docs, changelog, design issues, agent instructions

- [x] `docs/concepts/sources.md`: extend the `DerivedSource` section with an executed example
      (every printed line as a `# ...` comment). Run `uv run pytest tests/test_docs -q`.
- [x] `CHANGELOG.md` Added entry; `DESIGN_ISSUES.md` TD1 resolved with a **Fix:** note;
      `CLAUDE.md` and `.zed/rules`: the operator / derived-source criterion.
- [x] Commit.

### Task 4: verification and PR

- [x] `uv run ruff format .`, `uv run ruff check .`, `uv run --with basedpyright basedpyright`
      (exit code 0), `uv run pytest tests/` (one suite at a time on this machine).
- [x] Merge `origin/main` if it moved; push; PR against `main` ("Fixes ITL-939"); attach to
      Linear; `orca worktree set --pr`; CodeRabbit rounds; give the coordinator the read-only
      live SpiralDB command; `worker_done`.

## Progress notes

(successor: read this section first)
- Tasks 1-3 done and pushed: tests (47), implementation, docs, CHANGELOG, DESIGN_ISSUES, agent
  instructions. Coordinator confirmed: bare 64-hex default source_id; store behaviour as every
  source (`.cached(db)`).
- Findings to report, not fixed: before a job node has run, the zero-row table's Arrow types
  come from the Python schema (uint32 shows as int64), same as the plain form (ITL-956).
- Remaining: full suite, PR, Linear attach, CodeRabbit, live SpiralDB command, worker_done.
