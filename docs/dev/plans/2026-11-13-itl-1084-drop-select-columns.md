# ITL-1084 drop_columns / select_columns Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `DropColumns` / `SelectColumns` operators and `StreamBase.drop_columns` / `select_columns`
methods that act on tag and data columns in one call, changing nothing that exists.

**Architecture:** One private base class `_TagAndDataColumnsOperator(UnaryOperator)` in
`core/operators/column_selection.py` holds construction (`_canonical_columns`), config, identity,
validation, the sync table rebuild, the schema prediction and the async path; the two subclasses
only say which names of a column group they remove (`_removed`). One module helper
`_require_a_data_column(remaining)` holds the "a stream needs a data column" rule (coordinator:
ITL-1080 will apply it to the existing four). The stream methods normalize their arguments with
one private helper `_column_names(...)` in `core/streams/base.py`.

**Tech Stack:** Python, PyArrow, pytest via `uv run`.

## Decisions (coordinator, 2026-11-13)

- Edge rule 3, option A: when the result would have no data column, both new operators raise
  `InputValidationError` early: at `op(stream)` (`validate_unary_input`) and on the first row in
  `async_execute`. Today's existing four raise late (`ValueError` at iteration) on sync and stay
  silent on async; they are NOT changed here.
- Zero tags after `select_columns` is allowed (a tagless stream iterates fine today).
- Meta (`__`) columns are not carried, on either path (matches ITL-1080, which removes
  `Join._without_meta`, so this branch keeps its own small helper; after ITL-1080 merges it can
  be replaced by `meta_columns.without_meta_columns` / `without_data_meta`).

## Behaviour matched from the existing four

- Sync: `stream.as_table(columns={"source": True, "system_tags": True, "sort_by_tags": False})`,
  drop the removed tag columns, the removed data columns and their `_source_<c>` columns,
  rebuild `ArrowTableStream(table, tag_columns=remaining tags)`. System tags pass through.
- Strict: a name that is neither a tag nor a data column raises `InputValidationError`
  ("Missing columns: ... use strict=False ..."); non-strict ignores it.
- Identity: `(class name, sorted unique names list, strict)`; config `{"columns", "strict"}`.
- Async: own `async_execute`, per-row `tag.drop` / `data.drop`.

## Files

- Modify: `src/orcapod/core/operators/column_selection.py` (base class, two operators, helpers)
- Modify: `src/orcapod/core/operators/__init__.py`, `src/orcapod/pipeline/serialization.py`
- Modify: `src/orcapod/core/streams/base.py` (`_column_names`, two methods)
- Modify: `src/orcapod/protocols/core_protocols/streams.py` (two protocol methods)
- Create: `tests/test_core/operators/test_tag_and_data_columns.py`
- Modify: `tests/test_core/operators/test_async_sync_equivalence.py` (CASES),
  `tests/test_core/operators/test_operator_config.py`, `tests/test_hashing/golden/cases.py`
  (new case `pipeline/tag_and_data_columns`), golden `pipeline.json` (additions only)
- Docs: `docs/concepts/operators.md`, `docs/api/operators.md`, `docs/reference/hashing.md`,
  `CLAUDE.md`, `.zed/rules`, `changelog.d/ITL-1084.md`

## Tasks

- [ ] 1. Write `tests/test_core/operators/test_tag_and_data_columns.py` (public interface):
  argument forms (`'a','b'` == `['a','b']`), rejections (no name, mix, non-string, empty
  collection), drop tag+data, select tag+data, rows equal DropTagColumns / DropDataColumns /
  SelectTagColumns / SelectDataColumns for single-group names, strict/non-strict, no data column
  left raises `InputValidationError` at call time, zero tags allowed, identity spellings,
  prediction vs `unary_static_process(...).output_schema(...)`, source info of kept columns,
  meta not carried (function-pod input). Run: fails (ImportError / AttributeError).
- [ ] 2. Implement operators + stream methods + exports + registry. Run the new test file: pass.
- [ ] 3. Add CASES to async equivalence; config round-trip; run those files.
- [ ] 4. Golden: add case, regenerate, `git diff --stat` must show additions only; run golden test.
- [ ] 5. Docs, CLAUDE.md, .zed/rules, changelog fragment; run doc sample test for operators.md,
  `assemble_changelog.py --check`.
- [ ] 6. ruff format/check, basedpyright exit code; one pytest process on the touched test files.
- [ ] 7. Merge origin/main, push, PR ("Fixes ITL-1084", golden diffstat), Copilot reviewer,
  orca linear attach / worktree set, answer threads, CI green on 3.11/3.12/3.13, worker_done.

## Notes for a successor

(update as work proceeds)
