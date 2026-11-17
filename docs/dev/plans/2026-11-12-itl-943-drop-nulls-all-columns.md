# DropNulls No-Argument Form Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `DropNulls()` / `stream.drop_nulls()` with no columns means every user tag column and
every user data column of the input stream (developer, November 12, 2026: "plain no argument =
drop all is fair and as applied to both tags and data columns").

**Architecture:** `columns` becomes `str | Collection[str] | None = None`. `self.columns` is
`None` for the no-argument form and is never replaced by resolved names: the names are resolved
from the input (`stream.keys()` on the sync and schema paths, `tag.keys()` + `data.keys()` per
row on the async path) each time, so one operator object works on any stream. The table rule
(`_narrow_table`) takes the names as an argument. Identity for the no-argument form is the
fixed value `("DropNulls", None)`; the named form stays `("DropNulls", (sorted names))`, so no
existing hash moves.

**Tech Stack:** PyArrow, pytest, uv.

**PR:** #311, branch `eywalker/itl-943-operator-that-drops-rows-where-given`. One commit, one push.

---

## Decisions

1. `columns=None` (the default) = all user tag and data columns. An explicitly **empty**
   collection still raises `ValueError`: "no argument" and "an empty list" are different
   intents (an empty list is usually a computed list that came out empty, and silently turning
   it into "all" would drop rows nobody asked to drop). Stated in the docstring and tested.
2. Identity: `identity_structure()` is `("DropNulls", None)` for the no-argument form. `None`
   cannot collide with any tuple of names. `to_config()` gives `{"columns": None}`, and
   `from_config` round-trips through `cls(columns=None)`. `DropNulls()` and
   `DropNulls([every column])` give the same output rows and schema on a stream; their
   operator identities (content hash, pipeline hash, and so the output stream's hashes) differ.
3. "All" is `stream.keys()` (default column config): user tag and data columns only. System
   tags, source-info, context and meta columns are never tested and never narrowed.
4. Schema prediction resolves the names from `stream.keys()`, not from the schema it was asked
   for (which may include system tag columns).
5. Async path: names per row are `(*tag.keys(), *data.keys())`.

## Files

- `src/orcapod/core/operators/drop_nulls.py` — the operator.
- `src/orcapod/core/streams/base.py`, `src/orcapod/protocols/core_protocols/streams.py` —
  `drop_nulls(columns=None, label=None)`.
- `tests/test_core/operators/test_drop_nulls.py` — new section "no argument: every column".
- `tests/test_core/operators/test_async_sync_equivalence.py` — `Case("drop_nulls_all", ...)`
  and the tagless test extended.
- `tests/test_core/operators/test_operator_config.py` — config round trip of the no-argument form.
- `tests/test_hashing/golden/cases.py` + `data/pipeline.json` — new case
  `pipeline/drop_nulls_all` (additions only).
- `docs/concepts/operators.md`, `changelog.d/ITL-943.md`, `design_issues/ITL-943-DN1.md`,
  `CLAUDE.md`, `.zed/rules`.

## Tasks

- [ ] **1. Failing tests** in `tests/test_core/operators/test_drop_nulls.py` (public
  interface: `DropNulls()(stream)` and `stream.drop_nulls()`):
  - rows: `DropNulls()(readings())` keeps only id 1 (x, y, note all present);
  - schema: every tag and data column `T`, Arrow fields non-nullable; system tag and
    source-info fields unchanged, including when a source-info value is null;
  - equals `DropNulls([all names])` in table (`ALL` config), `output_schema`, row hashes;
    operator and stream hashes differ;
  - one object on two streams with different columns;
  - identity: `DropNulls().columns is None`, `DropNulls(None)` same hash, differs from any
    named form;
  - `DropNulls([])`, `DropNulls(())`, `DropNulls(set())` still raise `ValueError`;
    `stream.drop_nulls([])` too;
  - schema prediction vs `static_process(...).output_schema(...)` for `{}`, `system_tags`,
    `source+system_tags` (non-public on purpose, commented);
  - zero rows keep the types; an all-non-nullable stream is unchanged; idempotent;
  - tagless stream; stream method equals operator.
  Run `uv run pytest tests/test_core/operators/test_drop_nulls.py -q` and see them fail
  (`TypeError: missing 1 required positional argument`).
- [ ] **2. Implement** in `drop_nulls.py`:
  - `__init__(self, columns: str | Collection[str] | None = None, **kwargs)`;
    `self.columns: tuple[str, ...] | None`;
  - `_target_columns(tag_columns, data_columns) -> tuple[str, ...]`: `self.columns` or
    `(*tag_columns, *data_columns)`;
  - `_narrow_schema(schema, names)`, `_narrow_fields(table, names)`,
    `_narrow_table(table, names)` (static rule functions of the names);
  - `validate_unary_input`: nothing to check when `columns is None`;
  - `unary_output_schema`, `unary_static_process`: names from `stream.keys()`;
  - `async_execute`: names from `tag.keys()` and `data.keys()`;
  - `identity_structure`, `to_config`, docstrings.
  Stream method and protocol: `columns: str | Collection[str] | None = None`.
- [ ] **3. Async equivalence:** add `Case("drop_nulls_all", lambda: DropNulls(), ...)` and a
  no-argument assertion in `test_drop_nulls_without_tag_columns`.
- [ ] **4. Config test:** `DropNulls().to_config()["config"] == {"columns": None}`, round trip
  through `from_config` and `resolve_operator_from_config`.
- [ ] **5. Golden:** new case `pipeline/drop_nulls_all` in `cases.py`:
  `operator[*].content_hash`, `stream_summary(DropNulls()(_nullable_readings()))`, and the
  method form. Regenerate; `git diff --stat` on `data/` must show insertions only.
- [ ] **6. Docs:** `docs/concepts/operators.md` executed sample of the no-argument form with a
  `notes` column that is legitimately empty, the plain warning (drops every row with a null in
  ANY tag or data column; SpiralDB stores every column nullable; naming the columns is the
  precise form). Update `changelog.d/ITL-943.md`, `design_issues/ITL-943-DN1.md`, `CLAUDE.md`
  and `.zed/rules` (in step).
- [ ] **7. Verify:** `uv run ruff format .`, `uv run ruff check .`,
  `uv run --with basedpyright basedpyright` (exit code), operator + docs + golden tests, full
  suite once on 3.12 (`uv run --python 3.12 pytest tests/`).
- [ ] **8. Ship:** merge `origin/main` if it moved, one commit, one push, CI green, CodeRabbit
  review of the new head, every review thread (GraphQL `reviewThreads`) and review body
  answered, `worker_done` with the reviewed sha.

## Notes / progress log

- November 12: plan saved; work started.
- Tasks 1-6 done. Finding while extending the async matrix: a stream built straight from a
  table lists its `__` meta columns among `keys()` data columns (ITL-846), so the sync path's
  "all" tested and narrowed meta columns and the async path did not
  (`test_async_meta_equals_sync[drop_nulls_all]` failed). `_target_columns` now leaves out
  names with `constants.META_PREFIX`; `test_no_argument_leaves_meta_columns_alone` pins it.
- Golden: case `pipeline/drop_nulls_all` appended; `pipeline.json` gained 20 lines, none
  removed or changed. Identity of the no-argument form: `("DropNulls", None)`; its content
  hash and pipeline hash are equal (`operator[all].*` in the golden file).
