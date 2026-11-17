# ITL-1081: every stream gets run(), which returns a StreamView — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every stream kind has an argument-less `run()` that computes the whole stream (the
work `as_table()` does, through the same path and cache) and returns a `StreamView` of all
the computed rows; `StreamHead`/`HeadStatus` become `StreamView`/`ViewStatus`; `head()` and
the preview still never compute.

**Architecture:** `StreamBase.run()` calls `self.as_table()` (default configuration) and
wraps the result as a `StreamView` (status `ROWS`, all rows). Kinds with their own
semantics override: job nodes keep today's `run()` body then return the view of
`as_table()`; side-effect streams return a view of the rows that call delivered (their
`head()` stays rowless). `DynamicPodStream`'s old `run(*args, **kwargs)` becomes the private
`_refresh()` used by `as_table`/`iter_data`. `SourceNodeBase` (not a `StreamBase`) gets the
same `run()`. `StreamView` gains `head(n)`, `tail(n)`, `sort_by_tags()` (pending coordinator
approval of the method proposal), each returning a new view with an honest caption.

**Tech Stack:** Python 3.12 (`uv run`), PyArrow, Polars, pytest.

## Decisions (coordinator, November 13, 2026)

- `run()` takes NO arguments on any stream kind (developer correction).
- Uniform rule: `run()` = the full `as_table()` work, sources included: `PollingSource`
  polls, `CachedSource` reads and updates its cache, a table-backed source (Delta, SQLite,
  Postgres, SpiralDB) reads its whole table into memory (docstring must say so, and that
  `head()` is the bounded read).
- Blueprint nodes (`FunctionNode`, `OperatorNode`, `SourceNode`) raise what `as_table`
  raises. `SideEffectNode` (blueprint) delivers standalone, as its `as_table` does.
- Job nodes (`FunctionJobNode`, `OperatorJobNode`): today's body, then the view.
- Side-effect streams: `run()` delivers exactly as `as_table()` does (each call delivers
  again; docstring says so); returns a view of that call's rows; `head()` stays rowless.
- `StreamHeadProtocol` -> `StreamViewProtocol`, no alias. `head_of`, `check_head_size`
  keep their names (the method is still `head`).
- Captions: NOT_COMPUTED -> `not computed; run() computes it`; polling
  `nothing polled yet; run() polls it`; cached `not read yet; run() reads and updates the
  cache`; side effects `not run; run() delivers its side effects; displaying never delivers
  side effects`.
- `PipelineJob.run()` unchanged.

## Stream kinds and what run() does

| Kind | run() |
| -- | -- |
| ArrowTableStream (and `_RowsOfStream`) | base: as_table(), view of all rows |
| ArrowTableSource, Dict/List/DataFrame/CSV sources | base (held table) |
| DeltaTableSource, DBTableSource (SQLite, Postgres), SpiralDBTableSource | base: full read |
| PollingSource | base: polls and commits |
| CachedSource | base: reads + updates cache |
| DerivedSource, SourceProxy | base (delegate's as_table) |
| FunctionPodStream, cached FunctionPodStream | base: computes, cached |
| DynamicPodStream | base: `as_table` -> `_refresh` (recomputes if stale) |
| FunctionNode / OperatorNode (blueprints) | raise PipelineJobRequiredError |
| FunctionJobNode / OperatorJobNode | today's run body, then view of as_table() |
| SideEffectPodStream / SideEffectNode / SideEffectJobNode | deliver (as as_table), view of those rows |
| SourceNode / SourceJobNode (SourceNodeBase) | UnboundSourceError / bound source's as_table |

## Files

- Rename: `src/orcapod/core/streams/head.py` -> `src/orcapod/core/streams/view.py`
  (`StreamView`, `ViewStatus`, `run()` view constructor, view methods).
- Modify: `core/streams/base.py` (run), `core/streams/__init__.py`,
  `protocols/core_protocols/streams.py` + `__init__.py` (run, StreamViewProtocol),
  `core/operators/static_output_pod.py` (_refresh), `core/nodes/function_node.py`,
  `core/nodes/operator_node.py`, `core/nodes/source_node.py`, `side_effects.py`,
  every source with head/as_table (docstrings, imports), `core/function_pod.py`,
  `core/cached_function_pod.py`.
- Tests: rename imports in existing head tests; new `tests/test_core/streams/test_run.py`.
- Docs: `docs/concepts/streams.md`, README / getting-started if they show the preview,
  `CLAUDE.md`, `.zed/rules`, `changelog.d/ITL-1081.md`, `design_issues/ITL-1081-RV1.md`.

## Tasks

### Task 1: Rename StreamHead -> StreamView (mechanical)
- [ ] `git mv src/orcapod/core/streams/head.py src/orcapod/core/streams/view.py`
- [ ] `rg -l` + sed: `StreamHeadProtocol`->`StreamViewProtocol`, `StreamHead`->`StreamView`,
      `HeadStatus`->`ViewStatus`, `core.streams.head`->`core.streams.view` across src, tests,
      docs (not CHANGELOG.md, not other issues' changelog/design-issue/plan/spec files).
- [ ] NOT_COMPUTED_NOTE and the polling/cached/side-effect notes per Decisions; update
      tests pinning those captions.
- [ ] Run the head tests (`tests/test_core/streams`, `tests/test_pipeline/test_node_head*.py`).
- [ ] Commit `refactor(streams)!: rename StreamHead to StreamView, HeadStatus to ViewStatus`.

### Task 2: Failing tests for run() (TDD)
`tests/test_core/streams/test_run.py`, public interface, one fixture per kind:
- run() exists on every kind; signature `()` on StreamBase subclasses and SourceNodeBase
  (protocol conformance via `inspect.signature` over every StreamBase subclass).
- before run(): `head()` has no rows where it had none (function pod stream, operator
  stream, polling source, cached source).
- `v = s.run()`: `isinstance(v, StreamView)`, `v.as_table().equals(s.as_table())`,
  `s.head(n).table.equals(v.head(n).table)` once view methods land (until then
  `v.table.slice(0, n)`).
- call counting: function called once per row across run() + as_table() + run().
- operator stream after upstream change: run() recomputes as as_table() does.
- side-effect stream: each run() delivers again; view has the rows; head() rowless.
- job nodes: run() returns the view; loaded node raises as before.
- blueprint nodes raise PipelineJobRequiredError / UnboundSourceError.
- [ ] Run, confirm failures (AttributeError / None returned).

### Task 3: Implement run()
- [ ] `StreamView.of_run(stream, table, tag_columns, origin)` classmethod (all rows, ROWS).
- [ ] `StreamBase.run()`; `DynamicPodStream.run` -> `_refresh`; job node overrides;
      side-effect overrides; `SourceNodeBase.run()`; protocol `run()`.
- [ ] Docstrings per kind (table-backed sources: whole table read; side effects).
- [ ] Tests pass; commit `feat(streams)!: every stream has run(), returning a StreamView`.

### Task 4: StreamView methods (after coordinator approval)
- [ ] head(n), tail(n), sort_by_tags(), caption fields, truncated display.
- [ ] Tests for captions and display; commit.

### Task 5: Docs, rules, changelog, design issue
- [ ] streams.md section "Computing a stream outside a job"; README/getting-started.
- [ ] CLAUDE.md + .zed/rules one rule.
- [ ] `changelog.d/ITL-1081.md` (Added; Breaking changes and migration).
- [ ] `design_issues/ITL-1081-RV1.md`; `uv run python scripts/design_issues.py --check`;
      `uv run python scripts/assemble_changelog.py --check`.
- [ ] Doc sample tests; golden suite unchanged; ruff, basedpyright.

### Task 6: PR
- [ ] merge origin/main, push, PR (create-pr skill), 'Fixes ITL-1081', title marked `!`.
- [ ] CodeRabbit rounds, CI green on 3.11/3.12/3.13, worker_done.

## Progress notes
- 2026-11-13: survey done, decisions above approved; StreamView method proposal sent.
- 2026-11-13: developer correction: run() takes no arguments; view methods head/tail/
  sort_by_tags approved (no with_columns in v0.3); a cut view displays all rows, an uncut
  long view first 5 + last 5. Pass-through operator staleness is ITL-1083 (strict xfail
  test only). Display marks tag columns: text `*name` + "* = tag"; HTML bold accent
  #2a7fb8 + rule after the last tag column + "bold = tag", by position via HTMLParser
  post-processing of Polars' HTML (COLUMN_ROLE_MARKS is the one mapping).
- Tasks 1-5 done; next: PR, CodeRabbit, CI.

## CodeRabbit round 1 (review 5900666998 of 4a4f7c94) — plan, awaiting coordinator

| # | Finding | Verdict (evidence) | Action |
| -- | -- | -- | -- |
| a | streams.md:192 blanket "compute nothing" after run() | valid: side-effect stream delivers again, polling source polls again, SpiralDB reads again | reword streams.md:191-195 and the same claim in `StreamBase.run` docstring (streams/base.py:491-493) and `view.py:153` |
| b | FunctionJobNode.run discards rows when input timestamp unknown | reproduced (no DB: FunctionJobNode over FunctionPodStream over a pass-through select over FunctionPodStream -> run() 0 rows); with the ITL-1083 fix (f652d737) applied, 25 rows: it is ITL-1083 | no code change; reply pointing to ITL-1083 |
| c | ArrowTableStream run() says "computed rows", head() "rows" | valid; also FunctionJobNode (run "computed", head "rows", also after job.run) and OperatorJobNode in REPLAY / after job.run's execute (run "computed", head "rows") | `ArrowTableStream._RUN_ORIGIN = "held"`; FunctionJobNode.run origin "held" (head's noun for in-memory rows); OperatorJobNode.run origin = origin of its kept output stream's head; test: run().head(3).header() == head(3).header() for every kind + those modes |
| d | nitpick: docstrings on 12 test helpers | valid | add |
| - | LanguageTool "again" x2 streams.md:165; markdownlint MD037 plan line 52 | trivial | reword; backtick |
| - | pre-merge "out of scope" (tag marks, tail, sort_by_tags) | declined: approved by developer/coordinator on ITL-1081 (progress notes) | say so in top-level comment |
- 2026-11-14: round 1 approved as planned (coordinator): (b) also a second strict xfail
  (job node run() over a pass-through chain; same cause, different failing path: the job
  node's stale-clear in as_table; XPASSes with f652d737). Fixes committed; replies next.
- 2026-11-14: round 2 (review of 39dd1433): one Major, OperatorJobNode.run stamped before
  its lazy output was materialized (no DB / OFF) -> stale after run(). Approved fix: stamp
  again after as_table() only when this call computed. Test: lazy_input (no DB, OFF).
