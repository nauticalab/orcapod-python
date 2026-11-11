# ITL-828 Polars expression identity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `stream.polars_filter(<pl.Expr>)` gets a stable `pipeline_hash()`/`content_hash()`, so it
works upstream of a join and inside a `PipelineJob`.

**Architecture:** Design in `superpowers/specs/2026-11-11-itl-828-polars-expr-identity-design.md`
(Option C, approved by the coordinator and confirmed by the developer on 2026-11-11; the PR
is ready for review). A strict parser turns `expr.meta.serialize(format="json")` into an
Orcapod-owned canonical tree; the hash is `ContentHash("polars_expr_v1", sha256(canonical
JSON))`. A `PolarsExprHandler` registered for `pl.Expr` returns it, so `PolarsFilter`'s
identity structure keeps its shape. `PolarsFilter.predicates` is normalized to a tuple.

**Tech Stack:** Polars (>=1.36, locked 1.41.2), the semantic hasher handler registry.

Coordinator decisions (2026-11-11): (a) normalize predicates to a tuple, confirm no golden
moves; (b) keep the subset exactly as proposed, do not grow it (so `pl.Series` literals in
`is_in` are rejected: the proposal only has Python-list literals); (c) pin canonical JSON for
a corpus + golden hash entries (additions only); test the parser against Polars 1.36.1 and
1.41.2; docs state the supported subset and what the error tells the user to do; PR DRAFT
saying the identity design awaits developer confirmation.

Small deviation from the proposal text, recorded in the spec: the typed boolean literal is
spelled `{"boolean": ...}` to match the PR #286 dtype name.

Unsupported expressions raise lazily (when the identity is needed), so an in-memory
expression filter that is never hashed keeps working exactly as today.

---

## Files

- Create `src/orcapod/hashing/semantic_hashing/polars_expr.py`: `polars_expr_canonical(expr)`,
  `polars_expr_content_hash(expr)`, the strict JSON parser, error message.
- Modify `src/orcapod/hashing/semantic_hashing/builtin_handlers.py`: `PolarsExprHandler`,
  registration in `register_builtin_python_type_handlers`.
- Modify `src/orcapod/contexts/data/v0.1.json`: registry entry + changelog line.
- Modify `src/orcapod/core/operators/filters.py`: `self.predicates = tuple(predicates)`.
- Tests:
  - `tests/test_hashing/test_polars_expr_hashing.py`: canonical-JSON corpus (pinned),
    equalities/inequalities, rejections (UDF, selectors, alias, ...), message content,
    cross-PYTHONHASHSEED subprocess equality.
  - `tests/test_core/operators/test_polars_filter_expression.py`: public interface —
    expression filter upstream of `Join`; inside `PipelineJob` sync and async; two runs
    against one `DeltaTableDatabase` in fresh processes, second run all cached; list vs tuple
    predicates hash equal; keyword + expression mix.
  - `tests/test_hashing/golden/cases.py`: add `semantic/polars_expr` and
    `pipeline/polars_filter_expr` cases; regenerate, check diff adds lines only.
- Docs: `docs/reference/hashing.md` (label + grammar), operator docs for `polars_filter`
  (supported subset, the error), `CHANGELOG.md` `[0.3.0]`, `DESIGN_ISSUES.md` entry.

## Tasks

- [ ] 1. Failing tests: write both test files; run; confirm failures are the TypeError /
      missing module. Commit (WIP).
- [ ] 2. Implement `polars_expr.py` + handler + registration + tuple normalization. Run new
      tests to green.
- [ ] 3. Run the parser tests under `uv run --with polars==1.36.1` and locked 1.41.2 (and
      1.44.2 for information).
- [ ] 4. Golden: add cases, regenerate, verify `git diff` only adds lines; run golden tests.
- [ ] 5. Docs + CHANGELOG + DESIGN_ISSUES; docs tests (`tests/test_docs`).
- [ ] 6. Full suite, ruff, ruff format, basedpyright (exit code).
- [ ] 7. Merge origin/main (and PR #286 when it lands: align handler placement/docs), rerun.
- [ ] 8. Push, open DRAFT PR "Fixes ITL-828", link (`orca linear attach`,
      `orca worktree set --pr`), CodeRabbit rounds, worker_done.

## Progress notes (for a successor)

- 2026-11-11: spec + experiments committed; coordinator approved provisionally.
- 2026-11-11: implemented polars_expr_v1 (parser, handler, registry + v0.1.json), tuple
  predicates; corpus + golden additions (109 lines added, none changed); parser tests pass on
  Polars 1.36.1, 1.41.2, 1.44.2.
- Found: any PolarsFilter inside a PipelineJob upstream of a Join failed (Polars makes every
  column nullable; tag str vs str|None). Coordinator chose option C: restore nullability for
  every column except data columns (no golden move). Option A (all columns, moves the pinned
  pipeline:polars_filter row data hashes) is with the developer; DESIGN_ISSUES PF1b open.
- Found (coordinator filing separately): after an async job.run(), operator nodes read back
  empty; job tests assert through a function pod.
- Docs: docs/reference/hashing.md section "1c. Polars expression predicates"; PR #286 also adds
  a "1c" section there, so on merging #286 renumber ours to 1d and fix the anchor in
  docs/concepts/operators.md.
- (Superseded) The PR was a draft until the developer confirmed the identity design.
- 2026-11-11 (later): developer approved polars_expr_v1 as final and the FULL nullability fix
  (option A). PolarsFilter restores every column's nullability; versus main the golden diff
  changes exactly the two pipeline:polars_filter row data_hash lines (new value = source row
  hash) and otherwise adds lines. PR to be marked ready.
