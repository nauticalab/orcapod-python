# ITL-1112 / ITL-1126 follow-up: merge with main, CodeRabbit findings from PR #392

Branch `eywalker/itl-1112-binding-a-source-with-a-different-column`, PR #386. Head before
this work: 4ffdef06. Main at 810d1b03.

## 1. Merge origin/main (done: 5c9ac6cd, no conflicts)

PR #389 (ITL-1122) added a large-normalization sweep whose source kinds include a raw
`ArrowTableStream`; its JOB tests use it as a pipeline input, which ITL-1126 refuses
(developer decision; ITL-1127 adds the wrapper). Fix, tests only:

- `tests/test_core/operators/test_large_normalization.py`: `test_job_has_no_non_large_type_and_equals_plain`
  sweeps every kind except `ArrowTableStream` (the plain-stream sweeps keep it). One new test:
  a raw `ArrowTableStream` as a job input raises `SourceSpecMismatchError` naming the remedy
  (wrap the table as a source).
- `tests/test_core/sources/test_large_normalization_sources.py`: same for
  `test_job_nodes_have_no_non_large_type` (the table/head test keeps it), and the refusal test.
- Nothing else changes in either file; every other kind keeps its #389 assertions.

## 2. CodeRabbit findings raised on PR #392 against #386's code

### A (comment 4575906153) — CONFIRMED by reproduction

A source whose system-tag pair comes record-id first (an `ArrowTableStream` over a table
whose `_tag_record_id::…` column precedes `_tag_source_id::…`) is accepted, and the bound
node's table (and zero-row table) keeps record-id first while the unbound node's
`output_schema(all_info=True)` predicts source-id first. With an exact-schema source
(`_declared_form_for` returns `None`) the node passes the source through, so even its
`output_schema` reports the reversed order: the bind changed the node's schema order.

Fix: `_declared_form_for` also requires the system-tag pair in (source-id, record-id) order
for the pass-through, and `_DeclaredForm._declared_positions` orders the pair as a fourth
group (declared names, after renames), like the tag/data/source-info groups. This agrees with
main's row order (#390/ITL-1131: user tags, system tags, data, source info): the pair keeps
the positions it holds; only its internal order is fixed. Test first: a bound reversed-pair
source gives `as_table`, `head`, the zero-row table and `output_schema` in the unbound
node's order (exact and nullable-slot cases).

### C (comment 4575906162) — NOT REPRODUCIBLE; propose to decline

`Pipeline.compile` raises `InconsistentSourceError` for same-named slots whose schemas
differ, so same-named slots have equal declared tag and data schemas (and so equal
system-tag names). `validate`/`_fit` depend only on the name and those schemas, so a
rebuilt source gets the same verdict from every same-named slot: one accepting and the other
refusing cannot happen. Reproduction: two CSV sources with one source_id and differing
schemas are refused at compile; with equal schemas they deduplicate to one slot.

## 3. Verification

Targeted: tests/test_pipeline, tests/test_core/{nodes,sources,operators}, golden (3.12;
async files also 3.11), one process. Ruff, basedpyright. One push; CI on the merge.

## Status notes

- [x] merge
- [x] part 1 tests (e5155e34)
- [x] plan for A/C sent to coordinator: A approved, C decline approved; layout-by-kind: option 1 (known limit in PR body + changelog; coordinator files the issue)
- [x] A fix + tests
- [ ] push, CI, CodeRabbit, replies on #392 threads
