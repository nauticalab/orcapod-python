# ITL-1034 Row-alignment test suite Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A test suite that proves, by recomputation, that every per-row derived column of every
table Orcapod produces belongs to its own row, on sync and async paths, under forced completion
orders; plus a code sweep for the "built in one order, attached by position in another" pattern.

**Spec:** Linear ITL-1034 plus the coordinator's brief (task_a7a8603f2f52). The brief is the design;
no separate spec. ITL-1029 (the `_shape_table` site) is fixed on another branch: this suite must
FAIL on main because of it and pass once that fix is merged. No xfail, no fix of that site here.

**Architecture:** `tests/test_core/alignment/` holds three helper modules and the test modules.
`gate.py` forces completion order with asyncio events (no sleeps). `oracle.py` derives, from the
Python rows the test itself wrote, every origin row and its record id by the documented recipe
(`docs/reference/hashing.md` sites 5 and 6b). `checker.py` walks every row of a produced table and
recomputes each derived column from that row alone (content hash, source-info tokens, system tags,
record ids, context key, function outputs), falling back to an identity-keyed comparison with the
stored records only where a column embeds a per-run uuid.

**Tech stack:** pytest, pyarrow, asyncio; public Orcapod API (`ArrowTableSource`, `function_pod`,
operators, `PipelineJob`, `node.as_table / head / get_all_records / as_source`).

---

## Measured facts (prototype in the scratchpad, Nov 12 2026)

* ITL-1029 reproduces deterministically: async job, completion order reversed, 5 rows,
  `node.as_table(all_info=True)` shows each row with the hash of the mirrored row.
* The sync path is affected too whenever `sort_by_tags` permutes the rows (input not in tag
  order): `_shape_table` sorts, then appends hashes in `iter_data` order. Same site, same fix.
* Node `head()` goes through `_shape_table` too, but `head_column_config` forces
  `sort_by_tags=False`, so it is aligned (sweep).
* `_content_hash` recomputes as `data_context.arrow_hasher.hash_table(row.select(data_keys)).to_string()`
  on a one-row slice of the produced table (verified equal on source and node tables).
* Source tokens: `"<source_id>::<token>::<column>"`, token `"<col>=<value>"` with
  `record_id_column`, else `"row_<i>"`. Record id: `uuid5(_SOURCE_RECORD_ID_NAMESPACE,
  f"{source_id}::{token}")`, namespace `uuid5(NAMESPACE_URL, "https://orcapod.org/namespaces/source-record-id")`.
  (docs/reference/hashing.md site 5 states a different namespace string: a docs defect, report it.)
* Function output token: `"<pod uri>::<output datagram uuid hex>::<column>"`; the stored record's
  `__data_id` is that uuid. `__output_data_hash` / `__input_data_hash` are prefixed digests.
* Async concurrency is unlimited by default (`resolve_concurrency` -> None), so gating every row
  on an event cannot deadlock for small tables; each wait still has a 30 s safety timeout that
  raises a named error (a deadlock guard, never the mechanism of an assertion).

## File structure

* `tests/test_core/alignment/__init__.py` — empty.
* `tests/test_core/alignment/gate.py` — `CompletionGate`, `ORDERS` (natural, reversed,
  interleaved, one_slow, random seeds), the gated function pods (`async def` functions keyed on a
  unique data value).
* `tests/test_core/alignment/oracle.py` — `Origin`, `Oracle` (built from the test's own rows and
  source ids; record id -> origin, token -> origin), recipe functions `source_record_id`,
  `fold_record_ids`, `fold_values`.
* `tests/test_core/alignment/checker.py` — `assert_rows_aligned(table, *, data_keys, tag_keys,
  oracle, data_context_key, ...)` and the per-column rules; `assert_same_rows_by_identity(a, b)`
  for sync == async keyed on record ids.
* `tests/test_core/alignment/fixtures.py` — sources for the three tie scenarios (unique tags in
  shuffled order, duplicate tags, null tags), `COLUMN_CONFIGS`.
* `tests/test_core/alignment/test_checker.py` — the checker is not vacuous: each rule rejects a
  table with that one column permuted.
* `tests/test_core/alignment/test_gate.py` — the gate realises each order (observed via the
  node's `iter_data` order).
* `tests/test_core/alignment/test_streams.py` — `ArrowTableStream`, sources, `FunctionPodStream`
  (sync and the concurrent async-function path), `head()`.
* `tests/test_core/alignment/test_operators.py` — every operator x column config x tie scenario
  (pairwise reduction over operators x configs, full over scenarios).
* `tests/test_core/alignment/test_nodes.py` — function and operator job nodes: sync/async x
  orders x stores (in-memory, Delta) x fresh/readback (rerun, `PipelineJob.load`) x column
  configs x scenarios (full product), `get_all_records`, `head()`, `as_source()` (DerivedSource).
* `tests/test_core/alignment/test_job.py` — `run`, `run_async`, run inside a running loop; run
  summary under forced orders with failing rows; sync == async by identity.

## Checker rules (per produced row)

1. `_content_hash` (or a custom name): recompute from the row's data columns.
2. `_context_key`: equals the stream's `data_context_key`.
3. `_source_<c>`: scalar token -> its origin (oracle by `(source_id, token)`) -> row[c] must equal
   the origin's value of the column `c` came from (rename map), or the declared derivation
   (function output: `y == f(origin)`). A list token (Batch, GroupBy, MergeJoin) is checked element
   by element against the list value. A token whose prefix is a function uri is checked by
   identity against the stored record (`__data_id` hex) when records are given.
4. System tags, grouped by column-name suffix: `_tag_source_id::S` / `_tag_record_id::S`. A scalar
   record id must be an oracle record id whose source id equals the row's source-id value, and
   every source token of a column of that source must give the same record id. Folded (aggregated)
   ones must equal the documented fold of the member record ids recomputed from the row's
   list-valued tokens.
5. User tags and data without provenance columns: (tag, data) must match the row's origin (via
   record id when system tags are present, else by the set of origins with equal tags; with
   duplicate tags the value must be one of the candidates' values).
6. Stored records (`get_all_records(all_info=True)`): `__output_data_hash` recomputed from the
   data, `__input_data_hash` = content hash of the origin's input data (recomputed from the
   origin's own source row through `Data.content_hash` of the source stream), `__data_context`
   = context key, `_source_y` embeds `__data_id`.

## Coverage matrix (as designed)

| Path | sync/async | orders | ColumnConfig | scenarios | stores |
|---|---|---|---|---|---|
| ArrowTableStream / source as_table, head | sync | n/a | all 8 | 3 | n/a |
| FunctionPodStream (`pod(src)`), head | sync + async gather | 5 | all 8 | 3 | n/a |
| FunctionJobNode as_table/head/get_all_records/as_source | sync + async (run, run_async, in-loop) | 5 (+3 seeds) | all 8 | 3 | mem, delta; fresh + rerun + load |
| OperatorJobNode (Join, GroupBy downstream of the gated fn node) | sync + async | 5 | all 8 | 3 | mem, delta; fresh + rerun |
| Each operator on a source stream | sync (+ async via job for 3 ops) | n/a | pairwise | 3 | n/a |
| Run summary (failing rows) | sync + async | 5 | n/a | 1 | mem |

ColumnConfig set: `{}` (default), `sort_by_tags=True`, `content_hash=True`,
`content_hash="h"`, `source=True`, `system_tags=True`, `context=True`, `meta=True`,
`all_info=True`, and `all_info` with `sort_by_tags=False`.

## Tasks

### Task 1: gate + oracle + checker + checker self-test
- [ ] Write `gate.py`, `oracle.py`, `checker.py`, `fixtures.py`.
- [ ] `test_checker.py`: build a correct source table; assert the checker passes; for each rule,
      permute one column (`table.set_column(i, name, column.take(perm))`) and assert the checker
      raises `AssertionError` naming that column. Run, expect PASS.
- [ ] `test_gate.py`: run the gated node for each order; assert `iter_data` order == the order.
- [ ] Commit `test(alignment): row-alignment checker and completion-order gate (ITL-1034)`.

### Task 2: node paths (reproduces ITL-1029)
- [ ] `test_nodes.py` with the full product above. Run; expect FAILURES only on rows with
      `_content_hash` under `sort_by_tags` / async orders (the ITL-1029 site). Record the failing
      ids in this plan.
- [ ] Commit, push, open DRAFT PR (Fixes ITL-1034), status message to coordinator.

### Task 3: streams, operators, job, run summary
- [ ] `test_streams.py`, `test_operators.py`, `test_job.py`. Any failure outside the ITL-1029 site
      is a new defect: send it to the coordinator at once with a minimal repro, then fix here if a
      few lines (failing-first test) or wait for the coordinator.
- [ ] Commit.

### Task 4: sweep
- [ ] Verify every suspect line of the sweep report; put the table in the PR body.

### Task 5: finish
- [ ] changelog.d/ITL-1034.md (`### Added`: the suite; no library change).
- [ ] ruff format/check, basedpyright, full `uv run pytest tests/ -n 4` (or fewer processes).
- [ ] Merge origin/main when the coordinator says the ITL-1029 fix is in; rerun; PR ready;
      CodeRabbit rounds; worker_done.

## Progress notes
(Successors: append here.)

* Helpers + checker self-test done (16 pass). Sweep done (subagent). Second defect found and
  sent to coordinator (msg_c718d4fe9800): FunctionPodStream resumed iterator re-enumerates
  from 0 (function_pod.py:1043/1069) and drops rows; awaiting decision to fix here.
* Nov 13: suite complete (6100 tests: nodes, operators, streams, job, checker). ITL-1029
  fixed on main by #344 (cases pass unmarked). ITL-1039 (#348) and ITL-1040/838 (#350)
  merged; their marks removed. Remaining strict xfails name open issues only: ITL-1042,
  ITL-1043, ITL-1045, ITL-1046, ITL-1050. PR #346 body holds the matrix, runtime, sweep
  table and defect list. Left: CodeRabbit rounds and CI on #346, then worker_done. When
  an open issue's fix merges, merge main and drop its marks (they XPASS strictly).
* Nov 13 (later): main at 5c7a058f has #351 (ITL-1050), #354 (ITL-1053), #356 (ITL-1060),
  #358 (ITL-1045), #360 (ITL-1065) and #361. Merged; with --runxfail only the 60 ITL-1066
  cases failed, so every other mark is gone. head now takes every configuration and must
  equal as_table(cfg).slice(0, n); previews check sorted configs among the held rows.
  6431 pass, 60 strict xfails (ITL-1066 only). #346 merges last; worker_done on the
  coordinator's word.
* Nov 13 (later): merged main 3bc5e2af (#339), then 41798e87 (#362, ITL-1066). With
  --runxfail all 60 ITL-1066 cases passed; marks removed. 6491 pass, no marks. Still to
  come before #346 merges: #363 (ITL-1067), #364 (ITL-1068 warning filter), #365
  (ITL-1070: GroupBy drops non-by tags).
* Nov 13 (evening): merged main 163e5ddc (#363 ITL-1067, #364 ITL-1068, #365 ITL-1070).
  The suite passed unchanged. For ITL-1070, group_by_g now has data-only members and
  the grouped node's members are its data columns; operator streams, operator nodes
  (sync and async) and the grouped job node assert that the dropped tag n is absent from
  keys(), the schema and every table. #363 and #364 needed nothing: the suite has no
  all_info workaround and no warning filter. 6491 pass, no marks.
* Nov 14: CodeRabbit round on eae2391c. Stored records must be source A's record ids
  once each, and `_source_y` must equal the node output's whole token (function uri
  included). Operator nodes check `head` too, and node iterator reads check their
  expected rows. A system-tag pair's column must descend from its source's own column:
  its first hash is that source's schema hash, so two sources of one schema cannot be
  told apart. Three new self-tests. Column order was declined: it is covered by
  `tests/test_core/operators/test_join_tag_order.py`. 6494 pass, no marks.
* Nov 14 (later): answered 8 older review threads that had no reply. The checker now:
  - looks a null data value up like any value;
  - names a wrong folded record id under its own column;
  - requires a system-tag pair for every source a row's data came from;
  - checks #354's presentation order: nulls last, then ties by the system tags. List
    tags follow Polars, where a null element inside a list comes first; GroupBy's member
    sort puts nulls last at every depth. This was reported to the coordinator.
  New: a job-backed `SideEffectPod` node test (`SideEffectNode.as_table` has its own
  concatenation) on both orchestrators. 6503 pass, no marks.
* Nov 14 (CodeRabbit on 2926e559): the checker resolves an element no single value
  explains by the member's tags (one origin: checked; none: fails), and reads all_info
  inside the column config. SpiralDB previews: head(1) and head(3) under every config
  across the batch boundary. Side-effect previews must deliver nothing. One strict
  xfail, ITL-1091: a read of a side-effect job node after job.run() delivers every side
  effect again (6 -> 12 -> 18). 6527 pass, 1 xfailed.
* Nov 14 (CodeRabbit on 647c4d45): origins are recovered source by source, and a
  recovered origin's function tokens are checked. Merged main through b746c630 (#372,
  #374, #367, #369, #377). ITL-1091 is fixed by #377, so the strict xfail is gone. A
  side-effect job node's preview after its run now shows the rows the run passed on
  (each aligned under every config) and still delivers nothing. 6530 pass, no marks.
* Nov 15 (pre-push CodeRabbit CLI review on 62959d9e, 1 finding): a derived element that
  a known origin holds was skipped without its token being checked. With x present, an
  exchanged `_source_p` of a shared p passed. resolve_remaining now checks it against
  that origin; new self-test. #380's load path: bind + run(load_only=True), and grouped
  joins the load read-backs. 6639 pass, no marks.
