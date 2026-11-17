# Self-Paced Workshop Tutorial Implementation Plan (PLT-4864)

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Ship a self-paced, tested tutorial (parts 0-10) for next week's workshop, with one
synthetic dataset, the two prerequisite concept pages, a troubleshooting page, working
replacements for the stale hands-on material, and facilitator notes.

**Architecture:** Every tutorial page is a Markdown file under `docs/tutorial/` whose `python`
fences run under the existing docs harness (`tests/test_docs/`). Each page starts by calling
the dataset generator, so a page is runnable on its own in an empty directory. The generator
is a single standard-library script, `docs/tutorial/workshop_data.py`: MkDocs copies it into
the built site (so attendees can download it), the harness puts `docs/tutorial` on
`PYTHONPATH`, and an attendee runs `python workshop_data.py`.

**Tech Stack:** MkDocs Material (`pymdownx.details` for collapsed solutions), pytest, the
existing `_doc_samples` runner. No new dependencies. No changes under `src/`.

Source of the outline: `superpowers/plans/2026-11-09-docs-gap-report.md`, last section.

---

## Decisions already taken

| Decision | Choice | Why |
|---|---|---|
| Generator location | `docs/tutorial/workshop_data.py` | Not in `src/` (gate). Under `docs/` it is published with the site and sits next to the pages that use it. Confirmed by the coordinator. |
| Generator dependencies | Standard library only (`csv`, `random.Random(seed)`, `pathlib`) | Byte-identical output on every machine; nothing to install. |
| Solutions | Collapsed `??? success "Solution"` blocks on the same page | The solution code is then executed by the harness too. |
| Part 3 library failures | Teach the spellings that work; state both limits as known bugs with exact error text | Coordinator answer, option A (see "Library misbehaviour"). |
| Repository owner in URLs | `metamorphic-labs/orcapod-python` | Coordinator notice: the repo is moving; never write `nauticalab` or `walkerlab`. |
| Stale notebook | Delete `notebooks/tutorials/01_introduction_to_orcapod.ipynb` | Pre-approved. `docs/tutorial/` replaces it. |
| Stale example | Rewrite `examples/save_and_load_pipelines.py`; run it from the test suite | Brief prefers a working, tested replacement. |
| Environment | `uv sync --no-install-package pygraphviz`, then `uv run --no-sync ...` | `pygraphviz` cannot build here (no Graphviz headers); #267 removes it. |

## The dataset

`make_dataset(root="workshop_data") -> Path` writes about 0.4 MB, 27 files:

```
workshop_data/
    subjects.csv     subject_id, sex, age_weeks, genotype            4 rows
    sessions.csv     subject_id, session, date, duration_s           8 rows
    units.csv        subject_id, session, probe, unit, region,
                     depth_um, n_spikes                              64 rows
    sessions/<subject>_<session>/
        notes.txt
        spikes_imec0.csv, spikes_imec1.csv    time_s, unit
```

Four mice (two wildtype, two knockout), two sessions each, two probes per session, four
units per probe. Fixed seed; knockouts fire about 30% less so that the per-session summary
shows something.

## Behaviour verified by running code (base: #271 branch)

These are the facts the pages are written from. Each was observed, not read.

- `CSVSource(path, tag_columns=..., source_id=...)` needs `path` as `str`; a `pathlib.Path`
  raises `TypeError: 'PosixPath' object is not iterable`. Default `source_id` is the path.
- CSV string columns are Arrow `string`; `DictSource` and anything that passed through
  `polars_filter` are `large_string`.
- Function pods: exact-column rule, annotation rule, multiple outputs via `output_keys=[...]`.
- `stream.as_pandas_df()` exists on sources, pod streams and job nodes.
- `PipelineJob(name=..., database=db)`; `job.nodes` lists sources by `source_id`, operators as
  `SelectDataColumns_1`, `Join`, `GroupBy`, pods by `label`.
- Two input rows with identical data values share one stored result (64 unit rows, 63 calls).
- `DeltaTableDatabase("dir")` layout: `dir/<job name>/<function name>/<hash>/v0` and
  `dir/<job name>/_result/<function name>/<hash>`. Each write prints many
  `WARN deltalake_core::writer::stats` lines to stderr.
- Editing a body: stale result. `version="v1.0"`: recomputed.
- `job.save(path)` / `PipelineJob.load(path)`: a loaded job answers
  `job.nodes[label].get_all_records()`; `as_table()` on a loaded function node and
  `run()` both raise `RuntimeError` (read-only stubs).
- `show_graph()` is on `Pipeline` (`job.pipeline.show_graph()`), not on `PipelineJob`, and
  needs the Graphviz `dot` binary (`ExecutableNotFound` otherwise).
- `Pipeline` blueprint: `PipelineJob.from_pipeline(p, database=db)` → `unbound_sources`,
  `job.bind(sources={...})`, then `run()`.
- Files: `op.File(path)` validates existence, is hashed by content (edit → recompute; same
  content under another path → cache hit). A `pathlib.Path` column is hashed by its path
  string (edit → no recompute). `op.Directory(path, ignore=[...])`. `GroupBy` gives
  `list[File]`, usable as a `list[op.File]` parameter. A pod may return `Path` or `op.File`.
- Meta columns appear in `node.get_all_records(columns={"meta": True})`, not in
  `node.as_table(columns={"meta": True})`.
- A function that raises: in memory the exception propagates; in a job the default policy
  `"continue"` drops the failed rows and logs a traceback to stderr;
  `job.run(orchestrator=SyncPipelineOrchestrator(error_policy="fail_fast"))` re-raises.
  `StatusObserver` / `LoggingObserver` record per-row state, stdout and traceback.
- `@sink_pod` functions must take the context parameter (`ctx`), otherwise
  `TypeError: ... got an unexpected keyword argument 'ctx'`.

## Library misbehaviour (reported, not fixed, not hidden)

1. `polars_filter(<expression>)` cannot be hashed → fails before a join and inside any
   `PipelineJob` (`TypeError: SemanticAwarePythonHasher (strict): no implementation ... for
   type 'polars.expr.expr.Expr'`). Equality constraints (`polars_filter(genotype="wildtype")`)
   work everywhere.
2. `semi_join` between a `CSVSource` and a `DictSource`/filtered stream →
   `ArrowInvalid: Incompatible data types for corresponding join field keys ... string and
   ... large_string`. `join` on the same pair works.
3. `CSVSource` rejects a `pathlib.Path`.
4. `PipelineJob` has no `show_graph`; the gap report's outline assumed it.
5. A `sink_pod` without a `ctx` parameter fails at run time, not at decoration time.

## File structure

| File | Responsibility |
|---|---|
| `docs/tutorial/workshop_data.py` | Dataset generator and its CLI |
| `docs/tutorial/index.md` | What the tutorial is, the dataset, the list of parts |
| `docs/tutorial/00-setup.md` … `10-where-next.md` | Parts 0-10, one page each |
| `docs/concepts/pipeline-job.md` | `Pipeline` vs `PipelineJob`, stores, nodes, bind, save/load |
| `docs/concepts/file-types.md` | `op.File`, `op.Directory`, `Path`, what is hashed |
| `docs/troubleshooting.md` | Symptom → cause → fix, from the gap report table plus findings |
| `mkdocs.yml` | `Tutorial` nav section, two concept pages, troubleshooting |
| `tests/test_docs/test_doc_samples.py` | Page list + `docs/tutorial` on `PYTHONPATH` |
| `tests/test_docs/test_workshop_data.py` | Generator: deterministic, small, CLI works |
| `tests/test_docs/test_examples.py` | Runs `examples/save_and_load_pipelines.py` |
| `examples/save_and_load_pipelines.py` | Rewritten working example |
| `notebooks/tutorials/01_introduction_to_orcapod.ipynb` | Deleted |
| `superpowers/plans/2026-11-09-workshop-facilitator-notes.md` | Live path, checklist, open questions |
| `CLAUDE.md`, `.zed/rules` | "Documentation samples" section names the new tested paths |

## Tasks

### Task 1: Dataset generator (TDD)

- [x] Write `tests/test_docs/test_workshop_data.py`: two runs into two directories give the
  same relative file list and identical bytes; total size under 2 MB; `subjects.csv` has 4
  data rows, `sessions.csv` 8, `units.csv` 64; for each unit row, `n_spikes` equals the number
  of rows for that unit in the matching `spikes_<probe>.csv`;
  `python docs/tutorial/workshop_data.py out` exits 0 and prints `Wrote 27 files`.
- [x] Run it: `uv run --no-sync pytest tests/test_docs/test_workshop_data.py -q` → fails
  (module does not exist).
- [x] Add `docs/tutorial/workshop_data.py` (from the scratch draft used for the probes).
- [x] Run again → passes. Commit `feat(docs): add deterministic workshop dataset generator`.

### Task 2: Harness covers the new pages (TDD)

- [x] In `test_doc_samples.py`, extend `test_doc_pages_are_discovered` with
  `docs/tutorial/01-first-stream.md`, `docs/concepts/pipeline-job.md`,
  `docs/troubleshooting.md`; run → fails.
- [x] Extend `DOC_PAGES` with `docs/tutorial/*.md` and `docs/troubleshooting.md`; add
  `REPO_ROOT / "docs" / "tutorial"` to the subprocess `PYTHONPATH`. (The test stays red until
  the pages exist; it goes green in Task 3.)

### Task 3: Tutorial pages

Common shape of every part: one-paragraph motivation; "You will" list; a first block that
calls `make_dataset()`; samples with shown output; "What to remember"; two or three exercises
with collapsed solutions; "Next" link. Terms are defined at first use. For each page: write
it, run `uv run --no-sync python -m tests.test_docs._doc_samples docs/tutorial/<page>` with
the harness environment, correct the shown output from what was printed, re-run until green.

- [x] `index.md`, `00-setup.md` (install, extras, check command, the warnings to ignore, get
  the dataset).
- [x] `01-first-stream.md` (`CSVSource`, `DictSource`, tags vs data, `output_schema`, `keys`,
  `iter_data`, `as_pandas_df`; exercises: choose tags for `units.csv`; provoke the two
  construction errors).
- [x] `02-function-pods.md` (`firing_rate`; annotations; multiple outputs; exact-column rule
  and `select_data_columns`; exercise: fix a pod that fails on a wide stream).
- [x] `03-operators.md` (`join`, `semi_join`, `map_data`, `polars_filter`, `Join` vs
  `MergeJoin`; the two known bugs stated plainly; exercise: subjects + units, filtered).
- [x] `04-many-rows-into-one.md` (`group_by`, `batch`, pod over `list[float]`; exercise:
  per-session mean from per-probe rows).
- [x] `05-pipeline-job.md` (record, run, `job.nodes`, in-memory vs Delta store; exercise: run
  a script twice).
- [x] `06-reruns-and-caching.md` (new rows; edited body; major version; identity in brief;
  exercise: which of four edits recompute).
- [x] `07-files.md` (`op.File`, `op.Directory`, `Path` outputs, file hash caching; exercise:
  one output file per session).
- [x] `08-inspecting-and-tracing.md` (meta and provenance columns, `get_all_records`,
  `as_source`, `show_graph`).
- [x] `09-when-things-go-wrong.md` (reading the common errors, error policy, status and
  captured logs; links to the troubleshooting page).
- [x] `10-where-next.md` (sink pods, context injection, executors, `PollingSource`,
  save/load; short samples only where verified).
- [x] Commit per two or three pages: `docs(tutorial): add parts N-M`.

### Task 4: Concept pages and troubleshooting

- [x] `docs/concepts/pipeline-job.md`, `docs/concepts/file-types.md`,
  `docs/troubleshooting.md`; cross-link from `function-pods.md`, `file-hash-caching.md`,
  `getting-started.md`, `index.md` where the old text pointed at a section.
- [x] `mkdocs.yml` nav. `uv run --no-sync pytest tests/test_docs/test_mkdocs_build.py -q`.
- [x] Commit `docs(concepts): add PipelineJob and file types pages, troubleshooting`.

### Task 5: Stale hands-on material (TDD)

- [x] `tests/test_docs/test_examples.py`: run `examples/save_and_load_pipelines.py` with
  `cwd=tmp_path`, assert exit code 0 and that stdout contains `loaded job`. Run → fails with
  the `AttributeError` from the gap report.
- [x] Rewrite the example (build, run, save, load, read stored records). Run → passes.
- [x] `git rm notebooks/tutorials/01_introduction_to_orcapod.ipynb`; grep the repo for
  references to it.
- [x] Commit `fix(examples): replace broken save/load example; remove stale notebook`.

### Task 6: Facilitator notes, agent instructions, DESIGN_ISSUES

- [x] `superpowers/plans/2026-11-09-workshop-facilitator-notes.md`.
- [x] `CLAUDE.md` and `.zed/rules` "Documentation samples": add `docs/tutorial/`,
  `docs/troubleshooting.md`, the `workshop_data` import, and the example test.
- [x] One appended `DESIGN_ISSUES.md` entry for this issue, listing the library findings.

### Task 7: Verify and ship

- [x] Time the harness: `uv run --no-sync pytest tests/test_docs -q --durations=20`; target
  about a minute for the sample run.
- [x] `uv run --no-sync pytest tests/` — full suite; record pre-existing failures and skips by
  name.
- [x] `git fetch`; if #271 merged, rebase onto `origin/main` and re-run.
- [x] Push, open the PR against `main` (`Fixes PLT-4864`, `Part of PLT-4816`), attach to
  Linear, set the worktree PR, send `worker_done`.

## Self-review

- Brief items 1-7 map to Tasks 3, 1, 4, 4, 5, 2+7, 6.
- No dependency added; nothing under `src/`; `CHANGELOG.md`, `pyproject.toml` and README
  install lines untouched.
- The plan changes no public API, hash value or stored schema.
