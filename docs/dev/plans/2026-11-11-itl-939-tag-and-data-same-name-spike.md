# ITL-939 design spike plan: a tag and a data column sharing a name

> **For agentic workers:** this is a design spike. No library code changes. Execute inline,
> task by task; tick the boxes and commit after each task so a successor can continue.

**Goal:** a design document (`superpowers/specs/2026-11-11-itl-939-tag-and-data-same-name-design.md`)
that establishes, by running code, what Orcapod does today when a value is wanted both as a tag
and as data, compares four designs, and recommends the smallest safe feature.

**Architecture:** throwaway experiment scripts under `superpowers/specs/itl-939-experiments/`
drive only the public API (sources, stream methods, operators, `function_pod`) on a small
EDI-shaped table (`session`, `experiment`, keys `subject_id`, `session_start_datetime`,
`experiment_index`). Each script prints what it observes; the outputs are pasted into the doc.

**Tech Stack:** orcapod (this repo), pyarrow, `uv run`.

---

### Task 1: Experiment 01, what happens today with a shared name

**Files:** Create `superpowers/specs/itl-939-experiments/exp01_same_name_today.py`

- [x] Build an Arrow table with duplicate field names (`session_start_datetime` twice) and pass
      it to `ArrowTableSource` with the name in `tag_columns`; record the error or behaviour.
- [x] `MapData({"x": "<tag name>"})` and `MapTags({"<tag>": "<data name>"})` collisions.
- [x] `Join` of two streams where one stream's tag name equals the other's data name.
- [x] `function_pod` whose output key equals a tag name.
- [x] Run with `uv run python ...`; save stdout to `exp01_same_name_today.out`.

### Task 2: Experiment 02, today's workarounds on the EDI-shaped table

**Files:** Create `superpowers/specs/itl-939-experiments/exp02_workarounds_today.py`

- [x] W1 choose `tag_columns` so the column stays data.
- [x] W2 duplicate the column under another name before building the source.
- [x] W3 read the source twice (once with the column as data), join.
- [x] W4 anything else found (e.g. `MapTags` cannot copy; `PolarsFilter` sees both).
- [x] For each: compute elapsed time (`experiment_start - session_start_datetime`) with a pod,
      then Join with a second table keyed on the same columns; record lines, renames,
      `pipeline_hash` / `content_hash` changes and downstream Join behaviour.
- [x] Save stdout to `exp02_workarounds_today.out`.

### Task 3: Experiment 03, prototype of the smallest feature (design C), out of tree

**Files:** Create `superpowers/specs/itl-939-experiments/exp03_copy_tag_prototype.py`

- [x] A throwaway subclass of `UnaryOperator` defined in the script (not in `src/`) that copies
      named tag columns into the data group under chosen names, with source-info for the copy.
- [x] Show `keys()`, `output_schema()`, `as_table()`, `iter_data()`, a pod reading the copy,
      a downstream Join keyed on the original tags, and that streams not using it keep their
      hashes.
- [x] Save stdout to `exp03_copy_tag_prototype.out`.

### Task 4: Need analysis from the EDI schemas

- [x] List key columns that are plausibly computation inputs; 3-5 workshop computations.

### Task 5: Design document

- [x] Sections: today (1), need (2), designs A-D table + prose (3), recommendation and numbered
      decisions (4). Commit and push WIP.

### Task 6: Report and PR

- [x] `ask` the coordinator with the summary; wait for the reply.
- [x] Append the DESIGN_ISSUES.md entry for ITL-939 at the end.
- [ ] Merge origin/main, push, open docs-only PR ("Part of ITL-939"), attach to Linear,
      `orca worktree set --pr`. CodeRabbit rounds. `worker_done`.

## Progress notes

(successor: read this section first)
- Tasks 1-5 done (exp01-exp04 with .out; design doc written). Findings: Join/MergeJoin
  tag-vs-data collision crashes late (J1), pod output key == tag name accepted (P1); W1/W3
  give 9 rows instead of 5; GroupBy already turns non-key tags into data (precedent for C).
- Coordinator answered the ask: open the PR as is; decisions go to the developer; J1/P1 filed
  as ITL-940 (not fixed here). Doc gained (a)-(e): full SpiralDB recipe, before/after, D cache
  paragraph, hash comparison (exp05), divergence.
- PR #310 open (https://github.com/metamorphic-labs/orcapod-python/pull/310), attached to
  ITL-939 and the worktree. Next: CI + CodeRabbit rounds, then worker_done. Keep the session
  alive afterwards: the coordinator may dispatch the CopyTagsToData implementation.

## Continuation (dispatch ctx_0b661d657a04): the DerivedSource form

Developer rejected the operator (design C) and chose a source derived from other pods.

- [x] exp06: probe today's `DerivedSource` (origins, columns, provenance, identity, job, save/load).
- [x] exp07: out-of-tree prototype `ProtoDerivedSource` (tags_as_data), compared with the recipe.
- [x] Design doc: status and 4.4 mark C rejected; section 8 addendum (a)-(h) with 11 decisions.
- [ ] `ask` the coordinator with the addendum; record the answers here.
- [ ] Step 2 (NOT in this dispatch): implement on a new branch from origin/main named by the
      coordinator: `tags_as_data` on `DerivedSource`, tests first, docs, CHANGELOG, DESIGN_ISSUES.

Notes for a successor: no library code changed. pyspiral is not installed here; SQLite stood
in for the DB table source. Findings to report, not fix: plain `DerivedSource` has no system
tags and no `to_config`, and caches an empty table if created before the origin ran;
`append_column(name, col)` in the recipe makes the copy nullable.
- [x] Coordinator sent the developer's answers (design doc 8.10). Step 2 approved in this
      dispatch: branch `eywalker/itl-939-derived-source-tags-as-data` from origin/main, PR
      "Fixes ITL-939". Plan: `superpowers/plans/2026-11-11-itl-939-derived-source-tags-as-data.md`
      on that branch. Pending asks: bare-hex source_id form; "save if associated with a database".
