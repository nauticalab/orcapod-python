# ITL-933: build the workshop Spiral project `edi-tutorial-v1`

**Linear:** ITL-933 (Urgent; the workshop is November 17, 2026).
**Branch:** `eywalker/itl-933-build-the-workshop-spiral-project-edi` (from `origin/main`).
**Brief:** the coordinator's dispatch `task_58826b467f46`. Phase 1 is design plus a dry-run-tested
tool, with no writes to Spiral. Phase 2 (after approval): the coordinator runs create, copy and
verify; I fix what the live run shows and open the PR (body: `Part of ITL-933`).

## Goal

A reproducible tool under `workshop/edi_tutorial_dataset/` that copies a small subset of the
V1 EDI sample (`edi-sample-v1-855632`, dataset `default`) into a new Spiral project. The copy
declares columns non-nullable where the EDI schema specification (v0.3.0) requires them. It is
tested against a fake of the pyspiral client and documented in a short README.

## Facts measured in Phase 1 (read-only, November 11, 2026, pyspiral 0.16.33)

Scratch scripts and outputs are in the session scratchpad
(`/private/tmp/claude-502/-Users-eywalker-orca-workspaces-orcapod-python-itl-933-edi-tutorial-dataset/f447d959-4b29-40ec-b9bd-ec08e6d758b2/scratchpad/`):
`measure.py` → `measure.json`, `window_probe.py` → `windowA.json`, `schemas.py` → `schemas.json`,
`parse_spec.py` → `spec.json`, `compare.py` → `compare.txt`, `storage_probe.py`.

* **Schema vs. spec.** The 24 sample tables the spec defines match it exactly: same columns,
  same Arrow types, same key columns in the same order, no renames. Not in the spec:
  `sample_info`, `sample_unit` (sample bookkeeping) and `experiment_screen_capture_frame_270p`
  (the spec has `experiment_screen_capture`, one MP4 blob per experiment, instead).
  Spec tables absent from the sample: the five `_run` tables, `experiment_sync_event`,
  `experiment_screen_capture`, `atlas_info`, `atlas_preference_history`, `mri_dataset`.
* **Nulls.** Every column the spec marks required has zero nulls in the whole table, key
  columns included. `lfp_voltage_uv` was checked inside the window only (0 row nulls; exactly
  one null item per row, which the spec allows: `fixed_size_list(f32?)[384]`).
* **One clock.** Every `timestamp` is µs relative to `session_start_datetime`: spikes and LFP
  start at `recording_start_timestamp` (1,896.000 s), gaze at `experiment_start_timestamp`, and
  the 270p frames carry the same instants as `experiment_screen_capture_event` (as `int64`).
  One half-open window `[t0, t1)` therefore applies to every time-keyed table unchanged.
* **Rates.** LFP 600 Hz; gaze 200 Hz; screen-capture events 120 Hz; 270p frames about 30 Hz
  (about 41 KB each); free-viewing events about 26/s; spikes about 707/s over 100 units.
* **Storage.** All 52 `edi-sample-*` projects use Volume `vol_01m6s4jsjeedya88dc8t2ns96q`
  (selected). The Volume's URI is visible only to org owners; the infra README says the
  `edi-sample-datasets` Volume is `s3://edi-sample-datasets/` (whole bucket). A project created
  with `--volume` cannot choose a sub-prefix; only a separately registered Volume can.

## Subset proposal (to confirm with the developer)

* Window A: start of trial 215 to end of trial 225, `[5226928854, 5370814965)` µs, 143.9 s,
  11 consecutive valid trials.
* Units: the 32 units with the most spikes in the window among units whose `bombcell_label` is
  not `noise` (63,349 spikes).
* Trials kept: those overlapping the window, plus any trial referenced by a kept row (the
  window is trial-aligned, so 215–225).
* Everything else is copied whole. No value is ever edited.

## Design

Files:

* `workshop/edi_tutorial_dataset/edi_tutorial_dataset.py`: one self-contained module and CLI
  (loaded by path in tests, the layout PR #274 uses for `docs/tutorial/workshop_data.py`).
  Subcommands: `derive-map` (spec + live source schemas → map), `plan` (read-only dry run of
  the whole copy: rows, bytes, null checks, foreign keys), `create`, `probe`, `copy`
  (`--dry-run`), `verify`, `render-map`.
* `nullability_map.json`: per table and column: spec type, target nullability, key flag,
  foreign keys, and `pending` entries for what the developer must decide. `copy` refuses to run
  while any entry is pending.
* `subset.json`: window, unit IDs, and how each table is selected.
* `README.md`: what the dataset is, the commands, the nullability table.
* Tests: `tests/test_workshop/test_edi_tutorial_dataset.py` and
  `tests/test_workshop/fake_spiral.py` (an in-memory fake of the pyspiral surface the tool
  uses, including filter expressions and the live type rule for comparison literals).

Safety:

* Reads only from `--source-project`; the source is held behind a read-only facade.
* Writes only to `--target-project`, which must equal `--allow-writes`, differ from the
  source, not start with `edi-sample-v`, and carry the tool's marker table
  (`meta.edi_tutorial_build`), which only `create` writes, into a project it has just created.
  Every table it writes carries the marker in its metadata; a target holding any table
  without the marker is refused.
* Before any write, each batch is checked: column names and types equal the source's exactly
  (type strict), and every non-nullable column (top level and declared struct children) has no
  nulls. A null raises; nothing is coerced.
* Re-runs are idempotent: a table whose status metadata says complete for the same subset
  digest is skipped; an incomplete table is rewritten (Spiral upserts by key); a complete
  table from a different subset is refused.

`verify`: row counts per table against the plan; target types equal source types exactly;
nullability equals the map; key schema equals the source's key columns, non-null; no nulls in
required columns; every foreign key and trial reference resolves; values equal the source
selection row by row (streamed, both sides key-sorted).

## Steps

1. [x] Read the brief, the issue, the spec, the inputs, PR #274's layout, the ITL-926 findings
   and fake, and the pyspiral 0.16.33 API.
2. [x] Live read-only measurements (above).
3. [x] Write the fake and failing tests for: the target guards, the marker, null rejection,
   type strictness, idempotent re-run, dry run writes nothing, verify catches each defect.
4. [x] Implement the tool until the tests pass; ruff and basedpyright clean.
5. [x] Write `nullability_map.json` (with `derive-map`) and `subset.json`; run `plan` live
   (read-only) and record the per-table numbers.
6. [x] README (draft) with the human nullability table and the commands.
7. [ ] WIP commit and push; send the coordinator ONE `ask` with the mismatches, the subset
   numbers, the project/volume commands and the live-step commands.
8. [ ] Phase 2 (after approval): the coordinator runs `create`, `probe`, `copy`, `verify`; fix
   what the live run shows; record the project ID in the README; full suite; PR.

## Notes for a successor

* Live calls work from this terminal with `uv run --with pyspiral==0.16.33 ...`; the repo lock
  (0.14.9) does not use the developer's login.
* A scan must project at least one non-key column (`Reading key columns without including at
  least one column from any column group is currently not supported`); use `scan_keys` for keys
  only.
* Filter literals must carry the column's exact Arrow type (`pa.scalar(x, pa.uint64())`); an
  `int64` literal against a `u64` column fails with `Cannot compare different DTypes`.

## Phase 1 results (November 11, 2026)

* `plan` live, read-only: `superpowers/plans/2026-11-11-itl-933-plan-live.txt`. 204,948 rows,
  361 MB uncompressed, 103 s, peak RSS 715 MB.
* Source integrity break found by `plan`: 58 of 383 `lfp_location` rows reference
  `('AFNI_D99v2', 'White Matter')`, which `atlas_label` does not hold.
* Tests: 34 pass; mutation checks (null check, marker check, typed literals, trial references,
  prefix guard, abort) each fail at least one test.
* basedpyright: 0 errors, about 1,160 warnings (mostly `reportAny` from the untyped client and
  unannotated fixtures). To reduce in Phase 2 with Protocol types for the client surface.

## Phase 2 notes (November 11, 2026)

* Developer decisions applied (decisions.json): see the README. Project created by the
  coordinator: `edi-tutorial-v1-841226` (marker table only so far).
* Probe on the server: SpiralDB stores every top-level column nullable (keys included) and
  every struct field nullable; only list items keep a non-null flag. pyspiral's
  `Schema.from_arrow` already makes top-level columns nullable (my local check suggested
  struct fields kept the flag; the server does not). A top-level null under a non-null
  declaration is refused by Arrow's batch validation, not by a stored constraint.
* Consequence: copy enforces required columns before writing; verify expects the storable
  schema and checks required columns from the data. Orcapod sources still show `T | None`;
  the developer is choosing between O1 (nullability record in table metadata + Orcapod
  support), O2 (accept `T | None`) and O3 (source parameter). `annotate --key` can add the
  record later without touching data.
* `copy --tables X --replace` refreshes a table after a source fix (the EDI owner is adding
  the `White Matter` row to `atlas_label`).
* `copy --dry-run` ran live against `edi-tutorial-v1-841226`: exit 0, 204,947 rows.
