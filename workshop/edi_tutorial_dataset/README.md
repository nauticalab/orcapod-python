# EDI tutorial dataset (`edi-tutorial-v1`)

The Orcapod workshop reads EDI tables from SpiralDB. This directory holds the tool that built
its Spiral project: a small subset of the V1 EDI sample (`edi-sample-v1-855632`, dataset
`default`). Each column is declared non-nullable where the
[EDI schema specification](https://github.com/enigma-brain/enigma-data-interface)
(`edi_schema_spec.md`, version 0.3.0) requires a value.

**Project: `edi-tutorial-v1-841226`** (dataset `default`), built and verified on November 11,
2026 (ITL-933). It uses the Volume every `edi-sample-*` project uses
(`vol_01m6s4jsjeedya88dc8t2ns96q`, the `edi-sample-datasets` bucket); its description names
it `edi-tutorial-v1 · edi-sample-tutorial`.

## What is in it

One subject, one session, one recording, one probe and one FreeViewing experiment, as in the
sample, cut down to:

* **one window of 143.9 s**: µs `[5226928854, 5370814965)` relative to
  `session_start_datetime`, from the start of trial 215 to the end of trial 225 (11
  consecutive valid trials). Every `timestamp` in EDI uses that one clock, so the same window
  applies to every time-keyed table;
* **32 units**: the units with the most spikes in the window, among those whose
  `labels.bombcell_label` is not `noise`;
* the trials that overlap the window, plus any trial a kept row references;
* every other table whole, except `sample_info`, which describes the whole sample
  (`n_units` 100, its unit filter and sources). `copy` records the source's `sample_info`
  row in the provenance metadata of the project's `meta.edi_tutorial_build` table instead,
  with the source project, window, units, trials and row counts.

Values are copied unchanged (developer, November 11, 2026). Columns that summarize the
whole sample therefore still describe the whole sample, not the subset:

* `experiment`: `total_trials` (873), `correct_trials` (619), `incorrect_trials`,
  `experiment_start_timestamp`, `experiment_stop_timestamp`;
* `recording`: `recording_start_timestamp`, `recording_end_timestamp`;
* `session`: `session_end_datetime`;
* `lfp_acquisition`: `lfp_n_samples` (6,866,378), `recording_start_timestamp`;
* `unit`: every `quality_metrics` field (for example `num_spikes`, `firing_rate_hz`,
  `presence_ratio`), the `rolling_quality_metrics` series over the whole session, and the
  `waveform_metrics`;
* `sample_unit`: `firing_rate_hz`, `presence_ratio`, `fano_factor_detrended`.

204,948 rows, 361 MB uncompressed. The largest tables are `lfp_timeseries` (86,331 rows,
154 MB) and `experiment_screen_capture_frame_270p` (4,312 JPEG frames, 177 MB).
`lfp_timeseries`, `experiment_display_params`, `experiment_freeviewing_params` and
`experiment_screen_capture_event` hold `fixed_size_list` columns, which Orcapod sources
cannot hash until starfix supports the type (ITL-927). Some float columns hold NaN (gaze
positions during blinks, some unit quality metrics); they are copied bit for bit.

| Table | Selection | Rows | Key columns | Required values | Nullable values |
| --- | --- | ---: | ---: | --- | ---: |
| `atlas` | whole | 1 | 1 | `atlas_name` | 3 |
| `atlas_label` | whole | 199 | 2 | — | 3 |
| `chamber` | whole | 2 | 2 | — | 2 |
| `experiment` | whole | 1 | 4 | `task_id`, `task_version` | 5 |
| `experiment_display_params` | whole | 1 | 4 | — | 9 |
| `experiment_freeviewing_event` | window | 4,202 | 6 | `trial_index` | 0 |
| `experiment_freeviewing_params` | whole | 1 | 4 | — | 15 |
| `experiment_freeviewing_trial` | trials | 11 | 5 | `trial_start_timestamp` | 37 |
| `experiment_gaze_online_timeseries` | window | 28,780 | 5 | — | 8 |
| `experiment_screen_capture_event` | window | 17,249 | 6 | — | 15 |
| `experiment_screen_capture_frame_270p` | window | 4,312 | 7 | — | 1 (`frame_jpeg`) |
| `lfp_acquisition` | whole | 1 | 5 | `lfp_channel_ids`, `recording_start_timestamp` | 3 |
| `lfp_location` | whole | 383 | 9 | — | 5 |
| `lfp_timeseries` | window | 86,331 | 6 | `lfp_voltage_uv` | 0 |
| `penetration` | whole | 1 | 4 | `probe_id` | 11 |
| `probe` | whole | 22 | 1 | — | 3 |
| `probe_location` | whole | 1 | 6 | — | 4 |
| `probe_map` | whole | 1 | 4 | — | 6 |
| `recording` | whole | 1 | 3 | — | 5 |
| `sample_unit` | units | 32 | 6 | — | 4 |
| `session` | whole | 1 | 2 | `booth`, `session_end_datetime` | 3 |
| `spike_event` | window + units | 63,349 | 7 | `spike_amplitude_uv` | 0 |
| `subject` | whole | 1 | 1 | — | 3 |
| `task` | whole | 1 | 2 | — | 3 |
| `unit` | units | 32 | 6 | 4 column groups (structs) | 56 fields in the groups |
| `unit_location` | units | 32 | 9 | — | 5 |

## Nullability

`nullability_map.json` records, for every column, the source Arrow type, the
specification's type, the nullability to declare and the foreign keys. Key columns are
always required. Types are the sample's, exactly; only nullability changes. The sample's 24
specification tables match the specification exactly (columns, types, keys and key order).
Every required column holds no nulls in the source. Print the full per-column table with:

```bash
uv run python workshop/edi_tutorial_dataset/edi_tutorial_dataset.py render-map
```

The developer's decisions of November 11, 2026 cover what the specification leaves open.
They live in `decisions.json`, which `derive-map` applies:

* Tables the specification does not define (`experiment_screen_capture_frame_270p`,
  `sample_info`, `sample_unit`): key columns required, other columns nullable, as in the
  source.
* The four column groups of `unit` (`labels`, `quality_metrics`, `rolling_quality_metrics`,
  `waveform_metrics`) are required structs; their fields stay nullable, as the
  specification says.
* `accepted_unresolved` lists references known not to resolve in the source; `plan` and
  `verify` report them instead of failing. It held one entry until the source was fixed:
  58 of the 383 `lfp_location` rows referenced region `White Matter` of atlas
  `AFNI_D99v2`, which `atlas_label` lacked. This was reported to the EDI owners on November 11, 2026
  and fixed at the source the same day (`atlas_label` gained the row, 199 rows); the
  project's `atlas_label` was then copied again.

### What SpiralDB stores

SpiralDB cannot store a non-null column. Probed on the project with pyspiral 0.16.33 on
November 11, 2026, it keeps every top-level column nullable, key columns included, and every
struct field nullable; only list items keep a non-null flag. pyspiral's own
`Schema.from_arrow` already turns a non-null key column into a nullable one, and a null
written into a struct field declared non-null is accepted. So:

* the copy writes each batch with the map's nullability and refuses a null in a required
  column before writing (that table's transaction is aborted). Nothing is coerced;
* `verify` expects the schema SpiralDB can store (the source's types, every column
  nullable except list items) and checks, from the data, that every required column holds
  no nulls;
* Orcapod sources on these tables still show every column as `T | None`. Showing the
  declared nullability needs a record of it in each table's metadata, which Orcapod's
  Spiral connector reads and enforces: ITL-936. `annotate` gives each table that record
  by calling Orcapod's `write_nullability_record` with the exact schema `copy` wrote, after
  checking the table's data for nulls in the declared non-nullable columns. It writes table
  metadata only, never data, and needs an Orcapod that has ITL-936; without it, `annotate`
  stops with an error naming ITL-936.

## Commands

Every command needs `pyspiral` 0.16.33 or newer (older versions do not use the `spiral login`
credentials) and a `spiral login`. Run them from the repository root. `plan`, `verify` and
`orcapod-check` only read; `create`, `probe`, `copy` and `annotate` write, and only to a
project `create` made.

```bash
TOOL="uv run --with pyspiral==0.16.33 python workshop/edi_tutorial_dataset/edi_tutorial_dataset.py"

$TOOL plan                                  # read-only dry run: rows, bytes, nulls, foreign keys
$TOOL create --id-prefix edi-tutorial-v1 --allow-create edi-tutorial-v1 \
    --volume vol_01m6s4jsjeedya88dc8t2ns96q \
    --description "edi-tutorial-v1 · edi-sample-tutorial · ..."   # prints the new project ID
$TOOL probe --target-project ID --allow-writes ID   # how SpiralDB treats non-null columns
$TOOL copy --target-project ID --allow-writes ID --dry-run
$TOOL copy --target-project ID --allow-writes ID
$TOOL verify --target-project ID
$TOOL copy --target-project ID --allow-writes ID --tables atlas_label --replace
                                            # copy one table again after a source fix
$TOOL annotate --target-project ID --allow-writes ID   # needs Orcapod with ITL-936
$TOOL orcapod-check --target-project ID     # after ITL-936: required columns show as T
```

Safety rules:

* The tool reads only from `--source-project` (default `edi-sample-v1-855632`).
* It writes only to `--target-project`, which must be repeated as `--allow-writes`, must
  differ from the source and must not start with `edi-sample-v`; `create` refuses such an
  ID prefix too.
* The target must hold the marker table `meta.edi_tutorial_build`, which only `create`
  writes, into the project it has just made. Every table the tool writes carries the same
  marker in its metadata, and a target holding any table without it is refused.
* `copy` refuses to run while the map has pending decisions.
* Re-running `copy` skips tables already complete for the same subset and the same
  column declarations, and rewrites an incomplete one. A complete table built from a
  different subset or declarations is refused. (The project's tables were stamped before
  this rule excluded foreign keys from the stamp, so a plain re-run there reports them
  stale; use `--tables ... --replace`.)
* `copy --tables T1,T2 --replace` drops those tables (they must carry the marker) and
  copies them again from the source; running it twice gives the same tables. Between the
  drop and the end of the copy, the table is missing.

`verify` checks that the project holds exactly the subset's tables, that each table has the
schema SpiralDB can store for the source's types, that its key columns are the source's (in
order), that required columns hold no nulls, that every foreign key resolves, that the
provenance record matches the subset, and that every row equals the source bit for bit.

To rebuild the map from the specification (reads the source's schemas):

```bash
$TOOL derive-map --spec path/to/edi_schema_spec.md --spec-version 0.3.0 \
    --output workshop/edi_tutorial_dataset/nullability_map.json
```

The tests (`tests/test_workshop/`) run the tool against an in-memory fake of pyspiral that
models the storage behaviour seen live.
