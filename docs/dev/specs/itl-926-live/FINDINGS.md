# ITL-926 — Phase 1 findings (November 11, 2026)

Audit of `SpiralDBTableSource` / `DBTableSource` / `SpiralDBConnector`, an end-to-end run of
the workshop path through a disk-backed pyspiral fake (`tests/test_databases/fake_pyspiral.py`),
a pyspiral 0.14.9 → 0.16.33 API check, and one read-only live call against
`edi-sample-v1-855632`. Repro scripts: `superpowers/specs/itl-926-live/` and the session
scratchpad (listed in the plan).

## Live facts (EDI sample V1, read-only)

* Schema dump (developer): **every column is nullable, key columns included** (`utf8?`,
  `u32?`, `vortex.timestamp[µs](i64?)`). Keys are composite: string hashes, `subject_id`,
  `session_start_datetime` (timestamp µs), `u32` indexes. Value types include `u32`/`u64`,
  `f32`, `list(...)`, `fixed_size_list(...)[n]`, structs with nested lists (`unit`),
  `binary` (`frame_jpeg`), `date[days]`.
* pyspiral **0.16.33** (overlay env, repo code unchanged) authenticates with the developer's
  login, lists the 27 tables in 1.2 s and scans `atlas` (limit 3) in 1.2 s.
* pyspiral **0.14.9** (the repo lock) does **not** use that login: it started a device-login
  flow, which was aborted at once.
* A second read-only probe was blocked by the Claude Code permission classifier in this
  terminal, so the per-table Arrow types from a scan are still unknown (in particular the
  Arrow type and timezone of `vortex.timestamp`).

## Findings by severity

### Critical: workshop blockers

1. **All-nullable columns make every function pod with a plain type fail.** With the EDI
   schemas every tag and data column is `T | None`. `@function_pod def f(x: float)` on such a
   stream raises `ValueError: ... column 'x' has type float | None, the function expects float`.
   Pods annotated `x: float | None` work. Repro: `repros/c1_nullable_columns_refuse_plain_pods.py`.
2. **`fixed_size_list` columns crash source construction.** `starfix` 0.4.0 (latest) raises
   `NotImplementedError: Unsupported leaf type: fixed_size_list<...>` from
   `arrow_hasher.hash_table`. General: `ArrowTableSource` fails the same way. EDI tables hit:
   `experiment_display_params`, `experiment_freeviewing_params`,
   `experiment_screen_capture_event`, `lfp_timeseries`. Repro:
   `repros/c2_fixed_size_list_cannot_hash.py`. **Status:** starfix support is implemented
   (nauticalab/starfix#22, ITL-927) and ships in starfix 0.5.0; Orcapod gets it when it
   adopts 0.5.0 (ITL-910).
3. **Large tables cannot be used.** The source reads the whole table eagerly and
   `SourceStreamBuilder` calls `table.to_pylist()` for provenance tokens. Measured: 200k
   spike_event-shaped rows take 0.9 s and add about 210 MB of peak RSS for 8.6 MB of Arrow
   data (about 25×). There is no column projection, row filter or limit on the source.
   `spike_event`, `lfp_timeseries`, `experiment_gaze_online_timeseries` and
   `experiment_screen_capture_frame_270p` are at risk. Repro: `repros/c3_source_memory.py`.
   **Status:** this PR removes the whole-table `to_pylist()` (C3b, byte-identical), so the
   same 200k rows now add about 115 MB; reading large tables at all is the streaming source
   (ITL-186).
4. **pyspiral pin vs. login.** See "pyspiral" below.

### High

5. **Naive timestamp tags become UTC on one read path.** `FunctionJobNode.as_table()` returns
   a `timestamp[us]` tag as `timestamp[us, tz=UTC]`; `get_all_records()` and an in-memory pod
   keep `timestamp[us]`. General (not Spiral); repro `repros/h1_timestamp_tag_becomes_utc.py`.
   Being filed as a separate issue. EDI keys include
   `session_start_datetime`.
6. **Record identity is positional by default.** Inserting a row whose key sorts first
   re-keys every later row, giving duplicate pipeline entries; function results are still
   cached (one call). Repro: `repros/h2_positional_record_ids.py`. `record_id_column` takes one column only, so a composite primary key
   cannot be used, and a non-unique column is accepted silently, giving two rows the same
   record ID (`comp::s=a` twice).
7. **Reading a loaded job.** After `PipelineJob.load(path, store=...)` in a fresh process,
   `nodes[label].as_table()` raises `FunctionJobNode ... unavailable: no function pod and no
   database attached`; `get_all_records()` works. General (CSV control repro).
8. **`get_all_records()` on a `DeltaTableDatabase` store misses rows right after `run()`**
   (1 of 2), while `as_table()` and `InMemoryArrowDatabase` return all of them; a later job
   reads every row back. General; repro `repros/h4_get_all_records_delta_after_run.py`. Being
   filed as a separate issue.

### Medium

9. **Timezone:** SpiralDB drops the timezone of `timestamp[tz]` columns (seen live earlier).
   It cannot be fixed in Orcapod for tables written outside it; document it.
10. **Logical (extension) types do not survive a Spiral source.** An Orcapod `Path` column
    comes back as `large_string`: the connector's KV metadata (ITL-471) stores
    `field.metadata` only (the extension lives in the type), and `DBTableSource` rebuilds
    fields without metadata. The EDI tables have none.
11. **ITL-40:** the asymmetry is fixed at top level (`get_column_info` and `iter_batches`
    both map `string`/`binary` to the large variants). Nested types (`list<string>`,
    struct children) are normalized by neither, which breaks the "normalize recursively"
    rule; hashes are unaffected. **Status:** fixed in this PR: both paths use
    `arrow_utils.normalize_to_large_types`.
12. **Saved source config narrows integer widths:** `tag_schema` / `data_schema` in
    `to_config()` go through Python types, so an `int32`/`uint32` key is saved as `int64`,
    and a `SourceProxy` fallback gets the wrong width. General.
13. **`source_id` defaults to the bare table name.** Same-named tables in two projects or
    datasets (for example, two EDI sample projects) get the same `source_id`, and therefore
    the same record IDs and provenance tokens.
14. **Docs:** credentials live at `typer.get_app_dir("pyspiral")/auth.json`
    (`~/Library/Application Support/pyspiral/` on macOS), not `~/.config/pyspiral/` as the
    docstrings say.

### Low

15. `decimal128` and dictionary columns map to `typing.Any` (a general converter gap; the EDI
    tables have none). Spiral refuses struct-level nulls on write (write side only).

## Verified working (fake; same code path as live)

* `SpiralDBTableSource` with a composite key → `Join` (two Spiral tables on shared string,
  timestamp and `u32` keys; or one Spiral table with a `DictSource`) → function pod →
  `DeltaTableDatabase`. A re-run makes 0 function calls.
* `content_hash` and `pipeline_hash` are identical across processes. System-tag and
  source-info columns are present and correctly named (`<source_id>::row_<i>::<col>`).
* `save` → `PipelineJob.load` in a fresh process: the Spiral source is rebuilt from config
  (a `DictSource` comes back unbound, as designed), and `get_all_records()` returns every row.
* Zero rows and a missing key schema give clear errors. A Join on mismatched key widths
  (`int32` vs `int64`) gives a clear `InputValidationError`.

## pyspiral

* Every symbol and signature the connector uses is identical in 0.14.9 and 0.16.33 on
  Python 3.11, 3.12 and 3.13, except `Table.key_schema`, which now returns a Spiral `Schema`;
  the connector uses only `.names`, which works.
* `uv lock --upgrade-package pyspiral==0.16.33` moves only pyspiral. The Spiral unit tests
  pass on 0.16.33.
* Attendees installing `orcapod[spiraldb]` get 0.16.33 anyway (the floor is `>=0.14.0`).
* 0.14.9 does not use the login written by the 0.16.33 CLI.

Recommendation: move the lock to 0.16.33. Keep the floor at `>=0.14.0`, or raise it to
`>=0.16`.

## Integration tests vs. the workshop path

`tests/test_databases/test_spiraldb_connector_integration.py` covers the connector and
`ConnectorArrowDatabase` round trips plus ITL-471 metadata. It does not cover
`SpiralDBTableSource`, `PipelineJob`, `Join`, caching, save/load, nullability, composite keys,
timestamp keys, or large tables. The `spiral-integration` job needs the `SPIRAL_WORKLOAD_ID`
secret (GitHub OIDC), `vars.SPIRAL_PROJECT_ID` and `vars.SPIRAL_SERVER_URL`
(default `http://api.spiraldb.dev`), and runs on push and `workflow_dispatch` only.

## Live read-only run (coordinator, November 11, 2026, pyspiral 0.16.33)

* `inspect_tables.py`: all 27 tables. The scan types are `string` (non-large), `timestamp[us]`
  with **no timezone**, `date32`, `uint32`/`uint64`, `int16`/`int32`, `float`, `list<...>`,
  `fixed_size_list<...>[n]`, structs with nested lists (`unit`), and `large_binary`
  (`frame_jpeg`). Every field is nullable, key fields included. A 3-row scan takes 0.7–1.7 s;
  `experiment_display_params` takes 9.2 s and `unit` 14.5 s.
* `live_verify.py` read-only on `session,experiment`: all 7 checks pass. Every tag and data
  column is `T | None` (C1 confirmed live), and `content_hash` and `pipeline_hash` are
  identical in a fresh process. Both tables have 1 row in this sample.
* The protobuf `UserWarning` lines on import came from the locked `substrait` 0.26.0
  (protobuf 5 generated code under the protobuf 6 runtime). A fresh
  `pip install orcapod[spiraldb]` resolves substrait 0.30.0 and shows no warning, so attendees
  do not see it. The lock now pins 0.30.0 too.

## The attendee path: Orcapod only, after one `spiral login`

1. **Eager read.** Construction reads the whole table (`DBTableSource.__init__`). For
   spike_event-shaped data, 200k rows take 0.9 s and +210 MB of peak RSS (+115 MB with C3b),
   so `spike_event` (8,087,906 rows) needs roughly 8.5 GB (about 4.6 GB with C3b) and minutes.
   That is not usable on a laptop. `size_probe.py` gives per-table row counts from the
   manifests (no scan) and measured build costs for tables under a cap.
2. **Restricting rows or columns.** Nothing is pushed down to Spiral today: no projection,
   filter, key range or limit. The design for this is
   `superpowers/specs/2026-11-11-itl-186-spiral-streaming-source-design.md` (ITL-186). Any
   restriction that becomes part of the source must be part of its identity.
3. **Nullability.** `db_table_source.py:133` infers nullability from data only when the
   connector returns no column info. SpiralDB always returns column info, so nullability is
   taken from the Spiral schema (all nullable), as the rule requires.
4. **Dataframe methods.** `as_table`, `as_df`, `as_polars_df`, `as_pandas_df` and
   `as_lazy_frame` all work on a Spiral source (fake, same code path), with tags and data and
   no system columns in the default view.
   * Polars types: `Datetime(us, no tz)`, `Date`, `UInt32`/`UInt64`, `List(Float32)`,
     `Binary`, `Struct`.
   * pandas turns `date32` into `datetime64[ms]`.
   * `fixed_size_list` tables fail before any of these methods can run (C2).
5. **Discovery.** There is no source-level helper.
   `SpiralDBConnector(project).get_table_names()` and `.get_pk_columns(table)` are public
   today (`orcapod.databases`) and are what the docs will show. A
   `SpiralDBTableSource.list_tables(project)` helper is a new-API decision.
6. **Setup.** `pip install "orcapod[spiraldb]"` installs the `spiral` CLI (pyspiral console
   script), then `spiral login` once. Nothing else needs Spiral directly.
7. **Errors.**
   * A wrong table, dataset or project all used to say `Table 'x' not found in database.`.
     Now fixed: the message names the project and dataset.
   * Messages for a non-existent project and for no access come from the server and are
     unverified: run `error_messages.py`.
   * Not logged in: pyspiral starts a device-code login and **blocks** until it completes (seen
     with 0.14.9). In a notebook the cell waits on a URL. The docs must say to log in first.

## Decisions applied in the ITL-926 PR (November 11, 2026)

* **Default `source_id`** is `<project id>/<dataset>/<table>` (developer). The project id is
  pyspiral's `ProjectId`: the API resource id, created from an `id_prefix` plus a
  server-generated suffix, and distinct from the display `description`.
  Repro of the problem it fixes: `repros/source_id_collision_across_projects.py`. Before the
  fix, with two projects, the same `subject` table and one store, project B's job
  `get_all_records()` returned 4 rows (A's 2 and B's 2) with nothing to tell them apart.
  After the fix it returns B's 2. Results were never served across projects (they are keyed
  on row data). `PostgreSQLTableSource` and `SQLiteTableSource` still default to the bare
  table name; not changed here.
* **C3b** applied: the 44-case differential is byte-identical, plus a regression test.
* **C1**: no `infer_nullable`. Non-nullable Spiral columns arrive as `T` at every depth,
  through Join, pods and save/load (tests on the fake). `db_table_source.py`'s
  "infer from data" branch runs only when a connector returns no column info, never for
  Spiral, so it cannot override declared nullability in either direction.
* **C2**: starfix will support FixedSizeList (ITL-927); the type is kept intact here.

## Why `unit` (100 rows) takes about 35 s live

Orcapod's share is small: the same 100-row shape (4 structs with 31, 7 list, 10 and 8
fields) builds in 0.41 s in the fake, mostly struct and list hashing. The live 3-row `scan`
of `unit` alone took 14.5 s, against about 1 s for other tables, so the time is the Spiral
scan: likely one column group or fragment fetched after another. `scan_cost_probe.py unit
unit_location` reports column groups, fragments, and scan times for keys only, full, and
full again. Compacting the table may help; that is for the developer or Spiral to judge.

## Found during the CodeRabbit round (November 11, 2026)

* **`map` columns cannot be hashed either.** starfix 0.4.0 raises `NotImplementedError:
  Unsupported leaf type: map<...>` from `arrow_hasher.hash_table`, the same failure as
  `fixed_size_list` (C2), so no source can be built on a table with a map column. The
  connector itself reads maps correctly (large-normalized, key/item names and nullability
  kept; tested). The EDI sample tables have no map columns. Whether starfix 0.5.0 covers maps
  is to check (ITL-927/ITL-910).


## Second live read-only run (coordinator, November 11, 2026)

* `live_verify.py --tables unit,sample_unit --value-column firing_rate_hz`: all 7 checks pass
  (100 calls for 100 distinct values and 100 rows; no calls on run 2; hashes identical in a
  fresh process; the loaded job reads 100 rows).
* Every Delta write prints `deltalake_core::writer::stats` WARN lines ("Skipping column
  __node_content_hash because it's a binary field", ...). Attendees will see them. This is the
  known `RUST_LOG` item held from ITL-821.
* `unit` has 5 column groups (the root group, empty, plus one for each struct column:
  `labels`, `quality_metrics`, `rolling_quality_metrics`, `waveform_metrics`), each 1
  fragment of 100 rows. SpiralDB cannot scan key columns alone ("Reading key columns without
  including at least one column from any column group is currently not supported"), so
  `scan_cost_probe.py` now times one scan per column group instead.
