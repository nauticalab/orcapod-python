# ITL-186: streaming Spiral table source, design for decision (November 12, 2026)

Status: proposal. No library code is written. It replaces
`2026-11-11-itl-186-spiral-streaming-source-design.md` where the two differ (the differences
are listed at the end). Decisions are numbered D1 to D10; each has options and a
recommendation. Rulings the developer already gave on November 12 are marked FIXED.

## Summary of the proposal

`SpiralDBTableSource(project, table)` reads no rows when it is built. It reads the schema, the
key columns and Spiral's table id (three metadata calls, about 1 s). Rows are read only when
asked for, in batches of at most 8,192 rows, and each row's record id comes from its primary
key. A read can be restricted with `where=` and `limit=`, which Spiral applies before any row
reaches Orcapod. A cap (`max_rows`, default 100,000) stops a read that would be too large for
a job, with a message that says how to restrict it.

One finding limits what "streaming" can deliver by November 16: the source can stream a table
of any size in bounded memory, but **a pipeline job cannot**, because every node keeps every
row it produced (about 5 KB per row). That is in the nodes, not in the source, and is not
part of the first PR.

## What was measured

Live, read-only, pyspiral 0.16.33, project `edi-tutorial-v1-841226` (largest tables:
`lfp_timeseries` 86,331 rows, `spike_event` 63,349 rows). Scripts: `.untracked/itl186_probe_*.py`.

| Question | Result on `spike_event` (63,349 rows, 8 columns) |
|---|---|
| Metadata without a scan | row count, key columns, table id, last-modified time: 0.4 to 1.5 s per table |
| Does a scan yield batches? | Yes: 2 batches of 14,771 and 48,578 rows. Batch size follows Spiral's stored fragments and cannot be set |
| Time to first batch | 0.02 to 0.24 s; whole scan 0.3 to 0.9 s, 13.2 MB of Arrow |
| `limit=1000` | 1,000 rows, 0.21 MB: applied by Spiral |
| Column selection | one column: 0.25 MB instead of 13.2 MB: applied by Spiral |
| `where=` equality | works on `uint32`, string and timestamp key columns, and as a conjunction |
| `key_ranges=` | **not usable**: a key holding a timestamp raises `TypeError` (datetime and Arrow scalar both); every large EDI table has `session_start_datetime` in its key |
| `asof=` (a table version) | works; `Table.last_modified_at()` gives the current version |
| `Table.to_polars_lazy_frame()` | raises `ImportError` in our lock (`betterproto2` 0.9.1). With 0.10: column selection and `head` are pushed down, but `filter(unit_id == 106)` fails ("Substrait is only capable of representing unsafe casts") |
| `Table.to_arrow_dataset()` | fails on `count_rows` ("Reading key columns without ... column group") |
| `Scan.lazy()` | experimental; refuses a scan with a limit |
| Table id on rename | unchanged (`table_6p9S6T9WzymR` before and after `rename_table`) |
| Table id on drop and recreate, same name | new id (`table_8tEglDEJMS5P`) |

The last two rows used one throwaway table, `scratch.itl186_idprobe`, created and dropped in
`edi-tutorial-v1-841226`. Nothing else was written.

Offline, on spike_event-shaped synthetic data (script in the session scratchpad):

| Step | Speed | Memory |
|---|---|---|
| Build key tokens, record ids, source info and system tags for a batch, in Arrow | 565,000 rows/s | bounded by the batch |
| `iter_data()`: one `(Tag, Data)` pair per row | about 9,000 rows/s (1,000,000 rows in 113 s) | about 10 KB per row of the batch being iterated: +671 MB at 65,536 rows per batch, +90 MB at 8,192 |
| Keeping the pairs (what a job node does) | | about 5.3 KB per row: 200,000 rows = +1,065 MB |

So the proposed read path costs about 90 MB whatever the table size (today: about 4.6 GB at
construction for the 8,087,906-row `spike_event`). Iterating that table row by row would
still take about 15 minutes before any pod runs.

**What a job keeps (read from the code).** `OperatorNode.async_execute` appends every output
row to `collected` (ITL-896). `FunctionJobNode` keeps every output in `_cached_output_datas`
and starts each run by loading all stored records (`_load_cached_entries`). The sync
orchestrator does `list(iter_data())` on the source. The async `Join` keeps every row of both
inputs. With `materialize_results=True` (the default) the orchestrator keeps a further copy.
So on either orchestrator a job over N rows holds about N × 5 KB per node: 100,000 rows is
about 0.5 GB per node, 8,087,906 rows about 43 GB.

## D1. Locator and source id

FIXED: identity comes from how the table is located, never from its rows; Spiral's opaque
table id (with the project id) guarantees uniqueness; a readable form is what a person sees.

**Locator** = `(project id, table id)`. `<dataset>.<table>` is how the user names the table;
it is resolved to the table id when the source is built (`project.table(name).table_id`, no
scan). Not in the locator: credentials, tokens, `overrides` (server URL and other client
options), `batch_rows`, `max_rows`.

**Today.** Spiral's default source id is already `<project>/<dataset>/<table>` (ITL-926), so
two projects do not collide. A table dropped and recreated under the same name does: same
source id, same content hash, same record ids. `PostgreSQLTableSource` and `SQLiteTableSource`
still default to the bare table name (`db_table_source.py:79`), so two databases with a
same-named table of the same schema share identity. Not changed here; listed for a follow-up.

Options for where the table id lives:

* **A (recommended): inside the source id.** Default
  `edi-tutorial-v1-841226/default.spike_event@table_W7C01nkLTmEJ`. Readable and unique
  everywhere the source id goes (source-info tokens, record ids, `SourceRegistry`).
  `identity_structure` keeps its form `(class, schema, source_id)`. Cost: a renamed table gets
  a new source id, so its pipeline records are entered again (function results are still
  reused, because they are found by row content).
* **B: source id is `<project>/<table id>` only**, the readable name shown in `label`, `repr`
  and the saved file. A renamed table keeps its whole identity. Cost: every provenance token
  in stored data reads `edi-tutorial-v1-841226/table_W7C01nkLTmEJ::...`, which no longer says
  `spike_event`.
* **C: readable source id, table id only in `identity_structure`.** Rejected: the content hash
  is unique, but record ids and tokens of a recreated table collide with the old table's.

Under A and B a table dropped and recreated under the same name is a different source: its
pipeline records are entered again, and function results are still reused for rows whose
content is unchanged, because they are found by row content. Under B only, a renamed table keeps its identity.

A `source_id=` given by the user is used as it is, as for every source.

**What moves.** The default source id of Spiral sources changes, so their `content_hash`,
record ids and source-info tokens change. `pipeline_hash` does not. No golden value moves
(no Spiral source is in the golden set). Stores written through a Spiral source so far are
the local ITL-926 verification runs.

## D2. Restrictions given at construction (`where`, `limit`, `asof`)

They select different rows of the same table. Options:

* **(recommended) In the content hash, not in the source id.**
  `identity_structure = (class, schema, source_id, restriction)`, where the restriction is
  the canonical form of `where`, `limit` and an explicit `asof`; with none given it is
  absent, so an unrestricted source hashes as `(class, schema, source_id)`. A row has the
  same record id whatever restriction read it, so results are shared; two jobs with different
  restrictions have different node hashes, so each reads back only its own rows.
* In neither: a job built with `limit=1000` would read back the rows of another job's
  `limit=5000` run from a shared store.
* In the source id: the same row would get a different record id under each restriction.

When no `asof` is given, the source pins the table's current version when it is built, so all
reads from one source object see one version. That pin is not hashed and is not saved: a
loaded job reads the latest version.

## D3. Record identity per row

FIXED: from the primary key, never from row content.

* The token is built from **Spiral's key columns** (always present: Spiral requires a key
  schema, and keys are unique and non-null), not from `tag_columns`, which the user may set
  to fewer, non-unique columns. `record_id_column` is no longer needed for this source and is
  removed from it.
* Token format, options:
  * **(recommended) readable**: `subject_id=Goliath,unit_id=106,...` in key order, values as
    Arrow renders them, with `%`, `,`, `=` and `:` percent-escaped. Escaping is required:
    the real `spike_extraction_hash` values contain `::`, the separator inside source-info
    values. About 290 characters for `spike_event`.
  * short: a hex digest of the key values. Fixed length, but a person can no longer find the
    row from a provenance token.
* Record id stays `uuid5(source_id + "::" + token)`, as for every source.
* A table with no key schema cannot exist in Spiral. If pyspiral ever returns an empty key
  schema the source raises; it never falls back to row position, because position changes
  with every restriction.
* This replaces positional `row_<i>` tokens for Spiral sources, so their record ids change
  (together with D1).

## D4. How rows arrive

One scan per read: `scan(table.select(), where=, limit=, asof=).to_record_batches()`. Each
Spiral batch is cut into slices of at most `batch_rows` rows (default **8,192**; Spiral's own
batches were up to 48,578 rows), normalized to large types, checked against the nullability
record (#308), enriched in Arrow, and yielded row by row. Nothing is accumulated. No
`to_pylist()` of a table, no table hash. Memory: about 90 MB at the default.

Column selection is not offered in the first PR (it changes the schema and so the pipeline
hash; `SelectDataColumns` exists downstream).

## D5. What each public method does

| Method | Behaviour |
|---|---|
| construction, `output_schema`, `keys`, `content_hash`, `pipeline_hash`, `source_id`, `to_config`, `repr` | metadata only, never a scan. Schema from Spiral's schema plus the nullability record |
| `row_count_estimate` (new, read-only property) | Spiral's manifest count for the whole table; no scan |
| `iter_data`, `async_iter_data` | stream batches as in D4 |
| `as_table`, `as_df`, `as_polars_df`, `as_pandas_df`, `as_lazy_frame` | read the (restricted) rows into one Arrow table; `as_lazy_frame` stays `as_polars_df().lazy()` |
| `len` / count | none exists today; none added |

First N rows: `SpiralDBTableSource(project, "spike_event", limit=1000)`. A subset:
`where={"subject_id": "Goliath", "unit_id": 106}` (equality only, combined with "and"; it is
saved in the job file). Key ranges are not offered (see the measurements).

The size guard, options:

* **(recommended) One cap on every read**: `max_rows=100_000`. A read with no `where` whose
  manifest count (or `limit`) is above the cap fails before scanning; a read with `where`
  fails when row `max_rows + 1` arrives. The message gives the count and the three ways out:
  `limit=`, `where=`, or a larger `max_rows` (`None` removes the cap). 100,000 rows is about
  0.5 GB per job node.
* Cap only on the `as_table` family: `job.run()` on the full `spike_event` would then exhaust
  a laptop's memory.
* No cap.

## D6. One class, a sibling, or `PollingSource`?

* **(recommended) `SpiralDBTableSource` itself, with no mode.** It always builds without
  reading and always streams. "Static, whole table" is `as_table()` on a source within the
  cap. It no longer inherits from `DBTableSource` (whose constructor is the eager load);
  PostgreSQL and SQLite sources are untouched.

  ```python
  SpiralDBTableSource("edi-tutorial-v1-841226", "spike_event",
                      where={"unit_id": 106}, limit=None, max_rows=100_000)
  ```
  Saved form (`source_type` stays `spiraldb_table`):
  `{project_id, dataset, table_name, table_id, where, limit, asof, max_rows, batch_rows,
  tag_columns, system_tag_columns, source_id, overrides, + identity fields}`. On load the name
  is resolved again; if it now resolves to another table id, loading raises an error naming
  both ids. Loading reads no rows (today it reads the whole table).
* A mode switch (`streaming=True/False/None`): `None` would pick by table size, which makes
  behaviour depend on data. Not recommended.
* A sibling class `SpiralDBStreamSource(project, table, ...)` with its own `source_type`:
  two classes to teach, and the same table would have two content hashes (class name is
  hashed).
* `PollingSource(SpiralReader(project, table), tag_columns=...)`: `PollingSource` keeps every
  batch, has no end of data, reads on a timer, numbers rows per batch (record ids collide
  across batches), and is not in the load registry. Making it fit changes it for its
  existing users.

Pyspiral's lazy APIs (developer's question): Orcapod should not build on
`to_polars_lazy_frame`, `to_arrow_dataset`, `to_duckdb_relation` or `Scan.lazy()` now. They
fail in the locked environment, filters are not pushed down reliably, and the record id
(`uuid5`) cannot be a lazy Polars expression. `scan(where=, limit=, asof=)` is the stable base.
A lazy frame with pushdown is later work.

## D7. Sync, async and joins

* **Async**: `async_iter_data` reads each batch in a worker thread and the bounded channel
  (64 rows) applies backpressure, so the source holds one batch.
* **Sync**: `SourceJobNode.execute` does `list(iter_data())`; it works and holds every row.
* **Join**: unchanged. It keeps both inputs, and the operator node keeps the output.
* Recommended for the first PR: no orchestrator, node or Join change. The cap of D5 is what
  keeps both orchestrators safe. Releasing a Join side early (the earlier D6) saves little
  while the operator node keeps every output row.

## D8. Caching and re-runs

Second run with a store: the source scans again (the restricted rows, latest version). For
each row the function node finds its record and computes nothing; a row appended to the Spiral
table since the first run has a new key, so it is computed and recorded; existing rows keep
their record ids even when the new key sorts first. This is the issue's "full re-scan"
strategy, and it is enough for the workshop, whose tables are read-only. Watermark reads and
polling for new rows inside one run are not needed for the workshop and wait.

## D9. First PR (before November 16) and what waits

First PR, one class plus the connector's read side:

1. Construction from metadata only; locator, source id and restriction identity (D1, D2).
2. Batch streaming for `iter_data` / `async_iter_data` / `as_table`, key-based record ids (D3, D4).
3. `where=`, `limit=`, `asof=`, `max_rows`, `batch_rows`, `row_count_estimate` (D5).
4. `to_config` / `from_config` with the table id check (D6).
5. Tests on the fake, opt-in live tests, `docs/concepts/sources.md`, changelog fragment,
   design-issue entry.

Waits (each its own issue): bounded-memory job runs (nodes keeping rows), Join buffering,
watermark and polling, column selection, a lazy frame with pushdown, key ranges, the same
lazy treatment for PostgreSQL and SQLite sources and their default source ids, `betterproto2`
in the lock.

Hashes: no golden value moves. Spiral sources' default source id, content hash, record ids
and tokens change (D1, D3); pipeline hashes and every other source are unchanged.

## D10. Tests

Without a connection, on `tests/test_databases/fake_pyspiral.py` extended with `table_id`,
`last_modified_at`, row count, `scan(where=, limit=, asof=)`, several batches, and a scan
counter:

* construction, schema, hashes, `to_config` and load make zero scans;
* streamed rows equal `as_table()` rows (types, nullability, tokens, record ids);
* no batch handed to enrichment exceeds `batch_rows`;
* a row's record id is the same under `limit`, under `where`, and after a row with a smaller
  key is added;
* the cap: early failure, failure while streaming, `max_rows=None`;
* drop and recreate gives a new identity; a saved job whose name resolves to another table id
  fails to load;
* a job on both orchestrators: function pod, Join with a small source, second run computes
  nothing, appended row is the only one computed; save and load.

Opt-in live tests (skipped unless `ORCAPOD_SPIRAL_LIVE_PROJECT` is set), read-only on
`edi-tutorial-v1-841226`: construction of `spike_event` makes no scan and takes under 5 s;
`limit=1000`; one `where`; an async job over `limit=2000`. One write test confined to a
`scratch.` table of that project: append rows between two runs.

## Differences from the November 11 note

* No size-dependent mode (`streaming=None` choosing by row count); one behaviour.
* Record ids from the key (ruling), not global row offsets.
* The table id enters identity (ruling); restrictions enter the content hash.
* `head(n)` is replaced by `limit=`; the dataframe methods work under the cap instead of
  always raising.
* No Join or orchestrator change: measured to be ineffective while nodes keep their rows.

---

# Addendum, November 12, 2026: decisions, the D3 definition, and D2 measured

Developer decisions: **D1 option A**, **D5 cap approved for now** (default 100,000, `None`
removes it), **D6 one class, no mode**. D4, D7, D8, D9, D10 approved by the coordinator.
D3 changed (below). D2 is open; its facts are below.

## D3: the row token is a digest of the primary key

**Does Spiral have its own per-row id?** No. Read from pyspiral 0.16.33: a row is addressed
only by its key (`Table.key(*parts)`, `KeyRange`, `key_table`). The one row-index name in the
package, `__row_idx__`, is a key column the Hugging Face import helper adds when a dataset
has no key. There is no row uuid, row version or hidden row id in scan output.

**Definition.**

* Input: the row's key columns, in key-schema order, as a one-row Arrow table. Each field has
  the key column's name and its Arrow type normalized to the large variant. Each field is
  declared **non-nullable**, whatever Spiral or a nullability record reports (a Spiral key is
  never null). This matters: the Arrow hasher gives a different digest for a nullable and a
  non-nullable field (measured), so without this rule a nullability record written later
  would move every token.
* Hasher: the source's data context's `arrow_hasher.hash_table(...)`, so the algorithm is
  covered by the data context version. `string` and `large_string` input give the same
  digest (measured), and a row gives the same digest in any batch or slice (measured).
* Written form: `ContentHash.to_string()`, the labelled full digest, for example
  `arrow_v1:000002069471f502581c53b8f4efb4400ae7d8a1edf280582bdc8923c6b95acbcd9bfd`
  (79 characters, no `::`). No truncation.
* Source-info value: `<source id>::<token>::<column>`. Record id:
  `uuid5(<source id> + "::" + <token>)`, unchanged recipe.
* A null in a key column raises, naming the table and the column.

**Cost, and one deviation from "in Arrow per batch".** The data context's hasher has no
per-row entry point: its unit is a table. The token therefore needs one `hash_table` call
per row on a one-row slice of the batch. The values never become Python objects, but the
loop is in Python. Measured: 183 µs per row (8,192 rows in 1.5 s), on top of about 110 µs
per row for building the row. 100,000 rows: about 18 s of hashing. A vectorized per-row
digest would need a new hasher entry point (a data context matter); not proposed now.

**One point to confirm.** The labelled token goes into the `uuid5` recipe, so a future
change of the `arrow_v1` label would move record ids. That is the same position the labelled
default source id (`dict:arrow_v1:<hex>`) already has in the recipe.

## D2 measured: two jobs, one store, the restriction in or out of the content hash

Script and full output: `superpowers/specs/itl-186-experiments/d2_restriction_identity.py`,
`d2_output.txt`. A stand-in source over one 10-row table with key-based record ids; pipeline
`source -> double(v)`; job A runs first, then job B, same job name, same Delta store.

**Limit case: A reads k=0..4, B reads k=0..1.**

| | OUT, sync | OUT, async | IN, sync and async |
|---|---|---|---|
| B's function calls | 0 | 0 | 0 |
| B `node.as_table()` / `iter_data()` after `run()` | 0,1 | **0,1,2,3,4** | 0,1 |
| B `node.get_all_records()` | **0,1,2,3,4** | **0,1,2,3,4** | 0,1 |
| B saved and loaded, `get_all_records()` | **0,1,2,3,4** | **0,1,2,3,4** | 0,1 |
| node content hash, A vs B | same | same | different |
| table paths (pipeline hash) | same | same | same |
| stored result rows / pipeline-record rows | 5 / 5 | 5 / 5 | 5 / 7 |

**Where case: A reads even k, B reads k<3.** B computes only k=1 in every variant. OUT: B's
`get_all_records()` returns 0,1,2,4,6,8 (the union), and so does `as_table()` after an async
run; after a sync run `as_table()` returns 0,1,2. IN: 0,1,2 everywhere; 6 result rows,
8 pipeline-record rows.

What this says:

* **OUT.** Nothing is wrong in a value: every row returned is a real row of the table with
  its correct result, and nothing is computed twice. The read-back is larger than what job B
  asked for: it is every row any job with that pipeline has read from that table. Rows sent
  downstream during the run are only B's rows, on both orchestrators.
* **OUT is what happens today.** Two `DictSource`s with the same schema and source id and
  different rows give exactly the OUT columns above, including the difference between
  `as_table()` after a sync run (B's rows) and after an async run (all rows). That
  sync/async difference exists on `main` today and is not caused by this design.
* **IN.** Each job reads back exactly its rows. Results are not recomputed (the result table
  is shared, found by row content). B's pipeline records are written again under B's node
  hash (2 more rows in the limit case), in the same table. Node content hashes differ, so
  the two jobs are different node instances, which is the developer's objection.
* In the code the choice is one function, `SpiralDBTableSource._restriction_identity()`,
  returning nothing (OUT) or the canonical restriction (IN).
