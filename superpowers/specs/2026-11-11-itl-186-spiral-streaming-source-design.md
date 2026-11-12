# ITL-186 — Streaming (dynamic) SpiralDB table source: design proposal

Status: proposal for the developer's decisions (November 11, 2026). Not implemented. Written
under ITL-926 (workshop re-verification); the implementation is ITL-186, its own PR.

## Problem

`SpiralDBTableSource` reads the whole table when it is constructed
(`DBTableSource.__init__`: `list(connector.iter_batches(query))`) and then builds every
provenance token through `table.to_pylist()`. In the EDI sample, `spike_event` has 8,087,906
rows; `lfp_timeseries`, `experiment_gaze_online_timeseries` and
`experiment_screen_capture_frame_270p` are also large. Measured on spike_event-shaped data:
200,000 rows (8.6 MB of Arrow) take 0.9 s and about 210 MB of peak RSS. At that rate an
attendee calling `SpiralDBTableSource(project, "spike_event")` waits minutes and needs
roughly 8–9 GB of RAM, so on a laptop the kernel is likely to die. (Live per-table timings
come from `superpowers/specs/itl-926-live/size_probe.py`, which reads row counts from Spiral's
manifests without scanning.)

The developer's direction: the Spiral source must not read the whole table at once. It
streams batches, like Orcapod's dynamic `PollingSource`, and is used mainly through the
async orchestrator. Attendees work through Orcapod (the source plus its dataframe methods),
not through Spiral directly.

## What exists today

**PollingSource** (`core/sources/polling_source.py`) wraps a `DynamicSourceProtocol`
(`identity`, `schema`, `poll`, `fetch`, `close`, `to_config`/`from_config`).

* **Identity.** `identity_structure` is `(impl.identity(), tag_columns)` and does not depend
  on data. `pipeline_hash` is the declared schema, and system-tag column names come from that
  schema's hash, so no fetch is needed.
* **Accessors.** `iter_data` and `as_table` (and `as_df` etc., which wrap `as_table`) run at
  most one poll/fetch cycle, then return everything accumulated so far.
* **Async.** `async_iter_data` loops (sleep for `interval`, poll, fetch, yield). The loop
  stops on `duration`, on overrun or error limits, or on cancellation, and **never because
  the data ran out**.

It does not fit a finite 8M-row table:

* every batch is kept in memory (`_batches` only grows);
* every sync access re-concatenates all batches;
* a sync run reads one batch only;
* there is no end-of-data signal, and reads are paced by a fixed interval;
* each batch runs through `SourceStreamBuilder` with positional tokens `row_0..`, which
  restart in each batch, so **record IDs collide across batches**;
* each batch pays `to_pylist()` and a full table hash;
* `polling_source` is not in `SOURCE_REGISTRY`, so `PipelineJob.load` leaves it unbound.

Worth reusing: declaring the schema up front (no fetch needed for hashes or system-tag
names), the `async_iter_data` hook that `SourceJobNode.async_execute` already streams from
(`core/nodes/source_node.py:341-370`), and the per-batch schema check.

**pyspiral 0.16.33 scan API.**

* `Spiral.scan(table.select(...), where=, asof=, limit=, key_ranges=)`, then
  `Scan.to_record_batches()`, which yields a sync `RecordBatchReader` **in key order**.
  `to_unordered_record_batches` is the unordered variant. Batch size follows the physical
  fragments; `batch_readahead` prefetches. There is no async API.
* **Versioning.** `Table.last_modified_at()` gives a cheap µs timestamp, and passing it as
  `asof` pins every scan to that version.
* **Cheap size.** `spiral.core.manifest_stats(snapshot).key_space_stats.total_rows` reads the
  manifests only.
* **Resumption.** `KeyRange(begin=last_key.step(), end=Key.max())` via `key_ranges`, or
  reading the remaining `Scan.shards()`. There is no server-side cursor.

**Downstream.**

* The sync orchestrator materializes a source: `list(iter_data())` (`sync_orchestrator.py:91`).
* The async binary `Join` (`_binary_streaming_join`, `core/operators/join.py`) is a symmetric
  hash join that **keeps every row of both sides**, even after one side has closed. Streaming
  `spike_event` against a 2-row `session` table would therefore buffer all 8M rows inside the
  Join.
* `FunctionNode` keeps outputs in memory unless the run uses `materialize_results=False`;
  to be checked.

## Approaches

1. **Recommended: a streaming mode in `SpiralDBTableSource` itself.** The source implements
   `iter_data` / `async_iter_data` as generators over a scan pinned with `asof`, and enriches
   one batch at a time through a small per-batch helper factored out of
   `SourceStreamBuilder`. Small tables keep today's eager behaviour. There is one class to
   learn, the save/load registry entry stays, and hashes match the eager source's.
2. **A `DynamicSourceProtocol` implementation run by `PollingSource`.** This needs finite
   mode, end-of-data, no accumulation, global record IDs, no interval pacing and registry
   support added to `PollingSource`: most of its semantics, which risks its existing users.
3. **A generic streaming `DBTableSource`** over any connector's `iter_batches` (PostgreSQL
   too). This is the right long-term shape, but YAGNI before November 17. Approach 1 keeps
   the batch enrichment generic so that 3 can follow.

## Design (approach 1)

### Construction

Construction reads no rows. It reads the key schema, the Arrow schema (normalized to large
types, as in ITL-40) and the row estimate from the manifests, and pins
`asof = table.last_modified_at()`. Tag columns default to the key columns. The tag and data
schemas are declared from the Spiral schema, so `output_schema`, `pipeline_hash`,
`content_hash` and the system-tag column names never need data.

### Mode

There is a new keyword `streaming: bool | None = None`.

* `None` streams when the manifest row estimate exceeds `max_eager_rows` (default 1,000,000,
  configurable) and loads eagerly otherwise. This is predictable, because the estimate is
  printed in the error and in `repr`.
* `True` or `False` forces a mode.

The eager mode is exactly today's source, plus the C3b and ITL-40 fixes.

### Reading a stream

1. `scan(table.select(), asof=pinned).to_record_batches(batch_readahead=2)`.
2. Re-slice each batch to at most `batch_rows` rows (default 65,536), normalize its types,
   enrich it, and yield its rows.
3. The sync `iter_data` is a plain generator. `async_iter_data` pulls each batch with
   `asyncio.to_thread(reader.read_next_batch)` and yields between batches, so the event loop
   is never blocked by I/O.

Memory is bounded by about `batch_readahead + 1` batches, plus whatever downstream keeps.

### Per-batch enrichment

* Source-info tokens and record IDs are built per batch from the record-ID column values
  only, never `to_pylist()` of the table (the C3b change, byte-identical on the eager path).
* Positional tokens use a **global row offset** carried across batches (`row_{offset+i}`), so
  they equal the eager source's tokens for the same pinned version.
* The table hash, which is only used to default `source_id`, is not computed;
  `source_id` defaults to `<project id>/<dataset>/<table>` in both modes, as in the eager
  source since ITL-926, so same-named tables in different projects never collide.

### Record identity (decision D2, ties to H2)

* **Recommended:** key-based tokens for keyed tables, for example
  `subject_id=m1,session_start_datetime=2026-...,experiment_index=0`, built in Arrow from the
  key columns. Rows keep their identity when other rows are inserted, which matters
  more when streaming, because a re-run can see a newer version.
* Positional tokens remain for tables without a key (with `record_id_column` still available).
* This changes record IDs for keyed Spiral sources, eager mode included, so it is a hash
  decision.

### Identity (decision D1)

* **Recommended:** `content_hash` stays `(class, output_schema, source_id)` as today. Row-level
  data hashes already drive caching, and switching modes or growing the table then moves no
  hash.
* The pinned `asof` is recorded in `to_config` and in `repr`, but it is not hashed.
* Alternative: hash `asof` too, which makes every new table version a new `content_hash` and
  invalidates nothing stored, but it fragments node identities.

### Dataframe interface (decision D3)

* For a streaming source, `as_table`, `as_df`, `as_polars_df`, `as_pandas_df` and
  `as_lazy_frame` raise `ValueError`. The message names the estimated row count and the
  options: `source.head(n)`, filtering first, running the table in a `PipelineJob` with the
  async orchestrator, or `streaming=False` to load it anyway.
* New `head(n=1000)` returns an eager stream of the first `n` rows by key
  (`scan(limit=n, asof=<pinned>)`: the same version as the full source),
  with the same tags, types and record IDs as the full source.
* Small tables behave as today.
* Later: `as_lazy_frame` as a Polars IO-plugin source that pushes projection and predicates
  into the Spiral scan. This is out of scope before November 17.

### Execution

* **Async** (`job.run(orchestrator=AsyncPipelineOrchestrator())`): the source streams
  batches into the channel.
* **Join (decision D6).** `_binary_streaming_join` stops buffering side X once the other side
  has closed, because later X rows can only match rows already buffered on the closed side.
  With the small table closing first, the big side streams through using memory proportional
  to the small side. Output rows are unchanged (verify with
  `test_async_sync_equivalence.py`).
  * **Assumption: the small side closes first.** A small Spiral table, or one that is read
    eagerly, finishes long before a large stream, so this holds in the workshop path.
    Nothing enforces it, though. If the large side's rows arrive before the small side
    closes, they are buffered until it does, so memory grows with the rows that arrived
    first. That is never worse than today's Join, which keeps both sides to the end.
  * **Later work (not part of the approved D6):** buffer only the side the manifest row
    estimate marks as smaller and use the other side only as the probe, or do a lookup join
    against a side within `max_eager_rows`. Either bounds memory whatever the closing
    order.
* **Sync** over a streaming source would materialize it (`list(iter_data())`). The sync
  orchestrator raises a clear error naming the async orchestrator instead (decision D5;
  alternative: allow it and warn).
* Function nodes in such a run should use `materialize_results=False`. Their retention is
  checked in the implementation.

### Save and load

`to_config` adds `streaming`, `max_eager_rows`, `batch_rows` and the pinned `asof`
(informational). `from_config` re-pins to the latest version unless the config sets
`pin_asof=True` (decision D4; recommended default: latest). The registry entry
`spiraldb_table` is unchanged.

### Out of scope for the first version

Resuming an interrupted scan (cursor = last key plus `asof`, `key_ranges` from
`last_key.step()`), `where=`/`columns=` pushdown arguments, polling for new Spiral writes,
the Polars lazy plugin, and the generic streaming `DBTableSource`.

## Smallest deliverable before November 17 (P0)

1. Streaming mode with a pinned `asof`, `iter_data` / `async_iter_data`, global positional
   tokens, the per-batch C3b enrichment, and the `max_eager_rows` / `streaming=` switch.
2. The dataframe guard plus `head(n)`.
3. Join buffer release (D6).
4. Sync-orchestrator guard (D5).
5. Tests through the public interface with the fake, extended with multi-fragment batches,
   `asof`, `limit` and `last_modified_at`:
   * a stream equals the eager source row for row (tags, types, tokens, record IDs, hashes);
   * memory stays bounded on a synthetic 2M-row table;
   * an async Join of a streamed table against a small one;
   * save and load.
6. A live read-only check on `spike_event`: `head`, plus an async run with a cheap pod over
   one session's rows.

If D2 is approved, key-based record IDs replace step 1's tokens.

`lfp_timeseries` stays blocked by C2 (`fixed_size_list` hashing) until Orcapod adopts
starfix 0.5.0 (ITL-910)
whatever this design does.

Fallback if P0 slips: the workshop uses only small tables with the eager source, and the
docs say so.

## Decisions for the developer (with recommendations)

* **D1, identity of a streamed table.** Recommended: no `asof` in `content_hash`.
* **D2, key-based record IDs for keyed Spiral tables.** Recommended: yes, a hash change
  pre-v0.3, applied to the eager mode too.
* **D3, dataframe methods on a large table.** Recommended: raise with guidance, plus
  `head(n)`; the threshold is `max_eager_rows` = 1,000,000, with `streaming=` to override.
* **D4, which version a loaded job reads.** Recommended: the latest version, with an opt-in
  pin.
* **D5, sync run over a streaming source.** Recommended: refuse with guidance.
* **D6, Join releases a side's buffer once the other side closes.** Recommended: yes, as part
  of ITL-186.
* **D7, `max_eager_rows` default and `batch_rows` default.** Recommended: 1,000,000 and
  65,536; tune after the live size probe.
