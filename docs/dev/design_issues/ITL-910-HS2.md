## `src/orcapod/hashing/arrow_hashers.py` — Arrow hash format (starfix)

### HS2 — starfix hash format 1 collided on nested lists, null structs and Booleans
**Status:** resolved
**Severity:** high
**Issue:** ITL-910 (fixes the starfix side of ITL-848; ITL-927)

`StarfixArrowHasher` used starfix 0.4 (hash format 1, label `arrow_v0.1`), which lost
information for some inputs, so different data reached the same row hash: nested-list lengths
(`[[[1]], [[2], [3]]]` and `[[[1], [2]], [[3]]]`), a null struct and a struct whose fields are
all null, and non-nullable Boolean columns (`[True]` and `[True, False, False]`); a Boolean
column's hash also depended on chunking. starfix 0.4 also could not hash `fixed_size_list`
data, so a source with such a column failed at construction (ITL-927).

**Fix:** starfix 0.5 (hash format 2, prefix `00 00 02`) under the new label `arrow_v0.2`
(developer's label rule: the label moves whenever any table hash can change). Every
Arrow-derived hash moves; the golden files were regenerated once, explicitly. Order options
stay off. pyarrow's canonical extension types and `hash_schema` on extension columns hash with
their extension identity instead of as their storage type.

Stored rows: under the default `table_scope`, table paths do not move (developer, November
11, 2026: only a change of a table's schema goes into its path; a value-only change stays in
the same table), so a v0.3 node writes into the same tables as a v0.2 store. The first decision
that day was to refuse such a database (`StoreHashLabelError`); the developer reversed it in the
ITL-938 design session: v0.3 does not refuse a v0.2 store. It ignores rows hashed under
`arrow_v0.1`, computes every input again under `arrow_v0.2`, and leaves the old rows in place,
unchanged and distinguishable, for a later migration (ITL-913, ITL-914).

(Superseded before v0.3 was tagged: the label-based read filter described in the next two
paragraphs, `stored_hash_labels` and `SideEffectLogHashLabelError` were replaced by the data
context column and the table layouts, see DC1, ITL-944, and DC2, ITL-954. The text is kept
as the record of what ITL-910 first did.)

Key lookups cannot match a row of another label. Every read of a whole table drops the rows
of another label through one function, `hashing/stored_hash_labels.keep_rows_hashed_under`
(without it: 4 rows for 2 inputs): `FunctionJobNode._fetch_joined_records` (phase 1,
`get_all_records`, `as_table`, `iter_data`, `as_source`, loaded jobs) on
`__pipeline_base_entry_id`; `ResultCache.get_all_records` on `__input_data_hash`;
`OperatorJobNode._read_stored_records` and `CachedSource._read_cached_records` on the starfix
format at the start of the `_record_hash` record id. Each reader logs one warning with the
total number of rows it left out (`report_skipped_rows`; developer: total counts are enough,
eventually old entries should be detected and a migration suggested).
`stored_hash_labels(database, record_path)` counts a table's rows by label.

Side-effect invocation logs are the one exception (developer, same day): they are read by key
only, so deliveries logged under `arrow_v0.1` would not be found and every side effect would
be delivered again. A side-effect node whose log holds such rows raises
`SideEffectLogHashLabelError` before delivering anything. The developer wants a separate,
later discussion of what side-effect logs hold.

Known limits, follow-ups:
- The warning is logged once per reader object (node, result cache, cached source) and
  table, not once per process: stored paths are relative to the database, so a process-wide
  record of reported tables would silence a second store with the same layout.
- A global hash-version identifier stored with every row is being designed (not decided); it
  would also give operator-node and cached-source rows a label.
- Operator-node and cached-source record hashes carry no label, only the starfix format at
  the start of the hex digest. That separates `arrow_v0.1` (format 1) from `arrow_v0.2`
  (format 2) exactly, but a future label that keeps the starfix format could not be told apart
  there. Storing the label with those rows (a schema change) would close that gap.
- The node content hash does not separate the generations: a source with an explicit
  `source_id` gives the same node content hash under both labels.
- A `DeltaTableDatabase` object keeps the table version it read first, so a job object that
  read a table before a run does not see the rows the run added until the database is reopened
  (not specific to hash labels; seen when re-reading a node of the same job after an
  incremental run).
- Tables placed under a content hash (`table_scope="content_hash"`, `CachedSource` without an
  explicit `source_id`) move to new paths when the content hash moves, so their v0.2 rows are
  in another table.
