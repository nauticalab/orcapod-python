# Row identity today: record id and source-info token per source (factual note, ITL-1154)

Measured on branch `eywalker/itl-1154-v1-hash-definition-a-hash-fed-into-another` (with the
kind-prefixed default ids, `a6c3158e`), November 15, 2026. Nothing here proposes a change.

The same logical row (`subject = "s2"`, column `weight`) was read from each source with no
`source_id` given, under four conditions: the original rows (`s1, s2, s3`); a row appended at
the end; the rows reordered (`s3, s1, s2`); and the original rows re-read "on another
machine" (the same data at another absolute location: another temp directory). A record id
is `uuid5(namespace, "<source_id>::<row identifier>")`; a source-info token is
`<source_id>::<row identifier>::<column>`.

## Row identifier used

| Source | Without `record_id_column` | With `record_id_column` |
|---|---|---|
| `ArrowTableSource` | `row_<n>` (position) | `<column>=<value>` |
| `DictSource`, `DataFrameSource`, `ListSource` | `row_<n>` | not accepted (`TypeError`: the constructor has no `record_id_column`) |
| `CSVSource`, `DeltaTableSource` | `row_<n>` | `<column>=<value>` |
| `SQLiteTableSource`, `PostgreSQLTableSource`, `DBTableSource` | `row_<n>`: **the primary key is not used** (it only becomes the default tag columns) | `<column>=<value>` |
| `SpiralDBTableSource` | `arrow_v1:<hex>` of the row's primary-key columns (a table without a primary key is refused) | not offered |
| `PollingSource` | `row_<n>` **within each fetched batch** | not offered |
| `CachedSource` | the wrapped source's | the wrapped source's |

## What moves for the same logical row

"same" = record id and token unchanged; "moves" = both change.

| Source (default id) | (a) row appended at the end | (b) rows reordered | (c) re-read elsewhere |
|---|---|---|---|
| in-memory (`arrow_table:`/`dict:`/`list:`/`data_frame:` `arrow_v1:<hex>` of the whole table) | moves (the table hash, so the source id, changes) | moves | same |
| in-memory with `record_id_column` (`ArrowTableSource` only) | **moves** (the row identifier is stable but the source id is the table hash) | **moves** | same |
| `CSVSource` (`csv:<path as passed>`) | same | moves (`row_1` becomes `row_2`) | **moves** when the path differs (an absolute path, or a relative path from another directory) |
| `CSVSource` with `record_id_column` | same | same | **moves** when the path differs |
| `DeltaTableSource` (`delta_table:<directory name>`) | same | moves | same (only the directory name is used) |
| `DeltaTableSource` with `record_id_column` | same | same | same |
| `SQLiteTableSource` (`sqlite_table:<table>`) | same | moves (the row order the query returns) | same |
| `SQLiteTableSource` with `record_id_column` | same | same | same |
| `SpiralDBTableSource` (code, not run: needs Spiral) | same | same (rows named by key) | same table id: same; a re-created table gets a new table id: moves |
| `PollingSource` | see the defect below | | same default (hash of `impl.identity()`) |
| `CachedSource` | as the wrapped source | | |

Appending a row anywhere but at the end shifts the positions after it, so with `row_<n>` every
later row moves too.

## Defect found (not in scope; logging needs your yes)

**`PollingSource` gives rows of different batches the same record id.** Each fetched batch is
built on its own (`SourceStreamBuilder.build(..., source_id=...)`, no `record_id_column`), so
`row_<n>` restarts at 0 in every batch. Measured with two one-row batches (`id=1`, then
`id=2`): both rows have record id `38e65193727a…` and the token
`polling_source:semantic_v1:…::row_0::val`. Downstream, the entry ids of the two rows differ
only through their data, and their source-info tokens are identical. Shall I log it in
`design_issues/` and open a Linear issue?
