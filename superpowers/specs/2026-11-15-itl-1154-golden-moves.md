# ITL-1154: golden entries the v1 hash definition moves (measured, for approval)

Measured on November 15, 2026, before any regeneration. Base: `b21467d8` (`origin/main`
`4dd015a4`, #399 merged, merged into the branch). Each change's `src/` diff was applied
**alone** onto the base in a scratch worktree, every golden value was computed
(`cases.compute_all()`), and the result was compared with the pinned `v1` files. Names that
hold a digest (system-tag column names, table paths) are compared with the digest replaced
by `<H>`, so a moved name and its value are one entry.

Additions (`2026-11-15-itl-1154-golden-additions.tsv`, 94 entries, no pinned entry changed by
them): the new cases `schema:row_identifiers` (position, string, composite, timestamp, date and
boolean keys: 54) and `schema:location_default_source_ids` (CSV, Delta and SQLite defaults with
their record ids, and the polling default: 40).

The full list, one row per moved entry (group, pinned name, new name when the name moved,
pinned value, new value, the change(s) that move it), is
`2026-11-15-itl-1154-golden-moves.tsv` in this folder: **848 entries**.

## Moved entries by change

| Change | Commit | Moves alone | What moves, and why |
|---|---|---|---|
| Rule 1: a hash fed into another hash carries its label | `bcd0115b` | 825 | Every value a label feeds: structure hashes holding a nested hash (types, schemas, every `content_hash` / `pipeline_hash` of a stream, node or source, the function `uri`), the names holding those digests (system-tag column names, table paths), row data hashes of value-hashed logical-type columns, entry ids, record ids, default source ids derived from a moved hash |
| Rules 2+3: bytes / UUID tagged; UUID columns hashed as stored | `3a5c97d9` | 16 | `bytes` / `bytearray` / `uuid.UUID` values; the UUID and set-of-UUID column tables and row data hashes |
| frozenset tagged apart from set | `8655ee93` | 33 | frozenset values and a frozenset default; every identity that holds its inputs as a frozenset: `Join`, `MergeJoin`, and function nodes and pods (`(pod, {input})`), with the names and default source ids built on them |
| Fold labels `group_v1` / `batch_v1` | `74db624a` | 4 | the folded `_tag_source_id` values of the `Batch` and `GroupBy` cases |
| D1 + D6: pandas takes `arrow_ordered_v1:` + digest | `4086d0e9` | 12 | pandas DataFrame and Series values; row data hashes of a pandas column |
| D2: directory entries take `sha256:` + digest | `1a6d608e` | 0 | no golden case hashes a directory's content (covered by a unit test that rebuilds the digest) |
| Generic alias with no `__origin__` refused | `7fb983e3` | 0 | unreachable for every `typing` / `collections.abc` alias on 3.11 to 3.13 |
| Every source's default id names its kind (`csv:`, `delta_table:`, `sqlite_table:`, `postgresql_table:`, `db_table:`, `spiraldb_table:`, `polling_source:semantic_v1:`) | `a6c3158e` | 0 | no golden case reads a CSV, Delta, database, SpiralDB or polling source without a `source_id`; the defaults are pinned by unit tests (`test_default_source_ids.py`). Proposed: add golden cases for these defaults and their record ids, as additions |
| System tags (phase 3): the record id is the row identifier (`row_<n>` or the key's JSON), folded ids `<label>:<hex>`, `record_id_columns`, location defaults | `b5eb554b`, `856de423` | 226 (measured as the increment over every switch above: `10f1c4a8` against HEAD, each computed fresh) | 215 record-id system-tag values (row identifier text; `batch_v1:` / `group_v1:` folded ids); 3 stored entry-id lists of function tables (the system tags are in the entry-id preimage); the `tags_as_data` derived source with a key column: its default source id (the derivation hash now holds `("record_id_columns", (...))`) on 4 entries and its content hash; the 3 `source_stream_builder` stream content hashes (a producer-less stream's identity covers its whole table, system tags included). Every one of the 226 is also moved by an earlier switch (by its name, which holds a moved schema digest), so the total stays 848. No row data hash and no plain table hash moves |
| All together | | 848 | exactly the union of the rows above: no entry moves only through an interaction |

Overlaps: 31 entries are moved by both rule 1 and frozenset, 5 by rule 1 and rules 2+3, 4
by rule 1 and the folds, 2 by rule 1 and D1+D6.

## Each change moves only its expected set

- **Rule 1 moves exactly the values a label feeds.** In a fresh process with every label
  renamed (`label_feeds.py renamed`), the set of golden entries whose value or name changes
  is identical, key for key, to the set rule 1 moves (825 = 825). And
  `test_golden_values_move_exactly_where_a_label_feeds_them` checks, entry by entry, that a
  value moves under renamed labels if and only if a digest in it embeds a renamed label's
  hash (recorded from the semantic JSON trees, the logical-type cells and the labelled hash
  columns of every Arrow hash).
- **Under rule 1 alone no plain-column table hash moves.** In the `arrow` group the one
  moved entry is `extension_table.extension.uuid` (a value-hashed UUID column, before
  rule 3). The 14 row data hashes that move are all of value-hashed logical-type columns:
  `dataframe`, `ndarray`, `polars_dataframe`, `polars_series`, `uuid`, `set_uuid`, torch.
  Every plain-column `arrow` table, schema and row data hash keeps its value.
- **Rules 2+3** move only bytes, bytearray and UUID values and the UUID / set-of-UUID
  columns. **frozenset** moves only frozenset values and the identities built on a
  frozenset. **Folds** move only the four folded source ids. **D1+D6** move only pandas
  values and pandas cells.

## Migration note

Nothing to migrate beyond what v0.3 already requires. No release has shipped data context
`v1` (`std:v1:default`): its golden files are unreleased, and v0.3.0 is its first release.
Every value above is a `v1` value that changes before it is first released. Against
v0.1.4 / v0.2, v0.3 already changes every hash and every stored path (new data context,
new table layouts; ITL-944), so data written by an earlier release is already reported as
stale and recomputed, and data written by a development build of `v1` is recomputed the
same way: a moved entry id or path is not found, and the node computes again. Nothing is
lost or read wrongly, with one exception: a side-effect node (a sink) finds its completed
deliveries by entry id, so deliveries logged by a development build of `v1` are not
recognized and a run delivers them again (the changelog says so). The regeneration replaces the 848 entries in the unreleased `v1`
files with `--remove` (their pinned names) then `--add` (the new values); no other entry
changes and nothing else is added.
