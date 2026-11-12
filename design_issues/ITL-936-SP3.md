## `src/orcapod/databases/spiraldb_connector.py` — SpiralDB stores every column nullable (ITL-936)

### SP3 — Non-nullable columns arrive as `T | None` from SpiralDB; a null in a non-null struct field is stored as 0
**Status:** in progress
**Severity:** high
**Issue:** ITL-936

Live, pyspiral 0.16.33, scratch table in `edi-tutorial-v1-841226` (November 11, 2026): a
table written with `k`, `v`, struct `g` and its field `g.x`, and list `l` with its items all
declared non-null reports `Schema({k=i64?, l=fixed_size_list(f32)[2]?, v=i32?, w=i32?,
g={x=i32?, y=i32?}?})`: every top-level column and struct field nullable; only list items keep
`not null`. Writing a null to top-level `v` declared non-null is refused by Arrow on the
client. Writing `{k: 4, v: 40, g: {x: null, y: null}}` with `g.x` declared non-null is
**accepted and reads back as `g.x = 0`**: silent data corruption on SpiralDB's side.

**Fix:** Orcapod records each table's non-nullable paths in the table's key-value metadata
(`__orcapod_nullability__`, format `orcapod.nullability` v1; `orcapod.databases.
write_nullability_record` / `read_nullability_record`). `SpiralDBConnector` writes the record
before the first rows of a table, checks every non-nullable path for nulls before any write,
refuses a write that disagrees with the record, and on read restores the record in
`get_column_info` and checks and restores every batch in `iter_batches`
(`NullInNonNullableColumnError` / `NullabilityRecordError`, naming the table). Tables without
a record are unchanged. The pyspiral fake now reports nullability as the server does. Open:
the corruption itself is SpiralDB's (to raise with Spiral).

Live check (coordinator, `superpowers/specs/itl-936-live/live_nullability.py write`): the
connector's nullability record, refusals, a Join and save/load pass. Also found, for a
top-level struct column (nested structs not tested): SpiralDB cannot store a null struct.
Written without `push_down_nulls`, pyspiral refuses it (`Spiral does not support null column
groups. Found a null in the column named "g".`); with `push_down_nulls=True`, `g = null` reads
back as `g = {x: null, y: null}` with the struct valid (value and hash change). Null lists,
null list items and null struct fields survive. The connector now refuses a null struct at any depth
with `UnsupportedValueError` before writing, and never passes `push_down_nulls`.

If the rows of a table's first write fail, the connector removes the nullability record it
wrote just before them (a record naming columns the table lacks would make every read, and
every result-store flush, raise). A record whose chunks were written but whose header was not
is no record.

Writers are not coordinated (SpiralDB has no lock; metadata writes are not versioned). The
rollback removes the record only while the table still has no value columns, but if another
writer's first rows land between that check and the removal, the table is left without a
record: it reads as `T | None` (never a hidden null) and is repaired with
`write_nullability_record` after checking the data.

Known limitation: the connector treats "the table has no value columns yet" as "the table has
no rows yet", to avoid a scan on every first write (a live Spiral scan costs seconds even for a
few rows). A table whose columns are all key columns, already holding rows without a record,
is recorded by the next write; if that write adds value columns declared non-nullable, the
earlier rows hold nulls there and every read raises `NullInNonNullableColumnError` (a null is
never hidden). Follow-up idea, unverified: a manifest-only row count
(`spiral.core.manifest_stats(snapshot).key_space_stats.total_rows`, see the ITL-186 design)
could replace the shortcut cheaply once confirmed live.
