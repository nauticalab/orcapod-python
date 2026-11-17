## `src/orcapod/databases/delta_lake_databases.py`

### DT1 — the Delta database gave back other Arrow types than it was given
**Status:** resolved
**Severity:** critical
**Issue:** ITL-928

The Delta writer converts what Delta Lake has no type for: an unsigned integer to the signed
integer of the same width (a value above the signed maximum failed the flush), `timestamp[s]`,
`[ms]` and `[ns]` to `timestamp[us]` (nanosecond digits silently dropped), a zone other than UTC
to UTC; it cannot store `date64`, `time32/64` or `duration` at all. Every read of a Delta database
(function node hot-load and `get_all_records`, a fresh or loaded job, operator `REPLAY`,
`CachedSource`) gave back the converted type. A tag read back that way changes the record ids
computed from it (the base entry id hashes the tag's Arrow table), so a re-run of
`source(uint32 tag) -> pod -> pod` found its results by input hash, computed nothing, and wrote
the downstream node's pipeline rows again: 6 rows for 3.

**Fix:** `databases/delta_arrow_types.py`. Before a flush, a column with such a type at any depth
gets its whole Arrow type recorded in its top-level field metadata (`orcapod:arrow_type`, an
IPC-serialized schema; Delta keeps top-level field metadata, not nested), an unsigned integer is
written as the same-width signed integer bit for bit, and `timestamp[ns]`, `date64`, `time` and
`duration` as their integer value; s/ms timestamps and zones go through Delta's own lossless
conversion. Every read path (`get_all_records`, the snapshot scan, the snapshot extension after a
commit) casts recorded columns back. Logical-type columns are left to their extension metadata.
Tables written before the fix have no recorded type and read as before; such a database must be
rebuilt (developer, November 12, 2026). Not covered: Delta also gives back `fixed_size_binary`
as `binary` and a dictionary column as its value type; those are unchanged.
