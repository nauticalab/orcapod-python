## `src/orcapod/core/sources/spiraldb_table_source.py` — Spiral tables as a workshop source (ITL-926)

### SP5 — A key a function pod needed could only be copied into data by reading the whole table
**Status:** resolved
**Severity:** medium
**Issue:** ITL-996

In the EDI tables every time series has `timestamp` in its key, so the time of a spike is a
tag, and a function pod never sees tags. Copying it into data took
`DerivedSource(src, tags_as_data=...)`, which reads the whole origin as soon as its schema or
rows are asked for, as building a job does (7 to 8 seconds per source, measured), and is no
longer a Spiral source, so it loses `limit=`, `where=` and the bounded notebook preview.

**Fix:** `SpiralDBTableSource(..., tags_as_data={key column: new name})` copies the named key
columns into data in Arrow as each batch is read. The copy has the key column's type and field
metadata, non-nullable. Validation is `DerivedSource`'s rule (`validate_tags_as_data`) plus
"the tag must be a key column". The copies are in the schema (content and pipeline hash), the
sorted mapping is in the content hash when given, and the source id and record IDs do not
change.
