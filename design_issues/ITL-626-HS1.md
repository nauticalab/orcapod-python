## `tests/test_hashing/golden/` — hash stability across releases

### HS1 — No regression test pinned hash values, so a hash could change silently
**Status:** resolved
**Severity:** critical
**Issue:** ITL-626 (consolidates ITL-168)

Schema hashes name system-tag columns, pipeline hashes name database tables, and content and
data hashes key stored records. Any change to a hasher input (a type's canonical name, `Schema`
handling, the Arrow hasher, a record-id preimage) changed these values with no failing test, so
data written by one release could become unreadable or mis-keyed in the next. Between v0.1.4
and v0.2, for example, the semantic hashes of the `Path`, `UUID`, `DataFrame`, `list[Path]` and
`set[UUID]` type objects moved (ITL-638), which renamed the system-tag columns of every source
carrying those types. That change was noticed only by comparing releases.

**Fix:** `tests/test_hashing/golden/` pins about 600 values in committed JSON files, one
generator regenerates them, and every entry is its own test, so a failure names the value that
moved. Coverage: semantic hashes of scalars, collections, type objects and logical-type values;
`Schema` hashes and stored Arrow types for scalar, nested, nullable and logical types
(`Path`, `UUID`, `ndarray`, `DataFrame`, dataclass, pydantic, `list[Path]`, `set[UUID]`);
`compute_schema_hash`, `compute_source_schema_hash`, system-tag column names and
`SourceStreamBuilder` output; `StarfixArrowHasher` table and schema hashes (widths, layouts,
nullability, nesting, column and row order, extension columns); and end-to-end identities
(content and pipeline hashes, system-tag columns, record ids, per-row data hashes, database
table paths) of representative pipelines and a `PipelineJob`. Values are checked in
subprocesses with different `PYTHONHASHSEED` values and were verified identical on Python
3.11, 3.12 and 3.13. The generator's header says when regeneration is legitimate: a changed
value needs a migration assessment and the developer's approval. Not pinned yet: the
`StarfixArrowHasher` gaps tracked in ITL-848.
