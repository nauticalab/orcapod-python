## ITL-935 — Logical type identifiers

### LTI1 — Logical type identifiers
**Status:** resolved
**Severity:** high
**Issue:** ITL-935

**Problem:** A logical type was identified in hashes by its name alone, and most built-in
types wrote empty Arrow extension metadata, so there was no way to change a type's stored
layout or hashing without old and new data looking alike. On read, a column loaded as plain
storage was wrapped as the registered type by name, whatever its stored metadata said (a
possible silent misread).

**Fix:** A logical type's identifier is its Arrow extension name plus extension metadata
(developer, November 11, 2026, ITL-938 design). Every built-in type writes `{"version":1}`
(`logical_types/identifier.version_metadata`). The native containers (`list[T]` / `set[T]`
of a logical type) carry no version of their own, by design (developer, same day): they exist
only because Arrow cannot hold an extension type inside a list, their identifier is the
container kind plus the element's identifier, the type hash treats them like any generic
alias, and a change to how they are stored or hashed is a data context change. The
identifier text `<name>;<hex metadata>` (`LogicalTypeIdentifier.text`) replaces the bare name
in the type hash (`TypeObjectHandler`), in function signatures (`canonical_annotation_str`)
and at the start of a value-hashed cell token (`hashing/visitors.py`). On read the stored
metadata must be one of `readable_extension_metadata()`; otherwise
`LogicalTypeIdentifierError`. The built-in types that used to write empty metadata read it
as version 1. The hashed cell of a native container now encodes its elements with a count, a
null marker and length prefixes, which removes a collision between `[]`, `[None]` and
`[None, None]`.

Known limits, follow-ups:
- When pyarrow itself resolves the extension type while reading a file, the error is raised
  inside pyarrow and does not name the column (it names the stored identifier and what is
  readable).
- The Polars extension classes accept any metadata (`ext_from_params` ignores it); the check
  happens on the Arrow side.
- Dataclass and pydantic types keep `{"category": ...}` metadata with no version; their
  identity is reworked in PR #292.
- A third-party logical type registered for a generic alias (its own `list[T]`-like
  container) is hashed in the type and schema hash as origin and arguments, like every
  generic alias, so its own identifier is in the Arrow-side hashes but not in the schema
  hash or the stored path.
- A `DataContext` object passed to a source or pod is replaced by the registered context of
  the same key in parts of the pipeline, so a custom context with extra logical types cannot
  drive a whole pipeline; the path-level effect of a metadata-only change is therefore
  tested at the hash level.
