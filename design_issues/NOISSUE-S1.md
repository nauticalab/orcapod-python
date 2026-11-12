## `src/orcapod/core/sources/`

### S1 — `source_name` and `source_id` are redundant and inconsistent
**Status:** resolved
**Severity:** high

`RootSource` defines `source_id` (canonical registry key, defaults to content hash).
`ArrowTableSource` defines `source_name` (provenance token prefix, defaults to `source_id`).
These are intended as the same concept — a stable name for the source — but they're two
separate parameters that can silently diverge:

- **Provenance tokens** embed `source_name` (e.g. `"heights::row_0"`)
- **SourceRegistry** is keyed by `source_id`
- If they differ, provenance tokens cannot be resolved via the registry

Delegating sources make this worse:
- `CSVSource` sets `source_name = file_path` but never sets `source_id` → registry key is a
  content hash while provenance tokens use the file path
- `DeltaTableSource` sets `source_name = resolved.name` but never sets `source_id` → same issue

Additionally, delegating sources all return `self._arrow_source.identity_structure()` which is
`("ArrowTableSource", tag_columns, table_hash)`. This means the outer source type (CSV, Delta,
etc.) is invisible to the content hash, and `source_id` (defaulting to content hash) will be
identical for a CSVSource and an ArrowTableSource with the same data.

**Fix:** Dropped `source_name` entirely. `source_id` is now the single identifier used for
provenance strings, registry key, and `computed_label()`. Delegating sources set `source_id`
to their meaningful default (`CSVSource` → `file_path`, `DeltaTableSource` → `resolved.name`).
All delegating sources now pass `source_id=self.source_id` to their inner `ArrowTableSource`.
Added `computed_label()` to `RootSource` returning `_explicit_source_id`.
