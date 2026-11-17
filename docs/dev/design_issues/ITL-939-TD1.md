## Tag and data groups — a value wanted as both (ITL-939)

### TD1 — A key column cannot also be a pod input
**Status:** resolved
**Severity:** medium
**Issue:** ITL-939

A name cannot be in both the tag and the data group, so a key that a pod needs as data (EDI
`session_start_datetime`, for elapsed-time arithmetic) could not reach a pod without a
workaround, and two of the obvious workarounds (keeping the column out of `tag_columns`, or
reading the table twice and joining) give silently wrong joins. The developer rejected an
operator for this (an operator must not put new content into the data group) and chose a
source derived from other pods.

**Fix:** `DerivedSource(origin, tags_as_data={tag: data_name})`, also
`node.as_source(tags_as_data=...)`: a new root source over a source or a job node whose data
columns are the origin's plus a copy of each mapped tag, with the tag's exact Arrow type and
nullability, fresh source provenance, a schema-only `pipeline_hash`, and a default `source_id`
`tags_as_data:<label>:<hex>`, the content hash an operator node doing the derivation would have. Reloadable from a
saved job when its origin is. No existing hash or stored schema changed.

Left open, tracked elsewhere: the same-name collisions that fail late in `Join`/`MergeJoin`
and for a pod output key equal to a tag name (ITL-940); the gaps of the plain
`node.as_source()` form, which has no system tags and cannot be saved, and whose zero-row
table before the node has run takes its Arrow types from the Python schema (ITL-956);
`GroupBy` promoting non-key tags into data (ITL-955, fixed separately).
