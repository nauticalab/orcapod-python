## System tag run stability (ITL-817)

### RS2 — Default source record IDs are positional
**Status:** open
**Severity:** high
**Issue:** ITL-817

Without `record_id_column`, `stream_builder._make_provenance_token` derives a row's record ID
from its position (`row_{index}`). When the same rows arrive in a different order under the
same `source_id`, a moved row gets a different record ID while its user tags and data stay
correct. Every identity built on the system tags of a moved row then changes, and because
those identities also include the hash of the row's own data, the row is treated as a new
entry: a `sink_pod` re-delivers it on every run (all rows when every row moves) and a
function node records a second entry with identical tag values and a distinct record ID. No
result is paired with the wrong row. `record_id_column` alone does not prevent this when `source_id` is left to
default, because the default `source_id` is the table hash, which also changes with row order. This reproduces the signature reported
in ITL-817 exactly; whether it is the reporter's cause depends on how their sources are built.

The behavior is now documented on `ArrowTableSource`, `SourceStreamBuilder.build` and in
`docs/concepts/sources.md`, and `record_id_column` is pinned as order-independent by
`tests/test_pipeline/test_system_tag_run_stability.py`. Changing the default to a
content-derived record ID invalidates stored identities and is proposed, not implemented, in
`superpowers/specs/2026-11-09-itl-817-content-derived-record-ids.md`.
