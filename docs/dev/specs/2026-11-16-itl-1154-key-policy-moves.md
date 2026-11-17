# ITL-1154: key columns in a source's identity, golden moves

Developer decision, November 16, 2026 (choice A): a root source's identity is
`(class name, output schema, source_id, key columns)`, the key columns being the resolved
record-id key columns in key order, `()` for the position. The same element is added to
the sources with their own identity (`ListSource`, `PollingSource`, `SpiralDBTableSource`,
`DerivedSource` with `tags_as_data`). Source ids, record ids, source-info tokens and
pipeline hashes do not change.

## The moves

[`2026-11-16-itl-1154-key-policy-moves.tsv`](2026-11-16-itl-1154-key-policy-moves.tsv)
lists every moved entry with its pinned and new value. **107** entries move, all in the
open (unreleased) revision of data context `v1`; no entry is renamed or removed.

| Group | Moved |
|---|---|
| pipeline | 52 |
| datagram | 34 |
| schema | 20 |
| torch | 1 |
| semantic, arrow | 0 |

Cause, one for all: **the entry contains a root source content hash.**

- 97 are content hashes of sources, streams, nodes and datagrams downstream of a source.
- 10 are derived default source ids built from a hash that contains a root source content
  hash: `derived:semantic_v1:<hex>` of a node's content hash (`node.as_source()`, 2
  entries) and `tags_as_data:semantic_v1:<hex>` of a derivation whose origin enters
  through its content hash (2 `source_id` entries, and the 6 system-tag values that hold
  those ids).
- No pipeline hash, table hash (`arrow`), record id, location or in-memory default
  source id, or source-info token moves.

## How it was checked

1. The golden generator computed every entry with the change (`--stdout`) and the result
   was compared with the pinned files: 107 values differ, no name is missing and none is
   unpinned.
2. The same with only the base rule (`RootSource.identity_structure`) changed, the
   `ListSource` identity reverted: **106** move, equal to the coordinator's independent
   measurement on `0c468f1f`. The 107th, `pipeline:default_source_ids.list.content_hash`,
   is the `ListSource` identity, which defines its own structure.
3. After `--remove` of the 107 entries and `--add`, the golden files were compared key by
   key with the commit before: the 107 values changed exactly as the TSV lists, nothing
   else changed, no entry was removed, and the 18 added entries are the new case
   `schema:key_policy_identity`. Only the open revision of the `v1` history moved.

Against `origin/main`, only one of the 107 is a new move
(`default_source_ids.list.content_hash`): the other 91 that exist on `main` had already
moved in this PR, and 15 are this PR's own additions or renamed entries. The PR's totals
become 849 moved and 112 added.

## New golden entries (18)

`schema:key_policy_identity` pins, for `ArrowTableSource`, `DerivedSource` with
`tags_as_data`, `ListSource`, `SQLiteTableSource` and `PollingSource`, two sources that
differ only in key policy: their content hashes differ and, where a pipeline hash exists
without polling, their pipeline hashes are equal.

## Pins outside the golden folder

Moved by the same cause and re-pinned in the regeneration commit: the auto-select joined
content hash, the async/sync equivalence stream hash, the materialize node content hash and
its stored row hashes (re-recorded with `_broadcast_row_zero`; row a's pre-fix and fixed
hashes are still equal), two SpiralDB fake source content hashes, four struct-layout source
content hashes, the hashing reference's sample and worked example (re-run with
`superpowers/scripts/hash_audit_example.py`), and the step numbering of tutorial 08's
lineage, which follows content hashes.

The saved job fixture `tests/test_pipeline/fixtures/job_format_0_1_0.json` needs no
re-recording: a loaded job's sources are proxies that carry the stored hashes, and its
tests pass unchanged.
