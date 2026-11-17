# Content-Derived Default Record IDs — Proposal

**Issue:** ITL-817
**Date:** 2026-11-09
**Status:** Proposed — not implemented. Needs the developer's decision because it changes stored identities.

---

## Problem

A source row's record ID system tag is `uuid5(source_id :: token)`
(`core/sources/stream_builder.py`). The token is `"{record_id_column}={value}"` when the
caller names a `record_id_column`, and `"row_{index}"` otherwise.

The default is positional. If the same rows arrive in a different order under the same
`source_id`, a moved row gets a different record ID while its user tags and data stay with
it. Everything keyed on the system tags of a moved row then changes, and because those keys
also include a hash of the row's own data, the moved row is treated as a new entry rather
than matched to another row's:

- a `sink_pod` re-delivers every moved row on every run (measured with all rows moved:
  9/9/9 instead of 9/0/0);
- `FunctionJobNode` pipeline entries miss for those rows, so they are recorded again: the
  stored table shows two entries with identical tag values but distinct record IDs (the
  function's stored result for that data is reused, not recomputed).

No result is ever paired with the wrong row.

This is the signature reported in ITL-817. `DictSource`, `ListSource`, `DataFrameSource` and
`DerivedSource` do not accept `record_id_column` at all, so they can only be positional.

## Proposal

When `record_id_column` is `None`, derive the token from the row's content instead of its
position:

```
token = "content=" + arrow_hasher.hash_table(row over all non-system columns).to_hex()
```

`record_id_column`, when given, keeps today's behavior and today's values.

### Duplicate-row rule

Two rows with identical content would get the same token. Disambiguate with an occurrence
index among identical rows, in order of appearance:

```
token = "content=<hash>"        first occurrence
token = "content=<hash>#1"      second occurrence, and so on
```

Identical rows are interchangeable, so which of them gets which index cannot be observed.
The record ID of a row therefore depends only on its content and on how many identical rows
the source holds — not on where it sits.

## Blast radius

What changes, once, for every source built without `record_id_column`:

| Stored value | Effect |
|---|---|
| Record ID system tag (`_tag_record_id::…`) of every row | New value |
| Source-info tokens (`_source_<col>`), which embed the same token | New value |
| `FunctionJobNode` pipeline entries (`base_entry_id` hashes the system tags) | All miss once. The result cache is keyed on input data content hash and still hits, so functions are not recomputed; entries are re-recorded |
| `sink_pod` / `SideEffectPod` completion records | All miss once: **every tracked side effect is delivered again on the first run after the change** |
| `OperatorJobNode` logged rows (row hash covers system tags) | New rows appended beside the old ones |
| `CachedSource` cache (row hash covers system tags) | Old and new rows coexist, so a cached source would return each row twice until the cache is cleared |
| Folded record IDs from `Batch` / `GroupBy` | New value (derived from member record IDs) |

What does not change: `pipeline_hash` (schema and topology only), data `content_hash`,
system tag column names, and every value for sources that already set `record_id_column`.

## Not solved by this alone

`source_id` defaults to the table hash, which is row-order sensitive. A source with a default
`source_id` still changes every record ID when its rows are reordered. Order independence
needs either an explicit `source_id` or an order-insensitive default (for example a hash over
sorted row hashes). That is a second, separate identity change with the same blast radius and
should be decided together with this one.

## Migration options

1. **Switch at the v0.2 cut, no shim (recommended).** Matches the project's pre-v0.1 policy.
   Release note states the one-time re-delivery and tells users to clear `CachedSource`
   caches. Simple, but every tracked side effect fires once more.
2. **Explicit mode, default unchanged.** Add `record_id="position" | "content"` to the
   sources; flip the default in a later release. No surprise re-delivery; users who need
   order independence opt in today. Costs a parameter that outlives its purpose.
3. **Do nothing.** Keep the positional default, now documented, and require
   `record_id_column` (adding it to `DictSource`, `ListSource` and `DataFrameSource`) for
   order-independent identity. Zero invalidation.

## Cost

One row hash per source row at construction. The builder already materializes every row with
`to_pylist()` at the same point.
