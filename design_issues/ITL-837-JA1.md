## `src/orcapod/core/operators/join.py` — async path

### JA1 — `Join`'s async path rebuilt rows from Python values
**Status:** resolved
**Severity:** high
**Issue:** ITL-837

`Join.async_execute` merged each matched pair by building `Tag(merged_dict)` and
`Data(merged_dict)` from `as_dict()` values (`_merge_pair_rename` for two inputs,
`_merge_pair_passthrough` and `_rename_sys_tags` for the staggered path of three or more), so
every column's Arrow type was inferred again: `int16` and `int8` became `int64`, nullability
was lost, a `dict` column could not be built, and the Tag/Data content hashes differed from the
sync path for the same rows. `Data.content_hash()` is the input hash a function node stores, so
cache hits did not carry over between sync and async runs.

The developer's standing rules, which the fix follows:

- Never infer an Arrow type from Python values when the input already has one; no bare
  `pa.array(values)` / `from_pylist` without a schema, no hand-written inference table. Where
  no Arrow type is known, use the data context's type converter.
- Arrow types are normalized to their large variants (`arrow_utils.normalize_to_large_types`).
  Width, nullability (top level and nested), struct fields and logical types never change.
  Anything else that disagrees is an error, never a promotion.
- A list of a logical type is the logical list of that logical type.
- Operators take the data context from the stream or the datagrams they are given, never from
  `self.data_context` or a hand-called `contexts.resolve_context`; operators carry no
  converter state.
- `Join` is commutative: async output columns follow the sync path's canonical order.

**Fix:** the async merge builds each row from the inputs' Arrow tables (`as_table()`): tag
columns in canonical order with a shared key taking the right input's field as the sync path
does, data columns stacked with `hstack_tables`, source info merged, system tags renamed as
before. The new `Tag`/`Data` take the left (canonically first) input datagram's
`data_context_key`. A shared tag key whose Arrow types still differ after large-normalization
raises `InputValidationError` on both paths; the sync path used to keep the right input's type
silently. The rule (moved out of `MergeJoin` so both operators share it) is
`arrow_utils.shared_arrow_type`, which takes only the two types and returns the shared large
type or `None`; `core/operators/base.require_shared_arrow_type` raises the error naming the
operator, the column and both types, for every `Join` and `MergeJoin` call site.
`Datagram.copy()` no longer turns a lazy Python schema into an empty one (it broke `schema()`
of async `Index`/`Pick` rows; ITL-859).
`tests/test_core/operators/test_async_sync_equivalence.py` runs every operator with its own
async path through both paths and compares types, nullability, column order, system tags,
content and schema hashes; differences that are not type re-inference are strict xfails:
ITL-838 (`Data.select`/`drop` keep dropped columns' source info), ITL-846 (sync `SemiJoin`
over nested columns; `ArrowHasher.hash_schema` on extension types). The ITL-845 (O3, row-0
source info) xfails were removed once that fix merged; those provenance cases now pass. The
developer's principle behind the shared-key error: Orcapod is type strict and performs no
implicit type expansion or conversion, except normalization to large variants.
Meta columns (ITL-854, ITL-857; developer decision): `Join` drops every input's `Data` meta
(`__`) columns, and the `_source___*` source-info columns of a raw `ArrowTableStream`'s `__`
columns, on both paths. The sync path used to keep them and rename collisions with an index
suffix, which could duplicate a name an input already had (ITL-857). The async path used to
drop them. Nothing downstream of a `Join` reads or stores them, and meta is not hashed.
`output_schema()` leaves the `__` names a raw stream lists as data out of the prediction, and
two inputs that share only a meta name join. How meta columns should be handled across
operators and pods is a separate design question (ITL-867). `MergeJoin` still carries input
meta on its sync path (colliding names are merged into lists like data) and drops it on its
barrier async path (ITL-846). Since ITL-1080 no operator carries input meta, on either path (SA1).
