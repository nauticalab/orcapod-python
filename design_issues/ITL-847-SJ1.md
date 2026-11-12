## `src/orcapod/core/operators/semijoin.py` — nested columns and string/large_string keys

### SJ1 — `SemiJoin` failed on nested columns and on `string` vs `large_string` keys
**Status:** resolved
**Severity:** high
**Issue:** ITL-847, ITL-829 (first case only)

The sync path used pyarrow's Acero hash join (`join_type="left semi"`), which rejects any nested
column: `ArrowInvalid: Data type list<item: int8> is not supported in join non-key field`. A list,
large_list, struct or struct-backed logical column (a dataclass) on either input failed, as did a
struct or list key. Acero also rejected a key that is `string` on one side and `large_string` on
the other (a `CSVSource` against a `DictSource`, ITL-829). The async path built its lookup from
`as_dict()` values, so struct and list keys were unhashable, and a null key matched a null.

**Fix:** the sync path joins in Polars (`how="semi"`, `maintain_order="left"`) against the right
input's key columns only, then casts the result to the left table's schema, so every column keeps
the left input's exact Arrow type, top-level and nested nullability, and logical type. Before the
join each key goes through `require_shared_arrow_type` (Polars would silently unify `int8` with
`int16`); sync and async raise `InputValidationError` naming the column and both types. A null key
never matches: Polars already does that for scalar keys but treats a null struct or list as equal
to another, so right rows with a null key are dropped first. The async path checks key types on
the first row of each side and builds its lookup from each key's Arrow storage value, frozen into
a hashable form (nulls inside a struct or list compare equal, NaN equals NaN, as in Polars).
Results of inputs that already worked are unchanged (content and pipeline hashes, per-row hashes,
schemas, tables and row order, sync and async). Regressions:
`tests/test_core/operators/test_semijoin_nested.py`; the ITL-846 strict xfail in
`test_async_sync_equivalence.py` is removed, and the SemiJoin meta case now uses nested meta
columns. Open: ITL-829's second case (a non-nullable key against a nullable one, raised by
`schema_utils.intersection_schemas` during input validation) is unchanged and keeps its strict
xfail. The async path checks key types on the first row of each input, because
`async_execute` receives only channels and no stream exposes its Arrow schema without rows.
So when either input is empty (left or right), a key type mismatch that the sync path reports
yields an empty result on the async path instead of an error. `Join`'s async key check
(`_check_shared_key_types`) has the same limit.
