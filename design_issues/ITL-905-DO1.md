## `src/orcapod/semantic_types/canonical_order.py` — dict and set values in canonical order

### DO1 — A `dict` value's insertion order, or a `set`'s iteration order, changed its row hash
**Status:** resolved
**Severity:** high
**Issue:** ITL-905

A `dict[K, V]` value is stored as `large_list<struct<key, value>>` and a plain `set[T]` as
`large_list<T>`. The converter wrote both in Python iteration order, so `{"a": 1, "b": 2}`
and `{"b": 2, "a": 1}` had different row hashes (and missed each other's function pod cache
entries), while the semantic hasher, which sorts dict keys, gave them the same hash. A
`set[str]` was stored in an order that depends on `PYTHONHASHSEED`, so its row hash changed
from process to process. `set[<logical type>]` already sorted (`ListLogicalType`).

**Fix:** the developer ruled (November 11, 2026) that a dict and a set are order independent,
that Orcapod sorts them before storing them into Arrow, and that hashing an Arrow table never
sorts anything (starfix ignores only column order and struct field order). The converter's
`dict` branch sorts entries by storage key and a new `set` branch sorts elements, both with
the one ordering rule in `semantic_types/canonical_order.py` (None first; numeric; `str` by
code point; `bytes` bytewise; dates and times naturally). NaN, incomparable values and two
keys or elements with the same storage value raise `CanonicalOrderError`, wrapped with the
column name by the caller (decided November 12, 2026). `Datagram` built from Python values
holds key-ordered copies (also decided November 12, 2026) of
its dict values (inside `list`, `dict`, `Optional`), and `update` and `with_columns` do the
same, so a newly computed output and one served from the cache iterate alike (v0.3 reads no
table `0.2.0` wrote, so no row in `0.2.0`'s insertion order is ever served); the caller's dict
is never mutated. Set inputs written through an existing Arrow list schema also use the
same ordering rule, preserving the exact field; list inputs retain their supplied order.
On top of the v0.3 hash step, hashes move only for values not already
in canonical order (two golden entries, `pipeline:index_pick.index` row data hashes); see
the `0.3.0` changelog. This storage order is a separate layer from the semantic hasher's
ordering of its hash input (ITL-942), which sorts by canonical JSON text and stores nothing. Regressions: `tests/test_hashing/test_dict_order_hash.py` (including a
Delta store written by `0.2.0`, which v0.3 does not read) and `tests/test_hashing/test_set_order_hash.py`. Open: a
key/value list that arrives already in Arrow form keeps its stored order and hash, by the
same ruling; dicts inside dataclass or pydantic objects keep their own order in memory
(their stored and hashed form is canonical). A fresh user-class instance iterates its
nested dict in insertion order, while a read-back instance iterates it in key order;
an order-sensitive pod can observe that difference for equal hashed inputs. Reconstructing
user-class instances is outside this fix (coordinator decision, November 12, 2026).
`test_dataclass_dict_storage_is_canonical_but_fresh_object_is_unchanged` pins equal stored
values and hashes, the fresh/read-back iteration difference, and caller non-mutation.
