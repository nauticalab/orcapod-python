## `src/orcapod/core/datagrams/datagram.py`, `src/orcapod/core/operators/index.py`, `pick.py`

### DG1 — Datagram mutators discard known Arrow types; Index/Pick re-infer every column
**Status:** resolved
**Severity:** high
**Issue:** ITL-834

`Datagram.with_columns` and `update` took the Arrow-native path only when the dict cache was
not loaded. `Index`/`Pick` read `data[column]` first, so they fell into the dict path, which
dropped the Arrow table and rebuilt every column from Python types: `int8 -> int64`,
`float32 -> double`, `list<int8> -> large_list<int64>`, changing the content hash of columns
the operators only pass through. The extracted column was typed from the Python element type
(always `int64` for ints), and the all-rows-skipped path built its empty table from the
predicted Python schema, widening tags too. Two sibling defects of the same class:
`copy(include_cache=False)`, used by every mutator, discarded the meta Arrow table so narrow
meta columns widened on any mutation; and dict-path `with_columns`/`select`/`drop`/`rename`
copied a stale cached `_data_arrow_schema`, so `arrow_schema()` disagreed with `as_table()`
(which let `FunctionPodStream.as_table` silently drop a column of a mutated dict-born row).

**Fix:** `with_columns`/`update` use the Arrow table whenever it is loaded and carry existing
fields verbatim; only genuinely new columns are typed (from `column_types`, which now also
accepts a `pa.DataType` used verbatim) or inferred. The meta Arrow table is carried through
`copy` and the meta mutators keep untouched meta fields. Dict-path mutators reset the cached
Arrow schema. `Index`/`Pick` derive the extracted type from the input column's Arrow type,
keep the in-place column's position, and build the empty result from the input's Arrow
schema. Rule: whatever Arrow types the inputs have are respected; inferring from Python
values is reserved for when no Arrow type is known.

The extracted column's nullability is the same for every row and equals the predicted output
type's: `column_types` also accepts a `pa.Field` (used verbatim, nullability included), and
`Index`/`Pick` declare `NOT NULL` because an element's predicted type is never Optional. An
extracted `None` raises `ValueError` naming the operator, column and row (before, a `None` in
one row and a value in another gave the rows different nullability, so materializing failed in
`pa.concat_tables`). Known limit: the type converter maps `list[int | None]` and
`list[int]` to the same Arrow type and back to `list[int]`, so nullable elements cannot be
declared today (ITL-835 territory).
