# ITL-627: Extension-type metadata dropped for list-backed logical types

**Date:** 2026-08-22
**Issue:** [ITL-627](https://linear.app/metamorphic/issue/ITL-627)

---

## Overview

`ListLogicalType` (ITL-173, PR #251) wraps `list[T]` / `set[T]` columns in an Arrow
extension type (`extension<list[orcapod.path]>`, storage `large_list(large_string)`).
Two downstream paths mishandle that outer extension type:

1. **Join raises** — `list[<T>]` extension columns cannot survive the Polars round-trip
   that `Join` (and `MergeJoin`) perform. After a successful `pl.DataFrame(table)` import,
   `df.to_arrow()` calls `_deserialize` with empty metadata bytes and raises `ValueError`.

2. **Aggregation silently loses content hashing** — `SemanticHashingVisitor.visit_extension`
   falls through to passthrough for `list[File]` extension columns because `list[File]` is a
   `types.GenericAlias`, not an instance of `type`, so the existing guard short-circuits. The
   list elements are therefore hashed as JSON path strings rather than file contents.

3. **MergeJoin drops extension type when aggregating logical-type columns** —
   `MergeJoin.binary_static_process` builds merged arrays via `pa.array(merged_vals)`, which
   infers the element type from raw storage values and produces a plain `large_list(storage_type)`.
   If the colliding column had type `extension<orcapod.file>`, the merged output should be
   `extension<list[orcapod.file]>`, but instead becomes `large_list(large_binary)`. The schema
   prediction in `binary_output_schema` has the same problem: it predicts `list[T]` as a plain
   Python generic alias rather than the proper extension-wrapped form.

---

## Root Cause Analysis

### Defect 1 — Polars round-trip drops metadata bytes

`ListLogicalType.get_polars_extension_type()` calls `make_polars_extension_type` **without**
the `metadata=` argument:

```python
# list_logical_type_factory.py (current — broken)
polars_ext_class = make_polars_extension_type(
    self._logical_type_name,
    self._storage_type,
    # metadata= not passed → defaults to None
)
```

This means `pl.BaseExtension.__init__` stores `metadata=None`, so `ext_metadata()` returns
`None`. When Polars calls `to_arrow()` it exports the extension name correctly but passes
`b''` as the metadata bytes. PyArrow's `_import_from_c` calls `_deserialize` with those
empty bytes:

```
_deserialize("list[orcapod.path]", b'')
# b'' != b'{"category": "list", "element_ext_name": "orcapod.path", ...}'
# → ValueError
```

**Observed stack:** `polars/dataframe/frame.py to_arrow` → `pyarrow Array._import_from_c`
→ `orcapod/logical_types/registry.py _deserialize`.

### Defect 2 — Content hashing bypassed for list-backed extension columns

`SemanticHashingVisitor.visit_extension` checks `isinstance(python_type, type)` before
dispatching to the semantic handler:

```python
# visitors.py (current — broken)
python_type = self._type_converter.arrow_type_to_python_type(extension_type)
if python_type is typing.Any or not isinstance(python_type, type):
    return extension_type, storage_value  # ← list[File] exits here
```

`list[File]` is a `types.GenericAlias`, so `isinstance(list[File], type)` is `False`.
The column passes through unchanged. `normalize_extension_columns` then exposes the raw
`large_list(large_string)` storage to Starfix, which hashes the JSON path strings — not
file contents.

For contrast, `list<extension<orcapod.file>>` (plain list with extension element type)
**already works**: `visit_list` → `_visit_list_elements` → `visit_extension` per scalar
element → `FileHandler`. The fix for the extension-wrapped form must produce identical
output.

### Defect 3 — MergeJoin drops extension type when aggregating logical-type columns

`MergeJoin.binary_static_process` (line 276 of `merge_join.py`) builds merged arrays via:

```python
# merge_join.py (current — broken)
merged_array = pa.array(merged_vals)
```

`merged_vals` is a list of lists of raw storage values (e.g. `[[b"..json..", b"..json.."]]`
for a `File` column). PyArrow infers the array type from the values, producing
`large_list(large_binary)` — the `extension<list[orcapod.file]>` wrapper is never applied.

`binary_output_schema` has the same problem at line 104:

```python
# merge_join.py (current — broken)
merged_data_schema[key] = list[colliding_schema[key]]
```

This produces a plain `list[T]` Python generic alias regardless of whether `T` is a logical
type. The predicted output schema therefore diverges from the actual merged column type for
logical-type columns.

MergeJoin currently has no access to the type converter (`UniversalTypeConverter`). The fix
requires using `get_default_context().type_converter` to resolve the element extension type
and look up the corresponding `ListLogicalType`.

---

## Fixes

### Fix 1 — `list_logical_type_factory.py` (one line)

Pass the metadata bytes (decoded as a string) to `make_polars_extension_type`:

```python
polars_ext_class = make_polars_extension_type(
    self._logical_type_name,
    self._storage_type,
    metadata=self._metadata_bytes.decode("utf-8"),  # ← add this
)
```

With `ext_metadata()` now returning the JSON string, Polars encodes it to UTF-8 bytes on
`to_arrow()`, PyArrow receives the correct bytes, and `_deserialize` validates successfully.
The extension type is preserved through the full Polars round-trip.

Covers both `list[T]` and `set[T]` (same code path, same fix).

### Fix 2 — `visitors.py`

Extend `SemanticHashingVisitor.visit_extension` to detect list-backed extension types and
delegate to element-by-element visiting:

```
extension<list[orcapod.file]>  →  large_list(extension<orcapod.file>)  →  large_list(large_binary)
                                   ↑ virtual type                          ↑ per-element content hashes
```

**Algorithm:**

1. After resolving `python_type = type_converter.arrow_type_to_python_type(extension_type)`,
   check `typing.get_origin(python_type) in (list, set)` AND
   `pa.types.is_large_list(extension_type.storage_type)`.

2. Extract `elem_python_type = typing.get_args(python_type)[0]`.

3. Check `isinstance(elem_python_type, type)` and `type_handler_registry.has_handler(elem_python_type)`.
   If no handler → fall through to the existing passthrough (unchanged behaviour).

4. Get `elem_arrow_type = type_converter.python_type_to_arrow_type(elem_python_type)`.

5. Construct `virtual_list_type = pa.large_list(elem_arrow_type)`.

6. Return `self._visit_list_elements(virtual_list_type, storage_value)`.

`_visit_list_elements` visits each element via `self.visit(elem_arrow_type, item)` →
`visit_extension` scalar path → per-element content hash bytes → returns
`(pa.large_list(pa.large_binary()), [hash_bytes_0, hash_bytes_1, …])`.

**Invariant (symmetry):** the `i`-th element of the returned list is byte-for-byte
identical to the result `visit_extension` would return for the same element in isolation
as a scalar `extension<orcapod.file>` column. This means `extension<list[orcapod.file]>`
and `list<extension<orcapod.file>>` produce identical per-element hash tokens for the
same file contents.

**Recursive correctness:** Fix 2 handles `extension<list[list[orcapod.file]]]>` without
additional code. `visit_extension` detects the outer list, constructs
`virtual_list_type = large_list(extension<list[orcapod.file]>)`, and calls
`_visit_list_elements`. Each inner element is `extension<list[orcapod.file]>`, so
`visit(extension<list[orcapod.file]>, inner_list)` dispatches back to `visit_extension` →
recurses. The result is `large_list(large_list(large_binary))` with per-element hashes
at every nesting level.

### Fix 3 — `merge_join.py`

`binary_output_schema` is already correct: it stores Python types (`list[File]` for a merged
`File` column), and `Schema` is `Mapping[str, Python type]`. No change needed there.

The only site that needs fixing is **`binary_static_process`** — specifically the array
construction after merging. The fix has two parts:

**Part A** — before the Polars round-trip, snapshot the Arrow type of each colliding column:

```python
# Capture Arrow types of colliding columns BEFORE the Polars round-trip.
# The round-trip may strip extension metadata; we need the original type
# to reconstruct the correct list extension type after merging.
colliding_col_types: dict[str, pa.DataType] = {
    col: left_table.schema.field(col).type
    for col in colliding_keys
    if col in left_table.schema.names
}
```

**Part B** — after computing `merged_vals`, build the merged array with the correct type:

```python
# Replace the left column with merged list, drop right column.
elem_arrow_type = colliding_col_types.get(col)
if elem_arrow_type is not None and isinstance(elem_arrow_type, pa.ExtensionType):
    from orcapod.contexts import get_default_context
    tc = get_default_context().type_converter
    elem_python_type = tc.arrow_type_to_python_type(elem_arrow_type)
    list_lt = tc.get_logical_type_for_python_type(list[elem_python_type])
    if list_lt is not None:
        list_ext_type = list_lt.get_arrow_extension_type()
        storage_array = pa.array(merged_vals, type=list_ext_type.storage_type)
        merged_array = pa.ExtensionArray.from_storage(list_ext_type, storage_array)
    else:
        merged_array = pa.array(merged_vals)
else:
    merged_array = pa.array(merged_vals)
```

This handles the nested case naturally: when `elem_arrow_type` is
`extension<list[orcapod.file]>`, `elem_python_type` becomes `list[File]`,
`get_logical_type_for_python_type(list[list[File]])` returns
`ListLogicalType(ListLogicalType(LogicalFile()))`, and the merged array is
`extension<list[list[orcapod.file]]]>` — correct for the `list[File] × list[File]` case.

---

## Tests

### Defect 1 tests

**`test_list_logical_type_polars_ext_carries_metadata`** — `tests/test_logical_types/test_list_logical_type.py`

This is the direct regression test for the root cause. It targets the exact line that was
missing (`metadata=` not passed to `make_polars_extension_type`) without needing the full
join pipeline.

- Instantiate `ListLogicalType(LogicalPath(), is_set=False)`.
- Call `get_polars_extension_type().ext_metadata()`.
- Assert the result is not `None`.
- Parse it as JSON; assert `"category" == "list"` and `"element_ext_name" == "orcapod.path"`.
- With the buggy code, `ext_metadata()` returns `None` immediately — this test fails before
  a single Arrow operation is performed.

**`test_join_preserves_list_extension_column`** — `tests/test_core/operators/test_operators.py`
- Two streams: one with `list[Path]` data column, one with a scalar data column, shared tag.
- Join them via `Join.static_process`.
- Assert no error.
- Assert the output column type is still `extension<list[orcapod.path]>` (not downgraded to plain `large_list`).

**`test_merge_join_preserves_list_extension_column`** — `tests/test_core/operators/test_merge_join.py`
- Two streams: one has a non-colliding `list[Path]` data column and a shared tag; the
  other has a different non-colliding data column and the same tag.
- MergeJoin them — no colliding data columns, so the `list[Path]` column passes through.
- Assert no error and assert the output column type is still `extension<list[orcapod.path]>`
  (same verification as the Join test, exercising MergeJoin's Polars round-trip).

### Defect 2 tests — `tests/test_hashing/test_extension_type_hashing.py`

**`test_list_file_extension_hashed_to_list_of_large_binary`**
- Create an `extension<list[orcapod.file]>` column with two real files.
- Call `visitor.visit(ext_type, storage_value)` for one row.
- Assert `new_type == pa.large_list(pa.large_binary())`.
- Assert `new_data` is a Python list of exactly two `bytes` objects.

**`test_list_file_extension_is_hash_of_file_content_hashes`** ← explicit contract test
- Create two files with distinct content. Build a list storage value `[s0, s1]`.
- Call scalar `visit_extension(orcapod.file_ext_type, s0)` → `h0_bytes`.
- Call scalar `visit_extension(orcapod.file_ext_type, s1)` → `h1_bytes`.
- Call `visit_extension(list_file_ext_type, [s0, s1])` → `list_type, [r0, r1]`.
- Assert `r0 == h0_bytes` and `r1 == h1_bytes`.
- Meaning: the list result at position `i` is byte-for-byte the content hash of file `i`,
  identical to what scalar hashing of the same file produces. The Starfix table hasher
  therefore sees an ordered list of per-file content hashes — a single table hash derived
  from hashing a list of hash values.

**`test_list_file_extension_content_change_changes_hash`**
- Write one file with content `"v1"`, build storage value for the single-element list.
- Compute the list visit result; capture `r0_v1`.
- Overwrite the file with `"v2"`, rebuild `File` (re-validates existence), recompute storage.
- Compute the list visit result; capture `r0_v2`.
- Assert `r0_v1 != r0_v2` — a content change propagates into the per-element hash.

**`test_list_file_extension_same_content_same_hash`**
- Two files at different paths with identical content.
- Assert the per-element hash bytes for each are equal.

**`test_list_file_extension_passthrough_when_no_handler`**
- Use a `SemanticAwarePythonHasher` with an empty registry (no `FileHandler`).
- Assert `visit_extension` for `extension<list[orcapod.file]>` returns the extension type
  and storage value unchanged — the no-handler branch falls through cleanly.

**`test_list_path_extension_passthrough`**
- `extension<list[orcapod.path]>` column (Path has no content handler in the default context).
- Assert passthrough: returned type is still the extension type, data unchanged.

**`test_set_file_extension_hashed_to_list_of_large_binary`**
- Create an `extension<set[orcapod.file]>` column with two real files.
- Call `visitor.visit(ext_type, storage_value)` for one row.
- Assert `new_type == pa.large_list(pa.large_binary())`.
- Assert `new_data` is a list of exactly two `bytes` objects — same invariant as for `list[File]`.
  (`get_origin(set[File]) is set`, covered by Fix 2 condition `in (list, set)`.)

**`test_list_list_file_extension_hashed_recursively`**
- Create an `extension<list[list[orcapod.file]]]>` column: a list of two inner file-lists.
- Call `visitor.visit(outer_ext_type, [[s0, s1], [s2]])` for one row.
- Assert outer `new_type == pa.large_list(pa.large_list(pa.large_binary()))`.
- Assert `new_data[0]` equals `[h0_bytes, h1_bytes]` and `new_data[1]` equals `[h2_bytes]`
  where each `hi_bytes` is the scalar content hash for file `i`. Proves Fix 2 recurses.

**`test_dataclass_with_list_file_field_hashed`**
- Define a Dataclass (or use an existing registered one) with a `files: list[File]` field.
- The Dataclass is stored as a struct Arrow type; the `files` field has type
  `extension<list[orcapod.file]>`.
- Call `visitor.visit(struct_type, {"files": [s0, s1], ...})`.
- Assert the `files` field in the result has type `pa.large_list(pa.large_binary())` and
  the values are content hash bytes — confirms `visit_struct` → `visit` → `visit_extension`
  correctly delegates for nested extension-typed struct fields.

### Defect 3 tests — `tests/test_core/operators/test_merge_join.py`

**`test_merge_join_scalar_logical_type_column_yields_list_extension`**
- Two streams: both have a `File` data column and a shared tag; different files in each stream.
- MergeJoin them (colliding `File` column).
- Assert no error.
- Assert the output `File` column type is `extension<list[orcapod.file]>` (not plain `large_list`).

**`test_merge_join_schema_prediction_for_logical_type_column`**
- Call `output_schema()` on a MergeJoin whose inputs both have a `File` data column.
- Assert the predicted output type for that column is `extension<list[orcapod.file]>`.
- Proves `binary_output_schema` is consistent with `binary_static_process`.

**`test_merge_join_list_backed_column_yields_nested_list_extension`**
- Two streams: both have a `list[File]` data column (type `extension<list[orcapod.file]>`)
  and a shared tag; different file lists in each stream.
- MergeJoin them (colliding `list[File]` column).
- Assert no error.
- Assert the output column type is `extension<list[list[orcapod.file]]]>`.
- Fix 3's algorithm handles this naturally: `elem_python_type = list[File]` →
  `list_lt = ListLogicalType(ListLogicalType(LogicalFile()))` → same code path as scalar case.

---

## Out of scope

- Nested `list[list[T]]` MergeJoin where nesting depth exceeds two (e.g. `list[list[list[T]]]`
  × `list[list[list[T]]]`). Not a practical use case.
