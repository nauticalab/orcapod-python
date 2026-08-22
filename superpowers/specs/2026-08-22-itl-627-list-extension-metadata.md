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
   If it is not a `pa.ExtensionType` → fall through.

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

---

## Tests

### Defect 1 tests — `tests/test_core/operators/test_operators.py`

**`test_join_preserves_list_extension_column`**
- Two streams: one with `list[Path]` data column, one with a scalar data column, shared tag.
- Join them via `Join.static_process`.
- Assert no error.
- Assert the output column type is still `extension<list[orcapod.path]>` (not downgraded to plain `large_list`).

**`test_merge_join_preserves_list_extension_column`** (in `test_merge_join.py`)
- Two streams: one has a non-colliding `list[Path]` data column and a shared tag; the
  other has a different non-colliding data column and the same tag.
- MergeJoin them — no colliding data columns, so the `list[Path]` column passes through.
- Assert no error and assert the output column type is still `extension<list[orcapod.path]>`
  (same verification as the Join test, exercising MergeJoin's Polars round-trip).

### Defect 2 tests — `tests/test_hashing/test_extension_type_hashing.py`

**`test_list_file_extension_hashed_to_list_of_large_binary`**
- Create a `extension<list[orcapod.file]>` column with two real files.
- Call `visitor.visit(ext_type, storage_value)`.
- Assert `new_type == pa.large_list(pa.large_binary())`.
- Assert `new_data` is a list of two `bytes` objects.

**`test_list_file_extension_content_change_changes_hash`**
- Two runs: same file path, first with content `"v1"`, then `"v2"`.
- Assert the per-element hash bytes differ.

**`test_list_file_extension_same_content_same_hash`**
- Two files at different paths with identical content.
- Assert their per-element hash bytes are equal.

**`test_list_file_element_hash_matches_scalar_hash`** ← explicit contract test
- Scalar `orcapod.file` column with one file → scalar `visit_extension` → produces `hash_bytes_scalar`.
- `extension<list[orcapod.file]>` column with the same single file as a list `[file]` → `visit_extension` → produces `[hash_bytes_list_elem]`.
- Assert `hash_bytes_list_elem == hash_bytes_scalar`.
- This pins the symmetry invariant: a file inside a list hashes the same way as a standalone file.

**`test_list_file_extension_passthrough_when_no_handler`**
- Use a `SemanticAwarePythonHasher` with an empty registry (no `FileHandler`).
- Assert `visit_extension` returns the extension type and storage unchanged.

**`test_list_path_extension_passthrough`**
- `extension<list[orcapod.path]>` column (Path has no content handler).
- Assert passthrough: returned type is still the extension type, data unchanged.

---

## Out of scope

- `set[T]` semantic hashing — Defect 2 fix covers it mechanically (same code path), but no
  additional tests are added beyond what is listed; `set[File]` is not a practical use case.
- Nested `list[list[T]]` — not supported by `ListLogicalType` and not addressed here.
- Defect 2 for `MergeJoin` — `MergeJoin` builds new list columns from Python values via
  `pa.array(merged_vals)`, which produces a plain list type (no extension wrapper), so the
  existing `_visit_list_elements` path already handles it correctly.
