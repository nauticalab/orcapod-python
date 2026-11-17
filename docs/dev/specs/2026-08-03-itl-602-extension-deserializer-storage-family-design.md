# ITL-602: Extension deserializer should tolerate equivalent string/binary layouts

## Overview

The extension-type deserializer (`__arrow_ext_deserialize__` in `make_arrow_extension_type`)
performs a strict equality check between the physical storage type in the file and the
canonical storage type the extension was registered with. This rejects physically-different
but logically-identical layouts — `string`, `large_string`, and `string_view` are all
UTF-8; `binary`, `large_binary`, and `binary_view` are all raw bytes.

Two concrete triggers:

1. **Delta Lake always stores `large_string` columns as `"type": "string"` in its JSON
   schema log.** When the Python Delta reader reconstructs the Arrow schema from the Delta
   log and calls `_deserialize(storage_type=string)`, the strict check fires even on
   freshly-written tables — before any compaction.

2. **delta-rs `optimize.compact()` rewrites parquet files** normalizing the physical type
   to `string` and embedding `ARROW:extension:name` per-field. Reading the compacted files
   calls `_deserialize(storage_type=string)` and raises.

The root cause: commit `882c9f9e` introduced the strict check to prevent *silent
misrouted deserialization* when the same extension name is reused with a genuinely
different storage type (e.g. `large_string` → `large_binary`). The intent was correct but
the check was too blunt — `string`/`string_view`/`large_string` are the same logical type.

## Goals & Success Criteria

- `_deserialize` accepts any member of the UTF-8 string family (`string`, `large_string`,
  `string_view`) when `_storage` is `large_string`, and any member of the binary family
  (`binary`, `large_binary`, `binary_view`) when `_storage` is `large_binary`.
- PyArrow's built-in auto-cast delivers a column with canonical (`large_string` /
  `large_binary`) storage regardless of which variant was on disk — no caller changes needed.
- Cross-family mismatches (e.g. `string` where `large_binary` was registered) still raise
  immediately — that is a genuine semantic error.
- The serialized-metadata check remains fully strict.
- Tests that demonstrate the current failure are added first (red), then the fix makes them
  green.

## Scope & Boundaries

In scope:
- `src/orcapod/extension_types/registry.py` — `_canonical_storage` helper +
  updated `_deserialize` inside `make_arrow_extension_type`.
- `tests/test_extension_types/test_registry.py` — new failing tests (written first), then
  fixed by the implementation.
- `DESIGN_ISSUES.md` — no existing entry; none needed (this is an active fix, not deferred
  work).

Out of scope:
- Changes to `normalize_extension_columns`, `normalize_table_view_types`, or
  `delta_lake_databases.py` — PyArrow's auto-cast already delivers canonical storage, so no
  downstream normalization is required.
- Handling nested extension types (struct fields, list elements) — already out of scope per
  ET1 in DESIGN_ISSUES.md.
- Changing the serialized-metadata check — it stays strict.

## Design

### `_canonical_storage(t: pa.DataType) -> pa.DataType`

Module-level helper in `registry.py`:

```python
def _canonical_storage(t: "pa.DataType") -> "pa.DataType":
    """Return the canonical large-variant for string/binary families; identity elsewhere.

    Maps:
      string / string_view        → large_string
      binary / binary_view        → large_binary
      large_string / large_binary → unchanged (already canonical)
      anything else               → unchanged
    """
    if pa.types.is_string(t) or pa.types.is_string_view(t):
        return pa.large_string()
    if pa.types.is_binary(t) or pa.types.is_binary_view(t):
        return pa.large_binary()
    return t
```

### Updated `_deserialize` (inside `make_arrow_extension_type`)

```python
def _deserialize(cls, storage_type: pa.DataType, serialized: bytes) -> pa.ExtensionType:
    if serialized != _metadata:
        raise ValueError(
            f"Arrow extension type '{_name}': expected metadata "
            f"{_metadata!r} but got {serialized!r}."
        )
    if _canonical_storage(storage_type) != _storage:
        raise ValueError(
            f"Arrow extension type '{_name}': expected storage_type "
            f"{_storage!r} but got {storage_type!r}."
        )
    return cls()
```

Key properties:
- `_storage` is always the canonical large form for built-in orcapod types (`large_string`,
  `large_binary`) so `_canonical_storage(_storage) == _storage` trivially holds.
- Returning `cls()` triggers PyArrow's built-in auto-cast: when the physical data is
  `string` and the returned type has `large_string` storage, PyArrow casts automatically and
  the in-memory column has canonical `large_string` storage. Verified empirically with
  pyarrow 25.0.0.
- The error message preserves the original `storage_type` (not the canonical) for
  maximum debuggability.

### Why no downstream changes are needed

PyArrow's auto-cast means:
- The column returned to callers always has `large_string` / `large_binary` storage,
  regardless of physical file layout.
- `normalize_extension_columns` (used in hashing) sees canonical storage → hashes are
  identical before and after Delta compaction. No spurious cache misses.
- `normalize_table_view_types` in `DeltaTableDatabase` is unaffected (it handles plain
  `string_view` columns; extension columns now deserialize correctly before that step runs).

### Test strategy

Tests are written to **fail against the current code first**, demonstrating the missing
coverage. The implementation then makes them pass.

Failing tests added to `tests/test_extension_types/test_registry.py`:

| Test | What it checks |
|---|---|
| `test_deserialize_accepts_string_for_large_string` | Direct call to `__arrow_ext_deserialize__` with `string` — currently raises |
| `test_deserialize_accepts_string_view_for_large_string` | Same with `string_view` |
| `test_deserialize_accepts_binary_for_large_binary` | Same with `binary` |
| `test_deserialize_accepts_binary_view_for_large_binary` | Same with `binary_view` |
| `test_deserialize_rejects_binary_for_large_string` | Cross-family must still raise |
| `test_deserialize_rejects_string_for_large_binary` | Cross-family must still raise |
| `test_parquet_roundtrip_string_physical_reads_as_large_string` | Write parquet with `string` + extension metadata, read back — currently raises; after fix yields `large_string` column |
| `test_parquet_roundtrip_string_view_physical_reads_as_large_string` | Same with `string_view` |
| `test_metadata_mismatch_always_raises_regardless_of_storage_family` | Metadata check stays strict even if storage is compatible |

## References

- Linear issue: ITL-602
- Introduced by: `882c9f9e` (`fix(extension_types): validate storage_type/metadata in __arrow_ext_deserialize__`)
- Related: ITL-22 (`string_view` kernel gaps in DeltaTableDatabase, already patched with `normalize_table_view_types`)
