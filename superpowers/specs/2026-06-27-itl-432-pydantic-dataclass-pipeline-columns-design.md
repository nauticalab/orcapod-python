# ITL-432: Pydantic/Dataclass Models as Pipeline Columns

**Issue:** ITL-432  
**Date:** 2026-06-27

## Overview

Pydantic models and dataclasses cannot flow through Orcapod pipelines as column
types. Two independent defects are responsible:

- **Bug A** — Extension-typed columns crash `ArrowDigester` because no
  normalization step converts live `pa.ExtensionType` columns to the
  storage-type-plus-Arrow-metadata representation that starfix expects.
- **Bug B** — `pl.DataFrame(table).to_arrow()` raises `ValueError` because
  the synthesized Polars extension types carry no metadata, so
  `__arrow_ext_deserialize__` receives `b''` instead of the expected category
  bytes.

Both bugs are self-contained and addressed by surgical changes to
`StarfixArrowHasher` and the two logical type `__init__` methods.

---

## Why the extension type data is already fully captured

Pydantic models and dataclasses are stored as Arrow extension types whose
storage type is a `pa.struct` of the model/dataclass fields, with each field
recursively resolved to an Arrow type. The entirety of the model's data content
is therefore already captured in the extension type's storage value. No
separate semantic handler is needed to hash pydantic/dataclass columns — the
Arrow hashing layer already handles the underlying struct data once the
extension type wrapper is stripped.

---

## Bug A — Extension type reaching `ArrowDigester`

### Symptom

```
TypeError: unhashable type: '_ArrowExt___main___Cfg'
```

### Root cause

`SemanticHashingVisitor.visit_extension()` has a passthrough path: when no
semantic handler is registered for the resolved Python type, it returns the
extension type and storage value unchanged. The extension-typed column then
flows through `StarfixArrowHasher._process_table_columns` as-is and reaches
`ArrowDigester.hash_table`.

`ArrowDigester` has no `pa.types.is_extension()` branch. Extension types fall
through all type guards in `_data_type_to_value` and crash at `if dt in
_simple:` in `_primitive_data_type_string` because `pa.ExtensionType`
instances are not hashable (they override `__eq__` without `__hash__`).

The correct representation for an extension-typed column when stored outside
Python memory (IPC/Parquet) is: the **storage type** for the data, plus
`ARROW:extension:name` and `ARROW:extension:metadata` in **field metadata**.
This is exactly what `ArrowDigester` knows how to process.

### Fix: normalize extension columns to storage type + metadata in `StarfixArrowHasher`

After `SemanticHashingVisitor` processes each column, any column whose
resulting type is still a `pa.ExtensionType` was not handled by the visitor.
At that point, `StarfixArrowHasher._process_table_columns` normalizes it to
the IPC representation: storage type for the array, extension identity in
field metadata.

```python
if isinstance(new_type, pa.ExtensionType):
    # Extension type was not converted by the visitor.
    # Normalize to storage type + Arrow extension metadata (IPC representation)
    # so that ArrowDigester can hash it correctly.
    ext_type = new_type
    serialized = ext_type.__arrow_ext_serialize__()
    new_columns.append(pa.array(processed_data, type=ext_type.storage_type))
    new_fields.append(pa.field(
        field.name,
        ext_type.storage_type,
        nullable=field.nullable,
        metadata={
            b"ARROW:extension:name": ext_type.extension_name.encode("utf-8"),
            b"ARROW:extension:metadata": serialized,
        },
    ))
else:
    new_columns.append(pa.array(processed_data, type=new_type))
    new_fields.append(field.with_type(new_type))
```

The existing `has_extension_metadata` check in `hash_table` already detects
`ARROW:extension:name` in field metadata (not live `pa.ExtensionType` objects),
so `clean_schema_for_hashing` and `ArrowDigester(include_metadata=True)` are
invoked correctly after this change.

**Why not semantic handlers for pydantic/dataclass?** The model data is already
captured in the extension type storage value. Adding semantic handlers that call
`model_dump()` / `dataclasses.asdict()` and re-hash the dict would be redundant.
The storage struct value IS the dict. The IPC normalization approach is simpler
and more general — it handles any extension type with a passthrough, not only
pydantic/dataclass.

---

## Bug B — Metadata loss on Polars round-trip

### Symptom

```
ValueError: Arrow extension type '__main__.Cfg': expected metadata
b'{"category": "orcapod.pydantic"}' but got b''
```

### Root cause

`PydanticLogicalType.__init__` (line 93) and `DataclassLogicalType.__init__`
(line 93) both call `make_polars_extension_type(logical_name, storage_type)`
without passing `metadata`. The Arrow extension type is built with category
metadata (`b'{"category": "orcapod.pydantic"}'`), but the Polars extension
type carries no metadata string.

When `pl.DataFrame(table).to_arrow()` reconstructs the Arrow column, Polars
calls `__arrow_ext_deserialize__` with the Polars extension's metadata string
serialized to bytes — which is empty (`b''`). The strict equality check in
`_deserialize` fails because `b'' != b'{"category": "orcapod.pydantic"}'`.

### Fix: pass category metadata to `make_polars_extension_type`

In `PydanticLogicalType.__init__`:

```python
# Before:
self._polars_ext_class = make_polars_extension_type(logical_name, storage_type)

# After:
self._polars_ext_class = make_polars_extension_type(
    logical_name,
    storage_type,
    metadata=json.dumps({"category": PYDANTIC_CATEGORY}),
)
```

Same change in `DataclassLogicalType.__init__`, using `DATACLASS_CATEGORY`.

After this fix, the Polars extension type's `metadata_str` is
`'{"category": "orcapod.pydantic"}'`. When `to_arrow()` calls
`__arrow_ext_deserialize__`, it passes
`b'{"category": "orcapod.pydantic"}'`, which matches `_metadata` and
succeeds. The resulting table has live extension type columns, which Bug A's
normalization then processes correctly before hashing.

---

## Files changed

| File | Change |
|------|--------|
| `src/orcapod/hashing/arrow_hashers.py` | Normalize extension type columns to storage type + field metadata after visitor passthrough |
| `src/orcapod/extension_types/pydantic_logical_type_factory.py` | Pass `metadata` to `make_polars_extension_type()` in `PydanticLogicalType.__init__` |
| `src/orcapod/extension_types/dataclass_logical_type_factory.py` | Pass `metadata` to `make_polars_extension_type()` in `DataclassLogicalType.__init__` |
| `tests/test_hashing/test_pydantic_dataclass_hashing.py` | New regression tests (see below) |

---

## Tests

New file: `tests/test_hashing/test_pydantic_dataclass_hashing.py`

**Bug A regression — pydantic:**
Build a table with a pydantic model column (registered via the default
context), call `arrow_hasher.hash_table(table)`. Assert no `TypeError` is
raised and a `ContentHash` is returned.

**Bug A regression — dataclass:**
Same as above with a dataclass column.

**Bug B regression — pydantic Polars round-trip:**
Build a table with a pydantic model column, round-trip via
`pl.DataFrame(table).to_arrow()`, call `arrow_hasher.hash_table(round_tripped)`.
Assert no `ValueError` is raised and the hash equals that of the original table.

**Bug B regression — dataclass Polars round-trip:**
Same as above with a dataclass column.

---

## Out of scope

- Adding `__hash__` to synthesized extension types (tracked as follow-up in starfix)
- Semantic handlers for pydantic/dataclass (not needed; storage value IS the data)
- Schema cleaner changes (not needed; Polars metadata fix resolves the underlying cause)
- Deserialization relaxation (no backward-compatibility shims; greenfield project)
- Official starfix extension type support (tracked as separate Linear issue in Starfix v0.4.0)
