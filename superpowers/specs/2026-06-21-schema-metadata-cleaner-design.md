# Schema Metadata Cleaner + starfix v0.3.0 Adoption

**Linear:** PLT-1737  
**Date:** 2026-06-21  
**Status:** Approved

---

## Overview

starfix v0.3 introduces a binary `include_metadata` toggle on `ArrowDigester.hash_schema`
and `ArrowDigester.hash_table`. Orcapod only cares about identity-bearing metadata —
specifically `ARROW:extension:*` keys that encode extension type identity. All other
metadata (comments, vendor annotations, provenance, etc.) must not affect the logical
hash identity.

Rather than wait for starfix to grow native filtering, we preprocess the schema before
hashing: a cleaner walks the schema, strips every metadata key that isn't
`ARROW:extension:*`, and the cleaned schema is what gets hashed with
`include_metadata=True`. Schemas with no extension metadata after cleaning are hashed
with `include_metadata=False`, preserving byte-for-byte hash stability with pre-v0.3.0
Orcapod for non-extension-typed pipelines.

---

## Goals & Success Criteria

- `starfix` dependency bumped to `~=0.3.0`.
- `clean_schema_for_hashing(schema: pa.Schema) -> pa.Schema` implemented in
  `src/orcapod/hashing/schema_cleaner.py` as a semi-public utility.
- `has_extension_metadata(schema: pa.Schema) -> bool` implemented in the same module.
- Both `StarfixArrowHasher.hash_schema` and `StarfixArrowHasher.hash_table` use the
  cleaner + `include_metadata` decision.
- Existing golden-value tests pass unchanged (stability invariant holds).
- New test file `tests/test_hashing/test_schema_cleaner.py` covers cleaner correctness.
- New tests in `test_starfix_arrow_hasher.py` cover the hash path (extension-aware
  hashing, mixed metadata, key ordering).

---

## Design

### New module: `src/orcapod/hashing/schema_cleaner.py`

Two public functions:

#### `clean_schema_for_hashing(schema: pa.Schema) -> pa.Schema`

Returns a new schema with all metadata filtered to `ARROW:extension:*` keys only, at
both schema level and per-field level, recursively through nested types. All other
schema/field properties (names, types, nullability) are untouched.

Implementation uses a private `_clean_field(field: pa.Field) -> pa.Field` helper that:

- Filters field-level metadata to keys starting with `b"ARROW:extension:"`.
- For **primitive types**: returns `field.with_metadata(filtered_meta)` directly.
- For **`struct`**: rebuilds the struct type with `pa.struct([_clean_field(f) for f in field.type])`, then applies filtered field metadata.
- For **`list` / `large_list`**: rebuilds with `_clean_field(field.type.value_field)` as the value field.
- For **`fixed_size_list`**: same as list, preserving the list size.
- For **`map`**: rebuilds with cleaned key field and cleaned item field.
- For **`dictionary`**: cleans the value_type (dictionary storage type); index type carries no extension metadata.
- **Fallback** for any other type: filter field metadata, leave the type object untouched.

Schema-level metadata is filtered identically (keys starting with `b"ARROW:extension:"`
survive; all others are dropped; result is `{}` if no extension keys are present).

#### `has_extension_metadata(schema: pa.Schema) -> bool`

Walks the already-cleaned schema. Returns `True` at the first field that has
`b"ARROW:extension:name"` in its metadata. Recurses into nested types identically to
`_clean_field`. Checking the cleaned schema (rather than the raw schema) means this
function only needs to test for key presence, not re-filter.

---

### Updated `StarfixArrowHasher` in `src/orcapod/hashing/arrow_hashers.py`

Two methods are updated. All other class behaviour is unchanged.

**`hash_schema`:**

```python
def hash_schema(self, schema: pa.Schema) -> ContentHash:
    clean = clean_schema_for_hashing(schema)
    include_meta = has_extension_metadata(clean)
    digest = ArrowDigester.hash_schema(clean, include_metadata=include_meta)
    return ContentHash(method=self._hasher_id, digest=digest)
```

**`hash_table`:**

```python
def hash_table(self, table: pa.Table | pa.RecordBatch) -> ContentHash:
    if isinstance(table, pa.RecordBatch):
        table = pa.Table.from_batches([table])
    processed_table = self._process_table_columns(table)
    clean_schema = clean_schema_for_hashing(processed_table.schema)
    clean_table = pa.Table.from_arrays(processed_table.columns, schema=clean_schema)
    include_meta = has_extension_metadata(clean_schema)
    digest = ArrowDigester.hash_table(clean_table, include_metadata=include_meta)
    return ContentHash(method=self._hasher_id, digest=digest)
```

---

### Stability invariant

For schemas with no extension metadata anywhere, `clean_schema_for_hashing` is a no-op
and `has_extension_metadata` returns `False`. The call becomes
`ArrowDigester.hash_schema(schema, include_metadata=False)`, which is byte-for-byte
identical to the old `ArrowDigester.hash_schema(schema)` call (the v0.3 default is
`False`). All existing golden-value tests pass unchanged.

---

### Dependency change

`pyproject.toml`:
```
starfix~=0.3.0
```

(Changed from `starfix>=0.2.0`. `~=0.3.0` is PEP 440 compatible-release: allows patch increments, locks the minor.)

---

## File changes summary

| File | Change |
|---|---|
| `pyproject.toml` | `starfix~=0.3.0` |
| `uv.lock` | Updated by `uv add` |
| `src/orcapod/hashing/schema_cleaner.py` | **NEW** — `clean_schema_for_hashing`, `has_extension_metadata`, private helpers |
| `src/orcapod/hashing/arrow_hashers.py` | Update `hash_schema` and `hash_table` on `StarfixArrowHasher` |
| `tests/test_hashing/test_schema_cleaner.py` | **NEW** — cleaner unit tests |
| `tests/test_hashing/test_starfix_arrow_hasher.py` | Add `TestHashSchemaExtensionAware` and `TestHashTableExtensionAware` classes |
| `CHANGELOG.md` | Add note for v0.3 adoption and hash-change caveat |

---

## Test plan

### `tests/test_hashing/test_schema_cleaner.py` (new)

| Class | Coverage |
|---|---|
| `TestCleanSchemaForHashing` | Extension-free → metadata stripped to empty; extension-only → no-op; mixed → only `ARROW:extension:*` keys survive; schema-level metadata filtered |
| `TestCleanFieldRecursion` | `struct` with extension-tagged child; `list`, `large_list`, `fixed_size_list`, `map` with extension-tagged value/key fields; names/types/nullability untouched |
| `TestCleanSchemaFixtures` | Snapshot `(input, cleaned)` pairs for 3–4 representative schemas |
| `TestHasExtensionMetadata` | `False` for no-metadata schema; `True` when any field has `ARROW:extension:name`; recurses into nested types; `False` on cleaned extension-free schema |

### `tests/test_hashing/test_starfix_arrow_hasher.py` (additions)

| Class | Coverage |
|---|---|
| `TestHashSchemaExtensionAware` | Extension-free golden values unchanged (regression); mixed-metadata hash equals same-extension-keys-only hash; metadata-key ordering doesn't change the hash |
| `TestHashTableExtensionAware` | Table with mixed schema metadata → hash equals table with only extension schema metadata; extension-free table hash unchanged from golden |

---

## Scope & Boundaries

**In scope:**
- `schema_cleaner.py` utility.
- `has_extension_metadata` detector.
- Wiring in `StarfixArrowHasher.hash_schema` and `hash_table`.
- Tests, docstrings, changelog note.
- `starfix` dependency bump to `~=0.3.0`.

**Out of scope:**
- Changes to starfix or starfix-python.
- Native filtering in starfix (future — Starfix v0.4.0 project).
- Migrating existing on-disk caches (noted in changelog as a one-time hash invalidation
  for pipelines that had `ARROW:extension:*` keys alongside unrelated field metadata).

---

## Risks

- **Recursive correctness**: cleaner must handle every Arrow nested type. Tests cover
  `struct`, `list`, `large_list`, `fixed_size_list`, `map`. The fallback path handles any
  type not explicitly enumerated.
- **Extension type contract**: only `ARROW:extension:*` keys are identity-bearing. If an
  extension type were to stamp extra metadata under a non-`ARROW:extension:*` key, it
  would be silently stripped. Orcapod does not currently rely on any such pattern — this
  contract is documented in the module docstring.
