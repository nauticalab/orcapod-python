# Schema Metadata Cleaner + starfix v0.3.0 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement `clean_schema_for_hashing` and `has_extension_metadata` utilities in a new `schema_cleaner` module, wire them into `StarfixArrowHasher`, and bump the `starfix` dependency to `~=0.3.0`.

**Architecture:** A new pure-function module (`src/orcapod/hashing/schema_cleaner.py`) strips non-`ARROW:extension:*` metadata from Arrow schemas recursively. `StarfixArrowHasher.hash_schema` and `hash_table` call the cleaner, then pass `include_metadata=True/False` to `ArrowDigester` depending on whether any extension metadata survived cleaning. Schemas with no extension metadata hash byte-for-byte identically to pre-v0.3.0 output (stability invariant).

**Tech Stack:** Python 3.12, PyArrow 20+, starfix 0.3.x, pytest, uv

---

## File Map

| File | Action | Responsibility |
|---|---|---|
| `pyproject.toml` | Modify | Fix `starfix` specifier to `~=0.3.0` |
| `src/orcapod/hashing/schema_cleaner.py` | **Create** | `clean_schema_for_hashing`, `has_extension_metadata`, private helpers |
| `src/orcapod/hashing/arrow_hashers.py` | Modify | Wire cleaner into `StarfixArrowHasher.hash_schema` and `hash_table` |
| `tests/test_hashing/test_schema_cleaner.py` | **Create** | Unit tests for the cleaner module |
| `tests/test_hashing/test_starfix_arrow_hasher.py` | Modify | Add `TestHashSchemaExtensionAware` and `TestHashTableExtensionAware` |
| `CHANGELOG.md` | Modify | Add entry for v0.3.0 adoption + hash-invalidation caveat |

---

## Task 1: Fix dependency specifier in pyproject.toml

**Files:**
- Modify: `pyproject.toml`

The `uv add` command wrote `starfix>=0.3.0,<0.4.0`. The spec calls for `~=0.3.0` (PEP 440 compatible release — equivalent, more idiomatic).

- [ ] **Step 1: Edit pyproject.toml**

In `pyproject.toml`, change the starfix line:

```toml
# Before:
"starfix>=0.3.0,<0.4.0",

# After:
"starfix~=0.3.0",
```

- [ ] **Step 2: Sync lockfile**

```bash
uv lock
```

Expected: `uv.lock` regenerated or unchanged (both `>=0.3.0,<0.4.0` and `~=0.3.0` resolve to 0.3.1 — the only 0.3.x release).

- [ ] **Step 3: Verify starfix version installed**

```bash
uv run python -c "import importlib.metadata; print(importlib.metadata.version('starfix'))"
```

Expected output:
```
0.3.1
```

- [ ] **Step 4: Commit**

```bash
git add pyproject.toml uv.lock
git commit -m "chore(deps): pin starfix to ~=0.3.0 (PLT-1737)"
```

---

## Task 2: Implement `schema_cleaner.py` — cleaner (TDD)

**Files:**
- Create: `tests/test_hashing/test_schema_cleaner.py`
- Create: `src/orcapod/hashing/schema_cleaner.py`

### Background

Arrow schemas carry two kinds of metadata:
- **Schema-level metadata**: `schema.metadata` — a `dict[bytes, bytes] | None`
- **Field-level metadata**: `field.metadata` — same type, on each `pa.Field`

Nested types (`struct`, `list`, `large_list`, `fixed_size_list`, `map`) have child fields, each of which can also carry metadata. The cleaner must recurse.

Only keys starting with `b"ARROW:extension:"` are identity-bearing. Everything else is stripped.

PyArrow API notes relevant to this task:
- `pa.types.is_struct(t)` / `pa.types.is_list(t)` / `pa.types.is_large_list(t)` / `pa.types.is_fixed_size_list(t)` / `pa.types.is_map(t)` — type predicates
- `struct_type.field(i)` — returns the i-th child field (0-indexed)
- `list_type.value_field` / `large_list_type.value_field` / `fixed_size_list_type.value_field` — value field
- `fixed_size_list_type.list_size` — integer size
- `map_type.key_field` / `map_type.item_field` — key and item fields
- `map_type.keys_sorted` — bool
- `pa.list_(value_field)` — creates a variable-length list type
- `pa.list_(value_field, list_size)` — creates a fixed-size list type
- `pa.large_list(value_field)` — creates a large variable-length list type
- `pa.map_(key_field, item_field, keys_sorted)` — creates a map type (accepts `pa.Field` objects)
- `pa.field(name, type, nullable=True, metadata=None)` — creates a field
- `pa.schema(fields, metadata=None)` — creates a schema

- [ ] **Step 1: Write failing tests for `clean_schema_for_hashing`**

Create `tests/test_hashing/test_schema_cleaner.py`:

```python
"""
Tests for the schema metadata cleaner.

Coverage
--------
- clean_schema_for_hashing: extension-free, extension-only, mixed metadata,
  schema-level metadata, nested types (struct, list, large_list, fixed_size_list,
  map), deep nesting (list<struct<...>>, struct<struct<...>>), fixture snapshots
- has_extension_metadata: False for no metadata, True for extension field, recurse
  into nested types, True for deep extension-only field, False on cleaned schema
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.hashing.schema_cleaner import clean_schema_for_hashing, has_extension_metadata

_EXT_NAME = b"ARROW:extension:name"
_EXT_META = b"ARROW:extension:metadata"
_COMMENT  = b"comment"
_VENDOR   = b"vendor:tag"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ext_meta(name: str) -> dict[bytes, bytes]:
    return {_EXT_NAME: name.encode()}

def _mixed_meta(name: str) -> dict[bytes, bytes]:
    return {_EXT_NAME: name.encode(), _COMMENT: b"ignore me", _VENDOR: b"v1"}


# ---------------------------------------------------------------------------
# TestCleanSchemaForHashing
# ---------------------------------------------------------------------------

class TestCleanSchemaForHashing:
    def test_extension_free_schema_metadata_stripped_to_empty(self):
        """Schema-level metadata with no ARROW:extension:* keys is dropped entirely."""
        schema = pa.schema(
            [pa.field("x", pa.int32())],
            metadata={_COMMENT: b"hi", _VENDOR: b"v1"},
        )
        cleaned = clean_schema_for_hashing(schema)
        assert cleaned.metadata in (None, {})

    def test_extension_free_field_metadata_stripped_to_empty(self):
        """Field metadata with no ARROW:extension:* keys is dropped."""
        schema = pa.schema([
            pa.field("x", pa.int32(), metadata={_COMMENT: b"hi"}),
        ])
        cleaned = clean_schema_for_hashing(schema)
        assert cleaned.field("x").metadata in (None, {})

    def test_extension_only_schema_is_noop(self):
        """Schema with only ARROW:extension:* metadata is unchanged by the cleaner."""
        meta = {_EXT_NAME: b"my.type", _EXT_META: b"{}"}
        schema = pa.schema(
            [pa.field("x", pa.int32(), metadata=meta)],
            metadata={_EXT_NAME: b"schema.ext"},
        )
        cleaned = clean_schema_for_hashing(schema)
        assert cleaned.field("x").metadata == meta
        assert cleaned.metadata == {_EXT_NAME: b"schema.ext"}

    def test_mixed_metadata_only_extension_keys_survive(self):
        """Mixed metadata: only ARROW:extension:* keys survive on the field."""
        schema = pa.schema([
            pa.field("x", pa.int32(), metadata=_mixed_meta("my.type")),
        ])
        cleaned = clean_schema_for_hashing(schema)
        assert cleaned.field("x").metadata == {_EXT_NAME: b"my.type"}

    def test_schema_level_mixed_metadata_filtered(self):
        """Schema-level mixed metadata: only ARROW:extension:* keys survive."""
        schema = pa.schema(
            [pa.field("x", pa.int32())],
            metadata={_EXT_NAME: b"s.ext", _COMMENT: b"drop me"},
        )
        cleaned = clean_schema_for_hashing(schema)
        assert cleaned.metadata == {_EXT_NAME: b"s.ext"}

    def test_names_types_nullability_preserved(self):
        """Cleaner never touches names, types, or nullability."""
        schema = pa.schema([
            pa.field("a", pa.int32(), nullable=False, metadata={_COMMENT: b"x"}),
            pa.field("b", pa.float64(), nullable=True, metadata={_EXT_NAME: b"t"}),
        ])
        cleaned = clean_schema_for_hashing(schema)
        assert cleaned.field("a").name == "a"
        assert cleaned.field("a").type == pa.int32()
        assert cleaned.field("a").nullable is False
        assert cleaned.field("b").name == "b"
        assert cleaned.field("b").type == pa.float64()
        assert cleaned.field("b").nullable is True

    def test_returns_new_schema_object(self):
        """clean_schema_for_hashing always returns a new schema, never mutates."""
        schema = pa.schema([pa.field("x", pa.int32(), metadata={_COMMENT: b"hi"})])
        cleaned = clean_schema_for_hashing(schema)
        assert cleaned is not schema
        # Original is untouched
        assert schema.field("x").metadata == {_COMMENT: b"hi"}


# ---------------------------------------------------------------------------
# TestCleanFieldRecursion
# ---------------------------------------------------------------------------

class TestCleanFieldRecursion:
    def test_struct_child_metadata_cleaned(self):
        """Struct child fields have their non-extension metadata stripped."""
        schema = pa.schema([
            pa.field("s", pa.struct([
                pa.field("child", pa.int32(), metadata=_mixed_meta("child.t")),
            ])),
        ])
        cleaned = clean_schema_for_hashing(schema)
        child = cleaned.field("s").type.field(0)
        assert child.metadata == {_EXT_NAME: b"child.t"}

    def test_list_value_field_metadata_cleaned(self):
        """list<field> value field metadata is filtered."""
        schema = pa.schema([
            pa.field("lst", pa.list_(
                pa.field("item", pa.int32(), metadata=_mixed_meta("item.t"))
            )),
        ])
        cleaned = clean_schema_for_hashing(schema)
        value_field = cleaned.field("lst").type.value_field
        assert value_field.metadata == {_EXT_NAME: b"item.t"}

    def test_large_list_value_field_metadata_cleaned(self):
        """large_list value field metadata is filtered."""
        schema = pa.schema([
            pa.field("lst", pa.large_list(
                pa.field("item", pa.int32(), metadata=_mixed_meta("item.t"))
            )),
        ])
        cleaned = clean_schema_for_hashing(schema)
        value_field = cleaned.field("lst").type.value_field
        assert value_field.metadata == {_EXT_NAME: b"item.t"}

    def test_fixed_size_list_value_field_metadata_cleaned(self):
        """fixed_size_list value field metadata is filtered; list_size preserved."""
        schema = pa.schema([
            pa.field("lst", pa.list_(
                pa.field("item", pa.int32(), metadata=_mixed_meta("item.t")),
                3,
            )),
        ])
        cleaned = clean_schema_for_hashing(schema)
        assert cleaned.field("lst").type.list_size == 3
        value_field = cleaned.field("lst").type.value_field
        assert value_field.metadata == {_EXT_NAME: b"item.t"}

    def test_map_key_field_metadata_cleaned(self):
        """map key field metadata is filtered."""
        schema = pa.schema([
            pa.field("m", pa.map_(
                pa.field("key", pa.string(), nullable=False, metadata={_COMMENT: b"drop"}),
                pa.int32(),
            )),
        ])
        cleaned = clean_schema_for_hashing(schema)
        key_field = cleaned.field("m").type.key_field
        assert key_field.metadata in (None, {})

    def test_map_item_field_metadata_cleaned(self):
        """map item field metadata is filtered."""
        schema = pa.schema([
            pa.field("m", pa.map_(
                pa.string(),
                pa.field("value", pa.int32(), metadata=_mixed_meta("val.t")),
            )),
        ])
        cleaned = clean_schema_for_hashing(schema)
        item_field = cleaned.field("m").type.item_field
        assert item_field.metadata == {_EXT_NAME: b"val.t"}

    def test_deep_list_of_struct_cleaned(self):
        """list<struct<ext_field>>: extension metadata on the deeply-nested struct
        child field is preserved; unrelated keys are stripped at every level."""
        inner_field = pa.field("x", pa.int32(), metadata=_mixed_meta("inner.t"))
        schema = pa.schema([
            pa.field("lst", pa.list_(
                pa.field("item", pa.struct([inner_field]),
                         metadata={_COMMENT: b"mid level drop"}),
            )),
        ])
        cleaned = clean_schema_for_hashing(schema)
        item_field = cleaned.field("lst").type.value_field
        # Mid-level (value_field) metadata stripped
        assert item_field.metadata in (None, {})
        # Deep child preserved only for extension keys
        deep_child = item_field.type.field(0)
        assert deep_child.metadata == {_EXT_NAME: b"inner.t"}

    def test_deep_struct_of_struct_cleaned(self):
        """struct<struct<ext_field>>: extension metadata on the grandchild field
        is preserved; unrelated keys at every level are stripped."""
        grandchild = pa.field("gc", pa.int32(), metadata=_mixed_meta("gc.t"))
        schema = pa.schema([
            pa.field("outer", pa.struct([
                pa.field("inner", pa.struct([grandchild]),
                         metadata={_COMMENT: b"drop"}),
            ])),
        ])
        cleaned = clean_schema_for_hashing(schema)
        inner_field = cleaned.field("outer").type.field(0)
        assert inner_field.metadata in (None, {})
        gc = inner_field.type.field(0)
        assert gc.metadata == {_EXT_NAME: b"gc.t"}


# ---------------------------------------------------------------------------
# TestCleanSchemaFixtures  (input → expected snapshot pairs)
# ---------------------------------------------------------------------------

class TestCleanSchemaFixtures:
    def test_fixture_no_metadata(self):
        """Schema with no metadata at all: cleaner returns schema without metadata."""
        schema = pa.schema([
            pa.field("id", pa.int32()),
            pa.field("val", pa.float64()),
        ])
        cleaned = clean_schema_for_hashing(schema)
        assert cleaned.metadata in (None, {})
        assert cleaned.field("id").metadata in (None, {})
        assert cleaned.field("val").metadata in (None, {})

    def test_fixture_extension_field(self):
        """Schema with extension field: snapshot the cleaned (identity) output."""
        schema = pa.schema([
            pa.field("t", pa.binary(), metadata={
                _EXT_NAME: b"my_pkg.MyType",
                _EXT_META: b'{"version":1}',
            }),
        ])
        cleaned = clean_schema_for_hashing(schema)
        assert cleaned.field("t").metadata == {
            _EXT_NAME: b"my_pkg.MyType",
            _EXT_META: b'{"version":1}',
        }

    def test_fixture_mixed_deep(self):
        """Schema with mixed metadata at two levels of nesting."""
        schema = pa.schema([
            pa.field("top", pa.int32(), metadata={_COMMENT: b"drop", _EXT_NAME: b"top.t"}),
            pa.field("nested", pa.struct([
                pa.field("child", pa.utf8(), metadata={
                    _COMMENT: b"also drop",
                    _EXT_NAME: b"child.t",
                }),
            ]), metadata={_VENDOR: b"remove"}),
        ], metadata={_COMMENT: b"schema comment", _EXT_NAME: b"schema.ext"})

        cleaned = clean_schema_for_hashing(schema)

        assert cleaned.metadata == {_EXT_NAME: b"schema.ext"}
        assert cleaned.field("top").metadata == {_EXT_NAME: b"top.t"}
        assert cleaned.field("nested").metadata in (None, {})
        assert cleaned.field("nested").type.field(0).metadata == {_EXT_NAME: b"child.t"}
```

- [ ] **Step 2: Run tests — confirm ImportError**

```bash
uv run pytest tests/test_hashing/test_schema_cleaner.py -v 2>&1 | head -20
```

Expected: `ModuleNotFoundError: No module named 'orcapod.hashing.schema_cleaner'`

- [ ] **Step 3: Create `src/orcapod/hashing/schema_cleaner.py`**

```python
"""Schema metadata cleaner for Arrow hashing.

This module provides utilities for preprocessing Arrow schemas before
content-addressed hashing. Only ``ARROW:extension:*`` metadata keys are
identity-bearing in Orcapod; all other keys (comments, vendor annotations,
source-file provenance, etc.) are stripped so they cannot affect the hash.

Contract
--------
Only ``ARROW:extension:*`` metadata keys are treated as identity-bearing.
If an extension type were to stamp extra metadata under a non-``ARROW:extension:*``
key, that metadata would be silently stripped by ``clean_schema_for_hashing``.
Orcapod does not currently rely on any such pattern.

Public API
----------
- ``clean_schema_for_hashing(schema)`` — returns a cleaned copy of the schema.
- ``has_extension_metadata(schema)`` — returns True if the (cleaned) schema has
  any ``ARROW:extension:name`` key on any field at any nesting depth.
"""

from __future__ import annotations

import pyarrow as pa

_EXTENSION_PREFIX: bytes = b"ARROW:extension:"
_EXTENSION_NAME_KEY: bytes = b"ARROW:extension:name"


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _filter_metadata(metadata: dict[bytes, bytes] | None) -> dict[bytes, bytes]:
    """Return only the entries whose key starts with ``b'ARROW:extension:'``."""
    if not metadata:
        return {}
    return {k: v for k, v in metadata.items() if k.startswith(_EXTENSION_PREFIX)}


def _clean_type(arrow_type: pa.DataType) -> pa.DataType:
    """Recursively rebuild a composite type with cleaned child-field metadata.

    For primitive (leaf) types, returns the type unchanged. For composite types
    (struct, list, large_list, fixed_size_list, map), rebuilds the type using
    cleaned versions of all child fields.
    """
    if pa.types.is_struct(arrow_type):
        return pa.struct([
            _clean_field(arrow_type.field(i))
            for i in range(arrow_type.num_fields)
        ])
    if pa.types.is_list(arrow_type):
        return pa.list_(_clean_field(arrow_type.value_field))
    if pa.types.is_large_list(arrow_type):
        return pa.large_list(_clean_field(arrow_type.value_field))
    if pa.types.is_fixed_size_list(arrow_type):
        return pa.list_(_clean_field(arrow_type.value_field), arrow_type.list_size)
    if pa.types.is_map(arrow_type):
        return pa.map_(
            _clean_field(arrow_type.key_field),
            _clean_field(arrow_type.item_field),
            arrow_type.keys_sorted,
        )
    return arrow_type


def _clean_field(field: pa.Field) -> pa.Field:
    """Return a copy of ``field`` with metadata and child-type metadata cleaned.

    The field name, type identity (Arrow type), and nullability flag are
    preserved exactly. Only the metadata dict is filtered.
    """
    return pa.field(
        field.name,
        _clean_type(field.type),
        nullable=field.nullable,
        metadata=_filter_metadata(field.metadata),
    )


def _has_extension_in_type(arrow_type: pa.DataType) -> bool:
    """Return True if any child field of ``arrow_type`` has extension metadata."""
    if pa.types.is_struct(arrow_type):
        return any(
            _has_extension_in_field(arrow_type.field(i))
            for i in range(arrow_type.num_fields)
        )
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type) or pa.types.is_fixed_size_list(arrow_type):
        return _has_extension_in_field(arrow_type.value_field)
    if pa.types.is_map(arrow_type):
        return (
            _has_extension_in_field(arrow_type.key_field)
            or _has_extension_in_field(arrow_type.item_field)
        )
    return False


def _has_extension_in_field(field: pa.Field) -> bool:
    """Return True if ``field`` or any of its nested fields carries extension metadata."""
    if field.metadata and _EXTENSION_NAME_KEY in field.metadata:
        return True
    return _has_extension_in_type(field.type)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def clean_schema_for_hashing(schema: pa.Schema) -> pa.Schema:
    """Return a copy of ``schema`` with all non-extension metadata stripped.

    Walks the schema and every field recursively (through struct, list,
    large_list, fixed_size_list, and map child fields). Retains only metadata
    keys that start with ``b'ARROW:extension:'`` — at both schema level and
    per-field level. All other metadata is dropped.

    Names, types, and nullability flags are untouched.

    Args:
        schema: The ``pa.Schema`` to clean.

    Returns:
        A new ``pa.Schema`` with the same structure but with non-extension
        metadata removed at every level. Schema-level metadata becomes an
        empty dict if no extension keys were present.
    """
    return pa.schema(
        [_clean_field(schema.field(i)) for i in range(len(schema))],
        metadata=_filter_metadata(schema.metadata),
    )


def has_extension_metadata(schema: pa.Schema) -> bool:
    """Return True if ``schema`` has any ``ARROW:extension:name`` key at any depth.

    Intended to be called on the output of ``clean_schema_for_hashing`` so
    that the check is purely a key-presence test (no re-filtering needed).

    Recurses through struct, list, large_list, fixed_size_list, and map child
    fields identically to ``clean_schema_for_hashing``.

    Args:
        schema: A ``pa.Schema`` (typically the output of
            ``clean_schema_for_hashing``).

    Returns:
        True if any field at any nesting depth carries ``ARROW:extension:name``
        in its metadata; False otherwise.
    """
    if schema.metadata and _EXTENSION_NAME_KEY in schema.metadata:
        return True
    return any(
        _has_extension_in_field(schema.field(i)) for i in range(len(schema))
    )
```

- [ ] **Step 4: Run the cleaner tests**

```bash
uv run pytest tests/test_hashing/test_schema_cleaner.py -v
```

Expected: all tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/hashing/schema_cleaner.py tests/test_hashing/test_schema_cleaner.py
git commit -m "feat(hashing): add clean_schema_for_hashing and has_extension_metadata (PLT-1737)"
```

---

## Task 3: Add `has_extension_metadata` tests + fixture snapshot tests

**Files:**
- Modify: `tests/test_hashing/test_schema_cleaner.py`

The `TestHasExtensionMetadata` class and the deep-recursion fixture were already written in Task 2's test file. If they passed in Task 2, skip this task. If any were left out, add them now.

- [ ] **Step 1: Verify `TestHasExtensionMetadata` exists and passes**

```bash
uv run pytest tests/test_hashing/test_schema_cleaner.py::TestHasExtensionMetadata -v
```

Expected: all tests pass. (If the class was omitted in Task 2, add it now — see code below.)

```python
class TestHasExtensionMetadata:
    def test_false_for_no_metadata(self):
        schema = pa.schema([pa.field("x", pa.int32())])
        assert has_extension_metadata(schema) is False

    def test_false_for_unrelated_metadata_only(self):
        schema = pa.schema([
            pa.field("x", pa.int32(), metadata={_COMMENT: b"hi"}),
        ])
        assert has_extension_metadata(schema) is False

    def test_true_when_top_level_field_has_extension_name(self):
        schema = pa.schema([
            pa.field("x", pa.int32(), metadata={_EXT_NAME: b"my.type"}),
        ])
        assert has_extension_metadata(schema) is True

    def test_true_for_deeply_nested_extension_field(self):
        """Extension metadata only on a grandchild field still returns True."""
        schema = pa.schema([
            pa.field("outer", pa.struct([
                pa.field("inner", pa.struct([
                    pa.field("gc", pa.int32(), metadata={_EXT_NAME: b"gc.t"}),
                ])),
            ])),
        ])
        assert has_extension_metadata(schema) is True

    def test_false_on_cleaned_extension_free_schema(self):
        """After cleaning, an extension-free schema returns False."""
        schema = pa.schema([
            pa.field("x", pa.int32(), metadata={_COMMENT: b"drop me"}),
        ])
        cleaned = clean_schema_for_hashing(schema)
        assert has_extension_metadata(cleaned) is False

    def test_true_for_extension_in_list_value_field(self):
        """Extension metadata on a list's value field is detected."""
        schema = pa.schema([
            pa.field("lst", pa.list_(
                pa.field("item", pa.int32(), metadata={_EXT_NAME: b"item.t"}),
            )),
        ])
        assert has_extension_metadata(schema) is True

    def test_true_for_extension_in_map_item_field(self):
        schema = pa.schema([
            pa.field("m", pa.map_(
                pa.string(),
                pa.field("value", pa.int32(), metadata={_EXT_NAME: b"val.t"}),
            )),
        ])
        assert has_extension_metadata(schema) is True
```

- [ ] **Step 2: Run full test suite for schema_cleaner**

```bash
uv run pytest tests/test_hashing/test_schema_cleaner.py -v
```

Expected: all tests pass.

---

## Task 4: Wire `StarfixArrowHasher` (TDD)

**Files:**
- Modify: `tests/test_hashing/test_starfix_arrow_hasher.py`
- Modify: `src/orcapod/hashing/arrow_hashers.py`

### Background

`StarfixArrowHasher.hash_schema` currently calls `ArrowDigester.hash_schema(schema)` (no `include_metadata`). After this task it will:
1. Clean the schema with `clean_schema_for_hashing`.
2. Check `has_extension_metadata` on the cleaned schema.
3. Call `ArrowDigester.hash_schema(clean, include_metadata=include_meta)`.

`hash_table` similarly cleans the schema of the processed table before calling `ArrowDigester.hash_table`.

**Stability invariant:** For schemas with no extension metadata, `clean_schema_for_hashing` is a no-op, `has_extension_metadata` returns `False`, and `ArrowDigester.hash_schema(schema, include_metadata=False)` is byte-for-byte identical to the old `ArrowDigester.hash_schema(schema)` call (the v0.3 default is `False`). The existing golden-value tests must still pass.

- [ ] **Step 1: Write failing hash-path tests**

Append to `tests/test_hashing/test_starfix_arrow_hasher.py`:

```python
# ---------------------------------------------------------------------------
# Extension-aware hash path (PLT-1737)
# ---------------------------------------------------------------------------

_EXT_NAME_KEY = b"ARROW:extension:name"
_COMMENT_KEY  = b"comment"


class TestHashSchemaExtensionAware:
    def test_extension_free_golden_value_unchanged(self):
        """Stability invariant: extension-free schema hash is byte-identical to
        the pre-v0.3.0 golden value (include_metadata=False is the default)."""
        schema = pa.schema(
            [
                pa.field("id", pa.int32(), nullable=False),
                pa.field("value", pa.float64(), nullable=True),
            ]
        )
        h = _make_hasher().hash_schema(schema)
        assert h.digest.hex() == "000001d676ef0263a8e0e7500b1c97033993dbe445172ca0f9e7577b3994bfa6224b4c", (
            f"Stability invariant broken — extension-free golden digest changed. Got: {h.digest.hex()}"
        )

    def test_mixed_metadata_hash_equals_extension_only_hash(self):
        """Key property: a schema with ARROW:extension:name plus unrelated metadata
        hashes identically to the same schema with only the extension key."""
        ext_only_schema = pa.schema([
            pa.field("t", pa.binary(), metadata={_EXT_NAME_KEY: b"my.Type"}),
        ])
        mixed_schema = pa.schema([
            pa.field("t", pa.binary(), metadata={
                _EXT_NAME_KEY: b"my.Type",
                _COMMENT_KEY:  b"ignored",
            }),
        ])
        h_ext   = _make_hasher().hash_schema(ext_only_schema)
        h_mixed = _make_hasher().hash_schema(mixed_schema)
        assert h_ext.digest == h_mixed.digest

    def test_metadata_key_ordering_does_not_affect_hash(self):
        """Reordering metadata keys in the input does not change the hash."""
        s1 = pa.schema([pa.field("t", pa.binary(), metadata={
            _EXT_NAME_KEY: b"my.Type",
            b"ARROW:extension:metadata": b"{}",
        })])
        s2 = pa.schema([pa.field("t", pa.binary(), metadata={
            b"ARROW:extension:metadata": b"{}",
            _EXT_NAME_KEY: b"my.Type",
        })])
        assert _make_hasher().hash_schema(s1).digest == _make_hasher().hash_schema(s2).digest

    def test_extension_metadata_affects_hash(self):
        """Changing an ARROW:extension:name value changes the hash."""
        s1 = pa.schema([pa.field("t", pa.binary(), metadata={_EXT_NAME_KEY: b"TypeA"})])
        s2 = pa.schema([pa.field("t", pa.binary(), metadata={_EXT_NAME_KEY: b"TypeB"})])
        assert _make_hasher().hash_schema(s1).digest != _make_hasher().hash_schema(s2).digest

    def test_unrelated_metadata_does_not_affect_hash(self):
        """Adding only unrelated metadata keys does not change the hash."""
        s_clean = pa.schema([pa.field("x", pa.int32())])
        s_noise = pa.schema([pa.field("x", pa.int32(), metadata={_COMMENT_KEY: b"hi"})])
        assert _make_hasher().hash_schema(s_clean).digest == _make_hasher().hash_schema(s_noise).digest


class TestHashTableExtensionAware:
    def test_extension_free_table_golden_value_unchanged(self):
        """Stability invariant: extension-free table hash is byte-identical to
        the pre-v0.3.0 golden value."""
        table = pa.table({
            "id":    pa.array([1, 2, 3], type=pa.int32()),
            "score": pa.array([0.1, 0.2, 0.3], type=pa.float64()),
            "label": pa.array(["a", "b", "c"], type=pa.utf8()),
        })
        h = _make_hasher().hash_table(table)
        assert h.digest.hex() == "0000010cd7fe5462420b84f03a06925374e528817a3b72319e679a17e7380964878791", (
            f"Stability invariant broken — extension-free table golden digest changed. Got: {h.digest.hex()}"
        )

    def test_table_mixed_schema_metadata_equals_extension_only(self):
        """Table schema with mixed metadata hashes identically to same table
        schema with only the extension key (unrelated keys are stripped)."""
        schema_ext_only = pa.schema([
            pa.field("x", pa.int32(), metadata={_EXT_NAME_KEY: b"my.Type"}),
        ])
        schema_mixed = pa.schema([
            pa.field("x", pa.int32(), metadata={
                _EXT_NAME_KEY: b"my.Type",
                _COMMENT_KEY:  b"ignored",
            }),
        ])
        data = [pa.array([1, 2, 3], type=pa.int32())]
        t_ext   = pa.table(data, schema=schema_ext_only)
        t_mixed = pa.table(data, schema=schema_mixed)
        assert _make_hasher().hash_table(t_ext).digest == _make_hasher().hash_table(t_mixed).digest

    def test_table_unrelated_schema_metadata_does_not_affect_hash(self):
        """Adding only unrelated schema metadata does not change the table hash."""
        t_clean = pa.table({"x": pa.array([1, 2], type=pa.int32())})
        schema_noise = pa.schema([pa.field("x", pa.int32(), metadata={_COMMENT_KEY: b"hi"})])
        t_noise = pa.table([pa.array([1, 2], type=pa.int32())], schema=schema_noise)
        assert _make_hasher().hash_table(t_clean).digest == _make_hasher().hash_table(t_noise).digest
```

- [ ] **Step 2: Run new tests — confirm they fail**

```bash
uv run pytest tests/test_hashing/test_starfix_arrow_hasher.py::TestHashSchemaExtensionAware tests/test_hashing/test_starfix_arrow_hasher.py::TestHashTableExtensionAware -v 2>&1 | tail -20
```

Expected: several tests FAIL (the `test_mixed_metadata_hash_equals_extension_only_hash` and `test_table_mixed_schema_metadata_equals_extension_only` tests will fail because the cleaner is not yet wired in).

- [ ] **Step 3: Update `StarfixArrowHasher` in `arrow_hashers.py`**

At the top of `src/orcapod/hashing/arrow_hashers.py`, add the import after the existing imports:

```python
from orcapod.hashing.schema_cleaner import clean_schema_for_hashing, has_extension_metadata
```

Replace `hash_schema` (lines ~336–351 in the current file):

```python
    def hash_schema(self, schema: pa.Schema) -> ContentHash:
        """Hash an Arrow schema using the starfix canonical algorithm.

        The schema is preprocessed by ``clean_schema_for_hashing`` before
        hashing: non-``ARROW:extension:*`` metadata is stripped at every
        level. ``include_metadata=True`` is passed to ``ArrowDigester`` only
        when extension metadata is present after cleaning, preserving
        byte-for-byte hash stability with pre-v0.3.0 output for schemas that
        carry no extension metadata.

        Parameters
        ----------
        schema:
            The ``pa.Schema`` to hash.

        Returns
        -------
        ContentHash
            A ``ContentHash`` whose ``digest`` is the 35-byte versioned
            SHA-256 produced by ``ArrowDigester.hash_schema``.
        """
        clean = clean_schema_for_hashing(schema)
        include_meta = has_extension_metadata(clean)
        digest = ArrowDigester.hash_schema(clean, include_metadata=include_meta)
        return ContentHash(method=self._hasher_id, digest=digest)
```

Replace `hash_table` (lines ~353–377 in the current file):

```python
    def hash_table(self, table: pa.Table | pa.RecordBatch) -> ContentHash:
        """Hash an Arrow table (or ``RecordBatch``) using starfix.

        Semantic types are resolved to their content-hash strings before the
        table is passed to ``ArrowDigester.hash_table``. The table's schema
        is then preprocessed by ``clean_schema_for_hashing`` to strip
        non-``ARROW:extension:*`` metadata. ``include_metadata=True`` is
        passed to ``ArrowDigester`` only when extension metadata is present
        after cleaning.

        Parameters
        ----------
        table:
            The ``pa.Table`` or ``pa.RecordBatch`` to hash.

        Returns
        -------
        ContentHash
            A ``ContentHash`` whose ``digest`` is the 35-byte versioned
            SHA-256 produced by ``ArrowDigester.hash_table``.
        """
        if isinstance(table, pa.RecordBatch):
            table = pa.Table.from_batches([table])

        processed_table = self._process_table_columns(table)
        clean_schema = clean_schema_for_hashing(processed_table.schema)
        clean_table = pa.Table.from_arrays(
            processed_table.columns, schema=clean_schema
        )
        include_meta = has_extension_metadata(clean_schema)
        digest = ArrowDigester.hash_table(clean_table, include_metadata=include_meta)
        return ContentHash(method=self._hasher_id, digest=digest)
```

- [ ] **Step 4: Run the full hashing test suite**

```bash
uv run pytest tests/test_hashing/ -v
```

Expected: all tests pass, including the pre-existing golden-value tests in `TestHashSchema` and `TestHashTable`.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/hashing/arrow_hashers.py tests/test_hashing/test_starfix_arrow_hasher.py
git commit -m "feat(hashing): wire schema cleaner into StarfixArrowHasher (PLT-1737)"
```

---

## Task 5: Run the full test suite

- [ ] **Step 1: Run all tests**

```bash
uv run pytest tests/ -v --tb=short 2>&1 | tail -30
```

Expected: all tests pass. If any pre-existing tests fail, investigate before proceeding — do not skip.

- [ ] **Step 2: Commit if any fixes were needed**

If no fixes were needed, no additional commit is required.

---

## Task 6: Update CHANGELOG.md

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add entry under `## [Unreleased]`**

Insert the following block immediately after the `## [Unreleased]` heading (before any existing entries):

```markdown
### Changed

#### starfix v0.3.0 adoption + schema-metadata cleaner (PLT-1737)

Bumped `starfix` dependency to `~=0.3.0` and introduced a schema-metadata
cleaning step before all Arrow schema and table hashes.

**What changed:** `StarfixArrowHasher.hash_schema` and `hash_table` now strip
every metadata key that does not start with `ARROW:extension:` before passing
the schema to starfix. Only identity-bearing extension metadata (e.g.
`ARROW:extension:name`) is included in the hash. Unrelated keys (comments,
vendor annotations, source-file provenance, etc.) are ignored.

**Stability invariant:** Schemas with no `ARROW:extension:*` metadata anywhere
continue to produce byte-for-byte identical hashes to pre-v0.3.0 Orcapod.

**One-time hash invalidation:** Pipelines whose schemas contained
`ARROW:extension:*` keys *alongside* unrelated field metadata (e.g. a
`comment` key on the same field as `ARROW:extension:name`) will see a changed
hash. On-disk caches for those pipelines should be treated as stale and
recomputed.

**New utility:** `orcapod.hashing.schema_cleaner.clean_schema_for_hashing` and
`has_extension_metadata` are available as semi-public utilities for other
Orcapod code that needs to inspect or clean Arrow schema metadata.
```

- [ ] **Step 2: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs(changelog): add starfix v0.3.0 adoption and schema cleaner note (PLT-1737)"
```

---

## Task 7: Final verification

- [ ] **Step 1: Run the full test suite one last time**

```bash
uv run pytest tests/ -v --tb=short 2>&1 | tail -10
```

Expected output ends with:
```
========== N passed in X.XXs ==========
```

No failures, no errors.

- [ ] **Step 2: Confirm branch and diff**

```bash
git log --oneline main..HEAD
git diff main --stat
```

Expected log (4 commits):
```
<hash> docs(changelog): add starfix v0.3.0 adoption and schema cleaner note (PLT-1737)
<hash> feat(hashing): wire schema cleaner into StarfixArrowHasher (PLT-1737)
<hash> feat(hashing): add clean_schema_for_hashing and has_extension_metadata (PLT-1737)
<hash> chore(deps): pin starfix to ~=0.3.0 (PLT-1737)
```

Expected diff stat touches only:
- `pyproject.toml`
- `uv.lock`
- `src/orcapod/hashing/schema_cleaner.py` (new)
- `src/orcapod/hashing/arrow_hashers.py`
- `tests/test_hashing/test_schema_cleaner.py` (new)
- `tests/test_hashing/test_starfix_arrow_hasher.py`
- `CHANGELOG.md`
