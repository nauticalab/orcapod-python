# ITL-471: SpiralDB Arrow Metadata Preservation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve Arrow table- and column-level metadata (including nested struct/list field metadata) across SpiralDB write/read cycles using the native KV store, and replace the blanket extension-type guard in `ConnectorArrowDatabase` with a per-connector `validate_records` hook.

**Architecture:** Four private helpers in `spiraldb_connector.py` handle recursive metadata serialization/deserialization. `upsert_records` writes metadata to the SpiralDB KV store after each data write; `iter_batches` reads and restores it per scan. `DBConnectorProtocol` gains an abstract `validate_records` method; each connector implements it (rejecting for SQLite/PostgreSQL, no-op for SpiralDB); `ConnectorArrowDatabase` delegates to it instead of applying its own guard.

**Tech Stack:** PyArrow (`pa.Field`, `pa.DataType`, `pa.struct`, `pa.list_`, `pa.large_list`, `pa.fixed_size_list`), Python `base64` + `json` stdlib, pyspiral `Table.set_metadata` / `Table.get_metadata`, `unittest.mock`.

---

## File Map

| File | Action | What changes |
|---|---|---|
| `src/orcapod/databases/spiraldb_connector.py` | Modify | Add 4 private helpers; modify `upsert_records` and `iter_batches`; add `validate_records` no-op |
| `src/orcapod/protocols/db_connector_protocol.py` | Modify | Add abstract `validate_records` method |
| `src/orcapod/databases/sqlite_connector.py` | Modify | Add rejecting `validate_records` method |
| `src/orcapod/databases/postgresql_connector.py` | Modify | Add rejecting `validate_records` method |
| `src/orcapod/databases/connector_arrow_database.py` | Modify | Replace inline guard (~lines 247–273) with `self._connector.validate_records(records)` |
| `tests/test_databases/test_spiraldb_metadata_helpers.py` | Create | Pure-function tests for the 4 helpers |
| `tests/test_databases/test_spiraldb_connector.py` | Modify | Add `TestUpsertRecordsMetadata`, `TestIterBatchesMetadata`, `TestValidateRecords` |
| `tests/test_databases/test_connector_arrow_database.py` | Modify | Update `MockDBConnector` + `TestExtensionTypeWriteGuard` |
| `tests/test_databases/test_spiraldb_connector_integration.py` | Modify | Add `TestArrowMetadataRoundTrip` |

---

## Implementation Notes (read before starting)

- **Run all Python via `uv run`** — never call `python` or `pytest` directly.
- **Branch:** `eywalker/itl-471-spiraldbconnector-preserve-arrow-table-and-column-level`
- **`_ARROW_METADATA_KEY = "__arrow_metadata__"`** — the single reserved KV key; define it as a module-level constant in `spiraldb_connector.py`.
- **`validate_records` inheritance:** Connectors do NOT inherit from `DBConnectorProtocol` (structural subtyping only). The abstract method on the Protocol is for documentation/type checking. Each connector must implement it explicitly. `MockDBConnector` in the test file also needs it.
- **`field.with_type(new_type)`** — use this PyArrow method to create a new `pa.Field` with a different type but same name, nullability, and metadata. Check if it exists; if not, use `pa.field(f.name, new_type, f.nullable, f.metadata)`.
- **`pa.fixed_size_list`** — `pa.fixed_size_list(value_type_or_field, list_size)`. Use `arrow_type.list_size` for the size.
- **`pa.map_` children** — `pa.map_` does not support field-level metadata on key/item via constructor. Skip children for map types (no recursion into map key/item fields).
- **Integration tests** are gated on `SPIRAL_INTEGRATION_TESTS=1`. Run unit tests normally; skip integration if no credentials.

---

## Task 1: Checkout branch and run existing tests baseline

**Files:** (no source changes)

- [ ] **Step 1: Create and checkout the feature branch**

```bash
cd /path/to/orcapod-python  # your working directory
git checkout -b eywalker/itl-471-spiraldbconnector-preserve-arrow-table-and-column-level
git branch --show-current
```

Expected output: `eywalker/itl-471-spiraldbconnector-preserve-arrow-table-and-column-level`

- [ ] **Step 2: Verify existing tests pass**

```bash
uv run pytest tests/test_databases/ -v --tb=short -q
```

Expected: all pass (integration tests skipped). Note the count — it's the baseline.

---

## Task 2: Serialization helpers — `_serialize_field_meta_tree` and `_serialize_arrow_metadata`

**Files:**
- Create: `tests/test_databases/test_spiraldb_metadata_helpers.py`
- Modify: `src/orcapod/databases/spiraldb_connector.py`

### Step 1: Write failing tests for `_serialize_field_meta_tree`

- [ ] Create `tests/test_databases/test_spiraldb_metadata_helpers.py`:

```python
"""Pure-function tests for SpiralDB Arrow metadata helpers."""
from __future__ import annotations

import base64
import json

import pyarrow as pa
import pytest

from orcapod.databases.spiraldb_connector import (
    _ARROW_METADATA_KEY,
    _load_arrow_metadata,
    _restore_field,
    _serialize_arrow_metadata,
    _serialize_field_meta_tree,
)


def b64(s: bytes) -> str:
    return base64.b64encode(s).decode()


# ---------------------------------------------------------------------------
# _serialize_field_meta_tree
# ---------------------------------------------------------------------------


class TestSerializeFieldMetaTree:
    def test_primitive_with_metadata(self):
        field = pa.field("x", pa.int64(), metadata={b"unit": b"meters"})
        result = _serialize_field_meta_tree(field)
        assert result == {"meta": {b64(b"unit"): b64(b"meters")}}

    def test_primitive_no_metadata_returns_none(self):
        field = pa.field("x", pa.int64())
        assert _serialize_field_meta_tree(field) is None

    def test_struct_with_child_metadata(self):
        inner = pa.field("val", pa.float64(), metadata={b"key": b"v"})
        field = pa.field("s", pa.struct([inner]))
        result = _serialize_field_meta_tree(field)
        assert result == {
            "children": {"val": {"meta": {b64(b"key"): b64(b"v")}}}
        }

    def test_struct_with_both_own_and_child_metadata(self):
        inner = pa.field("val", pa.float64(), metadata={b"k": b"v"})
        field = pa.field("s", pa.struct([inner]), metadata={b"tag": b"yes"})
        result = _serialize_field_meta_tree(field)
        assert result == {
            "meta": {b64(b"tag"): b64(b"yes")},
            "children": {"val": {"meta": {b64(b"k"): b64(b"v")}}},
        }

    def test_struct_child_no_metadata_not_included(self):
        inner_with = pa.field("a", pa.int64(), metadata={b"k": b"v"})
        inner_without = pa.field("b", pa.int64())
        field = pa.field("s", pa.struct([inner_with, inner_without]))
        result = _serialize_field_meta_tree(field)
        assert "children" in result
        assert "a" in result["children"]
        assert "b" not in result["children"]

    def test_list_value_field_with_metadata(self):
        vf = pa.field("item", pa.int32(), metadata={b"k": b"v"})
        field = pa.field("lst", pa.list_(vf))
        result = _serialize_field_meta_tree(field)
        assert result == {
            "children": {"item": {"meta": {b64(b"k"): b64(b"v")}}}
        }

    def test_large_list_value_field_with_metadata(self):
        vf = pa.field("item", pa.int32(), metadata={b"k": b"v"})
        field = pa.field("lst", pa.large_list(vf))
        result = _serialize_field_meta_tree(field)
        assert result == {
            "children": {"item": {"meta": {b64(b"k"): b64(b"v")}}}
        }

    def test_fixed_size_list_value_field_with_metadata(self):
        vf = pa.field("item", pa.float32(), metadata={b"k": b"v"})
        field = pa.field("lst", pa.list_(vf, 3))
        result = _serialize_field_meta_tree(field)
        assert result == {
            "children": {"item": {"meta": {b64(b"k"): b64(b"v")}}}
        }

    def test_nested_struct_in_struct(self):
        leaf = pa.field("z", pa.int8(), metadata={b"deep": b"yes"})
        inner_struct = pa.field("inner", pa.struct([leaf]))
        outer = pa.field("outer", pa.struct([inner_struct]))
        result = _serialize_field_meta_tree(outer)
        assert result == {
            "children": {
                "inner": {
                    "children": {"z": {"meta": {b64(b"deep"): b64(b"yes")}}}
                }
            }
        }

    def test_no_metadata_anywhere_returns_none(self):
        inner = pa.field("a", pa.int64())
        field = pa.field("s", pa.struct([inner]))
        assert _serialize_field_meta_tree(field) is None


# ---------------------------------------------------------------------------
# _serialize_arrow_metadata
# ---------------------------------------------------------------------------


class TestSerializeArrowMetadata:
    def test_no_metadata_returns_none(self):
        table = pa.table({"x": pa.array([1, 2], type=pa.int64())})
        assert _serialize_arrow_metadata(table) is None

    def test_schema_metadata_only(self):
        table = pa.table(
            {"x": pa.array([1], type=pa.int64())},
            schema=pa.schema(
                [pa.field("x", pa.int64())],
                metadata={b"origin": b"test"},
            ),
        )
        result = _serialize_arrow_metadata(table)
        assert result is not None
        assert _ARROW_METADATA_KEY in result
        blob = json.loads(result[_ARROW_METADATA_KEY].decode())
        assert blob["schema"] == {b64(b"origin"): b64(b"test")}
        assert "fields" not in blob

    def test_field_metadata_only(self):
        schema = pa.schema([pa.field("x", pa.int64(), metadata={b"unit": b"m"})])
        table = pa.table({"x": pa.array([1], type=pa.int64())}, schema=schema)
        result = _serialize_arrow_metadata(table)
        assert result is not None
        blob = json.loads(result[_ARROW_METADATA_KEY].decode())
        assert "schema" not in blob
        assert blob["fields"]["x"] == {"meta": {b64(b"unit"): b64(b"m")}}

    def test_nested_field_metadata(self):
        inner = pa.field("val", pa.float64(), metadata={b"k": b"v"})
        schema = pa.schema([pa.field("s", pa.struct([inner]))])
        table = pa.table(
            {"s": pa.array([{"val": 1.0}], type=pa.struct([pa.field("val", pa.float64())]))},
            schema=schema,
        )
        result = _serialize_arrow_metadata(table)
        assert result is not None
        blob = json.loads(result[_ARROW_METADATA_KEY].decode())
        assert blob["fields"]["s"] == {
            "children": {"val": {"meta": {b64(b"k"): b64(b"v")}}}
        }

    def test_returns_bytes_value(self):
        schema = pa.schema(
            [pa.field("x", pa.int64())], metadata={b"k": b"v"}
        )
        table = pa.table({"x": pa.array([1])}, schema=schema)
        result = _serialize_arrow_metadata(table)
        assert isinstance(result[_ARROW_METADATA_KEY], bytes)
```

- [ ] **Step 2: Run to verify all fail**

```bash
uv run pytest tests/test_databases/test_spiraldb_metadata_helpers.py::TestSerializeFieldMetaTree tests/test_databases/test_spiraldb_metadata_helpers.py::TestSerializeArrowMetadata -v --tb=short
```

Expected: `ImportError` or `ModuleNotFoundError` — helpers don't exist yet.

- [ ] **Step 3: Implement `_ARROW_METADATA_KEY`, `_serialize_field_meta_tree`, `_serialize_arrow_metadata`**

Add immediately after the existing module-level `logger = logging.getLogger(__name__)` line in `src/orcapod/databases/spiraldb_connector.py`:

```python
import base64
import json

_ARROW_METADATA_KEY = "__arrow_metadata__"


def _serialize_field_meta_tree(field: "pa.Field") -> "dict | None":
    """Recursively build the metadata tree for a single ``pa.Field``.

    Walks the field's type tree, collecting ``field.metadata`` at each level.
    Covers struct inner fields, list/large_list/fixed_size_list value fields.
    Map key/item fields are not recursed (``pa.map_`` constructor does not
    accept field-level metadata on key/item).

    Args:
        field: The ``pa.Field`` to serialize.

    Returns:
        A dict with optional ``"meta"`` (base64-encoded k/v pairs) and
        ``"children"`` (recursive trees for composite type children) keys,
        or ``None`` if this field and all its descendants have no metadata.
    """
    import pyarrow as _pa  # noqa: PLC0415

    node: dict = {}

    if field.metadata:
        node["meta"] = {
            base64.b64encode(k).decode(): base64.b64encode(v).decode()
            for k, v in field.metadata.items()
        }

    ftype = field.type
    child_fields: list = []
    if _pa.types.is_struct(ftype):
        child_fields = [ftype.field(i) for i in range(ftype.num_fields)]
    elif (
        _pa.types.is_list(ftype)
        or _pa.types.is_large_list(ftype)
        or _pa.types.is_fixed_size_list(ftype)
    ):
        child_fields = [ftype.value_field]

    if child_fields:
        children: dict = {}
        for child in child_fields:
            child_tree = _serialize_field_meta_tree(child)
            if child_tree is not None:
                children[child.name] = child_tree
        if children:
            node["children"] = children

    return node if node else None


def _serialize_arrow_metadata(table: "pa.Table") -> "dict[str, bytes] | None":
    """Encode all Arrow metadata from ``table`` into a single SpiralDB KV entry.

    Serializes both schema-level (``table.schema.metadata``) and per-field
    metadata (including nested struct/list field metadata) into a single JSON
    blob stored under ``_ARROW_METADATA_KEY``.

    Args:
        table: The Arrow table whose metadata to encode.

    Returns:
        ``{"__arrow_metadata__": blob_bytes}`` if any metadata exists anywhere
        in the schema, or ``None`` if the schema and all fields (at every depth)
        have no metadata.
    """
    blob: dict = {}

    if table.schema.metadata:
        blob["schema"] = {
            base64.b64encode(k).decode(): base64.b64encode(v).decode()
            for k, v in table.schema.metadata.items()
        }

    field_trees: dict = {}
    for field in table.schema:
        tree = _serialize_field_meta_tree(field)
        if tree is not None:
            field_trees[field.name] = tree
    if field_trees:
        blob["fields"] = field_trees

    if not blob:
        return None

    return {_ARROW_METADATA_KEY: json.dumps(blob).encode("utf-8")}
```

- [ ] **Step 4: Run serialization tests — verify they pass**

```bash
uv run pytest tests/test_databases/test_spiraldb_metadata_helpers.py::TestSerializeFieldMetaTree tests/test_databases/test_spiraldb_metadata_helpers.py::TestSerializeArrowMetadata -v
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/databases/spiraldb_connector.py tests/test_databases/test_spiraldb_metadata_helpers.py
git commit -m "feat(spiraldb): add _serialize_field_meta_tree and _serialize_arrow_metadata helpers (ITL-471)"
```

---

## Task 3: Deserialization helpers — `_load_arrow_metadata` and `_restore_field`

**Files:**
- Modify: `tests/test_databases/test_spiraldb_metadata_helpers.py` (append new test classes)
- Modify: `src/orcapod/databases/spiraldb_connector.py` (append new helpers)

- [ ] **Step 1: Write failing tests for `_load_arrow_metadata` and `_restore_field`**

Append to `tests/test_databases/test_spiraldb_metadata_helpers.py`:

```python
# ---------------------------------------------------------------------------
# _load_arrow_metadata
# ---------------------------------------------------------------------------


class TestLoadArrowMetadata:
    def test_empty_kv_returns_none_and_empty(self):
        schema_meta, field_trees = _load_arrow_metadata({})
        assert schema_meta is None
        assert field_trees == {}

    def test_missing_key_returns_none_and_empty(self):
        schema_meta, field_trees = _load_arrow_metadata({"other_key": b"data"})
        assert schema_meta is None
        assert field_trees == {}

    def test_schema_metadata_decoded(self):
        blob = json.dumps({
            "schema": {b64(b"origin"): b64(b"test")}
        }).encode()
        schema_meta, field_trees = _load_arrow_metadata({_ARROW_METADATA_KEY: blob})
        assert schema_meta == {b"origin": b"test"}
        assert field_trees == {}

    def test_field_trees_returned_raw(self):
        blob = json.dumps({
            "fields": {"x": {"meta": {b64(b"unit"): b64(b"m")}}}
        }).encode()
        schema_meta, field_trees = _load_arrow_metadata({_ARROW_METADATA_KEY: blob})
        assert schema_meta is None
        assert "x" in field_trees
        assert field_trees["x"] == {"meta": {b64(b"unit"): b64(b"m")}}

    def test_roundtrip_with_serialize(self):
        schema = pa.schema(
            [pa.field("x", pa.int64(), metadata={b"unit": b"m"})],
            metadata={b"origin": b"test"},
        )
        table = pa.table({"x": pa.array([1])}, schema=schema)
        kv = _serialize_arrow_metadata(table)
        schema_meta, field_trees = _load_arrow_metadata(kv)
        assert schema_meta == {b"origin": b"test"}
        assert "x" in field_trees


# ---------------------------------------------------------------------------
# _restore_field
# ---------------------------------------------------------------------------


class TestRestoreField:
    def test_none_stored_returns_field_unchanged(self):
        field = pa.field("x", pa.int64(), metadata={b"k": b"v"})
        restored, changed = _restore_field(field, None)
        assert restored == field
        assert not changed

    def test_restores_own_metadata(self):
        field = pa.field("x", pa.int64())
        stored = {"meta": {b64(b"unit"): b64(b"m")}}
        restored, changed = _restore_field(field, stored)
        assert changed
        assert restored.metadata == {b"unit": b"m"}
        assert restored.type == pa.int64()
        assert restored.name == "x"

    def test_same_metadata_not_changed(self):
        field = pa.field("x", pa.int64(), metadata={b"k": b"v"})
        stored = {"meta": {b64(b"k"): b64(b"v")}}
        restored, changed = _restore_field(field, stored)
        # metadata is identical — no change needed
        assert not changed

    def test_restores_struct_child_metadata(self):
        inner = pa.field("val", pa.float64())
        field = pa.field("s", pa.struct([inner]))
        stored = {
            "children": {"val": {"meta": {b64(b"k"): b64(b"v")}}}
        }
        restored, changed = _restore_field(field, stored)
        assert changed
        assert pa.types.is_struct(restored.type)
        restored_inner = restored.type.field("val")
        assert restored_inner.metadata == {b"k": b"v"}

    def test_restores_list_value_field_metadata(self):
        vf = pa.field("item", pa.int32())
        field = pa.field("lst", pa.list_(vf))
        stored = {"children": {"item": {"meta": {b64(b"k"): b64(b"v")}}}}
        restored, changed = _restore_field(field, stored)
        assert changed
        assert pa.types.is_list(restored.type)
        assert restored.type.value_field.metadata == {b"k": b"v"}

    def test_restores_large_list_value_field_metadata(self):
        vf = pa.field("item", pa.int32())
        field = pa.field("lst", pa.large_list(vf))
        stored = {"children": {"item": {"meta": {b64(b"k"): b64(b"v")}}}}
        restored, changed = _restore_field(field, stored)
        assert changed
        assert pa.types.is_large_list(restored.type)
        assert restored.type.value_field.metadata == {b"k": b"v"}

    def test_no_children_stored_struct_unchanged(self):
        inner = pa.field("val", pa.float64())
        field = pa.field("s", pa.struct([inner]))
        # stored has meta for the outer field but no children
        stored = {"meta": {b64(b"tag"): b64(b"yes")}}
        restored, changed = _restore_field(field, stored)
        assert changed
        assert restored.metadata == {b"tag": b"yes"}
        # inner field should be unchanged
        assert restored.type.field("val").metadata is None

    def test_nested_struct_in_struct_restored(self):
        leaf = pa.field("z", pa.int8())
        inner_struct = pa.field("inner", pa.struct([leaf]))
        outer = pa.field("outer", pa.struct([inner_struct]))
        stored = {
            "children": {
                "inner": {
                    "children": {"z": {"meta": {b64(b"deep"): b64(b"yes")}}}
                }
            }
        }
        restored, changed = _restore_field(outer, stored)
        assert changed
        inner_type = restored.type.field("inner").type
        assert inner_type.field("z").metadata == {b"deep": b"yes"}

    def test_full_roundtrip_serialize_then_restore(self):
        """Serialize a field's metadata tree then restore it — must be identity."""
        vf = pa.field("item", pa.int32(), metadata={b"unit": b"bytes"})
        field = pa.field("lst", pa.list_(vf), metadata={b"role": b"data"})

        tree = _serialize_field_meta_tree(field)
        restored, changed = _restore_field(field, tree)
        assert changed is False  # already has this metadata, nothing changed
        assert restored.metadata == field.metadata
        assert restored.type.value_field.metadata == vf.metadata
```

- [ ] **Step 2: Run to verify they fail**

```bash
uv run pytest tests/test_databases/test_spiraldb_metadata_helpers.py::TestLoadArrowMetadata tests/test_databases/test_spiraldb_metadata_helpers.py::TestRestoreField -v --tb=short
```

Expected: `ImportError` — `_load_arrow_metadata` and `_restore_field` not defined yet.

- [ ] **Step 3: Implement `_load_arrow_metadata`, `_restore_arrow_type`, and `_restore_field`**

Append to `src/orcapod/databases/spiraldb_connector.py` (after `_serialize_arrow_metadata`):

```python
def _load_arrow_metadata(
    kv: "dict[str, bytes]",
) -> "tuple[dict[bytes, bytes] | None, dict[str, dict]]":
    """Decode Arrow metadata from a SpiralDB table KV store.

    Args:
        kv: The raw KV dict from ``Table.get_metadata()``.

    Returns:
        A tuple ``(schema_meta, field_trees)`` where ``schema_meta`` is None
        if absent and ``field_trees`` maps top-level column name to its raw
        metadata tree dict (as stored in the blob by ``_serialize_arrow_metadata``).
    """
    raw = kv.get(_ARROW_METADATA_KEY)
    if raw is None:
        return None, {}

    blob = json.loads(raw.decode("utf-8"))

    schema_meta: "dict[bytes, bytes] | None" = None
    if "schema" in blob:
        schema_meta = {
            base64.b64decode(k): base64.b64decode(v)
            for k, v in blob["schema"].items()
        }

    field_trees: "dict[str, dict]" = blob.get("fields", {})
    return schema_meta, field_trees


def _restore_arrow_type(
    arrow_type: "pa.DataType",
    children_stored: "dict[str, dict]",
) -> "tuple[pa.DataType, bool]":
    """Recursively restore nested field metadata within an Arrow type.

    Covers ``pa.struct`` (all inner fields), ``pa.list_`` / ``pa.large_list``
    / ``pa.fixed_size_list`` (value field). ``pa.map_`` key/item fields are
    not recursed — the ``pa.map_`` constructor does not accept field-level
    metadata on key or item.

    Args:
        arrow_type: The Arrow type whose nested fields to restore.
        children_stored: Dict of child field name → raw metadata tree.

    Returns:
        ``(new_type, changed)`` — ``changed`` is True if any descendant was
        modified.
    """
    import pyarrow as _pa  # noqa: PLC0415

    if _pa.types.is_struct(arrow_type):
        new_fields = []
        any_changed = False
        for i in range(arrow_type.num_fields):
            child = arrow_type.field(i)
            restored_child, changed = _restore_field(child, children_stored.get(child.name))
            new_fields.append(restored_child)
            any_changed = any_changed or changed
        if any_changed:
            return _pa.struct(new_fields), True
        return arrow_type, False

    if _pa.types.is_list(arrow_type):
        vf = arrow_type.value_field
        restored_vf, changed = _restore_field(vf, children_stored.get(vf.name))
        if changed:
            return _pa.list_(restored_vf), True
        return arrow_type, False

    if _pa.types.is_large_list(arrow_type):
        vf = arrow_type.value_field
        restored_vf, changed = _restore_field(vf, children_stored.get(vf.name))
        if changed:
            return _pa.large_list(restored_vf), True
        return arrow_type, False

    if _pa.types.is_fixed_size_list(arrow_type):
        vf = arrow_type.value_field
        restored_vf, changed = _restore_field(vf, children_stored.get(vf.name))
        if changed:
            return _pa.list_(restored_vf, arrow_type.list_size), True
        return arrow_type, False

    return arrow_type, False


def _restore_field(
    field: "pa.Field",
    stored: "dict | None",
) -> "tuple[pa.Field, bool]":
    """Recursively restore a field's metadata and rebuild its nested type.

    Walks the stored metadata tree in parallel with the field's type tree,
    reattaching ``field.metadata`` at every level and reconstructing composite
    types bottom-up when any descendant has metadata to restore.

    Args:
        field: The ``pa.Field`` from the wire (SpiralDB scan output).
        stored: The raw metadata tree dict for this field, as decoded by
            ``_load_arrow_metadata``. ``None`` if no stored metadata exists
            for this field.

    Returns:
        ``(restored_field, changed)`` where ``changed`` is True if any
        metadata or type was modified relative to ``field``.
    """
    import base64 as _base64  # noqa: PLC0415
    import pyarrow as _pa  # noqa: PLC0415

    if stored is None:
        return field, False

    # Decode this field's own metadata
    restored_meta: "dict[bytes, bytes] | None" = None
    if "meta" in stored:
        restored_meta = {
            _base64.b64decode(k): _base64.b64decode(v)
            for k, v in stored["meta"].items()
        }

    # Recursively restore the nested type
    children_stored = stored.get("children", {})
    new_type, type_changed = _restore_arrow_type(field.type, children_stored)

    meta_changed = restored_meta is not None and restored_meta != field.metadata
    if not meta_changed and not type_changed:
        return field, False

    final_meta = restored_meta if restored_meta is not None else field.metadata
    return _pa.field(field.name, new_type, field.nullable, final_meta), True
```

- [ ] **Step 4: Run deserialization tests — verify they pass**

```bash
uv run pytest tests/test_databases/test_spiraldb_metadata_helpers.py -v
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/databases/spiraldb_connector.py tests/test_databases/test_spiraldb_metadata_helpers.py
git commit -m "feat(spiraldb): add _load_arrow_metadata and _restore_field helpers (ITL-471)"
```

---

## Task 4: `validate_records` — protocol, connectors, and `ConnectorArrowDatabase`

**Files:**
- Modify: `src/orcapod/protocols/db_connector_protocol.py`
- Modify: `src/orcapod/databases/sqlite_connector.py`
- Modify: `src/orcapod/databases/postgresql_connector.py`
- Modify: `src/orcapod/databases/spiraldb_connector.py`
- Modify: `src/orcapod/databases/connector_arrow_database.py`
- Modify: `tests/test_databases/test_connector_arrow_database.py`
- Modify: `tests/test_databases/test_spiraldb_connector.py`

- [ ] **Step 1: Write failing tests**

**In `tests/test_databases/test_spiraldb_connector.py`**, append a new test class:

```python
class TestValidateRecords:
    def test_no_op_for_plain_columns(self, connector):
        """validate_records is a no-op on SpiralDBConnector for plain columns."""
        table = pa.table({
            "id": pa.array([b"a"], type=pa.large_binary()),
            "value": pa.array([1], type=pa.int64()),
        })
        connector.validate_records(table)  # must not raise

    def test_no_op_for_extension_typed_columns(self, connector):
        """SpiralDBConnector.validate_records allows extension-typed columns."""
        ext_field = pa.field(
            "payload",
            pa.large_string(),
            metadata={
                b"ARROW:extension:name": b"orcapod.path",
                b"ARROW:extension:metadata": b"",
            },
        )
        schema = pa.schema([pa.field("id", pa.large_binary()), ext_field])
        table = pa.table(
            {"id": pa.array([b"a"], type=pa.large_binary()),
             "payload": pa.array(["/tmp/x"], type=pa.large_string())},
            schema=schema,
        )
        connector.validate_records(table)  # must not raise — SpiralDB supports extension types
```

**In `tests/test_databases/test_connector_arrow_database.py`**, update `TestExtensionTypeWriteGuard`:

The fixture uses `MockDBConnector()`. `MockDBConnector` must gain a `validate_records` method. The existing tests (`test_rejects_in_memory_extension_type_column`, `test_rejects_metadata_only_extension_column`) should still raise `ValueError` because `MockDBConnector` uses the rejecting default. `test_plain_column_not_rejected` should still pass.

Add a new test at the end of `TestExtensionTypeWriteGuard`:

```python
    def test_permissive_connector_allows_extension_types(self):
        """A connector with a permissive validate_records lets extension columns through."""
        import pyarrow as pa

        class PermissiveConnector(MockDBConnector):
            def validate_records(self, records):
                pass  # no-op — this connector supports metadata

        db = ConnectorArrowDatabase(PermissiveConnector())
        ext_field = pa.field(
            "payload",
            pa.large_string(),
            metadata={
                b"ARROW:extension:name": b"orcapod.path",
                b"ARROW:extension:metadata": b"",
            },
        )
        schema = pa.schema([pa.field("__record_id", pa.large_binary()), ext_field])
        table = pa.table(
            {"__record_id": pa.array([b"id1"], type=pa.large_binary()),
             "payload": pa.array(["/tmp/test"], type=pa.large_string())},
            schema=schema,
        )
        db.add_records(("results",), table, record_id_column="__record_id")  # must not raise
```

- [ ] **Step 2: Run to verify they fail**

```bash
uv run pytest tests/test_databases/test_spiraldb_connector.py::TestValidateRecords tests/test_databases/test_connector_arrow_database.py::TestExtensionTypeWriteGuard::test_permissive_connector_allows_extension_types -v --tb=short
```

Expected: `AttributeError: 'SpiralDBConnector' object has no attribute 'validate_records'`

- [ ] **Step 3: Add `validate_records` to `DBConnectorProtocol`**

In `src/orcapod/protocols/db_connector_protocol.py`, add after the `upsert_records` method (before the `# ── Lifecycle` section):

```python
    def validate_records(self, records: "pa.Table") -> None:
        """Validate that ``records`` are safe to write through this connector.

        Implementations should raise ``ValueError`` if ``records`` contain
        columns that this connector cannot round-trip faithfully (e.g. Arrow
        extension-typed columns on connectors that drop field metadata).

        A connector that fully preserves Arrow field metadata should implement
        this as a no-op.

        Args:
            records: Arrow table to validate before writing.

        Raises:
            ValueError: If any column is incompatible with this connector.
        """
        ...
```

- [ ] **Step 4: Add rejecting `validate_records` to `SQLiteConnector`**

In `src/orcapod/databases/sqlite_connector.py`, add after the `upsert_records` method (before `def close`):

```python
    def validate_records(self, records: "pa.Table") -> None:
        """Reject Arrow extension-typed columns.

        ``SQLiteConnector`` does not preserve ``ARROW:extension:*`` field
        metadata — extension types would be silently demoted to their storage
        type on read.

        Args:
            records: Arrow table to validate.

        Raises:
            ValueError: If any column carries an Arrow extension type.
        """
        import pyarrow as _pa  # noqa: PLC0415

        _EXT_NAME_KEY = b"ARROW:extension:name"
        ext_fields: list[tuple[str, str]] = []
        for field in records.schema:
            if isinstance(field.type, _pa.ExtensionType):
                ext_fields.append((field.name, field.type.extension_name))
            elif field.metadata and _EXT_NAME_KEY in field.metadata:
                ext_fields.append(
                    (field.name, field.metadata[_EXT_NAME_KEY].decode("utf-8", errors="replace"))
                )
        if ext_fields:
            ext_info = ", ".join(f"{name!r}: {ext_name!r}" for name, ext_name in ext_fields)
            raise ValueError(
                f"SQLiteConnector does not support Arrow extension-typed columns "
                f"({ext_info}). SQLiteConnector does not preserve ARROW:extension:* "
                "field metadata across read/write cycles."
            )
```

- [ ] **Step 5: Add rejecting `validate_records` to `PostgreSQLConnector`**

In `src/orcapod/databases/postgresql_connector.py`, add after the `upsert_records` method (before `def close`):

```python
    def validate_records(self, records: "pa.Table") -> None:
        """Reject Arrow extension-typed columns.

        ``PostgreSQLConnector`` does not preserve ``ARROW:extension:*`` field
        metadata — extension types would be silently demoted to their storage
        type on read.

        Args:
            records: Arrow table to validate.

        Raises:
            ValueError: If any column carries an Arrow extension type.
        """
        import pyarrow as _pa  # noqa: PLC0415

        _EXT_NAME_KEY = b"ARROW:extension:name"
        ext_fields: list[tuple[str, str]] = []
        for field in records.schema:
            if isinstance(field.type, _pa.ExtensionType):
                ext_fields.append((field.name, field.type.extension_name))
            elif field.metadata and _EXT_NAME_KEY in field.metadata:
                ext_fields.append(
                    (field.name, field.metadata[_EXT_NAME_KEY].decode("utf-8", errors="replace"))
                )
        if ext_fields:
            ext_info = ", ".join(f"{name!r}: {ext_name!r}" for name, ext_name in ext_fields)
            raise ValueError(
                f"PostgreSQLConnector does not support Arrow extension-typed columns "
                f"({ext_info}). PostgreSQLConnector does not preserve ARROW:extension:* "
                "field metadata across read/write cycles."
            )
```

- [ ] **Step 6: Add no-op `validate_records` to `SpiralDBConnector`**

In `src/orcapod/databases/spiraldb_connector.py`, add after the `upsert_records` method (before `def close`):

```python
    def validate_records(self, records: "pa.Table") -> None:
        """No-op: ``SpiralDBConnector`` preserves Arrow field metadata via the native KV store."""
```

- [ ] **Step 7: Add `validate_records` to `MockDBConnector` in the test file**

In `tests/test_databases/test_connector_arrow_database.py`, add after `MockDBConnector.upsert_records` (before `def close`):

```python
    def validate_records(self, records: "pa.Table") -> None:
        """Rejecting default — mirrors real SQL connectors for test fidelity."""
        import pyarrow as _pa  # noqa: PLC0415

        _EXT_NAME_KEY = b"ARROW:extension:name"
        ext_fields: list[tuple[str, str]] = []
        for field in records.schema:
            if isinstance(field.type, _pa.ExtensionType):
                ext_fields.append((field.name, field.type.extension_name))
            elif field.metadata and _EXT_NAME_KEY in field.metadata:
                ext_fields.append(
                    (field.name, field.metadata[_EXT_NAME_KEY].decode("utf-8", errors="replace"))
                )
        if ext_fields:
            ext_info = ", ".join(f"{name!r}: {ext_name!r}" for name, ext_name in ext_fields)
            raise ValueError(
                f"MockDBConnector does not support Arrow extension-typed columns ({ext_info})."
            )
```

- [ ] **Step 8: Replace the inline guard in `ConnectorArrowDatabase.add_records`**

In `src/orcapod/databases/connector_arrow_database.py`, find the block starting with:

```python
        # Reject Arrow extension-typed columns: SQL connectors do not preserve
        # ARROW:extension:* field metadata, so extension types would be silently
        # dropped on read, making round-trips impossible.  Use DeltaTableDatabase
        # or write directly to Parquet instead.  See PLT-1795 for the planned fix.
        #
        # Two representations are checked:
        # 1. In-memory extension types: isinstance(field.type, pa.ExtensionType).
        # 2. Metadata-only extension columns: a plain Arrow type whose field metadata
        #    contains the b"ARROW:extension:name" key.  This arises when reading a
        #    Parquet/IPC file with an unregistered extension type — the array is
        #    decoded as its storage type but the metadata is preserved on the field.
        _EXT_NAME_KEY = b"ARROW:extension:name"
        ext_fields: list[tuple[str, str]] = []
        for field in records.schema:
            if isinstance(field.type, pa.ExtensionType):
                ext_fields.append((field.name, field.type.extension_name))
            elif field.metadata and _EXT_NAME_KEY in field.metadata:
                ext_fields.append((field.name, field.metadata[_EXT_NAME_KEY].decode("utf-8", errors="replace")))
        if ext_fields:
            ext_info = ", ".join(f"{name!r}: {ext_name!r}" for name, ext_name in ext_fields)
            raise ValueError(
                f"ConnectorArrowDatabase does not support Arrow extension-typed columns "
                f"({ext_info}). SQL connectors do not preserve ARROW:extension:* field "
                f"metadata, so extension types would be silently dropped on read. "
                f"Use DeltaTableDatabase or write directly to Parquet instead. "
                f"See PLT-1795 for the planned fix."
            )
```

Replace it entirely with:

```python
        self._connector.validate_records(records)
```

- [ ] **Step 9: Run validate_records tests**

```bash
uv run pytest tests/test_databases/test_spiraldb_connector.py::TestValidateRecords tests/test_databases/test_connector_arrow_database.py::TestExtensionTypeWriteGuard -v
```

Expected: all pass.

- [ ] **Step 10: Run full test suite to check nothing broke**

```bash
uv run pytest tests/test_databases/ -v --tb=short -q
```

Expected: all pass (integration tests skipped).

- [ ] **Step 11: Commit**

```bash
git add \
  src/orcapod/protocols/db_connector_protocol.py \
  src/orcapod/databases/sqlite_connector.py \
  src/orcapod/databases/postgresql_connector.py \
  src/orcapod/databases/spiraldb_connector.py \
  src/orcapod/databases/connector_arrow_database.py \
  tests/test_databases/test_connector_arrow_database.py \
  tests/test_databases/test_spiraldb_connector.py
git commit -m "feat(spiraldb): add per-connector validate_records hook, replace ConnectorArrowDatabase guard (ITL-471)"
```

---

## Task 5: Write path — persist metadata in `upsert_records`

**Files:**
- Modify: `tests/test_databases/test_spiraldb_connector.py` (append new test class)
- Modify: `src/orcapod/databases/spiraldb_connector.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_databases/test_spiraldb_connector.py`:

```python
class TestUpsertRecordsMetadata:
    def _make_mock_table(self, mock_project, pk_cols: list[str]) -> MagicMock:
        mock_table = MagicMock()
        mock_table.key_schema.names = pk_cols
        mock_project.table.return_value = mock_table
        return mock_table

    def test_set_metadata_called_after_write_when_schema_metadata_present(
        self, connector, mock_project
    ):
        mock_tbl = self._make_mock_table(mock_project, ["id"])
        schema = pa.schema(
            [pa.field("id", pa.string()), pa.field("val", pa.int64())],
            metadata={b"origin": b"test"},
        )
        records = pa.table(
            {"id": pa.array(["a"]), "val": pa.array([1], type=pa.int64())},
            schema=schema,
        )
        connector.upsert_records("t", records, id_column="id")
        mock_tbl.write.assert_called_once()
        mock_tbl.set_metadata.assert_called_once()
        kv = mock_tbl.set_metadata.call_args[0][0]
        assert "__arrow_metadata__" in kv

    def test_set_metadata_called_when_field_metadata_present(
        self, connector, mock_project
    ):
        mock_tbl = self._make_mock_table(mock_project, ["id"])
        schema = pa.schema([
            pa.field("id", pa.string()),
            pa.field("val", pa.int64(), metadata={b"unit": b"m"}),
        ])
        records = pa.table(
            {"id": pa.array(["a"]), "val": pa.array([1], type=pa.int64())},
            schema=schema,
        )
        connector.upsert_records("t", records, id_column="id")
        mock_tbl.set_metadata.assert_called_once()

    def test_set_metadata_not_called_when_no_metadata(
        self, connector, mock_project
    ):
        mock_tbl = self._make_mock_table(mock_project, ["id"])
        records = pa.table({"id": pa.array(["a"]), "val": pa.array([1])})
        connector.upsert_records("t", records, id_column="id")
        mock_tbl.write.assert_called_once()
        mock_tbl.set_metadata.assert_not_called()

    def test_set_metadata_called_for_skip_existing_path(
        self, connector, mock_sp, mock_project
    ):
        mock_tbl = self._make_mock_table(mock_project, ["id"])
        existing = pa.table({"id": pa.array(["a"]), "val": pa.array([1])})
        mock_sp.Spiral.return_value.scan.return_value.to_table.return_value = existing
        schema = pa.schema(
            [pa.field("id", pa.string()), pa.field("val", pa.int64())],
            metadata={b"version": b"1"},
        )
        records = pa.table(
            {"id": pa.array(["a", "b"]), "val": pa.array([1, 2], type=pa.int64())},
            schema=schema,
        )
        connector.upsert_records("t", records, id_column="id", skip_existing=True)
        # "b" is novel — write called; metadata from original records, not filtered subset
        mock_tbl.write.assert_called_once()
        mock_tbl.set_metadata.assert_called_once()
        kv = mock_tbl.set_metadata.call_args[0][0]
        import json, base64
        blob = json.loads(kv["__arrow_metadata__"].decode())
        assert blob["schema"][base64.b64encode(b"version").decode()] == base64.b64encode(b"1").decode()

    def test_set_metadata_not_called_when_skip_existing_all_exist(
        self, connector, mock_sp, mock_project
    ):
        mock_tbl = self._make_mock_table(mock_project, ["id"])
        existing = pa.table({"id": pa.array(["a", "b"]), "val": pa.array([1, 2])})
        mock_sp.Spiral.return_value.scan.return_value.to_table.return_value = existing
        records = pa.table({"id": pa.array(["a", "b"]), "val": pa.array([10, 20])})
        connector.upsert_records("t", records, id_column="id", skip_existing=True)
        mock_tbl.write.assert_not_called()
        mock_tbl.set_metadata.assert_not_called()
```

- [ ] **Step 2: Run to verify they fail**

```bash
uv run pytest tests/test_databases/test_spiraldb_connector.py::TestUpsertRecordsMetadata -v --tb=short
```

Expected: `AssertionError` — `set_metadata` not called (feature not implemented).

- [ ] **Step 3: Modify `upsert_records` to write metadata**

In `src/orcapod/databases/spiraldb_connector.py`, find the `upsert_records` method. The current structure is:

```python
        if not skip_existing:
            # Always upsert-by-key: existing rows overwritten, novel rows inserted.
            tbl.write(records)
            return

        # skip_existing=True: full scan → client-side key filter → write novel rows.
        existing = self._spiral.scan(tbl.select()).to_table()
        ...
        if len(novel) > 0:
            tbl.write(novel)
```

Replace the body after the guards (starting from `if not skip_existing:`) with:

```python
        meta_kv = _serialize_arrow_metadata(records)

        if not skip_existing:
            # Always upsert-by-key: existing rows overwritten, novel rows inserted.
            tbl.write(records)
            if meta_kv is not None:
                tbl.set_metadata(meta_kv)
            return

        # skip_existing=True: full scan → client-side key filter → write novel rows.
        existing = self._spiral.scan(tbl.select()).to_table()
        existing_keys = {
            tuple(row[k] for k in pk_cols)
            for row in existing.to_pylist()
        }
        mask = pa.array(
            [
                tuple(row[k] for k in pk_cols) not in existing_keys
                for row in records.to_pylist()
            ],
            type=pa.bool_(),
        )
        novel = records.filter(mask)
        if len(novel) > 0:
            tbl.write(novel)
            if meta_kv is not None:
                tbl.set_metadata(meta_kv)
```

- [ ] **Step 4: Run write-path tests**

```bash
uv run pytest tests/test_databases/test_spiraldb_connector.py::TestUpsertRecordsMetadata -v
```

Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/orcapod/databases/spiraldb_connector.py tests/test_databases/test_spiraldb_connector.py
git commit -m "feat(spiraldb): persist Arrow metadata in upsert_records via KV store (ITL-471)"
```

---

## Task 6: Read path — restore metadata in `iter_batches`

**Files:**
- Modify: `tests/test_databases/test_spiraldb_connector.py` (append new test class)
- Modify: `src/orcapod/databases/spiraldb_connector.py`

- [ ] **Step 1: Write failing tests**

Append to `tests/test_databases/test_spiraldb_connector.py`:

```python
class TestIterBatchesMetadata:
    def _wire_scan(self, mock_sp, mock_project, batches: list, kv: dict | None = None) -> None:
        mock_project.table.return_value.select.return_value = MagicMock()
        mock_project.table.return_value.get_metadata.return_value = kv or {}
        mock_sp.Spiral.return_value.scan.return_value.to_record_batches.return_value = iter(batches)

    def test_schema_metadata_reattached(self, connector, mock_sp, mock_project):
        import json, base64
        stored_blob = json.dumps({
            "schema": {
                base64.b64encode(b"origin").decode(): base64.b64encode(b"test").decode()
            }
        }).encode()
        raw_batch = pa.record_batch({"id": pa.array(["a"], type=pa.string())})
        self._wire_scan(mock_sp, mock_project, [raw_batch], {"__arrow_metadata__": stored_blob})

        result = list(connector.iter_batches('SELECT * FROM "t"'))
        assert len(result) == 1
        assert result[0].schema.metadata == {b"origin": b"test"}

    def test_field_metadata_reattached(self, connector, mock_sp, mock_project):
        import json, base64
        stored_blob = json.dumps({
            "fields": {
                "val": {"meta": {
                    base64.b64encode(b"unit").decode(): base64.b64encode(b"meters").decode()
                }}
            }
        }).encode()
        raw_batch = pa.record_batch({
            "id": pa.array(["a"], type=pa.string()),
            "val": pa.array([1.0], type=pa.float64()),
        })
        self._wire_scan(mock_sp, mock_project, [raw_batch], {"__arrow_metadata__": stored_blob})

        result = list(connector.iter_batches('SELECT * FROM "t"'))
        assert result[0].schema.field("val").metadata == {b"unit": b"meters"}

    def test_string_normalization_preserved_with_metadata(self, connector, mock_sp, mock_project):
        """string → large_string normalization still applies when field metadata present."""
        import json, base64
        stored_blob = json.dumps({
            "fields": {
                "label": {"meta": {
                    base64.b64encode(b"ARROW:extension:name").decode(): base64.b64encode(b"orcapod.path").decode()
                }}
            }
        }).encode()
        # SpiralDB returns pa.string() at the wire
        raw_batch = pa.record_batch({"label": pa.array(["x"], type=pa.string())})
        self._wire_scan(mock_sp, mock_project, [raw_batch], {"__arrow_metadata__": stored_blob})

        result = list(connector.iter_batches('SELECT * FROM "t"'))
        batch = result[0]
        # type must be large_string (normalized)
        assert batch.schema.field("label").type == pa.large_string()
        # metadata must be restored
        assert batch.schema.field("label").metadata == {b"ARROW:extension:name": b"orcapod.path"}

    def test_no_stored_metadata_behaves_as_before(self, connector, mock_sp, mock_project):
        """Tables with no stored metadata are returned unchanged (backward compat)."""
        raw_batch = pa.record_batch({"id": pa.array(["a"], type=pa.string()), "v": pa.array([1])})
        self._wire_scan(mock_sp, mock_project, [raw_batch], {})

        result = list(connector.iter_batches('SELECT * FROM "t"'))
        assert result[0].schema.field("id").type == pa.large_string()
        assert result[0].schema.metadata is None

    def test_get_metadata_called_once_per_scan_not_per_batch(self, connector, mock_sp, mock_project):
        b1 = pa.record_batch({"id": pa.array(["a"])})
        b2 = pa.record_batch({"id": pa.array(["b"])})
        self._wire_scan(mock_sp, mock_project, [b1, b2], {})
        list(connector.iter_batches('SELECT * FROM "t"'))
        mock_project.table.return_value.get_metadata.assert_called_once()
```

- [ ] **Step 2: Run to verify they fail**

```bash
uv run pytest tests/test_databases/test_spiraldb_connector.py::TestIterBatchesMetadata -v --tb=short
```

Expected: `AssertionError` — metadata not reattached (feature not implemented).

- [ ] **Step 3: Modify `iter_batches` to restore metadata**

In `src/orcapod/databases/spiraldb_connector.py`, find `iter_batches`. Replace the body starting from `table_name = _parse_table_name(query)` onward:

```python
        table_name = _parse_table_name(query)
        tbl = self._project.table(self._table_id(table_name))
        schema_meta, field_trees = _load_arrow_metadata(tbl.get_metadata())
        reader = self._spiral.scan(tbl.select()).to_record_batches()
        for batch in reader:
            schema = batch.schema
            new_fields = []
            needs_rebuild = False
            for field in schema:
                # Recursively restore field metadata (including nested struct/list).
                restored_field, field_changed = _restore_field(field, field_trees.get(field.name))
                # Apply large_string / large_binary normalization on the top-level type.
                if restored_field.type == _pa.string():
                    restored_field = _pa.field(
                        restored_field.name,
                        _pa.large_string(),
                        restored_field.nullable,
                        restored_field.metadata,
                    )
                    field_changed = True
                elif restored_field.type == _pa.binary():
                    restored_field = _pa.field(
                        restored_field.name,
                        _pa.large_binary(),
                        restored_field.nullable,
                        restored_field.metadata,
                    )
                    field_changed = True
                new_fields.append(restored_field)
                needs_rebuild = needs_rebuild or field_changed

            restored_schema_meta = schema_meta if schema_meta is not None else schema.metadata
            if restored_schema_meta != schema.metadata:
                needs_rebuild = True

            if needs_rebuild:
                target_schema = _pa.schema(new_fields, metadata=restored_schema_meta)
                batch = batch.cast(target_schema)
            yield batch
```

- [ ] **Step 4: Run read-path tests**

```bash
uv run pytest tests/test_databases/test_spiraldb_connector.py::TestIterBatchesMetadata -v
```

Expected: all pass.

- [ ] **Step 5: Run full test suite**

```bash
uv run pytest tests/test_databases/ -v --tb=short -q
```

Expected: all pass (integration tests skipped).

- [ ] **Step 6: Commit**

```bash
git add src/orcapod/databases/spiraldb_connector.py tests/test_databases/test_spiraldb_connector.py
git commit -m "feat(spiraldb): restore Arrow metadata in iter_batches via recursive _restore_field (ITL-471)"
```

---

## Task 7: Integration tests

**Files:**
- Modify: `tests/test_databases/test_spiraldb_connector_integration.py`

- [ ] **Step 1: Append `TestArrowMetadataRoundTrip` class**

Append to `tests/test_databases/test_spiraldb_connector_integration.py`:

```python
class TestArrowMetadataRoundTrip:
    """Integration tests for Arrow metadata preservation across write→read cycles.

    Requires SPIRAL_INTEGRATION_TESTS=1 and valid SpiralDB credentials.
    """

    def test_schema_metadata_round_trip(self, connector):
        """Schema-level metadata survives a full write→read cycle."""
        table_name = _unique_table("schema_meta")
        schema = pa.schema(
            [
                pa.field("__record_id", pa.string(), nullable=False),
                pa.field("value", pa.float64()),
            ],
            metadata={b"origin": b"test-suite", b"version": b"1"},
        )
        records = pa.table(
            {"__record_id": pa.array(["r1"]), "value": pa.array([1.0])},
            schema=schema,
        )
        try:
            connector.create_table_if_not_exists(
                table_name,
                columns=[
                    ColumnInfo("__record_id", pa.string(), nullable=False),
                    ColumnInfo("value", pa.float64()),
                ],
                pk_column="__record_id",
            )
            connector.upsert_records(table_name, records, id_column="__record_id")

            batches = list(connector.iter_batches(f'SELECT * FROM "{table_name}"'))
            result = pa.Table.from_batches(batches)
            assert result.schema.metadata is not None
            assert result.schema.metadata.get(b"origin") == b"test-suite"
            assert result.schema.metadata.get(b"version") == b"1"
        finally:
            connector.delete_table(table_name)

    def test_field_metadata_round_trip(self, connector):
        """Per-column field metadata survives a full write→read cycle."""
        table_name = _unique_table("field_meta")
        schema = pa.schema([
            pa.field("__record_id", pa.string(), nullable=False),
            pa.field("value", pa.float64(), metadata={b"unit": b"meters", b"sensor": b"lidar"}),
        ])
        records = pa.table(
            {"__record_id": pa.array(["r1"]), "value": pa.array([42.0])},
            schema=schema,
        )
        try:
            connector.create_table_if_not_exists(
                table_name,
                columns=[
                    ColumnInfo("__record_id", pa.string(), nullable=False),
                    ColumnInfo("value", pa.float64()),
                ],
                pk_column="__record_id",
            )
            connector.upsert_records(table_name, records, id_column="__record_id")

            batches = list(connector.iter_batches(f'SELECT * FROM "{table_name}"'))
            result = pa.Table.from_batches(batches)
            value_meta = result.schema.field("value").metadata
            assert value_meta is not None
            assert value_meta.get(b"unit") == b"meters"
            assert value_meta.get(b"sensor") == b"lidar"
        finally:
            connector.delete_table(table_name)

    def test_extension_type_metadata_round_trip(self, connector):
        """ARROW:extension:name / ARROW:extension:metadata field metadata survives."""
        table_name = _unique_table("ext_meta")
        ext_field = pa.field(
            "path_col",
            pa.large_string(),
            metadata={
                b"ARROW:extension:name": b"orcapod.path",
                b"ARROW:extension:metadata": b"",
            },
        )
        schema = pa.schema([
            pa.field("__record_id", pa.string(), nullable=False),
            ext_field,
        ])
        records = pa.table(
            {
                "__record_id": pa.array(["r1"]),
                "path_col": pa.array(["/tmp/test"], type=pa.large_string()),
            },
            schema=schema,
        )
        try:
            connector.create_table_if_not_exists(
                table_name,
                columns=[
                    ColumnInfo("__record_id", pa.string(), nullable=False),
                    ColumnInfo("path_col", pa.large_string()),
                ],
                pk_column="__record_id",
            )
            connector.upsert_records(table_name, records, id_column="__record_id")

            batches = list(connector.iter_batches(f'SELECT * FROM "{table_name}"'))
            result = pa.Table.from_batches(batches)
            meta = result.schema.field("path_col").metadata
            assert meta is not None
            assert meta.get(b"ARROW:extension:name") == b"orcapod.path"
        finally:
            connector.delete_table(table_name)

    def test_no_metadata_backward_compatible(self, connector):
        """Plain tables with no Arrow metadata read back without spurious KV entries."""
        table_name = _unique_table("no_meta")
        try:
            connector.create_table_if_not_exists(
                table_name,
                columns=[
                    ColumnInfo("__record_id", pa.string(), nullable=False),
                    ColumnInfo("value", pa.int64()),
                ],
                pk_column="__record_id",
            )
            records = pa.table({
                "__record_id": pa.array(["r1"]),
                "value": pa.array([99], type=pa.int64()),
            })
            connector.upsert_records(table_name, records, id_column="__record_id")

            batches = list(connector.iter_batches(f'SELECT * FROM "{table_name}"'))
            result = pa.Table.from_batches(batches)
            assert result.schema.metadata is None
            for field in result.schema:
                assert field.metadata is None
        finally:
            connector.delete_table(table_name)

    def test_connector_arrow_database_extension_type_round_trip(self, connector):
        """Full ConnectorArrowDatabase path: write extension-typed column, read back intact."""
        unique_suffix = uuid.uuid4().hex[:8]
        path = ("spiraldb", f"ext_roundtrip_{unique_suffix}")
        table_name = f"spiraldb__ext_roundtrip_{unique_suffix}"

        ext_field = pa.field(
            "path_col",
            pa.large_string(),
            metadata={
                b"ARROW:extension:name": b"orcapod.path",
                b"ARROW:extension:metadata": b"",
            },
        )
        schema = pa.schema([pa.field("__record_id", pa.large_binary()), ext_field])
        record = pa.table(
            {
                "__record_id": pa.array([b"test_r1"], type=pa.large_binary()),
                "path_col": pa.array(["/data/file.npy"], type=pa.large_string()),
            },
            schema=schema,
        )

        db = ConnectorArrowDatabase(connector)
        try:
            db.add_record(path, record_id=b"test_r1", record=record, flush=True)
            result = db.get_record_by_id(path, b"test_r1")
            assert result is not None
            meta = result.schema.field("path_col").metadata
            assert meta is not None
            assert meta.get(b"ARROW:extension:name") == b"orcapod.path"
        finally:
            connector.delete_table(table_name)

    def test_nested_struct_field_metadata_round_trip(self, connector):
        """Struct columns with inner-field metadata survive a full write→read cycle."""
        table_name = _unique_table("struct_meta")
        inner_field = pa.field("val", pa.float64(), metadata={b"unit": b"volts"})
        struct_col = pa.field("measurement", pa.struct([inner_field]))
        schema = pa.schema([
            pa.field("__record_id", pa.string(), nullable=False),
            struct_col,
        ])
        records = pa.table(
            {
                "__record_id": pa.array(["r1"]),
                "measurement": pa.array([{"val": 3.14}], type=pa.struct([pa.field("val", pa.float64())])),
            },
            schema=schema,
        )
        try:
            connector.create_table_if_not_exists(
                table_name,
                columns=[
                    ColumnInfo("__record_id", pa.string(), nullable=False),
                    ColumnInfo("measurement", pa.struct([pa.field("val", pa.float64())])),
                ],
                pk_column="__record_id",
            )
            connector.upsert_records(table_name, records, id_column="__record_id")

            batches = list(connector.iter_batches(f'SELECT * FROM "{table_name}"'))
            result = pa.Table.from_batches(batches)
            inner = result.schema.field("measurement").type.field("val")
            assert inner.metadata is not None
            assert inner.metadata.get(b"unit") == b"volts"
        finally:
            connector.delete_table(table_name)
```

- [ ] **Step 2: Verify integration tests are skipped (no credentials)**

```bash
uv run pytest tests/test_databases/test_spiraldb_connector_integration.py -v --tb=short
```

Expected: all skipped (`Set SPIRAL_INTEGRATION_TESTS=1 to run SpiralDB integration tests`).

- [ ] **Step 3: Commit**

```bash
git add tests/test_databases/test_spiraldb_connector_integration.py
git commit -m "test(spiraldb): add TestArrowMetadataRoundTrip integration tests (ITL-471)"
```

---

## Task 8: Final check and push

- [ ] **Step 1: Run the complete unit test suite**

```bash
uv run pytest tests/ -v --tb=short -q --ignore=tests/test_databases/test_spiraldb_connector_integration.py
```

Expected: all pass.

- [ ] **Step 2: Verify branch and push**

```bash
git log --oneline -8
git push -u origin eywalker/itl-471-spiraldbconnector-preserve-arrow-table-and-column-level
```

---

## Self-Review

**Spec coverage check:**

| Spec requirement | Task that covers it |
|---|---|
| `_serialize_arrow_metadata` helper | Task 2 |
| `_serialize_field_meta_tree` recursive helper | Task 2 |
| `_load_arrow_metadata` helper | Task 3 |
| `_restore_field` + `_restore_arrow_type` recursive helpers | Task 3 |
| `upsert_records` writes metadata after data write | Task 5 |
| `upsert_records` skips `set_metadata` when no metadata | Task 5 |
| `iter_batches` restores metadata per scan (once) | Task 6 |
| `iter_batches` backward compat (no stored metadata) | Task 6 |
| string/binary normalization preserved with metadata | Task 6 |
| `validate_records` on `DBConnectorProtocol` | Task 4 |
| `SQLiteConnector.validate_records` rejecting | Task 4 |
| `PostgreSQLConnector.validate_records` rejecting | Task 4 |
| `SpiralDBConnector.validate_records` no-op | Task 4 |
| `ConnectorArrowDatabase` guard replaced | Task 4 |
| Schema metadata integration test | Task 7 |
| Field metadata integration test | Task 7 |
| Extension type field metadata integration test | Task 7 |
| Nested struct field metadata integration test | Task 7 |
| Backward compatibility (no metadata) integration test | Task 7 |
| Full `ConnectorArrowDatabase` extension type round-trip | Task 7 |

All spec requirements covered. ✅

**Placeholder scan:** No TBDs, no "similar to Task N" shortcuts, all code blocks complete. ✅

**Type consistency:**
- `_serialize_field_meta_tree` → `dict | None` (Tasks 2, 3) ✅
- `_serialize_arrow_metadata` → `dict[str, bytes] | None` (Tasks 2, 5) ✅
- `_load_arrow_metadata` → `tuple[dict[bytes, bytes] | None, dict[str, dict]]` (Tasks 3, 6) ✅
- `_restore_field` → `tuple[pa.Field, bool]` (Tasks 3, 6) ✅
- `_ARROW_METADATA_KEY` used consistently as `"__arrow_metadata__"` (Tasks 2, 3, 6, tests) ✅
