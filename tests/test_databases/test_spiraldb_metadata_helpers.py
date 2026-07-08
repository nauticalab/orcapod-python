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

    def test_map_children_not_recursed(self):
        # pa.map_ key/item fields are not recursed — pa.map_() constructor
        # does not support field-level metadata on key/item fields.
        field = pa.field("m", pa.map_(pa.string(), pa.int32()))
        # No metadata on the outer field, no recursion into map children
        assert _serialize_field_meta_tree(field) is None

        # Even if the outer field has metadata, children are not in the result
        field_with_meta = pa.field("m", pa.map_(pa.string(), pa.int32()), metadata={b"tag": b"yes"})
        result = _serialize_field_meta_tree(field_with_meta)
        assert result == {"meta": {b64(b"tag"): b64(b"yes")}}
        assert "children" not in result


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


# ---------------------------------------------------------------------------
# _load_arrow_metadata
# ---------------------------------------------------------------------------


class TestLoadArrowMetadata:
    def test_empty_kv_returns_none_schema_and_empty_fields(self):
        schema_meta, field_trees = _load_arrow_metadata({})
        assert schema_meta is None
        assert field_trees == {}

    def test_key_absent_returns_none_schema_and_empty_fields(self):
        schema_meta, field_trees = _load_arrow_metadata({"other": b"val"})
        assert schema_meta is None
        assert field_trees == {}

    def test_schema_metadata_decoded(self):
        blob = {"schema": {b64(b"origin"): b64(b"test"), b64(b"key2"): b64(b"val2")}}
        kv = {_ARROW_METADATA_KEY: json.dumps(blob).encode("utf-8")}
        schema_meta, field_trees = _load_arrow_metadata(kv)
        assert schema_meta == {b"origin": b"test", b"key2": b"val2"}
        assert field_trees == {}

    def test_field_trees_returned_raw(self):
        raw_fields = {
            "col1": {"meta": {b64(b"unit"): b64(b"meters")}},
            "col2": {
                "children": {
                    "item": {"meta": {b64(b"k"): b64(b"v")}}
                }
            },
        }
        blob = {"fields": raw_fields}
        kv = {_ARROW_METADATA_KEY: json.dumps(blob).encode("utf-8")}
        schema_meta, field_trees = _load_arrow_metadata(kv)
        assert schema_meta is None
        assert field_trees == raw_fields

    def test_schema_and_fields_together(self):
        blob = {
            "schema": {b64(b"origin"): b64(b"test")},
            "fields": {
                "x": {"meta": {b64(b"unit"): b64(b"m")}}
            },
        }
        kv = {_ARROW_METADATA_KEY: json.dumps(blob).encode("utf-8")}
        schema_meta, field_trees = _load_arrow_metadata(kv)
        assert schema_meta == {b"origin": b"test"}
        assert field_trees == {"x": {"meta": {b64(b"unit"): b64(b"m")}}}

    def test_roundtrip_with_serialize(self):
        inner = pa.field("val", pa.float64(), metadata={b"desc": b"the value"})
        schema = pa.schema(
            [
                pa.field("x", pa.int64(), metadata={b"unit": b"meters"}),
                pa.field("s", pa.struct([inner])),
            ],
            metadata={b"origin": b"roundtrip_test"},
        )
        table = pa.table(
            {
                "x": pa.array([1, 2], type=pa.int64()),
                "s": pa.array(
                    [{"val": 1.0}, {"val": 2.0}],
                    type=pa.struct([pa.field("val", pa.float64())]),
                ),
            },
            schema=schema,
        )
        serialized = _serialize_arrow_metadata(table)
        assert serialized is not None
        schema_meta, field_trees = _load_arrow_metadata(serialized)
        assert schema_meta == {b"origin": b"roundtrip_test"}
        assert "x" in field_trees
        assert field_trees["x"]["meta"] == {b64(b"unit"): b64(b"meters")}
        assert "s" in field_trees
        assert "children" in field_trees["s"]
        assert "val" in field_trees["s"]["children"]


# ---------------------------------------------------------------------------
# _restore_field
# ---------------------------------------------------------------------------


class TestRestoreField:
    def test_none_stored_returns_unchanged(self):
        field = pa.field("x", pa.int64())
        restored, changed = _restore_field(field, None)
        assert restored is field
        assert changed is False

    def test_empty_dict_stored_returns_unchanged(self):
        field = pa.field("x", pa.int64())
        restored, changed = _restore_field(field, {})
        assert restored is field
        assert changed is False

    def test_primitive_with_meta_restored(self):
        field = pa.field("x", pa.int64())
        stored = {"meta": {b64(b"unit"): b64(b"meters")}}
        restored, changed = _restore_field(field, stored)
        assert changed is True
        assert restored.metadata == {b"unit": b"meters"}
        assert restored.name == "x"
        assert restored.type == pa.int64()

    def test_primitive_already_has_different_meta_overwritten(self):
        field = pa.field("x", pa.int64(), metadata={b"old": b"meta"})
        stored = {"meta": {b64(b"new"): b64(b"meta2")}}
        restored, changed = _restore_field(field, stored)
        assert changed is True
        assert restored.metadata == {b"new": b"meta2"}

    def test_struct_child_meta_restored(self):
        inner = pa.field("val", pa.float64())
        field = pa.field("s", pa.struct([inner]))
        stored = {
            "children": {
                "val": {"meta": {b64(b"desc"): b64(b"the value")}}
            }
        }
        restored, changed = _restore_field(field, stored)
        assert changed is True
        assert pa.types.is_struct(restored.type)
        restored_inner = restored.type.field("val")
        assert restored_inner.metadata == {b"desc": b"the value"}

    def test_struct_no_child_meta_unchanged(self):
        inner = pa.field("val", pa.float64())
        field = pa.field("s", pa.struct([inner]))
        stored = {}
        restored, changed = _restore_field(field, stored)
        assert restored is field
        assert changed is False

    def test_list_value_field_meta_restored(self):
        vf = pa.field("item", pa.int32())
        field = pa.field("lst", pa.list_(vf))
        stored = {
            "children": {
                "item": {"meta": {b64(b"k"): b64(b"v")}}
            }
        }
        restored, changed = _restore_field(field, stored)
        assert changed is True
        assert pa.types.is_list(restored.type)
        restored_vf = restored.type.value_field
        assert restored_vf.metadata == {b"k": b"v"}

    def test_large_list_value_field_meta_restored(self):
        vf = pa.field("item", pa.int32())
        field = pa.field("lst", pa.large_list(vf))
        stored = {
            "children": {
                "item": {"meta": {b64(b"k"): b64(b"v")}}
            }
        }
        restored, changed = _restore_field(field, stored)
        assert changed is True
        assert pa.types.is_large_list(restored.type)
        restored_vf = restored.type.value_field
        assert restored_vf.metadata == {b"k": b"v"}

    def test_nested_struct_deep_meta_restored(self):
        leaf = pa.field("z", pa.int8())
        inner_struct_field = pa.field("inner", pa.struct([leaf]))
        outer_field = pa.field("outer", pa.struct([inner_struct_field]))
        stored = {
            "children": {
                "inner": {
                    "children": {
                        "z": {"meta": {b64(b"deep"): b64(b"yes")}}
                    }
                }
            }
        }
        restored, changed = _restore_field(outer_field, stored)
        assert changed is True
        assert pa.types.is_struct(restored.type)
        inner_restored = restored.type.field("inner")
        assert pa.types.is_struct(inner_restored.type)
        leaf_restored = inner_restored.type.field("z")
        assert leaf_restored.metadata == {b"deep": b"yes"}

    def test_changed_false_when_nothing_to_restore(self):
        field = pa.field("x", pa.int64())
        _, changed = _restore_field(field, None)
        assert changed is False

    def test_changed_true_when_meta_restored(self):
        field = pa.field("x", pa.int64())
        stored = {"meta": {b64(b"unit"): b64(b"meters")}}
        _, changed = _restore_field(field, stored)
        assert changed is True
