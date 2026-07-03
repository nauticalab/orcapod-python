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

from orcapod.hashing.schema_cleaner import clean_schema_for_hashing, has_extension_metadata

_EXT_NAME = b"ARROW:extension:name"
_EXT_META = b"ARROW:extension:metadata"
_COMMENT = b"comment"
_VENDOR = b"vendor:tag"


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

    def test_clean_sparse_union_type(self):
        """_clean_type handles a sparse union arrow type (line 85 branch)."""
        import pyarrow as pa
        from orcapod.hashing.schema_cleaner import _clean_type

        # Build a sparse union: int32 | utf8
        fields = [pa.field("a", pa.int32()), pa.field("b", pa.utf8())]
        sparse = pa.sparse_union(fields)
        result = _clean_type(sparse)
        assert pa.types.is_union(result)
        assert result.mode == "sparse"

    def test_clean_dense_union_type(self):
        """_clean_type handles a dense union arrow type (line 86 branch)."""
        import pyarrow as pa
        from orcapod.hashing.schema_cleaner import _clean_type

        fields = [pa.field("a", pa.int32()), pa.field("b", pa.utf8())]
        dense = pa.dense_union(fields)
        result = _clean_type(dense)
        assert pa.types.is_union(result)
        assert result.mode == "dense"

    def test_dictionary_value_type_cleaned(self):
        """dictionary value_type child metadata is filtered; index_type preserved."""
        # Build a dictionary whose value type is a struct with mixed metadata on a child
        value_struct = pa.struct([
            pa.field("label", pa.utf8(), metadata=_mixed_meta("label.t")),
        ])
        schema = pa.schema([
            pa.field("d", pa.dictionary(pa.int8(), value_struct)),
        ])
        cleaned = clean_schema_for_hashing(schema)
        cleaned_type = cleaned.field("d").type
        assert pa.types.is_dictionary(cleaned_type)
        # index_type must be unchanged
        assert cleaned_type.index_type == pa.int8()
        # value_type child field: only extension keys survive
        label_field = cleaned_type.value_type.field(0)
        assert label_field.metadata == {_EXT_NAME: b"label.t"}

    def test_union_child_field_cleaned(self):
        """sparse_union child field metadata is filtered; type_codes and field names preserved."""
        schema = pa.schema([
            pa.field("u", pa.sparse_union([
                pa.field("a", pa.int32(), metadata=_mixed_meta("a.t")),
                pa.field("b", pa.utf8(), metadata={_COMMENT: b"drop"}),
            ])),
        ])
        cleaned = clean_schema_for_hashing(schema)
        cleaned_type = cleaned.field("u").type
        assert pa.types.is_union(cleaned_type)
        assert cleaned_type.mode == "sparse"
        assert list(cleaned_type.type_codes) == [0, 1]
        assert cleaned_type.field(0).name == "a"
        assert cleaned_type.field(0).metadata == {_EXT_NAME: b"a.t"}
        assert cleaned_type.field(1).name == "b"
        assert cleaned_type.field(1).metadata in (None, {})


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


# ---------------------------------------------------------------------------
# TestHasExtensionMetadata
# ---------------------------------------------------------------------------

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

    def test_true_for_extension_in_schema_level_metadata(self):
        schema = pa.schema(
            [pa.field("x", pa.int32())],
            metadata={_EXT_NAME: b"schema.ext"},
        )
        assert has_extension_metadata(schema) is True

    def test_true_for_extension_in_large_list_value_field(self):
        schema = pa.schema([
            pa.field("lst", pa.large_list(
                pa.field("item", pa.int32(), metadata={_EXT_NAME: b"item.t"}),
            )),
        ])
        assert has_extension_metadata(schema) is True

    def test_true_for_extension_in_dictionary_value_field(self):
        """Extension metadata on a dictionary value_type field is detected."""
        value_struct = pa.struct([
            pa.field("label", pa.utf8(), metadata={_EXT_NAME: b"label.t"}),
        ])
        schema = pa.schema([
            pa.field("d", pa.dictionary(pa.int8(), value_struct)),
        ])
        assert has_extension_metadata(schema) is True

    def test_true_for_extension_in_union_child_field(self):
        """Extension metadata on a union child field is detected."""
        schema = pa.schema([
            pa.field("u", pa.sparse_union([
                pa.field("a", pa.int32()),
                pa.field("b", pa.utf8(), metadata={_EXT_NAME: b"b.t"}),
            ])),
        ])
        assert has_extension_metadata(schema) is True
