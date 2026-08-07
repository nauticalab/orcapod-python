"""Tests for arrow_utils utility functions."""

import pyarrow as pa
import pytest

from orcapod.types import ColumnConfig
from orcapod.utils.arrow_utils import (
    add_source_info,
    apply_column_config,
    infer_schema_nullable,
    make_schema_non_nullable,
    normalize_extension_columns,
    normalize_table_view_types,
    normalize_view_types,
    prepare_prefixed_columns,
)


# ---------------------------------------------------------------------------
# Minimal extension types for normalize_extension_columns tests.
# These are self-contained and do not depend on the orcapod type-converter.
# ---------------------------------------------------------------------------


class _TestIntExt(pa.ExtensionType):
    """Extension type wrapping int32 storage, used in normalize tests."""

    def __init__(self):
        super().__init__(pa.int32(), "orcapod.test.int_ext")

    def __arrow_ext_serialize__(self):
        return b'{"category":"test_int"}'

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return cls()


class _TestBinaryExt(pa.ExtensionType):
    """Extension type wrapping large_binary storage, used in normalize tests."""

    def __init__(self):
        super().__init__(pa.large_binary(), "orcapod.test.binary_ext")

    def __arrow_ext_serialize__(self):
        return b'{"category":"test_binary"}'

    @classmethod
    def __arrow_ext_deserialize__(cls, storage_type, serialized):
        return cls()


# Register once at module import time; guard against re-registration when the
# test module is reloaded in the same process (e.g. under pytest-xdist or
# repeated runs inside an interactive session).
for _ext_instance in (_TestIntExt(), _TestBinaryExt()):
    try:
        pa.register_extension_type(_ext_instance)
    except KeyError:
        pass  # already registered; existing registration is still valid

_INT_EXT = _TestIntExt()
_BINARY_EXT = _TestBinaryExt()


class TestPreparePrefixedColumnsPreservesNullable:
    """prepare_prefixed_columns must preserve nullable flags from the source table."""

    def test_non_nullable_fields_preserved_in_data_table(self):
        """Fields with nullable=False in source table must remain nullable=False in data_table."""
        schema = pa.schema(
            [
                pa.field("tag", pa.large_string(), nullable=False),
                pa.field("val", pa.int64(), nullable=False),
            ]
        )
        table = pa.table(
            {
                "tag": pa.array(["a"], type=pa.large_string()),
                "val": pa.array([1], type=pa.int64()),
            },
            schema=schema,
        )
        # Confirm source has nullable=False
        assert table.schema.field("val").nullable is False

        data_table, _ = prepare_prefixed_columns(table, [])

        # data_table must preserve nullable=False
        assert data_table.schema.field("val").nullable is False

    def test_nullable_fields_preserved_in_data_table(self):
        """Fields with nullable=True in source table must remain nullable=True in data_table."""
        table = pa.table(
            {
                "tag": pa.array(["a"], type=pa.large_string()),
                "val": pa.array([1], type=pa.int64()),
            }
        )
        # Arrow defaults to nullable=True
        assert table.schema.field("val").nullable is True

        data_table, _ = prepare_prefixed_columns(table, [])

        # data_table must preserve nullable=True
        assert data_table.schema.field("val").nullable is True

    def test_mixed_nullable_fields_preserved_in_data_table(self):
        """Mix of nullable and non-nullable fields must be preserved correctly."""
        schema = pa.schema(
            [
                pa.field("tag", pa.large_string(), nullable=True),
                pa.field("val_nullable", pa.int64(), nullable=True),
                pa.field("val_non_nullable", pa.float64(), nullable=False),
            ]
        )
        table = pa.table(
            {
                "tag": pa.array(["a"], type=pa.large_string()),
                "val_nullable": pa.array([1], type=pa.int64()),
                "val_non_nullable": pa.array([1.5], type=pa.float64()),
            },
            schema=schema,
        )

        data_table, _ = prepare_prefixed_columns(table, [])

        assert data_table.schema.field("val_nullable").nullable is True
        assert data_table.schema.field("val_non_nullable").nullable is False


# ---------------------------------------------------------------------------
# make_schema_non_nullable
# ---------------------------------------------------------------------------


class TestMakeSchemaNotNullable:
    """make_schema_non_nullable must set nullable=False on every field while
    preserving both field-level and schema-level metadata."""

    def _schema_with_metadata(self) -> "pa.Schema":
        return pa.schema(
            [
                pa.field("id", pa.int64(), nullable=True, metadata={b"field_key": b"field_val"}),
                pa.field("name", pa.large_string(), nullable=True),
            ],
            metadata={b"schema_key": b"schema_val"},
        )

    def test_sets_nullable_false_on_all_fields(self):
        schema = self._schema_with_metadata()
        result = make_schema_non_nullable(schema)
        assert result.field("id").nullable is False
        assert result.field("name").nullable is False

    def test_preserves_field_level_metadata(self):
        """Field metadata must survive the nullable=False conversion."""
        schema = self._schema_with_metadata()
        result = make_schema_non_nullable(schema)
        assert result.field("id").metadata == {b"field_key": b"field_val"}

    def test_preserves_schema_level_metadata(self):
        """Schema-level metadata (e.g. pandas/extension metadata) must be kept."""
        schema = self._schema_with_metadata()
        result = make_schema_non_nullable(schema)
        assert result.metadata == {b"schema_key": b"schema_val"}

    def test_none_schema_metadata_stays_none(self):
        """When the source schema has no metadata, result metadata is None/empty."""
        schema = pa.schema([pa.field("x", pa.int64())])
        result = make_schema_non_nullable(schema)
        # pa.schema with metadata=None produces None or {}; either is acceptable
        assert not result.metadata


# ---------------------------------------------------------------------------
# infer_schema_nullable
# ---------------------------------------------------------------------------


class TestInferSchemaNullable:
    """infer_schema_nullable must derive nullable flags from actual null counts
    while preserving both field-level and schema-level metadata."""

    def _table_with_metadata(self) -> "pa.Table":
        schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=True, metadata={b"field_key": b"fv"}),
                pa.field("val", pa.int64(), nullable=True),
                pa.field("opt", pa.int64(), nullable=True),
            ],
            metadata={b"schema_key": b"sv"},
        )
        return pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),      # no nulls → nullable=False
                "val": pa.array([10, 20], type=pa.int64()),   # no nulls → nullable=False
                "opt": pa.array([None, 1], type=pa.int64()),  # has nulls → nullable=True
            },
            schema=schema,
        )

    def test_nullable_false_for_null_free_columns(self):
        table = self._table_with_metadata()
        result = infer_schema_nullable(table)
        assert result.field("id").nullable is False
        assert result.field("val").nullable is False

    def test_nullable_true_for_columns_with_nulls(self):
        table = self._table_with_metadata()
        result = infer_schema_nullable(table)
        assert result.field("opt").nullable is True

    def test_preserves_field_level_metadata(self):
        """Field metadata must survive nullable inference."""
        table = self._table_with_metadata()
        result = infer_schema_nullable(table)
        assert result.field("id").metadata == {b"field_key": b"fv"}

    def test_preserves_schema_level_metadata(self):
        """Schema-level metadata must be carried through to the inferred schema."""
        table = self._table_with_metadata()
        result = infer_schema_nullable(table)
        assert result.metadata == {b"schema_key": b"sv"}

    def test_none_schema_metadata_stays_none(self):
        """When the source table schema has no metadata, result metadata is None/empty."""
        table = pa.table({"x": pa.array([1, 2], type=pa.int64())})
        result = infer_schema_nullable(table)
        assert not result.metadata


# ---------------------------------------------------------------------------
# add_source_info
# ---------------------------------------------------------------------------


class TestAddSourceInfo:
    """add_source_info must produce one _source_<col> column per data column
    whose values are exactly '<source_token>::<col>' for every row."""

    def test_single_column_produces_correct_source_token(self):
        """_source_<col> value is '<src>::<col>' for every row."""
        table = pa.table({"x": pa.array([10, 20], type=pa.int64())})
        result = add_source_info(table, "mysrc")

        assert result.column("_source_x").to_pylist() == ["mysrc::x", "mysrc::x"]

    def test_multi_column_each_source_column_uses_its_own_name(self):
        """Each _source_<col> value is '<src>::<col>' — columns are independent."""
        table = pa.table({
            "x": pa.array([1, 2], type=pa.int64()),
            "y": pa.array([3, 4], type=pa.int64()),
            "z": pa.array([5, 6], type=pa.int64()),
        })
        result = add_source_info(table, "base")

        assert result.column("_source_x").to_pylist() == ["base::x", "base::x"]
        assert result.column("_source_y").to_pylist() == ["base::y", "base::y"]
        assert result.column("_source_z").to_pylist() == ["base::z", "base::z"]

    def test_per_row_source_tokens_combined_with_column_name(self):
        """With a per-row source list, each row's token is '<row_src>::<col>'."""
        table = pa.table({
            "a": pa.array([10, 20], type=pa.int64()),
            "b": pa.array([30, 40], type=pa.int64()),
        })
        result = add_source_info(table, ["src0", "src1"])

        assert result.column("_source_a").to_pylist() == ["src0::a", "src1::a"]
        assert result.column("_source_b").to_pylist() == ["src0::b", "src1::b"]

    def test_correct_number_of_source_columns_added(self):
        """Exactly one _source_<col> column is added per non-excluded data column."""
        table = pa.table({
            "p": pa.array([1], type=pa.int64()),
            "q": pa.array([2], type=pa.int64()),
            "r": pa.array([3], type=pa.int64()),
        })
        result = add_source_info(table, "s")

        assert result.num_columns == 6  # 3 data + 3 source
        assert "_source_p" in result.column_names
        assert "_source_q" in result.column_names
        assert "_source_r" in result.column_names


class TestSystemTagColumnNames:
    def test_returns_two_strings(self):
        from orcapod.utils.arrow_utils import system_tag_column_names

        src_col, rec_col = system_tag_column_names("abc123")
        assert isinstance(src_col, str)
        assert isinstance(rec_col, str)

    def test_source_id_col_starts_with_system_tag_prefix(self):
        from orcapod.system_constants import constants
        from orcapod.utils.arrow_utils import system_tag_column_names

        src_col, _ = system_tag_column_names("abc123")
        assert src_col.startswith(constants.SYSTEM_TAG_PREFIX)

    def test_record_id_col_starts_with_system_tag_prefix(self):
        from orcapod.system_constants import constants
        from orcapod.utils.arrow_utils import system_tag_column_names

        _, rec_col = system_tag_column_names("abc123")
        assert rec_col.startswith(constants.SYSTEM_TAG_PREFIX)

    def test_schema_hash_embedded_in_col_names(self):
        from orcapod.utils.arrow_utils import system_tag_column_names

        schema_hash = "deadbeef"
        src_col, rec_col = system_tag_column_names(schema_hash)
        assert schema_hash in src_col
        assert schema_hash in rec_col

    def test_different_hashes_produce_different_names(self):
        from orcapod.utils.arrow_utils import system_tag_column_names

        src1, rec1 = system_tag_column_names("aaa")
        src2, rec2 = system_tag_column_names("bbb")
        assert src1 != src2
        assert rec1 != rec2

    def test_col_names_match_add_system_tag_columns_output(self):
        """Column names from system_tag_column_names() must match those
        added to a table by add_system_tag_columns()."""
        import uuid

        import pyarrow as pa
        from orcapod.utils.arrow_utils import add_system_tag_columns, system_tag_column_names

        schema_hash = "testhash"
        table = pa.table({"id": pa.array([1]), "v": pa.array([1.0])})
        enriched = add_system_tag_columns(
            table, schema_hash, ["src_a"], [uuid.uuid4().bytes]
        )
        src_col, rec_col = system_tag_column_names(schema_hash)
        assert src_col in enriched.column_names
        assert rec_col in enriched.column_names


# ---------------------------------------------------------------------------
# apply_column_config
# ---------------------------------------------------------------------------


class TestApplyColumnConfigMetaNormalization:
    """apply_column_config must normalise unprefixed meta names by prepending '__'.

    ColumnConfig documents: "prefix '__' is added automatically if not present".
    Without normalisation, ``ColumnConfig(meta=["pipeline"])`` would DROP
    ``__pipeline*`` columns because ``"__pipeline_hash".startswith("pipeline")``
    is False.
    """

    def _make_table(self) -> pa.Table:
        return pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "result": pa.array([42], type=pa.int64()),
                "__pipeline_hash": pa.array(["abc"], type=pa.large_string()),
                "__data_id": pa.array(["xyz"], type=pa.large_string()),
            }
        )

    def test_unprefixed_meta_name_keeps_matching_meta_column(self):
        """meta=['pipeline'] (no leading '__') keeps __pipeline* columns."""
        table = self._make_table()
        config = ColumnConfig(meta=["pipeline"])
        result = apply_column_config(table, config, tag_keys=("id",))
        assert "__pipeline_hash" in result.column_names

    def test_unprefixed_meta_name_drops_non_matching_meta_column(self):
        """meta=['pipeline'] drops __data_id because it doesn't match 'pipeline'."""
        table = self._make_table()
        config = ColumnConfig(meta=["pipeline"])
        result = apply_column_config(table, config, tag_keys=("id",))
        assert "__data_id" not in result.column_names

    def test_prefixed_meta_name_also_works(self):
        """meta=['__pipeline'] (already prefixed) behaves identically."""
        table = self._make_table()
        config = ColumnConfig(meta=["__pipeline"])
        result = apply_column_config(table, config, tag_keys=("id",))
        assert "__pipeline_hash" in result.column_names
        assert "__data_id" not in result.column_names

    def test_meta_true_keeps_all_meta_columns(self):
        """meta=True keeps every __* column."""
        table = self._make_table()
        config = ColumnConfig(meta=True)
        result = apply_column_config(table, config, tag_keys=("id",))
        assert "__pipeline_hash" in result.column_names
        assert "__data_id" in result.column_names

    def test_meta_false_drops_all_meta_columns(self):
        """meta=False (default) drops every __* column."""
        table = self._make_table()
        config = ColumnConfig(meta=False)
        result = apply_column_config(table, config, tag_keys=("id",))
        assert "__pipeline_hash" not in result.column_names
        assert "__data_id" not in result.column_names


class TestNormalizeViewTypes:
    """View-type normalization (ENG-601): string_view/binary_view -> large variants."""

    def test_string_view_to_large_string(self):
        assert normalize_view_types(pa.string_view()) == pa.large_string()

    def test_binary_view_to_large_binary(self):
        assert normalize_view_types(pa.binary_view()) == pa.large_binary()

    def test_non_view_types_unchanged(self):
        for t in (pa.int64(), pa.string(), pa.large_string(), pa.binary(), pa.bool_()):
            assert normalize_view_types(t) == t

    def test_list_of_string_view(self):
        assert normalize_view_types(pa.list_(pa.string_view())) == pa.list_(
            pa.large_string()
        )

    def test_large_list_of_string_view(self):
        assert normalize_view_types(pa.large_list(pa.string_view())) == pa.large_list(
            pa.large_string()
        )

    def test_fixed_size_list_of_string_view(self):
        assert normalize_view_types(
            pa.list_(pa.string_view(), 3)
        ) == pa.list_(pa.large_string(), 3)

    def test_struct_with_string_view_field(self):
        src = pa.struct([pa.field("a", pa.string_view()), pa.field("b", pa.int64())])
        expected = pa.struct(
            [pa.field("a", pa.large_string()), pa.field("b", pa.int64())]
        )
        assert normalize_view_types(src) == expected

    def test_map_with_string_view(self):
        assert normalize_view_types(
            pa.map_(pa.string_view(), pa.string_view())
        ) == pa.map_(pa.large_string(), pa.large_string())

    def test_nested_list_of_struct_with_view(self):
        src = pa.list_(pa.struct([pa.field("a", pa.string_view())]))
        expected = pa.list_(pa.struct([pa.field("a", pa.large_string())]))
        assert normalize_view_types(src) == expected

    def test_list_preserves_value_field_attributes(self):
        src = pa.list_(
            pa.field("elem", pa.string_view(), nullable=False, metadata={b"k": b"v"})
        )
        result = normalize_view_types(src)
        vf = result.value_field
        assert vf.type == pa.large_string()
        assert vf.name == "elem"
        assert vf.nullable is False
        assert vf.metadata == {b"k": b"v"}

    def test_map_preserves_field_attributes_and_keys_sorted(self):
        src = pa.map_(
            pa.string_view(),
            pa.field("value", pa.string_view(), nullable=False),
            keys_sorted=True,
        )
        result = normalize_view_types(src)
        assert result.key_field.type == pa.large_string()
        assert result.item_field.type == pa.large_string()
        assert result.item_field.nullable is False
        assert result.keys_sorted is True


class TestNormalizeTableViewTypes:
    """Table-level view normalization (ENG-601)."""

    def test_casts_string_view_column(self):
        tbl = pa.table(
            [pa.array(["x", "y"], type=pa.string_view()), pa.array([1, 2])],
            names=["s", "n"],
        )
        result = normalize_table_view_types(tbl)
        assert result.schema.field("s").type == pa.large_string()
        assert result.schema.field("n").type == pa.int64()
        assert result.column("s").to_pylist() == ["x", "y"]

    def test_no_view_types_returns_same_table(self):
        tbl = pa.table({"s": ["x"], "n": [1]})  # string -> large_string by default? plain string
        result = normalize_table_view_types(tbl)
        # No view types present -> object returned unchanged (identity).
        assert result is tbl

    def test_preserves_field_nullability(self):
        schema = pa.schema([pa.field("s", pa.string_view(), nullable=False)])
        tbl = pa.table([pa.array(["x"], type=pa.string_view())], schema=schema)
        result = normalize_table_view_types(tbl)
        assert result.schema.field("s").type == pa.large_string()
        assert result.schema.field("s").nullable is False


# ---------------------------------------------------------------------------
# normalize_extension_columns
# ---------------------------------------------------------------------------


class TestNormalizeExtensionColumns:
    """normalize_extension_columns: pa.ExtensionType columns → IPC storage form.

    Covers:
    * fast-path identity return when no extension columns are present
    * storage type substitution for extension columns
    * correct ARROW:extension:name and ARROW:extension:metadata field metadata
    * data-value preservation
    * non-extension column passthrough in mixed tables
    * column count stability
    * schema-level metadata preservation
    * per-field metadata preservation alongside the new ARROW:extension:* keys
    * nullable flag preservation (both True and False)
    * multi-chunk column handling: data correctness and chunk-count preservation
    * multiple extension columns of different types in the same table
    """

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _int_ext_col(self, values: list[int]) -> pa.ChunkedArray:
        """Build a single-chunk ChunkedArray using _INT_EXT storage."""
        storage = pa.array(values, type=pa.int32())
        arr = pa.ExtensionArray.from_storage(_INT_EXT, storage)
        return pa.chunked_array([arr])

    def _binary_ext_col(self, values: list[bytes]) -> pa.ChunkedArray:
        """Build a single-chunk ChunkedArray using _BINARY_EXT storage."""
        storage = pa.array(values, type=pa.large_binary())
        arr = pa.ExtensionArray.from_storage(_BINARY_EXT, storage)
        return pa.chunked_array([arr])

    # ------------------------------------------------------------------
    # Fast-path: no extension columns
    # ------------------------------------------------------------------

    def test_no_extension_columns_returns_same_object(self):
        """Table with no extension columns is returned as the exact same object."""
        table = pa.table({"x": pa.array([1, 2], pa.int64()), "y": pa.array([3.0, 4.0])})
        result = normalize_extension_columns(table)
        assert result is table

    # ------------------------------------------------------------------
    # Type conversion
    # ------------------------------------------------------------------

    def test_extension_column_type_becomes_storage_type(self):
        """Normalized field has the extension type's storage type, not the extension type."""
        table = pa.Table.from_arrays(
            [self._int_ext_col([10, 20])],
            schema=pa.schema([pa.field("v", _INT_EXT)]),
        )
        result = normalize_extension_columns(table)
        assert result.schema.field("v").type == pa.int32()
        assert not isinstance(result.schema.field("v").type, pa.ExtensionType)

    # ------------------------------------------------------------------
    # Field metadata — extension identity
    # ------------------------------------------------------------------

    def test_extension_name_written_to_field_metadata(self):
        """ARROW:extension:name equals the extension type's registered name."""
        table = pa.Table.from_arrays(
            [self._int_ext_col([1])],
            schema=pa.schema([pa.field("v", _INT_EXT)]),
        )
        result = normalize_extension_columns(table)
        meta = result.schema.field("v").metadata
        assert b"ARROW:extension:name" in meta
        assert meta[b"ARROW:extension:name"] == b"orcapod.test.int_ext"

    def test_extension_metadata_matches_arrow_ext_serialize(self):
        """ARROW:extension:metadata equals the output of __arrow_ext_serialize__."""
        table = pa.Table.from_arrays(
            [self._int_ext_col([1])],
            schema=pa.schema([pa.field("v", _INT_EXT)]),
        )
        result = normalize_extension_columns(table)
        meta = result.schema.field("v").metadata
        assert b"ARROW:extension:metadata" in meta
        assert meta[b"ARROW:extension:metadata"] == b'{"category":"test_int"}'

    # ------------------------------------------------------------------
    # Data preservation
    # ------------------------------------------------------------------

    def test_data_values_preserved(self):
        """Storage values in the normalized column match the original extension values."""
        table = pa.Table.from_arrays(
            [self._int_ext_col([7, 8, 9])],
            schema=pa.schema([pa.field("v", _INT_EXT)]),
        )
        result = normalize_extension_columns(table)
        assert result.column("v").to_pylist() == [7, 8, 9]

    # ------------------------------------------------------------------
    # Non-extension columns (mixed table)
    # ------------------------------------------------------------------

    def test_non_extension_columns_pass_through_unchanged(self):
        """Plain columns in a mixed table keep their type and values unchanged."""
        schema = pa.schema([pa.field("ext", _INT_EXT), pa.field("plain", pa.int64())])
        table = pa.Table.from_arrays(
            [self._int_ext_col([1, 2]), pa.array([100, 200], type=pa.int64())],
            schema=schema,
        )
        result = normalize_extension_columns(table)
        assert result.schema.field("plain").type == pa.int64()
        assert result.column("plain").to_pylist() == [100, 200]

    def test_column_count_unchanged(self):
        """The result table has the same number of columns as the input."""
        schema = pa.schema([pa.field("e", _INT_EXT), pa.field("p", pa.int64())])
        table = pa.Table.from_arrays(
            [self._int_ext_col([1]), pa.array([99], type=pa.int64())],
            schema=schema,
        )
        result = normalize_extension_columns(table)
        assert result.num_columns == 2

    # ------------------------------------------------------------------
    # Metadata preservation
    # ------------------------------------------------------------------

    def test_schema_level_metadata_preserved(self):
        """Schema-level metadata is carried through to the result unchanged."""
        schema = pa.schema(
            [pa.field("v", _INT_EXT)],
            metadata={b"schema_key": b"schema_val"},
        )
        table = pa.Table.from_arrays([self._int_ext_col([1])], schema=schema)
        result = normalize_extension_columns(table)
        assert result.schema.metadata[b"schema_key"] == b"schema_val"

    def test_existing_field_metadata_preserved(self):
        """Pre-existing per-field metadata survives alongside the new ARROW:extension:* keys."""
        field = pa.field("v", _INT_EXT, metadata={b"custom_key": b"custom_val"})
        table = pa.Table.from_arrays(
            [self._int_ext_col([1])], schema=pa.schema([field])
        )
        result = normalize_extension_columns(table)
        meta = result.schema.field("v").metadata
        # Pre-existing key preserved
        assert meta[b"custom_key"] == b"custom_val"
        # Extension identity also added
        assert b"ARROW:extension:name" in meta

    # ------------------------------------------------------------------
    # Field attributes
    # ------------------------------------------------------------------

    def test_nullable_false_preserved(self):
        """Extension column with nullable=False keeps nullable=False after normalization."""
        field = pa.field("v", _INT_EXT, nullable=False)
        table = pa.Table.from_arrays(
            [self._int_ext_col([1, 2])], schema=pa.schema([field])
        )
        result = normalize_extension_columns(table)
        assert result.schema.field("v").nullable is False

    def test_nullable_true_preserved(self):
        """Extension column with nullable=True keeps nullable=True after normalization."""
        field = pa.field("v", _INT_EXT, nullable=True)
        table = pa.Table.from_arrays(
            [self._int_ext_col([1, 2])], schema=pa.schema([field])
        )
        result = normalize_extension_columns(table)
        assert result.schema.field("v").nullable is True

    # ------------------------------------------------------------------
    # Multi-chunk columns (zero-copy guarantee)
    # ------------------------------------------------------------------

    def _multi_chunk_int_ext_col(self) -> pa.ChunkedArray:
        """Two-chunk ChunkedArray of _INT_EXT values [1, 2] | [3, 4]."""
        arr1 = pa.ExtensionArray.from_storage(_INT_EXT, pa.array([1, 2], pa.int32()))
        arr2 = pa.ExtensionArray.from_storage(_INT_EXT, pa.array([3, 4], pa.int32()))
        return pa.chunked_array([arr1, arr2])

    def test_multi_chunk_data_values_preserved(self):
        """Multi-chunk extension column: all data values are correct after normalization."""
        table = pa.Table.from_arrays(
            [self._multi_chunk_int_ext_col()],
            schema=pa.schema([pa.field("v", _INT_EXT)]),
        )
        result = normalize_extension_columns(table)
        assert result.column("v").to_pylist() == [1, 2, 3, 4]

    def test_multi_chunk_column_chunk_count_preserved(self):
        """Multi-chunk extension column: chunk count is preserved (no combine_chunks copy)."""
        table = pa.Table.from_arrays(
            [self._multi_chunk_int_ext_col()],
            schema=pa.schema([pa.field("v", _INT_EXT)]),
        )
        result = normalize_extension_columns(table)
        # Original has 2 chunks; normalization must not collapse them into 1.
        assert result.column("v").num_chunks == 2

    # ------------------------------------------------------------------
    # Multiple extension columns
    # ------------------------------------------------------------------

    def test_multiple_extension_columns_all_normalized(self):
        """All extension-typed columns in the same table are independently normalized."""
        schema = pa.schema([pa.field("i", _INT_EXT), pa.field("b", _BINARY_EXT)])
        table = pa.Table.from_arrays(
            [self._int_ext_col([1, 2]), self._binary_ext_col([b"x", b"y"])],
            schema=schema,
        )
        result = normalize_extension_columns(table)
        # Both columns have storage types
        assert result.schema.field("i").type == pa.int32()
        assert result.schema.field("b").type == pa.large_binary()
        # Both carry correct extension names
        assert (
            result.schema.field("i").metadata[b"ARROW:extension:name"]
            == b"orcapod.test.int_ext"
        )
        assert (
            result.schema.field("b").metadata[b"ARROW:extension:name"]
            == b"orcapod.test.binary_ext"
        )
        # Data values correct for both
        assert result.column("i").to_pylist() == [1, 2]
        assert result.column("b").to_pylist() == [b"x", b"y"]


# ---------------------------------------------------------------------------
# make_empty_table — ITL-563
# ---------------------------------------------------------------------------


class TestMakeEmptyTable:
    """make_empty_table preserves field nullability from python_schema."""

    def _converter(self):
        from orcapod.contexts import get_default_type_converter
        return get_default_type_converter()

    def test_required_fields_are_non_nullable(self):
        """Plain types produce nullable=False Arrow fields."""
        from orcapod.utils.arrow_utils import make_empty_table

        schema = {"name": str, "count": int}
        table = make_empty_table(schema, self._converter())

        assert table.num_rows == 0
        assert table.schema.field("name").nullable is False
        assert table.schema.field("count").nullable is False

    def test_optional_fields_are_nullable(self):
        """Optional types produce nullable=True Arrow fields."""
        from orcapod.utils.arrow_utils import make_empty_table

        schema = {"name": str | None, "count": int | None}
        table = make_empty_table(schema, self._converter())

        assert table.num_rows == 0
        assert table.schema.field("name").nullable is True
        assert table.schema.field("count").nullable is True

    def test_mixed_nullability_per_field(self):
        """Mixed schema: required field non-nullable, optional field nullable."""
        from orcapod.utils.arrow_utils import make_empty_table

        schema = {"subject": str, "score": int | None}
        table = make_empty_table(schema, self._converter())

        assert table.num_rows == 0
        assert table.schema.field("subject").nullable is False
        assert table.schema.field("score").nullable is True

    def test_round_trips_through_arrow_table_stream(self):
        """Python schema → make_empty_table → ArrowTableStream.output_schema() is identity."""
        from orcapod.utils.arrow_utils import make_empty_table
        from orcapod.core.streams import ArrowTableStream

        converter = self._converter()
        python_schema = {"subject": str, "score": int | None}
        table = make_empty_table(python_schema, converter)

        # ArrowTableStream requires at least one data column; tag=subject, data=score
        stream = ArrowTableStream(table, tag_columns=["subject"])
        _, data_schema = stream.output_schema()

        assert data_schema["score"] == (int | None)
# fold_system_tag_values
# ---------------------------------------------------------------------------


class TestFoldSystemTagValues:
    """Folding N members' system-tag values into one scalar (NPIPE-204).

    The expected digests below are hard-coded on purpose.  A fold that used
    hash() or set-iteration order would still be self-consistent within one
    process; pinning the values is what catches it.
    """

    SOURCE_COL = "_tag_source_id::abc123"
    RECORD_COL = "_tag_record_id::abc123"

    RIDS = [
        bytes.fromhex("0102030405060708090a0b0c0d0e0f10"),
        bytes.fromhex("1112131415161718191a1b1c1d1e1f20"),
    ]
    EXPECTED_RID = bytes.fromhex("853be16a3f38565f8ced039f84fdbea6")
    EXPECTED_SID = (
        "7916442d59841140bedf6c1f5dcc1304ae9fce0ba885765c06e511086b85da2e"
    )

    def test_record_id_folds_to_16_bytes(self):
        from orcapod.utils.arrow_utils import fold_system_tag_values

        result = fold_system_tag_values(self.RECORD_COL, self.RIDS)
        assert isinstance(result, bytes)
        assert len(result) == 16

    def test_record_id_digest_is_pinned(self):
        from orcapod.utils.arrow_utils import fold_system_tag_values

        assert fold_system_tag_values(self.RECORD_COL, self.RIDS) == self.EXPECTED_RID

    def test_source_id_digest_is_pinned(self):
        from orcapod.utils.arrow_utils import fold_system_tag_values

        result = fold_system_tag_values(self.SOURCE_COL, ["src_a", "src_b"])
        assert result == self.EXPECTED_SID

    def test_order_matters(self):
        """Member order is part of the identity, matching the data lists."""
        from orcapod.utils.arrow_utils import fold_system_tag_values

        forward = fold_system_tag_values(self.RECORD_COL, self.RIDS)
        reverse = fold_system_tag_values(self.RECORD_COL, list(reversed(self.RIDS)))
        assert forward != reverse

    def test_single_member_is_still_folded(self):
        """A one-member group folds rather than passing the value through."""
        from orcapod.utils.arrow_utils import fold_system_tag_values

        result = fold_system_tag_values(self.RECORD_COL, self.RIDS[:1])
        assert isinstance(result, bytes) and len(result) == 16
        assert result != self.RIDS[0]

    def test_none_members_are_tolerated(self):
        from orcapod.utils.arrow_utils import fold_system_tag_values

        assert isinstance(
            fold_system_tag_values(self.RECORD_COL, [None, self.RIDS[0]]), bytes
        )
        assert isinstance(
            fold_system_tag_values(self.SOURCE_COL, [None, "src_a"]), str
        )

    def test_digest_is_stable_across_processes(self):
        """Fresh interpreter, therefore fresh PYTHONHASHSEED.

        This is the test that catches a fold built on hash() or set order:
        such a fold is self-consistent within one process and only diverges
        on a new driver run.
        """
        import subprocess
        import sys

        script = (
            "from orcapod.utils.arrow_utils import fold_system_tag_values\n"
            "rids = [bytes.fromhex('0102030405060708090a0b0c0d0e0f10'),\n"
            "        bytes.fromhex('1112131415161718191a1b1c1d1e1f20')]\n"
            "print(fold_system_tag_values('_tag_record_id::abc123', rids).hex())\n"
            "print(fold_system_tag_values('_tag_source_id::abc123', ['src_a','src_b']))\n"
        )
        out = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            text=True,
            check=True,
        ).stdout.split()
        assert out[0] == self.EXPECTED_RID.hex()
        assert out[1] == self.EXPECTED_SID
