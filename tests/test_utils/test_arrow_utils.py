"""Tests for arrow_utils utility functions."""

import pyarrow as pa
import pytest

from orcapod.types import ColumnConfig
from orcapod.utils.arrow_utils import (
    add_source_info,
    apply_column_config,
    infer_schema_nullable,
    make_schema_non_nullable,
    normalize_table_view_types,
    normalize_view_types,
    prepare_prefixed_columns,
)


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
        import pyarrow as pa
        from orcapod.utils.arrow_utils import add_system_tag_columns, system_tag_column_names

        schema_hash = "testhash"
        table = pa.table({"id": pa.array([1]), "v": pa.array([1.0])})
        enriched = add_system_tag_columns(table, schema_hash, ["src_a"], ["row_0"])
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
