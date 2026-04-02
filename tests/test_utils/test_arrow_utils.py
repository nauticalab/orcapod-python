"""Tests for arrow_utils utility functions."""

import pyarrow as pa
import pytest

from orcapod.utils.arrow_utils import (
    infer_schema_nullable,
    make_schema_non_nullable,
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
