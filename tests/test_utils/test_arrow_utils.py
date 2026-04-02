"""Tests for arrow_utils utility functions."""

import pyarrow as pa
import pytest

from orcapod.utils.arrow_utils import prepare_prefixed_columns


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
