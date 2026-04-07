"""
Unit tests for the restore_schema_nullability helper in arrow_utils.

RED phase: all tests in this file must fail before the helper exists.
"""

import pyarrow as pa
import polars as pl
import pytest

from orcapod.utils import arrow_utils


class TestPolarsRoundTripLosesNullability:
    """Demonstrate the root-cause: Polars always produces nullable=True."""

    def test_polars_roundtrip_makes_non_nullable_fields_nullable(self):
        """Polars DataFrame round-trip converts nullable=False to nullable=True."""
        schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("value", pa.large_string(), nullable=False),
            ]
        )
        table = pa.table({"id": [1, 2, 3], "value": ["a", "b", "c"]}, schema=schema)

        # Precondition: original table has non-nullable fields
        assert table.schema.field("id").nullable is False
        assert table.schema.field("value").nullable is False

        # After Polars round-trip all fields become nullable (the known bug)
        roundtrip = pl.DataFrame(table).to_arrow()
        assert roundtrip.schema.field("id").nullable is True
        assert roundtrip.schema.field("value").nullable is True

    def test_polars_join_makes_all_result_fields_nullable(self):
        """Polars inner join result has nullable=True for all fields."""
        left_schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("name", pa.large_string(), nullable=False),
            ]
        )
        right_schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("score", pa.float64(), nullable=False),
            ]
        )
        left = pa.table({"id": [1, 2], "name": ["a", "b"]}, schema=left_schema)
        right = pa.table({"id": [1, 2], "score": [9.5, 8.0]}, schema=right_schema)

        joined = (
            pl.DataFrame(left)
            .join(pl.DataFrame(right), on="id", how="inner")
            .to_arrow()
        )

        # Bug: all columns nullable after join
        assert joined.schema.field("id").nullable is True
        assert joined.schema.field("name").nullable is True
        assert joined.schema.field("score").nullable is True


class TestRestoreSchemaHullability:
    """Unit tests for arrow_utils.restore_schema_nullability."""

    def test_restores_non_nullable_flags_after_polars_roundtrip(self):
        """restore_schema_nullability fixes nullable=True caused by Polars."""
        original_schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("value", pa.large_string(), nullable=False),
            ]
        )
        table = pa.table(
            {"id": [1, 2, 3], "value": ["a", "b", "c"]}, schema=original_schema
        )

        # Simulate Polars round-trip (loses nullability)
        roundtrip = pl.DataFrame(table).to_arrow()
        assert roundtrip.schema.field("id").nullable is True  # confirms the bug

        # Fix: restore from original schema
        restored = arrow_utils.restore_schema_nullability(roundtrip, original_schema)

        assert restored.schema.field("id").nullable is False
        assert restored.schema.field("value").nullable is False

    def test_preserves_data_values_after_restore(self):
        """restore_schema_nullability does not alter data, only schema metadata."""
        original_schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("x", pa.float64(), nullable=False),
            ]
        )
        table = pa.table({"id": [1, 2], "x": [1.5, 2.5]}, schema=original_schema)
        roundtrip = pl.DataFrame(table).to_arrow()

        restored = arrow_utils.restore_schema_nullability(roundtrip, original_schema)

        assert restored.to_pydict() == table.to_pydict()

    def test_preserves_nullable_fields_that_are_nullable_in_reference(self):
        """Fields that are nullable in the reference schema remain nullable."""
        original_schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("optional_val", pa.large_string(), nullable=True),
            ]
        )
        table = pa.table(
            {"id": [1, 2], "optional_val": ["x", None]}, schema=original_schema
        )
        roundtrip = pl.DataFrame(table).to_arrow()

        restored = arrow_utils.restore_schema_nullability(roundtrip, original_schema)

        assert restored.schema.field("id").nullable is False
        assert restored.schema.field("optional_val").nullable is True

    def test_leaves_extra_columns_as_nullable(self):
        """Columns absent from the reference schema keep Polars-default nullable=True."""
        original_schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
            ]
        )
        full_schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("_exists", pa.bool_(), nullable=True),
            ]
        )
        table = pa.table({"id": [1, 2], "_exists": [True, False]}, schema=full_schema)
        roundtrip = pl.DataFrame(table).to_arrow()

        restored = arrow_utils.restore_schema_nullability(roundtrip, original_schema)

        assert restored.schema.field("id").nullable is False
        # "_exists" not in reference → left as Polars output (nullable=True)
        assert restored.schema.field("_exists").nullable is True

    def test_with_multiple_reference_schemas_later_wins(self):
        """When the same field name appears in multiple reference schemas, the last wins."""
        schema_a = pa.schema([pa.field("id", pa.int64(), nullable=True)])
        schema_b = pa.schema([pa.field("id", pa.int64(), nullable=False)])

        table = pa.table({"id": [1, 2]})
        roundtrip = pl.DataFrame(table).to_arrow()

        # schema_b comes last → nullable=False wins
        restored = arrow_utils.restore_schema_nullability(roundtrip, schema_a, schema_b)
        assert restored.schema.field("id").nullable is False

    def test_restores_non_nullable_in_polars_join_result(self):
        """restore_schema_nullability works on the output of a Polars join."""
        left_schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("name", pa.large_string(), nullable=False),
            ]
        )
        right_schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("score", pa.float64(), nullable=False),
            ]
        )
        left = pa.table({"id": [1, 2], "name": ["a", "b"]}, schema=left_schema)
        right = pa.table({"id": [1, 2], "score": [9.5, 8.0]}, schema=right_schema)

        joined = (
            pl.DataFrame(left)
            .join(pl.DataFrame(right), on="id", how="inner")
            .to_arrow()
        )

        restored = arrow_utils.restore_schema_nullability(
            joined, left_schema, right_schema
        )

        assert restored.schema.field("id").nullable is False
        assert restored.schema.field("name").nullable is False
        assert restored.schema.field("score").nullable is False
