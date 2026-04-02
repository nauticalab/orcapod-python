"""Tests for SourceStreamBuilder."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.sources.stream_builder import SourceStreamBuilder, SourceStreamResult


class TestSourceStreamBuilder:
    @pytest.fixture
    def builder(self):
        from orcapod.contexts import resolve_context
        from orcapod.config import DEFAULT_CONFIG

        ctx = resolve_context(None)
        return SourceStreamBuilder(data_context=ctx, config=DEFAULT_CONFIG)

    def test_build_returns_source_stream_result(self, builder):
        table = pa.table({"id": pa.array([1, 2]), "x": pa.array([10, 20])})
        result = builder.build(table, tag_columns=["id"])
        assert isinstance(result, SourceStreamResult)

    def test_build_stream_has_correct_row_count(self, builder):
        table = pa.table({"id": pa.array([1, 2, 3]), "x": pa.array([10, 20, 30])})
        result = builder.build(table, tag_columns=["id"])
        assert result.stream.as_table().num_rows == 3

    def test_build_source_id_defaults_to_table_hash(self, builder):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        result = builder.build(table, tag_columns=["id"])
        assert result.source_id is not None
        assert len(result.source_id) > 0

    def test_build_source_id_explicit(self, builder):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        result = builder.build(table, tag_columns=["id"], source_id="my_source")
        assert result.source_id == "my_source"

    def test_build_schema_hash_is_string(self, builder):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        result = builder.build(table, tag_columns=["id"])
        assert isinstance(result.schema_hash, str)
        assert len(result.schema_hash) > 0

    def test_build_tag_columns_tuple(self, builder):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        result = builder.build(table, tag_columns=["id"])
        assert result.tag_columns == ("id",)

    def test_build_validates_missing_tag_columns(self, builder):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        with pytest.raises(ValueError, match="tag_columns not found"):
            builder.build(table, tag_columns=["nonexistent"])

    def test_build_validates_missing_record_id_column(self, builder):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        with pytest.raises(ValueError, match="record_id_column"):
            builder.build(table, tag_columns=["id"], record_id_column="bad")

    def test_build_output_schema_has_tag_and_packet(self, builder):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        result = builder.build(table, tag_columns=["id"])
        tag_schema, packet_schema = result.stream.output_schema()
        assert "id" in tag_schema
        assert "x" in packet_schema

    def test_build_with_record_id_column(self, builder):
        table = pa.table({"id": pa.array([1, 2]), "x": pa.array([10, 20])})
        result = builder.build(table, tag_columns=["id"], record_id_column="id")
        assert result.stream.as_table().num_rows == 2

    def test_build_drops_system_columns_from_input(self, builder):
        table = pa.table(
            {
                "id": pa.array([1]),
                "x": pa.array([10]),
                "__system_col": pa.array(["sys"]),
            }
        )
        result = builder.build(table, tag_columns=["id"])
        tag_schema, packet_schema = result.stream.output_schema()
        assert "__system_col" not in packet_schema


class TestSourceStreamBuilderRespectNullable:
    """respect_nullable controls whether nullable Arrow fields become T | None."""

    @pytest.fixture
    def builder(self):
        from orcapod.contexts import resolve_context
        from orcapod.config import DEFAULT_CONFIG

        ctx = resolve_context(None)
        return SourceStreamBuilder(data_context=ctx, config=DEFAULT_CONFIG)

    def _make_nullable_table(self) -> pa.Table:
        """Arrow table where all fields default to nullable=True."""
        return pa.table({"id": pa.array([1]), "val": pa.array([10], type=pa.int64())})

    def test_default_strips_nullable_for_schema_hash(self, builder):
        """Default respect_nullable=False: schema hash uses plain T types."""
        table = self._make_nullable_table()
        result_default = builder.build(table, tag_columns=["id"])

        # Build explicit non-nullable schema to compare
        non_nullable_table = pa.table(
            {"id": pa.array([1]), "val": pa.array([10], type=pa.int64())},
            schema=pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=False),
                    pa.field("val", pa.int64(), nullable=False),
                ]
            ),
        )
        result_non_nullable = builder.build(non_nullable_table, tag_columns=["id"])
        assert result_default.schema_hash == result_non_nullable.schema_hash

    def test_respect_nullable_true_uses_nullable_types_for_hash(self, builder):
        """respect_nullable=True: schema hash uses T | None for nullable fields."""
        table = self._make_nullable_table()
        result_nullable = builder.build(table, tag_columns=["id"], respect_nullable=True)
        result_default = builder.build(table, tag_columns=["id"])
        # nullable and non-nullable schemas produce different hashes
        assert result_nullable.schema_hash != result_default.schema_hash

    def test_respect_nullable_true_output_schema_has_optional_types(self, builder):
        """respect_nullable=True: output schema reflects T | None for nullable fields."""
        table = self._make_nullable_table()
        result = builder.build(table, tag_columns=["id"], respect_nullable=True)
        _, packet_schema = result.stream.output_schema()
        assert packet_schema["val"] == int | None


from orcapod.core.sources.arrow_table_source import ArrowTableSource


class TestArrowTableSourceUsesBuilder:
    def test_arrow_table_source_works(self):
        """ArrowTableSource should use SourceStreamBuilder internally."""
        table = pa.table({"id": pa.array([1, 2]), "x": pa.array([10, 20])})
        src = ArrowTableSource(table=table, tag_columns=["id"])
        assert src.as_table().num_rows == 2
        tag_schema, packet_schema = src.output_schema()
        assert "id" in tag_schema
        assert "x" in packet_schema

    def test_arrow_table_source_has_stream_attr(self):
        table = pa.table({"id": pa.array([1, 2]), "x": pa.array([10, 20])})
        src = ArrowTableSource(table=table, tag_columns=["id"])
        assert hasattr(src, "_stream")

    def test_arrow_table_source_identity_uses_class_name(self):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        src = ArrowTableSource(table=table, tag_columns=["id"])
        identity = src.identity_structure()
        assert identity[0] == "ArrowTableSource"

    def test_resolve_field_raises_not_implemented(self):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        src = ArrowTableSource(table=table, tag_columns=["id"])
        with pytest.raises(NotImplementedError):
            src.resolve_field("row_0", "x")
