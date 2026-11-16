"""Tests for SourceStreamBuilder."""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.sources.stream_builder import SourceStreamBuilder, SourceStreamResult
from orcapod.core.sources.arrow_table_source import ArrowTableSource


class TestSourceStreamBuilder:
    @pytest.fixture
    def builder(self):
        from orcapod.contexts import resolve_context
        from orcapod.config import DEFAULT_CONFIG

        ctx = resolve_context(None)
        return SourceStreamBuilder(data_context=ctx, config=DEFAULT_CONFIG)

    def test_build_returns_source_stream_result(self, builder):
        table = pa.table({"id": pa.array([1, 2]), "x": pa.array([10, 20])})
        result = builder.build(table, tag_columns=["id"], source_kind="arrow_table")
        assert isinstance(result, SourceStreamResult)

    def test_build_stream_has_correct_row_count(self, builder):
        table = pa.table({"id": pa.array([1, 2, 3]), "x": pa.array([10, 20, 30])})
        result = builder.build(table, tag_columns=["id"], source_kind="arrow_table")
        assert result.stream.as_table().num_rows == 3

    def test_build_source_id_defaults_to_table_hash(self, builder):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        result = builder.build(table, tag_columns=["id"], source_kind="arrow_table")
        assert result.source_id is not None
        assert len(result.source_id) > 0

    def test_build_default_source_id_is_kind_and_labelled_table_hash(self, builder):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        result = builder.build(table, tag_columns=["id"], source_kind="arrow_table")
        assert result.source_id == f"arrow_table:{result.table_hash.to_string()}"

    def test_build_without_source_id_or_source_kind_raises(self, builder):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        with pytest.raises(ValueError, match="source_id or a source_kind"):
            builder.build(table, tag_columns=["id"])

    def test_build_source_id_explicit(self, builder):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        result = builder.build(table, tag_columns=["id"], source_id="my_source")
        assert result.source_id == "my_source"

    def test_build_schema_hash_is_string(self, builder):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        result = builder.build(table, tag_columns=["id"], source_kind="arrow_table")
        assert isinstance(result.schema_hash, str)
        assert len(result.schema_hash) > 0

    def test_build_tag_columns_tuple(self, builder):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        result = builder.build(table, tag_columns=["id"], source_kind="arrow_table")
        assert result.tag_columns == ("id",)

    def test_build_validates_missing_tag_columns(self, builder):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        with pytest.raises(ValueError, match="tag_columns not found"):
            builder.build(table, tag_columns=["nonexistent"])

    def test_build_validates_missing_record_id_column(self, builder):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        with pytest.raises(ValueError, match="record_id_column"):
            builder.build(table, tag_columns=["id"], record_id_column="bad")

    def test_build_output_schema_has_tag_and_data(self, builder):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        result = builder.build(table, tag_columns=["id"], source_kind="arrow_table")
        tag_schema, data_schema = result.stream.output_schema()
        assert "id" in tag_schema
        assert "x" in data_schema

    def test_build_with_record_id_column(self, builder):
        table = pa.table({"id": pa.array([1, 2]), "x": pa.array([10, 20])})
        result = builder.build(
            table, tag_columns=["id"], record_id_column="id", source_kind="arrow_table"
        )
        assert result.stream.as_table().num_rows == 2

    def test_build_drops_system_columns_from_input(self, builder):
        table = pa.table(
            {
                "id": pa.array([1]),
                "x": pa.array([10]),
                "__system_col": pa.array(["sys"]),
            }
        )
        result = builder.build(table, tag_columns=["id"], source_kind="arrow_table")
        tag_schema, data_schema = result.stream.output_schema()
        assert "__system_col" not in data_schema


class TestSourceStreamBuilderRespectsSchema:
    """build() trusts the incoming table's nullable flags as-is."""

    @pytest.fixture
    def builder(self):
        from orcapod.contexts import resolve_context
        from orcapod.config import DEFAULT_CONFIG

        ctx = resolve_context(None)
        return SourceStreamBuilder(data_context=ctx, config=DEFAULT_CONFIG)

    def test_nullable_true_schema_preserved(self, builder):
        """build() preserves nullable=True from incoming schema."""
        table = pa.table({"id": pa.array([1]), "val": pa.array([10], type=pa.int64())})
        # Arrow default: nullable=True
        result = builder.build(table, tag_columns=["id"], source_kind="arrow_table")
        _, data_schema = result.stream.output_schema()
        assert data_schema["val"] == int | None

    def test_nullable_false_schema_preserved(self, builder):
        """build() preserves nullable=False from incoming schema."""
        table = pa.table(
            {"id": pa.array([1]), "val": pa.array([10], type=pa.int64())},
            schema=pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=False),
                    pa.field("val", pa.int64(), nullable=False),
                ]
            ),
        )
        result = builder.build(table, tag_columns=["id"], source_kind="arrow_table")
        _, data_schema = result.stream.output_schema()
        assert data_schema["val"] is int

    def test_nullable_flags_affect_schema_hash(self, builder):
        """nullable=True and nullable=False schemas produce different hashes."""
        nullable_table = pa.table(
            {"id": pa.array([1]), "val": pa.array([10], type=pa.int64())}
        )
        non_nullable_table = pa.table(
            {"id": pa.array([1]), "val": pa.array([10], type=pa.int64())},
            schema=pa.schema(
                [
                    pa.field("id", pa.int64(), nullable=False),
                    pa.field("val", pa.int64(), nullable=False),
                ]
            ),
        )
        result_nullable = builder.build(
            nullable_table, tag_columns=["id"], source_kind="arrow_table"
        )
        result_non_nullable = builder.build(
            non_nullable_table, tag_columns=["id"], source_kind="arrow_table"
        )
        assert result_nullable.schema_hash != result_non_nullable.schema_hash


class TestArrowTableSourceUsesBuilder:
    def test_arrow_table_source_works(self):
        """ArrowTableSource should use SourceStreamBuilder internally."""
        table = pa.table({"id": pa.array([1, 2]), "x": pa.array([10, 20])})
        src = ArrowTableSource(table=table, tag_columns=["id"], tag_only_columns=True)
        assert src.as_table().num_rows == 2
        tag_schema, data_schema = src.output_schema()
        assert "id" in tag_schema
        assert "x" in data_schema

    def test_arrow_table_source_has_stream_attr(self):
        table = pa.table({"id": pa.array([1, 2]), "x": pa.array([10, 20])})
        src = ArrowTableSource(table=table, tag_columns=["id"], tag_only_columns=True)
        assert hasattr(src, "_stream")

    def test_arrow_table_source_identity_uses_class_name(self):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        src = ArrowTableSource(table=table, tag_columns=["id"], tag_only_columns=True)
        identity = src.identity_structure()
        assert identity[0] == "ArrowTableSource"


class TestProvenanceTokensMatchRowValues:
    """Provenance tokens are built from one column, never the whole table (ITL-926 C3b).

    The expected tokens are computed the way the builder used to build them, from
    ``table.to_pylist()`` rows, so the test fails if the per-column path changes any token
    (and therefore any record id, which is derived from the token).
    """

    @staticmethod
    def _table():
        import datetime as dt
        import decimal

        import pyarrow as pa

        utc = dt.timezone.utc
        return pa.table(
            {
                "k_str": pa.array(["b", "a", None, "c"], pa.string()),
                "k_int": pa.array([3, 1, 2, None], pa.int64()),
                "k_u64": pa.array([2**63 + 5, 1, 2, 3], pa.uint64()),
                "k_ts": pa.array(
                    [dt.datetime(2026, 1, 1, 9, 0, 0, 123456)] * 3 + [None],
                    pa.timestamp("us"),
                ),
                "k_tstz": pa.array(
                    [dt.datetime(2026, 1, 1, 9, tzinfo=utc)] * 4,
                    pa.timestamp("us", "UTC"),
                ),
                "k_date": pa.array([dt.date(2026, 1, 1)] * 4, pa.date32()),
                "k_float": pa.array([1.5, float("nan"), -0.0, 1e300], pa.float64()),
                "k_bool": pa.array([True, False, None, True]),
                "k_bin": pa.array([b"\x00\xff", b"a", None, b""], pa.binary()),
                "k_dec": pa.array(
                    [decimal.Decimal("1.10"), None, decimal.Decimal("0"), None],
                    pa.decimal128(5, 2),
                ),
                "v": pa.array([1.0, 2.0, 3.0, 4.0]),
            }
        )

    @pytest.mark.parametrize(
        "column",
        [
            "k_str",
            "k_int",
            "k_u64",
            "k_ts",
            "k_tstz",
            "k_date",
            "k_float",
            "k_bool",
            "k_bin",
            "k_dec",
            None,
        ],
    )
    def test_tokens_equal_row_based_tokens(self, column):
        from orcapod.sources import ArrowTableSource

        table = self._table()
        tag = column or "k_str"
        source = ArrowTableSource(
            table,
            tag_columns=[tag],
            record_id_column=column,
            source_id="s",
            tag_only_columns=True,
        )
        tokens = (
            source.as_table(columns={"source": True}).column("_source_v").to_pylist()
        )
        rows = table.to_pylist()
        expected = [
            f"s::{column}={row[column]}::v" if column else f"s::row_{i}::v"
            for i, row in enumerate(rows)
        ]
        assert tokens == expected


class TestUnhashableColumnTypes:
    """A column the Arrow hasher refuses is named in the error (ITL-926, ITL-967).

    The hasher (starfix 0.5, ITL-910) raises ``ValueError`` for a type it does not hash,
    such as ``map``, and names only the first such column. The source names every one,
    with its type. ``fixed_size_list`` is hashed since starfix 0.5.
    """

    _FSL = pa.list_(pa.uint8(), 3)
    _MAP = pa.map_(pa.string(), pa.int64())

    def _table(self, **extra: pa.Array[Any]) -> pa.Table:
        return pa.table({"k": ["a", "b"], "v": [1.0, 2.0], **extra})

    def test_fixed_size_list_column_is_hashed(self):
        """A source accepts a ``fixed_size_list`` column and keeps its type."""
        table = self._table(rgb=pa.array([[1, 2, 3], [4, 5, 6]], self._FSL))
        source = ArrowTableSource(table, tag_columns=["k"], tag_only_columns=True)
        assert source.as_table().schema.field("rgb").type == self._FSL

    def test_map_column_is_named(self):
        table = self._table(tags=pa.array([[("x", 1)], []], self._MAP))
        with pytest.raises(ValueError) as info:
            ArrowTableSource(table, tag_columns=["k"], tag_only_columns=True)
        message = str(info.value)
        assert message.startswith("cannot hash column 'tags' of type ")
        assert str(self._MAP) in message
        assert "'v'" not in message
        assert isinstance(info.value.__cause__, ValueError)
        # The hasher's own message is kept in the text as well as in the cause.
        assert str(info.value.__cause__) in message

    def test_every_unhashable_column_is_named(self):
        table = self._table(
            tags=pa.array([[("x", 1)], []], self._MAP),
            attrs=pa.array([[], [("y", 2)]], self._MAP),
        )
        with pytest.raises(ValueError) as info:
            ArrowTableSource(table, tag_columns=["k"], tag_only_columns=True)
        message = str(info.value)
        assert message.startswith("cannot hash columns ")
        assert "'tags'" in message
        assert "'attrs'" in message
        assert "'v'" not in message

    def test_hashable_table_is_hashed_once(self, monkeypatch):
        # Counts calls on the data context's hasher: the per-column probe must run only
        # after the whole-table hash has failed, never on the success path.
        from orcapod.contexts import resolve_context

        hasher = resolve_context(None).arrow_hasher
        calls: list[int] = []
        original = hasher.hash_table

        def counting(*args, **kwargs):
            calls.append(1)
            return original(*args, **kwargs)

        monkeypatch.setattr(hasher, "hash_table", counting)
        ArrowTableSource(self._table(), tag_columns=["k"], tag_only_columns=True)
        assert len(calls) == 1
