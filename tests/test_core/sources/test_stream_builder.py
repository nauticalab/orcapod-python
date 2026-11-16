"""Tests for SourceStreamBuilder."""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.sources.stream_builder import SourceStreamBuilder, SourceStreamResult
from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.errors import RowIdentifierError


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
        with pytest.raises(ValueError, match="record_id_columns"):
            builder.build(table, tag_columns=["id"], record_id_columns="bad")

    def test_build_output_schema_has_tag_and_data(self, builder):
        table = pa.table({"id": pa.array([1]), "x": pa.array([10])})
        result = builder.build(table, tag_columns=["id"], source_kind="arrow_table")
        tag_schema, data_schema = result.stream.output_schema()
        assert "id" in tag_schema
        assert "x" in data_schema

    def test_build_with_record_id_column(self, builder):
        table = pa.table({"id": pa.array([1, 2]), "x": pa.array([10, 20])})
        result = builder.build(
            table, tag_columns=["id"], record_id_columns="id", source_kind="arrow_table"
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


class TestKeyColumnOrder:
    """Several key columns are given in key order, never in a set (ITL-1154)."""

    @pytest.mark.parametrize("unordered", [{"a", "b"}, frozenset({"a", "b"})])
    def test_a_set_of_key_columns_is_refused(self, unordered: Any) -> None:
        table = pa.table({"a": [1, 2], "b": [3, 4], "v": [1.0, 2.0]})
        with pytest.raises(TypeError, match="key order"):
            ArrowTableSource(table, record_id_columns=unordered, source_id="s")

    def test_a_list_and_a_tuple_give_the_same_identifiers(self) -> None:
        table = pa.table({"a": [1, 2], "b": [3, 4], "v": [1.0, 2.0]})
        columns = {"system_tags": True}
        by_list = ArrowTableSource(table, record_id_columns=["b", "a"], source_id="s")
        by_tuple = ArrowTableSource(table, record_id_columns=("b", "a"), source_id="s")
        assert by_list.as_table(columns=columns).equals(
            by_tuple.as_table(columns=columns)
        )


class TestKeyRowIdentifiers:
    """A row named by key columns: the canonical JSON of its key (ITL-1154).

    The expected identifiers are written out by hand, so the test fails if the form
    of any allowed key type changes (which would move every record id built on it).
    """

    @staticmethod
    def _table() -> pa.Table:
        import datetime as dt

        return pa.table(
            {
                "k_str": pa.array(["b", "a", "c"], pa.string()),
                "k_int": pa.array([3, 1, 2], pa.int64()),
                "k_u64": pa.array([2**63 + 5, 1, 2], pa.uint64()),
                "k_bool": pa.array([True, False, None]),
                "k_ts": pa.array(
                    [
                        dt.datetime(2026, 1, 1, 9, 0, 0, 123456),
                        dt.datetime(1969, 12, 31, 23, 59, 59, 500000),
                        dt.datetime(2026, 1, 1),
                    ],
                    pa.timestamp("us"),
                ),
                "k_tstz": pa.array([0, 1500, 2000], pa.timestamp("ms", "UTC")),
                "k_date": pa.array(
                    [dt.date(2026, 1, 1), dt.date(1960, 1, 1), dt.date(2000, 2, 29)],
                    pa.date32(),
                ),
                "v": pa.array([1.0, 2.0, 3.0]),
            }
        )

    @pytest.mark.parametrize(
        ("columns", "expected"),
        [
            ("k_str", ['{"k_str":"b"}', '{"k_str":"a"}', '{"k_str":"c"}']),
            ("k_int", ['{"k_int":3}', '{"k_int":1}', '{"k_int":2}']),
            (
                "k_u64",
                ['{"k_u64":9223372036854775813}', '{"k_u64":1}', '{"k_u64":2}'],
            ),
            (
                "k_ts",
                [
                    '{"k_ts":"2026-01-01T09:00:00.123456"}',
                    '{"k_ts":"1969-12-31T23:59:59.500000"}',
                    '{"k_ts":"2026-01-01T00:00:00.000000"}',
                ],
            ),
            (
                "k_tstz",
                [
                    '{"k_tstz":"1970-01-01T00:00:00.000Z"}',
                    '{"k_tstz":"1970-01-01T00:00:01.500Z"}',
                    '{"k_tstz":"1970-01-01T00:00:02.000Z"}',
                ],
            ),
            (
                "k_date",
                [
                    '{"k_date":"2026-01-01"}',
                    '{"k_date":"1960-01-01"}',
                    '{"k_date":"2000-02-29"}',
                ],
            ),
            (
                ["k_int", "k_str"],
                [
                    '{"k_int":3,"k_str":"b"}',
                    '{"k_int":1,"k_str":"a"}',
                    '{"k_int":2,"k_str":"c"}',
                ],
            ),
            (None, ["row_0", "row_1", "row_2"]),
            ((), ["row_0", "row_1", "row_2"]),
        ],
        ids=[
            "str",
            "int",
            "u64",
            "ts",
            "tstz",
            "date",
            "composite",
            "none",
            "position",
        ],
    )
    def test_record_id_and_token(self, columns: Any, expected: list[str]) -> None:
        """The record id is the identifier; the token is ``<source>::<id>::<column>``."""
        source = ArrowTableSource(
            self._table(),
            tag_columns=["k_str"],
            record_id_columns=columns,
            source_id="s",
            tag_only_columns=True,
        )
        table = source.as_table(columns={"source": True, "system_tags": True})
        (record_id,) = [n for n in table.column_names if n.startswith("_tag_record_id")]
        assert table.column(record_id).type == pa.large_string()
        assert table.column(record_id).to_pylist() == expected
        assert table.column("_source_v").to_pylist() == [
            f"s::{identifier}::v" for identifier in expected
        ]

    def test_int_and_str_keys_differ(self) -> None:
        """``17`` and ``"17"`` are different keys."""

        def record_id(value: object) -> str:
            source = ArrowTableSource(
                pa.table({"k": [value], "v": [1.0]}),
                tag_columns=["v"],
                record_id_columns="k",
                source_id="s",
                tag_only_columns=True,
            )
            table = source.as_table(columns={"system_tags": True})
            (name,) = [n for n in table.column_names if n.startswith("_tag_record_id")]
            value = table.column(name).to_pylist()[0]
            assert isinstance(value, str)
            return value

        assert record_id(17) == '{"k":17}'
        assert record_id("17") == '{"k":"17"}'

    @pytest.mark.parametrize(
        ("values", "match"),
        [
            (pa.array([1.5, 2.5]), "type double"),
            (pa.array([b"a", b"b"]), "type binary"),
            (pa.array(["a", None]), "is null"),
            (pa.array(["a", "a"]), "appears in rows 0 and 1"),
        ],
        ids=["float", "bytes", "null", "duplicate"],
    )
    def test_refused_keys(self, values: pa.Array[Any], match: str) -> None:
        """A disallowed type, a null key and a repeated key are refused when built."""
        table = pa.table({"k": values, "v": [1.0, 2.0]})
        with pytest.raises(ValueError, match=match):
            ArrowTableSource(
                table, tag_columns=["v"], record_id_columns="k", tag_only_columns=True
            )

    def test_year_one_is_written_with_four_digits(self) -> None:
        """The year always has four digits, on every platform."""
        table = pa.table(
            {
                "k": pa.array([-62135596800], pa.timestamp("s")),
                "v": [1.0],
            }
        )
        source = ArrowTableSource(
            table, tag_columns=["v"], record_id_columns="k", tag_only_columns=True
        )
        stored = source.as_table(columns={"system_tags": True})
        (name,) = [n for n in stored.column_names if n.startswith("_tag_record_id")]
        assert stored.column(name).to_pylist() == ['{"k":"0001-01-01T00:00:00"}']

    @pytest.mark.parametrize(
        "values",
        [
            pa.array([253402300800], pa.timestamp("s")),
            pa.array([3_000_000], pa.date32()),
        ],
        ids=["timestamp", "date"],
    )
    def test_a_key_past_year_9999_is_refused(self, values: pa.Array[Any]) -> None:
        """Arrow stores moments a row identifier cannot write; they are refused."""
        table = pa.table({"k": values, "v": [1.0]})
        with pytest.raises(RowIdentifierError, match="outside years 1 to 9999"):
            ArrowTableSource(
                table, tag_columns=["v"], record_id_columns="k", tag_only_columns=True
            )


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
