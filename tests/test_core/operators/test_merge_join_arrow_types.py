"""MergeJoin types its output from its inputs' Arrow types (ITL-813, ITL-832).

The rules:

* Nothing is inferred from Python values. Every output column takes the
  large-normalized form of its input Arrow type (``string`` to
  ``large_string``, ``list`` to ``large_list``, recursively; the Arrow hasher
  treats the variants as identical). Integer and float width, nullability and
  logical (extension) types are never changed.
* A merged (colliding) column is ``large_list<normalized input element>``, or
  the logical list type of a logical element; its source-info column is
  ``large_list<large_string>``. Zero-row and non-empty results have the same
  types, equal to the predicted schema.
* When the two inputs' Arrow types for a column differ, both are normalized;
  if they are then equal the join proceeds, otherwise it raises
  ``InputValidationError`` naming the column and both types.

Predictions are compared against ``static_process(...)``, never against
``process(...)``, whose ``output_schema`` delegates back to the operator.
"""

from __future__ import annotations

import datetime
import decimal
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context, get_default_type_converter
from orcapod.hashing.arrow_hashers import StarfixArrowHasher
from orcapod.core.operators import MergeJoin, PolarsFilter
from orcapod.core.sources import CSVSource, DictSource
from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.system_constants import constants
from orcapod.utils.arrow_utils import (
    normalize_to_large_types,
    shared_arrow_type,
)

SCHEMA_CONFIGS: tuple[dict[str, bool], ...] = ({}, {"system_tags": True})
STRUCT = pa.struct([("a", pa.int8()), ("b", pa.string())])
LARGE_STRUCT = pa.struct([("a", pa.int8()), ("b", pa.large_string())])
SOURCE_LIST = pa.large_list(pa.large_string())
_TS = [datetime.datetime(2020, 1, 1), datetime.datetime(2021, 1, 1)]
_DEC = [decimal.Decimal("1.00"), decimal.Decimal("2.50")]

# (input Arrow type, two distinct values in ascending order, expected element)
ELEMENT_CASES: list[tuple[pa.DataType, list[Any], pa.DataType]] = [
    (pa.int8(), [1, 2], pa.int8()),
    (pa.int16(), [1, 2], pa.int16()),
    (pa.int32(), [1, 2], pa.int32()),
    (pa.int64(), [1, 2], pa.int64()),
    (pa.uint8(), [1, 2], pa.uint8()),
    (pa.float32(), [1.5, 2.5], pa.float32()),
    (pa.float64(), [1.5, 2.5], pa.float64()),
    (pa.string(), ["a", "b"], pa.large_string()),
    (pa.large_string(), ["a", "b"], pa.large_string()),
    (pa.binary(), [b"a", b"b"], pa.large_binary()),
    (pa.large_binary(), [b"a", b"b"], pa.large_binary()),
    (pa.bool_(), [False, True], pa.bool_()),
    (pa.timestamp("ms"), _TS, pa.timestamp("ms")),
    (
        pa.date32(),
        [datetime.date(2020, 1, 1), datetime.date(2021, 1, 1)],
        pa.date32(),
    ),
    (pa.list_(pa.int8()), [[1], [2]], pa.large_list(pa.int8())),
    (pa.large_list(pa.int32()), [[1], [2]], pa.large_list(pa.int32())),
    (pa.decimal128(5, 2), _DEC, pa.decimal128(5, 2)),
]
STRUCT_VALUES = [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]

# Pairs that differ only in layout: both normalize to the same large type.
LAYOUT_PAIRS: list[tuple[pa.DataType, pa.DataType, list[Any], pa.DataType]] = [
    (pa.string(), pa.large_string(), ["a", "b"], pa.large_string()),
    (pa.binary(), pa.large_binary(), [b"a", b"b"], pa.large_binary()),
    (
        pa.list_(pa.int64()),
        pa.large_list(pa.int64()),
        [[1], [2]],
        pa.large_list(pa.int64()),
    ),
    (
        pa.list_(pa.string()),
        pa.large_list(pa.large_string()),
        [["a"], ["b"]],
        pa.large_list(pa.large_string()),
    ),
]
STRUCT_LAYOUT_PAIR = (STRUCT, LARGE_STRUCT, STRUCT_VALUES, LARGE_STRUCT)

# Pairs that differ in more than layout: rejected.
MISMATCHED_PAIRS: list[tuple[pa.DataType, pa.DataType, list[Any]]] = [
    (pa.int8(), pa.int64(), [1, 2]),
    (pa.float32(), pa.float64(), [1.5, 2.5]),
    (pa.timestamp("ms"), pa.timestamp("us"), _TS),
    (pa.decimal128(5, 2), pa.decimal128(7, 2), _DEC),
    (pa.list_(pa.int8()), pa.large_list(pa.int64()), [[1], [2]]),
]


def _starfix_hasher() -> StarfixArrowHasher:
    """Return the default context's Arrow hasher, which hashes schemas too."""
    hasher = get_default_context().arrow_hasher
    assert isinstance(hasher, StarfixArrowHasher)
    return hasher


def _table(columns: dict[str, tuple[pa.DataType, list[Any]]]) -> pa.Table:
    """Build a table of non-nullable fields from ``{name: (type, values)}``."""
    return pa.Table.from_arrays(
        [pa.array(values, type=arrow_type) for arrow_type, values in columns.values()],
        schema=pa.schema(
            [
                pa.field(name, arrow_type, nullable=False)
                for name, (arrow_type, _) in columns.items()
            ]
        ),
    )


def _stream(
    columns: dict[str, tuple[pa.DataType, list[Any]]], tags: list[str]
) -> ArrowTableStream:
    """Build a stream with source info for every data column."""
    data_columns = [name for name in columns if name not in tags]
    return ArrowTableStream(
        _table(columns),
        tag_columns=tags,
        source_info={name: "origin" for name in data_columns},
    )


def _side(arrow_type: pa.DataType, value: Any, rows: int) -> ArrowTableStream:
    """Build one input: tag ``probe`` and data column ``x`` holding ``value``."""
    return _stream(
        {
            "probe": (pa.int64(), list(range(rows))),
            "x": (arrow_type, [value] * rows),
        },
        ["probe"],
    )


def _pair(
    left_type: pa.DataType,
    right_type: pa.DataType,
    values: list[Any],
    left_rows: int,
    right_rows: int | None = None,
) -> tuple[ArrowTableStream, ArrowTableStream]:
    """Build two streams sharing tag ``probe`` and colliding data column ``x``.

    Each row of the left stream holds ``values[0]`` and each row of the right
    stream ``values[1]``, so every merged row is ``values`` in sorted order.
    """
    if right_rows is None:
        right_rows = left_rows
    return (
        _side(left_type, values[0], left_rows),
        _side(right_type, values[1], right_rows),
    )


def _assert_matches_prediction(
    op: MergeJoin | PolarsFilter,
    result: StreamProtocol,
    *inputs: StreamProtocol,
) -> None:
    """Assert the materialized schema equals the operator's prediction."""
    for config in SCHEMA_CONFIGS:
        assert result.output_schema(columns=config) == op.output_schema(
            *inputs, columns=config
        ), config


def _merged_types(table: pa.Table) -> tuple[pa.DataType, pa.DataType]:
    """Return the Arrow types of merged column ``x`` and its source-info column."""
    return (
        table.schema.field("x").type,
        table.schema.field(f"{constants.SOURCE_PREFIX}x").type,
    )


def _ids(value: object) -> str:
    """Name a parametrized case by its Arrow types, leaving values out."""
    return str(value) if isinstance(value, pa.DataType) else ""


ROW_SHAPES = [(2, 2), (0, 2), (2, 0), (0, 0)]


class TestMergedColumnType:
    """A merged column is ``large_list<normalized input type>`` with or without rows."""

    @pytest.mark.parametrize(
        ("arrow_type", "values", "element"), ELEMENT_CASES, ids=_ids
    )
    @pytest.mark.parametrize(("left_rows", "right_rows"), ROW_SHAPES)
    def test_type_is_from_the_input(
        self,
        arrow_type: pa.DataType,
        values: list[Any],
        element: pa.DataType,
        left_rows: int,
        right_rows: int,
    ) -> None:
        """Zero-row left, right or both type ``x`` exactly as a non-empty join does.

        The element is the input type in its large variants: a narrow input
        never widens (``int8`` stays ``int8``), and the source-info column is
        ``large_list<large_string>``. The schema matches the prediction.
        """
        left, right = _pair(arrow_type, arrow_type, values, left_rows, right_rows)
        op = MergeJoin()
        result = op.static_process(left, right)
        table = result.as_table(columns={"source": True})

        assert _merged_types(table) == (pa.large_list(element), SOURCE_LIST)
        rows = min(left_rows, right_rows) * (1 if left_rows == right_rows else 0)
        assert len(table) == rows
        if rows:
            assert table.column("x").to_pylist() == [values] * rows
        _assert_matches_prediction(op, result, left, right)

    @pytest.mark.parametrize(
        ("arrow_type", "values", "element"), ELEMENT_CASES, ids=_ids
    )
    def test_empty_and_non_empty_results_concatenate(
        self, arrow_type: pa.DataType, values: list[Any], element: pa.DataType
    ) -> None:
        """Empty and non-empty results share one schema, so they concatenate."""
        del element
        tables = [
            MergeJoin()
            .static_process(*_pair(arrow_type, arrow_type, values, left, right))
            .as_table(columns={"source": True})
            for left, right in ROW_SHAPES
        ]
        assert len(pa.concat_tables(tables)) == 2

    @pytest.mark.parametrize(("left_rows", "right_rows"), ROW_SHAPES)
    def test_logical_element_keeps_its_logical_list_type(
        self, left_rows: int, right_rows: int
    ) -> None:
        """A ``Path`` column merges into the ``list[orcapod.path]`` logical list.

        The inner logical type is kept in the extension name; the element is
        never stripped to a plain list of strings.
        """
        converter = get_default_type_converter()
        path_type = converter.python_type_to_arrow_type(Path)
        assert isinstance(path_type, pa.ExtensionType)

        def make(value: str, rows: int) -> ArrowTableStream:
            """Build a stream with a ``Path``-typed column ``x`` holding ``value``."""
            storage = pa.array([value] * rows, type=path_type.storage_type)
            table = pa.Table.from_arrays(
                [
                    pa.array(list(range(rows)), type=pa.int64()),
                    pa.ExtensionArray.from_storage(path_type, storage),
                ],
                schema=pa.schema(
                    [
                        pa.field("probe", pa.int64(), nullable=False),
                        pa.field("x", path_type, nullable=False),
                    ]
                ),
            )
            return ArrowTableStream(
                table, tag_columns=["probe"], source_info={"x": "origin"}
            )

        left, right = make("/b", left_rows), make("/a", right_rows)
        op = MergeJoin()
        result = op.static_process(left, right)
        table = result.as_table(columns={"source": True})
        list_type = table.schema.field("x").type

        assert list_type == converter.python_type_to_arrow_type(list[Path])
        assert isinstance(list_type, pa.ExtensionType)
        assert list_type.extension_name == "list[orcapod.path]"
        assert list_type.storage_type == pa.large_list(pa.large_string())
        assert _merged_types(table)[1] == SOURCE_LIST
        if left_rows and right_rows:
            merged = table.column("x").combine_chunks()
            assert isinstance(merged, pa.ExtensionArray)
            assert merged.storage.to_pylist() == [["/a", "/b"]] * 2
        _assert_matches_prediction(op, result, left, right)

    @pytest.mark.parametrize(("left_rows", "right_rows"), ROW_SHAPES[1:])
    @pytest.mark.parametrize("right_type", [STRUCT, LARGE_STRUCT], ids=str)
    def test_struct_element_with_zero_rows(
        self, left_rows: int, right_rows: int, right_type: pa.DataType
    ) -> None:
        """A struct column merges into ``large_list<struct<..., large_string>>``.

        Struct fields keep their width and nullability; their string children
        take the large variant, also when the other input already has it.
        """
        left, right = (
            _side(STRUCT, STRUCT_VALUES[0], left_rows),
            _side(right_type, STRUCT_VALUES[1], right_rows),
        )
        op = MergeJoin()
        op.validate_inputs(left, right)
        result = op.static_process(left, right)
        table = result.as_table(columns={"source": True})
        assert _merged_types(table) == (pa.large_list(LARGE_STRUCT), SOURCE_LIST)
        assert len(table) == 0
        _assert_matches_prediction(op, result, left, right)

    @pytest.mark.xfail(
        raises=TypeError,
        strict=True,
        reason="ITL-839: MergeJoin sorts merged values as Python objects, and "
        "dicts do not order; pre-existing on main.",
    )
    def test_struct_element_with_rows(self) -> None:
        """Non-empty struct columns cannot be merged yet (ITL-839)."""
        MergeJoin().static_process(*_pair(STRUCT, STRUCT, STRUCT_VALUES, 2))


def _ordered(
    left: StreamProtocol, right: StreamProtocol, order: str
) -> tuple[StreamProtocol, StreamProtocol]:
    """Return the two streams in argument order ``"LR"`` or ``"RL"``."""
    return (left, right) if order == "LR" else (right, left)


class TestSharedArrowType:
    """The single rule for two Arrow types of one column: shared type or None."""

    @pytest.mark.parametrize(
        "arrow_type",
        [pa.int8(), pa.string(), pa.list_(pa.int8()), STRUCT],
        ids=str,
    )
    def test_equal_types_give_the_large_type(self, arrow_type: pa.DataType) -> None:
        """Equal types share their large-normalized form."""
        expected = normalize_to_large_types(arrow_type, keep_null=True)
        assert shared_arrow_type(arrow_type, arrow_type) == expected

    @pytest.mark.parametrize(
        ("left", "right", "values", "expected"),
        [*LAYOUT_PAIRS, STRUCT_LAYOUT_PAIR],
        ids=_ids,
    )
    def test_layout_only_difference_gives_the_large_type(
        self,
        left: pa.DataType,
        right: pa.DataType,
        values: list[Any],
        expected: pa.DataType,
    ) -> None:
        """Layout-only differences share the large variant, in either order."""
        del values
        assert shared_arrow_type(left, right) == expected
        assert shared_arrow_type(right, left) == expected

    @pytest.mark.parametrize(("left", "right", "values"), MISMATCHED_PAIRS, ids=_ids)
    def test_other_difference_gives_none(
        self, left: pa.DataType, right: pa.DataType, values: list[Any]
    ) -> None:
        """Width, unit, precision or nested-width differences share no type."""
        del values
        assert shared_arrow_type(left, right) is None
        assert shared_arrow_type(right, left) is None

    def test_struct_with_different_fields_gives_none(self) -> None:
        """Structs whose fields differ are not layout variants of each other."""
        other = pa.struct([("a", pa.int8()), ("c", pa.string())])
        assert shared_arrow_type(STRUCT, other) is None
        assert shared_arrow_type(other, STRUCT) is None

    def test_nested_nullability_difference_gives_none(self) -> None:
        """A list item's non-null flag is part of the type, not its layout."""
        non_null_items = pa.list_(pa.field("item", pa.int8(), nullable=False))
        nullable_items = pa.large_list(pa.int8())
        assert shared_arrow_type(non_null_items, nullable_items) is None
        assert shared_arrow_type(nullable_items, non_null_items) is None


class TestDisagreeingInputs:
    """MergeJoin over inputs whose Arrow types differ for a shared column."""

    @pytest.mark.parametrize(
        ("left_type", "right_type", "values", "expected"), LAYOUT_PAIRS, ids=_ids
    )
    @pytest.mark.parametrize("rows", [0, 2])
    @pytest.mark.parametrize("order", ["LR", "RL"])
    def test_colliding_column_with_layout_difference_merges(
        self,
        left_type: pa.DataType,
        right_type: pa.DataType,
        values: list[Any],
        expected: pa.DataType,
        rows: int,
        order: str,
    ) -> None:
        """The merged column is ``large_list`` of the large variant, in both orders."""
        first, second = _ordered(*_pair(left_type, right_type, values, rows), order)
        op = MergeJoin()
        op.validate_inputs(first, second)
        result = op.static_process(first, second)
        table = result.as_table(columns={"source": True})
        assert _merged_types(table) == (pa.large_list(expected), SOURCE_LIST)
        assert table.column("x").to_pylist() == [values] * rows
        _assert_matches_prediction(op, result, first, second)

    @pytest.mark.parametrize(
        ("left_type", "right_type", "values"), MISMATCHED_PAIRS, ids=_ids
    )
    @pytest.mark.parametrize("rows", [0, 2])
    @pytest.mark.parametrize("order", ["LR", "RL"])
    def test_colliding_column_with_other_difference_raises(
        self,
        left_type: pa.DataType,
        right_type: pa.DataType,
        values: list[Any],
        rows: int,
        order: str,
    ) -> None:
        """Width, unit or precision differences raise before the join.

        Validation cannot see them (the Python types agree), so
        ``static_process`` raises as soon as it has the input tables.
        """
        first, second = _ordered(*_pair(left_type, right_type, values, rows), order)
        op = MergeJoin()
        op.validate_inputs(first, second)
        with pytest.raises(InputValidationError, match=r"'x'"):
            op.static_process(first, second)

    @pytest.mark.parametrize(
        ("left_type", "right_type", "values"), MISMATCHED_PAIRS, ids=_ids
    )
    @pytest.mark.parametrize("order", ["LR", "RL"])
    def test_disagreement_error_names_operator_column_and_both_types(
        self,
        left_type: pa.DataType,
        right_type: pa.DataType,
        values: list[Any],
        order: str,
    ) -> None:
        """The error names MergeJoin, the column and both input types as given."""
        first, second = _ordered(*_pair(left_type, right_type, values, 2), order)
        with pytest.raises(InputValidationError) as excinfo:
            _ = MergeJoin()(first, second).as_table()
        message = str(excinfo.value)
        assert message.startswith("MergeJoin inputs disagree")
        assert "'x'" in message
        assert str(left_type) in message
        assert str(right_type) in message

    @pytest.mark.parametrize(
        ("left_type", "right_type", "values", "expected"), LAYOUT_PAIRS[:2], ids=_ids
    )
    @pytest.mark.parametrize("rows", [0, 2])
    @pytest.mark.parametrize("order", ["LR", "RL"])
    def test_shared_tag_with_layout_difference_joins(
        self,
        left_type: pa.DataType,
        right_type: pa.DataType,
        values: list[Any],
        expected: pa.DataType,
        rows: int,
        order: str,
    ) -> None:
        """A shared tag key that differs only in layout takes the large variant."""
        left = _stream(
            {"k": (left_type, [values[0]] * rows), "p": (pa.int64(), [0] * rows)},
            ["k"],
        )
        right = _stream(
            {"k": (right_type, [values[0]] * rows), "q": (pa.int64(), [0] * rows)},
            ["k"],
        )
        first, second = _ordered(left, right, order)
        op = MergeJoin()
        result = op.static_process(first, second)
        table = result.as_table()
        assert table.schema.field("k").type == expected
        assert len(table) == rows * rows
        _assert_matches_prediction(op, result, first, second)

    @pytest.mark.parametrize(
        ("left_type", "right_type", "values"), MISMATCHED_PAIRS[:4], ids=_ids
    )
    @pytest.mark.parametrize("rows", [0, 2])
    @pytest.mark.parametrize("order", ["LR", "RL"])
    def test_shared_tag_with_other_difference_raises(
        self,
        left_type: pa.DataType,
        right_type: pa.DataType,
        values: list[Any],
        rows: int,
        order: str,
    ) -> None:
        """A shared tag key that differs in width, unit or precision raises."""
        left = _stream(
            {"k": (left_type, [values[0]] * rows), "p": (pa.int64(), [0] * rows)},
            ["k"],
        )
        right = _stream(
            {"k": (right_type, [values[0]] * rows), "q": (pa.int64(), [0] * rows)},
            ["k"],
        )
        first, second = _ordered(left, right, order)
        with pytest.raises(InputValidationError, match=r"'k'"):
            MergeJoin().static_process(first, second)

    def test_csv_and_dict_sources_on_a_string_column(self, tmp_path: Path) -> None:
        """A CSV ``string`` column colliding with a dict ``large_string`` one merges."""
        csv_path = tmp_path / "labels.csv"
        csv_path.write_text("id,label\n1,a\n2,b\n")
        csv_source = CSVSource(str(csv_path), tag_columns=["id"])
        dict_source = DictSource(
            [{"id": 1, "label": "x"}, {"id": 2, "label": "y"}], tag_columns=["id"]
        )
        assert csv_source.as_table().schema.field("label").type == pa.string()
        assert dict_source.as_table().schema.field("label").type == pa.large_string()

        table = MergeJoin()(csv_source, dict_source).as_table()
        assert table.schema.field("label").type == pa.large_list(pa.large_string())
        labels = table.sort_by("id").column("label").to_pylist()
        assert labels == [["a", "x"], ["b", "y"]]


def _sources(rows: int) -> tuple[StreamProtocol, StreamProtocol]:
    """Build two sources with source info and system tags; ``rows=0`` filters all.

    The left source has a ``string`` tag ``probe``, a colliding ``int8``
    column ``x`` and pass-through columns of several layouts; the right
    source has the same tag as ``large_string``.
    """
    left = ArrowTableSource(
        _table(
            {
                "probe": (pa.string(), ["p0", "p1"]),
                "x": (pa.int8(), [1, 2]),
                "s": (pa.string(), ["a", "b"]),
                "b": (pa.binary(), [b"a", b"b"]),
                "l": (pa.list_(pa.int8()), [[1], [2]]),
                "st": (STRUCT, [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]),
            }
        ),
        tag_columns=["probe"],
    )
    right = ArrowTableSource(
        _table(
            {
                "probe": (pa.large_string(), ["p0", "p1"]),
                "x": (pa.int8(), [3, 0]),
                "f": (pa.float32(), [1.0, 2.0]),
            }
        ),
        tag_columns=["probe"],
    )
    if rows:
        return left, right
    drop_all = PolarsFilter(predicates=[pl.lit(False)])
    return drop_all.static_process(left), drop_all.static_process(right)


class TestPassThroughColumns:
    """Columns the join does not merge take their large-normalized input type."""

    @pytest.mark.parametrize("rows", [0, 2])
    def test_merge_join_pass_through_types(self, rows: int) -> None:
        """Data, source-info, tag and system-tag columns: normalized input type.

        System-tag record ids stay ``large_binary``, the type a source gives
        them (ITL-942); ``float32`` keeps its width.
        """
        left, right = _sources(rows)
        op = MergeJoin()
        result = op.static_process(left, right)
        table = result.as_table(all_info=True)

        assert table.schema.field("s").type == pa.large_string()
        assert table.schema.field("b").type == pa.large_binary()
        assert table.schema.field("l").type == pa.large_list(pa.int8())
        assert table.schema.field("st").type == LARGE_STRUCT
        assert table.schema.field("f").type == pa.float32()
        assert table.schema.field("probe").type == pa.large_string()
        assert table.schema.field("x").type == pa.large_list(pa.int8())
        for name in ("_source_s", "_source_b", "_source_f"):
            assert table.schema.field(name).type == pa.large_string(), name
        assert table.schema.field("_source_x").type == SOURCE_LIST

        input_system_tag_types = {
            normalize_to_large_types(f.type)
            for stream in (left, right)
            for f in stream.as_table(all_info=True).schema
            if f.name.startswith("_tag")
        }
        output_system_tag_types = {
            f.type for f in table.schema if f.name.startswith("_tag")
        }
        assert output_system_tag_types == input_system_tag_types
        assert pa.large_binary() in output_system_tag_types
        assert len(table) == rows
        if rows:
            # The empty inputs come from PolarsFilter, which drops non-null
            # flags, so their prediction differs in nullability only (ITL-820).
            _assert_matches_prediction(op, result, left, right)

    def test_nullable_pass_through_keeps_nulls_and_nullability(self) -> None:
        """The final cast keeps a nullable column's nulls and each field's nullability.

        The tag is non-nullable and the pass-through data column is nullable
        and holds a null; after the join both flags and the values survive.
        """
        left = ArrowTableStream(
            pa.Table.from_arrays(
                [
                    pa.array([0, 1], pa.int64()),
                    pa.array(["a", None], pa.string()),
                ],
                schema=pa.schema(
                    [
                        pa.field("k", pa.int64(), nullable=False),
                        pa.field("s", pa.string(), nullable=True),
                    ]
                ),
            ),
            tag_columns=["k"],
        )
        right = _stream({"k": (pa.int64(), [0, 1]), "q": (pa.int8(), [5, 6])}, ["k"])
        table = MergeJoin().static_process(left, right).as_table().sort_by("k")

        assert table.schema.field("k").type == pa.int64()
        assert not table.schema.field("k").nullable
        assert table.schema.field("s").type == pa.large_string()
        assert table.schema.field("s").nullable
        assert table.column("s").to_pylist() == ["a", None]
        assert table.column("q").to_pylist() == [5, 6]

    @pytest.mark.parametrize("rows", [0, 1])
    def test_null_typed_columns_stay_null(self, rows: int) -> None:
        """``null`` is a type, not a layout: pass-through and merged columns keep it.

        With rows only the pass-through column is checked: merging non-empty
        null values hits the unorderable-values bug (ITL-839).
        """
        keys = pa.array(list(range(rows)), pa.int64())
        nulls = pa.array([None] * rows, pa.null())
        left_columns: dict[str, pa.Array[Any]] = {"k": keys, "z": nulls}
        right_columns: dict[str, pa.Array[Any]] = {
            "k": keys,
            "y": pa.array([1] * rows, pa.int8()),
        }
        if not rows:
            left_columns["x"] = nulls
            right_columns["x"] = nulls
        left, right = (
            ArrowTableStream(
                pa.Table.from_arrays(list(columns.values()), names=list(columns)),
                tag_columns=["k"],
            )
            for columns in (left_columns, right_columns)
        )
        op = MergeJoin()
        result = op.static_process(left, right)
        table = result.as_table()
        assert table.schema.field("z").type == pa.null()
        if not rows:
            assert table.schema.field("x").type == pa.large_list(pa.null())
        assert len(table) == rows
        _assert_matches_prediction(op, result, left, right)

    def test_merge_join_merged_values_and_sources_stay_sorted(self) -> None:
        """Merged values are sorted per row and their source tokens follow them."""
        left, right = _sources(2)
        table = MergeJoin().static_process(left, right).as_table(all_info=True)
        rows = table.sort_by("probe").to_pylist()
        assert [row["x"] for row in rows] == [[1, 3], [0, 2]]
        left_tokens = left.as_table(all_info=True).column("_source_x").to_pylist()
        right_tokens = right.as_table(all_info=True).column("_source_x").to_pylist()
        assert [row["_source_x"] for row in rows] == [
            [left_tokens[0], right_tokens[0]],
            [right_tokens[1], left_tokens[1]],
        ]

    def test_types_are_commutative(self) -> None:
        """Every column has the same type whichever input comes first."""
        left, right = _sources(2)
        op = MergeJoin()
        forward = op.static_process(left, right).as_table(all_info=True)
        backward = op.static_process(right, left).as_table(all_info=True)
        assert {f.name: f.type for f in forward.schema} == {
            f.name: f.type for f in backward.schema
        }

    def test_polars_filter_emits_normalized_input_types(self) -> None:
        """Filtering gives every column its large-normalized input type."""
        source, _ = _sources(2)
        op = PolarsFilter(predicates=[pl.col("x") > 1])
        result = op.static_process(source)
        input_schema = source.as_table(all_info=True).schema
        output_schema = result.as_table(all_info=True).schema

        assert len(result.as_table()) == 1
        for field in output_schema:
            expected = normalize_to_large_types(input_schema.field(field.name).type)
            assert field.type == expected, field.name
        assert output_schema.field("s").type == pa.large_string()
        assert output_schema.field("st").type == LARGE_STRUCT


def _row_hashes(stream: StreamProtocol) -> list[tuple[str, str]]:
    """Return each row's (tag content hash, data content hash)."""
    return [
        (tag.content_hash().to_hex(), data.content_hash().to_hex())
        for tag, data in stream.iter_data()
    ]


def _small_layout(arrow_type: pa.DataType) -> pa.DataType:
    """Return ``arrow_type`` with every large variant replaced by its small one."""
    if pa.types.is_large_string(arrow_type):
        return pa.string()
    if pa.types.is_large_binary(arrow_type):
        return pa.binary()
    if pa.types.is_large_list(arrow_type) or pa.types.is_list(arrow_type):
        field = arrow_type.value_field
        return pa.list_(field.with_type(_small_layout(field.type)))
    if pa.types.is_struct(arrow_type):
        return pa.struct([f.with_type(_small_layout(f.type)) for f in arrow_type])
    return arrow_type


def _relayout(
    stream: StreamProtocol, overrides: dict[str, pa.DataType] | None = None
) -> ArrowTableStream:
    """Rebuild ``stream`` in the small layouts, with optional per-column types.

    The small layouts are what MergeJoin emitted for a merged column of
    ``int64`` or ``string`` values before this change (``list<...>``).
    """
    overrides = overrides or {}
    table = stream.as_table(columns={"source": True, "system_tags": True})
    schema = pa.schema(
        [
            field.with_type(overrides.get(field.name, _small_layout(field.type)))
            for field in table.schema
        ]
    )
    tag_columns, _ = stream.keys()
    return ArrowTableStream(table.cast(schema), tag_columns=tag_columns)


def _stream_hash(stream: StreamProtocol) -> str:
    """Return the content hash of ``stream``'s full table."""
    table = stream.as_table(columns={"source": True, "system_tags": True})
    return ArrowTableStream(table, tag_columns=stream.keys()[0]).content_hash().to_hex()


class TestContentHashes:
    """Large normalization changes no hash; keeping a narrow width does."""

    def test_normalization_never_changes_a_hash(self) -> None:
        """Ask the hasher: each type and its large variant hash the same.

        Both the schema hash and the hash of the same values agree, while
        ``int8`` and ``int64`` differ in both.
        """
        hasher = _starfix_hasher()
        cases: list[tuple[pa.DataType, list[Any]]] = [
            (pa.string(), ["a"]),
            (pa.binary(), [b"a"]),
            (pa.list_(pa.int64()), [[1]]),
            (pa.list_(pa.string()), [["a"]]),
            (STRUCT, [{"a": 1, "b": "x"}]),
            (pa.struct([pa.field("b", pa.string(), nullable=False)]), [{"b": "q"}]),
            (pa.list_(pa.field("element", pa.int64())), [[1]]),
        ]
        for arrow_type, values in cases:
            large = normalize_to_large_types(arrow_type)
            assert large != arrow_type
            assert hasher.hash_schema(
                pa.schema([pa.field("x", arrow_type)])
            ) == hasher.hash_schema(pa.schema([pa.field("x", large)])), arrow_type
            assert hasher.hash_table(
                pa.table({"x": pa.array(values, type=arrow_type)})
            ) == hasher.hash_table(pa.table({"x": pa.array(values, type=large)})), (
                arrow_type
            )

        # The hasher has no value hashing for maps; their schema hash agrees.
        map_type = pa.map_(pa.string(), pa.int64())
        assert hasher.hash_schema(
            pa.schema([pa.field("x", map_type)])
        ) == hasher.hash_schema(
            pa.schema([pa.field("x", normalize_to_large_types(map_type))])
        )

        narrow = pa.schema([pa.field("x", pa.int8())])
        wide = pa.schema([pa.field("x", pa.int64())])
        assert hasher.hash_schema(narrow) != hasher.hash_schema(wide)
        assert hasher.hash_table(
            pa.table({"x": pa.array([1], pa.int8())})
        ) != hasher.hash_table(pa.table({"x": pa.array([1], pa.int64())}))

    def test_non_nullable_list_items_keep_their_hash(self) -> None:
        """Normalizing ``list<item not null>`` keeps the flag, and so its hashes.

        ``normalize_to_large_types`` used to rebuild a list's value field as
        ``large_list(element_type)``, dropping the item's non-null flag, which
        the Arrow hasher includes in the schema hash (ITL-842 audits the wider
        question). A merged column of such lists keeps the flag too.
        """
        hasher = _starfix_hasher()
        strict_items = pa.list_(pa.field("item", pa.int64(), nullable=False))
        large = normalize_to_large_types(strict_items)
        assert large == pa.large_list(pa.field("item", pa.int64(), nullable=False))
        assert isinstance(large, pa.LargeListType)
        assert not large.value_field.nullable
        assert hasher.hash_schema(
            pa.schema([pa.field("x", strict_items)])
        ) == hasher.hash_schema(pa.schema([pa.field("x", large)]))
        assert hasher.hash_table(
            pa.table({"x": pa.array([[1]], type=strict_items)})
        ) == hasher.hash_table(pa.table({"x": pa.array([[1]], type=large)}))

        result = MergeJoin().static_process(
            *_pair(strict_items, strict_items, [[1], [2]], 2)
        )
        assert result.as_table().schema.field("x").type == pa.large_list(large)

    @pytest.mark.parametrize("arrow_type", [pa.int64(), pa.string()], ids=str)
    def test_layout_change_keeps_merge_join_hashes(
        self, arrow_type: pa.DataType
    ) -> None:
        """An ``int64`` or ``string`` merge hashes as in the old ``list`` layout.

        Before this change the merged column was ``list<int64>`` or
        ``list<string>`` and pass-through columns kept their input layout;
        rows and the whole stream hash the same either way.
        """
        values = [1, 2] if arrow_type == pa.int64() else ["a", "b"]
        result = MergeJoin().static_process(*_pair(arrow_type, arrow_type, values, 2))
        old_layout = _relayout(result)
        assert pa.types.is_list(old_layout.as_table().schema.field("x").type)
        assert _row_hashes(result) == _row_hashes(old_layout)
        assert _stream_hash(result) == _stream_hash(old_layout)

    def test_layout_change_keeps_pass_through_hashes(self) -> None:
        """Pass-through columns of several layouts hash as in their input layout."""
        left, right = _sources(2)
        result = MergeJoin().static_process(left, right)
        assert _row_hashes(result) == _row_hashes(_relayout(result))
        filtered = PolarsFilter(predicates=[pl.lit(True)]).static_process(left)
        assert _row_hashes(filtered) == _row_hashes(_relayout(filtered))

    def test_narrow_width_changes_the_hash(self) -> None:
        """An ``int8`` merge no longer widens to ``int64``, so its hash changes.

        Before this change Arrow inferred ``list<int64>`` from the Python
        values; the merged column is now ``large_list<int8>``. This is the one
        intended hash change of ITL-832.
        """
        result = MergeJoin().static_process(*_pair(pa.int8(), pa.int8(), [1, 2], 2))
        assert result.as_table().schema.field("x").type == pa.large_list(pa.int8())
        widened = _relayout(result, {"x": pa.list_(pa.int64())})
        assert [data for _, data in _row_hashes(result)] != [
            data for _, data in _row_hashes(widened)
        ]
        assert [tag for tag, _ in _row_hashes(result)] == [
            tag for tag, _ in _row_hashes(widened)
        ]
