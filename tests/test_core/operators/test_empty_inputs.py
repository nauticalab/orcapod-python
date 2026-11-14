"""Zero-row input behaviour for every operator (ITL-813).

An operator fed a stream with no rows must return a zero-row stream whose
schema equals the operator's predicted ``output_schema()``. The motivating
failure: ``pa.array([0] * 0)`` is ``null``-typed, so a join helper key built
that way does not match the ``int64`` key of a non-empty side and Polars
refuses the join.

Predictions are compared against the materialized stream returned by
``static_process``, never against ``process(...)``, whose ``output_schema``
delegates straight back to the operator and would make the check circular.
"""

from __future__ import annotations

import types
from typing import Any, Union, get_args, get_origin

import polars as pl
import pyarrow as pa
import pytest

from orcapod.core.operators import (
    Batch,
    DropColumns,
    DropDataColumns,
    DropTagColumns,
    GroupBy,
    Index,
    Join,
    MapData,
    MapTags,
    MergeJoin,
    Pick,
    PolarsFilter,
    SelectColumns,
    SelectDataColumns,
    SelectTagColumns,
    SemiJoin,
)
from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.system_constants import constants
from orcapod.types import SourceInfoValue
from orcapod.utils.arrow_utils import normalize_to_large_types

INT = pa.int64()
STR = pa.large_string()

SCHEMA_CONFIGS: tuple[dict[str, bool], ...] = ({}, {"system_tags": True})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _stream(
    columns: dict[str, tuple[pa.DataType, list[Any]]],
    tags: list[str],
    *,
    nullable: bool = True,
    source_info: dict[str, SourceInfoValue] | None = None,
) -> ArrowTableStream:
    """Build a stream from ``{name: (arrow_type, values)}``.

    Pass an empty ``values`` list for every column to get a zero-row stream.
    ``source_info`` maps data columns to a source-info value; columns left out
    get a null one.
    """
    table = pa.Table.from_arrays(
        [pa.array(values, type=arrow_type) for arrow_type, values in columns.values()],
        schema=pa.schema(
            [
                pa.field(name, arrow_type, nullable=nullable)
                for name, (arrow_type, _) in columns.items()
            ]
        ),
    )
    return ArrowTableStream(table, tag_columns=tags, source_info=source_info)


def _int_stream(
    tags: list[str], data: list[str], rows: int, *, nullable: bool = True
) -> ArrowTableStream:
    """Build an all-``int64`` stream with ``rows`` rows (``0`` for empty).

    Tag values are ``0..rows-1`` so streams built with the same ``rows`` share
    every tag value.
    """
    values = list(range(rows))
    return _stream(
        {name: (INT, values) for name in [*tags, *data]}, tags, nullable=nullable
    )


def _filtered_empty(tags: list[str], data: list[str]) -> StreamProtocol:
    """Build an empty stream the way a pipeline does: a filter dropping every row.

    Unlike a hand-built zero-row table, the result carries system-tag and
    source-info columns, which the join operators rename and reorder.
    """
    table = pa.table({name: pa.array([1, 2], type=INT) for name in [*tags, *data]})
    source = ArrowTableSource(table, tag_columns=tags)
    empty = PolarsFilter(predicates=[pl.lit(False)]).static_process(source)
    assert len(empty.as_table()) == 0
    return empty


def _ignoring_nullability(schema: Any) -> dict[str, frozenset[Any]]:
    """Map each field of ``schema`` to its type with ``| None`` stripped.

    Lets two schemas be compared on field names and types while tolerating a
    difference in nullability only.

    Args:
        schema: A ``Schema`` (field name to Python type).

    Returns:
        Field name to the set of non-``None`` members of its type.
    """
    normalized = {}
    for name, python_type in schema.items():
        is_union = get_origin(python_type) in (Union, types.UnionType)
        members = get_args(python_type) if is_union else (python_type,)
        normalized[name] = frozenset(members) - {type(None)}
    return normalized


def _assert_empty_result(
    op: Any,
    streams: list[Any],
    expected_columns: set[str],
    schema_configs: tuple[dict[str, bool], ...] = SCHEMA_CONFIGS,
) -> Any:
    """Assert ``op`` over ``streams`` yields zero rows with the predicted schema.

    Args:
        op: The operator under test.
        streams: Its input streams.
        expected_columns: Expected user-facing (tag and data) column names.
        schema_configs: Column configs under which the materialized schema
            must equal the prediction.

    Returns:
        The materialized result stream, for further assertions.
    """
    op.validate_inputs(*streams)
    result = op.static_process(*streams)

    assert set(result.as_table().column_names) == expected_columns

    for config in schema_configs:
        table = result.as_table(columns=config)
        assert len(table) == 0
        null_typed = [f.name for f in table.schema if pa.types.is_null(f.type)]
        assert null_typed == [], f"null-typed columns {null_typed} with {config}"
        assert result.output_schema(columns=config) == op.output_schema(
            *streams, columns=config
        ), f"materialized schema differs from prediction with {config}"

    # Source-info columns are not part of output_schema, but must be typed too.
    with_source = result.as_table(columns={"source": True, "system_tags": True})
    assert [f.name for f in with_source.schema if pa.types.is_null(f.type)] == []

    # The public entry point must agree.
    assert len(op.process(*streams).as_table()) == 0
    return result


# ---------------------------------------------------------------------------
# Join
# ---------------------------------------------------------------------------


class TestJoinEmptyInput:
    """``Join`` over two or three streams of which at least one has no rows."""

    def test_issue_snippet(self):
        """The reproduction from ITL-813, verbatim."""
        full = ArrowTableStream(
            pa.table({"probe": [2, 3], "n_units": [40, 0]}), tag_columns=["probe"]
        )
        empty = ArrowTableStream(
            pa.table(
                {"probe": pa.array([], pa.int64()), "bins": pa.array([], pa.int64())}
            ),
            tag_columns=["probe"],
        )
        table = Join()(full, empty).as_table()
        assert len(table) == 0
        assert set(table.column_names) == {"probe", "n_units", "bins"}

    @pytest.mark.parametrize(
        "empty_positions",
        [(0,), (1,), (0, 1)],
        ids=["first-empty", "second-empty", "all-empty"],
    )
    def test_two_streams(self, empty_positions):
        """Two inputs, with the first, the second, or both empty."""
        data_columns = ["n_units", "bins"]
        streams = [
            _int_stream(["probe"], [col], 0 if i in empty_positions else 2)
            for i, col in enumerate(data_columns)
        ]
        _assert_empty_result(Join(), streams, {"probe", *data_columns})

    @pytest.mark.parametrize(
        "empty_positions",
        [(0,), (1,), (2,), (0, 2), (0, 1, 2)],
        ids=["first-empty", "second-empty", "third-empty", "two-empty", "all-empty"],
    )
    def test_three_streams(self, empty_positions):
        """Three inputs, with one, two, or all of them empty."""
        data_columns = ["n_units", "bins", "depth"]
        streams = [
            _int_stream(["probe"], [col], 0 if i in empty_positions else 2)
            for i, col in enumerate(data_columns)
        ]
        _assert_empty_result(Join(), streams, {"probe", *data_columns})

    def test_no_shared_tags(self):
        """With no shared tag the helper key is the only join key."""
        streams = [
            _int_stream(["probe"], ["n_units"], 2),
            _int_stream(["session"], ["bins"], 0),
        ]
        _assert_empty_result(Join(), streams, {"probe", "session", "n_units", "bins"})

    @pytest.mark.parametrize("n_streams", [2, 3])
    def test_empty_streams_carrying_system_tags(self, n_streams):
        """Empty streams produced by a filter keep their system-tag columns."""
        data_columns = ["n_units", "bins", "depth"][:n_streams]
        streams = [_filtered_empty(["probe"], [col]) for col in data_columns]
        result = _assert_empty_result(Join(), streams, {"probe", *data_columns})
        tag_schema, _ = result.output_schema(columns={"system_tags": True})
        assert len(tag_schema) > 1, "expected system-tag columns in the result"


# ---------------------------------------------------------------------------
# MergeJoin
# ---------------------------------------------------------------------------

MERGE_SIDES = {
    "left-empty": (0, 2),
    "right-empty": (2, 0),
    "both-empty": (0, 0),
}


class TestMergeJoinEmptyInput:
    """``MergeJoin`` with the left, the right, or both inputs empty."""

    def test_issue_snippet(self):
        """The ``MergeJoin`` reproduction from the ITL-813 correction."""
        full = ArrowTableStream(
            pa.table({"probe": [2, 3], "n_units": [40, 0]}), tag_columns=["probe"]
        )
        empty = ArrowTableStream(
            pa.table(
                {"probe": pa.array([], pa.int64()), "bins": pa.array([], pa.int64())}
            ),
            tag_columns=["probe"],
        )
        table = MergeJoin()(full, empty).as_table()
        assert len(table) == 0
        assert set(table.column_names) == {"probe", "n_units", "bins"}

    @pytest.mark.parametrize("rows", MERGE_SIDES.values(), ids=MERGE_SIDES.keys())
    def test_non_colliding_columns(self, rows):
        """Without a colliding data column every output column stays scalar."""
        left_rows, right_rows = rows
        streams = [
            _int_stream(["probe"], ["n_units"], left_rows),
            _int_stream(["probe"], ["bins"], right_rows),
        ]
        _assert_empty_result(MergeJoin(), streams, {"probe", "n_units", "bins"})

    @pytest.mark.parametrize("rows", MERGE_SIDES.values(), ids=MERGE_SIDES.keys())
    def test_colliding_column_becomes_list(self, rows):
        """A colliding data column is ``list[T]`` even when no row survives."""
        left_rows, right_rows = rows
        streams = [
            _int_stream(["probe"], ["n_units", "bins"], left_rows, nullable=False),
            _int_stream(["probe"], ["n_units", "depth"], right_rows, nullable=False),
        ]
        result = _assert_empty_result(
            MergeJoin(), streams, {"probe", "n_units", "bins", "depth"}
        )
        _, data_schema = result.output_schema()
        assert data_schema["n_units"] == list[int]
        assert data_schema["bins"] is int
        assert data_schema["depth"] is int
        n_units_type = result.as_table().schema.field("n_units").type
        assert pa.types.is_list(n_units_type) or pa.types.is_large_list(n_units_type)
        assert n_units_type.value_type == INT

    @pytest.mark.parametrize("rows", MERGE_SIDES.values(), ids=MERGE_SIDES.keys())
    def test_colliding_string_column(self, rows):
        """A colliding non-numeric column is typed ``list[str]`` with no rows."""
        left_rows, right_rows = rows

        def make(n: int) -> ArrowTableStream:
            """Build a stream of ``n`` rows with an ``int`` tag and a ``str`` column."""
            return _stream(
                {
                    "probe": (INT, list(range(n))),
                    "label": (STR, [f"v{i}" for i in range(n)]),
                },
                ["probe"],
                nullable=False,
            )

        result = _assert_empty_result(
            MergeJoin(), [make(left_rows), make(right_rows)], {"probe", "label"}
        )
        _, data_schema = result.output_schema()
        assert data_schema["label"] == list[str]

    @pytest.mark.parametrize(
        ("arrow_type", "values"),
        [
            (pa.int8(), [1, 2]),
            (INT, [1, 2]),
            (pa.float32(), [1.5, 2.5]),
            (pa.string(), ["a", "b"]),
            (STR, ["a", "b"]),
            (pa.large_binary(), [b"a", b"b"]),
            (pa.bool_(), [False, True]),
            (pa.large_list(pa.int8()), [[1], [2]]),
        ],
        ids=lambda v: str(v) if isinstance(v, pa.DataType) else "",
    )
    def test_colliding_column_type_matches_non_empty_result(self, arrow_type, values):
        """Empty and non-empty results type a merged column ``large_list<input>``.

        The element is the input's Arrow type in its large variants (width
        kept), never what Arrow infers from Python values (ITL-832), so empty
        and non-empty results of the same join can be concatenated. This holds
        for the merged data column and for its source-info column, which is
        ``large_list<large_string>``.
        """

        def merged_type(rows: int) -> tuple[pa.DataType, pa.DataType]:
            """Return the merged data and source-info Arrow types for ``rows`` rows."""
            streams = [
                _stream(
                    {
                        "probe": (INT, list(range(rows))),
                        "x": (arrow_type, values[:rows]),
                    },
                    ["probe"],
                    nullable=False,
                    source_info={"x": "origin"},
                )
                for _ in range(2)
            ]
            table = (
                MergeJoin().static_process(*streams).as_table(columns={"source": True})
            )
            return (
                table.schema.field("x").type,
                table.schema.field(f"{constants.SOURCE_PREFIX}x").type,
            )

        assert merged_type(0) == merged_type(2)
        assert merged_type(0) == (
            pa.large_list(normalize_to_large_types(arrow_type)),
            pa.large_list(pa.large_string()),
        )

    def test_no_shared_tags(self):
        """With no shared tag the helper key is the only join key."""
        streams = [
            _int_stream(["probe"], ["n_units"], 2),
            _int_stream(["session"], ["bins"], 0),
        ]
        _assert_empty_result(
            MergeJoin(), streams, {"probe", "session", "n_units", "bins"}
        )

    def test_empty_streams_carrying_system_tags(self):
        """Empty streams produced by a filter keep their system-tag columns."""
        streams = [
            _filtered_empty(["probe"], ["n_units"]),
            _filtered_empty(["probe"], ["bins"]),
        ]
        op = MergeJoin()
        # Only the default config is compared in full. With system tags the
        # materialized fields are non-nullable while the prediction keeps the
        # inputs' nullable flags; that mismatch also occurs with rows present
        # and is reported on ITL-813 rather than fixed there. Names and types
        # are still compared below, ignoring nullability only.
        result = _assert_empty_result(
            op, streams, {"probe", "n_units", "bins"}, schema_configs=({},)
        )
        config = {"system_tags": True}
        tag_schema, _ = result.output_schema(columns=config)
        predicted_tag_schema, _ = op.output_schema(*streams, columns=config)
        assert _ignoring_nullability(tag_schema) == _ignoring_nullability(
            predicted_tag_schema
        )
        assert len(tag_schema) > 1, "expected system-tag columns in the result"
        assert len(result.as_table(columns=config)) == 0


# ---------------------------------------------------------------------------
# Every other operator
# ---------------------------------------------------------------------------


def _empty_dict_stream() -> ArrowTableStream:
    """Build a zero-row stream with a map-valued data column (for ``Pick``)."""
    return _stream(
        {"probe": (INT, []), "stats": (pa.map_(STR, INT), [])},
        ["probe"],
        nullable=False,
    )


def _empty_list_stream() -> ArrowTableStream:
    """Build a zero-row stream with a list-valued data column (for ``Index``)."""
    return _stream(
        {"probe": (INT, []), "spikes": (pa.large_list(INT), [])},
        ["probe"],
        nullable=False,
    )


# (id, operator factory, streams factory, expected default columns)
OTHER_OPERATOR_CASES: list[tuple[str, Any, Any, set[str]]] = [
    (
        "SemiJoin-right-empty",
        SemiJoin,
        lambda: [
            _int_stream(["probe"], ["n_units"], 2),
            _int_stream(["probe"], ["bins"], 0),
        ],
        {"probe", "n_units"},
    ),
    (
        "SemiJoin-left-empty",
        SemiJoin,
        lambda: [
            _int_stream(["probe"], ["n_units"], 0),
            _int_stream(["probe"], ["bins"], 2),
        ],
        {"probe", "n_units"},
    ),
    (
        "SemiJoin-both-empty",
        SemiJoin,
        lambda: [
            _int_stream(["probe"], ["n_units"], 0),
            _int_stream(["probe"], ["bins"], 0),
        ],
        {"probe", "n_units"},
    ),
    (
        "Batch-single-batch",
        Batch,
        lambda: [_int_stream(["probe"], ["n_units"], 0, nullable=False)],
        {"probe", "n_units"},
    ),
    (
        "Batch-size-2",
        lambda: Batch(batch_size=2),
        lambda: [_int_stream(["probe"], ["n_units"], 0, nullable=False)],
        {"probe", "n_units"},
    ),
    (
        "Batch-size-2-drop-partial",
        lambda: Batch(batch_size=2, drop_partial_batch=True),
        lambda: [_int_stream(["probe"], ["n_units"], 0, nullable=False)],
        {"probe", "n_units"},
    ),
    (
        "GroupBy",
        lambda: GroupBy(by=["probe"]),
        lambda: [_int_stream(["probe", "session"], ["n_units"], 0, nullable=False)],
        # The non-key tag ``session`` is dropped (ITL-1070).
        {"probe", "n_units"},
    ),
    (
        "SelectTagColumns",
        lambda: SelectTagColumns(["probe"]),
        lambda: [_int_stream(["probe", "session"], ["n_units"], 0)],
        {"probe", "n_units"},
    ),
    (
        "DropTagColumns",
        lambda: DropTagColumns(["session"]),
        lambda: [_int_stream(["probe", "session"], ["n_units"], 0)],
        {"probe", "n_units"},
    ),
    (
        "SelectDataColumns",
        lambda: SelectDataColumns(["n_units"]),
        lambda: [_int_stream(["probe"], ["n_units", "bins"], 0)],
        {"probe", "n_units"},
    ),
    (
        "DropDataColumns",
        lambda: DropDataColumns(["bins"]),
        lambda: [_int_stream(["probe"], ["n_units", "bins"], 0)],
        {"probe", "n_units"},
    ),
    (
        "DropColumns",
        lambda: DropColumns(["session", "bins"]),
        lambda: [_int_stream(["probe", "session"], ["n_units", "bins"], 0)],
        {"probe", "n_units"},
    ),
    (
        "SelectColumns",
        lambda: SelectColumns(["probe", "n_units"]),
        lambda: [_int_stream(["probe", "session"], ["n_units", "bins"], 0)],
        {"probe", "n_units"},
    ),
    (
        "MapTags",
        lambda: MapTags({"probe": "electrode"}),
        lambda: [_int_stream(["probe"], ["n_units"], 0)],
        {"electrode", "n_units"},
    ),
    (
        "MapData",
        lambda: MapData({"n_units": "count"}),
        lambda: [_int_stream(["probe"], ["n_units"], 0)],
        {"probe", "count"},
    ),
    (
        "PolarsFilter-empty-input",
        lambda: PolarsFilter(predicates=[pl.col("n_units") > 0]),
        lambda: [_int_stream(["probe"], ["n_units"], 0)],
        {"probe", "n_units"},
    ),
    (
        "PolarsFilter-constraint-empty-input",
        lambda: PolarsFilter(constraints={"probe": 1}),
        lambda: [_int_stream(["probe"], ["n_units"], 0)],
        {"probe", "n_units"},
    ),
    (
        "PolarsFilter-drops-every-row",
        lambda: PolarsFilter(predicates=[pl.col("n_units") > 100]),
        lambda: [_int_stream(["probe"], ["n_units"], 2)],
        {"probe", "n_units"},
    ),
    (
        "Pick",
        lambda: Pick("stats", "mean"),
        lambda: [_empty_dict_stream()],
        {"probe", "stats"},
    ),
    (
        "Index",
        lambda: Index("spikes", 0),
        lambda: [_empty_list_stream()],
        {"probe", "spikes"},
    ),
]


class TestOtherOperatorsEmptyInput:
    """No other operator builds a helper column; this pins that they stay safe.

    Input nullability is chosen per operator to stay clear of two mismatches
    between predicted and materialized schemas that are not specific to empty
    inputs (reported on ITL-813, not fixed there): ``PolarsFilter`` drops
    ``nullable=False`` on its Polars round trip, and the aggregating operators
    predict ``list[T | None]`` for a nullable column but materialize ``list[T]``.
    """

    @pytest.mark.parametrize(
        ("make_op", "make_streams", "expected_columns"),
        [case[1:] for case in OTHER_OPERATOR_CASES],
        ids=[case[0] for case in OTHER_OPERATOR_CASES],
    )
    def test_zero_rows_with_predicted_schema(
        self, make_op, make_streams, expected_columns
    ):
        """Each operator returns zero rows with its predicted schema."""
        _assert_empty_result(make_op(), make_streams(), expected_columns)
