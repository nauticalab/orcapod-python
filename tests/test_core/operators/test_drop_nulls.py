"""``DropNulls``: drop rows with a null in a named column, narrow ``T | None`` to ``T`` (ITL-943).

The operator keeps the rows where every named column is non-null and reports each
named column as non-nullable afterwards, in ``output_schema()`` and in the Arrow
table. Everything else passes through: other columns' types and nullability,
system tags, source info and row order.
"""

from __future__ import annotations

import math
import typing
from collections.abc import Sequence
from pathlib import Path
from typing import Any, TypeAlias

import numpy as np
import pyarrow as pa
import pytest

from orcapod import function_pod
from orcapod.core.sources import ArrowTableSource, DictSource
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.operators import DropNulls, Join
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.system_constants import constants

Column: TypeAlias = "pa.Array[Any] | pa.ChunkedArray[Any]"

ALL = {"source": True, "system_tags": True}


def _source(
    columns: dict[str, Column],
    tags: Sequence[str] = ("id",),
    nullable: Sequence[str] = (),
    source_id: str = "src",
) -> ArrowTableSource:
    """Build a source; every column not listed in ``nullable`` is non-nullable."""
    table = pa.table(list(columns.values()), names=list(columns))
    schema = pa.schema([f.with_nullable(f.name in nullable) for f in table.schema])
    return ArrowTableSource(
        table.cast(schema), tag_columns=list(tags), source_id=source_id
    )


def readings(nullable: Sequence[str] = ("x", "y")) -> ArrowTableSource:
    """Four rows; ``x`` is null in row 2, ``y`` in row 3; ``note`` is nullable too."""
    return _source(
        {
            "id": pa.array([1, 2, 3, 4], pa.int16()),
            "x": pa.array([10, None, 30, 40], pa.int8()),
            "y": pa.array([1.5, 2.5, None, 4.5], pa.float32()),
            "note": pa.array(["a", None, "c", None], pa.string()),
        },
        nullable=[*nullable, "note"],
    )


def _ids(stream: StreamProtocol) -> list[Any]:
    """Return the ``id`` column of ``stream`` as a list, in row order."""
    return stream.as_table().column("id").to_pylist()


# ── rows ────────────────────────────────────────────────────────────────────


def test_drops_rows_where_the_column_is_null() -> None:
    """Rows whose named column is null are dropped; the others pass through."""
    result = DropNulls("x")(readings())
    assert result.as_table().to_pylist() == [
        {"id": 1, "x": 10, "y": 1.5, "note": "a"},
        {"id": 3, "x": 30, "y": None, "note": "c"},
        {"id": 4, "x": 40, "y": 4.5, "note": None},
    ]


def test_every_named_column_must_be_non_null() -> None:
    """With several names, a row survives only if all of them are non-null."""
    assert _ids(DropNulls(["x", "y"])(readings())) == [1, 4]


def test_row_order_is_preserved() -> None:
    """Surviving rows keep the input order; nothing is sorted."""
    source = _source(
        {
            "id": pa.array([4, 1, 3, 2], pa.int16()),
            "x": pa.array([40, 10, None, 20], pa.int8()),
        },
        nullable=["x"],
    )
    assert [tag["id"] for tag, _ in DropNulls("x")(source).iter_data()] == [4, 1, 2]


def test_nan_is_not_null() -> None:
    """A NaN is a value: its row survives and the column still becomes non-nullable."""
    source = _source(
        {
            "id": pa.array([1, 2, 3], pa.int16()),
            "v": pa.array([1.0, float("nan"), None], pa.float64()),
        },
        nullable=["v"],
    )
    result = DropNulls("v")(source)
    first, second = result.as_table().column("v").to_pylist()
    assert first == 1.0
    assert isinstance(second, float) and math.isnan(second)
    assert result.as_table().schema.field("v").nullable is False
    assert result.output_schema()[1]["v"] is float


# ── types ───────────────────────────────────────────────────────────────────


def test_output_schema_narrows_only_the_named_columns() -> None:
    """``output_schema()`` reports ``T`` for the named column and leaves the rest."""
    source = readings()
    assert source.output_schema()[1] == {
        "x": int | None,
        "y": float | None,
        "note": str | None,
    }
    tag_schema, data_schema = DropNulls("x")(source).output_schema()
    assert tag_schema == {"id": int}
    assert data_schema == {"x": int, "y": float | None, "note": str | None}


def test_table_narrows_only_the_named_columns() -> None:
    """Only the named field loses its nullable flag; widths and other flags are kept."""
    source = readings()
    before = source.as_table(columns=ALL).schema
    after = DropNulls("x")(source).as_table(columns=ALL).schema
    assert after.names == before.names
    assert after.field("x") == pa.field("x", pa.int8(), nullable=False)
    # Width and nullability are kept; ``string`` takes its large variant.
    assert after.field("y") == pa.field("y", pa.float32(), nullable=True)
    assert after.field("note") == pa.field("note", pa.large_string(), nullable=True)
    for name in before.names:
        if name not in ("x", "note"):
            assert after.field(name) == before.field(name)


def test_schema_prediction_matches_the_computed_stream() -> None:
    """The predicted schema equals the schema of the computed stream."""
    source = readings()
    op = DropNulls(["x", "id"])
    for config in ({}, {"system_tags": True}, {"source": True, "system_tags": True}):
        # Non-public on purpose: a prediction is verified against the static result.
        computed = op.static_process(source).output_schema(columns=config)
        assert op(source).output_schema(columns=config) == computed
        assert op.output_schema(source, columns=config) == computed


def test_zero_surviving_rows_keep_the_narrowed_types() -> None:
    """A result with no rows has the schema of a non-empty result."""
    source = _source(
        {
            "id": pa.array([1, 2], pa.int16()),
            "x": pa.array([None, None], pa.int8()),
            "s": pa.array(["a", "b"], pa.string()),
        },
        nullable=["x", "s"],
    )
    non_empty = _source(
        {
            "id": pa.array([1, 2], pa.int16()),
            "x": pa.array([5, None], pa.int8()),
            "s": pa.array(["a", "b"], pa.string()),
        },
        nullable=["x", "s"],
    )
    result = DropNulls("x")(source)
    table = result.as_table(columns=ALL)
    assert table.num_rows == 0
    assert list(result.iter_data()) == []
    assert table.schema.field("x") == pa.field("x", pa.int8(), nullable=False)
    assert result.output_schema() == ({"id": int}, {"x": int, "s": str | None})
    expected = DropNulls("x")(non_empty).as_table(columns=ALL).schema
    assert table.schema == expected


def test_already_non_nullable_column_is_a_no_op() -> None:
    """Naming a non-nullable column changes no schema, row or hash."""
    source = _source(
        {
            "id": pa.array([1, 2], pa.int16()),
            "x": pa.array([10, 20], pa.int8()),
        }
    )
    result = DropNulls("x")(source)
    assert result.output_schema() == source.output_schema()
    assert result.as_table(columns=ALL) == source.as_table(columns=ALL)
    for (tag, data), (in_tag, in_data) in zip(result.iter_data(), source.iter_data()):
        assert tag.content_hash() == in_tag.content_hash()
        assert data.content_hash() == in_data.content_hash()


def test_applying_twice_equals_applying_once() -> None:
    """The operator is idempotent."""
    once = DropNulls("x")(readings())
    twice = DropNulls("x")(once)
    assert twice.as_table(columns=ALL) == once.as_table(columns=ALL)
    assert twice.output_schema() == once.output_schema()


# ── tag columns ─────────────────────────────────────────────────────────────


def keyed(nullable: Sequence[str] = ("site", "x")) -> ArrowTableSource:
    """Tags ``site`` (null in row 2) and ``id``; ``x`` is null in row 3."""
    return _source(
        {
            "site": pa.array(["a", None, "c"], pa.large_string()),
            "id": pa.array([1, 2, 3], pa.int16()),
            "x": pa.array([10, 20, None], pa.int8()),
        },
        tags=("site", "id"),
        nullable=nullable,
    )


def test_tag_column_is_narrowed() -> None:
    """A named tag column is tested and narrowed like a data column."""
    source = keyed()
    assert source.output_schema()[0] == {"site": str | None, "id": int}
    result = DropNulls("site")(source)
    assert _ids(result) == [1, 3]
    assert result.keys() == source.keys()
    assert result.output_schema() == (
        {"site": str, "id": int},
        {"x": int | None},
    )
    assert result.as_table().schema.field("site").nullable is False
    assert result.as_table().schema.field("x").nullable is True


def test_tag_and_data_columns_in_one_call() -> None:
    """Tag and data columns can be named together."""
    result = DropNulls(["x", "site"])(keyed())
    assert _ids(result) == [1]
    assert result.output_schema() == ({"site": str, "id": int}, {"x": int})


def test_narrowed_nullable_key_joins_with_a_non_nullable_key() -> None:
    """A source whose key columns are nullable by specification (as a Spiral table's
    are) joins a source whose keys are ``T`` as it does after ``DropNulls``: a null
    key matches nothing, so the join gives the key ``T`` either way (ITL-829)."""
    events = _source(
        {
            "session": pa.array(["s1", None, "s3"], pa.large_string()),
            "start": pa.array([100, 200, 300], pa.int64()),
        },
        tags=("session",),
        nullable=["session", "start"],
        source_id="events",
    )
    sessions = _source(
        {
            "session": pa.array(["s1", "s3", "s4"], pa.large_string()),
            "animal": pa.array(["m1", "m3", "m4"], pa.large_string()),
        },
        tags=("session",),
        source_id="sessions",
    )
    for joined in (
        Join()(events, sessions),
        Join()(DropNulls("session")(events), sessions),
    ):
        assert joined.output_schema() == (
            {"session": str},
            {"start": int | None, "animal": str},
        )
        assert not joined.as_table().schema.field("session").nullable
        rows = sorted(joined.as_table().to_pylist(), key=lambda r: r["session"])
        assert rows == [
            {"session": "s1", "start": 100, "animal": "m1"},
            {"session": "s3", "start": 300, "animal": "m3"},
        ]


def test_stream_without_tag_columns() -> None:
    """A stream with no tag columns is filtered and narrowed the same way."""
    source = _source(
        {
            "x": pa.array([1, None, 3], pa.int8()),
            "s": pa.array(["a", "b", "c"], pa.string()),
        },
        tags=(),
        nullable=["x", "s"],
    )
    result = DropNulls("x")(source)
    assert result.as_table().to_pylist() == [{"x": 1, "s": "a"}, {"x": 3, "s": "c"}]
    assert result.output_schema() == ({}, {"x": int, "s": str | None})


# ── nested and logical columns ──────────────────────────────────────────────

ITEMS = pa.large_list(pa.field("item", pa.int8(), nullable=True))
POINT = pa.struct([pa.field("a", pa.int8()), pa.field("b", pa.large_string())])


def nested() -> ArrowTableSource:
    """A source with a nullable list column and a nullable struct column."""
    return _source(
        {
            "id": pa.array([1, 2, 3, 4], pa.int16()),
            "items": pa.array([[1, None], None, [], [4]], ITEMS),
            "point": pa.array(
                [{"a": 1, "b": None}, {"a": 2, "b": "x"}, None, {"a": None, "b": None}],
                POINT,
            ),
        },
        nullable=["items", "point"],
    )


def test_only_the_top_level_value_is_tested_for_a_list() -> None:
    """A null list is null; an empty list and a list holding nulls are not."""
    result = DropNulls("items")(nested())
    # A list holding a null and an empty list are not null lists.
    assert result.as_table().column("items").to_pylist() == [[1, None], [], [4]]
    field = result.as_table().schema.field("items")
    assert field.nullable is False
    assert field.type == ITEMS  # the item field keeps its name and nullability
    # The Python schema does not express item nullability; the Arrow type above does.
    assert nested().output_schema()[1]["items"] == list[int] | None
    assert result.output_schema()[1]["items"] == list[int]


def test_only_the_top_level_value_is_tested_for_a_struct() -> None:
    """A null struct is null; a struct whose fields are null is not."""
    result = DropNulls("point")(nested())
    # A struct whose children are all null is not a null struct.
    assert _ids(result) == [1, 2, 4]
    field = result.as_table().schema.field("point")
    assert field.nullable is False
    assert field.type == POINT  # children stay nullable


def logical() -> DictSource:
    """A source with nullable ``Path`` and ``numpy.ndarray`` columns."""
    return DictSource(
        data=[
            {"k": "a", "p": Path("/a"), "arr": np.arange(3)},
            {"k": "b", "p": None, "arr": np.arange(2)},
            {"k": "c", "p": Path("/c"), "arr": None},
        ],
        tag_columns=["k"],
        source_id="logical",
    )


def test_logical_columns_keep_their_logical_type() -> None:
    """A logical column keeps its extension type and becomes non-nullable."""
    source = logical()
    before = source.as_table().schema
    assert source.output_schema()[1] == {
        "p": Path | None,
        "arr": np.ndarray | None,  # pyright: ignore[reportMissingTypeArgument]  # the schema holds the bare class
    }

    result = DropNulls(["p", "arr"])(source)
    assert result.output_schema()[1] == {"p": Path, "arr": np.ndarray}
    after = result.as_table().schema
    for name in ("p", "arr"):
        assert isinstance(after.field(name).type, pa.BaseExtensionType)
        assert after.field(name).type == before.field(name).type
        assert after.field(name).nullable is False
    ((tag, data),) = list(result.iter_data())
    assert tag["k"] == "a"
    assert data["p"] == Path("/a")
    array = data["arr"]
    assert isinstance(array, np.ndarray)
    assert array.tolist() == [0, 1, 2]


# ── arguments ───────────────────────────────────────────────────────────────


def test_missing_column_is_an_input_validation_error() -> None:
    """A name that is not a column of the stream is refused at call time."""
    with pytest.raises(InputValidationError, match="nope"):
        DropNulls(["x", "nope"])(readings())


def test_system_and_source_columns_cannot_be_named() -> None:
    """System-tag and source-info columns are not tag or data columns."""
    source = readings()
    hidden = [
        n
        for n in source.as_table(columns=ALL).column_names
        if n.startswith((constants.SYSTEM_TAG_PREFIX, constants.SOURCE_PREFIX))
    ]
    assert len(hidden) >= 2
    for name in hidden:
        with pytest.raises(InputValidationError):
            DropNulls(name)(source)


def test_no_column_is_an_error_at_construction() -> None:
    """An empty collection of names is refused when the operator is built."""
    with pytest.raises(ValueError, match="at least one"):
        DropNulls([])


# ── identity ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "columns",
    [["x", "y"], ("x", "y"), {"x", "y"}, ["y", "x"], ["x", "y", "x"], frozenset("xy")],
    ids=["list", "tuple", "set", "reversed", "duplicate", "frozenset"],
)
def test_identity_is_the_set_of_names(columns: Any) -> None:
    """Container type, order and duplicates do not change the operator's hashes."""
    reference = DropNulls(["x", "y"])
    op = DropNulls(columns)
    assert op.columns == ("x", "y")
    assert op.content_hash() == reference.content_hash()
    assert op.pipeline_hash() == reference.pipeline_hash()
    source = readings()
    assert op(source).content_hash() == reference(source).content_hash()
    assert op(source).pipeline_hash() == reference(source).pipeline_hash()


def test_a_bare_name_is_one_column() -> None:
    """A string is one column name, never a collection of characters."""
    assert DropNulls("note").columns == ("note",)
    assert DropNulls("x").content_hash() == DropNulls(["x"]).content_hash()


def test_different_names_are_different_identities() -> None:
    """Operators over different names hash differently, as do their outputs."""
    source = readings()
    assert DropNulls("x").content_hash() != DropNulls("y").content_hash()
    assert DropNulls("x").content_hash() != DropNulls(["x", "y"]).content_hash()
    assert (
        DropNulls("x")(source).pipeline_hash() != DropNulls("y")(source).pipeline_hash()
    )
    assert DropNulls("x")(source).pipeline_hash() != source.pipeline_hash()


def test_the_caller_s_collection_is_not_kept_or_mutated() -> None:
    """The operator copies the names; later changes to the list do not reach it."""
    names = ["y", "x"]
    op = DropNulls(names)
    names.append("z")
    assert names == ["y", "x", "z"]
    assert op.columns == ("x", "y")


# ── hashes and provenance of surviving rows ─────────────────────────────────


def test_narrowed_rows_hash_as_rows_declared_non_nullable() -> None:
    """Nullability is part of the Arrow schema hash, so narrowing a data column
    changes the row's ``Data`` hash, to the hash the same row has in a stream
    that declared the column non-nullable from the start. Tags are untouched."""
    columns: dict[str, Column] = {
        "id": pa.array([1, 3], pa.int16()),
        "x": pa.array([10, 30], pa.int8()),
    }
    declared = _source(columns)
    nullable = _source(
        {
            "id": pa.array([1, 2, 3], pa.int16()),
            "x": pa.array([10, None, 30], pa.int8()),
        },
        nullable=["x"],
    )
    input_rows = {tag["id"]: (tag, data) for tag, data in nullable.iter_data()}
    narrowed = list(DropNulls("x")(nullable).iter_data())
    assert len(narrowed) == 2
    for (tag, data), (ref_tag, ref_data) in zip(narrowed, declared.iter_data()):
        in_tag, in_data = input_rows[tag["id"]]
        assert data.content_hash() == ref_data.content_hash()
        assert data.content_hash() != in_data.content_hash()
        assert tag.content_hash() == in_tag.content_hash() == ref_tag.content_hash()


def test_narrowing_a_tag_changes_the_tag_hash_only() -> None:
    """Narrowing a tag column changes the row's tag hash and not its data hash."""
    source = keyed()
    input_rows = {tag["id"]: (tag, data) for tag, data in source.iter_data()}
    for tag, data in DropNulls("site")(source).iter_data():
        in_tag, in_data = input_rows[tag["id"]]
        assert tag.content_hash() != in_tag.content_hash()
        assert data.content_hash() == in_data.content_hash()


def test_system_tags_and_source_info_pass_through() -> None:
    """System-tag and source-info columns keep their names, types and values."""
    source = readings()
    before = source.as_table(columns=ALL)
    after = DropNulls(["x", "y"])(source).as_table(columns=ALL)
    hidden = [
        n
        for n in before.column_names
        if n.startswith((constants.SYSTEM_TAG_PREFIX, constants.SOURCE_PREFIX))
    ]
    assert hidden and after.column_names == before.column_names
    kept = before.take(pa.array([0, 3]))
    for name in hidden:
        assert after.schema.field(name) == before.schema.field(name)
        assert after.column(name).to_pylist() == kept.column(name).to_pylist()

    input_rows = {tag["id"]: (tag, data) for tag, data in source.iter_data()}
    for tag, data in DropNulls(["x", "y"])(source).iter_data():
        in_tag, in_data = input_rows[tag["id"]]
        assert tag.system_tags() == in_tag.system_tags()
        assert data.source_info() == in_data.source_info()


def test_the_input_stream_is_not_changed() -> None:
    """Running the operator leaves the input stream's schema and rows as they were."""
    source = readings()
    schema = source.as_table(columns=ALL).schema
    rows = source.as_table(columns=ALL).to_pylist()
    DropNulls(["x", "y"])(source).as_table()
    assert source.as_table(columns=ALL).schema == schema
    assert source.as_table(columns=ALL).to_pylist() == rows


# ── stream method ───────────────────────────────────────────────────────────


def test_stream_method_equals_the_operator() -> None:
    """``stream.drop_nulls`` gives the operator's rows and hashes."""
    source = readings()
    by_method = source.drop_nulls(["x", "y"])
    by_operator = DropNulls(["x", "y"])(source)
    assert by_method.as_table(columns=ALL) == by_operator.as_table(columns=ALL)
    assert by_method.content_hash() == by_operator.content_hash()
    assert by_method.pipeline_hash() == by_operator.pipeline_hash()
    assert _ids(source.drop_nulls("x")) == [1, 3, 4]


# ── downstream ──────────────────────────────────────────────────────────────


@function_pod(output_keys="doubled")
def double_x(x: int) -> int:
    """A pod with a non-optional parameter."""
    return x * 2


def test_pod_with_a_non_optional_parameter_runs_only_after_the_operator() -> None:
    """A pod taking a plain ``int`` refuses ``int | None`` and runs on the narrowed stream."""
    source = _source(
        {
            "id": pa.array([1, 2, 3], pa.int16()),
            "x": pa.array([10, None, 30], pa.int64()),
        },
        nullable=["x"],
    )
    with pytest.raises(ValueError, match="not compatible"):
        double_x.pod(source)

    result = double_x.pod(source.drop_nulls("x"))
    assert result.as_table().to_pylist() == [
        {"id": 1, "doubled": 20},
        {"id": 3, "doubled": 60},
    ]


def test_join_after_the_operator() -> None:
    """A narrowed stream joins, and the joined column stays non-nullable."""
    weights = _source(
        {
            "id": pa.array([1, 2, 3, 4], pa.int16()),
            "w": pa.array([1.0, 2.0, 3.0, 4.0], pa.float64()),
        },
        source_id="weights",
    )
    joined = Join()(DropNulls("x")(readings()), weights)
    assert joined.output_schema()[1]["x"] is int
    assert sorted(_ids(joined)) == [1, 3, 4]
    assert joined.as_table().schema.field("x").nullable is False


# ── no argument: every tag and data column ──────────────────────────────────


def _hidden(table: pa.Table) -> list[str]:
    """Return the system-tag and source-info column names of ``table``."""
    return [
        n
        for n in table.column_names
        if n.startswith((constants.SYSTEM_TAG_PREFIX, constants.SOURCE_PREFIX))
    ]


def test_no_argument_drops_rows_with_a_null_in_any_column() -> None:
    """Without names, a row survives only if every tag and data column has a value."""
    result = DropNulls()(readings())
    assert result.as_table().to_pylist() == [{"id": 1, "x": 10, "y": 1.5, "note": "a"}]


def test_no_argument_narrows_every_tag_and_data_column() -> None:
    """Every tag and data column is ``T`` afterwards, in the schema and in Arrow."""
    source = keyed()
    assert source.output_schema() == (
        {"site": str | None, "id": int},
        {"x": int | None},
    )
    result = DropNulls()(source)
    assert _ids(result) == [1]
    assert result.keys() == source.keys()
    assert result.output_schema() == ({"site": str, "id": int}, {"x": int})
    schema = result.as_table().schema
    assert [schema.field(n).nullable for n in ("site", "id", "x")] == [False] * 3


def test_no_argument_equals_naming_every_column() -> None:
    """``DropNulls()`` and ``DropNulls([every column])`` give the same rows and schema.

    The operators themselves are different identities, and so are their outputs'
    stream hashes.
    """
    source = keyed()
    implicit = DropNulls()
    named = DropNulls(["site", "id", "x"])
    by_all, by_name = implicit(source), named(source)
    assert by_all.as_table(columns=ALL) == by_name.as_table(columns=ALL)
    assert by_all.as_table(columns=ALL).schema == by_name.as_table(columns=ALL).schema
    assert by_all.output_schema() == by_name.output_schema()
    for (tag, data), (ref_tag, ref_data) in zip(
        by_all.iter_data(), by_name.iter_data(), strict=True
    ):
        assert tag.content_hash() == ref_tag.content_hash()
        assert data.content_hash() == ref_data.content_hash()
        assert tag.system_tags() == ref_tag.system_tags()

    assert implicit.content_hash() != named.content_hash()
    assert implicit.pipeline_hash() != named.pipeline_hash()
    assert by_all.content_hash() != by_name.content_hash()
    assert by_all.pipeline_hash() != by_name.pipeline_hash()


def test_no_argument_identity_is_one_fixed_value() -> None:
    """The no-argument operator stores no names: its identity is the same on any stream."""
    op = DropNulls()
    assert op.columns is None
    before = (op.content_hash(), op.pipeline_hash())
    assert _ids(op(readings())) == [1]
    assert _ids(op(keyed())) == [1]
    assert op.columns is None
    assert (op.content_hash(), op.pipeline_hash()) == before
    assert DropNulls(None).content_hash() == op.content_hash()
    assert DropNulls().content_hash() == op.content_hash()
    for names in ("x", ["id", "x", "y", "note"], ["site", "id", "x"]):
        assert DropNulls(names).content_hash() != op.content_hash()


@pytest.mark.parametrize("empty", [[], (), set(), frozenset()])
def test_an_empty_collection_is_still_an_error(empty: Any) -> None:
    """An empty collection is not "no argument": it is refused, never read as "all"."""
    with pytest.raises(ValueError, match="at least one"):
        DropNulls(empty)
    with pytest.raises(ValueError, match="at least one"):
        readings().drop_nulls(empty)


def test_no_argument_leaves_system_and_source_columns_alone() -> None:
    """Only user tag and data columns are tested and narrowed.

    Row 1's source-info value for ``x`` is null: the row still survives, and the
    system-tag and source-info fields keep their types, flags and values.
    """
    source = readings()
    before = source.as_table(columns=ALL)
    hidden = _hidden(before)
    assert len(hidden) >= 2
    source_x = f"{constants.SOURCE_PREFIX}x"
    position = before.schema.get_field_index(source_x)
    nullable_token = before.schema.field(position).with_nullable(True)
    stream = ArrowTableStream(
        before.set_column(
            position,
            nullable_token,
            pa.array([None, "t2", "t3", "t4"], nullable_token.type),
        ),
        tag_columns=["id"],
    )
    given = stream.as_table(columns=ALL)
    assert given.schema.field(source_x).nullable is True
    result = DropNulls()(stream)
    after = result.as_table(columns=ALL)
    assert _ids(result) == [1]
    assert after.column(source_x).to_pylist() == [None]
    assert after.column_names == given.column_names
    for name in _hidden(given):
        assert after.schema.field(name) == given.schema.field(name)
        assert after.column(name).to_pylist() == given.column(name).to_pylist()[:1]


def test_no_argument_schema_prediction_matches_the_computed_stream() -> None:
    """The predicted schema equals the computed stream's, with and without system tags."""
    op = DropNulls()
    for source in (readings(), keyed()):
        for config in (
            {},
            {"system_tags": True},
            {"source": True, "system_tags": True},
        ):
            # Non-public on purpose: a prediction is verified against the static result.
            computed = op.static_process(source).output_schema(columns=config)
            assert op(source).output_schema(columns=config) == computed
            assert op.output_schema(source, columns=config) == computed
            assert all(
                type(None) not in typing.get_args(python_type)
                for name, python_type in {**computed[0], **computed[1]}.items()
                if not name.startswith(constants.SYSTEM_TAG_PREFIX)
            )


def test_no_argument_zero_surviving_rows_keep_the_narrowed_types() -> None:
    """A result with no rows has the schema of a non-empty result."""
    empty = _source(
        {
            "id": pa.array([1, 2], pa.int16()),
            "x": pa.array([None, 7], pa.int8()),
            "s": pa.array(["a", None], pa.string()),
        },
        nullable=["id", "x", "s"],
    )
    non_empty = _source(
        {
            "id": pa.array([1, 2], pa.int16()),
            "x": pa.array([5, 7], pa.int8()),
            "s": pa.array(["a", None], pa.string()),
        },
        nullable=["id", "x", "s"],
    )
    result = DropNulls()(empty)
    table = result.as_table(columns=ALL)
    assert table.num_rows == 0
    assert list(result.iter_data()) == []
    assert result.output_schema() == ({"id": int}, {"x": int, "s": str})
    assert table.schema.field("id") == pa.field("id", pa.int16(), nullable=False)
    assert table.schema.field("x") == pa.field("x", pa.int8(), nullable=False)
    assert table.schema.field("s") == pa.field("s", pa.large_string(), nullable=False)
    assert table.schema == DropNulls()(non_empty).as_table(columns=ALL).schema


def test_no_argument_on_a_non_nullable_stream_is_a_no_op() -> None:
    """A stream without a nullable column is unchanged, and twice equals once."""
    source = _source(
        {
            "id": pa.array([1, 2], pa.int16()),
            "x": pa.array([10, 20], pa.int8()),
        }
    )
    result = DropNulls()(source)
    assert result.output_schema() == source.output_schema()
    assert result.as_table(columns=ALL) == source.as_table(columns=ALL)
    for (tag, data), (in_tag, in_data) in zip(
        result.iter_data(), source.iter_data(), strict=True
    ):
        assert tag.content_hash() == in_tag.content_hash()
        assert data.content_hash() == in_data.content_hash()

    once = DropNulls()(readings())
    twice = DropNulls()(once)
    assert twice.as_table(columns=ALL) == once.as_table(columns=ALL)
    assert twice.output_schema() == once.output_schema()


def test_no_argument_on_a_stream_without_tag_columns() -> None:
    """A stream with no tag columns: every data column is tested and narrowed."""
    source = _source(
        {
            "x": pa.array([1, None, 3], pa.int8()),
            "s": pa.array(["a", "b", None], pa.string()),
        },
        tags=(),
        nullable=["x", "s"],
    )
    result = DropNulls()(source)
    assert result.as_table().to_pylist() == [{"x": 1, "s": "a"}]
    assert result.output_schema() == ({}, {"x": int, "s": str})


def test_no_argument_stream_method_equals_the_operator() -> None:
    """``stream.drop_nulls()`` gives the no-argument operator's rows and hashes."""
    source = readings()
    by_method = source.drop_nulls()
    by_operator = DropNulls()(source)
    assert by_method.as_table(columns=ALL) == by_operator.as_table(columns=ALL)
    assert by_method.content_hash() == by_operator.content_hash()
    assert by_method.pipeline_hash() == by_operator.pipeline_hash()
    assert _ids(source.drop_nulls(label="complete")) == [1]


def test_no_argument_does_not_change_the_input_stream() -> None:
    """Running the no-argument operator leaves the input stream as it was."""
    source = readings()
    schema = source.as_table(columns=ALL).schema
    rows = source.as_table(columns=ALL).to_pylist()
    DropNulls()(source).as_table()
    assert source.as_table(columns=ALL).schema == schema
    assert source.as_table(columns=ALL).to_pylist() == rows


def test_no_argument_leaves_meta_columns_alone() -> None:
    """A meta (``__``) column is not a data column for "every column".

    A stream built straight from a table lists its meta columns among its data
    keys (ITL-846); a null there still drops no row and the field stays nullable.
    """
    meta = f"{constants.META_PREFIX}note"
    table = readings().as_table(columns=ALL)
    table = table.append_column(
        pa.field(meta, pa.large_string(), nullable=True),
        pa.array([None, "m2", "m3", "m4"], pa.large_string()),
    )
    stream = ArrowTableStream(table, tag_columns=["id"])
    result = DropNulls()(stream)
    rows = list(result.iter_data())
    assert [tag["id"] for tag, _ in rows] == [1]
    data = rows[0][1]
    assert data.as_dict() == {"x": 10, "y": 1.5, "note": "a"}
    meta_field = data.arrow_schema(columns={"meta": True}).field(meta)
    assert meta_field == pa.field(meta, pa.large_string(), nullable=True)
    assert data.as_table(columns={"meta": True}).column(meta).to_pylist() == [None]
    # Non-public on purpose: a prediction is verified against the static result.
    computed = DropNulls().static_process(stream).output_schema()
    assert DropNulls().output_schema(stream) == computed
