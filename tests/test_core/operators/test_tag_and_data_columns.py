"""``drop_columns`` / ``select_columns``: one call across tag and data columns (ITL-1084).

``DropColumns`` and ``SelectColumns`` act on named columns whether each is a tag or a
data column. For names of one group they give the rows of ``DropTagColumns``,
``DropDataColumns``, ``SelectTagColumns`` and ``SelectDataColumns``; their identities
differ by design. Tests drive the operators through ``op(stream)`` and the stream
methods, and read rows through the stream's public methods.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.operators import (
    DropColumns,
    DropDataColumns,
    DropTagColumns,
    SelectColumns,
    SelectDataColumns,
    SelectTagColumns,
)
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.sources import DictSource
from orcapod.system_constants import constants
from tests.test_core.operators.test_async_sync_equivalence import (
    async_rows,
    cached_pod_stream,
    double_a,
    meta_stream,
    sync_rows,
)

META = constants.META_PREFIX


def _source() -> DictSource:
    """Tags ``cohort``, ``subject``; data ``age``, ``weight``, ``name``."""
    return DictSource(
        data=[
            {"cohort": "a", "subject": "m1", "age": 12, "weight": 25.3, "name": "x"},
            {"cohort": "a", "subject": "m2", "age": 8, "weight": 22.1, "name": "y"},
            {"cohort": "b", "subject": "m3", "age": 15, "weight": 27.8, "name": "z"},
        ],
        tag_columns=["cohort", "subject"],
        tag_only_columns=True,
        source_id="subjects",
    )


def _rows(stream: StreamProtocol) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    return [(tag.as_dict(), data.as_dict()) for tag, data in stream.iter_data()]


def _table(stream: StreamProtocol) -> pa.Table:
    return stream.as_table(all_info=True)


# ── the stream methods' arguments ───────────────────────────────────────────


@pytest.mark.parametrize("method", ["drop_columns", "select_columns"])
def test_positional_names_equal_one_collection(method: str) -> None:
    source = _source()
    by_position = getattr(source, method)("subject", "age")
    by_collection = getattr(source, method)(["subject", "age"])
    assert by_position.keys() == by_collection.keys()
    assert _rows(by_position) == _rows(by_collection)
    assert by_position.content_hash() == by_collection.content_hash()
    assert by_position.pipeline_hash() == by_collection.pipeline_hash()


@pytest.mark.parametrize("method", ["drop_columns", "select_columns"])
def test_one_positional_name(method: str) -> None:
    source = _source()
    assert (
        getattr(source, method)("age").pipeline_hash()
        == getattr(source, method)(["age"]).pipeline_hash()
    )


@pytest.mark.parametrize("method", ["drop_columns", "select_columns"])
@pytest.mark.parametrize(
    ("args", "error"),
    [
        ((), TypeError),
        ((["age"], "weight"), TypeError),
        (("age", ["weight"]), TypeError),
        (("age", 3), TypeError),
        (([("age",)],), TypeError),
        ((3,), TypeError),
        (([],), ValueError),
    ],
    ids=[
        "no_name",
        "collection_then_name",
        "name_then_collection",
        "non_string_positional",
        "non_string_in_collection",
        "not_a_name",
        "empty_collection",
    ],
)
def test_bad_arguments_are_rejected(
    method: str, args: tuple[Any, ...], error: type[Exception]
) -> None:
    with pytest.raises(error):
        getattr(_source(), method)(*args)


def test_strict_and_label_are_keyword_only() -> None:
    stream = _source().drop_columns("age", "nope", strict=False, label="dropped")
    assert stream.keys() == (("cohort", "subject"), ("weight", "name"))
    assert stream.label == "dropped"


# ── what is kept ────────────────────────────────────────────────────────────


def test_drop_columns_drops_tags_and_data() -> None:
    stream = _source().drop_columns("subject", "age")
    assert stream.keys() == (("cohort",), ("weight", "name"))
    assert _rows(stream)[0] == ({"cohort": "a"}, {"weight": 25.3, "name": "x"})
    columns = _table(stream).column_names
    assert f"{constants.SOURCE_PREFIX}age" not in columns
    assert f"{constants.SOURCE_PREFIX}weight" in columns
    assert any(name.startswith(constants.SYSTEM_TAG_PREFIX) for name in columns)


def test_select_columns_keeps_only_the_named_columns() -> None:
    stream = _source().select_columns("weight", "cohort")
    # The stream's column order, whatever order the names were given in.
    assert stream.keys() == (("cohort",), ("weight",))
    assert _rows(stream)[2] == ({"cohort": "b"}, {"weight": 27.8})
    columns = _table(stream).column_names
    assert f"{constants.SOURCE_PREFIX}weight" in columns
    assert f"{constants.SOURCE_PREFIX}age" not in columns
    assert f"{constants.SOURCE_PREFIX}name" not in columns
    assert any(name.startswith(constants.SYSTEM_TAG_PREFIX) for name in columns)


def test_select_columns_may_keep_no_tag() -> None:
    stream = _source().select_columns("age")
    assert stream.keys() == ((), ("age",))
    assert [data for _, data in _rows(stream)] == [
        {"age": 12},
        {"age": 8},
        {"age": 15},
    ]


# ── one group: the rows of the existing operators ───────────────────────────


GROUP_CASES: list[tuple[str, Callable[[], Any], Callable[[], Any]]] = [
    ("drop_tags", lambda: DropColumns(["subject"]), lambda: DropTagColumns(["subject"])),
    ("drop_data", lambda: DropColumns(["age", "name"]), lambda: DropDataColumns(["age", "name"])),
    ("select_tags", lambda: SelectColumns(["cohort", "age", "weight", "name"]), lambda: SelectTagColumns(["cohort"])),
    ("select_data", lambda: SelectColumns(["cohort", "subject", "weight"]), lambda: SelectDataColumns(["weight"])),
]  # fmt: skip


@pytest.mark.parametrize(
    ("make_new", "make_old"),
    [(new, old) for _, new, old in GROUP_CASES],
    ids=[label for label, _, _ in GROUP_CASES],
)
def test_one_group_gives_the_rows_of_the_existing_operator(
    make_new: Callable[[], Any], make_old: Callable[[], Any]
) -> None:
    new = make_new()(_source())
    old = make_old()(_source())
    assert new.keys() == old.keys()
    assert _table(new).equals(_table(old))
    for (tag, data), (old_tag, old_data) in zip(new.iter_data(), old.iter_data()):
        assert tag.content_hash() == old_tag.content_hash()
        assert data.content_hash() == old_data.content_hash()
        assert data.source_info() == old_data.source_info()
    # Identities differ by design: a new operator is a new pipeline step.
    assert new.pipeline_hash() != old.pipeline_hash()


# ── strict ──────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("op_cls", [DropColumns, SelectColumns])
def test_strict_rejects_an_unknown_name(op_cls: type) -> None:
    with pytest.raises(InputValidationError, match="nope"):
        op_cls(["age", "nope"])(_source())


def test_non_strict_ignores_an_unknown_name() -> None:
    assert DropColumns(["age", "nope"], strict=False)(_source()).keys() == (
        ("cohort", "subject"),
        ("weight", "name"),
    )
    assert SelectColumns(["age", "nope"], strict=False)(_source()).keys() == (
        (),
        ("age",),
    )


# ── no data column left ─────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "make_op",
    [
        lambda: DropColumns(["age", "weight", "name"]),
        lambda: DropColumns(["subject", "age", "weight", "name"]),
        lambda: SelectColumns(["cohort", "subject"]),
        lambda: SelectColumns(["nope"], strict=False),
    ],
    ids=["drop_all_data", "drop_all_data_and_a_tag", "select_tags_only", "select_none"],
)
def test_no_data_column_left_raises_at_the_call(make_op: Callable[[], Any]) -> None:
    with pytest.raises(InputValidationError, match="needs at least one data column"):
        make_op()(_source())


@pytest.mark.parametrize(
    "make_op",
    [lambda: DropColumns(["age", "weight", "name"]), lambda: SelectColumns(["cohort"])],
    ids=["drop_all_data", "select_tags_only"],
)
def test_no_data_column_left_raises_on_the_async_path(
    make_op: Callable[[], Any],
) -> None:
    with pytest.raises(InputValidationError, match="needs at least one data column"):
        async_rows(make_op(), _source())


# ── identity ────────────────────────────────────────────────────────────────


@pytest.mark.parametrize("op_cls", [DropColumns, SelectColumns])
def test_every_spelling_of_one_set_of_names_has_one_identity(op_cls: type) -> None:
    hashes = {
        op_cls(columns)(_source()).pipeline_hash()
        for columns in (
            ["age", "subject"],
            ["subject", "age"],
            ("subject", "age", "age"),
            {"age", "subject"},
        )
    }
    assert len(hashes) == 1
    assert (
        op_cls(["age"])(_source()).pipeline_hash()
        == op_cls("age")(_source()).pipeline_hash()
    )


@pytest.mark.parametrize("op_cls", [DropColumns, SelectColumns])
def test_strict_enters_the_identity(op_cls: type) -> None:
    assert (
        op_cls(["age"])(_source()).pipeline_hash()
        != op_cls(["age"], strict=False)(_source()).pipeline_hash()
    )


def test_drop_and_select_of_one_name_differ() -> None:
    assert (
        DropColumns(["age"])(_source()).pipeline_hash()
        != SelectColumns(["age"])(_source()).pipeline_hash()
    )


# ── schema prediction ───────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "make_op",
    [
        lambda: DropColumns(["subject", "age"]),
        lambda: SelectColumns(["cohort", "weight"]),
        lambda: SelectColumns(["weight"]),
        lambda: DropColumns(["nope"], strict=False),
    ],
    ids=["drop", "select", "select_no_tag", "drop_nothing"],
)
@pytest.mark.parametrize(
    "columns",
    [None, {"system_tags": True}, {"source": True}],
    ids=["default", "system_tags", "source"],
)
def test_prediction_equals_the_computed_schema(
    make_op: Callable[[], Any], columns: dict[str, bool] | None
) -> None:
    op = make_op()
    source = _source()
    predicted = op(source).output_schema(columns=columns)
    # Verifying a schema prediction is the standing exception: compare against
    # the computed stream, ``unary_static_process(...).output_schema(...)``.
    computed = op.unary_static_process(source).output_schema(columns=columns)
    assert predicted == computed


# ── meta columns are not carried ────────────────────────────────────────────


@pytest.mark.parametrize(
    "make_stream",
    [
        lambda: meta_stream("s", "v", [f"{META}m"]),
        lambda: cached_pod_stream("a", double_a),
    ],
    ids=["raw_meta_columns", "cached_pod"],
)
@pytest.mark.parametrize(
    "make_op",
    [
        lambda: DropColumns(["k"]),
        lambda: SelectColumns(["k", "v", "out_a"], strict=False),
    ],
    ids=["drop", "select"],
)
def test_meta_columns_are_not_carried(
    make_stream: Callable[[], StreamProtocol], make_op: Callable[[], Any]
) -> None:
    stream = make_stream()
    for rows in (sync_rows(make_op(), stream), async_rows(make_op(), stream)):
        assert rows
        for _, data in rows:
            assert not data.meta_columns
            assert not any(name.startswith(META) for name in data.keys())
    result = make_op()(stream)
    assert not any(name.startswith(META) for name in result.keys()[1])
    assert not any(
        name.startswith(META)
        for name in result.output_schema(columns={"meta": True})[1]
    )
