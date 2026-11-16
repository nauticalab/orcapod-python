"""``map_tags``, ``map_data`` and ``map_columns`` refuse a name they cannot rename (ITL-1160).

A key of the mapping that is not a column of the mapped kind is refused by default
(``strict=True``) with an ``InputValidationError`` naming the missing names and the
columns of that kind; a name that is a column of the other kind says which method
renames it. The check runs when the operator is called on a stream, before any row
is processed, and on the async path against the first row. ``strict=False`` keeps
the earlier behaviour: such a name is ignored. ``strict`` is part of the
operator's identity, as for the column selection operators. ``MapColumns`` renames tag and data columns in one step; a tag
stays a tag and data stays data. Tests drive the operators through ``op(stream)``
and the stream methods.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.operators import MapColumns, MapData, MapTags
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
SOURCE = constants.SOURCE_PREFIX


def _source() -> DictSource:
    """Tags ``cohort``, ``subject``; data ``age``, ``weight``."""
    return DictSource(
        data=[
            {"cohort": "a", "subject": "m1", "age": 12, "weight": 25.3},
            {"cohort": "a", "subject": "m2", "age": 8, "weight": 22.1},
            {"cohort": "b", "subject": "m3", "age": 15, "weight": 27.8},
        ],
        tag_columns=["cohort", "subject"],
        tag_only_columns=True,
        source_id="subjects",
    )


def _rows(stream: StreamProtocol) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    return [(tag.as_dict(), data.as_dict()) for tag, data in stream.iter_data()]


def _async_rows_as_dicts(
    op: Any, stream: StreamProtocol
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    return [(tag.as_dict(), data.as_dict()) for tag, data in async_rows(op, stream)]


# ── refusal: a name that is not a column of the mapped kind ────────────────

# (operator class, stream method, name map, fragments the message must contain)
REFUSALS = [
    (
        MapTags,
        "map_tags",
        {"nope": "z"},
        ["['nope'] not among the tag columns ['cohort', 'subject']", "strict=False"],
    ),
    (
        MapTags,
        "map_tags",
        {"age": "z"},
        ["['age'] not among the tag columns", "'age' is a data column: use map_data"],
    ),
    (
        MapData,
        "map_data",
        {"nope": "z"},
        ["['nope'] not among the data columns ['age', 'weight']", "strict=False"],
    ),
    (
        MapData,
        "map_data",
        {"subject": "z"},
        [
            "['subject'] not among the data columns",
            "'subject' is a tag column: use map_tags",
        ],
    ),
    (
        MapColumns,
        "map_columns",
        {"nope": "z"},
        [
            "['nope'] not among the tag columns ['cohort', 'subject'] or the data "
            "columns ['age', 'weight']",
            "strict=False",
        ],
    ),
]
REFUSAL_IDS = [
    "map_tags_missing",
    "map_tags_data_name",
    "map_data_missing",
    "map_data_tag_name",
    "map_columns_missing",
]


def _assert_message(
    error: pytest.ExceptionInfo[InputValidationError], parts: list[str]
):
    for part in parts:
        assert part in str(error.value)


@pytest.mark.parametrize(
    ("op_cls", "method", "name_map", "parts"), REFUSALS, ids=REFUSAL_IDS
)
def test_the_stream_method_refuses_by_default(
    op_cls: type, method: str, name_map: dict[str, str], parts: list[str]
) -> None:
    with pytest.raises(InputValidationError) as error:
        getattr(_source(), method)(name_map)
    _assert_message(error, parts)


@pytest.mark.parametrize(
    ("op_cls", "method", "name_map", "parts"), REFUSALS, ids=REFUSAL_IDS
)
def test_the_operator_refuses_when_called(
    op_cls: type, method: str, name_map: dict[str, str], parts: list[str]
) -> None:
    op = op_cls(name_map)  # building it is fine: the input is not known yet
    with pytest.raises(InputValidationError) as error:
        op(_source())
    _assert_message(error, parts)


@pytest.mark.parametrize(
    ("op_cls", "method", "name_map", "parts"), REFUSALS, ids=REFUSAL_IDS
)
def test_the_async_path_refuses(
    op_cls: type, method: str, name_map: dict[str, str], parts: list[str]
) -> None:
    with pytest.raises(InputValidationError) as error:
        async_rows(op_cls(name_map), _source())
    _assert_message(error, parts)


@pytest.mark.parametrize(
    ("method", "name_map"),
    [
        ("map_tags", {"subject": "animal", "nope": "z"}),
        ("map_data", {"age": "years", "nope": "z"}),
        ("map_columns", {"subject": "animal", "age": "years", "nope": "z"}),
    ],
)
def test_one_missing_name_refuses_the_whole_map(
    method: str, name_map: dict[str, str]
) -> None:
    with pytest.raises(InputValidationError, match=r"\['nope'\]"):
        getattr(_source(), method)(name_map)


def test_every_missing_name_is_named() -> None:
    with pytest.raises(InputValidationError) as error:
        _source().map_tags({"age": "a", "nope": "n", "weight": "w"})
    message = str(error.value)
    assert "['age', 'nope', 'weight']" in message
    assert "'age' is a data column: use map_data" in message
    assert "'weight' is a data column: use map_data" in message
    assert "'nope' is a" not in message


@pytest.mark.parametrize(
    ("op", "stream_factory"),
    [
        (
            lambda: MapData({f"{META}m": "renamed"}),
            lambda: meta_stream("s", "v", [f"{META}m"]),
        ),
        (
            lambda: MapColumns({f"{META}m": "renamed"}),
            lambda: meta_stream("s", "v", [f"{META}m"]),
        ),
    ],
    ids=["map_data", "map_columns"],
)
def test_a_meta_column_is_not_a_column_to_rename(
    op: Callable[[], Any], stream_factory: Callable[[], StreamProtocol]
) -> None:
    with pytest.raises(InputValidationError, match="not among the"):
        op()(stream_factory())
    with pytest.raises(InputValidationError, match="not among the"):
        async_rows(op(), stream_factory())


# ── strict=False keeps the earlier behaviour ───────────────────────────────


@pytest.mark.parametrize(
    ("op_cls", "method", "name_map"),
    [(op_cls, method, name_map) for op_cls, method, name_map, _ in REFUSALS],
    ids=REFUSAL_IDS,
)
def test_non_strict_ignores_the_name(
    op_cls: type, method: str, name_map: dict[str, str]
) -> None:
    source = _source()
    by_method = getattr(source, method)(name_map, strict=False)
    assert by_method.keys() == source.keys()
    assert _rows(by_method) == _rows(source)
    assert _async_rows_as_dicts(op_cls(name_map, strict=False), source) == _rows(source)


@pytest.mark.parametrize(
    ("method", "name_map", "keys"),
    [
        (
            "map_tags",
            {"subject": "animal", "age": "z"},
            (("cohort", "animal"), ("age", "weight")),
        ),
        (
            "map_data",
            {"age": "years", "subject": "z"},
            (("cohort", "subject"), ("years", "weight")),
        ),
        (
            "map_columns",
            {"subject": "animal", "nope": "z"},
            (("cohort", "animal"), ("age", "weight")),
        ),
    ],
)
def test_non_strict_renames_the_names_that_exist(
    method: str, name_map: dict[str, str], keys: tuple[tuple[str, ...], ...]
) -> None:
    assert getattr(_source(), method)(name_map, strict=False).keys() == keys


# ── strict is part of the identity ─────────────────────────────────────────


@pytest.mark.parametrize(
    ("op_cls", "name_map"),
    [
        (MapTags, {"subject": "animal"}),
        (MapData, {"age": "years"}),
        (MapColumns, {"subject": "animal", "age": "years"}),
    ],
    ids=["map_tags", "map_data", "map_columns"],
)
def test_strict_enters_the_identity(op_cls: type, name_map: dict[str, str]) -> None:
    strict = op_cls(name_map)(_source())
    lenient = op_cls(name_map, strict=False)(_source())
    assert strict.content_hash() != lenient.content_hash()
    assert strict.pipeline_hash() != lenient.pipeline_hash()
    # The rows are the same: an accepted input gives the same result.
    assert _rows(strict) == _rows(lenient)


@pytest.mark.parametrize("op_cls", [MapTags, MapData, MapColumns])
@pytest.mark.parametrize("strict", [True, False])
def test_strict_survives_a_config_round_trip(op_cls: type, strict: bool) -> None:
    op = op_cls({"a": "b"}, drop_unmapped=True, strict=strict)
    config = op.to_config()
    assert config["config"]["strict"] is strict
    restored = op_cls.from_config(config)
    assert restored.strict is strict
    assert restored.drop_unmapped is True
    assert restored.name_map == {"a": "b"}


@pytest.mark.parametrize("method", ["map_tags", "map_data", "map_columns"])
def test_strict_and_label_are_keyword_only(method: str) -> None:
    with pytest.raises(TypeError):
        getattr(_source(), method)({"age": "years"}, False, True)


# ── MapColumns: tag and data columns in one step ───────────────────────────


def test_map_columns_renames_tags_and_data() -> None:
    stream = _source().map_columns(
        {"subject": "animal", "age": "years"}, label="renamed"
    )
    assert stream.keys() == (("cohort", "animal"), ("years", "weight"))
    assert _rows(stream)[0] == (
        {"cohort": "a", "animal": "m1"},
        {"years": 12, "weight": 25.3},
    )
    columns = stream.as_table(all_info=True).column_names
    assert f"{SOURCE}years" in columns
    assert f"{SOURCE}age" not in columns
    assert stream.label == "renamed"


def test_map_columns_is_one_step() -> None:
    source = _source()
    stream = source.map_columns({"subject": "animal", "age": "years"})
    assert isinstance(stream.producer, MapColumns)
    assert list(stream.upstreams) == [source]


def test_map_columns_gives_the_rows_of_map_tags_then_map_data() -> None:
    source = _source()
    one_step = source.map_columns({"subject": "animal", "age": "years"})
    two_steps = source.map_tags({"subject": "animal"}).map_data({"age": "years"})
    assert one_step.keys() == two_steps.keys()
    assert _rows(one_step) == _rows(two_steps)
    assert (
        one_step.as_table(all_info=True).drop_columns(["_content_hash"]).column_names
        == two_steps.as_table(all_info=True)
        .drop_columns(["_content_hash"])
        .column_names
    )
    # A different pipeline: one operator step, not two.
    assert one_step.pipeline_hash() != two_steps.pipeline_hash()


def test_map_columns_of_one_group_has_its_own_identity() -> None:
    source = _source()
    assert (
        source.map_columns({"age": "years"}).pipeline_hash()
        != source.map_data({"age": "years"}).pipeline_hash()
    )
    assert _rows(source.map_columns({"age": "years"})) == _rows(
        source.map_data({"age": "years"})
    )


def test_map_columns_swaps_two_names_within_a_group() -> None:
    stream = _source().map_columns({"age": "weight", "weight": "age"})
    assert stream.keys() == (("cohort", "subject"), ("weight", "age"))
    assert _rows(stream)[0][1] == {"weight": 12, "age": 25.3}


def test_map_columns_drop_unmapped_keeps_only_the_named_columns() -> None:
    stream = _source().map_columns(
        {"subject": "animal", "weight": "kg"}, drop_unmapped=True
    )
    assert stream.keys() == (("animal",), ("kg",))
    columns = stream.as_table(all_info=True).column_names
    assert f"{SOURCE}age" not in columns
    assert any(c.startswith(constants.SYSTEM_TAG_PREFIX) for c in columns)


@pytest.mark.parametrize(
    ("name_map", "match"),
    [
        ({"subject": "age"}, r"not renamed already have \['age'\]"),
        ({"age": "cohort"}, r"not renamed already have \['cohort'\]"),
        ({"subject": "x", "age": "x"}, r"two columns would both be named \['x'\]"),
    ],
    ids=["tag_onto_data", "data_onto_tag", "two_onto_one"],
)
def test_map_columns_refuses_a_collision(name_map: dict[str, str], match: str) -> None:
    with pytest.raises(InputValidationError, match=match):
        MapColumns(name_map)(_source())
    with pytest.raises(InputValidationError, match=match):
        async_rows(MapColumns(name_map), _source())


def test_map_columns_refuses_leaving_no_data_column() -> None:
    op = MapColumns({"subject": "animal"}, drop_unmapped=True)
    with pytest.raises(InputValidationError, match="needs at least one data column"):
        op(_source())
    with pytest.raises(InputValidationError, match="needs at least one data column"):
        async_rows(op, _source())


def test_map_columns_refuses_a_meta_named_target() -> None:
    with pytest.raises(InputValidationError, match="'__x'.*reserved for meta"):
        MapColumns({"age": "__x"})


@pytest.mark.parametrize(
    "make_op",
    [
        lambda: MapColumns({"subject": "animal", "age": "years"}),
        lambda: MapColumns({"subject": "animal", "weight": "kg"}, drop_unmapped=True),
        lambda: MapColumns({"nope": "z"}, strict=False),
    ],
    ids=["rename", "drop_unmapped", "nothing"],
)
@pytest.mark.parametrize(
    "columns",
    [None, {"system_tags": True}, {"source": True}],
    ids=["default", "system_tags", "source"],
)
def test_map_columns_prediction_equals_the_computed_schema(
    make_op: Callable[[], Any], columns: dict[str, bool] | None
) -> None:
    op = make_op()
    source = _source()
    predicted = op(source).output_schema(columns=columns)
    # Verifying a schema prediction is the standing exception: compare against
    # the computed stream, ``unary_static_process(...).output_schema(...)``.
    computed = op.unary_static_process(source).output_schema(columns=columns)
    assert predicted == computed


@pytest.mark.parametrize(
    "make_stream",
    [
        lambda: meta_stream("s", "v", [f"{META}m"]),
        lambda: cached_pod_stream("a", double_a),
    ],
    ids=["raw_meta_columns", "cached_pod"],
)
def test_map_columns_does_not_carry_meta_columns(
    make_stream: Callable[[], StreamProtocol],
) -> None:
    stream = make_stream()
    data_column = stream.keys()[1][0]

    def make_op() -> MapColumns:
        return MapColumns({"k": "key", data_column: "value"})

    for rows in (sync_rows(make_op(), stream), async_rows(make_op(), stream)):
        assert rows
        for tag, data in rows:
            assert tag.keys() == ("key",)
            assert data.keys() == ("value",)
            assert not data.meta_columns
    result = make_op()(stream)
    assert not any(name.startswith(META) for name in result.keys()[1])
    assert not any(
        name.startswith(META)
        for name in result.output_schema(columns={"meta": True})[1]
    )


def test_map_columns_keeps_arrow_types() -> None:
    stream = _source().map_columns({"subject": "animal", "age": "years"})
    source_schema = _source().as_table().schema
    schema = stream.as_table().schema
    assert schema.field("animal").type == source_schema.field("subject").type
    assert schema.field("years").type == source_schema.field("age").type
    assert schema.field("years").type == pa.int64()


# ── a new name a stream would read as a system column ──────────────────────

RESERVED_NEW_NAMES = [
    f"{constants.SYSTEM_TAG_PREFIX}y",
    f"{constants.SOURCE_PREFIX}y",
    constants.CONTEXT_KEY,
]


@pytest.mark.parametrize(
    ("op_cls", "old_name"),
    [
        (MapTags, "subject"),
        (MapData, "age"),
        (MapColumns, "subject"),
        (MapColumns, "age"),
    ],
    ids=["map_tags", "map_data", "map_columns_tag", "map_columns_data"],
)
@pytest.mark.parametrize(
    "new_name", RESERVED_NEW_NAMES, ids=["system_tag", "source", "context_key"]
)
@pytest.mark.parametrize("strict", [True, False])
def test_a_system_column_name_is_refused_as_a_new_name(
    op_cls: type, old_name: str, new_name: str, strict: bool
) -> None:
    # Refused when built, so neither the sync nor the async path can produce one.
    with pytest.raises(InputValidationError, match=f"{new_name!r}.*reserved"):
        op_cls({old_name: new_name}, strict=strict)


@pytest.mark.parametrize(
    ("method", "old_name"),
    [("map_tags", "subject"), ("map_data", "age"), ("map_columns", "age")],
)
@pytest.mark.parametrize(
    "new_name", RESERVED_NEW_NAMES, ids=["system_tag", "source", "context_key"]
)
def test_the_stream_methods_refuse_a_system_column_name(
    method: str, old_name: str, new_name: str
) -> None:
    with pytest.raises(InputValidationError, match="reserved"):
        getattr(_source(), method)({old_name: new_name})


@pytest.mark.parametrize("new_name", ["_age", "_content_hash"])
def test_other_underscore_names_stay_usable_as_new_names(new_name: str) -> None:
    """Only names a stream reads as system columns are refused (see ITL-1160)."""
    source = _source()
    for stream, async_op in (
        (source.map_data({"age": new_name}), MapData({"age": new_name})),
        (source.map_columns({"age": new_name}), MapColumns({"age": new_name})),
    ):
        assert stream.keys() == (("cohort", "subject"), (new_name, "weight"))
        assert stream.as_table().column_names[2] == new_name
        rows = async_rows(async_op, source)
        assert all(data.keys() == (new_name, "weight") for _, data in rows)


@pytest.mark.parametrize(
    ("op_cls", "old_name"),
    [
        (MapTags, "subject"),
        (MapData, "age"),
        (MapColumns, "subject"),
        (MapColumns, "age"),
    ],
    ids=["map_tags", "map_data", "map_columns_tag", "map_columns_data"],
)
@pytest.mark.parametrize("new_name", ["a::b", ":a"], ids=["separator", "leading_colon"])
def test_a_new_name_with_the_token_separator_is_refused(
    op_cls: type, old_name: str, new_name: str
) -> None:
    """A source-info token splits on ``::`` (ITL-1154): no column name may blur it."""
    with pytest.raises(InputValidationError, match="separator"):
        op_cls({old_name: new_name})


# ── renames as keyword arguments (ITL-1171) ────────────────────────────────

KEYWORD_FORMS = [
    ("map_tags", {"subject": "animal"}),
    ("map_data", {"age": "years", "weight": "kg"}),
    ("map_columns", {"subject": "animal", "age": "years"}),
]


@pytest.mark.parametrize(("method", "renames"), KEYWORD_FORMS)
@pytest.mark.parametrize("drop_unmapped", [False, True])
def test_keyword_renames_build_the_dict_form(
    method: str, renames: dict[str, str], drop_unmapped: bool
) -> None:
    source = _source()
    by_dict = getattr(source, method)(renames, drop_unmapped=drop_unmapped)
    by_keyword = getattr(source, method)(drop_unmapped=drop_unmapped, **renames)
    assert by_keyword.content_hash() == by_dict.content_hash()
    assert by_keyword.pipeline_hash() == by_dict.pipeline_hash()
    assert by_keyword.keys() == by_dict.keys()
    assert _rows(by_keyword) == _rows(by_dict)


@pytest.mark.parametrize("method", ["map_tags", "map_data", "map_columns"])
def test_a_dict_and_keyword_renames_together_are_refused(method: str) -> None:
    with pytest.raises(TypeError, match="either as one dict .* or as keyword"):
        getattr(_source(), method)({"age": "years"}, weight="kg")


@pytest.mark.parametrize("method", ["map_tags", "map_data", "map_columns"])
def test_the_dict_is_positional_and_drop_unmapped_keyword_only(method: str) -> None:
    source = _source()
    with pytest.raises(TypeError):
        getattr(source, method)({"age": "years"}, True)
    # ``name_map=`` is no longer the dict: it reads as a rename of a column
    # called ``name_map``, whose new name is not a string.
    with pytest.raises(TypeError, match="must be strings"):
        getattr(source, method)(name_map={"age": "years"})


@pytest.mark.parametrize("method", ["map_tags", "map_data", "map_columns"])
@pytest.mark.parametrize(
    "call",
    [lambda m, s: getattr(s, m)(age=3), lambda m, s: getattr(s, m)({"age": 3})],
    ids=["keyword", "dict"],
)
def test_a_name_that_is_not_a_string_is_refused(
    method: str, call: Callable[[str, StreamProtocol], Any]
) -> None:
    with pytest.raises(TypeError, match="must be strings"):
        call(method, _source())


@pytest.mark.parametrize("method", ["map_tags", "map_data", "map_columns"])
def test_no_rename_is_the_empty_mapping(method: str) -> None:
    source = _source()
    none = getattr(source, method)()
    empty = getattr(source, method)({})
    assert none.content_hash() == empty.content_hash()
    assert none.keys() == source.keys()


def test_an_option_named_column_is_renamed_through_the_dict() -> None:
    source = DictSource(
        data=[{"k": "a", "label": 1, "strict": 2}],
        tag_columns=["k"],
        tag_only_columns=True,
        source_id="options",
    )
    renamed = source.map_data({"label": "lbl", "strict": "st"}, label="renamed")
    assert renamed.keys() == (("k",), ("lbl", "st"))
    assert renamed.label == "renamed"
    # As keywords, ``label`` and ``strict`` are the options, not renames.
    assert source.map_data(label="node").keys() == source.keys()
