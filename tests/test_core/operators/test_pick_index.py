"""Tests for Pick and Index operators."""
from __future__ import annotations

import pytest

from orcapod.logical_types.base_logical_type import BaseLogicalType


class ConcreteLogicalType(BaseLogicalType):
    """Minimal concrete subclass for testing defaults."""
    pass


def test_base_logical_type_pick_field_raises():
    lt = ConcreteLogicalType()
    with pytest.raises(NotImplementedError, match="does not support pick"):
        lt.pick_field("some_key")


def test_base_logical_type_index_element_raises():
    lt = ConcreteLogicalType()
    with pytest.raises(NotImplementedError, match="does not support index"):
        lt.index_element()


import dataclasses

from orcapod.core.operators import Pick
from orcapod.core.sources import DictSource
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.errors import InputValidationError


# Module-level dataclass — must be at top level so it has a stable FQCN
@dataclasses.dataclass
class _PickTestModel:
    value: int


# ── fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def dict_stream() -> ArrowTableStream:
    """Stream with 1 tag and 1 dict[str, int] data column (with source info)."""
    return DictSource(
        [
            {"animal": "cat",  "scores": {"speed": 8,  "stealth": 9}},
            {"animal": "dog",  "scores": {"speed": 7,  "strength": 6}},
            {"animal": "bird", "scores": {"speed": 10, "stealth": 7}},
        ],
        tag_columns=["animal"],
        data_schema={"animal": str, "scores": dict[str, int]},
    )


# ── build-time validation tests ───────────────────────────────────────────────

def test_pick_missing_column_raises(dict_stream):
    with pytest.raises(InputValidationError, match="not found in data schema"):
        Pick("nonexistent", "speed")(dict_stream)


def test_pick_out_collision_raises(dict_stream):
    with pytest.raises(InputValidationError, match="already exists"):
        Pick("scores", "speed", out="scores")(dict_stream)


# ── functional tests: Pick ────────────────────────────────────────────────────

def test_pick_dict_str_int_type_resolution():
    """Picking a key from dict[str, int] yields int in the output schema and as values.

    This test saves a dict[str, int] column into a stream, applies Pick to
    extract one entry, and asserts that:
    - the output schema records the column type as ``int`` (not dict or Any)
    - the actual extracted values are Python ints
    """
    from orcapod.contexts import get_default_type_converter
    converter = get_default_type_converter()
    table = converter.python_dicts_to_arrow_table(
        [
            {"id": 1, "counts": {"apples": 3, "bananas": 7}},
            {"id": 2, "counts": {"apples": 5, "bananas": 2}},
        ],
        python_schema={"id": int, "counts": dict[str, int]},
    )
    stream = ArrowTableStream(table, tag_columns=["id"])

    result = Pick("counts", "apples")(stream)

    # Schema: output column must be int, not dict
    tag_schema, data_schema = result.output_schema()
    assert data_schema["counts"] == int, (
        f"Expected int output type but got {data_schema['counts']}"
    )

    # Values: extracted entries must be Python ints
    rows = list(result.iter_data())
    assert len(rows) == 2
    values = [data["counts"] for _, data in rows]
    assert values == [3, 5]
    assert all(isinstance(v, int) for v in values), (
        f"Expected all values to be int, got types: {[type(v) for v in values]}"
    )


def test_pick_dict_default_out(dict_stream):
    """pick with out=None replaces the column in-place, source token updated."""
    result = Pick("scores", "speed")(dict_stream)
    tag_schema, data_schema = result.output_schema()

    assert "scores" in data_schema
    assert data_schema["scores"] == int

    rows = list(result.iter_data())
    assert len(rows) == 3
    values = [data["scores"] for _, data in rows]
    assert values == [8, 7, 10]

    # source token for 'scores' should now end with "['speed']"
    for _, data in rows:
        src = data.source_info().get("scores")
        assert src is not None and src.endswith("['speed']"), f"unexpected source: {src}"


def test_pick_dict_explicit_out(dict_stream):
    """pick with out='speed_score' adds new column, original unchanged."""
    result = Pick("scores", "speed", out="speed_score")(dict_stream)
    tag_schema, data_schema = result.output_schema()

    assert "scores" in data_schema       # original preserved
    assert "speed_score" in data_schema
    assert data_schema["speed_score"] == int

    rows = list(result.iter_data())
    assert [data["speed_score"] for _, data in rows] == [8, 7, 10]

    for _, data in rows:
        src = data.source_info().get("speed_score")
        assert src is not None and src.endswith("['speed']")


def test_pick_dict_missing_key_skip():
    """Packets missing the key are skipped; others pass through."""
    from orcapod.contexts import get_default_type_converter
    converter = get_default_type_converter()
    table = converter.python_dicts_to_arrow_table(
        [
            {"id": 1, "data": {"x": 10}},
            {"id": 2, "data": {"y": 20}},   # missing "x"
            {"id": 3, "data": {"x": 30}},
        ],
        python_schema={"id": int, "data": dict[str, int]},
    )
    stream = ArrowTableStream(table, tag_columns=["id"])

    result = Pick("data", "x", fail_on_miss=False)(stream)
    rows = list(result.iter_data())
    assert len(rows) == 2
    ids = [tag["id"] for tag, _ in rows]
    assert ids == [1, 3]


def test_pick_dict_missing_key_fail():
    """fail_on_miss=True raises RuntimeError when key is absent."""
    from orcapod.contexts import get_default_type_converter
    converter = get_default_type_converter()
    table = converter.python_dicts_to_arrow_table(
        [
            {"id": 1, "data": {"x": 10}},
            {"id": 2, "data": {"y": 20}},
        ],
        python_schema={"id": int, "data": dict[str, int]},
    )
    stream = ArrowTableStream(table, tag_columns=["id"])

    with pytest.raises(RuntimeError, match="fail_on_miss=True"):
        list(Pick("data", "x", fail_on_miss=True)(stream).iter_data())


def test_pick_extension_type_not_implemented():
    """pick on a Pydantic/dataclass column raises NotImplementedError."""
    from orcapod.contexts import get_default_type_converter
    converter = get_default_type_converter()
    converter.register_python_class(_PickTestModel)
    table = converter.python_dicts_to_arrow_table(
        [{"id": 1, "rec": _PickTestModel(value=42)}],
        python_schema={"id": int, "rec": _PickTestModel},
    )
    stream = ArrowTableStream(table, tag_columns=["id"])

    with pytest.raises(NotImplementedError, match="does not support pick"):
        Pick("rec", "value")(stream)


# ── helpers for Index tests ───────────────────────────────────────────────────

def _make_list_stream(rows: list[dict]) -> ArrowTableStream:
    """Build a stream with an int tag and a list[int] data column."""
    from orcapod.contexts import get_default_type_converter
    converter = get_default_type_converter()
    table = converter.python_dicts_to_arrow_table(
        rows,
        python_schema={"id": int, "items": list[int]},
    )
    return ArrowTableStream(table, tag_columns=["id"])


# ── build-time validation tests: Index ───────────────────────────────────────

from orcapod.core.operators import Index


def test_index_missing_column_raises():
    stream = _make_list_stream([{"id": 1, "items": [10, 20]}])
    with pytest.raises(InputValidationError, match="not found in data schema"):
        Index("nonexistent", 0)(stream)


def test_index_out_collision_raises():
    stream = _make_list_stream([{"id": 1, "items": [10, 20]}])
    with pytest.raises(InputValidationError, match="already exists"):
        Index("items", 0, out="items")(stream)


# ── functional tests: Index ───────────────────────────────────────────────────

def test_index_list_default_out():
    """index with out=None replaces the column in-place."""
    stream = DictSource(
        [
            {"grp": "a", "vals": [10, 20, 30]},
            {"grp": "b", "vals": [40, 50, 60]},
        ],
        tag_columns=["grp"],
        data_schema={"grp": str, "vals": list[int]},
    )
    result = Index("vals", 1)(stream)
    tag_schema, data_schema = result.output_schema()

    assert "vals" in data_schema
    assert data_schema["vals"] == int

    rows = list(result.iter_data())
    assert len(rows) == 2
    assert [data["vals"] for _, data in rows] == [20, 50]

    for _, data in rows:
        src = data.source_info().get("vals")
        assert src is not None and src.endswith("[1]"), f"unexpected source: {src}"


def test_index_list_explicit_out():
    """index with out='first' adds a new column; original preserved."""
    stream = DictSource(
        [
            {"grp": "a", "vals": [10, 20, 30]},
            {"grp": "b", "vals": [40, 50, 60]},
        ],
        tag_columns=["grp"],
        data_schema={"grp": str, "vals": list[int]},
    )
    result = Index("vals", 0, out="first")(stream)
    tag_schema, data_schema = result.output_schema()

    assert "vals" in data_schema       # original preserved
    assert "first" in data_schema
    assert data_schema["first"] == int

    rows = list(result.iter_data())
    assert [data["first"] for _, data in rows] == [10, 40]

    for _, data in rows:
        src = data.source_info().get("first")
        assert src is not None and src.endswith("[0]")


def test_index_negative_index():
    """Negative index follows Python semantics (-1 is last element)."""
    stream = _make_list_stream([
        {"id": 1, "items": [10, 20, 30]},
        {"id": 2, "items": [40, 50]},
    ])
    result = Index("items", -1)(stream)
    rows = list(result.iter_data())
    assert [data["items"] for _, data in rows] == [30, 50]


def test_index_oob_skip():
    """Out-of-bounds packets are skipped when fail_on_miss=False."""
    stream = _make_list_stream([
        {"id": 1, "items": [10, 20, 30]},
        {"id": 2, "items": [40]},          # index 2 is OOB
        {"id": 3, "items": [70, 80, 90]},
    ])
    result = Index("items", 2, fail_on_miss=False)(stream)
    rows = list(result.iter_data())
    assert len(rows) == 2
    ids = [tag["id"] for tag, _ in rows]
    assert ids == [1, 3]


def test_index_oob_fail():
    """fail_on_miss=True raises RuntimeError on out-of-bounds."""
    stream = _make_list_stream([
        {"id": 1, "items": [10, 20, 30]},
        {"id": 2, "items": [40]},
    ])
    with pytest.raises(RuntimeError, match="fail_on_miss=True"):
        list(Index("items", 2, fail_on_miss=True)(stream).iter_data())


# ── integration tests ──────────────────────────────────────────────────────────

def test_chained_pick_then_index():
    """stream.pick('col', 'key').index('col', 1) chains correctly end-to-end."""
    # Use DictSource so that source info tokens are populated from the start.
    stream = DictSource(
        [
            {"id": 1, "data": {"scores": [10, 20, 30]}},
            {"id": 2, "data": {"scores": [40, 50, 60]}},
        ],
        tag_columns=["id"],
        data_schema={"id": int, "data": dict[str, list[int]]},
    )

    result = stream.pick("data", "scores").index("data", 1)
    tag_schema, data_schema = result.output_schema()

    assert "data" in data_schema
    assert data_schema["data"] == int

    rows = list(result.iter_data())
    assert [data["data"] for _, data in rows] == [20, 50]

    # Source token should encode both projections
    for _, data in rows:
        src = data.source_info().get("data")
        assert src is not None
        assert "['scores']" in src
        assert "[1]" in src


def test_composition_with_join():
    """pick used in a pipeline that also includes join."""
    from orcapod.contexts import get_default_type_converter
    converter = get_default_type_converter()

    left_table = converter.python_dicts_to_arrow_table(
        [
            {"id": 1, "meta": {"label": "alpha"}},
            {"id": 2, "meta": {"label": "beta"}},
        ],
        python_schema={"id": int, "meta": dict[str, str]},
    )
    right_table = converter.python_dicts_to_arrow_table(
        [
            {"id": 1, "value": 100},
            {"id": 2, "value": 200},
        ],
        python_schema={"id": int, "value": int},
    )
    left = ArrowTableStream(left_table, tag_columns=["id"])
    right = ArrowTableStream(right_table, tag_columns=["id"])

    result = left.pick("meta", "label").join(right)
    tag_schema, data_schema = result.output_schema()

    assert "meta" in data_schema    # now holds str, not dict
    assert "value" in data_schema
    assert data_schema["meta"] == str

    rows = list(result.iter_data())
    assert len(rows) == 2
