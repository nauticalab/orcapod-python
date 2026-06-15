import uuid as _uuid_module
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

import numpy as np
import polars as pl
import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context
from orcapod.extension_types.registry import (
    LogicalTypeRegistry,
    make_arrow_extension_type,
)
from orcapod.semantic_types import universal_converter
from orcapod.semantic_types.universal_converter import UniversalTypeConverter


def test_python_type_to_arrow_type_basic():
    assert universal_converter.python_type_to_arrow_type(int) == pa.int64()
    assert universal_converter.python_type_to_arrow_type(float) == pa.float64()
    assert universal_converter.python_type_to_arrow_type(str) == pa.large_string()
    assert universal_converter.python_type_to_arrow_type(bool) == pa.bool_()
    assert universal_converter.python_type_to_arrow_type(bytes) == pa.large_binary()


def test_python_type_to_arrow_type_datetime():
    assert universal_converter.python_type_to_arrow_type(datetime) == pa.timestamp(
        "us", tz="UTC"
    )


def test_arrow_type_to_python_type_timestamp_with_tz():
    assert (
        universal_converter.arrow_type_to_python_type(pa.timestamp("us", tz="UTC"))
        is datetime
    )


def test_arrow_type_to_python_type_timestamp_no_tz():
    assert universal_converter.arrow_type_to_python_type(pa.timestamp("us")) is datetime


def test_datetime_converter_rejects_naive():
    to_arrow, _ = universal_converter.get_conversion_functions(datetime)
    naive = datetime(2024, 1, 15, 12, 30, 45, 123456)  # no tzinfo
    with pytest.raises(ValueError, match="Naive datetime"):
        to_arrow(naive)


def test_datetime_converter_rejects_stub_tzinfo():
    """Rejects datetimes whose tzinfo.utcoffset() returns None (effectively naive)."""
    import datetime as dt_mod

    class StubTzInfo(dt_mod.tzinfo):
        def utcoffset(self, d):
            return None  # technically set but semantically naive

        def tzname(self, d):
            return "Stub"

        def dst(self, d):
            return None

    to_arrow, _ = universal_converter.get_conversion_functions(datetime)
    stub_aware = datetime(2024, 1, 15, 12, 30, 45, tzinfo=StubTzInfo())
    with pytest.raises(ValueError, match="Naive datetime"):
        to_arrow(stub_aware)


def test_datetime_converter_accepts_aware():
    to_arrow, _ = universal_converter.get_conversion_functions(datetime)
    aware = datetime(2024, 1, 15, 12, 30, 45, 123456, tzinfo=timezone.utc)
    result = to_arrow(aware)
    assert result == aware


def test_datetime_converter_accepts_non_utc_aware():
    """Non-UTC timezone-aware datetimes pass through the converter unchanged.

    PyArrow normalises the value to UTC when writing to a pa.timestamp("us", tz="UTC")
    column; the converter itself does not normalise — it only enforces the timezone
    policy for naive datetimes.
    """
    import zoneinfo

    to_arrow, _ = universal_converter.get_conversion_functions(datetime)
    eastern = zoneinfo.ZoneInfo("America/New_York")
    non_utc = datetime(2024, 1, 15, 12, 30, 45, tzinfo=eastern)
    result = to_arrow(non_utc)
    assert result == non_utc  # converter passes through unchanged


def test_datetime_converter_passes_none_through():
    """None passes through the datetime converter unchanged (PyArrow enforces nullability)."""
    to_arrow, _ = universal_converter.get_conversion_functions(datetime)
    assert to_arrow(None) is None


def test_tz_less_arrow_timestamp_reads_as_naive():
    """Reading a tz-less Arrow timestamp column produces naive (timezone-less) datetimes.

    PyArrow's ``.as_py()`` on a tz-less timestamp returns a naive datetime.  The
    converter passes it through unchanged — no UTC attachment.  To write these values
    back via the converter use the ``"coerce_utc"`` timezone policy, or attach timezone
    info manually before calling ``python_dicts_to_arrow_table``.
    """
    converter = get_default_context().type_converter
    naive_ts = datetime(2024, 5, 1, 9, 0, 0)
    table = pa.table({"ts": pa.array([naive_ts], type=pa.timestamp("us"))})

    rows_out = converter.arrow_table_to_python_dicts(table)
    result = rows_out[0]["ts"]

    assert result.tzinfo is None
    assert result == datetime(2024, 5, 1, 9, 0, 0)


def test_datetime_coerce_utc_converts_naive():
    """coerce_utc policy attaches timezone.utc to naive datetimes instead of raising."""
    converter = UniversalTypeConverter(datetime_timezone="coerce_utc")
    to_arrow = converter.get_python_to_arrow_converter(datetime)
    naive = datetime(2024, 1, 15, 12, 30, 45, 123456)
    result = to_arrow(naive)
    assert result == datetime(2024, 1, 15, 12, 30, 45, 123456, tzinfo=timezone.utc)


def test_datetime_coerce_utc_preserves_aware():
    """coerce_utc policy leaves already-aware datetimes unchanged."""
    converter = UniversalTypeConverter(datetime_timezone="coerce_utc")
    to_arrow = converter.get_python_to_arrow_converter(datetime)
    aware = datetime(2024, 1, 15, 12, 30, 45, 123456, tzinfo=timezone.utc)
    result = to_arrow(aware)
    assert result == aware


def test_datetime_round_trip():
    converter = get_default_context().type_converter
    ts = datetime(2024, 3, 15, 10, 30, 45, 123456, tzinfo=timezone.utc)
    rows_in = [{"event": "launch", "ts": ts}]

    # No explicit schema — exercises schema inference from data (type(value) -> datetime)
    table = converter.python_dicts_to_arrow_table(rows_in)

    # Arrow schema must use timestamp(us, UTC) and be non-nullable for a plain datetime field
    assert table.schema.field("ts").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("ts").nullable is False

    rows_out = converter.arrow_table_to_python_dicts(table)
    assert len(rows_out) == 1
    assert rows_out[0]["event"] == "launch"
    assert rows_out[0]["ts"] == ts


def test_optional_datetime_round_trip():
    converter = get_default_context().type_converter
    ts = datetime(2024, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
    rows_in = [
        {"label": "a", "ts": ts},
        {"label": "b", "ts": None},
    ]
    python_schema = {"label": str, "ts": datetime | None}

    table = converter.python_dicts_to_arrow_table(rows_in, python_schema=python_schema)

    assert table.schema.field("ts").type == pa.timestamp("us", tz="UTC")
    assert table.schema.field("ts").nullable is True

    rows_out = converter.arrow_table_to_python_dicts(table)
    assert rows_out[0]["ts"] == ts
    assert rows_out[1]["ts"] is None


def test_python_type_to_arrow_type_numpy():
    assert universal_converter.python_type_to_arrow_type(np.int32) == pa.int32()
    assert universal_converter.python_type_to_arrow_type(np.float64) == pa.float64()
    assert universal_converter.python_type_to_arrow_type(np.bool_) == pa.bool_()


def test_python_type_to_arrow_type_custom():
    """Path converts to an Arrow extension type when the default LogicalTypeRegistry is wired in."""
    arrow_type = universal_converter.python_type_to_arrow_type(Path)
    # Path is registered in the default logical_type_registry — expect an extension type.
    assert isinstance(arrow_type, pa.ExtensionType)
    assert arrow_type.extension_name == "pathlib.Path"
    assert pa.types.is_large_string(arrow_type.storage_type)


def test_python_type_to_arrow_type_upath():
    from upath import UPath

    arrow_type = universal_converter.python_type_to_arrow_type(UPath)
    # UPath is registered in the default logical_type_registry — expect an extension type.
    assert isinstance(arrow_type, pa.ExtensionType)
    assert arrow_type.extension_name == "upath.UPath"
    assert pa.types.is_large_string(arrow_type.storage_type)


def test_optional_upath_converter():
    """Test that Optional[UPath] correctly converts UPath values via the LogicalTypeRegistry."""
    from upath import UPath

    to_arrow, to_python = universal_converter.get_conversion_functions(UPath | None)

    # UPath is registered — python_to_storage returns the string representation.
    path = UPath("/tmp/test.txt")
    result = to_arrow(path)
    assert result == str(path)

    # Test with None
    assert to_arrow(None) is None


def test_complex_union_raises_error():
    """Test that complex unions (multiple non-None types) raise ValueError."""
    from upath import UPath

    with pytest.raises(ValueError, match="Complex unions"):
        universal_converter.get_conversion_functions(UPath | Path)

    with pytest.raises(ValueError, match="Complex unions"):
        universal_converter.python_type_to_arrow_type(UPath | Path)


def test_python_type_to_arrow_type_context():
    ctx = get_default_context()
    assert universal_converter.python_type_to_arrow_type(int, ctx) == pa.int64()


def test_python_type_to_arrow_type_unsupported():
    class CustomType:
        pass

    with pytest.raises(Exception):
        universal_converter.python_type_to_arrow_type(CustomType)


def test_arrow_type_to_python_type_basic():
    assert universal_converter.arrow_type_to_python_type(pa.int64()) is int
    assert universal_converter.arrow_type_to_python_type(pa.float64()) is float
    assert universal_converter.arrow_type_to_python_type(pa.large_string()) is str
    assert universal_converter.arrow_type_to_python_type(pa.bool_()) is bool
    assert universal_converter.arrow_type_to_python_type(pa.large_binary()) is bytes


def test_arrow_type_to_python_type_context():
    ctx = get_default_context()
    assert universal_converter.arrow_type_to_python_type(pa.int64(), ctx) is int


def test_arrow_type_to_python_type_unsupported():
    class FakeArrowType:
        pass

    with pytest.raises(Exception):
        universal_converter.arrow_type_to_python_type(
            cast(pa.DataType, FakeArrowType())
        )


def test_get_conversion_functions_basic():
    to_arrow, to_python = universal_converter.get_conversion_functions(int)
    assert callable(to_arrow)
    assert callable(to_python)
    assert to_arrow(42) == 42
    assert to_python(42) == 42


def test_get_conversion_functions_custom():
    to_arrow, to_python = universal_converter.get_conversion_functions(str)
    assert to_arrow("abc") == "abc"
    assert to_python("abc") == "abc"


def test_get_conversion_functions_context():
    ctx = get_default_context()
    to_arrow, to_python = universal_converter.get_conversion_functions(float, ctx)
    assert to_arrow(1.5) == 1.5
    assert to_python(1.5) == 1.5


def test_python_type_to_arrow_type_list():
    # Unparameterized list should raise ValueError
    with pytest.raises(ValueError):
        universal_converter.python_type_to_arrow_type(list)


def test_python_type_to_arrow_type_dict():
    # Unparameterized dict should raise ValueError
    with pytest.raises(ValueError):
        universal_converter.python_type_to_arrow_type(dict)


def test_python_type_to_arrow_type_list_of_dict():
    # For list[dict[str, int]], expect LargeListType of LargeListType of StructType
    arrow_type = universal_converter.python_type_to_arrow_type(list[dict[str, int]])
    # Should be LargeListType
    assert arrow_type.__class__.__name__.endswith("ListType")
    # Next level should also be LargeListType
    arrow_type = cast(pa.ListType, arrow_type)
    inner_list = arrow_type.value_type
    assert inner_list.__class__.__name__.endswith("ListType")
    # Innermost should be StructType
    struct_type = inner_list.value_type
    assert isinstance(struct_type, pa.StructType)
    assert struct_type[0].name == "key"
    assert struct_type[0].type == pa.large_string()
    assert struct_type[1].name == "value"
    assert struct_type[1].type == pa.int64()


def test_python_type_to_arrow_type_dict_of_list():
    # dict[str, list[int]] should be a LargeListType of StructType, with value field as LargeListType
    arrow_type = universal_converter.python_type_to_arrow_type(dict[str, list[int]])
    assert arrow_type.__class__.__name__.endswith("ListType")
    arrow_type = cast(pa.ListType, arrow_type)
    struct_type = arrow_type.value_type
    assert isinstance(struct_type, pa.StructType)
    assert struct_type[0].name == "key"
    assert struct_type[0].type == pa.large_string()
    assert struct_type[1].name == "value"
    value_type = struct_type[1].type
    assert value_type.__class__.__name__.endswith("ListType")
    assert value_type.value_type == pa.int64()


def test_python_type_to_arrow_type_list_of_list():
    arrow_type = universal_converter.python_type_to_arrow_type(list[list[int]])
    assert arrow_type.__class__.__name__.endswith("ListType")
    arrow_type = cast(pa.ListType, arrow_type)
    inner_list = arrow_type.value_type
    assert inner_list.__class__.__name__.endswith("ListType")
    assert inner_list.value_type == pa.int64()


def test_python_type_to_arrow_type_deeply_nested():
    # dict[str, list[list[dict[str, float]]]]
    complex_type = dict[str, list[list[dict[str, float]]]]
    arrow_type = universal_converter.python_type_to_arrow_type(complex_type)
    # Should be a LargeListType of StructType
    assert arrow_type.__class__.__name__.endswith("ListType")
    arrow_type = cast(pa.ListType, arrow_type)
    struct_type = arrow_type.value_type
    assert isinstance(struct_type, pa.StructType)
    assert struct_type[0].name == "key"
    assert struct_type[0].type == pa.large_string()
    assert struct_type[1].name == "value"
    outer_list = struct_type[1].type
    assert outer_list.__class__.__name__.endswith("ListType")
    inner_list = outer_list.value_type
    assert inner_list.__class__.__name__.endswith("ListType")
    inner_struct_list = inner_list.value_type
    assert inner_struct_list.__class__.__name__.endswith("ListType")
    inner_struct = inner_struct_list.value_type
    assert isinstance(inner_struct, pa.StructType)
    assert inner_struct[0].name == "key"
    assert inner_struct[0].type == pa.large_string()
    assert inner_struct[1].name == "value"
    assert inner_struct[1].type == pa.float64()


# Roundtrip tests for complex types
def test_roundtrip_list_of_int():
    py_val = [1, 2, 3, 4]
    to_arrow, to_python = universal_converter.get_conversion_functions(list[int])
    arr = to_arrow(py_val)
    py_val2 = to_python(arr)
    assert py_val == py_val2


def test_roundtrip_dict_str_int():
    py_val = {"a": 1, "b": 2}
    to_arrow, to_python = universal_converter.get_conversion_functions(dict[str, int])
    arr = to_arrow(py_val)
    py_val2 = to_python(arr)
    # dict roundtrip may come back as dict or list of pairs
    if isinstance(py_val2, dict):
        assert py_val == py_val2
    else:
        # Accept list of pairs
        assert sorted(py_val.items()) == sorted(
            [(d["key"], d["value"]) for d in py_val2]
        )


def test_roundtrip_list_of_list_of_float():
    py_val = [[1.1, 2.2], [3.3, 4.4]]
    to_arrow, to_python = universal_converter.get_conversion_functions(
        list[list[float]]
    )
    arr = to_arrow(py_val)
    py_val2 = to_python(arr)
    assert py_val == py_val2


def test_roundtrip_set_of_int():
    py_val = {1, 2, 3}
    to_arrow, to_python = universal_converter.get_conversion_functions(set[int])
    arr = to_arrow(py_val)
    py_val2 = to_python(arr)
    # set will come back as list
    assert py_val != py_val2
    assert set(py_val) == set(py_val2)


def test_roundtrip_various_complex_types():
    cases = [
        ([1, 2, 3], list[int]),
        ([["a", "b"], ["c"]], list[list[str]]),
        ({"a": 1, "b": 2}, dict[str, int]),
        ([{"x": 1.1, "y": 2.2}, {"x": 3.3, "y": 4.4}], list[dict[str, float]]),
        ({"a": [1, 2], "b": [3]}, dict[str, list[int]]),
        (
            [{"a": [1, 2]}, {"b": [3], "c": [4, 5, 6]}],
            list[dict[str, list[int]]],
        ),
        (
            [[{"k": "a", "v": 1.1}, {"k": "b", "v": 2.2}], [{"k": "c", "v": 3.3}]],
            list[list[dict[str, float]]],
        ),
        (
            {"outer": [{"inner": [1, 2]}, {"inner": [3, 4]}]},
            dict[str, list[dict[str, list[int]]]],
        ),
        ({"a": {"b": {"c": 42}}}, dict[str, dict[str, dict[str, int]]]),
        ({"a": None, "b": 2}, dict[str, int]),
        (
            [{"x": [1, 2], "y": [3, 4]}, {"x": [5], "y": [6, 7]}],
            list[dict[str, list[int]]],
        ),
    ]
    for py_val, typ in cases:
        to_arrow, to_python = universal_converter.get_conversion_functions(typ)
        arr = to_arrow(py_val)
        py_val2 = to_python(arr)
        assert py_val == py_val2, f"Failed roundtrip for type {typ} with value {py_val}"


def test_incomplete_roundtrip_types():
    cases = [({"a": {1, 2}, "b": {3}}, dict[str, set[int]], {"a": [1, 2], "b": [3]})]

    for py_val, typ, expected_return in cases:
        to_arrow, to_python = universal_converter.get_conversion_functions(typ)
        arr = to_arrow(py_val)
        py_val2 = to_python(arr)
        assert py_val2 == expected_return, (
            f"Failed roundtrip for type {typ} with value {py_val}"
        )


def test_roundtrip_minimal_key_list_issue():
    py_val = [{"test": [1, 2, 3], "next": [3, 4]}]
    typ = list[dict[str, list[int]]]
    to_arrow, to_python = universal_converter.get_conversion_functions(typ)
    arr = to_arrow(py_val)
    py_val2 = to_python(arr)
    print("Original:", py_val)
    print("Roundtrip:", py_val2)
    assert py_val == py_val2


def test_roundtrip_simpler_key_issue_dict_str_list():
    py_val = {"a": [1, 2]}
    typ = dict[str, list[int]]
    to_arrow, to_python = universal_converter.get_conversion_functions(typ)
    arr = to_arrow(py_val)
    py_val2 = to_python(arr)
    print("Original dict[str, list[int]]:", py_val)
    print("Roundtrip:", py_val2)
    assert py_val == py_val2


def test_roundtrip_simpler_key_issue_list_dict_str_int():
    py_val = [{"key": "a", "value": 1}]
    typ = list[dict[str, int]]
    to_arrow, to_python = universal_converter.get_conversion_functions(typ)
    arr = to_arrow(py_val)
    py_val2 = to_python(arr)
    print("Original list[dict[str, int]]:", py_val)
    print("Roundtrip:", py_val2)
    assert py_val == py_val2


def test_inspect_arrow_schema_dict_str_list():
    py_val = {"test": [1, 2]}
    typ = dict[str, list[int]]
    arrow_type = universal_converter.python_type_to_arrow_type(typ)
    print("Arrow type for dict[str, list[int]]:", arrow_type)
    to_arrow_struct, to_python = universal_converter.get_conversion_functions(typ)
    arr = to_arrow_struct(py_val)
    assert arr == [{"key": "test", "value": [1, 2]}]


def test_schema_as_required_strips_optional_fields():
    from orcapod.types import Schema

    s = Schema({"a": int, "b": str}, optional_fields=["b"])
    result = s.as_required()
    assert result == Schema({"a": int, "b": str})
    assert result.optional_fields == frozenset()


def test_schema_as_required_idempotent():
    from orcapod.types import Schema

    s = Schema({"a": int, "b": str}, optional_fields=["a", "b"])
    once = s.as_required()
    twice = s.as_required().as_required()
    assert once == twice


def test_python_schema_to_arrow_non_nullable():
    """Plain types (no | None) must produce nullable=False Arrow fields."""
    from orcapod.types import Schema

    ctx = get_default_context()
    schema = ctx.type_converter.python_schema_to_arrow_schema(
        Schema({"a": int, "b": str, "c": float, "d": bool, "e": bytes})
    )
    for name in ("a", "b", "c", "d", "e"):
        assert schema.field(name).nullable is False, (
            f"Field '{name}' should be nullable=False for a plain type"
        )


def test_python_schema_to_arrow_optional_nullable():
    """Optional types (T | None) must produce nullable=True Arrow fields."""
    from orcapod.types import Schema

    ctx = get_default_context()
    schema = ctx.type_converter.python_schema_to_arrow_schema(
        Schema({"x": int | None, "y": str | None})
    )
    assert schema.field("x").nullable is True
    assert schema.field("y").nullable is True


def test_arrow_schema_to_python_nullable_becomes_optional():
    """nullable=True Arrow fields must reconstruct as T | None."""
    ctx = get_default_context()
    arrow_schema = pa.schema([pa.field("x", pa.int64(), nullable=True)])
    python_schema = ctx.type_converter.arrow_schema_to_python_schema(arrow_schema)
    assert python_schema["x"] == int | None


def test_arrow_schema_to_python_non_nullable_stays_plain():
    """nullable=False Arrow fields must reconstruct as plain T."""
    ctx = get_default_context()
    arrow_schema = pa.schema([pa.field("x", pa.int64(), nullable=False)])
    python_schema = ctx.type_converter.arrow_schema_to_python_schema(arrow_schema)
    assert python_schema["x"] == int


def test_round_trip_preserves_optionality():
    """Python schema → Arrow → Python schema is lossless for nullable/non-nullable."""
    from orcapod.types import Schema

    ctx = get_default_context()
    original = Schema({"required": int, "nullable_field": int | None})
    arrow = ctx.type_converter.python_schema_to_arrow_schema(original)
    recovered = ctx.type_converter.arrow_schema_to_python_schema(arrow)

    assert recovered["required"] == int
    assert recovered["nullable_field"] == int | None
    assert recovered == original


# ---------------------------------------------------------------------------
# ENG-389: Any <-> pa.null() round-trip
# ---------------------------------------------------------------------------


def test_any_to_arrow_type():
    """typing.Any maps to pa.null()."""
    assert universal_converter.python_type_to_arrow_type(Any) == pa.null()


def test_list_any_to_arrow_type():
    """list[Any] maps to pa.large_list(pa.null())."""
    assert (
        universal_converter.python_type_to_arrow_type(list[Any])
        == pa.large_list(pa.null())
    )


def test_dict_any_any_to_arrow_type():
    """dict[Any, Any] maps to pa.large_list(pa.struct([("key", pa.null()), ("value", pa.null())]))."""
    expected = pa.large_list(
        pa.struct([("key", pa.null()), ("value", pa.null())])
    )
    assert universal_converter.python_type_to_arrow_type(dict[Any, Any]) == expected


def test_null_arrow_to_any_python_type():
    """pa.null() maps back to typing.Any."""
    assert universal_converter.arrow_type_to_python_type(pa.null()) is Any


def test_list_any_round_trip():
    """list[Any] round-trips: list[Any] -> pa.large_list(pa.null()) -> list[Any]."""
    arrow_type = universal_converter.python_type_to_arrow_type(list[Any])
    assert universal_converter.arrow_type_to_python_type(arrow_type) == list[Any]


def test_dict_any_any_round_trip():
    """dict[Any, Any] round-trips through Arrow and back to dict[Any, Any]."""
    arrow_type = universal_converter.python_type_to_arrow_type(dict[Any, Any])
    assert universal_converter.arrow_type_to_python_type(arrow_type) == dict[Any, Any]


def test_empty_container_inference_to_arrow_no_error():
    """Inferring schema from empty containers and converting to Arrow does not raise."""
    from orcapod.semantic_types.pydata_utils import infer_python_schema_from_pylist_data
    from orcapod.semantic_types.universal_converter import UniversalTypeConverter

    schema = infer_python_schema_from_pylist_data([{"items": [], "meta": {}}])
    converter = UniversalTypeConverter()
    # Must not raise ValueError: Unsupported Python type: typing.Any
    arrow_schema = converter.python_schema_to_arrow_schema(schema)
    assert "items" in [f.name for f in arrow_schema]
    assert "meta" in [f.name for f in arrow_schema]


def test_pyarrow_empty_list_with_null_type():
    """PyArrow accepts empty lists for pa.large_list(pa.null()) and pa.large_list(pa.struct(...)) columns."""
    schema = pa.schema([
        pa.field("items", pa.large_list(pa.null())),
        pa.field("meta", pa.large_list(pa.struct([("key", pa.null()), ("value", pa.null())]))),
    ])
    table = pa.Table.from_pylist([{"items": [], "meta": []}], schema=schema)
    assert table.num_rows == 1
    assert table.schema.field("items").type == pa.large_list(pa.null())


# ── LogicalTypeRegistry priority tests ───────────────────────────────────────


def _make_logical_type_stub(py_type: type, arrow_name: str):
    """Return a minimal LogicalTypeProtocol conforming stub."""
    _ArrowExtClass = make_arrow_extension_type(arrow_name, pa.large_string())

    class _PolarsExt(pl.BaseExtension):
        def __init__(self):
            super().__init__(arrow_name, pl.String, None)
        @classmethod
        def ext_from_params(cls, ext_name, storage_dtype, metadata_str):
            return cls()

    class _Stub:
        logical_type_name = arrow_name
        python_type = py_type

        def get_arrow_extension_type(self):
            return _ArrowExtClass()

        def get_polars_extension_type(self):
            return _PolarsExt()

        def python_to_storage(self, value):
            return str(value)

        def storage_to_python(self, storage_value):
            return storage_value

    return _Stub()


class _MyCustomClass:
    pass


def test_converter_uses_logical_type_registry_for_registered_type():
    """When a LogicalType is registered, converter returns its Arrow extension type."""
    arrow_name = f"test.MyCustomClass.{_uuid_module.uuid4().hex[:8]}"
    lt = _make_logical_type_stub(_MyCustomClass, arrow_name)

    registry = LogicalTypeRegistry()
    registry.register_logical_type(lt)

    converter = UniversalTypeConverter(logical_type_registry=registry)

    result = converter.python_type_to_arrow_type(_MyCustomClass)
    expected_ext = lt.get_arrow_extension_type()
    assert result == expected_ext


def test_converter_falls_through_for_unregistered_type():
    """If type not in LogicalTypeRegistry, converter falls through to old system (int → int64)."""
    registry = LogicalTypeRegistry()
    converter = UniversalTypeConverter(logical_type_registry=registry)

    result = converter.python_type_to_arrow_type(int)
    assert result == pa.int64()


def test_converter_without_registry_unchanged():
    """With no logical_type_registry, converter behaves exactly as before."""
    converter = UniversalTypeConverter()
    assert converter.python_type_to_arrow_type(str) == pa.large_string()


def test_data_context_type_converter_holds_logical_type_registry():
    """DataContext's type_converter is constructed with the same logical_type_registry."""
    from orcapod.contexts import get_default_context
    ctx = get_default_context()
    assert hasattr(ctx.type_converter, "_logical_type_registry")
    assert ctx.type_converter._logical_type_registry is ctx.logical_type_registry
