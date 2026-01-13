from typing import cast
import pytest
import pyarrow as pa
import numpy as np
from pathlib import Path
from orcapod.semantic_types import universal_converter
from orcapod.contexts import get_default_context


def test_python_type_to_arrow_type_basic():
    assert universal_converter.python_type_to_arrow_type(int) == pa.int64()
    assert universal_converter.python_type_to_arrow_type(float) == pa.float64()
    assert universal_converter.python_type_to_arrow_type(str) == pa.large_string()
    assert universal_converter.python_type_to_arrow_type(bool) == pa.bool_()
    assert universal_converter.python_type_to_arrow_type(bytes) == pa.large_binary()


def test_python_type_to_arrow_type_numpy():
    assert universal_converter.python_type_to_arrow_type(np.int32) == pa.int32()
    assert universal_converter.python_type_to_arrow_type(np.float64) == pa.float64()
    assert universal_converter.python_type_to_arrow_type(np.bool_) == pa.bool_()


def test_python_type_to_arrow_type_custom():
    arrow_type = universal_converter.python_type_to_arrow_type(Path)
    # Should be a StructType with field 'path' of type large_string
    assert isinstance(arrow_type, pa.StructType)
    assert len(arrow_type) == 1
    field = arrow_type[0]
    assert field.name == "path"
    assert field.type == pa.large_string()

def test_python_type_to_arrow_type_upath():
    from upath import UPath
    arrow_type = universal_converter.python_type_to_arrow_type(UPath)
    # Should be a StructType with field 'upath' of type large_string
    assert isinstance(arrow_type, pa.StructType)
    assert len(arrow_type) == 1
    field = arrow_type[0]
    assert field.name == "upath"
    assert field.type == pa.large_string()

def test_optional_upath_converter():
    """Test that Optional[UPath] correctly converts UPath values."""
    from upath import UPath
    
    to_arrow, to_python = universal_converter.get_conversion_functions(UPath | None)
    
    # Test with UPath value
    path = UPath("/tmp/test.txt")
    result = to_arrow(path)
    assert result == {"upath": "/tmp/test.txt"}
    
    # Test with None
    assert to_arrow(None) is None


def test_complex_union_raises_error():
    """Test that complex unions (multiple non-None types) raise ValueError."""
    from upath import UPath
    from pathlib import Path
    
    with pytest.raises(ValueError, match="Complex unions"):
        universal_converter.get_conversion_functions(UPath | Path)

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
