from pathlib import Path, PosixPath

import pytest

from orcapod.semantic_types import pydata_utils


def test_pylist_to_pydict_typical():
    data = [{"a": 1, "b": 2}, {"a": 3, "c": 4}]
    result = pydata_utils.pylist_to_pydict(data)
    assert result == {"a": [1, 3], "b": [2, None], "c": [None, 4]}


def test_pylist_to_pydict_missing_keys():
    data = [{"a": 1}, {"b": 2}, {"a": 3, "b": 4}]
    result = pydata_utils.pylist_to_pydict(data)
    assert result == {"a": [1, None, 3], "b": [None, 2, 4]}


def test_pylist_to_pydict_empty():
    assert pydata_utils.pylist_to_pydict([]) == {}


def test_pylist_to_pydict_empty_dicts():
    data = [{}, {}, {}]
    assert pydata_utils.pylist_to_pydict(data) == {}


def test_pydict_to_pylist_typical():
    data = {"a": [1, 3], "b": [2, None], "c": [None, 4]}
    result = pydata_utils.pydict_to_pylist(data)
    assert result == [{"a": 1, "b": 2, "c": None}, {"a": 3, "b": None, "c": 4}]


def test_pydict_to_pylist_uneven_lengths():
    data = {"a": [1, 2], "b": [3]}
    with pytest.raises(ValueError):
        pydata_utils.pydict_to_pylist(data)


def test_pydict_to_pylist_empty():
    assert pydata_utils.pydict_to_pylist({}) == []


def test_pydict_to_pylist_empty_lists():
    data = {"a": [], "b": []}
    assert pydata_utils.pydict_to_pylist(data) == []


def test_infer_python_schema_from_pylist_data_typical():
    data = [{"a": 1, "b": 2.0}, {"a": 3, "b": None}]
    schema = pydata_utils.infer_python_schema_from_pylist_data(data)
    assert schema["a"] in (int, int | None)
    assert schema["b"] in (float | None, float)


def test_infer_python_schema_from_pylist_data_complex():
    data = [
        {"path": Path("/tmp/file1"), "size": 123},
        {"path": Path("/tmp/file2"), "size": None},
    ]
    schema = pydata_utils.infer_python_schema_from_pylist_data(data)
    assert schema["path"] in (Path, PosixPath)
    assert schema["size"] == int | None


def test_infer_python_schema_from_pylist_data_empty():
    assert pydata_utils.infer_python_schema_from_pylist_data([]) == {}


def test_infer_python_schema_from_pylist_data_mixed_types():
    data = [{"a": 1}, {"a": "x"}, {"a": 2.5}]
    schema = pydata_utils.infer_python_schema_from_pylist_data(data)
    # Should be Union[int, float, str] or Any
    assert "a" in schema


def test_infer_python_schema_from_pydict_data_typical():
    data = {"a": [1, 2], "b": [None, 3.5]}
    schema = pydata_utils.infer_python_schema_from_pydict_data(data)
    assert schema["a"] in (int, int | None)
    assert schema["b"] in (float | None, float)


def test_infer_python_schema_from_pydict_data_empty():
    assert pydata_utils.infer_python_schema_from_pydict_data({}) == {}


def test_infer_python_schema_from_pydict_data_empty_lists():
    data = {"a": [], "b": []}
    schema = pydata_utils.infer_python_schema_from_pydict_data(data)
    assert schema["a"] == str | None
    assert schema["b"] == str | None


def test_infer_python_schema_from_pydict_data_mixed_types():
    data = {"a": [1, "x", 2.5]}
    schema = pydata_utils.infer_python_schema_from_pydict_data(data)
    assert "a" in schema


def test_round_trip_pylist_pydict():
    data = [{"a": 1, "b": 2}, {"a": 3, "c": 4}]
    pydict = pydata_utils.pylist_to_pydict(data)
    pylist = pydata_utils.pydict_to_pylist(pydict)
    # Should be equivalent to original data (order of keys may differ)
    for orig, roundtrip in zip(data, pylist):
        # Compare dicts for value equality, ignoring key order and missing keys
        for k in orig:
            assert orig[k] == roundtrip[k]


def test_round_trip_pydict_pylist():
    data = {"a": [1, 3], "b": [2, None], "c": [None, 4]}
    pylist = pydata_utils.pydict_to_pylist(data)
    pydict = pydata_utils.pylist_to_pydict(pylist)
    for k in data:
        assert pydict[k] == data[k]
