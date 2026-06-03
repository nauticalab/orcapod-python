# tests/test_semantic_types/test_dataclass_encoding.py
from __future__ import annotations

import dataclasses

import pytest

from orcapod.semantic_types.dataclass_encoding import (
    DATACLASS_TYPE_FIELD,
    DATACLASS_TYPE_PREFIX,
    _DATACLASS_REGISTRY,
    register_dataclass,
)


@dataclasses.dataclass
class _Simple:
    a: int
    b: str


def test_constants():
    assert DATACLASS_TYPE_FIELD == "__type"
    assert DATACLASS_TYPE_PREFIX == "dataclass:"


def test_register_explicit():
    register_dataclass(_Simple)
    key = f"{_Simple.__module__}.{_Simple.__qualname__}"
    assert _DATACLASS_REGISTRY[key] is _Simple


def test_register_returns_class():
    result = register_dataclass(_Simple)
    assert result is _Simple


def test_register_as_decorator():
    @register_dataclass
    @dataclasses.dataclass
    class _Decorated:
        x: float

    key = f"{_Decorated.__module__}.{_Decorated.__qualname__}"
    assert _DATACLASS_REGISTRY[key] is _Decorated


def test_register_non_dataclass_raises():
    with pytest.raises(TypeError, match="not a dataclass"):
        register_dataclass(int)


import typing

import pyarrow as pa
from orcapod.semantic_types.dataclass_encoding import (
    DATACLASS_TYPE_FIELD,
    has_dataclass_type_sentinel,
    dataclass_to_arrow_struct_type,
    dataclass_to_struct_dict,
)
from orcapod.semantic_types.universal_converter import UniversalTypeConverter


def test_sentinel_large_string():
    t = pa.struct([pa.field("__type", pa.large_string()), pa.field("a", pa.int64())])
    assert has_dataclass_type_sentinel(t) is True


def test_sentinel_string_compat():
    # older Arrow versions wrote pa.string() instead of pa.large_string()
    t = pa.struct([pa.field("__type", pa.string()), pa.field("a", pa.int64())])
    assert has_dataclass_type_sentinel(t) is True


def test_sentinel_missing_field():
    t = pa.struct([pa.field("a", pa.int64()), pa.field("b", pa.large_string())])
    assert has_dataclass_type_sentinel(t) is False


def test_sentinel_non_struct():
    assert has_dataclass_type_sentinel(pa.int64()) is False


def test_struct_type_basic_fields():
    @dataclasses.dataclass
    class _Point:
        x: int
        y: float

    converter = UniversalTypeConverter()
    result = dataclass_to_arrow_struct_type(_Point, converter)

    assert pa.types.is_struct(result)
    # __type must be the first field
    assert result[0].name == "__type"
    assert result[0].type == pa.large_string()
    assert result.field("x").type == pa.int64()
    assert result.field("y").type == pa.float64()


def test_struct_type_string_field():
    @dataclasses.dataclass
    class _Named:
        name: str

    converter = UniversalTypeConverter()
    result = dataclass_to_arrow_struct_type(_Named, converter)
    assert result.field("name").type == pa.large_string()


def test_struct_type_non_dataclass_raises():
    converter = UniversalTypeConverter()
    with pytest.raises(TypeError, match="not a dataclass"):
        dataclass_to_arrow_struct_type(int, converter)


def _build_field_converters(cls: type, converter: UniversalTypeConverter) -> dict:
    """Helper: build per-field Python-to-Arrow converters for a dataclass."""
    hints = typing.get_type_hints(cls)
    return {
        f.name: converter.get_python_to_arrow_converter(hints[f.name])
        for f in dataclasses.fields(cls)
    }


def test_struct_dict_simple():
    @dataclasses.dataclass
    class _Box:
        width: int
        label: str

    converter = UniversalTypeConverter()
    field_converters = _build_field_converters(_Box, converter)
    obj = _Box(width=10, label="big")
    result = dataclass_to_struct_dict(obj, field_converters)

    fqcn = f"{_Box.__module__}.{_Box.__qualname__}"
    assert result[DATACLASS_TYPE_FIELD] == f"dataclass:{fqcn}"
    assert result["width"] == 10
    assert result["label"] == "big"


def test_struct_dict_type_error_on_class():
    with pytest.raises(TypeError, match="not a dataclass instance"):
        dataclass_to_struct_dict(_Simple, {})


def test_struct_dict_type_error_on_non_dataclass():
    with pytest.raises(TypeError, match="not a dataclass instance"):
        dataclass_to_struct_dict(42, {})


from unittest.mock import MagicMock, patch
from orcapod.semantic_types.dataclass_encoding import struct_dict_to_dataclass


@dataclasses.dataclass
class _TierOne:
    value: int


def test_tier1_import():
    """Tier 1: class is importable via importlib."""
    fqcn = f"{_TierOne.__module__}.{_TierOne.__qualname__}"
    struct_dict = {
        "__type": f"dataclass:{fqcn}",
        "value": 7,
    }
    field_converters = {"value": lambda v: v}
    cache: dict = {}

    # Patch importlib so tier 1 returns _TierOne
    module_path, _, class_attr = fqcn.rpartition(".")
    with patch("orcapod.semantic_types.dataclass_encoding.importlib.import_module") as mock_import:
        mock_mod = MagicMock()
        setattr(mock_mod, class_attr, _TierOne)
        mock_import.return_value = mock_mod

        result = struct_dict_to_dataclass(struct_dict, field_converters, cache)

    assert isinstance(result, _TierOne)
    assert result.value == 7
    # Cache should be populated
    assert cache[fqcn] is _TierOne


def test_tier1_cache_hit():
    """Tier 1: cache hit skips importlib entirely."""
    fqcn = "some.module.SomeClass"
    cache = {fqcn: _TierOne}
    struct_dict = {"__type": f"dataclass:{fqcn}", "value": 3}
    field_converters = {"value": lambda v: v}

    with patch("orcapod.semantic_types.dataclass_encoding.importlib.import_module") as mock_import:
        result = struct_dict_to_dataclass(struct_dict, field_converters, cache)
        mock_import.assert_not_called()

    assert isinstance(result, _TierOne)
    assert result.value == 3


def test_tier2_registry(monkeypatch):
    """Tier 2: importlib fails, class found in registry."""
    @dataclasses.dataclass
    class _RegClass:
        score: float

    fqcn = "fake.module.RegClass"
    monkeypatch.setitem(_DATACLASS_REGISTRY, fqcn, _RegClass)

    struct_dict = {"__type": f"dataclass:{fqcn}", "score": 9.5}
    field_converters = {"score": lambda v: v}
    cache: dict = {}

    with patch("importlib.import_module", side_effect=ImportError("no module")):
        result = struct_dict_to_dataclass(struct_dict, field_converters, cache)

    assert isinstance(result, _RegClass)
    assert result.score == 9.5
    assert cache[fqcn] is _RegClass


def test_tier3_synthesize():
    """Tier 3: neither importable nor registered — synthesize a dataclass."""
    fqcn = "totally.unknown.Ghost"
    struct_dict = {"__type": f"dataclass:{fqcn}", "name": "phantom", "age": 99}
    field_converters = {"name": lambda v: v, "age": lambda v: v}
    cache: dict = {}

    with patch("importlib.import_module", side_effect=ImportError("no module")):
        result = struct_dict_to_dataclass(struct_dict, field_converters, cache)

    assert dataclasses.is_dataclass(result)
    assert result.name == "phantom"  # type: ignore[attr-defined]
    assert result.age == 99  # type: ignore[attr-defined]
    # Synthesized class cached under fqcn for future rows
    assert fqcn in cache


def test_missing_type_field_tier3():
    """Struct without __type falls through to tier 3 silently."""
    struct_dict = {"value": 42}
    field_converters = {"value": lambda v: v}
    cache: dict = {}

    result = struct_dict_to_dataclass(struct_dict, field_converters, cache)

    assert dataclasses.is_dataclass(result)
    assert result.value == 42  # type: ignore[attr-defined]
    # No cache entry — no valid fqcn to cache under
    assert len(cache) == 0


def test_malformed_type_field_tier3():
    """Invalid __type format (fails regex) falls through to tier 3."""
    struct_dict = {"__type": "not-valid!!!", "x": 1}
    field_converters = {"x": lambda v: v}
    cache: dict = {}

    result = struct_dict_to_dataclass(struct_dict, field_converters, cache)

    assert dataclasses.is_dataclass(result)
    assert result.x == 1  # type: ignore[attr-defined]


from orcapod.semantic_types.universal_converter import UniversalTypeConverter as _UTC


def test_utc_simple_round_trip():
    """Full encode->decode round-trip through UniversalTypeConverter."""
    @dataclasses.dataclass
    class _Color:
        r: int
        g: int
        b: int

    converter = _UTC()
    arrow_type = converter.python_type_to_arrow_type(_Color)
    assert has_dataclass_type_sentinel(arrow_type)

    obj = _Color(r=255, g=128, b=0)
    encode = converter.get_python_to_arrow_converter(_Color)
    encoded = encode(obj)
    assert encoded["__type"] == f"dataclass:{_Color.__module__}.{_Color.__qualname__}"

    decode = converter.get_arrow_to_python_converter(arrow_type)
    with patch("orcapod.semantic_types.dataclass_encoding.importlib.import_module") as mock_import:
        mock_mod = MagicMock()
        setattr(mock_mod, "_Color", _Color)
        mock_import.return_value = mock_mod
        result = decode(encoded)

    assert isinstance(result, _Color)
    assert result.r == 255 and result.g == 128 and result.b == 0


def test_utc_nested_round_trip():
    """Nested dataclass encodes and decodes recursively."""
    @dataclasses.dataclass
    class _Inner:
        y: float

    @dataclasses.dataclass
    class _Outer:
        x: int
        inner: _Inner

    converter = _UTC()
    arrow_type = converter.python_type_to_arrow_type(_Outer)

    # Nested struct: inner field should itself be a __type-bearing struct
    inner_arrow = arrow_type.field("inner").type
    assert has_dataclass_type_sentinel(inner_arrow)

    obj = _Outer(x=1, inner=_Inner(y=3.14))
    encode = converter.get_python_to_arrow_converter(_Outer)
    encoded = encode(obj)

    assert encoded["inner"]["__type"] == f"dataclass:{_Inner.__module__}.{_Inner.__qualname__}"
    assert encoded["inner"]["y"] == 3.14

    decode = converter.get_arrow_to_python_converter(arrow_type)

    inner_fqcn = f"{_Inner.__module__}.{_Inner.__qualname__}"
    outer_fqcn = f"{_Outer.__module__}.{_Outer.__qualname__}"
    inner_attr = inner_fqcn.rpartition(".")[2]
    outer_attr = outer_fqcn.rpartition(".")[2]

    with patch("orcapod.semantic_types.dataclass_encoding.importlib.import_module") as mock_import:
        def fake_import(module_path):
            mod = MagicMock()
            setattr(mod, inner_attr, _Inner)
            setattr(mod, outer_attr, _Outer)
            return mod
        mock_import.side_effect = fake_import
        result = decode(encoded)

    assert isinstance(result, _Outer)
    assert result.x == 1
    assert isinstance(result.inner, _Inner)
    assert result.inner.y == 3.14


def test_utc_clear_cache_clears_dataclass_cache():
    """clear_cache() also clears the per-instance dataclass lookup cache."""
    converter = _UTC()

    @dataclasses.dataclass
    class _Temp:
        n: int

    fqcn = f"{_Temp.__module__}.{_Temp.__qualname__}"
    converter._dataclass_lookup_cache[fqcn] = _Temp
    converter.clear_cache()
    assert fqcn not in converter._dataclass_lookup_cache
