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


import pyarrow as pa
from orcapod.semantic_types.dataclass_encoding import has_dataclass_type_sentinel


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
