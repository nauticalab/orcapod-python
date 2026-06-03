# tests/test_semantic_types/test_dataclass_encoding.py
from __future__ import annotations

import dataclasses
import typing

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
