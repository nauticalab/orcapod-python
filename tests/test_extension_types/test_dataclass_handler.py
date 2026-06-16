"""Tests for DataclassLogicalType and DataclassHandlerFactory (PLT-1657)."""

from __future__ import annotations

import dataclasses
import uuid

import pyarrow as pa
import polars as pl
import pytest

from orcapod.extension_types.protocols import LogicalTypeProtocol, ResolutionContext


# ---------------------------------------------------------------------------
# Shared dataclass fixtures (module-level so Arrow extension names are stable)
# ---------------------------------------------------------------------------

@dataclasses.dataclass
class Flat:
    x: int
    y: str


@dataclasses.dataclass
class AllPrimitives:
    i: int
    f: float
    s: str
    b: bool
    by: bytes


@dataclasses.dataclass
class WithList:
    items: list[int]


@dataclasses.dataclass
class Inner:
    a: int


@dataclasses.dataclass
class Outer:
    inner: Inner
    z: str


# Cyclic fixtures — must be module-level so get_type_hints resolves the string
# annotations ('_SelfRef', '_IndirectB', '_IndirectA') in module globals.

@dataclasses.dataclass
class _SelfRef:
    value: int
    child: _SelfRef  # type: ignore[name-defined]  # PEP 563 → string; resolved at get_type_hints time


@dataclasses.dataclass
class _IndirectA:
    value: int
    b: _IndirectB  # type: ignore[name-defined]  # forward ref; _IndirectB defined below


@dataclasses.dataclass
class _IndirectB:
    a: _IndirectA


# ---------------------------------------------------------------------------
# DataclassLogicalType — unit tests
# ---------------------------------------------------------------------------

def _make_flat_lt():
    """Construct a DataclassLogicalType for Flat without using the full factory."""
    from orcapod.extension_types.dataclass_handler import DataclassLogicalType
    import pyarrow as pa
    storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.large_string())])
    identity = lambda v: v
    field_converters = [("x", identity, identity), ("y", identity, identity)]
    fqcn = f"{Flat.__module__}.{Flat.__qualname__}"
    return DataclassLogicalType(fqcn, Flat, storage, field_converters)


def test_dataclass_logical_type_satisfies_protocol():
    lt = _make_flat_lt()
    assert isinstance(lt, LogicalTypeProtocol)


def test_dataclass_logical_type_logical_name():
    lt = _make_flat_lt()
    expected = f"{Flat.__module__}.{Flat.__qualname__}"
    assert lt.logical_type_name == expected


def test_dataclass_logical_type_python_type():
    lt = _make_flat_lt()
    assert lt.python_type is Flat


def test_dataclass_logical_type_get_arrow_extension_type():
    lt = _make_flat_lt()
    ext = lt.get_arrow_extension_type()
    assert isinstance(ext, pa.ExtensionType)
    assert ext.extension_name == lt.logical_type_name
    expected_storage = pa.struct([pa.field("x", pa.int64()), pa.field("y", pa.large_string())])
    assert ext.storage_type == expected_storage


def test_dataclass_logical_type_get_arrow_extension_type_cached():
    lt = _make_flat_lt()
    ext1 = lt.get_arrow_extension_type()
    ext2 = lt.get_arrow_extension_type()
    assert ext1 is ext2


def test_dataclass_logical_type_get_polars_extension_type():
    lt = _make_flat_lt()
    polars_ext = lt.get_polars_extension_type()
    assert isinstance(polars_ext, pl.BaseExtension)


def test_dataclass_logical_type_get_polars_extension_type_cached():
    lt = _make_flat_lt()
    p1 = lt.get_polars_extension_type()
    p2 = lt.get_polars_extension_type()
    assert p1 is p2


def test_dataclass_logical_type_arrow_metadata_contains_category():
    lt = _make_flat_lt()
    ext = lt.get_arrow_extension_type()
    import json
    meta = json.loads(ext.__arrow_ext_serialize__().decode("utf-8"))
    assert meta["category"] == "orcapod.dataclass"


def test_dataclass_logical_type_python_to_storage():
    lt = _make_flat_lt()
    result = lt.python_to_storage(Flat(x=7, y="hello"))
    assert result == {"x": 7, "y": "hello"}


def test_dataclass_logical_type_storage_to_python():
    lt = _make_flat_lt()
    result = lt.storage_to_python({"x": 7, "y": "hello"})
    assert result == Flat(x=7, y="hello")
    assert isinstance(result, Flat)
