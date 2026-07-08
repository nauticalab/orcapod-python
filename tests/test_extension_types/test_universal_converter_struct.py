"""Unit tests for UniversalTypeConverter behaviour with plain Arrow struct types.

A plain struct (no ARROW:extension:* field metadata) should be inferred as a
dynamic TypedDict by arrow_schema_to_python_schema, and that TypedDict should
round-trip back to the identical Arrow struct type via python_type_to_arrow_type.
"""
from __future__ import annotations

import typing

import pyarrow as pa
import pytest

from orcapod.contexts import get_default_context


# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

PLAIN_STRUCT = pa.struct([
    pa.field("total", pa.int64()),
    pa.field("delta", pa.int64()),
])


# ---------------------------------------------------------------------------
# Fixture
# ---------------------------------------------------------------------------


@pytest.fixture
def converter():
    """``UniversalTypeConverter`` from the default ``DataContext``.

    Note: ``get_default_context()`` returns a cached singleton, so the returned
    converter may be shared across tests. The tests in this module are written to
    be order-independent under that constraint.
    """
    return get_default_context().type_converter


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_plain_struct_infers_as_dynamic_typeddict(converter):
    """arrow_schema_to_python_schema on a plain struct returns a dynamic TypedDict, not Any."""
    schema = pa.schema([pa.field("result", PLAIN_STRUCT, nullable=False)])
    python_schema = converter.arrow_schema_to_python_schema(schema)

    result_type = python_schema["result"]

    assert typing.is_typeddict(result_type), (
        f"Expected a TypedDict, got {result_type!r}"
    )
    assert converter.is_dynamic_typeddict(result_type), (
        "Expected converter.is_dynamic_typeddict() to return True"
    )
    assert not hasattr(result_type, "__dataclass_fields__"), (
        "Plain struct must not be mistaken for a dataclass"
    )
    assert result_type is not typing.Any, (
        "arrow_schema_to_python_schema must not return Any for a plain struct"
    )


def test_dynamic_typeddict_roundtrips_to_struct(converter):
    """The TypedDict returned for a plain struct maps back to the identical Arrow struct."""
    schema = pa.schema([pa.field("result", PLAIN_STRUCT, nullable=False)])
    python_schema = converter.arrow_schema_to_python_schema(schema)

    result_type = python_schema["result"]
    arrow_type_back = converter.python_type_to_arrow_type(result_type)

    assert arrow_type_back == PLAIN_STRUCT, (
        f"Round-trip failed: expected {PLAIN_STRUCT!r}, got {arrow_type_back!r}"
    )


def test_dynamic_typeddict_write_back(converter):
    """python_dicts_to_struct_dicts with a TypedDict schema correctly writes struct data."""
    schema = pa.schema([pa.field("result", PLAIN_STRUCT, nullable=False)])
    python_schema = converter.arrow_schema_to_python_schema(schema)

    data = [
        {"result": {"total": 8, "delta": 2}},
        {"result": {"total": 17, "delta": 3}},
    ]
    struct_dicts = converter.python_dicts_to_struct_dicts(data, python_schema=python_schema)
    table = pa.Table.from_pylist(struct_dicts, schema=schema)

    assert table.num_rows == 2
    assert table.schema == schema

    rows = table.column("result").to_pylist()
    assert rows[0] == {"total": 8, "delta": 2}
    assert rows[1] == {"total": 17, "delta": 3}
