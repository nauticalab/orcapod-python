"""Tests for LogicalTypeRegistry factories parameter and default context factory wiring."""

from __future__ import annotations

import dataclasses

import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel

from orcapod.contexts import create_registry
from orcapod.extension_types.dataclass_logical_type_factory import (
    DataclassLogicalTypeFactory,
    DATACLASS_CATEGORY,
)
from orcapod.extension_types.pydantic_logical_type_factory import (
    PydanticLogicalTypeFactory,
    PYDANTIC_CATEGORY,
)
from orcapod.extension_types.registry import LogicalTypeRegistry


# ── Module-level dataclasses (local classes cannot be registered) ────────────

@dataclasses.dataclass
class _SimplePoint:
    """Minimal dataclass used as a test fixture."""
    x: int
    y: int


class _SimpleModel(BaseModel):
    """Minimal pydantic model used as a test fixture."""
    name: str
    score: float


# ── Registry constructor unit tests ─────────────────────────────────────────

def test_registry_factories_param_registers_category():
    """factories param registers the factory under the given category."""
    factory = DataclassLogicalTypeFactory()
    registry = LogicalTypeRegistry(
        factories=[{"factory": factory, "category": DATACLASS_CATEGORY, "python_bases": [object]}]
    )
    assert registry._category_factories.get(DATACLASS_CATEGORY) is factory


def test_registry_factories_param_registers_python_base():
    """factories param registers the factory under each python_base."""
    factory = DataclassLogicalTypeFactory()
    registry = LogicalTypeRegistry(
        factories=[{"factory": factory, "category": DATACLASS_CATEGORY, "python_bases": [object]}]
    )
    assert registry._python_class_factories.get(object) is factory


def test_registry_factories_param_empty_list_is_noop():
    """factories=[] constructs successfully with no registered factories."""
    registry = LogicalTypeRegistry(factories=[])
    assert registry._category_factories == {}
    assert registry._python_class_factories == {}


def test_registry_factories_param_none_is_noop():
    """factories=None (default) constructs successfully."""
    registry = LogicalTypeRegistry(factories=None)
    assert registry._category_factories == {}
    assert registry._python_class_factories == {}


# ── Default context integration tests ────────────────────────────────────────
#
# All tests use create_registry().get_context() — NOT get_default_context() —
# to avoid cross-test contamination via the global singleton cache.


def test_default_context_has_dataclass_factory():
    """Default context registers DataclassLogicalTypeFactory under orcapod.dataclass."""
    ctx = create_registry().get_context()
    registry = ctx.type_converter._logical_type_registry
    factory = registry._category_factories.get(DATACLASS_CATEGORY)
    assert isinstance(factory, DataclassLogicalTypeFactory)


def test_default_context_has_pydantic_factory():
    """Default context registers PydanticLogicalTypeFactory under orcapod.pydantic."""
    ctx = create_registry().get_context()
    registry = ctx.type_converter._logical_type_registry
    factory = registry._category_factories.get(PYDANTIC_CATEGORY)
    assert isinstance(factory, PydanticLogicalTypeFactory)


# ── Auto-registration tests ───────────────────────────────────────────────────


def test_default_context_dataclass_auto_registered_on_use():
    """register_python_class on a dataclass works zero-setup via the default context."""
    converter = create_registry().get_context().type_converter
    arrow_type = converter.register_python_class(_SimplePoint)
    assert isinstance(arrow_type, pa.ExtensionType)
    fqcn = f"{_SimplePoint.__module__}.{_SimplePoint.__qualname__}"
    assert arrow_type.extension_name == fqcn


def test_default_context_pydantic_auto_registered_on_use():
    """register_python_class on a pydantic model works zero-setup via the default context."""
    converter = create_registry().get_context().type_converter
    arrow_type = converter.register_python_class(_SimpleModel)
    assert isinstance(arrow_type, pa.ExtensionType)
    fqcn = f"{_SimpleModel.__module__}.{_SimpleModel.__qualname__}"
    assert arrow_type.extension_name == fqcn


# ── Parquet round-trip tests ─────────────────────────────────────────────────


def test_default_context_dataclass_parquet_roundtrip(tmp_path):
    """Dataclass round-trips through Parquet with no manual factory registration."""
    # Write path — fresh context, no manual factory setup
    write_converter = create_registry().get_context().type_converter
    write_converter.register_python_class(_SimplePoint)
    arrow_schema = write_converter.python_schema_to_arrow_schema({"point": _SimplePoint})
    rows = [{"point": _SimplePoint(x=3, y=7)}]
    table = write_converter.python_dicts_to_arrow_table(rows, arrow_schema=arrow_schema)

    parquet_path = tmp_path / "point.parquet"
    pq.write_table(table, parquet_path)

    # Read path — another fresh context, no manual factory setup
    read_converter = create_registry().get_context().type_converter
    read_table = pq.read_table(parquet_path)
    read_converter.register_discovered_extensions(read_table.schema)
    read_table = read_converter.apply_extension_types(read_table)

    rows_out = read_converter.arrow_table_to_python_dicts(read_table)
    assert len(rows_out) == 1
    result = rows_out[0]["point"]
    assert isinstance(result, _SimplePoint)
    assert result.x == 3
    assert result.y == 7


def test_default_context_pydantic_parquet_roundtrip(tmp_path):
    """Pydantic model round-trips through Parquet with no manual factory registration."""
    # Write path — fresh context, no manual factory setup
    write_converter = create_registry().get_context().type_converter
    write_converter.register_python_class(_SimpleModel)
    arrow_schema = write_converter.python_schema_to_arrow_schema({"model": _SimpleModel})
    rows = [{"model": _SimpleModel(name="alice", score=9.5)}]
    table = write_converter.python_dicts_to_arrow_table(rows, arrow_schema=arrow_schema)

    parquet_path = tmp_path / "model.parquet"
    pq.write_table(table, parquet_path)

    # Read path — another fresh context, no manual factory setup
    read_converter = create_registry().get_context().type_converter
    read_table = pq.read_table(parquet_path)
    read_converter.register_discovered_extensions(read_table.schema)
    read_table = read_converter.apply_extension_types(read_table)

    rows_out = read_converter.arrow_table_to_python_dicts(read_table)
    assert len(rows_out) == 1
    result = rows_out[0]["model"]
    assert isinstance(result, _SimpleModel)
    assert result.name == "alice"
    assert result.score == 9.5
