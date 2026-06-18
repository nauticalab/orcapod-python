"""Tests for LogicalTypeRegistry factories parameter and default context factory wiring."""

from __future__ import annotations

import dataclasses

import pytest

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
    x: int
    y: int


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

from orcapod.contexts import create_registry


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
