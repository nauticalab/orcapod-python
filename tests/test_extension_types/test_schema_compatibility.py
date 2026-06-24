"""Integration tests for extension-type-backed schema compatibility.

Two complementary angles:

Arrow-level identity
    ``converter.python_schema_to_arrow_schema`` assigns each dataclass a unique
    Arrow extension name derived from its fully-qualified class name.  Two
    dataclasses with identical struct shapes but different class names therefore
    produce *different* extension names — the core identity guarantee of the
    extension type system.

Python-type-level compatibility
    ``check_schema_compatibility`` from ``schema_utils`` uses beartype
    ``is_subhint`` to compare Python type annotations.  Same class → compatible;
    different class with the same struct shape → incompatible.  This is the
    property that prevents silent data corruption when two unrelated dataclasses
    happen to share the same fields.
"""
from __future__ import annotations

import dataclasses

import pyarrow as pa

from orcapod.contexts import create_registry
from orcapod.types import Schema
from orcapod.utils.schema_utils import check_schema_compatibility


# Module-level dataclasses — DataclassLogicalTypeFactory rejects local classes
# because they have no stable fully-qualified class name for reconstruction.

@dataclasses.dataclass
class _PointA:
    x: int
    y: int


@dataclasses.dataclass
class _PointB:
    """Same struct shape as _PointA but a different class name."""
    x: int
    y: int


# ── Arrow-level identity tests ────────────────────────────────────────────────


def test_arrow_schema_distinct_extension_names_for_same_shape():
    """_PointA and _PointB produce different Arrow extension names despite identical shapes.

    This is the core identity guarantee: struct shape alone does not determine
    type identity in the extension type system.
    """
    converter_a = create_registry().get_context().type_converter
    converter_b = create_registry().get_context().type_converter

    type_a = converter_a.register_python_class(_PointA)
    type_b = converter_b.register_python_class(_PointB)

    assert isinstance(type_a, pa.ExtensionType)
    assert isinstance(type_b, pa.ExtensionType)

    fqcn_a = f"{_PointA.__module__}.{_PointA.__qualname__}"
    fqcn_b = f"{_PointB.__module__}.{_PointB.__qualname__}"
    assert type_a.extension_name == fqcn_a
    assert type_b.extension_name == fqcn_b
    assert type_a.extension_name != type_b.extension_name


def test_arrow_schema_same_extension_name_idempotent():
    """Registering _PointA twice returns the same extension name both times."""
    converter = create_registry().get_context().type_converter

    type_first = converter.register_python_class(_PointA)
    type_second = converter.register_python_class(_PointA)

    assert isinstance(type_first, pa.ExtensionType)
    assert isinstance(type_second, pa.ExtensionType)
    assert type_first.extension_name == type_second.extension_name


# ── Python-type-level compatibility tests ─────────────────────────────────────


def test_python_schema_compatibility_passes_same_type():
    """Incoming _PointA is compatible with receiving _PointA."""
    result = check_schema_compatibility(
        {"value": _PointA},
        Schema({"value": _PointA}),
    )
    assert result is True


def test_python_schema_compatibility_rejects_different_type_same_shape():
    """Incoming _PointA is NOT compatible with receiving _PointB.

    Both dataclasses share the same struct shape {x: int, y: int}, but they
    are different Python types.  The old shape-based system would have accepted
    this silently; the extension type system correctly rejects it.
    """
    result = check_schema_compatibility(
        {"value": _PointA},
        Schema({"value": _PointB}),
    )
    assert result is False
