"""Tests for Pick and Index operators."""
from __future__ import annotations

import pytest

from orcapod.extension_types.base_logical_type import BaseLogicalType


class ConcreteLogicalType(BaseLogicalType):
    """Minimal concrete subclass for testing defaults."""
    pass


def test_base_logical_type_pick_field_raises():
    lt = ConcreteLogicalType()
    with pytest.raises(NotImplementedError, match="does not yet support pick"):
        lt.pick_field("some_key")


def test_base_logical_type_index_element_raises():
    lt = ConcreteLogicalType()
    with pytest.raises(NotImplementedError, match="does not yet support index"):
        lt.index_element()
