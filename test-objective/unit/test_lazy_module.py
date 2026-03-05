"""Specification-derived tests for LazyModule.

Tests based on documented behavior: deferred import until first attribute access.
"""

from __future__ import annotations

import pytest

from orcapod.utils.lazy_module import LazyModule


class TestLazyModule:
    """Per design, LazyModule defers import until first attribute access."""

    def test_not_loaded_initially(self):
        lazy = LazyModule("json")
        assert lazy.is_loaded is False

    def test_loads_on_attribute_access(self):
        lazy = LazyModule("json")
        # Accessing an attribute should trigger the import
        _ = lazy.dumps
        assert lazy.is_loaded is True

    def test_attribute_access_works(self):
        lazy = LazyModule("json")
        # Should be able to use the module's functions
        result = lazy.dumps({"key": "value"})
        assert isinstance(result, str)

    def test_force_load(self):
        lazy = LazyModule("json")
        mod = lazy.force_load()
        assert lazy.is_loaded is True
        assert mod is not None

    def test_invalid_module_raises(self):
        lazy = LazyModule("nonexistent_module_xyz_12345")
        with pytest.raises(ModuleNotFoundError):
            _ = lazy.dumps

    def test_module_name_property(self):
        lazy = LazyModule("json")
        assert lazy.module_name == "json"

    def test_repr(self):
        lazy = LazyModule("json")
        r = repr(lazy)
        assert "json" in r

    def test_str(self):
        lazy = LazyModule("json")
        s = str(lazy)
        assert "json" in s
