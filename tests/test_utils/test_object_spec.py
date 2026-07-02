"""Tests for parse_objectspec, focusing on the ``_optional`` flag (ITL-459)."""

from __future__ import annotations

import sys
import importlib
from typing import Any

import pytest

from orcapod.utils.object_spec import parse_objectspec, _create_instance_from_spec, _resolve_type_from_spec


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class _Sentinel:
    """Trivial class used as a resolvable target in tests."""
    def __init__(self, value: Any = None) -> None:
        self.value = value


# ---------------------------------------------------------------------------
# _resolve_type_from_spec — _optional flag
# ---------------------------------------------------------------------------

class TestResolveTypeFromSpecOptional:
    def test_resolves_present_type(self):
        spec = {"_type": "builtins.int"}
        assert _resolve_type_from_spec(spec) is int

    def test_missing_module_no_optional_raises(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "nonexistent_pkg_xyz", None)
        spec = {"_type": "nonexistent_pkg_xyz.SomeClass"}
        with pytest.raises(ImportError):
            _resolve_type_from_spec(spec)

    def test_missing_module_with_optional_returns_none(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "nonexistent_pkg_xyz", None)
        spec = {"_type": "nonexistent_pkg_xyz.SomeClass", "_optional": True}
        assert _resolve_type_from_spec(spec) is None

    def test_optional_false_still_raises(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "nonexistent_pkg_xyz", None)
        spec = {"_type": "nonexistent_pkg_xyz.SomeClass", "_optional": False}
        with pytest.raises(ImportError):
            _resolve_type_from_spec(spec)


# ---------------------------------------------------------------------------
# _create_instance_from_spec — _optional flag
# ---------------------------------------------------------------------------

class TestCreateInstanceFromSpecOptional:
    def test_creates_instance_normally(self):
        spec = {
            "_class": "tests.test_utils.test_object_spec._Sentinel",
            "_config": {"value": 42},
        }
        obj = _create_instance_from_spec(spec, ref_lut={}, validate=False)
        assert isinstance(obj, _Sentinel)
        assert obj.value == 42

    def test_missing_module_no_optional_raises(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "nonexistent_pkg_xyz", None)
        spec = {"_class": "nonexistent_pkg_xyz.SomeClass", "_config": {}}
        with pytest.raises((ImportError, ValueError)):
            _create_instance_from_spec(spec, ref_lut={}, validate=False)

    def test_missing_module_with_optional_returns_none(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "nonexistent_pkg_xyz", None)
        spec = {
            "_class": "nonexistent_pkg_xyz.SomeClass",
            "_config": {},
            "_optional": True,
        }
        assert _create_instance_from_spec(spec, ref_lut={}, validate=False) is None

    def test_optional_false_still_raises(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "nonexistent_pkg_xyz", None)
        spec = {
            "_class": "nonexistent_pkg_xyz.SomeClass",
            "_config": {},
            "_optional": False,
        }
        with pytest.raises((ImportError, ValueError)):
            _create_instance_from_spec(spec, ref_lut={}, validate=False)


# ---------------------------------------------------------------------------
# parse_objectspec — _optional integration
# ---------------------------------------------------------------------------

class TestParseObjectspecOptional:
    def test_list_with_optional_missing_drops_entry(self, monkeypatch):
        """Optional dict items that fail to resolve are dropped from the list entirely."""
        monkeypatch.setitem(sys.modules, "nonexistent_pkg_xyz", None)
        specs = [
            {"_type": "builtins.int"},
            {"_type": "nonexistent_pkg_xyz.SomeClass", "_optional": True},
            {"_type": "builtins.str"},
        ]
        result = parse_objectspec(specs)
        # The missing optional entry is dropped; the list has 2 elements, not 3.
        assert result == [int, str]

    def test_class_spec_with_optional_missing_returns_none(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "nonexistent_pkg_xyz", None)
        spec = {
            "_class": "nonexistent_pkg_xyz.Whatever",
            "_config": {},
            "_optional": True,
        }
        assert parse_objectspec(spec) is None

    def test_class_spec_without_optional_raises(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "nonexistent_pkg_xyz", None)
        spec = {"_class": "nonexistent_pkg_xyz.Whatever", "_config": {}}
        with pytest.raises((ImportError, ValueError)):
            parse_objectspec(spec)


# ---------------------------------------------------------------------------
# LogicalTypeRegistry — None filtering
# ---------------------------------------------------------------------------

class TestParseObjectspecOptionalListFiltering:
    def test_optional_class_spec_filtered_from_list(self, monkeypatch):
        """_optional _class items that fail to resolve are dropped from the list."""
        monkeypatch.setitem(sys.modules, "nonexistent_pkg_xyz", None)
        specs = [
            {"_type": "builtins.int"},
            {"_class": "nonexistent_pkg_xyz.SomeClass", "_config": {}, "_optional": True},
            {"_type": "builtins.str"},
        ]
        result = parse_objectspec(specs)
        assert result == [int, str]

    def test_optional_class_spec_included_when_present(self):
        """_optional _class items that resolve successfully are included."""
        specs = [
            {"_type": "builtins.int"},
            {"_class": "builtins.list", "_config": {}, "_optional": True},
            {"_type": "builtins.str"},
        ]
        result = parse_objectspec(specs)
        assert result[0] is int
        assert isinstance(result[1], list)
        assert result[2] is str


# ---------------------------------------------------------------------------
# PythonTypeHandlerRegistry — None filtering
# ---------------------------------------------------------------------------

class TestPythonTypeHandlerRegistryNoneFiltering:
    def test_none_pair_in_handlers_is_skipped(self):
        from orcapod.hashing.semantic_hashing.type_handler_registry import PythonTypeHandlerRegistry
        from orcapod.hashing.semantic_hashing.builtin_handlers import BytesHandler

        # A pair containing None (simulating optional missing dep) should be skipped.
        handlers = [
            (bytes, BytesHandler()),
            [None, None],           # optional pair where both resolved to None
            [None, BytesHandler()],  # partial None also skipped
        ]
        registry = PythonTypeHandlerRegistry(handlers=handlers)
        handler = registry.get_handler(b"hello")
        assert handler is not None

    def test_empty_pair_in_handlers_is_skipped(self):
        """Empty list entries (optional pair where all elements were filtered) are skipped."""
        from orcapod.hashing.semantic_hashing.type_handler_registry import PythonTypeHandlerRegistry
        from orcapod.hashing.semantic_hashing.builtin_handlers import BytesHandler

        # parse_objectspec filters _optional elements from the inner list, leaving [].
        handlers = [
            (bytes, BytesHandler()),
            [],  # inner list became empty after optional filtering
        ]
        registry = PythonTypeHandlerRegistry(handlers=handlers)
        assert registry.get_handler(b"x") is not None

    def test_none_entry_itself_skipped(self):
        from orcapod.hashing.semantic_hashing.type_handler_registry import PythonTypeHandlerRegistry
        from orcapod.hashing.semantic_hashing.builtin_handlers import BytesHandler

        handlers = [None, (bytes, BytesHandler())]
        registry = PythonTypeHandlerRegistry(handlers=handlers)
        assert registry.get_handler(b"x") is not None


# ---------------------------------------------------------------------------
# v0.1.json default context — SI auto-registration
# ---------------------------------------------------------------------------

class TestDefaultContextSIAutoRegistration:
    """Verify that the default context auto-registers SI types when SI is installed,
    and starts cleanly when SI is absent."""

    def test_default_context_loads_without_spikeinterface(self, monkeypatch):
        """Default context construction must not raise even when SI is absent."""
        # Block SI imports
        si_keys = [k for k in sys.modules if k == "spikeinterface" or k.startswith("spikeinterface.")]
        saved = {k: sys.modules.pop(k) for k in si_keys}
        monkeypatch.setitem(sys.modules, "spikeinterface", None)
        monkeypatch.setitem(sys.modules, "spikeinterface.core", None)

        # Also force the context to be re-created by clearing the cached default
        from orcapod import contexts as _ctx_mod
        saved_default = getattr(_ctx_mod, "_default_context", None)
        try:
            _ctx_mod._default_context = None
            ctx = _ctx_mod.get_default_context()
            assert ctx is not None
        finally:
            _ctx_mod._default_context = saved_default
            for k in list(sys.modules):
                if k == "spikeinterface" or k.startswith("spikeinterface."):
                    del sys.modules[k]
            sys.modules.update(saved)

    def test_default_context_has_si_types_when_installed(self):
        """When SI is installed, BaseRecording is recognised by the default context."""
        si_core = pytest.importorskip("spikeinterface.core", reason="spikeinterface not installed")
        from orcapod.contexts import get_default_context

        ctx = get_default_context()
        arrow_type = ctx.type_converter.python_type_to_arrow_type(si_core.BaseRecording)
        assert arrow_type.extension_name == "spikeinterface.recording"
