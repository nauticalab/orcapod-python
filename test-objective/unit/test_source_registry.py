"""Specification-derived tests for SourceRegistry.

Tests documented behaviors of SourceRegistry including registration,
lookup, replacement, unregistration, idempotency, and introspection.
"""

import pyarrow as pa
import pytest

from orcapod.core.sources import ArrowTableSource
from orcapod.core.sources.source_registry import SourceRegistry


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_source(tag_val: str = "a", data_val: int = 1) -> ArrowTableSource:
    """Create a minimal ArrowTableSource for registry testing."""
    table = pa.table(
        {
            "tag": pa.array([tag_val], type=pa.large_string()),
            "data": pa.array([data_val], type=pa.int64()),
        }
    )
    return ArrowTableSource(table, tag_columns=["tag"])


# ---------------------------------------------------------------------------
# Registration and Lookup
# ---------------------------------------------------------------------------


class TestRegisterAndGet:
    """register() + get() roundtrip behaviors."""

    def test_register_and_get_roundtrip(self):
        """A registered source can be retrieved with get()."""
        registry = SourceRegistry()
        source = _make_source()
        registry.register("s1", source)
        assert registry.get("s1") is source

    def test_register_multiple_sources(self):
        """Multiple sources can be registered under different IDs."""
        registry = SourceRegistry()
        s1 = _make_source("a", 1)
        s2 = _make_source("b", 2)
        registry.register("s1", s1)
        registry.register("s2", s2)
        assert registry.get("s1") is s1
        assert registry.get("s2") is s2

    def test_register_empty_id_raises_value_error(self):
        """register() with empty string id raises ValueError."""
        registry = SourceRegistry()
        source = _make_source()
        with pytest.raises(ValueError):
            registry.register("", source)

    def test_register_none_source_raises_value_error(self):
        """register() with None source raises ValueError."""
        registry = SourceRegistry()
        with pytest.raises(ValueError):
            registry.register("s1", None)

    def test_register_same_object_idempotent(self):
        """Registering the same object under the same id is a no-op."""
        registry = SourceRegistry()
        source = _make_source()
        registry.register("s1", source)
        registry.register("s1", source)  # same object, no error
        assert registry.get("s1") is source
        assert len(registry) == 1

    def test_register_different_object_same_id_keeps_existing(self):
        """Registering a different object under an existing id keeps the original."""
        registry = SourceRegistry()
        s1 = _make_source("a", 1)
        s2 = _make_source("b", 2)
        registry.register("s1", s1)
        registry.register("s1", s2)  # different object, warns, keeps s1
        assert registry.get("s1") is s1
        assert len(registry) == 1


# ---------------------------------------------------------------------------
# Replace
# ---------------------------------------------------------------------------


class TestReplace:
    """replace() unconditionally overwrites and returns previous."""

    def test_replace_overwrites(self):
        """replace() overwrites existing entry."""
        registry = SourceRegistry()
        s1 = _make_source("a", 1)
        s2 = _make_source("b", 2)
        registry.register("s1", s1)
        registry.replace("s1", s2)
        assert registry.get("s1") is s2

    def test_replace_returns_previous(self):
        """replace() returns the previous source object."""
        registry = SourceRegistry()
        s1 = _make_source("a", 1)
        s2 = _make_source("b", 2)
        registry.register("s1", s1)
        old = registry.replace("s1", s2)
        assert old is s1

    def test_replace_returns_none_if_no_previous(self):
        """replace() returns None when there was no previous entry."""
        registry = SourceRegistry()
        source = _make_source()
        old = registry.replace("new_id", source)
        assert old is None

    def test_replace_empty_id_raises(self):
        """replace() with empty id raises ValueError."""
        registry = SourceRegistry()
        with pytest.raises(ValueError):
            registry.replace("", _make_source())


# ---------------------------------------------------------------------------
# Unregister
# ---------------------------------------------------------------------------


class TestUnregister:
    """unregister() removes and returns source."""

    def test_unregister_removes_and_returns(self):
        """unregister() removes entry and returns the source."""
        registry = SourceRegistry()
        source = _make_source()
        registry.register("s1", source)
        removed = registry.unregister("s1")
        assert removed is source
        assert "s1" not in registry

    def test_unregister_missing_raises_key_error(self):
        """unregister() on missing id raises KeyError."""
        registry = SourceRegistry()
        with pytest.raises(KeyError):
            registry.unregister("nonexistent")

    def test_unregister_decrements_length(self):
        """unregister() decreases the registry length."""
        registry = SourceRegistry()
        source = _make_source()
        registry.register("s1", source)
        assert len(registry) == 1
        registry.unregister("s1")
        assert len(registry) == 0


# ---------------------------------------------------------------------------
# Lookup: get() and get_optional()
# ---------------------------------------------------------------------------


class TestLookup:
    """get() and get_optional() behaviors."""

    def test_get_missing_raises_key_error(self):
        """get() on missing id raises KeyError."""
        registry = SourceRegistry()
        with pytest.raises(KeyError):
            registry.get("nonexistent")

    def test_get_optional_missing_returns_none(self):
        """get_optional() on missing id returns None."""
        registry = SourceRegistry()
        result = registry.get_optional("nonexistent")
        assert result is None

    def test_get_optional_existing_returns_source(self):
        """get_optional() returns the source when it exists."""
        registry = SourceRegistry()
        source = _make_source()
        registry.register("s1", source)
        result = registry.get_optional("s1")
        assert result is source


# ---------------------------------------------------------------------------
# Introspection: __contains__, __len__, __iter__, clear(), list_ids()
# ---------------------------------------------------------------------------


class TestIntrospection:
    """Dunder methods and introspection on SourceRegistry."""

    def test_contains(self):
        """__contains__ returns True for registered ids."""
        registry = SourceRegistry()
        source = _make_source()
        registry.register("s1", source)
        assert "s1" in registry
        assert "s2" not in registry

    def test_len_empty(self):
        """__len__ returns 0 for empty registry."""
        registry = SourceRegistry()
        assert len(registry) == 0

    def test_len_after_registrations(self):
        """__len__ returns correct count."""
        registry = SourceRegistry()
        registry.register("s1", _make_source("a", 1))
        registry.register("s2", _make_source("b", 2))
        assert len(registry) == 2

    def test_iter(self):
        """__iter__ yields registered source ids."""
        registry = SourceRegistry()
        s1 = _make_source("a", 1)
        s2 = _make_source("b", 2)
        registry.register("s1", s1)
        registry.register("s2", s2)
        ids = set(registry)
        assert ids == {"s1", "s2"}

    def test_clear_removes_all(self):
        """clear() removes all entries."""
        registry = SourceRegistry()
        registry.register("s1", _make_source("a", 1))
        registry.register("s2", _make_source("b", 2))
        assert len(registry) == 2
        registry.clear()
        assert len(registry) == 0
        assert "s1" not in registry
        assert "s2" not in registry

    def test_list_ids_returns_list(self):
        """list_ids() returns a list of registered ids."""
        registry = SourceRegistry()
        registry.register("s1", _make_source("a", 1))
        registry.register("s2", _make_source("b", 2))
        ids = registry.list_ids()
        assert isinstance(ids, list)
        assert set(ids) == {"s1", "s2"}

    def test_list_ids_empty(self):
        """list_ids() returns empty list for empty registry."""
        registry = SourceRegistry()
        assert registry.list_ids() == []

    def test_clear_then_register(self):
        """After clear(), new registrations work normally."""
        registry = SourceRegistry()
        s1 = _make_source("a", 1)
        registry.register("s1", s1)
        registry.clear()
        s2 = _make_source("b", 2)
        registry.register("s1", s2)
        assert registry.get("s1") is s2
