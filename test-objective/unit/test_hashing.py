"""Tests for BaseSemanticHasher and TypeHandlerRegistry.

Specification-derived tests covering deterministic hashing of primitives,
structures, ContentHash pass-through, identity_structure resolution,
strict-mode errors, collision resistance, and registry operations.
"""

from __future__ import annotations

import threading
from typing import Any
from unittest.mock import MagicMock

import pytest

from orcapod.hashing.semantic_hashing.semantic_hasher import BaseSemanticHasher
from orcapod.hashing.semantic_hashing.type_handler_registry import (
    BuiltinTypeHandlerRegistry,
    TypeHandlerRegistry,
)
from orcapod.types import ContentHash


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def registry() -> TypeHandlerRegistry:
    """An empty TypeHandlerRegistry."""
    return TypeHandlerRegistry()


@pytest.fixture
def hasher(registry: TypeHandlerRegistry) -> BaseSemanticHasher:
    """A strict BaseSemanticHasher backed by an empty registry."""
    return BaseSemanticHasher(
        hasher_id="test_v1",
        type_handler_registry=registry,
        strict=True,
    )


@pytest.fixture
def lenient_hasher(registry: TypeHandlerRegistry) -> BaseSemanticHasher:
    """A non-strict BaseSemanticHasher backed by an empty registry."""
    return BaseSemanticHasher(
        hasher_id="test_v1",
        type_handler_registry=registry,
        strict=False,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeHandler:
    """Minimal object satisfying TypeHandlerProtocol for testing."""

    def __init__(self, return_value: Any = "handled") -> None:
        self._return_value = return_value

    def handle(self, obj: Any, hasher: BaseSemanticHasher) -> Any:
        return self._return_value


class _IdentityObj:
    """Object implementing identity_structure() for hashing."""

    def __init__(self, structure: Any) -> None:
        self._structure = structure

    def identity_structure(self) -> Any:
        return self._structure

    def content_hash(self, hasher: Any = None) -> ContentHash:
        if hasher is not None:
            return hasher.hash_object(self.identity_structure())
        h = BaseSemanticHasher(
            "test_v1", type_handler_registry=TypeHandlerRegistry(), strict=False
        )
        return h.hash_object(self.identity_structure())


# ===================================================================
# BaseSemanticHasher -- primitive hashing
# ===================================================================


class TestBaseSemanticHasherPrimitives:
    """Primitives (int, str, float, bool, None) are hashed deterministically."""

    @pytest.mark.parametrize(
        "value",
        [0, 1, -42, 3.14, -0.0, "", "hello", True, False, None],
        ids=lambda v: f"{type(v).__name__}({v!r})",
    )
    def test_primitive_produces_content_hash(
        self, hasher: BaseSemanticHasher, value: Any
    ) -> None:
        result = hasher.hash_object(value)
        assert isinstance(result, ContentHash)

    @pytest.mark.parametrize("value", [42, "hello", 3.14, True, None])
    def test_primitive_deterministic(
        self, hasher: BaseSemanticHasher, value: Any
    ) -> None:
        """Same input always produces the same hash."""
        h1 = hasher.hash_object(value)
        h2 = hasher.hash_object(value)
        assert h1 == h2

    def test_different_primitives_differ(self, hasher: BaseSemanticHasher) -> None:
        """Different inputs produce different hashes (collision resistance)."""
        h_int = hasher.hash_object(42)
        h_str = hasher.hash_object("42")
        assert h_int != h_str


# ===================================================================
# BaseSemanticHasher -- structures
# ===================================================================


class TestBaseSemanticHasherStructures:
    """Structures (list, dict, tuple, set) are expanded and hashed."""

    def test_list_hashed(self, hasher: BaseSemanticHasher) -> None:
        result = hasher.hash_object([1, 2, 3])
        assert isinstance(result, ContentHash)

    def test_dict_hashed(self, hasher: BaseSemanticHasher) -> None:
        result = hasher.hash_object({"a": 1, "b": 2})
        assert isinstance(result, ContentHash)

    def test_tuple_hashed(self, hasher: BaseSemanticHasher) -> None:
        result = hasher.hash_object((1, 2, 3))
        assert isinstance(result, ContentHash)

    def test_set_hashed(self, hasher: BaseSemanticHasher) -> None:
        result = hasher.hash_object({1, 2, 3})
        assert isinstance(result, ContentHash)

    def test_list_and_tuple_differ(self, hasher: BaseSemanticHasher) -> None:
        """list and tuple with same elements produce different hashes."""
        h_list = hasher.hash_object([1, 2, 3])
        h_tuple = hasher.hash_object((1, 2, 3))
        assert h_list != h_tuple

    def test_set_order_independent(self, hasher: BaseSemanticHasher) -> None:
        """Sets with the same elements hash identically regardless of insertion order."""
        h1 = hasher.hash_object({3, 1, 2})
        h2 = hasher.hash_object({1, 2, 3})
        assert h1 == h2

    def test_dict_key_order_independent(self, hasher: BaseSemanticHasher) -> None:
        """Dicts with the same key-value pairs hash identically regardless of order."""
        h1 = hasher.hash_object({"b": 2, "a": 1})
        h2 = hasher.hash_object({"a": 1, "b": 2})
        assert h1 == h2

    def test_nested_structures(self, hasher: BaseSemanticHasher) -> None:
        """Nested structures are hashed correctly."""
        nested = {"key": [1, (2, 3)], "other": {"inner": True}}
        result = hasher.hash_object(nested)
        assert isinstance(result, ContentHash)
        # Determinism
        assert result == hasher.hash_object(nested)

    def test_different_structures_differ(self, hasher: BaseSemanticHasher) -> None:
        h1 = hasher.hash_object([1, 2])
        h2 = hasher.hash_object([1, 2, 3])
        assert h1 != h2


# ===================================================================
# BaseSemanticHasher -- ContentHash passthrough
# ===================================================================


class TestBaseSemanticHasherContentHash:
    """ContentHash inputs are returned as-is (terminal)."""

    def test_content_hash_passthrough(self, hasher: BaseSemanticHasher) -> None:
        ch = ContentHash(method="sha256", digest=b"\x00" * 32)
        result = hasher.hash_object(ch)
        assert result is ch


# ===================================================================
# BaseSemanticHasher -- identity_structure resolution
# ===================================================================


class TestBaseSemanticHasherIdentityStructure:
    """Objects implementing identity_structure() are resolved via it."""

    def test_identity_structure_object(self, hasher: BaseSemanticHasher) -> None:
        obj = _IdentityObj(structure={"name": "test", "version": 1})
        result = hasher.hash_object(obj)
        assert isinstance(result, ContentHash)

    def test_identity_structure_deterministic(
        self, hasher: BaseSemanticHasher
    ) -> None:
        obj1 = _IdentityObj(structure=[1, 2, 3])
        obj2 = _IdentityObj(structure=[1, 2, 3])
        assert hasher.hash_object(obj1) == hasher.hash_object(obj2)

    def test_different_identity_structures_differ(
        self, hasher: BaseSemanticHasher
    ) -> None:
        obj1 = _IdentityObj(structure="alpha")
        obj2 = _IdentityObj(structure="beta")
        assert hasher.hash_object(obj1) != hasher.hash_object(obj2)


# ===================================================================
# BaseSemanticHasher -- strict mode
# ===================================================================


class TestBaseSemanticHasherStrictMode:
    """Unknown type in strict mode raises TypeError."""

    def test_unknown_type_strict_raises(self, hasher: BaseSemanticHasher) -> None:
        class Unknown:
            pass

        with pytest.raises(TypeError, match="no TypeHandlerProtocol registered"):
            hasher.hash_object(Unknown())

    def test_unknown_type_lenient_succeeds(
        self, lenient_hasher: BaseSemanticHasher
    ) -> None:
        class Unknown:
            pass

        result = lenient_hasher.hash_object(Unknown())
        assert isinstance(result, ContentHash)


# ===================================================================
# BaseSemanticHasher -- collision resistance
# ===================================================================


class TestBaseSemanticHasherCollisionResistance:
    """Different inputs produce different hashes."""

    def test_int_vs_string(self, hasher: BaseSemanticHasher) -> None:
        assert hasher.hash_object(1) != hasher.hash_object("1")

    def test_empty_list_vs_empty_tuple(self, hasher: BaseSemanticHasher) -> None:
        assert hasher.hash_object([]) != hasher.hash_object(())

    def test_empty_dict_vs_empty_list(self, hasher: BaseSemanticHasher) -> None:
        assert hasher.hash_object({}) != hasher.hash_object([])

    def test_none_vs_string_none(self, hasher: BaseSemanticHasher) -> None:
        assert hasher.hash_object(None) != hasher.hash_object("None")

    def test_true_vs_one(self, hasher: BaseSemanticHasher) -> None:
        """bool True and int 1 produce different hashes due to JSON encoding."""
        h_true = hasher.hash_object(True)
        h_one = hasher.hash_object(1)
        assert h_true != h_one


# ===================================================================
# TypeHandlerRegistry -- register/get_handler roundtrip
# ===================================================================


class TestTypeHandlerRegistryBasics:
    """register() + get_handler() roundtrip."""

    def test_register_and_get_handler(self, registry: TypeHandlerRegistry) -> None:
        handler = _FakeHandler()
        registry.register(int, handler)
        assert registry.get_handler(42) is handler

    def test_get_handler_returns_none_for_unregistered(
        self, registry: TypeHandlerRegistry
    ) -> None:
        assert registry.get_handler("hello") is None


# ===================================================================
# TypeHandlerRegistry -- MRO-aware lookup
# ===================================================================


class TestTypeHandlerRegistryMRO:
    """MRO-aware lookup: handler for parent class matches subclass."""

    def test_subclass_inherits_parent_handler(
        self, registry: TypeHandlerRegistry
    ) -> None:
        class Base:
            pass

        class Child(Base):
            pass

        handler = _FakeHandler()
        registry.register(Base, handler)
        assert registry.get_handler(Child()) is handler

    def test_specific_handler_overrides_parent(
        self, registry: TypeHandlerRegistry
    ) -> None:
        class Base:
            pass

        class Child(Base):
            pass

        parent_handler = _FakeHandler("parent")
        child_handler = _FakeHandler("child")
        registry.register(Base, parent_handler)
        registry.register(Child, child_handler)
        assert registry.get_handler(Child()) is child_handler
        assert registry.get_handler(Base()) is parent_handler


# ===================================================================
# TypeHandlerRegistry -- unregister
# ===================================================================


class TestTypeHandlerRegistryUnregister:
    """unregister() removes handler."""

    def test_unregister_existing(self, registry: TypeHandlerRegistry) -> None:
        handler = _FakeHandler()
        registry.register(int, handler)
        result = registry.unregister(int)
        assert result is True
        assert registry.get_handler(42) is None

    def test_unregister_nonexistent(self, registry: TypeHandlerRegistry) -> None:
        result = registry.unregister(float)
        assert result is False


# ===================================================================
# TypeHandlerRegistry -- has_handler
# ===================================================================


class TestTypeHandlerRegistryHasHandler:
    """has_handler() boolean check."""

    def test_has_handler_true(self, registry: TypeHandlerRegistry) -> None:
        registry.register(int, _FakeHandler())
        assert registry.has_handler(int) is True

    def test_has_handler_false(self, registry: TypeHandlerRegistry) -> None:
        assert registry.has_handler(str) is False

    def test_has_handler_via_mro(self, registry: TypeHandlerRegistry) -> None:
        class Base:
            pass

        class Child(Base):
            pass

        registry.register(Base, _FakeHandler())
        assert registry.has_handler(Child) is True


# ===================================================================
# TypeHandlerRegistry -- registered_types
# ===================================================================


class TestTypeHandlerRegistryRegisteredTypes:
    """registered_types() lists types."""

    def test_registered_types_empty(self, registry: TypeHandlerRegistry) -> None:
        assert registry.registered_types() == []

    def test_registered_types_populated(self, registry: TypeHandlerRegistry) -> None:
        registry.register(int, _FakeHandler())
        registry.register(str, _FakeHandler())
        types = registry.registered_types()
        assert set(types) == {int, str}


# ===================================================================
# TypeHandlerRegistry -- thread safety
# ===================================================================


class TestTypeHandlerRegistryThreadSafety:
    """Concurrent register/lookup doesn't crash."""

    def test_concurrent_register_lookup(self, registry: TypeHandlerRegistry) -> None:
        errors: list[Exception] = []

        def register_types(start: int, count: int) -> None:
            try:
                for i in range(start, start + count):
                    t = type(f"Type{i}", (), {})
                    registry.register(t, _FakeHandler(f"handler_{i}"))
            except Exception as exc:
                errors.append(exc)

        def lookup_types() -> None:
            try:
                for _ in range(100):
                    registry.get_handler(42)
                    registry.registered_types()
                    registry.has_handler(int)
            except Exception as exc:
                errors.append(exc)

        threads = []
        for i in range(5):
            threads.append(
                threading.Thread(target=register_types, args=(i * 20, 20))
            )
            threads.append(threading.Thread(target=lookup_types))

        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert errors == [], f"Concurrent operations raised: {errors}"


# ===================================================================
# BuiltinTypeHandlerRegistry
# ===================================================================


class TestBuiltinTypeHandlerRegistry:
    """BuiltinTypeHandlerRegistry is pre-populated with built-in handlers."""

    def test_construction(self) -> None:
        reg = BuiltinTypeHandlerRegistry()
        assert len(reg.registered_types()) > 0
