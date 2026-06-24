"""Tests for SemanticAwarePythonHasher and PythonTypeSemanticHasherRegistry.

Specification-derived tests covering deterministic hashing of primitives,
structures, ContentHash pass-through, identity_structure resolution,
strict-mode errors, collision resistance, and registry operations.
"""

from __future__ import annotations

import threading
from typing import Any
from unittest.mock import MagicMock

import pytest

from orcapod.hashing.semantic_hashing.semantic_hasher import SemanticAwarePythonHasher
from orcapod.hashing.semantic_hashing.type_handler_registry import (
    BuiltinPythonTypeSemanticHasherRegistry,
    PythonTypeSemanticHasherRegistry,
)
from orcapod.types import ContentHash


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def registry() -> PythonTypeSemanticHasherRegistry:
    """An empty PythonTypeSemanticHasherRegistry."""
    return PythonTypeSemanticHasherRegistry()


@pytest.fixture
def hasher(registry: PythonTypeSemanticHasherRegistry) -> SemanticAwarePythonHasher:
    """A strict SemanticAwarePythonHasher backed by an empty registry."""
    return SemanticAwarePythonHasher(
        hasher_id="test_v1",
        type_semantic_hasher_registry=registry,
        strict=True,
    )


@pytest.fixture
def lenient_hasher(registry: PythonTypeSemanticHasherRegistry) -> SemanticAwarePythonHasher:
    """A non-strict SemanticAwarePythonHasher backed by an empty registry."""
    return SemanticAwarePythonHasher(
        hasher_id="test_v1",
        type_semantic_hasher_registry=registry,
        strict=False,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeHandler:
    """Minimal object satisfying PythonTypeSemanticHasherProtocol for testing."""

    def __init__(self, return_value: Any = "handled") -> None:
        self._return_value = return_value

    def hash(self, obj: Any, hasher: SemanticAwarePythonHasher) -> ContentHash:
        return ContentHash(method="fake", digest=str(self._return_value).encode())


class _IdentityObj:
    """Object implementing identity_structure() for hashing."""

    def __init__(self, structure: Any) -> None:
        self._structure = structure

    def identity_structure(self) -> Any:
        return self._structure

    def content_hash(self, hasher: Any = None) -> ContentHash:
        if hasher is not None:
            return hasher.hash_object(self.identity_structure())
        h = SemanticAwarePythonHasher(
            "test_v1", type_semantic_hasher_registry=PythonTypeSemanticHasherRegistry(), strict=False
        )
        return h.hash_object(self.identity_structure())


# ===================================================================
# SemanticAwarePythonHasher -- primitive hashing
# ===================================================================


class TestSemanticAwarePythonHasherPrimitives:
    """Primitives (int, str, float, bool, None) are hashed deterministically."""

    @pytest.mark.parametrize(
        "value",
        [0, 1, -42, 3.14, -0.0, "", "hello", True, False, None],
        ids=lambda v: f"{type(v).__name__}({v!r})",
    )
    def test_primitive_produces_content_hash(
        self, hasher: SemanticAwarePythonHasher, value: Any
    ) -> None:
        result = hasher.hash_object(value)
        assert isinstance(result, ContentHash)

    @pytest.mark.parametrize("value", [42, "hello", 3.14, True, None])
    def test_primitive_deterministic(
        self, hasher: SemanticAwarePythonHasher, value: Any
    ) -> None:
        """Same input always produces the same hash."""
        h1 = hasher.hash_object(value)
        h2 = hasher.hash_object(value)
        assert h1 == h2

    def test_different_primitives_differ(self, hasher: SemanticAwarePythonHasher) -> None:
        """Different inputs produce different hashes (collision resistance)."""
        h_int = hasher.hash_object(42)
        h_str = hasher.hash_object("42")
        assert h_int != h_str


# ===================================================================
# SemanticAwarePythonHasher -- structures
# ===================================================================


class TestSemanticAwarePythonHasherStructures:
    """Structures (list, dict, tuple, set) are expanded and hashed."""

    def test_list_hashed(self, hasher: SemanticAwarePythonHasher) -> None:
        result = hasher.hash_object([1, 2, 3])
        assert isinstance(result, ContentHash)

    def test_dict_hashed(self, hasher: SemanticAwarePythonHasher) -> None:
        result = hasher.hash_object({"a": 1, "b": 2})
        assert isinstance(result, ContentHash)

    def test_tuple_hashed(self, hasher: SemanticAwarePythonHasher) -> None:
        result = hasher.hash_object((1, 2, 3))
        assert isinstance(result, ContentHash)

    def test_set_hashed(self, hasher: SemanticAwarePythonHasher) -> None:
        result = hasher.hash_object({1, 2, 3})
        assert isinstance(result, ContentHash)

    def test_list_and_tuple_differ(self, hasher: SemanticAwarePythonHasher) -> None:
        """list and tuple with same elements produce different hashes."""
        h_list = hasher.hash_object([1, 2, 3])
        h_tuple = hasher.hash_object((1, 2, 3))
        assert h_list != h_tuple

    def test_set_order_independent(self, hasher: SemanticAwarePythonHasher) -> None:
        """Sets with the same elements hash identically regardless of insertion order."""
        h1 = hasher.hash_object({3, 1, 2})
        h2 = hasher.hash_object({1, 2, 3})
        assert h1 == h2

    def test_dict_key_order_independent(self, hasher: SemanticAwarePythonHasher) -> None:
        """Dicts with the same key-value pairs hash identically regardless of order."""
        h1 = hasher.hash_object({"b": 2, "a": 1})
        h2 = hasher.hash_object({"a": 1, "b": 2})
        assert h1 == h2

    def test_nested_structures(self, hasher: SemanticAwarePythonHasher) -> None:
        """Nested structures are hashed correctly."""
        nested = {"key": [1, (2, 3)], "other": {"inner": True}}
        result = hasher.hash_object(nested)
        assert isinstance(result, ContentHash)
        # Determinism
        assert result == hasher.hash_object(nested)

    def test_different_structures_differ(self, hasher: SemanticAwarePythonHasher) -> None:
        h1 = hasher.hash_object([1, 2])
        h2 = hasher.hash_object([1, 2, 3])
        assert h1 != h2


# ===================================================================
# SemanticAwarePythonHasher -- ContentHash passthrough
# ===================================================================


class TestSemanticAwarePythonHasherContentHash:
    """ContentHash inputs are returned as-is (terminal)."""

    def test_content_hash_passthrough(self, hasher: SemanticAwarePythonHasher) -> None:
        ch = ContentHash(method="sha256", digest=b"\x00" * 32)
        result = hasher.hash_object(ch)
        assert result is ch


# ===================================================================
# SemanticAwarePythonHasher -- identity_structure resolution
# ===================================================================


class TestSemanticAwarePythonHasherIdentityStructure:
    """Objects implementing identity_structure() are resolved via it."""

    def test_identity_structure_object(self, hasher: SemanticAwarePythonHasher) -> None:
        obj = _IdentityObj(structure={"name": "test", "version": 1})
        result = hasher.hash_object(obj)
        assert isinstance(result, ContentHash)

    def test_identity_structure_deterministic(
        self, hasher: SemanticAwarePythonHasher
    ) -> None:
        obj1 = _IdentityObj(structure=[1, 2, 3])
        obj2 = _IdentityObj(structure=[1, 2, 3])
        assert hasher.hash_object(obj1) == hasher.hash_object(obj2)

    def test_different_identity_structures_differ(
        self, hasher: SemanticAwarePythonHasher
    ) -> None:
        obj1 = _IdentityObj(structure="alpha")
        obj2 = _IdentityObj(structure="beta")
        assert hasher.hash_object(obj1) != hasher.hash_object(obj2)


# ===================================================================
# SemanticAwarePythonHasher -- strict mode
# ===================================================================


class TestSemanticAwarePythonHasherStrictMode:
    """Unknown type in strict mode raises TypeError."""

    def test_unknown_type_strict_raises(self, hasher: SemanticAwarePythonHasher) -> None:
        class Unknown:
            pass

        with pytest.raises(TypeError, match="no PythonTypeSemanticHasherProtocol registered"):
            hasher.hash_object(Unknown())

    def test_unknown_type_lenient_succeeds(
        self, lenient_hasher: SemanticAwarePythonHasher
    ) -> None:
        class Unknown:
            pass

        result = lenient_hasher.hash_object(Unknown())
        assert isinstance(result, ContentHash)


# ===================================================================
# SemanticAwarePythonHasher -- collision resistance
# ===================================================================


class TestSemanticAwarePythonHasherCollisionResistance:
    """Different inputs produce different hashes."""

    def test_int_vs_string(self, hasher: SemanticAwarePythonHasher) -> None:
        assert hasher.hash_object(1) != hasher.hash_object("1")

    def test_empty_list_vs_empty_tuple(self, hasher: SemanticAwarePythonHasher) -> None:
        assert hasher.hash_object([]) != hasher.hash_object(())

    def test_empty_dict_vs_empty_list(self, hasher: SemanticAwarePythonHasher) -> None:
        assert hasher.hash_object({}) != hasher.hash_object([])

    def test_none_vs_string_none(self, hasher: SemanticAwarePythonHasher) -> None:
        assert hasher.hash_object(None) != hasher.hash_object("None")

    def test_true_vs_one(self, hasher: SemanticAwarePythonHasher) -> None:
        """bool True and int 1 produce different hashes due to JSON encoding."""
        h_true = hasher.hash_object(True)
        h_one = hasher.hash_object(1)
        assert h_true != h_one


# ===================================================================
# PythonTypeSemanticHasherRegistry -- register/get_semantic_hasher roundtrip
# ===================================================================


class TestPythonTypeSemanticHasherRegistryBasics:
    """register() + get_semantic_hasher() roundtrip."""

    def test_register_and_get_semantic_hasher(self, registry: PythonTypeSemanticHasherRegistry) -> None:
        handler = _FakeHandler()
        registry.register(int, handler)
        assert registry.get_semantic_hasher(42) is handler

    def test_get_semantic_hasher_returns_none_for_unregistered(
        self, registry: PythonTypeSemanticHasherRegistry
    ) -> None:
        assert registry.get_semantic_hasher("hello") is None


# ===================================================================
# PythonTypeSemanticHasherRegistry -- MRO-aware lookup
# ===================================================================


class TestPythonTypeSemanticHasherRegistryMRO:
    """MRO-aware lookup: handler for parent class matches subclass."""

    def test_subclass_inherits_parent_handler(
        self, registry: PythonTypeSemanticHasherRegistry
    ) -> None:
        class Base:
            pass

        class Child(Base):
            pass

        handler = _FakeHandler()
        registry.register(Base, handler)
        assert registry.get_semantic_hasher(Child()) is handler

    def test_specific_handler_overrides_parent(
        self, registry: PythonTypeSemanticHasherRegistry
    ) -> None:
        class Base:
            pass

        class Child(Base):
            pass

        parent_handler = _FakeHandler("parent")
        child_handler = _FakeHandler("child")
        registry.register(Base, parent_handler)
        registry.register(Child, child_handler)
        assert registry.get_semantic_hasher(Child()) is child_handler
        assert registry.get_semantic_hasher(Base()) is parent_handler


# ===================================================================
# PythonTypeSemanticHasherRegistry -- unregister
# ===================================================================


class TestPythonTypeSemanticHasherRegistryUnregister:
    """unregister() removes handler."""

    def test_unregister_existing(self, registry: PythonTypeSemanticHasherRegistry) -> None:
        handler = _FakeHandler()
        registry.register(int, handler)
        result = registry.unregister(int)
        assert result is True
        assert registry.get_semantic_hasher(42) is None

    def test_unregister_nonexistent(self, registry: PythonTypeSemanticHasherRegistry) -> None:
        result = registry.unregister(float)
        assert result is False


# ===================================================================
# PythonTypeSemanticHasherRegistry -- has_semantic_hasher
# ===================================================================


class TestPythonTypeSemanticHasherRegistryHasSemanticHasher:
    """has_semantic_hasher() boolean check."""

    def test_has_semantic_hasher_true(self, registry: PythonTypeSemanticHasherRegistry) -> None:
        registry.register(int, _FakeHandler())
        assert registry.has_semantic_hasher(int) is True

    def test_has_semantic_hasher_false(self, registry: PythonTypeSemanticHasherRegistry) -> None:
        assert registry.has_semantic_hasher(str) is False

    def test_has_semantic_hasher_via_mro(self, registry: PythonTypeSemanticHasherRegistry) -> None:
        class Base:
            pass

        class Child(Base):
            pass

        registry.register(Base, _FakeHandler())
        assert registry.has_semantic_hasher(Child) is True


# ===================================================================
# PythonTypeSemanticHasherRegistry -- registered_types
# ===================================================================


class TestPythonTypeSemanticHasherRegistryRegisteredTypes:
    """registered_types() lists types."""

    def test_registered_types_empty(self, registry: PythonTypeSemanticHasherRegistry) -> None:
        assert registry.registered_types() == []

    def test_registered_types_populated(self, registry: PythonTypeSemanticHasherRegistry) -> None:
        registry.register(int, _FakeHandler())
        registry.register(str, _FakeHandler())
        types = registry.registered_types()
        assert set(types) == {int, str}


# ===================================================================
# PythonTypeSemanticHasherRegistry -- thread safety
# ===================================================================


class TestPythonTypeSemanticHasherRegistryThreadSafety:
    """Concurrent register/lookup doesn't crash."""

    def test_concurrent_register_lookup(self, registry: PythonTypeSemanticHasherRegistry) -> None:
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
                    registry.get_semantic_hasher(42)
                    registry.registered_types()
                    registry.has_semantic_hasher(int)
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
# BuiltinPythonTypeSemanticHasherRegistry
# ===================================================================


class TestBuiltinPythonTypeSemanticHasherRegistry:
    """BuiltinPythonTypeSemanticHasherRegistry is pre-populated with built-in handlers."""

    def test_construction(self) -> None:
        reg = BuiltinPythonTypeSemanticHasherRegistry()
        assert len(reg.registered_types()) > 0
