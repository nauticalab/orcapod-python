"""
Comprehensive test suite for the BaseSemanticHasher system.

Covers:
  - BaseSemanticHasher: primitives, container type-tagging, determinism,
    circular references, strict vs non-strict mode
  - ContentIdentifiable protocol: independent hashing, composability
  - TypeHandlerRegistry: registration, MRO-aware lookup, unregister
  - Built-in handlers: bytes, UUID, Path, functions, type objects
  - ContentHash as terminal: returned as-is without re-hashing
  - ContentIdentifiableMixin: content_hash, __eq__, __hash__, caching,
    cache invalidation, injectable hasher
  - Custom type handler registration and extension
  - get_default_semantic_hasher / get_default_type_handler_registry
"""

from __future__ import annotations

import hashlib
import json
import tempfile
from collections import OrderedDict, namedtuple
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from orcapod.hashing.builtin_handlers import (
    BytesHandler,
    FunctionHandler,
    PathContentHandler,
    TypeObjectHandler,
    UUIDHandler,
    register_builtin_handlers,
)
from orcapod.hashing.content_identifiable_mixin import ContentIdentifiableMixin
from orcapod.hashing.defaults import get_default_semantic_hasher
from orcapod.hashing.semantic_hasher import BaseSemanticHasher, _is_namedtuple
from orcapod.hashing.type_handler_registry import (
    TypeHandlerRegistry,
    get_default_type_handler_registry,
)
from orcapod.types import ContentHash

# ---------------------------------------------------------------------------
# Helpers and fixtures
# ---------------------------------------------------------------------------


def make_hasher(strict: bool = True) -> BaseSemanticHasher:
    """Create a fresh BaseSemanticHasher with an isolated registry."""
    registry = TypeHandlerRegistry()
    register_builtin_handlers(registry)
    return BaseSemanticHasher(
        hasher_id="test_v1", type_handler_registry=registry, strict=strict
    )


@pytest.fixture
def hasher() -> BaseSemanticHasher:
    return make_hasher(strict=True)


@pytest.fixture
def lenient_hasher() -> BaseSemanticHasher:
    return make_hasher(strict=False)


# ---------------------------------------------------------------------------
# Simple content-identifiable classes for testing
# ---------------------------------------------------------------------------


class SimpleRecord(ContentIdentifiableMixin):
    """A simple content-identifiable record."""

    def __init__(self, name: str, value: int, *, semantic_hasher=None) -> None:
        super().__init__(semantic_hasher=semantic_hasher)
        self.name = name
        self.value = value

    def identity_structure(self) -> Any:
        return {"name": self.name, "value": self.value}


class NestedRecord(ContentIdentifiableMixin):
    """A content-identifiable record that embeds another ContentIdentifiable."""

    def __init__(
        self, label: str, inner: SimpleRecord, *, semantic_hasher=None
    ) -> None:
        super().__init__(semantic_hasher=semantic_hasher)
        self.label = label
        self.inner = inner

    def identity_structure(self) -> Any:
        return {"label": self.label, "inner": self.inner}


class ListRecord(ContentIdentifiableMixin):
    """A content-identifiable record that holds a list of ContentIdentifiables."""

    def __init__(self, items: list, *, semantic_hasher=None) -> None:
        super().__init__(semantic_hasher=semantic_hasher)
        self.items = items

    def identity_structure(self) -> Any:
        return {"items": self.items}


# ---------------------------------------------------------------------------
# 1. BaseSemanticHasher: primitives
# ---------------------------------------------------------------------------


class TestPrimitives:
    def test_none_hashes(self, hasher):
        h = hasher.hash_object(None)
        assert isinstance(h, ContentHash)

    def test_bool_hashes(self, hasher):
        assert isinstance(hasher.hash_object(True), ContentHash)
        assert isinstance(hasher.hash_object(False), ContentHash)

    def test_int_hashes(self, hasher):
        assert isinstance(hasher.hash_object(0), ContentHash)
        assert isinstance(hasher.hash_object(42), ContentHash)

    def test_float_hashes(self, hasher):
        assert isinstance(hasher.hash_object(3.14), ContentHash)

    def test_str_hashes(self, hasher):
        assert isinstance(hasher.hash_object("hello"), ContentHash)
        assert isinstance(hasher.hash_object("unicode: 🐋"), ContentHash)

    def test_primitives_hash_to_content_hash(self, hasher):
        h = hasher.hash_object(42)
        assert isinstance(h, ContentHash)
        assert h.method == "test_v1"

    def test_different_primitives_differ(self, hasher):
        assert hasher.hash_object(1) != hasher.hash_object(2)
        assert hasher.hash_object("a") != hasher.hash_object("b")
        assert hasher.hash_object(None) != hasher.hash_object("")

    def test_bool_vs_int_differs(self, hasher):
        """True and 1 must produce different hashes -- JSON encodes them differently."""
        assert hasher.hash_object(True) != hasher.hash_object(1)

    def test_same_primitive_same_hash(self, hasher):
        assert hasher.hash_object(42) == hasher.hash_object(42)
        assert hasher.hash_object("hello") == hasher.hash_object("hello")


# ---------------------------------------------------------------------------
# 2. BaseSemanticHasher: container type-tagging and determinism
# ---------------------------------------------------------------------------


class TestContainers:
    def test_empty_list_hashes(self, hasher):
        assert isinstance(hasher.hash_object([]), ContentHash)

    def test_list_order_preserved(self, hasher):
        """[1,2,3] and [3,2,1] must differ."""
        assert hasher.hash_object([1, 2, 3]) != hasher.hash_object([3, 2, 1])

    def test_list_vs_tuple_differs(self, hasher):
        """list and tuple with same elements must differ (type-tagged)."""
        assert hasher.hash_object([1, 2, 3]) != hasher.hash_object((1, 2, 3))

    def test_list_vs_set_differs(self, hasher):
        assert hasher.hash_object([1, 2, 3]) != hasher.hash_object({1, 2, 3})

    def test_tuple_vs_set_differs(self, hasher):
        assert hasher.hash_object((1, 2, 3)) != hasher.hash_object({1, 2, 3})

    def test_dict_order_independent(self, hasher):
        h1 = hasher.hash_object({"x": 1, "y": 2})
        h2 = hasher.hash_object({"y": 2, "x": 1})
        assert h1 == h2

    def test_set_order_independent(self, hasher):
        h1 = hasher.hash_object({1, 2, 3})
        h2 = hasher.hash_object({3, 1, 2})
        assert h1 == h2

    def test_frozenset_equals_set(self, hasher):
        """set and frozenset with same elements should hash the same
        (both tagged as 'set' in the expansion)."""
        assert hasher.hash_object({1, 2, 3}) == hasher.hash_object(frozenset([1, 2, 3]))

    def test_ordered_dict_same_as_dict(self, hasher):
        od = OrderedDict([("z", 1), ("a", 2)])
        d = {"z": 1, "a": 2}
        assert hasher.hash_object(od) == hasher.hash_object(d)

    def test_nested_list(self, hasher):
        h1 = hasher.hash_object([1, [2, [3]]])
        h2 = hasher.hash_object([1, [2, [3]]])
        assert h1 == h2

    def test_nested_list_vs_flat_differs(self, hasher):
        assert hasher.hash_object([[1, 2], 3]) != hasher.hash_object([1, 2, 3])

    def test_identical_objects_same_hash(self, hasher):
        obj = {"nested": [1, 2, {"deep": True}]}
        assert hasher.hash_object(obj) == hasher.hash_object(obj)

    def test_hash_returns_content_hash(self, hasher):
        h = hasher.hash_object({"key": "val"})
        assert isinstance(h, ContentHash)
        assert len(h.digest) == 32  # SHA-256 = 32 bytes


# ---------------------------------------------------------------------------
# 3. BaseSemanticHasher: namedtuples
# ---------------------------------------------------------------------------


Point = namedtuple("Point", ["x", "y"])
Person = namedtuple("Person", ["name", "age", "email"])


class TestNamedTuples:
    def test_namedtuple_hashes(self, hasher):
        h = hasher.hash_object(Point(3, 4))
        assert isinstance(h, ContentHash)

    def test_namedtuple_vs_plain_tuple_differs(self, hasher):
        """namedtuple and plain tuple with same values must differ."""
        assert hasher.hash_object(Point(3, 4)) != hasher.hash_object((3, 4))

    def test_namedtuple_different_fields_different_hash(self, hasher):
        """Two namedtuples with same values but different field names must differ."""
        AB = namedtuple("AB", ["a", "b"])
        XY = namedtuple("XY", ["x", "y"])
        assert hasher.hash_object(AB(1, 2)) != hasher.hash_object(XY(1, 2))

    def test_namedtuple_same_content_same_hash(self, hasher):
        p1 = Point(3, 4)
        p2 = Point(3, 4)
        assert hasher.hash_object(p1) == hasher.hash_object(p2)

    def test_is_namedtuple_helper(self):
        assert _is_namedtuple(Point(1, 2)) is True
        assert _is_namedtuple((1, 2)) is False
        assert _is_namedtuple([1, 2]) is False
        assert _is_namedtuple("hello") is False


# ---------------------------------------------------------------------------
# 4. BaseSemanticHasher: circular references
# ---------------------------------------------------------------------------


class TestCircularReferences:
    def test_list_circular_ref_does_not_hang(self, hasher):
        circ: Any = [1, 2, 3]
        circ.append(circ)
        # Should terminate (circular ref replaced by sentinel) rather than recurse
        h = hasher.hash_object(circ)
        assert isinstance(h, ContentHash)

    def test_dict_circular_ref_does_not_hang(self, hasher):
        circ: Any = {"a": 1}
        circ["self"] = circ
        h = hasher.hash_object(circ)
        assert isinstance(h, ContentHash)

    def test_circular_ref_same_structure_same_hash(self, hasher):
        """Two structurally identical circular lists produce the same hash."""
        a: Any = [1, 2]
        a.append(a)
        b: Any = [1, 2]
        b.append(b)
        assert hasher.hash_object(a) == hasher.hash_object(b)

    def test_circular_differs_from_non_circular(self, hasher):
        """A list with a back-ref sentinel differs from one with a plain value."""
        circ: Any = [1, 2]
        circ.append(circ)
        plain = [1, 2, [1, 2]]  # structurally different
        assert hasher.hash_object(circ) != hasher.hash_object(plain)


# ---------------------------------------------------------------------------
# 5. BaseSemanticHasher: strict vs non-strict mode
# ---------------------------------------------------------------------------


class Unhandled:
    """An unregistered, non-ContentIdentifiable class."""

    def __init__(self, x: int) -> None:
        self.x = x


class TestStrictMode:
    def test_strict_raises_on_unknown_type(self, hasher):
        with pytest.raises(TypeError, match="no TypeHandler registered"):
            hasher.hash_object(Unhandled(1))

    def test_non_strict_returns_content_hash(self, lenient_hasher):
        h = lenient_hasher.hash_object(Unhandled(42))
        assert isinstance(h, ContentHash)

    def test_non_strict_same_object_same_hash(self, lenient_hasher):
        h1 = lenient_hasher.hash_object(Unhandled(42))
        h2 = lenient_hasher.hash_object(Unhandled(42))
        assert h1 == h2

    def test_strict_mode_flag(self):
        strict = BaseSemanticHasher(hasher_id="s", strict=True)
        lenient = BaseSemanticHasher(hasher_id="s", strict=False)
        assert strict.strict is True
        assert lenient.strict is False


# ---------------------------------------------------------------------------
# 6. Built-in handlers: bytes and bytearray
# ---------------------------------------------------------------------------


class TestBytesHandler:
    def test_bytes_hashes(self, hasher):
        h = hasher.hash_object(b"hello")
        assert isinstance(h, ContentHash)

    def test_bytearray_hashes(self, hasher):
        h = hasher.hash_object(bytearray(b"hello"))
        assert isinstance(h, ContentHash)

    def test_bytes_determinism(self, hasher):
        assert hasher.hash_object(b"data") == hasher.hash_object(b"data")

    def test_bytes_vs_string_differs(self, hasher):
        assert hasher.hash_object(b"hello") != hasher.hash_object("hello")

    def test_different_bytes_differ(self, hasher):
        assert hasher.hash_object(b"abc") != hasher.hash_object(b"xyz")

    def test_empty_bytes_hashes(self, hasher):
        assert isinstance(hasher.hash_object(b""), ContentHash)


# ---------------------------------------------------------------------------
# 7. Built-in handlers: UUID
# ---------------------------------------------------------------------------


class TestUUIDHandler:
    def test_uuid_hashes(self, hasher):
        u = UUID("550e8400-e29b-41d4-a716-446655440000")
        assert isinstance(hasher.hash_object(u), ContentHash)

    def test_uuid_determinism(self, hasher):
        u = UUID("550e8400-e29b-41d4-a716-446655440000")
        assert hasher.hash_object(u) == hasher.hash_object(u)

    def test_different_uuids_differ(self, hasher):
        u1 = UUID("550e8400-e29b-41d4-a716-446655440000")
        u2 = UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
        assert hasher.hash_object(u1) != hasher.hash_object(u2)


# ---------------------------------------------------------------------------
# 8. Built-in handlers: Path (content-based)
# ---------------------------------------------------------------------------


class TestPathHandler:
    def test_path_hashes_file_content(self, hasher):
        with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".txt") as f:
            f.write(b"hello world")
            tmp_path = Path(f.name)

        try:
            h = hasher.hash_object(tmp_path)
            assert isinstance(h, ContentHash)
        finally:
            tmp_path.unlink()

    def test_path_same_content_same_hash(self, hasher):
        with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".txt") as f1:
            f1.write(b"identical content")
            p1 = Path(f1.name)

        with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".txt") as f2:
            f2.write(b"identical content")
            p2 = Path(f2.name)

        try:
            assert hasher.hash_object(p1) == hasher.hash_object(p2)
        finally:
            p1.unlink()
            p2.unlink()

    def test_path_different_content_different_hash(self, hasher):
        with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".txt") as f1:
            f1.write(b"content A")
            p1 = Path(f1.name)

        with tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".txt") as f2:
            f2.write(b"content B")
            p2 = Path(f2.name)

        try:
            assert hasher.hash_object(p1) != hasher.hash_object(p2)
        finally:
            p1.unlink()
            p2.unlink()

    def test_missing_path_raises(self, hasher):
        with pytest.raises(FileNotFoundError):
            hasher.hash_object(Path("/nonexistent/path/file.txt"))

    def test_directory_raises(self, hasher):
        with tempfile.TemporaryDirectory() as d:
            with pytest.raises(IsADirectoryError):
                hasher.hash_object(Path(d))


# ---------------------------------------------------------------------------
# 9. ContentHash as terminal
# ---------------------------------------------------------------------------


class TestContentHashTerminal:
    def test_content_hash_returned_as_is(self, hasher):
        """hash_object(ContentHash) must return the same object unchanged."""
        ch = ContentHash("sha256", b"\x01" * 32)
        result = hasher.hash_object(ch)
        assert result is ch

    def test_content_hash_in_list_embeds_as_token(self, hasher):
        """A ContentHash inside a list is embedded as its to_string() token,
        so the list hash depends on the ContentHash value."""
        ch1 = ContentHash("sha256", b"\x01" * 32)
        ch2 = ContentHash("sha256", b"\x02" * 32)
        assert hasher.hash_object([ch1]) != hasher.hash_object([ch2])

    def test_content_hash_in_list_same_value_same_hash(self, hasher):
        ch = ContentHash("sha256", b"\xab" * 32)
        assert hasher.hash_object([ch]) == hasher.hash_object([ch])

    def test_different_methods_differ(self, hasher):
        ch1 = ContentHash("sha256", b"\x01" * 32)
        ch2 = ContentHash("md5", b"\x01" * 32)
        assert hasher.hash_object(ch1) != hasher.hash_object(ch2)

    def test_different_digests_differ(self, hasher):
        ch1 = ContentHash("sha256", b"\x01" * 32)
        ch2 = ContentHash("sha256", b"\x02" * 32)
        assert hasher.hash_object(ch1) != hasher.hash_object(ch2)

    def test_no_double_hashing(self, hasher):
        """A ContentHash embedded in a structure should NOT be hashed again.
        Its hash_object result is the ContentHash itself."""
        ch = ContentHash("sha256", b"\xde\xad" * 16)
        # Directly hashing the ContentHash returns it as-is
        assert hasher.hash_object(ch) is ch
        # Its token in a parent structure is its to_string()
        # -- verified indirectly via consistency of nested structures
        h1 = hasher.hash_object({"key": ch})
        h2 = hasher.hash_object({"key": ch})
        assert h1 == h2


# ---------------------------------------------------------------------------
# 10. Built-in handlers: functions
# ---------------------------------------------------------------------------


def sample_function(a: int, b: int) -> int:
    """A sample function."""
    return a + b


def another_function(x: str) -> str:
    return x.upper()


class TestFunctionHandler:
    def test_function_produces_content_hash(self, hasher):
        h = hasher.hash_object(sample_function)
        assert isinstance(h, ContentHash)

    def test_same_function_same_hash(self, hasher):
        assert hasher.hash_object(sample_function) == hasher.hash_object(
            sample_function
        )

    def test_different_functions_different_hash(self, hasher):
        assert hasher.hash_object(sample_function) != hasher.hash_object(
            another_function
        )

    def test_lambda_hashed(self, hasher):
        f = lambda x: x * 2  # noqa: E731
        assert isinstance(hasher.hash_object(f), ContentHash)

    def test_function_in_structure(self, hasher):
        h = hasher.hash_object({"func": sample_function, "val": 42})
        assert isinstance(h, ContentHash)


# ---------------------------------------------------------------------------
# 11. Built-in handlers: type objects
# ---------------------------------------------------------------------------


class TestTypeObjectHandler:
    def test_type_object_hashed(self, hasher):
        assert isinstance(hasher.hash_object(int), ContentHash)

    def test_different_types_differ(self, hasher):
        assert hasher.hash_object(int) != hasher.hash_object(str)

    def test_custom_class_hashed(self, hasher):
        assert isinstance(hasher.hash_object(SimpleRecord), ContentHash)


# ---------------------------------------------------------------------------
# 12. ContentIdentifiable: independent hashing and composability
# ---------------------------------------------------------------------------


class TestContentIdentifiable:
    def test_content_identifiable_hashes(self, hasher):
        rec = SimpleRecord("foo", 42, semantic_hasher=hasher)
        assert isinstance(hasher.hash_object(rec), ContentHash)

    def test_content_identifiable_in_structure_is_opaque(self, hasher):
        """X with identity_structure [A,B] inside [X, C] is NOT the same as [[A,B], C].
        The parent sees X's hash token, not its raw structure."""
        inner = SimpleRecord("bar", 99, semantic_hasher=hasher)

        # Hash of [inner, 42] -- inner contributes only its hash token
        h_with_inner = hasher.hash_object([inner, 42])

        # Hash of [[inner's identity structure], 42] -- raw structure exposed
        h_with_raw = hasher.hash_object([inner.identity_structure(), 42])

        assert h_with_inner != h_with_raw

    def test_content_identifiable_hash_consistent_with_direct(self, hasher):
        """The token embedded for X inside a structure equals hash_object(X)."""
        inner = SimpleRecord("bar", 99, semantic_hasher=hasher)
        direct_hash = hasher.hash_object(inner)

        # Build a one-element list containing only the ContentHash we'd expect
        # to be embedded for inner, and hash that list.
        token_list_hash = hasher.hash_object([direct_hash])

        # Build the same list with the live object and hash it.
        live_list_hash = hasher.hash_object([inner])

        assert token_list_hash == live_list_hash

    def test_nested_content_identifiable(self, hasher):
        inner = SimpleRecord("x", 1, semantic_hasher=hasher)
        outer = NestedRecord("outer", inner, semantic_hasher=hasher)
        h_outer = hasher.hash_object(outer)
        assert isinstance(h_outer, ContentHash)
        # Changing inner changes outer
        inner2 = SimpleRecord("x", 2, semantic_hasher=hasher)
        outer2 = NestedRecord("outer", inner2, semantic_hasher=hasher)
        assert hasher.hash_object(outer) != hasher.hash_object(outer2)

    def test_list_of_content_identifiables(self, hasher):
        items = [
            SimpleRecord("a", 1, semantic_hasher=hasher),
            SimpleRecord("b", 2, semantic_hasher=hasher),
        ]
        rec = ListRecord(items, semantic_hasher=hasher)
        assert isinstance(hasher.hash_object(rec), ContentHash)

    def test_same_content_same_hash(self, hasher):
        r1 = SimpleRecord("test", 5, semantic_hasher=hasher)
        r2 = SimpleRecord("test", 5, semantic_hasher=hasher)
        assert hasher.hash_object(r1) == hasher.hash_object(r2)

    def test_different_content_different_hash(self, hasher):
        r1 = SimpleRecord("test", 5, semantic_hasher=hasher)
        r2 = SimpleRecord("test", 6, semantic_hasher=hasher)
        assert hasher.hash_object(r1) != hasher.hash_object(r2)

    def test_primitive_identity_structure_equals_direct_structure_hash(self, hasher):
        """An object whose identity_structure() returns a plain primitive structure
        must hash identically to hashing that structure directly.

        Since hash_object recurses on the result of identity_structure(), and the
        returned structure contains only primitives (no non-primitive leaves that
        would be hashed independently), the two paths are equivalent.
        """
        rec = SimpleRecord("hello", 42, semantic_hasher=hasher)
        # hash_object via ContentIdentifiable path
        h_via_obj = hasher.hash_object(rec)
        # hash_object directly on the same primitive structure
        h_via_struct = hasher.hash_object(rec.identity_structure())
        assert h_via_obj == h_via_struct

    def test_nested_primitive_identity_structure_equals_direct(self, hasher):
        """Same invariant for a deeper nested structure."""

        class DeepRecord(ContentIdentifiableMixin):
            def identity_structure(self):
                return {"outer": {"inner": [1, 2, 3]}, "flag": True}

        rec = DeepRecord(semantic_hasher=hasher)
        assert hasher.hash_object(rec) == hasher.hash_object(rec.identity_structure())


# ---------------------------------------------------------------------------
# 13. ContentIdentifiableMixin
# ---------------------------------------------------------------------------


class TestContentIdentifiableMixin:
    def test_content_hash_returns_content_hash(self, hasher):
        rec = SimpleRecord("foo", 1, semantic_hasher=hasher)
        assert isinstance(rec.content_hash(), ContentHash)

    def test_content_hash_cached(self, hasher):
        rec = SimpleRecord("foo", 1, semantic_hasher=hasher)
        h1 = rec.content_hash()
        h2 = rec.content_hash()
        assert h1 is h2

    def test_cache_invalidation(self, hasher):
        rec = SimpleRecord("foo", 1, semantic_hasher=hasher)
        h1 = rec.content_hash()
        rec._invalidate_content_hash_cache()
        h2 = rec.content_hash()
        assert h1 == h2
        assert h1 is not h2

    def test_eq_same_content(self, hasher):
        r1 = SimpleRecord("foo", 1, semantic_hasher=hasher)
        r2 = SimpleRecord("foo", 1, semantic_hasher=hasher)
        assert r1 == r2

    def test_eq_different_content(self, hasher):
        r1 = SimpleRecord("foo", 1, semantic_hasher=hasher)
        r2 = SimpleRecord("foo", 2, semantic_hasher=hasher)
        assert r1 != r2

    def test_eq_not_implemented_for_other_types(self, hasher):
        rec = SimpleRecord("foo", 1, semantic_hasher=hasher)
        assert rec.__eq__("not a mixin") is NotImplemented

    def test_hash_same_content(self, hasher):
        r1 = SimpleRecord("foo", 1, semantic_hasher=hasher)
        r2 = SimpleRecord("foo", 1, semantic_hasher=hasher)
        assert hash(r1) == hash(r2)

    def test_hash_different_content(self, hasher):
        r1 = SimpleRecord("foo", 1, semantic_hasher=hasher)
        r2 = SimpleRecord("foo", 2, semantic_hasher=hasher)
        assert hash(r1) != hash(r2)

    def test_usable_as_dict_key(self, hasher):
        r1 = SimpleRecord("foo", 1, semantic_hasher=hasher)
        r2 = SimpleRecord("foo", 1, semantic_hasher=hasher)
        d = {r1: "value"}
        assert d[r2] == "value"

    def test_usable_in_set(self, hasher):
        r1 = SimpleRecord("foo", 1, semantic_hasher=hasher)
        r2 = SimpleRecord("foo", 1, semantic_hasher=hasher)
        r3 = SimpleRecord("bar", 2, semantic_hasher=hasher)
        s = {r1, r2, r3}
        assert len(s) == 2

    def test_injectable_hasher(self):
        custom_hasher = BaseSemanticHasher(hasher_id="injected_v9")
        rec = SimpleRecord("foo", 1, semantic_hasher=custom_hasher)
        assert rec.content_hash().method == "injected_v9"

    def test_default_global_hasher_used_when_none_injected(self):
        rec = SimpleRecord("foo", 1)
        default = get_default_semantic_hasher()
        assert rec.content_hash().method == default.hasher_id

    def test_not_implemented_identity_structure(self):
        class NoImpl(ContentIdentifiableMixin):
            pass

        obj = NoImpl()
        with pytest.raises(NotImplementedError):
            obj.identity_structure()

    def test_repr_includes_hash(self, hasher):
        rec = SimpleRecord("foo", 1, semantic_hasher=hasher)
        r = repr(rec)
        assert "SimpleRecord" in r
        assert "content_hash" in r


# ---------------------------------------------------------------------------
# 14. TypeHandlerRegistry
# ---------------------------------------------------------------------------


class _DummyHandler:
    def __init__(self, tag: str) -> None:
        self.tag = tag

    def handle(self, obj: Any, hasher: Any) -> Any:
        return f"{self.tag}:{obj}"


class Base:
    pass


class Child(Base):
    pass


class GrandChild(Child):
    pass


class TestTypeHandlerRegistry:
    def test_register_and_get_exact(self):
        reg = TypeHandlerRegistry()
        h = _DummyHandler("base")
        reg.register(Base, h)
        assert reg.get_handler(Base()) is h

    def test_mro_lookup_child(self):
        reg = TypeHandlerRegistry()
        h = _DummyHandler("base")
        reg.register(Base, h)
        assert reg.get_handler(Child()) is h

    def test_mro_lookup_grandchild(self):
        reg = TypeHandlerRegistry()
        h = _DummyHandler("base")
        reg.register(Base, h)
        assert reg.get_handler(GrandChild()) is h

    def test_more_specific_handler_wins(self):
        reg = TypeHandlerRegistry()
        h_base = _DummyHandler("base")
        h_child = _DummyHandler("child")
        reg.register(Base, h_base)
        reg.register(Child, h_child)
        assert reg.get_handler(Child()) is h_child
        assert reg.get_handler(GrandChild()) is h_child

    def test_unregistered_returns_none(self):
        reg = TypeHandlerRegistry()
        assert reg.get_handler(Base()) is None

    def test_unregister_removes_handler(self):
        reg = TypeHandlerRegistry()
        h = _DummyHandler("base")
        reg.register(Base, h)
        assert reg.unregister(Base) is True
        assert reg.get_handler(Base()) is None

    def test_unregister_nonexistent_returns_false(self):
        reg = TypeHandlerRegistry()
        assert reg.unregister(Base) is False

    def test_replace_existing_handler(self):
        reg = TypeHandlerRegistry()
        h1 = _DummyHandler("first")
        h2 = _DummyHandler("second")
        reg.register(Base, h1)
        reg.register(Base, h2)
        assert reg.get_handler(Base()) is h2

    def test_register_non_type_raises(self):
        reg = TypeHandlerRegistry()
        with pytest.raises(TypeError):
            reg.register("not_a_type", _DummyHandler("x"))  # type: ignore[arg-type]

    def test_has_handler_exact(self):
        reg = TypeHandlerRegistry()
        reg.register(Base, _DummyHandler("b"))
        assert reg.has_handler(Base) is True

    def test_has_handler_via_mro(self):
        reg = TypeHandlerRegistry()
        reg.register(Base, _DummyHandler("b"))
        assert reg.has_handler(Child) is True

    def test_has_handler_false(self):
        reg = TypeHandlerRegistry()
        assert reg.has_handler(Base) is False

    def test_registered_types_snapshot(self):
        reg = TypeHandlerRegistry()
        reg.register(Base, _DummyHandler("b"))
        reg.register(Child, _DummyHandler("c"))
        types = reg.registered_types()
        assert Base in types
        assert Child in types

    def test_len(self):
        reg = TypeHandlerRegistry()
        assert len(reg) == 0
        reg.register(Base, _DummyHandler("b"))
        assert len(reg) == 1
        reg.register(Child, _DummyHandler("c"))
        assert len(reg) == 2

    def test_get_handler_for_type(self):
        reg = TypeHandlerRegistry()
        h = _DummyHandler("b")
        reg.register(Base, h)
        assert reg.get_handler_for_type(Base) is h
        assert reg.get_handler_for_type(Child) is h  # via MRO
        assert reg.get_handler_for_type(int) is None


# ---------------------------------------------------------------------------
# 15. Custom handler registration and extension
# ---------------------------------------------------------------------------


class Celsius:
    def __init__(self, degrees: float) -> None:
        self.degrees = degrees


class CelsiusHandler:
    def handle(self, obj: Any, hasher: Any) -> Any:
        return {"__type__": "Celsius", "degrees": obj.degrees}


class TestCustomHandlerRegistration:
    def test_register_custom_type(self):
        registry = TypeHandlerRegistry()
        register_builtin_handlers(registry)
        registry.register(Celsius, CelsiusHandler())
        custom_hasher = BaseSemanticHasher(
            hasher_id="custom_v1", type_handler_registry=registry, strict=True
        )
        assert isinstance(custom_hasher.hash_object(Celsius(100.0)), ContentHash)

    def test_custom_handler_determinism(self):
        registry = TypeHandlerRegistry()
        register_builtin_handlers(registry)
        registry.register(Celsius, CelsiusHandler())
        custom_hasher = BaseSemanticHasher(
            hasher_id="custom_v1", type_handler_registry=registry
        )
        h1 = custom_hasher.hash_object(Celsius(37.5))
        h2 = custom_hasher.hash_object(Celsius(37.5))
        assert h1 == h2

    def test_custom_handler_different_values_differ(self):
        registry = TypeHandlerRegistry()
        register_builtin_handlers(registry)
        registry.register(Celsius, CelsiusHandler())
        custom_hasher = BaseSemanticHasher(
            hasher_id="custom_v1", type_handler_registry=registry
        )
        assert custom_hasher.hash_object(Celsius(0.0)) != custom_hasher.hash_object(
            Celsius(100.0)
        )

    def test_unregistered_type_still_strict(self):
        hasher = BaseSemanticHasher(hasher_id="strict_v1", strict=True)
        with pytest.raises(TypeError):
            hasher.hash_object(Celsius(42.0))

    def test_custom_handler_in_nested_structure(self):
        registry = TypeHandlerRegistry()
        register_builtin_handlers(registry)
        registry.register(Celsius, CelsiusHandler())
        custom_hasher = BaseSemanticHasher(
            hasher_id="custom_v1", type_handler_registry=registry
        )
        h = custom_hasher.hash_object({"temp": Celsius(36.6), "unit": "C"})
        assert isinstance(h, ContentHash)

    def test_handler_returning_content_hash_is_terminal(self):
        """A handler that returns a ContentHash must not be re-hashed."""

        class DirectHashHandler:
            def handle(self, obj: Any, hasher: Any) -> ContentHash:
                return ContentHash("direct", b"\xaa" * 32)

        registry = TypeHandlerRegistry()
        register_builtin_handlers(registry)
        registry.register(Celsius, DirectHashHandler())
        custom_hasher = BaseSemanticHasher(
            hasher_id="custom_v1", type_handler_registry=registry
        )
        result = custom_hasher.hash_object(Celsius(0.0))
        # The ContentHash returned by the handler should come back as-is
        assert result == ContentHash("direct", b"\xaa" * 32)

    def test_mro_aware_custom_handler(self):
        class FancyCelsius(Celsius):
            pass

        registry = TypeHandlerRegistry()
        register_builtin_handlers(registry)
        registry.register(Celsius, CelsiusHandler())
        custom_hasher = BaseSemanticHasher(
            hasher_id="custom_v1", type_handler_registry=registry
        )
        h = custom_hasher.hash_object(FancyCelsius(20.0))
        assert isinstance(h, ContentHash)
        # FancyCelsius inherits Celsius's handler so same hash as Celsius
        assert h == custom_hasher.hash_object(Celsius(20.0))

    def test_register_on_global_default_registry(self):
        class Kelvin:
            def __init__(self, k: float) -> None:
                self.k = k

        class KelvinHandler:
            def handle(self, obj: Any, hasher: Any) -> Any:
                return {"__type__": "Kelvin", "k": obj.k}

        global_registry = get_default_type_handler_registry()
        global_registry.register(Kelvin, KelvinHandler())
        try:
            default_hasher = get_default_semantic_hasher()
            assert isinstance(default_hasher.hash_object(Kelvin(273.15)), ContentHash)
        finally:
            global_registry.unregister(Kelvin)


# ---------------------------------------------------------------------------
# 16. Global singletons
# ---------------------------------------------------------------------------


class TestGlobalSingletons:
    def test_get_default_semantic_hasher_returns_semantic_hasher(self):
        assert isinstance(get_default_semantic_hasher(), BaseSemanticHasher)

    def test_get_default_semantic_hasher_has_versioned_id(self):
        assert get_default_semantic_hasher().hasher_id == "semantic_v0.1"

    def test_get_default_type_handler_registry_is_singleton(self):
        r1 = get_default_type_handler_registry()
        r2 = get_default_type_handler_registry()
        assert r1 is r2

    def test_default_registry_has_builtin_handlers(self):
        import types as _types

        reg = get_default_type_handler_registry()
        assert reg.has_handler(bytes)
        assert reg.has_handler(bytearray)
        assert reg.has_handler(UUID)
        assert reg.has_handler(Path)
        assert reg.has_handler(_types.FunctionType)
        assert reg.has_handler(type)

    def test_default_registry_has_no_content_hash_handler(self):
        """ContentHash is handled as a terminal -- no registry entry needed."""
        reg = get_default_type_handler_registry()
        assert not reg.has_handler(ContentHash)

    def test_default_hasher_can_hash_common_types(self):
        h = get_default_semantic_hasher()
        assert isinstance(h.hash_object(None), ContentHash)
        assert isinstance(h.hash_object(42), ContentHash)
        assert isinstance(h.hash_object("hello"), ContentHash)
        assert isinstance(h.hash_object([1, 2, 3]), ContentHash)
        assert isinstance(h.hash_object({"a": 1}), ContentHash)
        assert isinstance(h.hash_object(b"bytes"), ContentHash)
        assert isinstance(
            h.hash_object(UUID("550e8400-e29b-41d4-a716-446655440000")), ContentHash
        )

    def test_content_hash_conversion_methods(self):
        h = get_default_semantic_hasher()
        ch = h.hash_object({"x": 1})
        assert isinstance(ch.to_hex(), str)
        assert len(ch.to_hex()) == 64
        assert isinstance(ch.to_int(), int)
        assert isinstance(ch.to_hex(16), str)
        assert len(ch.to_hex(16)) == 16


# ---------------------------------------------------------------------------
# 17. JSON normalization consistency
# ---------------------------------------------------------------------------


def _sha256_json(obj: Any, hasher_id: str) -> "ContentHash":
    """Manually JSON-serialize *obj* with the same settings as BaseSemanticHasher
    and return the resulting ContentHash."""
    json_bytes = json.dumps(
        obj,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    digest = hashlib.sha256(json_bytes).digest()
    return ContentHash(hasher_id, digest)


class TestJsonNormalizationConsistency:
    """Verify that hash_object produces hashes identical to directly SHA-256
    hashing the canonical tagged-JSON form that _expand_structure produces.

    These tests treat BaseSemanticHasher as a black box and anchor its output to
    a human-verifiable serialization format, ensuring the algorithm is
    transparent and reproducible without the library.
    """

    HASHER_ID = "test_v1"

    @pytest.fixture
    def h(self) -> BaseSemanticHasher:
        return make_hasher(strict=True)

    # ------------------------------------------------------------------
    # Helper: build the normalized tagged tree by hand
    # ------------------------------------------------------------------

    def _dict_tree(self, items: dict) -> dict:
        """Produce the tagged dict form: {"__type__": "dict", "items": {sorted}}."""
        return {"__type__": "dict", "items": dict(sorted(items.items()))}

    def _list_tree(self, items: list) -> dict:
        return {"__type__": "list", "items": items}

    def _set_tree(self, items: list) -> dict:
        """Produce the tagged set form: {"__type__": "set", "items": [sorted by str]}."""
        return {"__type__": "set", "items": sorted(items, key=str)}

    def _tuple_tree(self, items: list) -> dict:
        return {"__type__": "tuple", "items": items}

    def _namedtuple_tree(self, name: str, fields: dict) -> dict:
        return {
            "__type__": "namedtuple",
            "name": name,
            "fields": dict(sorted(fields.items())),
        }

    # ------------------------------------------------------------------
    # Tests
    # ------------------------------------------------------------------

    def test_flat_dict(self, h):
        """A plain dict with string values."""
        structure = {"beta": 2, "alpha": 1}
        expected_tree = self._dict_tree({"beta": 2, "alpha": 1})
        assert h.hash_object(structure) == _sha256_json(expected_tree, self.HASHER_ID)

    def test_list_of_primitives(self, h):
        """A plain list of integers."""
        structure = [10, 20, 30]
        expected_tree = self._list_tree([10, 20, 30])
        assert h.hash_object(structure) == _sha256_json(expected_tree, self.HASHER_ID)

    def test_set_of_integers(self, h):
        """A set -- elements must be sorted by str() before hashing."""
        structure = {3, 1, 2}
        # str(1)="1", str(2)="2", str(3)="3" → already ascending
        expected_tree = self._set_tree([1, 2, 3])
        assert h.hash_object(structure) == _sha256_json(expected_tree, self.HASHER_ID)

    def test_tuple_of_primitives(self, h):
        structure = (7, 8, 9)
        expected_tree = self._tuple_tree([7, 8, 9])
        assert h.hash_object(structure) == _sha256_json(expected_tree, self.HASHER_ID)

    def test_namedtuple(self, h):
        """A namedtuple -- fields sorted alphabetically, name preserved."""
        Coord = namedtuple("Coord", ["y", "x"])
        structure = Coord(y=4, x=3)
        expected_tree = self._namedtuple_tree("Coord", {"y": 4, "x": 3})
        assert h.hash_object(structure) == _sha256_json(expected_tree, self.HASHER_ID)

    def test_nested_dict_with_list(self, h):
        """A dict whose value is a list."""
        structure = {"nums": [1, 2, 3], "label": "test"}
        expected_items = {
            "nums": self._list_tree([1, 2, 3]),
            "label": "test",
        }
        expected_tree = self._dict_tree(expected_items)
        assert h.hash_object(structure) == _sha256_json(expected_tree, self.HASHER_ID)

    def test_dict_with_unsorted_keys_normalises(self, h):
        """Insertion order must not affect the hash -- keys are always sorted."""
        # These two Python dicts are semantically identical; both should produce
        # the same normalized JSON and therefore the same hash.
        structure_forward = {"z": 26, "a": 1, "m": 13}
        structure_backward = {"m": 13, "z": 26, "a": 1}

        expected_tree = self._dict_tree({"z": 26, "a": 1, "m": 13})
        canonical_hash = _sha256_json(expected_tree, self.HASHER_ID)

        assert h.hash_object(structure_forward) == canonical_hash
        assert h.hash_object(structure_backward) == canonical_hash

    def test_set_with_string_elements(self, h):
        """Set of strings -- sorted lexicographically (same as str() sort for strs)."""
        structure = {"banana", "apple", "cherry"}
        expected_tree = self._set_tree(["apple", "banana", "cherry"])
        assert h.hash_object(structure) == _sha256_json(expected_tree, self.HASHER_ID)

    def test_deeply_nested_structure(self, h):
        """A multi-level nested structure to confirm the tree is built correctly."""
        structure = {"outer": {"inner": [True, None, 42]}, "flag": False}
        inner_list = self._list_tree([True, None, 42])
        inner_dict = self._dict_tree({"inner": inner_list})
        expected_tree = self._dict_tree({"outer": inner_dict, "flag": False})
        assert h.hash_object(structure) == _sha256_json(expected_tree, self.HASHER_ID)

    def test_primitive_string(self, h):
        """A bare string is JSON-serialized directly (no type-tagging wrapper)."""
        value = "hello world"
        assert h.hash_object(value) == _sha256_json(value, self.HASHER_ID)

    def test_primitive_int(self, h):
        value = 12345
        assert h.hash_object(value) == _sha256_json(value, self.HASHER_ID)

    def test_primitive_none(self, h):
        assert h.hash_object(None) == _sha256_json(None, self.HASHER_ID)

    def test_primitive_bool(self, h):
        assert h.hash_object(True) == _sha256_json(True, self.HASHER_ID)
        assert h.hash_object(False) == _sha256_json(False, self.HASHER_ID)


# ---------------------------------------------------------------------------
# 18. hash_object process_identity_structure flag
# ---------------------------------------------------------------------------


class TestProcessIdentityStructure:
    """
    Verify the two modes of hash_object when applied to ContentIdentifiable objects:

      process_identity_structure=False (default):
        hash_object defers to obj.content_hash(), which uses the object's own
        BaseSemanticHasher (potentially different from the calling hasher).
        The result reflects the object's local hasher configuration.

      process_identity_structure=True:
        hash_object calls obj.identity_structure() and hashes the result
        using the *calling* hasher, ignoring the object's local hasher.

    For non-ContentIdentifiable objects the flag has no observable effect.
    """

    def test_default_mode_uses_object_content_hash(self):
        """With process_identity_structure=False (default), hash_object returns
        exactly what obj.content_hash() returns -- using the object's own hasher."""
        obj_hasher = make_hasher(strict=True)
        calling_hasher = make_hasher(strict=True)
        # Give the object a *different* hasher (different hasher_id)
        obj_hasher_id_hasher = BaseSemanticHasher(hasher_id="obj_hasher_v1")
        rec = SimpleRecord("hello", 1, semantic_hasher=obj_hasher_id_hasher)

        result = calling_hasher.hash_object(rec, process_identity_structure=False)
        # Must equal what the object's own content_hash() returns
        assert result == rec.content_hash()
        # And its method tag must be the object's hasher_id, NOT the calling hasher's
        assert result.method == "obj_hasher_v1"

    def test_process_identity_structure_uses_calling_hasher(self):
        """With process_identity_structure=True, hash_object processes the
        identity_structure using the *calling* hasher."""
        obj_hasher = BaseSemanticHasher(hasher_id="obj_hasher_v1")
        calling_hasher = make_hasher(strict=True)  # hasher_id = "test_v1"
        rec = SimpleRecord("hello", 1, semantic_hasher=obj_hasher)

        result = calling_hasher.hash_object(rec, process_identity_structure=True)
        # Must equal hashing the identity_structure directly through the calling hasher
        assert result == calling_hasher.hash_object(rec.identity_structure())
        # The method tag must be the *calling* hasher's id
        assert result.method == "test_v1"

    def test_two_modes_differ_when_hashers_differ(self):
        """When the object's hasher differs from the calling hasher, the two modes
        produce different hashes."""
        obj_hasher = BaseSemanticHasher(hasher_id="obj_v99")
        calling_hasher = make_hasher(strict=True)  # hasher_id = "test_v1"
        rec = SimpleRecord("data", 42, semantic_hasher=obj_hasher)

        h_defer = calling_hasher.hash_object(rec, process_identity_structure=False)
        h_process = calling_hasher.hash_object(rec, process_identity_structure=True)

        # Different hasher_ids produce different ContentHash method tags
        assert h_defer.method != h_process.method
        # And therefore different hashes
        assert h_defer != h_process

    def test_two_modes_agree_when_hashers_are_equivalent(self):
        """When the object's hasher is equivalent to the calling hasher (same
        configuration, same hasher_id), both modes produce the same hash."""
        # Both use hasher_id="test_v1" with the same registry
        hasher_a = make_hasher(strict=True)
        hasher_b = make_hasher(strict=True)
        rec = SimpleRecord("same", 7, semantic_hasher=hasher_a)

        h_defer = hasher_b.hash_object(rec, process_identity_structure=False)
        h_process = hasher_b.hash_object(rec, process_identity_structure=True)

        assert h_defer == h_process

    def test_default_argument_is_false(self):
        """Calling hash_object without the flag is equivalent to False."""
        obj_hasher = BaseSemanticHasher(hasher_id="obj_hasher_v1")
        calling_hasher = make_hasher(strict=True)
        rec = SimpleRecord("x", 0, semantic_hasher=obj_hasher)

        assert calling_hasher.hash_object(rec) == calling_hasher.hash_object(
            rec, process_identity_structure=False
        )

    def test_content_hash_cached_result_used_in_defer_mode(self):
        """In defer mode the object's cached content_hash is reused -- calling
        hash_object twice returns the identical ContentHash object."""
        obj_hasher = BaseSemanticHasher(hasher_id="cached_v1")
        calling_hasher = make_hasher(strict=True)
        rec = SimpleRecord("y", 5, semantic_hasher=obj_hasher)

        # Prime the cache
        first_call = rec.content_hash()
        result = calling_hasher.hash_object(rec, process_identity_structure=False)
        # Should be the exact same object (cache hit)
        assert result is first_call

    # ------------------------------------------------------------------
    # Non-ContentIdentifiable objects: flag has no effect
    # ------------------------------------------------------------------

    def test_flag_has_no_effect_on_primitives(self):
        """process_identity_structure has no observable effect on primitives."""
        h = make_hasher(strict=True)
        for value in [42, "hello", None, True, 3.14]:
            assert h.hash_object(
                value, process_identity_structure=False
            ) == h.hash_object(value, process_identity_structure=True)

    def test_flag_has_no_effect_on_plain_structures(self):
        """process_identity_structure has no effect on plain dicts/lists/sets/tuples."""
        h = make_hasher(strict=True)
        structures = [
            [1, 2, 3],
            {"a": 1, "b": 2},
            {10, 20, 30},
            (7, 8, 9),
        ]
        for s in structures:
            assert h.hash_object(s, process_identity_structure=False) == h.hash_object(
                s, process_identity_structure=True
            )

    def test_flag_has_no_effect_on_content_hash_terminal(self):
        """process_identity_structure has no effect when the object is a ContentHash."""
        h = make_hasher(strict=True)
        ch = ContentHash("some_method", b"\xaa" * 32)
        assert h.hash_object(ch, process_identity_structure=False) is ch
        assert h.hash_object(ch, process_identity_structure=True) is ch

    def test_flag_has_no_effect_on_handler_dispatched_types(self):
        """process_identity_structure has no effect on types handled by a registered
        TypeHandler (e.g. bytes, UUID)."""
        h = make_hasher(strict=True)
        u = UUID("550e8400-e29b-41d4-a716-446655440000")
        assert h.hash_object(u, process_identity_structure=False) == h.hash_object(
            u, process_identity_structure=True
        )
        assert h.hash_object(
            b"data", process_identity_structure=False
        ) == h.hash_object(b"data", process_identity_structure=True)

    def test_nested_content_identifiable_in_structure_respects_defer_mode(self):
        """When a ContentIdentifiable is embedded inside a structure, the calling
        hasher expands the structure and encounters the CI object via _expand_element,
        which always calls hash_object(obj) to get a token.  In that context
        the default (defer) mode is used -- the embedded object contributes its
        own content_hash token to the parent structure."""
        obj_hasher = BaseSemanticHasher(hasher_id="inner_v1")
        calling_hasher = make_hasher(strict=True)
        inner = SimpleRecord("inner", 99, semantic_hasher=obj_hasher)

        # The token embedded for `inner` inside the list should equal inner.content_hash()
        token_from_inner_ch = calling_hasher.hash_object([inner.content_hash()])
        token_from_list = calling_hasher.hash_object([inner])
        assert token_from_inner_ch == token_from_list
