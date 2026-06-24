"""Tests for UUIDSemanticHasher hash() method behaviour.

Verifies that UUIDSemanticHasher produces a ContentHash based on the 16-byte
binary representation of a UUID, consistent with OrcaPod's canonical
``pa.binary(16)`` Arrow storage format.
"""

from __future__ import annotations

import uuid as _uuid

from orcapod.hashing.semantic_hashing.semantic_hasher import SemanticAwarePythonHasher
from orcapod.types import ContentHash


def _make_hasher() -> SemanticAwarePythonHasher:
    from orcapod.hashing.semantic_hashing.builtin_handlers import (
        register_builtin_python_type_semantic_hashers,
    )
    from orcapod.hashing.semantic_hashing.type_handler_registry import (
        PythonTypeSemanticHasherRegistry,
    )

    registry = PythonTypeSemanticHasherRegistry()
    register_builtin_python_type_semantic_hashers(registry)
    return SemanticAwarePythonHasher(
        hasher_id="test_v1", type_semantic_hasher_registry=registry, strict=True
    )


def test_uuid_handler_returns_content_hash():
    """UUIDSemanticHasher should return a ContentHash for a UUID."""
    hasher = _make_hasher()
    u = _uuid.UUID("550e8400-e29b-41d4-a716-446655440000")
    result = hasher.hash_object(u)
    assert isinstance(result, ContentHash)


def test_uuid_handler_same_uuid_same_hash():
    """Same UUID value produces the same ContentHash."""
    hasher = _make_hasher()
    u = _uuid.UUID("550e8400-e29b-41d4-a716-446655440000")
    assert hasher.hash_object(u) == hasher.hash_object(u)


def test_uuid_handler_different_uuids_produce_different_hashes():
    """Different UUID values must produce different ContentHash objects."""
    hasher = _make_hasher()
    u1 = _uuid.uuid4()
    u2 = _uuid.uuid4()
    assert hasher.hash_object(u1) != hasher.hash_object(u2)
