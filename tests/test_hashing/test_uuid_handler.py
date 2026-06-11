"""Tests for UUIDHandler low-level handle() method behaviour.

Verifies that UUIDHandler returns the 16-byte binary representation of a
UUID, consistent with OrcaPod's canonical ``pa.binary(16)`` Arrow storage
format.
"""

from __future__ import annotations

import uuid as _uuid


def test_uuid_handler_returns_bytes():
    """UUIDHandler should return the 16-byte binary representation."""
    from orcapod.hashing.semantic_hashing.builtin_handlers import UUIDHandler

    handler = UUIDHandler()
    u = _uuid.UUID("550e8400-e29b-41d4-a716-446655440000")
    result = handler.handle(u, hasher=None)  # type: ignore[arg-type]
    assert result == u.bytes
    assert isinstance(result, bytes)
    assert len(result) == 16


def test_uuid_handler_different_uuids_produce_different_bytes():
    from orcapod.hashing.semantic_hashing.builtin_handlers import UUIDHandler

    handler = UUIDHandler()
    u1 = _uuid.uuid4()
    u2 = _uuid.uuid4()
    assert handler.handle(u1, None) != handler.handle(u2, None)  # type: ignore[arg-type]
