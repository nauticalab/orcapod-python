"""Tests for schema_utils utility functions."""

from __future__ import annotations

import pytest

from orcapod.contexts import resolve_context
from orcapod.types import Schema


@pytest.fixture
def semantic_hasher():
    return resolve_context(None).semantic_hasher


class TestComputeSchemaHash:
    def test_returns_string(self, semantic_hasher):
        from orcapod.utils.schema_utils import compute_schema_hash

        result = compute_schema_hash(
            Schema({"id": int}),
            Schema({"value": float}),
            semantic_hasher,
            None,
        )
        assert isinstance(result, str)

    def test_deterministic(self, semantic_hasher):
        from orcapod.utils.schema_utils import compute_schema_hash

        tag = Schema({"id": int})
        data = Schema({"value": float})
        assert compute_schema_hash(tag, data, semantic_hasher, None) == \
               compute_schema_hash(tag, data, semantic_hasher, None)

    def test_full_length_when_char_count_is_none(self, semantic_hasher):
        """When char_count is None (the default), the full SHA-256 hex digest is returned."""
        from orcapod.utils.schema_utils import compute_schema_hash

        result = compute_schema_hash(
            Schema({"id": int}), Schema({"value": float}), semantic_hasher, None
        )
        # SHA-256 digest is 32 bytes = 64 hex characters
        assert len(result) == 64

    def test_length_equals_char_count_when_explicit(self, semantic_hasher):
        """When an explicit char_count is given, result is truncated to that length."""
        from orcapod.utils.schema_utils import compute_schema_hash

        result = compute_schema_hash(
            Schema({"id": int}), Schema({"value": float}), semantic_hasher, 12
        )
        assert len(result) == 12

    def test_different_schemas_produce_different_hashes(self, semantic_hasher):
        from orcapod.utils.schema_utils import compute_schema_hash

        h1 = compute_schema_hash(Schema({"id": int}), Schema({"v": float}), semantic_hasher, None)
        h2 = compute_schema_hash(Schema({"id": int}), Schema({"v": int}), semantic_hasher, None)
        assert h1 != h2
