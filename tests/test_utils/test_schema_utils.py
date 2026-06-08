"""Tests for schema_utils utility functions."""

from __future__ import annotations

import pytest

from orcapod.config import DEFAULT_CONFIG
from orcapod.contexts import resolve_context
from orcapod.types import Schema


@pytest.fixture
def semantic_hasher():
    return resolve_context(None).semantic_hasher


@pytest.fixture
def char_count():
    return DEFAULT_CONFIG.hashing.schema_n_char


class TestComputeSchemaHash:
    def test_returns_string(self, semantic_hasher, char_count):
        from orcapod.utils.schema_utils import compute_schema_hash

        result = compute_schema_hash(
            Schema({"id": int}),
            Schema({"value": float}),
            semantic_hasher,
            char_count,
        )
        assert isinstance(result, str)

    def test_deterministic(self, semantic_hasher, char_count):
        from orcapod.utils.schema_utils import compute_schema_hash

        tag = Schema({"id": int})
        data = Schema({"value": float})
        assert compute_schema_hash(tag, data, semantic_hasher, char_count) == \
               compute_schema_hash(tag, data, semantic_hasher, char_count)

    def test_length_equals_char_count(self, semantic_hasher, char_count):
        from orcapod.utils.schema_utils import compute_schema_hash

        result = compute_schema_hash(
            Schema({"id": int}), Schema({"value": float}), semantic_hasher, char_count
        )
        assert len(result) == char_count

    def test_different_schemas_produce_different_hashes(self, semantic_hasher, char_count):
        from orcapod.utils.schema_utils import compute_schema_hash

        h1 = compute_schema_hash(Schema({"id": int}), Schema({"v": float}), semantic_hasher, char_count)
        h2 = compute_schema_hash(Schema({"id": int}), Schema({"v": int}), semantic_hasher, char_count)
        assert h1 != h2
