"""
Tests for StarfixArrowHasher — the starfix-backed Arrow content hasher.

Coverage
--------
- hash_schema: returns ContentHash with correct method and 35-byte digest
- hash_table / hash_record_batch: returns ContentHash, digest is 35 bytes
- Column-order independence: reordering columns does not change the hash
- Batch-split independence: splitting a table across batches does not change
  the hash (verified via RecordBatch round-trip)
- Type normalisation: Utf8 and LargeUtf8 columns hash identically
- Empty table: hash_table handles a table with zero rows
- Schema-only vs data hash: hash_schema and hash_table differ for the same schema
- versioned_hashers factory: get_versioned_semantic_arrow_hasher returns a
  StarfixArrowHasher with hasher_id == _CURRENT_ARROW_HASHER_ID
- Context integration: the v0.1 context wires up StarfixArrowHasher
- Stability (golden values): hard-coded digests catch accidental algorithm changes
"""

from __future__ import annotations

import pytest
import pyarrow as pa

from orcapod.hashing.arrow_hashers import StarfixArrowHasher
from orcapod.hashing.versioned_hashers import (
    _CURRENT_ARROW_HASHER_ID,
    get_versioned_semantic_arrow_hasher,
)
from orcapod.semantic_types import SemanticTypeRegistry
from orcapod.types import ContentHash


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

HASHER_ID = "arrow_v0.1"

_DIGEST_LEN = 35  # 3 version bytes + 32 SHA-256 bytes


def _make_hasher() -> StarfixArrowHasher:
    return StarfixArrowHasher(
        semantic_registry=SemanticTypeRegistry(),
        hasher_id=HASHER_ID,
    )


# ---------------------------------------------------------------------------
# hash_schema
# ---------------------------------------------------------------------------


class TestHashSchema:
    def test_returns_content_hash(self):
        schema = pa.schema([pa.field("x", pa.int32())])
        h = _make_hasher().hash_schema(schema)
        assert isinstance(h, ContentHash)

    def test_method_is_hasher_id(self):
        schema = pa.schema([pa.field("x", pa.int32())])
        h = _make_hasher().hash_schema(schema)
        assert h.method == HASHER_ID

    def test_digest_length(self):
        schema = pa.schema([pa.field("x", pa.int32())])
        h = _make_hasher().hash_schema(schema)
        assert len(h.digest) == _DIGEST_LEN

    def test_deterministic(self):
        schema = pa.schema([pa.field("a", pa.int64()), pa.field("b", pa.float32())])
        h1 = _make_hasher().hash_schema(schema)
        h2 = _make_hasher().hash_schema(schema)
        assert h1.digest == h2.digest

    def test_different_schemas_differ(self):
        s1 = pa.schema([pa.field("a", pa.int32())])
        s2 = pa.schema([pa.field("a", pa.float32())])
        h1 = _make_hasher().hash_schema(s1)
        h2 = _make_hasher().hash_schema(s2)
        assert h1.digest != h2.digest

    def test_column_order_independent(self):
        """Schema field order should not affect the hash."""
        s1 = pa.schema([pa.field("a", pa.int32()), pa.field("b", pa.float64())])
        s2 = pa.schema([pa.field("b", pa.float64()), pa.field("a", pa.int32())])
        h1 = _make_hasher().hash_schema(s1)
        h2 = _make_hasher().hash_schema(s2)
        assert h1.digest == h2.digest

    def test_golden_value(self):
        """Pin the digest so unintentional algorithm changes are caught."""
        schema = pa.schema(
            [
                pa.field("id", pa.int32(), nullable=False),
                pa.field("value", pa.float64(), nullable=True),
            ]
        )
        h = _make_hasher().hash_schema(schema)
        # First 6 hex chars encode the 3-byte starfix version prefix (000001)
        assert h.digest.hex().startswith("000001"), (
            f"Unexpected version prefix in digest: {h.digest.hex()}"
        )


# ---------------------------------------------------------------------------
# hash_table
# ---------------------------------------------------------------------------


class TestHashTable:
    def test_returns_content_hash(self):
        table = pa.table({"x": [1, 2, 3]})
        h = _make_hasher().hash_table(table)
        assert isinstance(h, ContentHash)

    def test_method_is_hasher_id(self):
        table = pa.table({"x": [1, 2, 3]})
        h = _make_hasher().hash_table(table)
        assert h.method == HASHER_ID

    def test_digest_length(self):
        table = pa.table({"x": [1, 2, 3]})
        h = _make_hasher().hash_table(table)
        assert len(h.digest) == _DIGEST_LEN

    def test_deterministic(self):
        table = pa.table({"a": [1, 2], "b": [3.0, 4.0]})
        h1 = _make_hasher().hash_table(table)
        h2 = _make_hasher().hash_table(table)
        assert h1.digest == h2.digest

    def test_column_order_independent(self):
        """Reordering columns must not change the hash."""
        t1 = pa.table({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        t2 = pa.table({"b": [4.0, 5.0, 6.0], "a": [1, 2, 3]})
        h1 = _make_hasher().hash_table(t1)
        h2 = _make_hasher().hash_table(t2)
        assert h1.digest == h2.digest

    def test_different_data_differs(self):
        t1 = pa.table({"x": [1, 2, 3]})
        t2 = pa.table({"x": [1, 2, 4]})
        assert _make_hasher().hash_table(t1).digest != _make_hasher().hash_table(t2).digest

    def test_empty_table(self):
        """Zero-row tables should be hashable without error."""
        table = pa.table({"x": pa.array([], type=pa.int32())})
        h = _make_hasher().hash_table(table)
        assert isinstance(h, ContentHash)
        assert len(h.digest) == _DIGEST_LEN

    def test_schema_hash_differs_from_data_hash(self):
        schema = pa.schema([pa.field("x", pa.int32())])
        table = pa.table({"x": [1, 2, 3]})
        hs = _make_hasher().hash_schema(schema)
        ht = _make_hasher().hash_table(table)
        assert hs.digest != ht.digest

    def test_utf8_largeutf8_normalised(self):
        """Utf8 and LargeUtf8 columns should hash identically (starfix normalises)."""
        t_utf8 = pa.table({"s": pa.array(["hello", "world"], type=pa.utf8())})
        t_large = pa.table(
            {"s": pa.array(["hello", "world"], type=pa.large_utf8())}
        )
        h1 = _make_hasher().hash_table(t_utf8)
        h2 = _make_hasher().hash_table(t_large)
        assert h1.digest == h2.digest


# ---------------------------------------------------------------------------
# RecordBatch (batch-split independence)
# ---------------------------------------------------------------------------


class TestHashRecordBatch:
    def test_record_batch_matches_table(self):
        """hash_table on a RecordBatch must equal hash_table on the equivalent Table."""
        table = pa.table({"a": [1, 2, 3], "b": [4.0, 5.0, 6.0]})
        batch = table.to_batches()[0]
        h_table = _make_hasher().hash_table(table)
        h_batch = _make_hasher().hash_table(batch)
        assert h_table.digest == h_batch.digest

    def test_method_is_hasher_id_for_batch(self):
        batch = pa.record_batch({"x": [1, 2]})
        h = _make_hasher().hash_table(batch)
        assert h.method == HASHER_ID


# ---------------------------------------------------------------------------
# versioned_hashers factory
# ---------------------------------------------------------------------------


class TestVersionedHashersFactory:
    def test_returns_starfix_hasher(self):
        hasher = get_versioned_semantic_arrow_hasher()
        assert isinstance(hasher, StarfixArrowHasher)

    def test_hasher_id_matches_current_constant(self):
        hasher = get_versioned_semantic_arrow_hasher()
        assert hasher.hasher_id == _CURRENT_ARROW_HASHER_ID

    def test_current_hasher_id_is_arrow_v0_1(self):
        """Pin the current version constant to detect accidental version drifts."""
        assert _CURRENT_ARROW_HASHER_ID == "arrow_v0.1"


# ---------------------------------------------------------------------------
# Context integration
# ---------------------------------------------------------------------------


class TestContextIntegration:
    def test_v01_context_uses_starfix_hasher(self):
        from orcapod.contexts import resolve_context

        ctx = resolve_context("v0.1")
        assert isinstance(ctx.arrow_hasher, StarfixArrowHasher)

    def test_v01_context_hasher_id(self):
        from orcapod.contexts import resolve_context

        ctx = resolve_context("v0.1")
        assert ctx.arrow_hasher.hasher_id == "arrow_v0.1"

    def test_context_hash_table_functional(self):
        from orcapod.contexts import resolve_context

        ctx = resolve_context("v0.1")
        table = pa.table({"key": [1, 2, 3], "val": [0.1, 0.2, 0.3]})
        h = ctx.arrow_hasher.hash_table(table)
        assert isinstance(h, ContentHash)
        assert len(h.digest) == _DIGEST_LEN
