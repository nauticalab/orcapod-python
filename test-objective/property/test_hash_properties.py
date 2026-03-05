"""Property-based tests for hashing determinism and ContentHash roundtrips.

Tests that hashing invariants hold for any valid input.
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from orcapod.contexts import get_default_context
from orcapod.types import ContentHash


# ---------------------------------------------------------------------------
# Strategies
# ---------------------------------------------------------------------------

# Primitives that the hasher should handle
hashable_primitives = st.one_of(
    st.integers(min_value=-10000, max_value=10000),
    st.floats(allow_nan=False, allow_infinity=False),
    st.text(min_size=0, max_size=50),
    st.booleans(),
    st.none(),
)

# ContentHash strategy
content_hashes = st.builds(
    ContentHash,
    method=st.text(min_size=1, max_size=20).filter(lambda s: ":" not in s),
    digest=st.binary(min_size=4, max_size=32),
)


# ===================================================================
# Hash determinism
# ===================================================================


class TestHashDeterminism:
    """Per design: hash(X) == hash(X) for any X."""

    @given(hashable_primitives)
    @settings(max_examples=50)
    def test_same_input_same_hash(self, value):
        ctx = get_default_context()
        hasher = ctx.semantic_hasher
        h1 = hasher.hash_object(value)
        h2 = hasher.hash_object(value)
        assert h1 == h2


# ===================================================================
# ContentHash string roundtrip
# ===================================================================


class TestContentHashStringRoundtrip:
    """Per design: from_string(to_string(h)) == h."""

    @given(content_hashes)
    @settings(max_examples=50)
    def test_roundtrip(self, h):
        s = h.to_string()
        recovered = ContentHash.from_string(s)
        assert recovered.method == h.method
        assert recovered.digest == h.digest


class TestContentHashHexConsistency:
    """to_hex() truncation should be consistent."""

    @given(content_hashes, st.integers(min_value=1, max_value=64))
    @settings(max_examples=50)
    def test_truncation_is_prefix(self, h, length):
        full_hex = h.to_hex()
        truncated = h.to_hex(length)
        assert full_hex.startswith(truncated)


class TestContentHashEquality:
    """Equal ContentHash objects have equal conversions."""

    @given(content_hashes)
    @settings(max_examples=50)
    def test_equal_hashes_equal_conversions(self, h):
        h2 = ContentHash(h.method, h.digest)
        assert h.to_hex() == h2.to_hex()
        assert h.to_int() == h2.to_int()
        assert h.to_uuid() == h2.to_uuid()
        assert h.to_base64() == h2.to_base64()
