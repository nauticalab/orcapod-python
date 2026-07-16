# tests/test_core/test_empty_data.py
"""Tests for EmptyData and its associated exception types."""
from __future__ import annotations

import pytest

from orcapod.errors import (
    EmptyDataAccessError,
    EmptyDataHashMissingError,
    EphemeralResultMissingError,
)


class TestExceptionTypes:
    def test_empty_data_access_error_is_exception(self):
        exc = EmptyDataAccessError("sentinel", "as_dict")
        assert isinstance(exc, Exception)
        assert exc.empty_data == "sentinel"
        assert exc.method_name == "as_dict"

    def test_empty_data_hash_missing_error_is_exception(self):
        exc = EmptyDataHashMissingError("sentinel")
        assert isinstance(exc, Exception)
        assert exc.empty_data == "sentinel"

    def test_ephemeral_result_missing_error_is_exception(self):
        exc = EphemeralResultMissingError(
            tag="tag",
            cached_content_hash=None,
            node_identity_path=("a", "b"),
            message="gone",
        )
        assert isinstance(exc, Exception)
        assert exc.tag == "tag"
        assert exc.cached_content_hash is None
        assert exc.node_identity_path == ("a", "b")
        assert "gone" in str(exc)
