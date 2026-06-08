"""Tests for tag_columns bare-string normalization (ENG-571).

Covers:
- _normalize_column_list: bare string, list, tuple, empty, invalid inputs
- Per-source integration: all user-facing sources accept bare string identically
  to a single-element list.
"""
from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterator
from typing import Any

import pyarrow as pa
import pytest

from orcapod.utils.schema_utils import _normalize_column_list


# ---------------------------------------------------------------------------
# Helper unit tests
# ---------------------------------------------------------------------------


class TestNormalizeColumnList:
    def test_bare_string_returns_single_element_list(self):
        assert _normalize_column_list("session_id") == ["session_id"]

    def test_single_element_list_unchanged(self):
        assert _normalize_column_list(["session_id"]) == ["session_id"]

    def test_multi_element_list_unchanged(self):
        assert _normalize_column_list(["a", "b", "c"]) == ["a", "b", "c"]

    def test_tuple_returns_list(self):
        assert _normalize_column_list(("a", "b")) == ["a", "b"]

    def test_empty_list_returns_empty_list(self):
        assert _normalize_column_list([]) == []

    def test_integer_raises_type_error(self):
        with pytest.raises(TypeError, match="tag_columns must be a string or iterable"):
            _normalize_column_list(42)

    def test_float_raises_type_error(self):
        with pytest.raises(TypeError, match="tag_columns must be a string or iterable"):
            _normalize_column_list(3.14)

    def test_list_with_non_string_element_raises_type_error(self):
        with pytest.raises(TypeError, match="All tag_columns elements must be strings"):
            _normalize_column_list([1, "b"])

    def test_list_with_all_non_string_elements_raises_type_error(self):
        with pytest.raises(TypeError, match="All tag_columns elements must be strings"):
            _normalize_column_list([1, 2, 3])
