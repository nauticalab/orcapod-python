"""Tests for PandasDataFrameHandler and PandasSeriesHandler."""
from __future__ import annotations

import pandas as pd
import pytest

from orcapod.contexts import get_default_context
from orcapod.types import ContentHash


@pytest.fixture(scope="module")
def arrow_hasher():
    """Return the default context's arrow hasher for explicit handler construction."""
    return get_default_context().arrow_hasher


class TestPandasDataFrameHandler:
    def test_returns_content_hash(self, arrow_hasher):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasDataFrameHandler
        df = pd.DataFrame({"a": [1.0, 2.0], "b": [3, 4]})
        result = PandasDataFrameHandler(arrow_hasher).handle(df, hasher=None)
        assert isinstance(result, ContentHash)
        assert result.method == "arrow_v0.1"

    def test_same_content_same_hash(self, arrow_hasher):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasDataFrameHandler
        handler = PandasDataFrameHandler(arrow_hasher)
        df = pd.DataFrame({"x": [1.0, 2.0, 3.0]})
        assert handler.handle(df, None) == handler.handle(df.copy(), None)

    def test_different_data_different_hash(self, arrow_hasher):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasDataFrameHandler
        handler = PandasDataFrameHandler(arrow_hasher)
        df1 = pd.DataFrame({"x": [1.0, 2.0]})
        df2 = pd.DataFrame({"x": [3.0, 4.0]})
        assert handler.handle(df1, None).digest != handler.handle(df2, None).digest

    def test_different_index_different_hash(self, arrow_hasher):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasDataFrameHandler
        handler = PandasDataFrameHandler(arrow_hasher)
        df1 = pd.DataFrame({"x": [1, 2]}, index=[0, 1])
        df2 = pd.DataFrame({"x": [1, 2]}, index=[10, 20])
        assert handler.handle(df1, None).digest != handler.handle(df2, None).digest

    def test_different_column_names_different_hash(self, arrow_hasher):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasDataFrameHandler
        handler = PandasDataFrameHandler(arrow_hasher)
        df1 = pd.DataFrame({"a": [1, 2]})
        df2 = pd.DataFrame({"b": [1, 2]})
        assert handler.handle(df1, None).digest != handler.handle(df2, None).digest

    def test_wrong_type_raises_type_error(self, arrow_hasher):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasDataFrameHandler
        with pytest.raises(TypeError, match="PandasDataFrameHandler"):
            PandasDataFrameHandler(arrow_hasher).handle("not a dataframe", hasher=None)

    def test_non_arrow_column_raises_value_error(self, arrow_hasher):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasDataFrameHandler

        class _Opaque:
            pass

        df = pd.DataFrame({"bad": [_Opaque(), _Opaque()]})
        with pytest.raises(ValueError, match="cannot convert"):
            PandasDataFrameHandler(arrow_hasher).handle(df, hasher=None)


class TestPandasSeriesHandler:
    def test_returns_content_hash(self, arrow_hasher):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasSeriesHandler
        s = pd.Series([1.0, 2.0, 3.0], name="x")
        result = PandasSeriesHandler(arrow_hasher).handle(s, hasher=None)
        assert isinstance(result, ContentHash)
        assert result.method == "arrow_v0.1"

    def test_same_content_same_hash(self, arrow_hasher):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasSeriesHandler
        handler = PandasSeriesHandler(arrow_hasher)
        s = pd.Series([1.0, 2.0, 3.0])
        assert handler.handle(s, None) == handler.handle(s.copy(), None)

    def test_different_data_different_hash(self, arrow_hasher):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasSeriesHandler
        handler = PandasSeriesHandler(arrow_hasher)
        s1 = pd.Series([1.0, 2.0])
        s2 = pd.Series([3.0, 4.0])
        assert handler.handle(s1, None).digest != handler.handle(s2, None).digest

    def test_unnamed_and_named_different_hash(self, arrow_hasher):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasSeriesHandler
        handler = PandasSeriesHandler(arrow_hasher)
        s1 = pd.Series([1.0, 2.0])            # name=None → stored as sentinel
        s2 = pd.Series([1.0, 2.0], name="x")  # name="x"
        assert handler.handle(s1, None).digest != handler.handle(s2, None).digest

    def test_different_index_different_hash(self, arrow_hasher):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasSeriesHandler
        handler = PandasSeriesHandler(arrow_hasher)
        s1 = pd.Series([1, 2], index=[0, 1], name="v")
        s2 = pd.Series([1, 2], index=[10, 20], name="v")
        assert handler.handle(s1, None).digest != handler.handle(s2, None).digest

    def test_wrong_type_raises_type_error(self, arrow_hasher):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasSeriesHandler
        with pytest.raises(TypeError, match="PandasSeriesHandler"):
            PandasSeriesHandler(arrow_hasher).handle(42, hasher=None)

    def test_reserved_name_raises_value_error(self, arrow_hasher):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasSeriesHandler
        s = pd.Series([1.0, 2.0], name="__pandas_series_unnamed__")
        with pytest.raises(ValueError, match="reserved"):
            PandasSeriesHandler(arrow_hasher).handle(s, hasher=None)
