"""Tests for PandasDataFrameHandler and PandasSeriesHandler (``pandas_*_v1``, ITL-897)."""

from __future__ import annotations

import pandas as pd
import pytest

from orcapod.contexts import get_default_context
from orcapod.types import ContentHash

_HASHER = get_default_context().semantic_hasher


class TestPandasDataFrameHandler:
    def test_returns_content_hash(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import (
            PandasDataFrameHandler,
        )

        df = pd.DataFrame({"a": [1.0, 2.0], "b": [3, 4]})
        result = PandasDataFrameHandler().handle(df, hasher=_HASHER)
        assert isinstance(result, ContentHash)
        assert result.method == "pandas_dataframe_v1"

    def test_same_content_same_hash(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import (
            PandasDataFrameHandler,
        )

        handler = PandasDataFrameHandler()
        df = pd.DataFrame({"x": [1.0, 2.0, 3.0]})
        assert handler.handle(df, _HASHER) == handler.handle(df.copy(), _HASHER)

    def test_different_data_different_hash(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import (
            PandasDataFrameHandler,
        )

        handler = PandasDataFrameHandler()
        df1 = pd.DataFrame({"x": [1.0, 2.0]})
        df2 = pd.DataFrame({"x": [3.0, 4.0]})
        assert (
            handler.handle(df1, _HASHER).digest != handler.handle(df2, _HASHER).digest
        )

    def test_different_index_different_hash(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import (
            PandasDataFrameHandler,
        )

        handler = PandasDataFrameHandler()
        df1 = pd.DataFrame({"x": [1, 2]}, index=[0, 1])
        df2 = pd.DataFrame({"x": [1, 2]}, index=[10, 20])
        assert (
            handler.handle(df1, _HASHER).digest != handler.handle(df2, _HASHER).digest
        )

    def test_different_column_names_different_hash(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import (
            PandasDataFrameHandler,
        )

        handler = PandasDataFrameHandler()
        df1 = pd.DataFrame({"a": [1, 2]})
        df2 = pd.DataFrame({"b": [1, 2]})
        assert (
            handler.handle(df1, _HASHER).digest != handler.handle(df2, _HASHER).digest
        )

    def test_wrong_type_raises_type_error(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import (
            PandasDataFrameHandler,
        )

        with pytest.raises(TypeError, match="PandasDataFrameHandler"):
            PandasDataFrameHandler().handle("not a dataframe", hasher=_HASHER)

    def test_non_arrow_column_raises_value_error(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import (
            PandasDataFrameHandler,
        )

        class _Opaque:
            pass

        df = pd.DataFrame({"bad": [_Opaque(), _Opaque()]})
        with pytest.raises(ValueError, match="cannot convert"):
            PandasDataFrameHandler().handle(df, hasher=_HASHER)


class TestPandasSeriesHandler:
    def test_returns_content_hash(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import (
            PandasSeriesHandler,
        )

        s = pd.Series([1.0, 2.0, 3.0], name="x")
        result = PandasSeriesHandler().handle(s, hasher=_HASHER)
        assert isinstance(result, ContentHash)
        assert result.method == "pandas_series_v1"

    def test_same_content_same_hash(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import (
            PandasSeriesHandler,
        )

        handler = PandasSeriesHandler()
        s = pd.Series([1.0, 2.0, 3.0])
        assert handler.handle(s, _HASHER) == handler.handle(s.copy(), _HASHER)

    def test_different_data_different_hash(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import (
            PandasSeriesHandler,
        )

        handler = PandasSeriesHandler()
        s1 = pd.Series([1.0, 2.0])
        s2 = pd.Series([3.0, 4.0])
        assert handler.handle(s1, _HASHER).digest != handler.handle(s2, _HASHER).digest

    def test_unnamed_and_named_different_hash(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import (
            PandasSeriesHandler,
        )

        handler = PandasSeriesHandler()
        s1 = pd.Series([1.0, 2.0])  # name=None
        s2 = pd.Series([1.0, 2.0], name="x")  # name="x"
        assert handler.handle(s1, _HASHER).digest != handler.handle(s2, _HASHER).digest

    def test_different_index_different_hash(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import (
            PandasSeriesHandler,
        )

        handler = PandasSeriesHandler()
        s1 = pd.Series([1, 2], index=[0, 1], name="v")
        s2 = pd.Series([1, 2], index=[10, 20], name="v")
        assert handler.handle(s1, _HASHER).digest != handler.handle(s2, _HASHER).digest

    def test_wrong_type_raises_type_error(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import (
            PandasSeriesHandler,
        )

        with pytest.raises(TypeError, match="PandasSeriesHandler"):
            PandasSeriesHandler().handle(42, hasher=_HASHER)

    def test_storage_sentinel_name_hashes_apart_from_unnamed(self):
        """The unnamed-Series sentinel is a storage detail; hashing types the name itself."""
        from orcapod.hashing.semantic_hashing.builtin_handlers import (
            PandasSeriesHandler,
        )

        handler = PandasSeriesHandler()
        named = pd.Series([1.0, 2.0], name="__pandas_series_unnamed__")
        unnamed = pd.Series([1.0, 2.0])
        assert handler.handle(named, _HASHER) != handler.handle(unnamed, _HASHER)
