"""Tests for LogicalPandasDataFrame and LogicalPandasSeries."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

from orcapod.extension_types.protocols import LogicalTypeProtocol


class TestLogicalPandasDataFrameProtocol:
    def test_isinstance_logical_type(self):
        from orcapod.extension_types.pandas_type import LogicalPandasDataFrame
        assert isinstance(LogicalPandasDataFrame(), LogicalTypeProtocol)

    def test_logical_type_name(self):
        from orcapod.extension_types.pandas_type import LogicalPandasDataFrame
        assert LogicalPandasDataFrame().logical_type_name == "pandas.dataframe"

    def test_python_type(self):
        from orcapod.extension_types.pandas_type import LogicalPandasDataFrame
        assert LogicalPandasDataFrame().python_type is pd.DataFrame

    def test_arrow_ext_name(self):
        from orcapod.extension_types.pandas_type import LogicalPandasDataFrame
        assert LogicalPandasDataFrame().get_arrow_extension_type().extension_name == "pandas.dataframe"

    def test_arrow_ext_storage_type(self):
        from orcapod.extension_types.pandas_type import LogicalPandasDataFrame
        assert LogicalPandasDataFrame().get_arrow_extension_type().storage_type == pa.large_binary()

    def test_arrow_ext_is_cached(self):
        from orcapod.extension_types.pandas_type import LogicalPandasDataFrame
        lt = LogicalPandasDataFrame()
        assert lt.get_arrow_extension_type() is lt.get_arrow_extension_type()

    def test_polars_ext_is_cached(self):
        from orcapod.extension_types.pandas_type import LogicalPandasDataFrame
        lt = LogicalPandasDataFrame()
        assert lt.get_polars_extension_type() is lt.get_polars_extension_type()
