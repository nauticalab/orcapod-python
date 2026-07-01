# pandas.DataFrame and pandas.Series Native Types Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use sensei:subagent-driven-development (recommended) or sensei:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `pd.DataFrame` and `pd.Series` as native orcapod value types — serialised as Arrow IPC stream bytes in `large_binary` extension columns — so the `UniversalTypeConverter` no longer raises `ValueError: Unsupported Python type` for these types.

**Architecture:** Each type gets a `LogicalXxx(BaseLogicalType)` class in a new `pandas_type.py` module, following the exact structure of `numpy_type.py`. Storage uses Arrow IPC stream bytes (`preserve_index=True`) stored as `large_binary`. Content hashing uses SHA-256 of those same IPC bytes, returned directly as `ContentHash`. Both types and handlers are registered in `contexts/data/v0.1.json`.

**Tech Stack:** `pandas>=2.2.3` (already a first-class dependency), `pyarrow` (already a dependency), `pyarrow.ipc` for IPC stream serialisation, `pytest`, `uv run` for all commands.

---

## File Map

| Action | Path | Responsibility |
|--------|------|----------------|
| **Create** | `src/orcapod/extension_types/pandas_type.py` | `LogicalPandasDataFrame` + `LogicalPandasSeries` logical types |
| **Modify** | `src/orcapod/hashing/semantic_hashing/builtin_handlers.py` | Add `PandasDataFrameHandler` + `PandasSeriesHandler` |
| **Modify** | `src/orcapod/contexts/data/v0.1.json` | Register both logical types + both handlers |
| **Create** | `tests/test_extension_types/test_pandas_type.py` | Unit tests for both logical types |
| **Create** | `tests/test_hashing/test_pandas_handlers.py` | Unit tests for both hash handlers |
| **Modify** | `tests/test_extension_types/test_roundtrips.py` | End-to-end integration tests |

---

## Task 1: `LogicalPandasDataFrame` — class shell + protocol tests

**Files:**
- Create: `src/orcapod/extension_types/pandas_type.py`
- Create: `tests/test_extension_types/test_pandas_type.py`

- [ ] **Step 1.1: Write failing protocol tests**

Create `tests/test_extension_types/test_pandas_type.py`:

```python
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
```

- [ ] **Step 1.2: Run to verify failure**

```bash
uv run pytest tests/test_extension_types/test_pandas_type.py::TestLogicalPandasDataFrameProtocol -v
```

Expected: `ModuleNotFoundError: No module named 'orcapod.extension_types.pandas_type'`

- [ ] **Step 1.3: Create `pandas_type.py` with the class shell**

Create `src/orcapod/extension_types/pandas_type.py`:

```python
"""``pandas.DataFrame`` and ``pandas.Series`` logical types for orcapod.

``LogicalPandasDataFrame`` maps ``pd.DataFrame`` <-> Arrow ``large_binary`` using
Arrow IPC stream format as the storage envelope, with ``preserve_index=True`` so
that all index kinds (RangeIndex, named index, MultiIndex) round-trip losslessly.

``LogicalPandasSeries`` maps ``pd.Series`` <-> Arrow ``large_binary`` by wrapping
the Series as a single-column DataFrame before applying the same IPC path. The
Series name and index are both preserved. An unnamed Series (``name=None``) uses
the sentinel column name ``"__orcapod:unnamed__"`` to distinguish it from a Series
genuinely named ``"__orcapod:unnamed__"`` (the latter would be a bug in user code).
"""

from __future__ import annotations

import io
from typing import TYPE_CHECKING, Any

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.ipc

from orcapod.extension_types.base_logical_type import BaseLogicalType
from orcapod.extension_types.registry import make_arrow_extension_type, make_polars_extension_type

if TYPE_CHECKING:
    from orcapod.extension_types.protocols import TypeConverterProtocol


_SERIES_UNNAMED_SENTINEL = "__orcapod:unnamed__"


def _table_to_ipc_bytes(table: pa.Table) -> bytes:
    """Serialise an Arrow Table to IPC stream bytes.

    Args:
        table: The Arrow Table to serialise.

    Returns:
        Raw bytes in Arrow IPC stream format.
    """
    buf = io.BytesIO()
    with pa.ipc.new_stream(buf, table.schema) as writer:
        writer.write_table(table)
    return buf.getvalue()


def _ipc_bytes_to_table(data: Any) -> pa.Table:
    """Deserialise IPC stream bytes to an Arrow Table.

    Args:
        data: Raw bytes (or buffer) containing an Arrow IPC stream.

    Returns:
        The deserialised Arrow Table.
    """
    buf = io.BytesIO(bytes(data))
    return pa.ipc.open_stream(buf).read_all()


class LogicalPandasDataFrame(BaseLogicalType):
    """Logical type for ``pd.DataFrame``.

    Stores DataFrames as Arrow ``large_binary`` using Arrow IPC stream format.
    ``preserve_index=True`` ensures that all index kinds — default ``RangeIndex``,
    named indices, and ``MultiIndex`` — are round-tripped losslessly.

    Any DataFrame whose columns are Arrow-serialisable (numeric, string/object,
    nullable integer, boolean, datetime, categorical) is supported. Columns with
    truly mixed Python types that Arrow cannot infer raise ``ValueError``.

    The extension name ``"pandas.dataframe"`` is library-qualified, following the
    ``"numpy.ndarray"`` precedent.

    Example:
        >>> lt = LogicalPandasDataFrame()
        >>> df = pd.DataFrame({"x": [1.0, 2.0]}, index=pd.Index([10, 20], name="id"))
        >>> recovered = lt.storage_to_python(lt.python_to_storage(df))
        >>> df.equals(recovered)
        True
    """

    _arrow_ext_class = make_arrow_extension_type("pandas.dataframe", pa.large_binary())
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type("pandas.dataframe", pa.large_binary())
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "pandas.dataframe"
    python_type: type = pd.DataFrame

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the cached Arrow extension type for ``pd.DataFrame``.

        Returns:
            A ``pa.ExtensionType`` instance with extension name
            ``"pandas.dataframe"`` and storage type ``pa.large_binary()``.
        """
        if LogicalPandasDataFrame._arrow_ext is None:
            LogicalPandasDataFrame._arrow_ext = LogicalPandasDataFrame._arrow_ext_class()
        return LogicalPandasDataFrame._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the cached Polars extension type for ``pd.DataFrame``.

        Returns:
            A ``pl.BaseExtension`` instance registered under ``"pandas.dataframe"``.
        """
        if LogicalPandasDataFrame._polars_ext is None:
            LogicalPandasDataFrame._polars_ext = LogicalPandasDataFrame._polars_ext_class()
        return LogicalPandasDataFrame._polars_ext

    def python_to_storage(self, value: Any, converter: TypeConverterProtocol | None = None) -> bytes:
        """Serialise a ``pd.DataFrame`` to Arrow IPC stream bytes.

        Args:
            value: A ``pd.DataFrame`` instance. All columns must be
                Arrow-serialisable (numeric, string, nullable int, bool,
                datetime, categorical). Columns with mixed non-Arrow types raise
                ``ValueError``.
            converter: Ignored. Present for protocol conformance.

        Returns:
            Raw bytes in Arrow IPC stream format encoding the DataFrame's
            schema, index, and data.

        Raises:
            ValueError: If any column cannot be converted to an Arrow type.
        """
        try:
            table = pa.Table.from_pandas(value, preserve_index=True)
        except pa.lib.ArrowInvalid as exc:
            raise ValueError(
                f"LogicalPandasDataFrame: cannot convert DataFrame to Arrow. "
                f"Check for columns with mixed or non-Arrow-serialisable types. "
                f"Original error: {exc}"
            ) from exc
        return _table_to_ipc_bytes(table)

    def storage_to_python(self, storage_value: Any, converter: TypeConverterProtocol | None = None) -> pd.DataFrame:
        """Reconstruct a ``pd.DataFrame`` from Arrow IPC stream bytes.

        Args:
            storage_value: Raw bytes as stored in Arrow ``large_binary``.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A ``pd.DataFrame`` with columns, dtypes, and index as originally stored.
        """
        return _ipc_bytes_to_table(storage_value).to_pandas()


class LogicalPandasSeries(BaseLogicalType):
    """Logical type for ``pd.Series``.

    Stores Series as Arrow ``large_binary`` using Arrow IPC stream format.
    The Series is wrapped as a single-column DataFrame before serialisation,
    which preserves the Series name and index losslessly.

    An unnamed Series (``name=None``) uses the internal sentinel column name
    ``"__orcapod:unnamed__"`` to distinguish it from a Series named with an
    empty string.

    The extension name ``"pandas.series"`` is library-qualified.

    Example:
        >>> lt = LogicalPandasSeries()
        >>> s = pd.Series([1.0, 2.0, 3.0], name="metric")
        >>> recovered = lt.storage_to_python(lt.python_to_storage(s))
        >>> s.equals(recovered)
        True
    """

    _arrow_ext_class = make_arrow_extension_type("pandas.series", pa.large_binary())
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type("pandas.series", pa.large_binary())
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "pandas.series"
    python_type: type = pd.Series

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the cached Arrow extension type for ``pd.Series``.

        Returns:
            A ``pa.ExtensionType`` instance with extension name
            ``"pandas.series"`` and storage type ``pa.large_binary()``.
        """
        if LogicalPandasSeries._arrow_ext is None:
            LogicalPandasSeries._arrow_ext = LogicalPandasSeries._arrow_ext_class()
        return LogicalPandasSeries._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the cached Polars extension type for ``pd.Series``.

        Returns:
            A ``pl.BaseExtension`` instance registered under ``"pandas.series"``.
        """
        if LogicalPandasSeries._polars_ext is None:
            LogicalPandasSeries._polars_ext = LogicalPandasSeries._polars_ext_class()
        return LogicalPandasSeries._polars_ext

    def python_to_storage(self, value: Any, converter: TypeConverterProtocol | None = None) -> bytes:
        """Serialise a ``pd.Series`` to Arrow IPC stream bytes.

        Args:
            value: A ``pd.Series`` instance. The series name and index are
                preserved. An unnamed series (``name=None``) is stored under
                the sentinel column name ``"__orcapod:unnamed__"``.
            converter: Ignored. Present for protocol conformance.

        Returns:
            Raw bytes in Arrow IPC stream format.

        Raises:
            ValueError: If the Series values cannot be converted to an Arrow type.
        """
        col_name = value.name if value.name is not None else _SERIES_UNNAMED_SENTINEL
        df = value.to_frame(name=col_name)
        try:
            table = pa.Table.from_pandas(df, preserve_index=True)
        except pa.lib.ArrowInvalid as exc:
            raise ValueError(
                f"LogicalPandasSeries: cannot convert Series to Arrow. "
                f"Check for non-Arrow-serialisable values. "
                f"Original error: {exc}"
            ) from exc
        return _table_to_ipc_bytes(table)

    def storage_to_python(self, storage_value: Any, converter: TypeConverterProtocol | None = None) -> pd.Series:
        """Reconstruct a ``pd.Series`` from Arrow IPC stream bytes.

        Args:
            storage_value: Raw bytes as stored in Arrow ``large_binary``.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A ``pd.Series`` with its original name and index restored.
        """
        df = _ipc_bytes_to_table(storage_value).to_pandas()
        series = df.iloc[:, 0]
        if series.name == _SERIES_UNNAMED_SENTINEL:
            series = series.rename(None)
        return series
```

- [ ] **Step 1.4: Run protocol tests to verify they pass**

```bash
uv run pytest tests/test_extension_types/test_pandas_type.py::TestLogicalPandasDataFrameProtocol -v
```

Expected: 7 tests PASS

- [ ] **Step 1.5: Commit**

```bash
git add src/orcapod/extension_types/pandas_type.py tests/test_extension_types/test_pandas_type.py
git commit -m "feat(extension_types): add LogicalPandasDataFrame class shell with protocol tests"
```

---

## Task 2: `LogicalPandasDataFrame` — round-trip tests

**Files:**
- Modify: `tests/test_extension_types/test_pandas_type.py` (add classes)
- (Implementation already complete in Task 1's class shell)

- [ ] **Step 2.1: Add storage and round-trip tests**

Append to `tests/test_extension_types/test_pandas_type.py`:

```python
class TestLogicalPandasDataFrameStorage:
    def test_python_to_storage_returns_bytes(self):
        from orcapod.extension_types.pandas_type import LogicalPandasDataFrame
        df = pd.DataFrame({"a": [1.0, 2.0], "b": [3, 4]})
        result = LogicalPandasDataFrame().python_to_storage(df)
        assert isinstance(result, bytes)

    def test_non_arrow_column_raises_value_error(self):
        from orcapod.extension_types.pandas_type import LogicalPandasDataFrame

        class _Opaque:
            pass

        df = pd.DataFrame({"bad": [_Opaque(), _Opaque()]})
        with pytest.raises(ValueError, match="cannot convert"):
            LogicalPandasDataFrame().python_to_storage(df)


class TestLogicalPandasDataFrameRoundTrip:
    def _rt(self, df: pd.DataFrame) -> pd.DataFrame:
        from orcapod.extension_types.pandas_type import LogicalPandasDataFrame
        lt = LogicalPandasDataFrame()
        return lt.storage_to_python(lt.python_to_storage(df))

    def test_default_range_index(self):
        df = pd.DataFrame({"x": [1.0, 2.0, 3.0], "y": [4, 5, 6]})
        pd.testing.assert_frame_equal(self._rt(df), df)

    def test_named_index(self):
        df = pd.DataFrame({"x": [10, 20]}, index=pd.Index([100, 200], name="id"))
        pd.testing.assert_frame_equal(self._rt(df), df)

    def test_multiindex(self):
        idx = pd.MultiIndex.from_tuples([("a", 1), ("b", 2)], names=["letter", "num"])
        df = pd.DataFrame({"val": [0.5, 1.5]}, index=idx)
        pd.testing.assert_frame_equal(self._rt(df), df)

    def test_float64_column(self):
        df = pd.DataFrame({"f": np.array([1.1, 2.2, 3.3], dtype=np.float64)})
        pd.testing.assert_frame_equal(self._rt(df), df)

    def test_int32_column(self):
        df = pd.DataFrame({"i": np.array([1, 2, 3], dtype=np.int32)})
        pd.testing.assert_frame_equal(self._rt(df), df)

    def test_uint8_column(self):
        df = pd.DataFrame({"u": np.array([0, 128, 255], dtype=np.uint8)})
        pd.testing.assert_frame_equal(self._rt(df), df)

    def test_string_object_column(self):
        df = pd.DataFrame({"s": ["hello", "world", None]})
        pd.testing.assert_frame_equal(self._rt(df), df)

    def test_nullable_int_column(self):
        df = pd.DataFrame({"n": pd.array([1, 2, None], dtype=pd.Int64Dtype())})
        pd.testing.assert_frame_equal(self._rt(df), df)

    def test_boolean_column(self):
        df = pd.DataFrame({"b": [True, False, True]})
        pd.testing.assert_frame_equal(self._rt(df), df)

    def test_datetime_column(self):
        df = pd.DataFrame({"t": pd.to_datetime(["2024-01-01", "2024-06-01"])})
        pd.testing.assert_frame_equal(self._rt(df), df)

    def test_categorical_column(self):
        df = pd.DataFrame({"c": pd.Categorical(["a", "b", "a"])})
        pd.testing.assert_frame_equal(self._rt(df), df)

    def test_empty_dataframe(self):
        df = pd.DataFrame({"x": pd.Series([], dtype=float)})
        pd.testing.assert_frame_equal(self._rt(df), df)
```

- [ ] **Step 2.2: Run to verify all pass**

```bash
uv run pytest tests/test_extension_types/test_pandas_type.py::TestLogicalPandasDataFrameStorage tests/test_extension_types/test_pandas_type.py::TestLogicalPandasDataFrameRoundTrip -v
```

Expected: all tests PASS (implementation was already complete in Task 1)

- [ ] **Step 2.3: Commit**

```bash
git add tests/test_extension_types/test_pandas_type.py
git commit -m "test(extension_types): add round-trip tests for LogicalPandasDataFrame"
```

---

## Task 3: `LogicalPandasSeries` — protocol + round-trip tests

**Files:**
- Modify: `tests/test_extension_types/test_pandas_type.py` (add classes)
- (Implementation already complete in Task 1's class shell)

- [ ] **Step 3.1: Add `LogicalPandasSeries` tests**

Append to `tests/test_extension_types/test_pandas_type.py`:

```python
class TestLogicalPandasSeriesProtocol:
    def test_isinstance_logical_type(self):
        from orcapod.extension_types.pandas_type import LogicalPandasSeries
        assert isinstance(LogicalPandasSeries(), LogicalTypeProtocol)

    def test_logical_type_name(self):
        from orcapod.extension_types.pandas_type import LogicalPandasSeries
        assert LogicalPandasSeries().logical_type_name == "pandas.series"

    def test_python_type(self):
        from orcapod.extension_types.pandas_type import LogicalPandasSeries
        assert LogicalPandasSeries().python_type is pd.Series

    def test_arrow_ext_name(self):
        from orcapod.extension_types.pandas_type import LogicalPandasSeries
        assert LogicalPandasSeries().get_arrow_extension_type().extension_name == "pandas.series"

    def test_arrow_ext_storage_type(self):
        from orcapod.extension_types.pandas_type import LogicalPandasSeries
        assert LogicalPandasSeries().get_arrow_extension_type().storage_type == pa.large_binary()

    def test_arrow_ext_is_cached(self):
        from orcapod.extension_types.pandas_type import LogicalPandasSeries
        lt = LogicalPandasSeries()
        assert lt.get_arrow_extension_type() is lt.get_arrow_extension_type()

    def test_polars_ext_is_cached(self):
        from orcapod.extension_types.pandas_type import LogicalPandasSeries
        lt = LogicalPandasSeries()
        assert lt.get_polars_extension_type() is lt.get_polars_extension_type()


class TestLogicalPandasSeriesRoundTrip:
    def _rt(self, s: pd.Series) -> pd.Series:
        from orcapod.extension_types.pandas_type import LogicalPandasSeries
        lt = LogicalPandasSeries()
        return lt.storage_to_python(lt.python_to_storage(s))

    def test_unnamed_series_name_is_none(self):
        s = pd.Series([1.0, 2.0, 3.0])
        result = self._rt(s)
        pd.testing.assert_series_equal(result, s)
        assert result.name is None

    def test_named_series(self):
        s = pd.Series([10, 20, 30], name="count")
        result = self._rt(s)
        pd.testing.assert_series_equal(result, s)
        assert result.name == "count"

    def test_named_index_preserved(self):
        s = pd.Series([1.0, 2.0], index=pd.Index([10, 20], name="row_id"), name="val")
        pd.testing.assert_series_equal(self._rt(s), s)

    def test_float64_values(self):
        s = pd.Series(np.array([1.1, 2.2], dtype=np.float64), name="f")
        pd.testing.assert_series_equal(self._rt(s), s)

    def test_int32_values(self):
        s = pd.Series(np.array([1, 2, 3], dtype=np.int32), name="i")
        pd.testing.assert_series_equal(self._rt(s), s)

    def test_string_values(self):
        s = pd.Series(["a", "b", None], name="words")
        pd.testing.assert_series_equal(self._rt(s), s)

    def test_bool_values(self):
        s = pd.Series([True, False, True], name="flags")
        pd.testing.assert_series_equal(self._rt(s), s)

    def test_datetime_values(self):
        s = pd.Series(pd.to_datetime(["2024-01-01", "2024-06-01"]), name="ts")
        pd.testing.assert_series_equal(self._rt(s), s)

    def test_empty_series(self):
        s = pd.Series([], dtype=float, name="empty")
        pd.testing.assert_series_equal(self._rt(s), s)
```

- [ ] **Step 3.2: Run all pandas type tests**

```bash
uv run pytest tests/test_extension_types/test_pandas_type.py -v
```

Expected: all tests PASS

- [ ] **Step 3.3: Commit**

```bash
git add tests/test_extension_types/test_pandas_type.py
git commit -m "test(extension_types): add protocol and round-trip tests for LogicalPandasSeries"
```

---

## Task 4: Hashing handlers

**Files:**
- Modify: `src/orcapod/hashing/semantic_hashing/builtin_handlers.py`
- Create: `tests/test_hashing/test_pandas_handlers.py`

- [ ] **Step 4.1: Write failing handler tests**

Create `tests/test_hashing/test_pandas_handlers.py`:

```python
"""Tests for PandasDataFrameHandler and PandasSeriesHandler."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from orcapod.types import ContentHash


class TestPandasDataFrameHandler:
    def test_returns_content_hash(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasDataFrameHandler
        df = pd.DataFrame({"a": [1.0, 2.0], "b": [3, 4]})
        result = PandasDataFrameHandler().handle(df, hasher=None)
        assert isinstance(result, ContentHash)
        assert result.method == "sha256"

    def test_same_content_same_hash(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasDataFrameHandler
        handler = PandasDataFrameHandler()
        df = pd.DataFrame({"x": [1.0, 2.0, 3.0]})
        assert handler.handle(df, None) == handler.handle(df.copy(), None)

    def test_different_data_different_hash(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasDataFrameHandler
        handler = PandasDataFrameHandler()
        df1 = pd.DataFrame({"x": [1.0, 2.0]})
        df2 = pd.DataFrame({"x": [3.0, 4.0]})
        assert handler.handle(df1, None).digest != handler.handle(df2, None).digest

    def test_different_index_different_hash(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasDataFrameHandler
        handler = PandasDataFrameHandler()
        df1 = pd.DataFrame({"x": [1, 2]}, index=[0, 1])
        df2 = pd.DataFrame({"x": [1, 2]}, index=[10, 20])
        assert handler.handle(df1, None).digest != handler.handle(df2, None).digest

    def test_different_column_names_different_hash(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasDataFrameHandler
        handler = PandasDataFrameHandler()
        df1 = pd.DataFrame({"a": [1, 2]})
        df2 = pd.DataFrame({"b": [1, 2]})
        assert handler.handle(df1, None).digest != handler.handle(df2, None).digest

    def test_wrong_type_raises_type_error(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasDataFrameHandler
        with pytest.raises(TypeError, match="PandasDataFrameHandler"):
            PandasDataFrameHandler().handle("not a dataframe", hasher=None)

    def test_non_arrow_column_raises_value_error(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasDataFrameHandler

        class _Opaque:
            pass

        df = pd.DataFrame({"bad": [_Opaque(), _Opaque()]})
        with pytest.raises(ValueError, match="cannot convert"):
            PandasDataFrameHandler().handle(df, hasher=None)


class TestPandasSeriesHandler:
    def test_returns_content_hash(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasSeriesHandler
        s = pd.Series([1.0, 2.0, 3.0], name="x")
        result = PandasSeriesHandler().handle(s, hasher=None)
        assert isinstance(result, ContentHash)
        assert result.method == "sha256"

    def test_same_content_same_hash(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasSeriesHandler
        handler = PandasSeriesHandler()
        s = pd.Series([1.0, 2.0, 3.0])
        assert handler.handle(s, None) == handler.handle(s.copy(), None)

    def test_different_data_different_hash(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasSeriesHandler
        handler = PandasSeriesHandler()
        s1 = pd.Series([1.0, 2.0])
        s2 = pd.Series([3.0, 4.0])
        assert handler.handle(s1, None).digest != handler.handle(s2, None).digest

    def test_unnamed_and_named_different_hash(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasSeriesHandler
        handler = PandasSeriesHandler()
        s1 = pd.Series([1.0, 2.0])            # name=None → stored as sentinel
        s2 = pd.Series([1.0, 2.0], name="x")  # name="x"
        assert handler.handle(s1, None).digest != handler.handle(s2, None).digest

    def test_different_index_different_hash(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasSeriesHandler
        handler = PandasSeriesHandler()
        s1 = pd.Series([1, 2], index=[0, 1], name="v")
        s2 = pd.Series([1, 2], index=[10, 20], name="v")
        assert handler.handle(s1, None).digest != handler.handle(s2, None).digest

    def test_wrong_type_raises_type_error(self):
        from orcapod.hashing.semantic_hashing.builtin_handlers import PandasSeriesHandler
        with pytest.raises(TypeError, match="PandasSeriesHandler"):
            PandasSeriesHandler().handle(42, hasher=None)
```

- [ ] **Step 4.2: Run to verify failure**

```bash
uv run pytest tests/test_hashing/test_pandas_handlers.py -v
```

Expected: `ImportError` — `PandasDataFrameHandler` / `PandasSeriesHandler` do not exist yet

- [ ] **Step 4.3: Add handlers to `builtin_handlers.py`**

Append the following two classes to the end of `src/orcapod/hashing/semantic_hashing/builtin_handlers.py` (after the existing `NumpyArrayHandler` class):

```python
class PandasDataFrameHandler:
    """Hasher for ``pd.DataFrame`` — content hash via SHA-256 of Arrow IPC stream bytes.

    Serialises the DataFrame to Arrow IPC stream format (with ``preserve_index=True``)
    and returns a ``ContentHash`` produced by SHA-256 of those bytes. The serialisation
    path is identical to ``LogicalPandasDataFrame.python_to_storage``, so the hash
    input and storage representation are always consistent.

    Returning ``ContentHash`` directly (rather than the raw bytes) avoids the
    hex-expansion and JSON-serialisation overhead that the semantic hasher would apply
    to ``bytes`` returns — important for large DataFrames.
    """

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> ContentHash:
        """Return a SHA-256 ``ContentHash`` of the IPC bytes for ``obj``.

        Args:
            obj: A ``pd.DataFrame`` instance.
            hasher: Ignored. Present for protocol conformance.

        Returns:
            A ``ContentHash`` with ``method="sha256"`` and digest equal to the
            SHA-256 of the DataFrame's Arrow IPC stream bytes.

        Raises:
            TypeError: If ``obj`` is not a ``pd.DataFrame``.
            ValueError: If any column cannot be converted to an Arrow type.
        """
        import hashlib
        import io
        import pandas as pd
        import pyarrow as pa
        import pyarrow.ipc

        if not isinstance(obj, pd.DataFrame):
            raise TypeError(
                f"PandasDataFrameHandler: expected pd.DataFrame, got {type(obj)!r}"
            )
        try:
            table = pa.Table.from_pandas(obj, preserve_index=True)
        except pa.lib.ArrowInvalid as exc:
            raise ValueError(
                f"PandasDataFrameHandler: cannot convert DataFrame to Arrow. "
                f"Check for columns with mixed or non-Arrow-serialisable types. "
                f"Original error: {exc}"
            ) from exc
        buf = io.BytesIO()
        with pa.ipc.new_stream(buf, table.schema) as writer:
            writer.write_table(table)
        return ContentHash(method="sha256", digest=hashlib.sha256(buf.getvalue()).digest())


class PandasSeriesHandler:
    """Hasher for ``pd.Series`` — content hash via SHA-256 of Arrow IPC stream bytes.

    Wraps the Series as a single-column DataFrame (using the sentinel column name
    ``"__orcapod:unnamed__"`` for an unnamed Series) and applies the same IPC
    serialisation path as ``PandasDataFrameHandler``. The hash input is always
    consistent with what ``LogicalPandasSeries.python_to_storage`` stores.
    """

    _UNNAMED_SENTINEL = "__orcapod:unnamed__"

    def handle(self, obj: Any, hasher: "SemanticHasherProtocol") -> ContentHash:
        """Return a SHA-256 ``ContentHash`` of the IPC bytes for ``obj``.

        Args:
            obj: A ``pd.Series`` instance.
            hasher: Ignored. Present for protocol conformance.

        Returns:
            A ``ContentHash`` with ``method="sha256"`` and digest equal to the
            SHA-256 of the Series' Arrow IPC stream bytes.

        Raises:
            TypeError: If ``obj`` is not a ``pd.Series``.
            ValueError: If the Series values cannot be converted to an Arrow type.
        """
        import hashlib
        import io
        import pandas as pd
        import pyarrow as pa
        import pyarrow.ipc

        if not isinstance(obj, pd.Series):
            raise TypeError(
                f"PandasSeriesHandler: expected pd.Series, got {type(obj)!r}"
            )
        col_name = obj.name if obj.name is not None else self._UNNAMED_SENTINEL
        df = obj.to_frame(name=col_name)
        try:
            table = pa.Table.from_pandas(df, preserve_index=True)
        except pa.lib.ArrowInvalid as exc:
            raise ValueError(
                f"PandasSeriesHandler: cannot convert Series to Arrow. "
                f"Check for non-Arrow-serialisable values. "
                f"Original error: {exc}"
            ) from exc
        buf = io.BytesIO()
        with pa.ipc.new_stream(buf, table.schema) as writer:
            writer.write_table(table)
        return ContentHash(method="sha256", digest=hashlib.sha256(buf.getvalue()).digest())
```

- [ ] **Step 4.4: Run handler tests**

```bash
uv run pytest tests/test_hashing/test_pandas_handlers.py -v
```

Expected: all tests PASS

- [ ] **Step 4.5: Commit**

```bash
git add src/orcapod/hashing/semantic_hashing/builtin_handlers.py tests/test_hashing/test_pandas_handlers.py
git commit -m "feat(hashing): add PandasDataFrameHandler and PandasSeriesHandler"
```

---

## Task 5: Registration in `v0.1.json` + integration tests

**Files:**
- Modify: `src/orcapod/contexts/data/v0.1.json`
- Modify: `tests/test_extension_types/test_roundtrips.py`

- [ ] **Step 5.1: Write failing integration tests**

Append to `tests/test_extension_types/test_roundtrips.py`:

```python
def test_builtin_dataframe_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """pd.DataFrame round-trips through storage with extension name ``pandas.dataframe``."""
    import pandas as pd
    df = pd.DataFrame(
        {"x": [1.0, 2.0, 3.0], "label": ["a", "b", "c"]},
        index=pd.Index([10, 20, 30], name="row_id"),
    )
    result, read_converter = _write_and_read(
        {"frame": pd.DataFrame},
        [{"frame": df}],
        storage_backend,
        tmp_path,
    )

    field = result.schema.field("frame")
    assert hasattr(field.type, "extension_name"), (
        f"Expected extension type on field 'frame', got plain type {field.type!r}"
    )
    assert field.type.extension_name == "pandas.dataframe"

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    recovered = rows[0]["frame"]
    assert isinstance(recovered, pd.DataFrame)
    pd.testing.assert_frame_equal(recovered, df)


def test_builtin_series_round_trip(storage_backend: _StorageBackend, tmp_path: Path) -> None:
    """pd.Series round-trips through storage with extension name ``pandas.series``."""
    import pandas as pd
    s = pd.Series([10.0, 20.0, 30.0], name="metric", index=pd.Index([1, 2, 3], name="id"))
    result, read_converter = _write_and_read(
        {"series": pd.Series},
        [{"series": s}],
        storage_backend,
        tmp_path,
    )

    field = result.schema.field("series")
    assert hasattr(field.type, "extension_name"), (
        f"Expected extension type on field 'series', got plain type {field.type!r}"
    )
    assert field.type.extension_name == "pandas.series"

    rows = read_converter.arrow_table_to_python_dicts(result)
    assert len(rows) == 1
    recovered = rows[0]["series"]
    assert isinstance(recovered, pd.Series)
    pd.testing.assert_series_equal(recovered, s)
```

- [ ] **Step 5.2: Run to verify failure**

```bash
uv run pytest tests/test_extension_types/test_roundtrips.py::test_builtin_dataframe_round_trip tests/test_extension_types/test_roundtrips.py::test_builtin_series_round_trip -v
```

Expected: FAIL with `ValueError: Unsupported Python type` (types not yet registered in context)

- [ ] **Step 5.3: Register both logical types in `v0.1.json`**

In `src/orcapod/contexts/data/v0.1.json`, find the `logical_types` array and append after the numpy entry:

```json
{
    "_class": "orcapod.extension_types.numpy_type.LogicalNumpyArray",
    "_config": {}
},
{
    "_class": "orcapod.extension_types.pandas_type.LogicalPandasDataFrame",
    "_config": {}
},
{
    "_class": "orcapod.extension_types.pandas_type.LogicalPandasSeries",
    "_config": {}
}
```

- [ ] **Step 5.4: Register both handlers in `v0.1.json`**

In `src/orcapod/contexts/data/v0.1.json`, find the `python_type_handler_registry` handlers array and append after the numpy handler entry:

```json
[{"_type": "numpy.ndarray"}, {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.NumpyArrayHandler", "_config": {}}],
[{"_type": "pandas.core.frame.DataFrame"}, {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.PandasDataFrameHandler", "_config": {}}],
[{"_type": "pandas.core.series.Series"}, {"_class": "orcapod.hashing.semantic_hashing.builtin_handlers.PandasSeriesHandler", "_config": {}}]
```

- [ ] **Step 5.5: Update the changelog in `v0.1.json`**

Append to the `changelog` array in `v0.1.json`:

```json
"Added pandas.DataFrame and pandas.Series as native value types via LogicalPandasDataFrame and LogicalPandasSeries (Arrow IPC / large_binary), with index preservation and PandasDataFrameHandler / PandasSeriesHandler for content hashing (PLT-1869)"
```

- [ ] **Step 5.6: Run integration tests**

```bash
uv run pytest tests/test_extension_types/test_roundtrips.py::test_builtin_dataframe_round_trip tests/test_extension_types/test_roundtrips.py::test_builtin_series_round_trip -v
```

Expected: all variants (parquet + delta backends) PASS

- [ ] **Step 5.7: Run the full test suite**

```bash
uv run pytest tests/ -x -q
```

Expected: all tests pass, no regressions

- [ ] **Step 5.8: Commit**

```bash
git add src/orcapod/contexts/data/v0.1.json tests/test_extension_types/test_roundtrips.py
git commit -m "feat(contexts): register LogicalPandasDataFrame/Series and pandas hashing handlers in v0.1 context (PLT-1869)"
```

---

## Final Step: Push and open PR

- [ ] **Push the branch**

```bash
git push -u origin eywalker/plt-1869-orcapod-support-pandasdataframe-as-a-native-value-type
```

- [ ] **Open PR against `main`**

```bash
gh pr create \
  --title "feat(extension_types): add pandas.DataFrame and pandas.Series as native value types" \
  --body "$(cat <<'EOF'
## Summary

- Adds `LogicalPandasDataFrame` and `LogicalPandasSeries` to `extension_types/pandas_type.py`, mapping `pd.DataFrame` / `pd.Series` to Arrow `large_binary` via Arrow IPC stream bytes with `preserve_index=True`
- Adds `PandasDataFrameHandler` and `PandasSeriesHandler` in `builtin_handlers.py` for SHA-256 content hashing
- Registers both types and handlers in `contexts/data/v0.1.json`
- Full test coverage: protocol conformance, round-trips (12 DataFrame cases, 8 Series cases), hashing determinism, error paths, end-to-end integration over Parquet and Delta backends

Closes PLT-1869

## Test plan
- [ ] `uv run pytest tests/test_extension_types/test_pandas_type.py -v` — all pass
- [ ] `uv run pytest tests/test_hashing/test_pandas_handlers.py -v` — all pass
- [ ] `uv run pytest tests/test_extension_types/test_roundtrips.py -v` — all pass (parquet + delta)
- [ ] `uv run pytest tests/ -x -q` — no regressions

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
  )"
```
