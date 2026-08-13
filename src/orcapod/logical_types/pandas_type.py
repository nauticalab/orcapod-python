"""``pandas.DataFrame`` and ``pandas.Series`` logical types for orcapod.

``LogicalPandasDataFrame`` maps ``pd.DataFrame`` <-> Arrow ``large_binary`` using
Arrow IPC stream format as the storage envelope, with ``preserve_index=True`` so
that all index kinds (RangeIndex, named index, MultiIndex) round-trip losslessly.

``LogicalPandasSeries`` maps ``pd.Series`` <-> Arrow ``large_binary`` by wrapping
the Series as a single-column DataFrame before applying the same IPC path. The
Series name and index are both preserved. An unnamed Series (``name=None``) uses
the sentinel column name ``"__pandas_series_unnamed__"`` so the series name survives
the DataFrame round-trip.
"""

from __future__ import annotations

import io
from typing import TYPE_CHECKING, Any

import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.ipc

from orcapod.logical_types.base_logical_type import BaseLogicalType
from orcapod.logical_types.registry import make_arrow_extension_type, make_polars_extension_type

if TYPE_CHECKING:
    from orcapod.logical_types.protocols import TypeConverterProtocol


_SERIES_UNNAMED_SENTINEL = "__pandas_series_unnamed__"


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
    ``"__pandas_series_unnamed__"`` to distinguish it from a Series named with
    an empty string.

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
                the internal sentinel column name ``"__pandas_series_unnamed__"``.
                Passing a Series whose ``name`` is exactly that sentinel raises
                ``ValueError`` to prevent silent round-trip corruption.
            converter: Ignored. Present for protocol conformance.

        Returns:
            Raw bytes in Arrow IPC stream format.

        Raises:
            ValueError: If ``value.name`` equals the reserved sentinel, or if
                the Series values cannot be converted to an Arrow type.
        """
        if value.name == _SERIES_UNNAMED_SENTINEL:
            raise ValueError(
                f"LogicalPandasSeries: Series name {_SERIES_UNNAMED_SENTINEL!r} is "
                "reserved by orcapod for unnamed Series storage. "
                "Rename the Series before storing it."
            )
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
