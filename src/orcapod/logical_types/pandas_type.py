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
import pyarrow as pa
import pyarrow.ipc

from orcapod.hashing.pandas_value_hash import (
    PANDAS_DATAFRAME_OPTIONS,
    PANDAS_SERIES_OPTIONS,
    PANDAS_UNNAMED_SERIES_COLUMN,
    pandas_attrs_as_stored,
    pandas_frame_to_arrow,
    require_restorable,
)
from orcapod.logical_types.ignore_list import IgnoreListLogicalType

if TYPE_CHECKING:
    from orcapod.logical_types.protocols import TypeConverterProtocol


_SERIES_UNNAMED_SENTINEL = PANDAS_UNNAMED_SERIES_COLUMN


_LAYOUT_VERSION = 1
"""Version of the stored layout and hashing of the types in this module.

The extension metadata is ``{"ignore":[...],"version":1}``: the version, and the property
names the data context configures the value hash to ignore (none by default).

Before v0.3 these types were stored under the names ``pandas.dataframe`` and
``pandas.series`` (with empty metadata). Those names are not registered any more, so a
column stored under them is refused on read, never read as the current types.
"""


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


class LogicalPandasDataFrame(IgnoreListLogicalType):
    """Logical type for ``pd.DataFrame``.

    Stores DataFrames as Arrow ``large_binary`` using Arrow IPC stream format.
    ``preserve_index=True`` ensures that all index kinds — default ``RangeIndex``,
    named indices, and ``MultiIndex`` — are round-tripped losslessly.

    Any DataFrame whose columns are Arrow-serialisable (numeric, string/object,
    nullable integer, boolean, datetime, categorical) is supported. Columns with
    truly mixed Python types that Arrow cannot infer raise ``ValueError``.

    The extension name ``"orcapod.pandas.dataframe"`` names the library, under Orcapod's own prefix.

    Example:
        >>> lt = LogicalPandasDataFrame()
        >>> df = pd.DataFrame({"x": [1.0, 2.0]}, index=pd.Index([10, 20], name="id"))
        >>> recovered = lt.storage_to_python(lt.python_to_storage(df))
        >>> df.equals(recovered)
        True
    """

    EXTENSION_NAME = "orcapod.pandas.dataframe"
    STORAGE_TYPE = pa.large_binary()
    VERSION = _LAYOUT_VERSION
    OPTIONS = PANDAS_DATAFRAME_OPTIONS
    python_type: type = pd.DataFrame

    def python_to_storage(
        self, value: Any, converter: TypeConverterProtocol | None = None
    ) -> bytes:
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
            ValueError: If the frame cannot be converted to Arrow, storage would not
                restore it (labels and names that collide once written as text,
                MultiIndex levels written in another order, a column, index level or
                column-axis level that cannot be read back; see ``require_restorable``),
                or ``attrs`` cannot be written as JSON. The message names the column or
                level.
        """
        pandas_attrs_as_stored(value.attrs)  # refuses attrs that are not JSON
        table = pandas_frame_to_arrow(value)
        require_restorable(table, value)
        return _table_to_ipc_bytes(table)

    def storage_to_python(
        self, storage_value: Any, converter: TypeConverterProtocol | None = None
    ) -> pd.DataFrame:
        """Reconstruct a ``pd.DataFrame`` from Arrow IPC stream bytes.

        Args:
            storage_value: Raw bytes as stored in Arrow ``large_binary``.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A ``pd.DataFrame`` with columns, dtypes, and index as originally stored.
        """
        return _ipc_bytes_to_table(storage_value).to_pandas()


class LogicalPandasSeries(IgnoreListLogicalType):
    """Logical type for ``pd.Series``.

    Stores Series as Arrow ``large_binary`` using Arrow IPC stream format.
    The Series is wrapped as a single-column DataFrame before serialisation,
    which preserves the Series name and index losslessly.

    An unnamed Series (``name=None``) uses the internal sentinel column name
    ``"__pandas_series_unnamed__"`` to distinguish it from a Series named with
    an empty string.

    The extension name ``"orcapod.pandas.series"`` names the library, under Orcapod's own prefix.

    Example:
        >>> lt = LogicalPandasSeries()
        >>> s = pd.Series([1.0, 2.0, 3.0], name="metric")
        >>> recovered = lt.storage_to_python(lt.python_to_storage(s))
        >>> s.equals(recovered)
        True
    """

    EXTENSION_NAME = "orcapod.pandas.series"
    STORAGE_TYPE = pa.large_binary()
    VERSION = _LAYOUT_VERSION
    OPTIONS = PANDAS_SERIES_OPTIONS
    python_type: type = pd.Series

    def python_to_storage(
        self, value: Any, converter: TypeConverterProtocol | None = None
    ) -> bytes:
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
            ValueError: If ``value.name`` equals the reserved sentinel, the Series
                cannot be converted to Arrow, storage would not restore it (see
                ``require_restorable``: the name and the index names collide once written
                as text, MultiIndex levels written in another order, values or an index
                level that cannot be read back), or ``attrs`` cannot be written as JSON.
        """
        if value.name == _SERIES_UNNAMED_SENTINEL:
            raise ValueError(
                f"LogicalPandasSeries: Series name {_SERIES_UNNAMED_SENTINEL!r} is "
                "reserved by orcapod for unnamed Series storage. "
                "Rename the Series before storing it."
            )
        col_name = value.name if value.name is not None else _SERIES_UNNAMED_SENTINEL
        pandas_attrs_as_stored(value.attrs)  # refuses attrs that are not JSON
        df = value.to_frame(name=col_name)
        table = pandas_frame_to_arrow(df)
        require_restorable(table, df)
        return _table_to_ipc_bytes(table)

    def storage_to_python(
        self, storage_value: Any, converter: TypeConverterProtocol | None = None
    ) -> pd.Series:
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
