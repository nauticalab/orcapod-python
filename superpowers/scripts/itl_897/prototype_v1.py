"""Prototype of the proposed pandas hash, for the ITL-897 investigation scripts only.

Historical record (November 12, 2026): this script belongs to the investigation of
November 11, 2026 and was run against ``main`` as it was then. It is kept as the record of
that investigation and is not maintained: on this branch the library has the final hash
(``orcapod/hashing/pandas_value_hash.py``), which
supersedes this prototype. The prototype puts the algorithm label in its descriptor; the
final hash does not (its ``kind`` is the logical type's name, and the label never enters
the computation).

``install()`` patches ``PandasDataFrameHandler.handle`` / ``PandasSeriesHandler.handle`` in
the running process (never in the library) so that::

    pandas_dataframe_v1 / pandas_series_v1 =
        sha256(descriptor + b"\\x00" + values_digest)

where ``descriptor`` is canonical JSON ``{"fields": [<Arrow field name>, ...], "kind": ...}``
listing the fields of ``pa.Table.from_pandas(value, preserve_index=True)`` in order (data
columns, then index levels), and ``values_digest`` is today's Arrow hasher digest of that same
table. This is the minimal variant: it adds order only. It is not the final grammar.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

import pyarrow as pa


def _labelled(label: str, table: pa.Table, values: bytes) -> Any:
    from orcapod.types import ContentHash

    descriptor = json.dumps(
        {"kind": label, "fields": table.schema.names},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()
    return ContentHash(label, hashlib.sha256(descriptor + b"\x00" + values).digest())


def install() -> None:
    """Patch the pandas handlers in this process with the prototype hash."""
    from orcapod.hashing.semantic_hashing import builtin_handlers as bh

    def df_handle(self: Any, obj: Any, hasher: Any) -> Any:
        table = pa.Table.from_pandas(obj, preserve_index=True)
        values = self._get_arrow_hasher().hash_table(table).digest
        return _labelled("pandas_dataframe_v1", table, values)

    def series_handle(self: Any, obj: Any, hasher: Any) -> Any:
        name = obj.name if obj.name is not None else self._UNNAMED_SENTINEL
        table = pa.Table.from_pandas(obj.to_frame(name=name), preserve_index=True)
        values = self._get_arrow_hasher().hash_table(table).digest
        return _labelled("pandas_series_v1", table, values)

    bh.PandasDataFrameHandler.handle = df_handle  # type: ignore[method-assign]
    bh.PandasSeriesHandler.handle = series_handle  # type: ignore[method-assign]


def install_arrow_table(*, separated: bool) -> None:
    """Patch ``ArrowTableHandler`` with an order-dependent ``arrow_table_v1`` prototype.

    The descriptor lists the table's top-level field names in order; the values digest is
    today's Arrow hasher digest. With ``separated=True``, ``Datagram`` and producer-less
    ``ArrowTableStream`` identities no longer route their table through the type-dispatched
    handler: they return ``arrow_hasher.hash_table(table)`` (a terminal ``ContentHash``),
    which is what the handler returns today, so stream and datagram hashes stay put.
    """
    from orcapod.core.datagrams.datagram import Datagram
    from orcapod.core.streams.arrow_table_stream import ArrowTableStream
    from orcapod.hashing.semantic_hashing import builtin_handlers as bh

    def table_handle(self: Any, obj: Any, hasher: Any) -> Any:
        if isinstance(obj, pa.RecordBatch):
            obj = pa.Table.from_batches([obj])
        values = self._get_arrow_hasher().hash_table(obj).digest
        return _labelled("arrow_table_v1", obj, values)

    bh.ArrowTableHandler.handle = table_handle  # type: ignore[method-assign]
    if not separated:
        return

    def datagram_identity(self: Any) -> Any:
        return self.data_context.arrow_hasher.hash_table(self._ensure_data_table())

    original_stream_identity = ArrowTableStream.identity_structure

    def stream_identity(self: Any) -> Any:
        structure = original_stream_identity(self)
        if self._producer is not None:
            return structure
        name, table, tag_columns = structure
        return (name, self.data_context.arrow_hasher.hash_table(table), tag_columns)

    Datagram.identity_structure = datagram_identity  # type: ignore[method-assign]
    ArrowTableStream.identity_structure = stream_identity  # type: ignore[method-assign]


# ---------------------------------------------------------------------------
# Full descriptor: one hash move that settles every mismatch in claim (a)
# ---------------------------------------------------------------------------

_MASKED_DTYPES = {
    "Int8",
    "Int16",
    "Int32",
    "Int64",
    "UInt8",
    "UInt16",
    "UInt32",
    "UInt64",
    "Float32",
    "Float64",
    "boolean",
}


def pandas_label(label: Any) -> Any:
    """Typed JSON for a pandas label, so ``0`` and ``"0"`` differ.

    Covers every label kind that hashes and stores on v0.2.0: str, None, bool, int, float,
    tuples (MultiIndex columns), Timestamp / datetime, date, Timedelta / timedelta, Period.
    """
    import datetime as dt

    import numpy as np
    import pandas as pd

    if label is None or isinstance(label, str):
        return label
    if isinstance(label, (bool, np.bool_)):
        return {"bool": bool(label)}
    if isinstance(label, (int, np.integer)):
        return {"int": int(label)}
    if isinstance(label, (float, np.floating)):
        return {"float": float(label).hex()}
    if isinstance(label, tuple):
        return {"tuple": [pandas_label(part) for part in label]}
    if isinstance(label, (pd.Timestamp, dt.datetime, np.datetime64)):
        ts = pd.Timestamp(label)
        if not isinstance(ts, pd.Timestamp):
            return {"nat": "datetime"}
        return {"timestamp": [ts.value, ts.unit, None if ts.tz is None else str(ts.tz)]}
    if isinstance(label, dt.date):
        return {"date": label.isoformat()}
    if isinstance(label, (pd.Timedelta, dt.timedelta, np.timedelta64)):
        td = pd.Timedelta(label)
        if not isinstance(td, pd.Timedelta):
            return {"nat": "timedelta"}
        return {"timedelta": [td.value, td.unit]}
    if isinstance(label, pd.Period):
        return {"period": [label.ordinal, label.freqstr]}
    raise ValueError(
        f"label {label!r} of type {type(label).__name__} is outside the grammar"
    )


def arrow_type_descriptor(arrow_type: pa.DataType) -> Any:
    """Arrow type with struct fields in order at every depth (layout-normalized to large)."""
    from orcapod.utils.arrow_utils import normalize_to_large_types

    if pa.types.is_struct(arrow_type):
        return {
            "struct": [
                [f.name, arrow_type_descriptor(f.type)]
                for f in arrow_type  # pyright: ignore
            ]
        }
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return {"list": arrow_type_descriptor(arrow_type.value_type)}
    return str(normalize_to_large_types(arrow_type))


def pandas_dtype_descriptor(dtype: Any) -> Any:
    """Orcapod-owned grammar for a pandas dtype (never ``str(dtype)`` alone)."""
    import pandas as pd

    if isinstance(dtype, pd.CategoricalDtype):
        categories = dtype.categories
        return {
            "categorical": {
                "categories": [
                    pandas_dtype_descriptor(categories.dtype),
                    [pandas_label(c) for c in categories],
                ],
                "ordered": bool(dtype.ordered),
            }
        }
    if isinstance(dtype, pd.ArrowDtype):
        return {"arrow": arrow_type_descriptor(dtype.pyarrow_dtype)}
    if isinstance(dtype, pd.StringDtype):
        return {"string": dtype.storage}
    if isinstance(dtype, pd.DatetimeTZDtype):
        return {"datetimetz": [dtype.unit, str(dtype.tz)]}
    if isinstance(dtype, pd.PeriodDtype):
        return {"period": dtype.freq.freqstr}
    if isinstance(dtype, pd.IntervalDtype):
        return {"interval": [pandas_dtype_descriptor(dtype.subtype), dtype.closed]}
    if isinstance(dtype, pd.api.extensions.ExtensionDtype):
        if dtype.name in _MASKED_DTYPES:
            return {"masked": dtype.name}
        raise ValueError(f"pandas dtype {dtype!r} is outside the grammar")
    return {"numpy": dtype.name}


def _index_descriptor(index: Any) -> list[Any]:
    return [
        [pandas_label(name), pandas_dtype_descriptor(index.get_level_values(i).dtype)]
        for i, name in enumerate(index.names)
    ]


def frame_descriptor(df: Any) -> dict[str, Any]:
    return {
        "kind": "pandas.dataframe",
        "height": len(df),
        "columns": [
            [pandas_label(label), pandas_dtype_descriptor(dtype)]
            for label, dtype in zip(df.columns, df.dtypes)
        ],
        "columns_names": [pandas_label(name) for name in df.columns.names],
        "index": _index_descriptor(df.index),
    }


def series_descriptor(series: Any) -> dict[str, Any]:
    return {
        "kind": "pandas.series",
        "height": len(series),
        "name": pandas_label(series.name),
        "dtype": pandas_dtype_descriptor(series.dtype),
        "index": _index_descriptor(series.index),
    }


def _full_hash(
    label: str, descriptor: dict[str, Any], table: pa.Table, handler: Any
) -> Any:
    from orcapod.types import ContentHash

    values = handler._get_arrow_hasher().hash_table(table).digest  # noqa: SLF001
    text = json.dumps(
        descriptor, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    )
    return ContentHash(label, hashlib.sha256(text.encode() + b"\x00" + values).digest())


def install_full_descriptor() -> None:
    """Patch the pandas handlers with the full-descriptor prototype (one hash move)."""
    from orcapod.hashing.semantic_hashing import builtin_handlers as bh

    def df_handle(self: Any, obj: Any, hasher: Any) -> Any:
        table = pa.Table.from_pandas(obj, preserve_index=True)
        return _full_hash("pandas_dataframe_v1", frame_descriptor(obj), table, self)

    def series_handle(self: Any, obj: Any, hasher: Any) -> Any:
        table = pa.Table.from_pandas(obj.to_frame(name="values"), preserve_index=True)
        return _full_hash("pandas_series_v1", series_descriptor(obj), table, self)

    bh.PandasDataFrameHandler.handle = df_handle  # type: ignore[method-assign]
    bh.PandasSeriesHandler.handle = series_handle  # type: ignore[method-assign]
