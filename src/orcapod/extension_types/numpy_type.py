"""``numpy.ndarray`` logical type for orcapod.

``LogicalNumpyArray`` maps ``numpy.ndarray`` ↔ Arrow ``large_binary`` using
numpy's own ``.npy`` binary format as the storage envelope. This preserves
dtype (including structured/record field names and types), shape, byte order,
and the raw data buffer for all array kinds: 1-D, 2-D, N-D, and structured.

Object-dtype arrays (``dtype=object`` or structured dtypes containing object
fields) are explicitly rejected — they require pickling, which is disabled for
security. See the ``Limitations`` section in the ITL-460 design spec.
"""

from __future__ import annotations

import io
from typing import TYPE_CHECKING, Any

import numpy as np
import polars as pl
import pyarrow as pa

from orcapod.extension_types.base_logical_type import BaseLogicalType
from orcapod.extension_types.registry import make_arrow_extension_type, make_polars_extension_type

if TYPE_CHECKING:
    from orcapod.extension_types.protocols import TypeConverterProtocol


class LogicalNumpyArray(BaseLogicalType):
    """Logical type for ``numpy.ndarray``.

    Stores arrays as Arrow ``large_binary`` using numpy's ``.npy`` on-disk
    format as the storage envelope. The ``.npy`` format is self-describing:
    it encodes dtype (including structured/record field names), shape, byte
    order, and raw data — so no custom parsing is required and all array
    kinds round-trip exactly.

    The extension name ``"numpy.ndarray"`` is chosen over the ``orcapod.*``
    convention because it is a highly stable, well-known name that makes
    Arrow schemas immediately self-documenting and may enable interoperability
    with other ecosystem tools that independently serialise numpy arrays.

    Object-dtype arrays are not supported. ``python_to_storage`` raises
    ``ValueError`` immediately (before calling ``np.save``) when
    ``array.dtype.kind == "O"``. ``np.save(..., allow_pickle=False)`` acts as
    a secondary guard for structured dtypes that contain object-typed fields.

    Example:
        >>> lt = LogicalNumpyArray()
        >>> arr = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float64)
        >>> recovered = lt.storage_to_python(lt.python_to_storage(arr))
        >>> np.array_equal(recovered, arr)
        True
    """

    _arrow_ext_class = make_arrow_extension_type("numpy.ndarray", pa.large_binary())
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type("numpy.ndarray", pa.large_binary())
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = "numpy.ndarray"
    python_type: type = np.ndarray

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the cached Arrow extension type for ``numpy.ndarray``.

        Returns:
            A ``pa.ExtensionType`` instance with extension name
            ``"numpy.ndarray"`` and storage type ``pa.large_binary()``.
        """
        if LogicalNumpyArray._arrow_ext is None:
            LogicalNumpyArray._arrow_ext = LogicalNumpyArray._arrow_ext_class()
        return LogicalNumpyArray._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the cached Polars extension type for ``numpy.ndarray``.

        Returns:
            A ``pl.BaseExtension`` instance registered under
            ``"numpy.ndarray"``.
        """
        if LogicalNumpyArray._polars_ext is None:
            LogicalNumpyArray._polars_ext = LogicalNumpyArray._polars_ext_class()
        return LogicalNumpyArray._polars_ext

    def python_to_storage(self, value: Any, converter: TypeConverterProtocol | None = None) -> bytes:
        """Serialise a ``numpy.ndarray`` to its ``.npy`` byte representation.

        Args:
            value: A ``numpy.ndarray`` instance. Object-dtype arrays
                (``dtype.kind == "O"``) are rejected immediately.
            converter: Ignored. Present for protocol conformance.

        Returns:
            Raw bytes in numpy's ``.npy`` format encoding dtype, shape,
            byte order, and data.

        Raises:
            ValueError: If ``value.dtype.kind == "O"`` (object-dtype array),
                or if the array contains object-typed fields in a structured
                dtype (caught by ``allow_pickle=False`` in ``np.save``).
        """
        if value.dtype.kind == "O":
            raise ValueError(
                f"LogicalNumpyArray does not support object-dtype arrays "
                f"(dtype={value.dtype!r}). Object arrays require pickling, "
                "which is disabled for security. Serialise the array contents "
                "manually (e.g. to bytes or a File) before passing to orcapod."
            )
        buf = io.BytesIO()
        np.save(buf, value, allow_pickle=False)
        return buf.getvalue()

    def storage_to_python(self, storage_value: Any, converter: TypeConverterProtocol | None = None) -> np.ndarray:
        """Reconstruct a ``numpy.ndarray`` from its ``.npy`` byte representation.

        Args:
            storage_value: Raw bytes as stored in Arrow ``large_binary``.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A ``numpy.ndarray`` with dtype and shape as originally stored.
        """
        buf = io.BytesIO(bytes(storage_value))
        return np.load(buf, allow_pickle=False)
