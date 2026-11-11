"""``numpy.ndarray`` logical type: the v0.3 value-based array format.

An array is stored as one row of the shared array struct
(``array_format.ARRAY_STORAGE_TYPE``): an Orcapod dtype tag, the shape, and the
element bytes in C order, little-endian and packed. The extension name is
``orcapod.ndarray``. The format and the dtype tags are specified in
``superpowers/specs/2026-11-10-array-format-v0.3.md`` and documented in
``docs/concepts/arrays.md``.

Writing canonicalises what is not part of an array's identity: memory order
and strides (stored C order), byte order (stored little-endian) and structured
offsets, padding and alignment (fields packed). Structured field order is part
of the identity, as NumPy's own dtype equality has it: fields are stored in
declared order, at every nesting depth. So a stored row is the array's
canonical value, and its hash (``NumpyArrayHandler``) is a function of the
stored row alone.

Refused at write time, with a ``ValueError`` naming the dtype (the type
converter adds the column): masked arrays, object dtypes, NumPy 2
``StringDType`` and other "extended data" arrays, ``longdouble`` and
``clongdouble`` (platform-dependent), datetimes and timedeltas without a unit,
and structured dtypes with field titles.
"""

from __future__ import annotations

import json
import re
from typing import TYPE_CHECKING, Any

import numpy as np
import numpy.typing as npt
import polars as pl
import pyarrow as pa

from orcapod.errors import UnsupportedValueError
from orcapod.logical_types.array_format import (
    ARRAY_FORMAT_METADATA,
    ARRAY_STORAGE_TYPE,
)
from orcapod.logical_types.base_logical_type import BaseLogicalType
from orcapod.logical_types.registry import (
    make_arrow_extension_type,
    make_polars_extension_type,
)

if TYPE_CHECKING:
    from orcapod.logical_types.protocols import TypeConverterProtocol

NDARRAY_EXTENSION_NAME = "orcapod.ndarray"
"""Arrow extension name (and logical type name) of the array format."""

_INT_TAGS = {1: "int8", 2: "int16", 4: "int32", 8: "int64"}
_UINT_TAGS = {1: "uint8", 2: "uint16", 4: "uint32", 8: "uint64"}
_FLOAT_TAGS = {2: "float16", 4: "float32", 8: "float64"}
_COMPLEX_TAGS = {8: "complex64", 16: "complex128"}

_TAG_TO_CODE: dict[str, str] = {
    "bool": "|b1",
    **{tag: f"<i{size}" for size, tag in _INT_TAGS.items()},
    **{tag: f"<u{size}" for size, tag in _UINT_TAGS.items()},
    **{tag: f"<f{size}" for size, tag in _FLOAT_TAGS.items()},
    **{tag: f"<c{size}" for size, tag in _COMPLEX_TAGS.items()},
}
_SIZED_TAG = re.compile(r"^(ucs4|bytes|void)\[([0-9]+)\]$")
_TIME_TAG = re.compile(r"^(datetime64|timedelta64)\[([0-9]*)([A-Za-z]+)\]$")
_SIZED_CODES = {"ucs4": "<U", "bytes": "|S", "void": "|V"}


def _refuse(dtype: object, reason: str) -> UnsupportedValueError:
    """Build the refusal for an array that cannot be stored.

    Args:
        dtype: The offending dtype, shown with ``repr``.
        reason: Why it cannot be stored and, where possible, what to do.

    Returns:
        The error to raise; the type converter prefixes the column name.
    """
    return UnsupportedValueError(
        f"numpy arrays with dtype {dtype!r} cannot be stored: {reason}"
    )


def _tag_value(dtype: np.dtype[Any]) -> str | list[Any]:
    """Return the tag of ``dtype`` as a string or, for structured dtypes, a JSON list."""
    if dtype.fields is not None:
        entries: list[Any] = []
        fields = {str(key): value for key, value in dtype.fields.items()}
        for name in dtype.names or ():
            field = fields[name]
            if len(field) > 2:
                raise _refuse(dtype, "structured fields with titles are not supported")
            sub = field[0]
            if sub.subdtype is not None:
                base, sub_shape = sub.subdtype
                entries.append([name, _tag_value(base), list(sub_shape)])
            else:
                entries.append([name, _tag_value(sub)])
        return entries
    if dtype.subdtype is not None:
        raise _refuse(dtype, "a subarray dtype is only supported as a structured field")
    kind, size = dtype.kind, dtype.itemsize
    if dtype.char in "gG":
        raise _refuse(
            dtype,
            "longdouble and clongdouble differ in width and format between platforms "
            "(convert with .astype(np.float64) or .astype(np.complex128))",
        )
    if kind == "b":
        return "bool"
    if kind == "i" and size in _INT_TAGS:
        return _INT_TAGS[size]
    if kind == "u" and size in _UINT_TAGS:
        return _UINT_TAGS[size]
    if kind == "f" and size in _FLOAT_TAGS:
        return _FLOAT_TAGS[size]
    if kind == "c" and size in _COMPLEX_TAGS:
        return _COMPLEX_TAGS[size]
    if kind in "Mm":
        unit, count = np.datetime_data(dtype)
        if unit == "generic":
            raise _refuse(
                dtype, "datetimes and timedeltas need a unit, e.g. datetime64[ns]"
            )
        name = "datetime64" if kind == "M" else "timedelta64"
        return f"{name}[{'' if count == 1 else count}{unit}]"
    if kind == "U":
        return f"ucs4[{size // 4}]"
    if kind == "S":
        return f"bytes[{size}]"
    if kind == "V":
        return f"void[{size}]"
    if kind == "O":
        raise _refuse(
            dtype,
            "object arrays are extended data and are not supported "
            "(they would need pickling)",
        )
    if kind == "T":
        raise _refuse(
            dtype,
            "StringDType arrays are extended data and are not supported "
            "(use a fixed-width string dtype such as 'U10')",
        )
    raise _refuse(dtype, "this dtype has no array-format tag")


def dtype_to_tag(dtype: np.dtype[Any]) -> str:
    """Return the Orcapod dtype tag of a NumPy dtype.

    Args:
        dtype: Any NumPy dtype, in any byte order.

    Returns:
        A scalar tag such as ``"float32"`` or ``"datetime64[ns]"``, or for a
        structured dtype a JSON list of ``[name, tag]`` / ``[name, tag, shape]``
        entries in declared field order (``separators=(",", ":")``,
        ``ensure_ascii=True``).

    Raises:
        UnsupportedValueError: If the dtype cannot be stored.
    """
    value = _tag_value(dtype)
    if isinstance(value, str):
        return value
    return json.dumps(value, separators=(",", ":"), ensure_ascii=True)


def _is_subarray_shape(shape: Any) -> bool:
    """Return whether a parsed tag value is a valid subarray shape.

    Args:
        shape: The third part of a structured tag entry.

    Returns:
        ``True`` for a list of non-negative ``int`` values (``bool`` excluded).
    """
    return isinstance(shape, list) and all(type(d) is int and d >= 0 for d in shape)


def _dtype_from_value(value: Any) -> np.dtype[Any]:
    """Return the canonical NumPy dtype of a parsed tag value.

    Args:
        value: A scalar tag string, or the parsed JSON list of a structured tag.

    Returns:
        The little-endian, packed NumPy dtype.

    Raises:
        ValueError: If the value is not a known tag.
    """
    if isinstance(value, list):
        fields: list[Any] = []
        for entry in value:
            if (
                not isinstance(entry, list)
                or len(entry) not in (2, 3)
                or not isinstance(entry[0], str)
                or (len(entry) == 3 and not _is_subarray_shape(entry[2]))
            ):
                expected = (
                    "[name, tag] or [name, tag, shape], with a string name and a "
                    "shape that is a list of non-negative integers"
                )
                raise ValueError(
                    f"malformed array dtype tag entry {entry!r}: expected {expected}"
                )
            if len(entry) == 3:
                fields.append((entry[0], _dtype_from_value(entry[1]), tuple(entry[2])))
            else:
                fields.append((entry[0], _dtype_from_value(entry[1])))
        return np.dtype(fields)
    if not isinstance(value, str):
        raise ValueError(f"malformed array dtype tag element {value!r}")
    if value in _TAG_TO_CODE:
        return np.dtype(_TAG_TO_CODE[value])
    if m := _SIZED_TAG.match(value):
        return np.dtype(f"{_SIZED_CODES[m.group(1)]}{m.group(2)}")
    if m := _TIME_TAG.match(value):
        code = "<M8" if m.group(1) == "datetime64" else "<m8"
        return np.dtype(f"{code}[{m.group(2)}{m.group(3)}]")
    raise ValueError(f"unknown array dtype tag {value!r}")


def tag_to_dtype(tag: str) -> np.dtype[Any]:
    """Return the canonical NumPy dtype of an Orcapod dtype tag.

    The canonical dtype is little-endian and, for structured tags, packed with
    fields in tag order (the declared order of the written array).

    Args:
        tag: A tag produced by ``dtype_to_tag``.

    Returns:
        The canonical NumPy dtype.

    Raises:
        ValueError: If the tag is malformed or unknown.
    """
    if tag.startswith("["):
        return _dtype_from_value(json.loads(tag))
    return _dtype_from_value(tag)


def _copy_by_field_name(dst: npt.NDArray[Any], src: npt.NDArray[Any]) -> None:
    """Copy ``src`` into ``dst`` field by field, by name, recursively.

    ``dst`` has the same fields in the same declared order as ``src`` but a
    packed, little-endian layout; copying by name keeps the pairing explicit
    whatever the source offsets are.
    """
    names = dst.dtype.names
    if names is None:
        dst[...] = src
        return
    for name in names:
        _copy_by_field_name(dst[name], src[name])


def canonical_array(value: npt.NDArray[Any]) -> tuple[str, npt.NDArray[Any]]:
    """Return the dtype tag and the canonical form of an array.

    Args:
        value: A ``numpy.ndarray`` (subclasses are treated as plain arrays).

    Returns:
        ``(tag, array)`` where ``array`` is a C-contiguous plain ``np.ndarray`` of
        ``tag_to_dtype(tag)`` holding the same values. It may share memory with
        ``value`` when ``value`` is already canonical; do not modify it.

    Raises:
        UnsupportedValueError: For masked arrays and dtypes that cannot be stored.
        TypeError: If ``value`` is not a ``numpy.ndarray``.
    """
    if isinstance(value, np.ma.MaskedArray):
        raise UnsupportedValueError(
            f"numpy masked arrays (dtype {value.dtype!r}) are extended data and "
            "cannot be stored: store the data and the mask as two arrays"
        )
    if not isinstance(value, np.ndarray):
        raise TypeError(f"expected a numpy.ndarray, got {type(value).__name__}")
    # The base implementation, so a subclass's own ``view`` cannot change the values.
    plain = np.ndarray.view(value, np.ndarray)
    tag = dtype_to_tag(plain.dtype)
    dtype = tag_to_dtype(tag)
    if dtype.names is None:
        return tag, plain.astype(dtype, order="C", copy=False)
    out = np.empty(plain.shape, dtype=dtype)
    _copy_by_field_name(out, plain)
    return tag, out


def array_bytes(array: npt.NDArray[Any]) -> memoryview:
    """Return the bytes of a C-contiguous array without copying.

    Args:
        array: A C-contiguous array, as returned by ``canonical_array``.

    Returns:
        A byte view of the array's buffer (empty when it holds no bytes).
    """
    if array.size == 0 or array.dtype.itemsize == 0:
        return memoryview(b"")
    # A uint8 view sidesteps dtypes NumPy will not export as a buffer (datetime64).
    return array.reshape(-1).view(np.uint8).data


class LogicalNumpyArray(BaseLogicalType):
    """Logical type for ``numpy.ndarray`` in the v0.3 value-based array format.

    Stores each array as ``{"dtype": tag, "shape": [...], "data": bytes}`` in
    ``ARRAY_STORAGE_TYPE`` under the extension name ``orcapod.ndarray``. A value
    reads back as a writeable, C-ordered, little-endian plain ``np.ndarray``.

    Example:
        >>> lt = LogicalNumpyArray()
        >>> arr = np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float64)
        >>> recovered = lt.storage_to_python(lt.python_to_storage(arr))
        >>> np.array_equal(recovered, arr)
        True
    """

    _arrow_ext_class = make_arrow_extension_type(
        NDARRAY_EXTENSION_NAME, ARRAY_STORAGE_TYPE, ARRAY_FORMAT_METADATA
    )
    _arrow_ext: pa.ExtensionType | None = None
    _polars_ext_class = make_polars_extension_type(
        NDARRAY_EXTENSION_NAME, ARRAY_STORAGE_TYPE, ARRAY_FORMAT_METADATA.decode()
    )
    _polars_ext: pl.BaseExtension | None = None

    logical_type_name: str = NDARRAY_EXTENSION_NAME
    python_type: type = np.ndarray

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the cached Arrow extension type ``orcapod.ndarray``.

        Returns:
            The ``pa.ExtensionType`` over ``ARRAY_STORAGE_TYPE``.
        """
        if LogicalNumpyArray._arrow_ext is None:
            LogicalNumpyArray._arrow_ext = LogicalNumpyArray._arrow_ext_class()
        return LogicalNumpyArray._arrow_ext

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the cached Polars extension type ``orcapod.ndarray``.

        Returns:
            The ``pl.BaseExtension`` registered under ``orcapod.ndarray``.
        """
        if LogicalNumpyArray._polars_ext is None:
            LogicalNumpyArray._polars_ext = LogicalNumpyArray._polars_ext_class()
        return LogicalNumpyArray._polars_ext

    def python_to_storage(
        self, value: Any, converter: TypeConverterProtocol | None = None
    ) -> dict[str, Any]:
        """Encode an array as its canonical stored row.

        Args:
            value: A ``numpy.ndarray``.
            converter: Ignored. Present for protocol conformance.

        Returns:
            ``{"dtype": tag, "shape": list_of_ints, "data": bytes}``.

        Raises:
            UnsupportedValueError: For masked arrays and dtypes that cannot be stored.
            TypeError: If ``value`` is not a ``numpy.ndarray``.
        """
        tag, array = canonical_array(value)
        return {
            "dtype": tag,
            "shape": [int(d) for d in array.shape],
            "data": array_bytes(array).tobytes(),
        }

    def storage_to_python(
        self, storage_value: Any, converter: TypeConverterProtocol | None = None
    ) -> npt.NDArray[Any]:
        """Decode a stored row into a writeable ``numpy.ndarray``.

        Args:
            storage_value: The stored ``{"dtype", "shape", "data"}`` mapping.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A C-ordered plain ``np.ndarray`` of the canonical dtype.

        Raises:
            ValueError: If a field is missing or null, or the data length does
                not match the dtype and shape.
        """
        tag = storage_value.get("dtype")
        shape = storage_value.get("shape")
        data = storage_value.get("data")
        if tag is None or shape is None or data is None:
            raise ValueError(
                "stored numpy array value has a null field "
                f"(dtype={tag!r}, shape={shape!r}, data is None: {data is None})"
            )
        dtype = tag_to_dtype(tag)
        dims = tuple(int(d) for d in shape)
        count = int(np.prod(dims, dtype=np.int64)) if dims else 1
        if len(data) != count * dtype.itemsize:
            raise ValueError(
                f"stored numpy array value has {len(data)} data bytes; dtype {tag!r} "
                f"and shape {list(dims)} need {count * dtype.itemsize}"
            )
        if dtype.itemsize == 0:
            return np.zeros(dims, dtype=dtype)
        return np.frombuffer(data, dtype=dtype, count=count).reshape(dims).copy()

    def storage_sort_key(self, storage_value: Any) -> tuple[Any, ...]:
        """Ordering key for stored values, used where operators sort values.

        Orders by the stored ``(dtype tag, shape, data)``; ``None`` sorts first.

        Args:
            storage_value: A stored row, or ``None``.

        Returns:
            A tuple that orders stored rows deterministically.
        """
        if storage_value is None:
            return (0,)
        return (
            1,
            storage_value["dtype"],
            list(storage_value["shape"]),
            bytes(storage_value["data"]),
        )
