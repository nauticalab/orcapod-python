"""``numpy.ndarray`` logical type: ``.npy`` storage and a strict value hash (ITL-981).

Storage and hashing are separate. An array is stored as one ``.npy`` file
(NumPy's own format, version 3.0, never pickled) in a ``large_binary`` cell under
the extension name ``orcapod.numpy.ndarray``, and reads back as it was given:
dtype including byte order, shape, element bits, C or Fortran contiguity and
structured field offsets are kept. The hash is defined by Orcapod on the value,
never on the stored bytes.

What ``.npy`` does not keep is never part of the hash: strides other than C or
Fortran contiguity (a strided view is stored C-contiguous), the subclass, the
``writeable`` flag, dtype metadata, the ``align`` flag of a structured dtype (its
offsets are kept) and padding bytes between structured fields (NumPy does not
define their content; they are written as zero, so memory the array happened
to sit on never reaches a database).

Value hash ``D`` (label ``ndarray_v1``), in two steps. With
``lp(x) = u64le(len(utf8(x))) || utf8(x)``:

    E = array_format.array_digest_v1(tag, shape, bytes)
    D = SHA-256(lp("orcapod.numpy.ndarray") || E || u8(memory_order)
                || lp(byte_order_text) || lp(struct_layout_text))

``E`` is taken on the canonical form of the array (``canonical_array``: C order,
little-endian, structured fields packed in declared order) and covers dtype,
shape and element bits. The three further fields are the properties the hash
can ignore (``array_format.NDARRAY_OPTIONS``):

- ``memory_order``: 1 when the array is Fortran-contiguous and not
  C-contiguous, else 0 (the flag ``.npy`` keeps).
- ``byte_order``: one character per scalar leaf of the dtype, depth first in
  declared field order (a subarray field counts once): ``<``, ``>``, or ``|``
  for a type without byte order. NumPy's native marker is resolved with
  ``sys.byteorder``, so the text is the same on every machine.
- ``struct_layout``: compact JSON of ``L(dtype)``: ``null`` for an
  unstructured dtype, else ``[itemsize, [offset, L(field)], ...]`` in declared
  field order (a subarray field uses its base dtype).

An ignored property's field is written with its canonical value: 0, the text
with every ``>`` replaced by ``<``, the layout of the packed dtype. The ignore
list is saved in the extension metadata (``ignore_list``), not in the preimage.

Refused at write time, with ``UnsupportedValueError`` naming the dtype (the type
converter adds the column): masked arrays, object dtypes, NumPy 2
``StringDType``, ``longdouble`` and ``clongdouble`` (platform-dependent),
datetimes and timedeltas without a unit, structured dtypes with field titles,
structured dtypes with overlapping or out-of-order field offsets (``.npy`` has
no form for them), and dtypes whose ``.npy`` header is longer than NumPy reads
without pickle support.
"""

from __future__ import annotations

import ast
import hashlib
import io
import json
import math
import re
import struct
import sys
from typing import TYPE_CHECKING, Any

import numpy as np
import numpy.typing as npt
import pyarrow as pa
from numpy.lib import format as npy_format

from orcapod.errors import UnsupportedValueError
from orcapod.logical_types.array_format import (
    NDARRAY_EXTENSION_NAME,
    NDARRAY_OPTIONS,
    array_digest_v1,
    length_prefixed,
)
from orcapod.logical_types.ignore_list import IgnoreListLogicalType

if TYPE_CHECKING:
    from orcapod.logical_types.protocols import TypeConverterProtocol

NPY_FORMAT_VERSION = (3, 0)
"""The ``.npy`` format version every array is stored in.

Version 3.0 is the only one that stores structured field names outside Latin-1.
"""

_NPY_MAGIC = b"\x93NUMPY" + bytes(NPY_FORMAT_VERSION)

_MAX_NPY_HEADER_BYTES = 10000
"""Longest ``.npy`` header ``read_array`` parses when pickle is not allowed."""

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


def _plain_array(value: Any) -> npt.NDArray[Any]:
    """Return ``value`` as a plain ``np.ndarray`` view, refusing what is not one.

    Raises:
        UnsupportedValueError: For masked arrays.
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
    return np.ndarray.view(value, np.ndarray)


def _require_storable_dtype(dtype: np.dtype[Any]) -> None:
    """Raise unless arrays of ``dtype`` have a ``.npy`` form Orcapod accepts.

    Raises:
        UnsupportedValueError: For the dtypes ``dtype_to_tag`` refuses, and for a
            structured dtype whose fields overlap or are not in offset order.
    """
    dtype_to_tag(dtype)
    try:
        # NumPy cannot describe such a dtype in a ``.npy`` header.
        _ = dtype.descr
    except ValueError as exc:
        raise _refuse(
            dtype,
            "structured fields with overlapping or out-of-order offsets have no "
            ".npy form (declare the fields in offset order)",
        ) from exc


def _without_metadata(dtype: np.dtype[Any]) -> np.dtype[Any]:
    """Return ``dtype`` with the same layout and no dtype ``metadata``.

    ``.npy`` does not store dtype metadata, and NumPy warns when asked to write
    it. Orcapod never counts it, so it is dropped before writing, at every
    depth. A dtype without metadata is returned unchanged.
    """
    if dtype.fields is not None:
        fields = {str(key): value for key, value in dtype.fields.items()}
        names = list(dtype.names or ())
        formats = [_without_metadata(fields[name][0]) for name in names]
        unchanged = all(
            new is fields[name][0] for name, new in zip(names, formats, strict=True)
        )
        if unchanged and dtype.metadata is None:
            return dtype
        return np.dtype(
            {
                "names": names,
                "formats": formats,
                "offsets": [fields[name][1] for name in names],
                "itemsize": dtype.itemsize,
            }
        )
    if dtype.subdtype is not None:
        base, shape = dtype.subdtype
        stripped = _without_metadata(base)
        if stripped is base and dtype.metadata is None:
            return dtype
        return np.dtype((stripped, shape))
    return dtype if dtype.metadata is None else np.dtype(dtype.str)


def _has_padding(dtype: np.dtype[Any]) -> bool:
    """Return whether a structured dtype has bytes that belong to no field."""
    if dtype.fields is None and dtype.subdtype is None:
        return False
    return tag_to_dtype(dtype_to_tag(dtype)).itemsize != dtype.itemsize


def _with_zeroed_padding(array: npt.NDArray[Any]) -> npt.NDArray[Any]:
    """Return a copy of a structured array whose padding bytes are zero.

    Padding between structured fields is not part of the value and NumPy does
    not define its content (``np.empty`` leaves whatever the memory held), so it
    is never written to a store. The copy has the same dtype, field values and
    C or Fortran order; ``array`` is not modified.
    """
    fortran = array.flags.f_contiguous and not array.flags.c_contiguous
    out = np.zeros(array.shape, dtype=array.dtype, order="F" if fortran else "C")
    _copy_by_field_name(out, array)
    return out


def array_to_npy(value: Any) -> bytes:
    """Return the ``.npy`` file an array is stored as.

    The array is written as it is: byte order, C or Fortran contiguity and
    structured offsets are kept. A view that is neither C- nor Fortran-contiguous
    is written C-contiguous, and padding bytes between structured fields are
    written as zero. ``value`` is not modified.

    Args:
        value: A ``numpy.ndarray`` (a subclass is stored as a plain array).

    Returns:
        The bytes of a ``.npy`` file of format ``NPY_FORMAT_VERSION``.

    Raises:
        UnsupportedValueError: For masked arrays and dtypes that cannot be stored.
        TypeError: If ``value`` is not a ``numpy.ndarray``.
    """
    plain = _plain_array(value)
    _require_storable_dtype(plain.dtype)
    stored_dtype = _without_metadata(plain.dtype)
    if stored_dtype is not plain.dtype:
        plain = plain.view(stored_dtype)
    if _has_padding(stored_dtype):
        plain = _with_zeroed_padding(plain)
    buffer = io.BytesIO()
    npy_format.write_array(
        buffer, plain, version=NPY_FORMAT_VERSION, allow_pickle=False
    )
    data = buffer.getvalue()
    (header_bytes,) = struct.unpack_from("<I", data, len(_NPY_MAGIC))
    if header_bytes > _MAX_NPY_HEADER_BYTES:
        raise _refuse(
            plain.dtype,
            f"its .npy header is {header_bytes} bytes and NumPy reads at most "
            f"{_MAX_NPY_HEADER_BYTES} without pickle support (too many fields)",
        )
    return data


def _require_declared_payload(data: bytes) -> None:
    """Raise unless the bytes after the header are exactly the declared array.

    ``read_array`` allocates the array its header declares before it reads the
    elements, so a small cell that declares a huge shape would ask for that much
    memory. The header is checked first: its length, and that dtype item size
    times the element count equals the number of bytes that follow it. A header
    that declares an object array is left for ``read_array`` to refuse.

    Raises:
        ValueError: If the header is too long, incomplete or malformed, or the
            payload length differs from what the header declares.
    """
    prefix = len(_NPY_MAGIC) + 4
    if len(data) < prefix:
        raise ValueError("stored numpy array value is truncated: no .npy header")
    (header_bytes,) = struct.unpack_from("<I", data, len(_NPY_MAGIC))
    if header_bytes > _MAX_NPY_HEADER_BYTES:
        raise ValueError(
            f"stored numpy array value has a .npy header of {header_bytes} bytes; "
            f"at most {_MAX_NPY_HEADER_BYTES} are read"
        )
    payload_start = prefix + header_bytes
    if len(data) < payload_start:
        raise ValueError(
            "stored numpy array value is truncated: incomplete .npy header"
        )
    try:
        header = ast.literal_eval(bytes(data[prefix:payload_start]).decode("utf-8"))
        shape = header["shape"]
        dtype = npy_format.descr_to_dtype(header["descr"])
        if not isinstance(shape, tuple) or not all(
            type(dim) is int and dim >= 0 for dim in shape
        ):
            raise ValueError(f"shape {shape!r} is not a tuple of non-negative integers")
    except (
        ValueError,
        TypeError,
        KeyError,
        SyntaxError,
        MemoryError,
        RecursionError,
    ) as exc:
        raise ValueError(
            f"stored numpy array value has a malformed .npy header: {exc}"
        ) from exc
    if dtype.hasobject:
        return
    declared = dtype.itemsize * math.prod(shape)
    stored = len(data) - payload_start
    if declared != stored:
        raise ValueError(
            f"stored numpy array value declares {declared} element bytes (shape "
            f"{shape}, dtype {dtype}) but holds {stored}"
        )


def npy_to_array(data: bytes) -> npt.NDArray[Any]:
    """Return the array stored in a ``.npy`` file written by ``array_to_npy``.

    Args:
        data: The stored bytes.

    Returns:
        A new, writeable plain ``np.ndarray`` with the stored dtype (including
        byte order and structured offsets), shape and contiguity; padding
        bytes between structured fields are zero.

    Raises:
        ValueError: If ``data`` is not a ``.npy`` file of format
            ``NPY_FORMAT_VERSION``, is truncated, holds more or fewer element
            bytes than its header declares, or holds an object array.
    """
    if bytes(data[: len(_NPY_MAGIC)]) != _NPY_MAGIC:
        raise ValueError(
            "stored numpy array value is not a .npy file of format "
            f"{NPY_FORMAT_VERSION[0]}.{NPY_FORMAT_VERSION[1]}"
        )
    _require_declared_payload(data)
    array = npy_format.read_array(io.BytesIO(data), allow_pickle=False)
    if array.dtype.fields is not None and _has_padding(array.dtype):
        # NumPy fills a structured array field by field, so the padding of the
        # array it returns is whatever the new memory held, not the stored zeros.
        array = _with_zeroed_padding(array)
    return array


def _byte_order_text(dtype: np.dtype[Any]) -> str:
    """Return one byte-order character per scalar leaf of ``dtype``."""
    if dtype.subdtype is not None:
        return _byte_order_text(dtype.subdtype[0])
    if dtype.fields is not None:
        fields = {str(key): value for key, value in dtype.fields.items()}
        return "".join(_byte_order_text(fields[name][0]) for name in dtype.names or ())
    order = dtype.byteorder
    if order == "=":
        return "<" if sys.byteorder == "little" else ">"
    return order


def _struct_layout(dtype: np.dtype[Any]) -> Any:
    """Return ``L(dtype)``: ``None``, or ``[itemsize, [offset, L(field)], ...]``."""
    if dtype.subdtype is not None:
        dtype = dtype.subdtype[0]
    if dtype.fields is None:
        return None
    fields = {str(key): value for key, value in dtype.fields.items()}
    layout: list[Any] = [int(dtype.itemsize)]
    for name in dtype.names or ():
        layout.append([int(fields[name][1]), _struct_layout(fields[name][0])])
    return layout


def array_value_digest(value: Any, ignore: frozenset[str]) -> bytes:
    """Return the value digest ``D`` of an array (see the module docstring).

    Args:
        value: A ``numpy.ndarray`` (a subclass is hashed as a plain array).
        ignore: Names from ``NDARRAY_OPTIONS`` whose fields are written with
            their canonical value.

    Returns:
        The 32-byte SHA-256 digest.

    Raises:
        UnsupportedValueError: For masked arrays and dtypes that cannot be stored.
        TypeError: If ``value`` is not a ``numpy.ndarray``.
    """
    plain = _plain_array(value)
    _require_storable_dtype(plain.dtype)
    tag, canonical = canonical_array(plain)
    element_digest = array_digest_v1(tag, canonical.shape, array_bytes(canonical))

    fortran = plain.flags.f_contiguous and not plain.flags.c_contiguous
    memory_order = 1 if fortran and "memory_order" not in ignore else 0
    byte_order = _byte_order_text(plain.dtype)
    if "byte_order" in ignore:
        byte_order = byte_order.replace(">", "<")
    layout_dtype = canonical.dtype if "struct_layout" in ignore else plain.dtype
    layout = json.dumps(_struct_layout(layout_dtype), separators=(",", ":"))

    h = hashlib.sha256()
    h.update(length_prefixed(NDARRAY_EXTENSION_NAME))
    h.update(element_digest)
    h.update(bytes([memory_order]))
    h.update(length_prefixed(byte_order))
    h.update(length_prefixed(layout))
    return h.digest()


class LogicalNumpyArray(IgnoreListLogicalType):
    """Logical type for ``numpy.ndarray``: one ``.npy`` file per value.

    The extension name is ``orcapod.numpy.ndarray`` over ``large_binary``; the
    extension metadata lists the properties the value hash ignores. A value reads
    back as a writeable plain ``np.ndarray`` with the dtype (including byte
    order and structured offsets), shape and C or Fortran contiguity it had.

    Args:
        ignore: Names from ``NDARRAY_OPTIONS`` the value hash of this type
            ignores. Storage is the same under every list.

    Example:
        >>> lt = LogicalNumpyArray()
        >>> arr = np.asfortranarray(np.array([[1.0, 2.0], [3.0, 4.0]], dtype=">f8"))
        >>> recovered = lt.storage_to_python(lt.python_to_storage(arr))
        >>> recovered.dtype.str, recovered.flags.f_contiguous
        ('>f8', True)
    """

    EXTENSION_NAME = NDARRAY_EXTENSION_NAME
    STORAGE_TYPE = pa.large_binary()
    VERSION = 1
    OPTIONS = NDARRAY_OPTIONS
    python_type: type = np.ndarray

    def python_to_storage(
        self, value: Any, converter: TypeConverterProtocol | None = None
    ) -> bytes:
        """Encode an array as its ``.npy`` file.

        Args:
            value: A ``numpy.ndarray``.
            converter: Ignored. Present for protocol conformance.

        Returns:
            The ``.npy`` bytes (``array_to_npy``).

        Raises:
            UnsupportedValueError: For masked arrays and dtypes that cannot be stored.
            TypeError: If ``value`` is not a ``numpy.ndarray``.
        """
        return array_to_npy(value)

    def storage_to_python(
        self, storage_value: Any, converter: TypeConverterProtocol | None = None
    ) -> npt.NDArray[Any]:
        """Decode a stored ``.npy`` file into a writeable ``numpy.ndarray``.

        Args:
            storage_value: The stored bytes.
            converter: Ignored. Present for protocol conformance.

        Returns:
            The array as it was given (``npy_to_array``).

        Raises:
            ValueError: If the bytes are not a ``.npy`` file of the stored format.
        """
        return npy_to_array(storage_value)

    def storage_sort_key(self, storage_value: Any) -> tuple[Any, ...]:
        """Ordering key for stored values, used where operators sort values.

        Orders by the strict value digest of the decoded array, never by the
        stored bytes (NumPy does not promise them across versions); ``None``
        sorts first.

        Args:
            storage_value: Stored ``.npy`` bytes, or ``None``.

        Returns:
            A tuple that orders stored values deterministically.
        """
        if storage_value is None:
            return (0,)
        return (1, array_value_digest(npy_to_array(storage_value), frozenset()))
