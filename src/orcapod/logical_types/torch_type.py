"""``torch.Tensor`` logical type: stored with ``torch.save``, hashed by content (ITL-871).

Storage and hash are decided separately (developer, November 12, 2026).

**Storage.** One cell is the bytes ``torch.save`` writes for one tensor, in a
``large_binary`` column under the extension name ``orcapod.torch.tensor``. A cell
is read with ``torch.load(map_location="cpu", weights_only=True)``, always with
both arguments explicit: PyTorch's restricted loader, which runs no arbitrary
pickle. There is no unrestricted fallback. What is written is the *stored form*
of the value (``stored_form``): on the CPU, without autograd history, and, when
the tensor does not cover its underlying storage exactly (a slice, a step slice,
an expanded view), a ``clone(memory_format=torch.preserve_format)`` of it, so only
the elements that were passed are stored. The caller's tensor is never modified.

**Hash.** The value hash ``D`` (``tensor_digest``) is defined by Orcapod and never
reads the stored bytes. It is strict by default::

    E = array_format.array_digest_v1(dtype tag, shape, element bytes)
    D = SHA-256(lp("orcapod.torch.tensor") || E || u8(requires_grad) || u8(conj_bit)
                || u8(neg_bit) || u64le(ndim) || i64le(each stride) || lp(class name))
    lp(x) = u64le(len(utf8(x))) || utf8(x)

``E`` covers what can never be ignored: dtype, shape and every element bit (the
logical values, lazy conjugate and negative bits applied, C order, little-endian).
The five optional properties (``OPTIONS``: ``requires_grad``, ``memory_layout``,
``conj_bit``, ``neg_bit``, ``subclass``) are read from the stored form, so
``hash(read(write(v))) == hash(v)``. An ignored property's slot is written with its
canonical value (0, the C-contiguous strides of the shape, ``torch.Tensor``). Which
properties are ignored is the type's ignore list (``logical_types/ignore_list.py``,
ITL-981): configured per data context in ``contexts/logical_types.json`` and
written in the extension metadata, ``{"ignore":[...],"version":1}``.

A property storage cannot restore never counts in the hash and has no option
name: device, autograd history (``grad_fn``, ``.grad``, hooks), the storage offset
and hidden elements of a larger buffer, pinned or shared memory.

Supported dtypes: ``bool``, ``int8`` to ``int64``, ``uint8`` to ``uint64``,
``float16``, ``float32``, ``float64``, ``complex64`` and ``complex128`` (element
bytes as NumPy's), and the torch-only dtypes ``bfloat16``, ``complex32`` and the
float8 family, hashed as their raw bits under the torch dtype name. Refused at
write time with ``UnsupportedValueError`` naming the reason (the type converter
adds the column): sparse and mkldnn layouts, quantized, nested and meta tensors,
named tensors, other dtypes, and subclasses other than ``torch.nn.Parameter`` (the
restricted loader cannot read them back).

This module imports torch; it is loaded on first use through the deferred
registration in ``contexts/logical_types.json`` (``orcapod.utils.deferred_types``), so
``import orcapod`` never imports torch.
"""

from __future__ import annotations

import hashlib
import io
import pickle
import struct
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import torch

from orcapod.errors import UnsupportedValueError
from orcapod.logical_types.array_format import array_digest_v1
from orcapod.logical_types.ignore_list import IgnoreListLogicalType
from orcapod.logical_types.numpy_type import array_bytes, canonical_array

if TYPE_CHECKING:
    from orcapod.logical_types.protocols import TypeConverterProtocol

TORCH_TENSOR_EXTENSION_NAME = "orcapod.torch.tensor"
"""Arrow extension name (and logical type name) of a stored tensor. Also the
leading type text of the value hash, which keeps a tensor apart from every other
array type."""

TORCH_TENSOR_HASH_LABEL = "torch_tensor_v1"
"""``ContentHash.method`` of a tensor hash."""

TORCH_TENSOR_VERSION = 1
"""Version of the stored layout and of the hash, written in the extension metadata."""

OPTIONS = frozenset(
    {"requires_grad", "memory_layout", "conj_bit", "neg_bit", "subclass"}
)
"""The properties the value hash includes by default and can be told to ignore."""

_CLASS_NAMES: dict[type, str] = {
    torch.Tensor: "torch.Tensor",
    torch.nn.Parameter: "torch.nn.parameter.Parameter",
}
"""The classes that can be stored, with the name each is hashed under. The names
are part of the hash definition and stay fixed if PyTorch moves a class."""

_PICKLE_PROTOCOL = 2
"""``torch.save``'s default pickle protocol, passed explicitly."""

_SHARED_TAGS = (
    "bool",
    "int8",
    "int16",
    "int32",
    "int64",
    "uint8",
    "uint16",
    "uint32",
    "uint64",
    "float16",
    "float32",
    "float64",
    "complex64",
    "complex128",
)
"""Tags whose element bytes are the same as NumPy's (and whose name is the torch dtype name)."""

_RAW_BIT_TAGS: dict[str, tuple[str, int]] = {
    "bfloat16": ("int16", 1),
    "complex32": ("int16", 2),
    "float8_e4m3fn": ("uint8", 1),
    "float8_e5m2": ("uint8", 1),
    "float8_e4m3fnuz": ("uint8", 1),
    "float8_e5m2fnuz": ("uint8", 1),
    "float8_e8m0fnu": ("uint8", 1),
}
"""Tags without a NumPy dtype: the NumPy tag their bits are hashed as, and how many
of those make one element (``complex32`` is a real then an imaginary binary16)."""

_SUPPORTED_TAGS = frozenset(_SHARED_TAGS) | frozenset(_RAW_BIT_TAGS)


def _tag_of(dtype: Any) -> str | None:
    """Return the tag of a torch dtype (its name), or ``None`` if it is not supported."""
    tag = str(dtype).removeprefix("torch.")
    return tag if tag in _SUPPORTED_TAGS else None


def _refuse(what: str, reason: str) -> UnsupportedValueError:
    """Build the refusal for a tensor that cannot be stored.

    Args:
        what: The offending property, e.g. ``"layout torch.sparse_coo"``.
        reason: Why it cannot be stored and, where possible, what to do.

    Returns:
        The error to raise; the type converter prefixes the column name.
    """
    return UnsupportedValueError(
        f"torch tensors with {what} cannot be stored: {reason}"
    )


def _check_storable(value: Any) -> str:
    """Refuse what cannot be stored, and return the dtype tag of ``value``.

    Raises:
        TypeError: If ``value`` is not a ``torch.Tensor``.
        UnsupportedValueError: For extended or unsupported tensors.
    """
    if not isinstance(value, torch.Tensor):
        raise TypeError(f"expected a torch.Tensor, got {type(value).__name__}")
    if value.is_nested:
        raise _refuse(
            "nested (jagged) layout",
            "nested tensors are extended data and are not supported",
        )
    if value.layout != torch.strided:
        raise _refuse(
            f"layout {value.layout}",
            "sparse and other non-dense layouts are extended data and are not "
            "supported (convert with .to_dense())",
        )
    if value.is_quantized:
        raise _refuse(
            f"quantized dtype {value.dtype}",
            "quantized tensors are extended data and are not supported "
            "(convert with .dequantize())",
        )
    if value.device.type == "meta":
        raise _refuse("device 'meta'", "a meta tensor holds no data")
    names = getattr(value, "names", None)
    if names and any(name is not None for name in names):
        raise _refuse(
            f"dimension names {tuple(names)}",
            "named tensors are not supported (drop the names with .rename(None))",
        )
    tag = _tag_of(value.dtype)
    if tag is None:
        raise _refuse(
            f"dtype {value.dtype}",
            "this dtype is not supported in Orcapod v0.3 (supported: bool, integer, "
            "float16/32/64, bfloat16, the float8 types, complex32/64/128)",
        )
    return tag


def _c_strides(shape: Sequence[int]) -> tuple[int, ...]:
    """Return the strides, in elements, of a C-contiguous tensor of ``shape``.

    This is PyTorch's own rule: the last stride is 1 and each earlier stride is the
    next one times the next dimension's size, a size of 0 counting as 1.
    """
    strides: list[int] = []
    step = 1
    for dim in reversed(shape):
        strides.append(step)
        step *= max(int(dim), 1)
    return tuple(reversed(strides))


def _covers_storage(t: Any) -> bool:
    """Whether ``t`` holds exactly the elements of its underlying storage.

    True when the tensor starts at the beginning of its storage, the storage holds
    exactly as many elements as the tensor, and no two indices share an element
    (the strides are a permutation of a contiguous layout). Such a tensor is
    stored as it is; any other is stored as a clone.
    """
    if t.storage_offset() != 0:
        return False
    if t.untyped_storage().nbytes() != t.numel() * t.element_size():
        return False
    if t.numel() == 0:
        return True
    expected = 1
    # Dimensions from the innermost (smallest stride) outwards; a dimension of
    # size 1 takes no room, whatever its stride.
    for stride, size in sorted(zip(t.stride(), t.shape, strict=True)):
        if size == 1:
            continue
        if stride != expected:
            return False
        expected *= size
    return True


def stored_form(value: Any) -> Any:
    """Return the tensor that is written for ``value`` and that it is hashed as.

    The stored form is on the CPU and has no autograd history; ``requires_grad``,
    the class (``torch.Tensor`` or ``torch.nn.Parameter``), dtype, shape and
    elements are those of ``value``. A tensor made under ``torch.inference_mode()``
    that requires grad is cloned, because only an ordinary tensor can carry the
    flag outside that mode. When ``value`` covers its underlying storage
    exactly, its strides and lazy conjugate and negative bits are kept. Otherwise
    the stored form is ``clone(memory_format=torch.preserve_format)``: only the
    elements of ``value``, with PyTorch's strides for that clone and with the lazy
    bits resolved into the elements. ``value`` itself is never modified; the
    stored form may share memory with it.

    Args:
        value: A ``torch.Tensor`` or ``torch.nn.Parameter``.

    Returns:
        The stored form, a new tensor object.

    Raises:
        TypeError: If ``value`` is not a ``torch.Tensor``.
        UnsupportedValueError: For tensors that cannot be stored.
    """
    _check_storable(value)
    if type(value) not in _CLASS_NAMES:
        raise _refuse(
            f"class {type(value).__module__}.{type(value).__qualname__}",
            "only torch.Tensor and torch.nn.Parameter can be read back by PyTorch's "
            "restricted loader (convert with .as_subclass(torch.Tensor), or register "
            "a logical type for the class)",
        )
    t = value.detach()
    if t.device.type != "cpu":
        t = t.to("cpu")
    if not _covers_storage(t):
        t = t.clone(memory_format=torch.preserve_format)
    if value.requires_grad and t.is_inference():
        # A tensor made under torch.inference_mode() cannot be given requires_grad
        # outside it; its clone is an ordinary tensor.
        t = t.clone(memory_format=torch.preserve_format)
    if isinstance(value, torch.nn.Parameter):
        return torch.nn.Parameter(t, requires_grad=value.requires_grad)
    if value.requires_grad:
        # ``t`` is a tensor object of this function's own: the caller's is untouched.
        t.requires_grad_(True)
    return t


def canonical_tensor(value: Any) -> tuple[str, tuple[int, ...], memoryview]:
    """Return the dtype tag, shape and canonical element bytes of a tensor.

    The bytes are the elements in C order, little-endian, packed, read from a
    detached CPU copy with lazy conjugate and negative bits resolved. They are
    the input of the element digest ``E``. ``value`` itself is never modified, and
    its memory is never handed to NumPy (that would make its storage
    non-resizable).

    Args:
        value: A ``torch.Tensor``.

    Returns:
        ``(tag, shape, data)``, the arguments of ``array_format.array_digest_v1``.

    Raises:
        TypeError: If ``value`` is not a ``torch.Tensor``.
        UnsupportedValueError: For tensors that cannot be stored.
    """
    tag = _check_storable(value)
    t = value.detach()
    if t.device.type != "cpu":
        t = t.to("cpu")
    t = t.resolve_conj().resolve_neg().contiguous()
    if t.untyped_storage().data_ptr() == value.untyped_storage().data_ptr():
        # Still the caller's memory. ``Tensor.numpy()`` below makes the storage it
        # exports non-resizable, so it must not be the caller's: take a copy.
        t = t.clone()
    if tag == "complex32":
        # NumPy has no complex32: each element's real then imaginary binary16.
        t = torch.view_as_real(t)
    raw = _RAW_BIT_TAGS.get(tag)
    if raw is not None:
        t = t.view(getattr(torch, raw[0]))
    _, array = canonical_array(t.numpy())
    return tag, tuple(int(d) for d in value.shape), array_bytes(array)


def _lp(text: str) -> bytes:
    """Return ``text`` as UTF-8 behind its length as an unsigned 64-bit little-endian integer."""
    raw = text.encode("utf-8")
    return struct.pack("<Q", len(raw)) + raw


def tensor_digest(value: Any, ignore: frozenset[str] = frozenset()) -> bytes:
    """Return the value hash ``D`` of a tensor (the module docstring gives the formula).

    Args:
        value: A ``torch.Tensor`` or ``torch.nn.Parameter``.
        ignore: Names from ``OPTIONS``. The slot of an ignored property is written
            with its canonical value, so two tensors that differ only in ignored
            properties have equal digests.

    Returns:
        The 32-byte SHA-256 digest.

    Raises:
        ValueError: If ``ignore`` holds a name that is not in ``OPTIONS``.
        TypeError: If ``value`` is not a ``torch.Tensor``.
        UnsupportedValueError: For tensors that cannot be stored, and for a
            subclass other than ``torch.nn.Parameter``: the hash includes the
            class, so such a class needs a value handler of its own.
    """
    unknown = ignore - OPTIONS
    if unknown:
        raise ValueError(
            f"unknown torch tensor hash options {sorted(unknown)}; "
            f"the options are {sorted(OPTIONS)}"
        )
    if isinstance(value, torch.Tensor) and type(value) not in _CLASS_NAMES:
        raise UnsupportedValueError(
            f"a {type(value).__module__}.{type(value).__qualname__} cannot be hashed "
            "as a torch tensor: the tensor value hash includes the class and is "
            "defined for torch.Tensor and torch.nn.Parameter only. Register a value "
            "handler for the class (a subclass with its own logical type needs its "
            "own value handler too), or convert with .as_subclass(torch.Tensor)"
        )
    stored = stored_form(value)
    tag, shape, data = canonical_tensor(stored)
    strides = _c_strides(shape) if "memory_layout" in ignore else stored.stride()
    stored_class = torch.Tensor if "subclass" in ignore else type(stored)
    preimage = b"".join(
        (
            _lp(TORCH_TENSOR_EXTENSION_NAME),
            array_digest_v1(tag, shape, data),
            bytes(
                (
                    "requires_grad" not in ignore and stored.requires_grad,
                    "conj_bit" not in ignore and stored.is_conj(),
                    "neg_bit" not in ignore and stored.is_neg(),
                )
            ),
            struct.pack("<Q", len(strides)),
            struct.pack(f"<{len(strides)}q", *strides),
            _lp(_CLASS_NAMES[stored_class]),
        )
    )
    return hashlib.sha256(preimage).digest()


def tensor_to_bytes(value: Any) -> bytes:
    """Return what ``torch.save`` writes for the stored form of ``value``.

    Args:
        value: A ``torch.Tensor`` or ``torch.nn.Parameter``.

    Returns:
        The archive bytes: ``torch.save``'s zip format and pickle protocol 2, its
        defaults, passed explicitly.

    Raises:
        TypeError: If ``value`` is not a ``torch.Tensor``.
        UnsupportedValueError: For tensors that cannot be stored.
    """
    buffer = io.BytesIO()
    torch.save(
        stored_form(value),
        buffer,
        pickle_protocol=_PICKLE_PROTOCOL,
        _use_new_zipfile_serialization=True,
    )
    return buffer.getvalue()


def tensor_from_bytes(data: bytes) -> Any:
    """Read a tensor from the bytes ``tensor_to_bytes`` wrote.

    Always ``torch.load(map_location="cpu", weights_only=True)``: PyTorch's
    restricted loader. There is no fallback to an unrestricted load, and no class
    is ever added to the loader's allow list.

    Args:
        data: The stored bytes of one cell.

    Returns:
        A CPU ``torch.Tensor`` or ``torch.nn.Parameter``.

    Raises:
        ValueError: If PyTorch's restricted loader refuses the bytes (not an
            archive, a pickle that is not a plain tensor, a dtype the installed
            PyTorch does not have), or the archive holds something other than one
            tensor.
    """
    try:
        # Without autograd: PyTorch rebuilds some dtypes (complex32, the float8
        # family) in place, which a leaf that requires grad refuses. requires_grad
        # is restored all the same.
        with torch.no_grad():
            value = torch.load(io.BytesIO(data), map_location="cpu", weights_only=True)
    except Exception as exc:  # noqa: BLE001  # torch.load raises many unrelated types
        # An UnpicklingError's text advises an unrestricted load: it stays on the
        # cause only. Other errors say what is wrong (for example a dtype the
        # installed PyTorch does not have).
        detail = type(exc).__name__
        if not isinstance(exc, pickle.UnpicklingError) and str(exc):
            detail += f": {str(exc).splitlines()[0]}"
        raise ValueError(
            "stored torch tensor value cannot be read by the restricted loader "
            f"(weights_only=True) of PyTorch {torch.__version__} ({detail}). "
            "Orcapod never loads a stored tensor without that restriction"
        ) from exc
    if type(value) not in _CLASS_NAMES:
        raise ValueError(
            "stored torch tensor value holds a "
            f"{type(value).__module__}.{type(value).__qualname__}, not a tensor"
        )
    return value


class LogicalTorchTensor(IgnoreListLogicalType):
    """Logical type for ``torch.Tensor``: one ``torch.save`` archive per cell.

    The column is ``large_binary`` under the extension name
    ``orcapod.torch.tensor``; the extension metadata carries the ignore list of
    the value hash. A value reads back as a CPU ``torch.Tensor`` (or
    ``torch.nn.Parameter``) with the stored dtype, shape, elements, strides,
    ``requires_grad`` and lazy conjugate and negative bits.

    Example:
        >>> lt = LogicalTorchTensor()
        >>> t = torch.arange(6.0).reshape(2, 3)
        >>> torch.equal(lt.storage_to_python(lt.python_to_storage(t)), t)
        True
    """

    EXTENSION_NAME = TORCH_TENSOR_EXTENSION_NAME
    STORAGE_TYPE = pa.large_binary()
    VERSION = TORCH_TENSOR_VERSION
    OPTIONS = OPTIONS
    python_type: type = torch.Tensor

    def python_to_storage(
        self, value: Any, converter: TypeConverterProtocol | None = None
    ) -> bytes:
        """Encode a tensor as its ``torch.save`` archive.

        Args:
            value: A ``torch.Tensor`` or ``torch.nn.Parameter``.
            converter: Ignored. Present for protocol conformance.

        Returns:
            The bytes of the cell.

        Raises:
            UnsupportedValueError: For tensors that cannot be stored.
            TypeError: If ``value`` is not a ``torch.Tensor``.
        """
        return tensor_to_bytes(value)

    def storage_to_python(
        self, storage_value: Any, converter: TypeConverterProtocol | None = None
    ) -> Any:
        """Decode a cell with PyTorch's restricted loader.

        Args:
            storage_value: The stored bytes.
            converter: Ignored. Present for protocol conformance.

        Returns:
            A CPU ``torch.Tensor`` or ``torch.nn.Parameter``.

        Raises:
            ValueError: If the bytes are not a tensor archive the restricted
                loader reads.
        """
        return tensor_from_bytes(bytes(storage_value))

    def storage_sort_key(self, storage_value: Any) -> tuple[Any, ...]:
        """Ordering key for stored values, used where operators sort values.

        The stored bytes are PyTorch's and give no stable order, so a cell is
        ordered by the strict value hash of the tensor it holds; ``None`` sorts
        first.

        Args:
            storage_value: The stored bytes of a cell, or ``None``.

        Returns:
            A tuple that orders stored cells deterministically.
        """
        if storage_value is None:
            return (0,)
        return (1, tensor_digest(tensor_from_bytes(bytes(storage_value))))
