"""Shared tensors, reference digest and assertions for the ``torch.Tensor`` tests (ITL-871).

Import this module only after ``pytest.importorskip("torch")``. Every tensor here
is a CPU, contiguous, plain ``torch.Tensor`` without gradients, so it reads back
unchanged.
"""

from __future__ import annotations

import hashlib
import struct
from collections.abc import Sequence
from typing import Any

import numpy as np
import torch

TENSORS: dict[str, Any] = {
    "bool": torch.tensor([[True, False, True], [False, False, True]]),
    "uint8": torch.tensor([0, 1, 255], dtype=torch.uint8),
    "int8": torch.tensor([-128, -1, 0, 1, 127], dtype=torch.int8),
    "int16": torch.tensor([-32768, 0, 32767], dtype=torch.int16),
    "int64": torch.tensor([[-(2**63), 0], [1, 2**63 - 1]], dtype=torch.int64),
    "uint16": torch.from_numpy(np.array([0, 1, 2**16 - 1], dtype=np.uint16)),
    "uint32": torch.from_numpy(np.array([0, 1, 2**32 - 1], dtype=np.uint32)),
    "uint64": torch.from_numpy(np.array([0, 1, 2**64 - 1], dtype=np.uint64)),
    "float16": torch.tensor([0.5, -2.0, 65504.0], dtype=torch.float16),
    "float32": torch.arange(6, dtype=torch.float32).reshape(2, 3),
    "float64_special": torch.tensor(
        [float("nan"), float("inf"), float("-inf"), -0.0, 0.0, 1e-308],
        dtype=torch.float64,
    ),
    "complex64": torch.tensor([1 + 2j, -3.5 - 0.5j], dtype=torch.complex64),
    "complex128": torch.tensor([[1 + 2j], [3 - 4j]], dtype=torch.complex128),
    "bfloat16": torch.tensor([1.5, -2.25, 3.0, float("nan")], dtype=torch.bfloat16),
    "complex32": torch.tensor([1 + 2j, -0.5 + 0.25j], dtype=torch.complex32),
    "zero_d": torch.tensor(2.5),
    "empty_0x3": torch.zeros((0, 3), dtype=torch.float32),
    "empty_3x0": torch.zeros((3, 0), dtype=torch.float32),
    "three_d": torch.arange(24, dtype=torch.int32).reshape(2, 3, 4),
}
"""One tensor per supported dtype and shape class."""

FLOAT8_DTYPES = (
    "float8_e4m3fn",
    "float8_e5m2",
    "float8_e4m3fnuz",
    "float8_e5m2fnuz",
    "float8_e8m0fnu",
)
"""The float8 dtypes Orcapod stores as raw bits, where the installed torch has them."""

for _name in FLOAT8_DTYPES:
    _dtype = getattr(torch, _name, None)
    if _dtype is not None:
        # Powers of two are exact in every float8 format, including e8m0 (exponent only).
        TENSORS[_name] = torch.tensor([0.5, 1.0, 2.0, 4.0]).to(_dtype)

NUMPY_DTYPES = frozenset(
    {"bool", "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64"}
    | {"float16", "float32", "float64", "complex64", "complex128"}
)
"""Tags whose element bytes are NumPy's: such a tensor stores the row of its ndarray."""


def shares_numpy_dtype(t: Any) -> bool:
    """Whether ``t``'s dtype is one NumPy has (so ``t.numpy()`` is the equal ndarray)."""
    return tag_of(t) in NUMPY_DTYPES


NUMERIC = (
    "int8",
    "float32",
    "complex128",
    "bfloat16",
    "zero_d",
    "empty_0x3",
    "three_d",
)
"""Keys of tensors that support ``x * 2`` and ``x.sum()``."""

LABEL = "torch_tensor_v1"
EXTENSION_NAME = "orcapod.torch.tensor"


def tag_of(t: Any) -> str:
    """The expected dtype tag: the torch dtype name (``torch.float32`` -> ``float32``)."""
    return str(t.dtype).removeprefix("torch.")


def value_bytes(t: Any) -> bytes:
    """The element bytes of ``t`` in C order, little-endian (independent of Orcapod)."""
    c = t.detach().cpu().resolve_conj().resolve_neg().contiguous()
    if c.dtype == torch.bfloat16:
        c = c.view(torch.int16)
    elif c.dtype == torch.complex32:
        # Real then imaginary part of each element, each binary16.
        c = torch.view_as_real(c).view(torch.int16)
    elif tag_of(c).startswith("float8_"):
        c = c.view(torch.uint8)
    arr = c.numpy()
    return arr.astype(arr.dtype.newbyteorder("<"), copy=False).tobytes()


def reference_digest(tag: str, shape: Sequence[int], data: bytes) -> bytes:
    """The version-1 array digest, written from the spec with ``hashlib`` and ``struct`` only."""
    tag_bytes = tag.encode("utf-8")
    preimage = (
        struct.pack("<Q", len(tag_bytes))
        + tag_bytes
        + struct.pack("<Q", len(shape))
        + b"".join(struct.pack("<q", d) for d in shape)
        + struct.pack("<Q", len(data))
        + data
    )
    return hashlib.sha256(preimage).digest()


def expected_digest(t: Any) -> bytes:
    """The reference digest of a tensor's value."""
    return reference_digest(tag_of(t), list(t.shape), value_bytes(t))


TENSOR_CLASS = "torch.Tensor"
PARAMETER_CLASS = "torch.nn.parameter.Parameter"
"""The class names written in the value hash."""


def c_strides(shape: Sequence[int]) -> tuple[int, ...]:
    """The strides, in elements, of a C-contiguous tensor of ``shape`` (as torch gives them)."""
    strides: list[int] = []
    step = 1
    for dim in reversed(list(shape)):
        strides.append(step)
        step *= max(int(dim), 1)
    return tuple(reversed(strides))


def reference_value_digest(
    element_digest: bytes,
    *,
    strides: Sequence[int],
    requires_grad: bool = False,
    conj_bit: bool = False,
    neg_bit: bool = False,
    class_name: str = TENSOR_CLASS,
) -> bytes:
    """The value hash ``D`` of a tensor, written from the spec with ``hashlib`` and ``struct``.

    ``D = SHA-256(lp(extension name) || E || u8(requires_grad) || u8(conj_bit) ||
    u8(neg_bit) || u64le(ndim) || i64le(each stride) || lp(class name))``, where
    ``lp(x) = u64le(len(utf8(x))) || utf8(x)``.
    """

    def lp(text: str) -> bytes:
        """Return ``text`` as UTF-8 behind its length (unsigned 64-bit little-endian)."""
        raw = text.encode("utf-8")
        return struct.pack("<Q", len(raw)) + raw

    preimage = (
        lp(EXTENSION_NAME)
        + element_digest
        + bytes([int(requires_grad), int(conj_bit), int(neg_bit)])
        + struct.pack("<Q", len(strides))
        + b"".join(struct.pack("<q", int(s)) for s in strides)
        + lp(class_name)
    )
    return hashlib.sha256(preimage).digest()


def assert_tensors_identical(got: Any, expected: Any) -> None:
    """Assert ``got`` is a plain CPU tensor equal to ``expected`` bit for bit.

    Args:
        got: The value read back.
        expected: The value written.
    """
    assert type(got) is torch.Tensor, f"expected torch.Tensor, got {type(got)!r}"
    assert got.dtype == expected.dtype, f"dtype {got.dtype} != {expected.dtype}"
    assert tuple(got.shape) == tuple(expected.shape)
    assert got.device.type == "cpu"
    assert not got.requires_grad
    assert got.is_contiguous()
    assert value_bytes(got) == value_bytes(expected)


def same_tensor(a: Any, b: Any) -> bool:
    """Whether ``a`` is a tensor with ``b``'s dtype, shape and bits."""
    return (
        type(a) is torch.Tensor
        and a.dtype == b.dtype
        and tuple(a.shape) == tuple(b.shape)
        and value_bytes(a) == value_bytes(b)
    )


def assert_same_tensors_any_order(got: list[Any], expected: list[Any]) -> None:
    """Assert two lists hold the same tensors, each bit for bit, in any order."""
    assert len(got) == len(expected)
    remaining = list(expected)
    for g in got:
        for i, e in enumerate(remaining):
            if same_tensor(g, e):
                del remaining[i]
                break
        else:
            raise AssertionError(f"unexpected tensor {g!r}")
