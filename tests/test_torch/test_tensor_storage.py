"""How a ``torch.Tensor`` is stored: ``torch.save`` bytes, restricted load (ITL-871).

Tests go through sources and their streams. A cell is written with ``torch.save``
and read with ``torch.load(map_location="cpu", weights_only=True)``.
"""

from __future__ import annotations

import io
from typing import Any

import pytest

pytest.importorskip("torch")

import pyarrow as pa  # noqa: E402
import torch  # noqa: E402

from orcapod.contexts import get_default_context  # noqa: E402
from orcapod.errors import UnsupportedValueError  # noqa: E402
from orcapod.sources import ArrowTableSource, DictSource  # noqa: E402
from tests.test_torch.tensor_cases import (  # noqa: E402
    EXTENSION_NAME,
    TENSORS,
    value_bytes,
)


def _source(value: Any) -> DictSource:
    """A one-row source with ``value`` in column ``x``."""
    return DictSource(data=[{"k": "r", "x": value}], tag_columns=["k"])


def stored_cell(value: Any) -> bytes:
    """The stored bytes of a one-row ``x`` column."""
    chunk = _source(value).as_table().column("x").chunk(0)
    assert isinstance(chunk, pa.ExtensionArray)
    return chunk.storage[0].as_py()


def read_back(value: Any) -> Any:
    """The value of a one-row ``x`` column, read back from its source."""
    return next(iter(_source(value).iter_data()))[1].as_dict()["x"]


def read_cell(cell: bytes) -> Any:
    """Read a tensor column whose one cell holds ``cell``."""
    table = _source(torch.zeros(1)).as_table()
    column_type = table.schema.field("x").type
    storage = pa.array([cell], type=column_type.storage_type)
    forged = pa.Table.from_arrays(
        [table.column("k"), pa.ExtensionArray.from_storage(column_type, storage)],
        names=["k", "x"],
    )
    source = ArrowTableSource(forged, tag_columns=["k"])
    return next(iter(source.iter_data()))[1].as_dict()["x"]


def _base() -> Any:
    """A 2x3x4 float32 tensor."""
    return torch.arange(24, dtype=torch.float32).reshape(2, 3, 4)


# ---------------------------------------------------------------------------
# Column type and cell
# ---------------------------------------------------------------------------


def test_column_is_orcapod_torch_tensor_over_large_binary() -> None:
    """A tensor column is ``orcapod.torch.tensor`` over ``large_binary`` with an ignore list."""
    ext = _source(torch.arange(3.0)).as_table().schema.field("x").type
    assert isinstance(ext, pa.ExtensionType)
    assert ext.extension_name == EXTENSION_NAME == "orcapod.torch.tensor"
    assert ext.storage_type == pa.large_binary()
    assert ext.__arrow_ext_serialize__() == b'{"ignore":[],"version":1}'


def test_a_cell_is_what_torch_save_writes() -> None:
    """A cell is a ``torch.save`` archive that torch's restricted loader reads."""
    t = _base()
    loaded = torch.load(
        io.BytesIO(stored_cell(t)), map_location="cpu", weights_only=True
    )
    assert type(loaded) is torch.Tensor
    assert torch.equal(loaded, t)


# ---------------------------------------------------------------------------
# What reads back
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(TENSORS))
def test_dtype_shape_and_bits_read_back(name: str) -> None:
    """Each supported dtype and shape reads back as a plain CPU tensor, bit for bit."""
    expected = TENSORS[name]
    got = read_back(expected)
    assert type(got) is torch.Tensor
    assert got.dtype == expected.dtype
    assert tuple(got.shape) == tuple(expected.shape)
    assert got.device.type == "cpu"
    assert value_bytes(got) == value_bytes(expected)


def test_requires_grad_reads_back() -> None:
    """``requires_grad`` is stored; the tensor reads back as a leaf."""
    got = read_back(_base().requires_grad_(True))
    assert got.requires_grad and got.is_leaf and got.grad is None
    assert not read_back(_base()).requires_grad


@pytest.mark.parametrize("requires_grad", [True, False])
def test_parameter_reads_back_as_a_parameter(requires_grad: bool) -> None:
    """An ``nn.Parameter`` reads back as an ``nn.Parameter`` with its ``requires_grad``."""
    got = read_back(torch.nn.Parameter(_base(), requires_grad=requires_grad))
    assert type(got) is torch.nn.Parameter
    assert got.requires_grad is requires_grad
    assert torch.equal(got.detach(), _base())


def test_strides_of_a_tensor_that_covers_its_storage_read_back() -> None:
    """Transposed and channels-last layouts read back with their strides."""
    transposed = _base().transpose(0, 2)
    got = read_back(transposed)
    assert got.stride() == transposed.stride() == (1, 4, 12)
    assert torch.equal(got, transposed)

    four_d = torch.arange(120, dtype=torch.float32).reshape(2, 3, 4, 5)
    channels_last = four_d.to(memory_format=torch.channels_last)
    got = read_back(channels_last)
    assert got.stride() == channels_last.stride()
    assert got.is_contiguous(memory_format=torch.channels_last)
    assert torch.equal(got, four_d)


def test_conj_bit_reads_back() -> None:
    """A lazy conjugate view that covers its storage keeps its bit and its values."""
    conj = torch.tensor([1 + 2j, 3 - 4j]).conj()
    got = read_back(conj)
    assert got.is_conj()
    assert torch.equal(got, conj)


def test_neg_bit_reads_back() -> None:
    """A lazy negative view that covers its storage keeps its bit and its values."""
    # torch._neg_view: the only way to get a negative view that covers its storage.
    neg = torch._neg_view(torch.tensor([1.0, -2.0]))  # pyright: ignore[reportPrivateUsage]
    got = read_back(neg)
    assert got.is_neg()
    assert torch.equal(got, neg)


# ---------------------------------------------------------------------------
# Only the passed elements are stored
# ---------------------------------------------------------------------------


def test_a_slice_stores_only_its_own_elements() -> None:
    """A 3-element slice of a 1,000,000-element tensor stores 3 elements, not the buffer."""
    big = torch.arange(1_000_000, dtype=torch.float32)
    piece = big[5:8]
    cell = stored_cell(piece)
    assert len(cell) < 10_000
    got = read_back(piece)
    assert torch.equal(got, piece)
    assert got.storage_offset() == 0
    assert got.untyped_storage().nbytes() == 3 * 4


@pytest.mark.parametrize(
    ("value", "strides"),
    [
        (_base()[:, ::2], (8, 4, 1)),
        (torch.ones(1).expand(4), (1,)),
        (torch.arange(30, dtype=torch.float32)[6:].reshape(2, 3, 4), (12, 4, 1)),
        # Dense inside a larger buffer: the clone keeps the transposed strides.
        (
            torch.arange(48, dtype=torch.float32)[24:].reshape(2, 3, 4).transpose(0, 2),
            (1, 4, 12),
        ),
    ],
    ids=["step_slice", "expanded", "offset", "offset_transposed"],
)
def test_a_view_of_a_larger_buffer_reads_back_as_its_clone(
    value: Any, strides: tuple[int, ...]
) -> None:
    """A view that does not cover its storage reads back as ``clone(preserve_format)``."""
    got = read_back(value)
    assert torch.equal(got, value)
    assert got.stride() == strides
    assert got.stride() == value.clone(memory_format=torch.preserve_format).stride()
    assert got.untyped_storage().nbytes() == value.numel() * value.element_size()


# ---------------------------------------------------------------------------
# The user's tensor is not modified
# ---------------------------------------------------------------------------


def _snapshot(t: Any) -> tuple[Any, ...]:
    """Everything storing could change about ``t``."""
    return (
        t.requires_grad,
        t.grad_fn,
        t.is_leaf,
        t.grad,
        t.device.type,
        t.dtype,
        tuple(t.shape),
        t.stride(),
        t.storage_offset(),
        t.is_conj(),
        t.is_neg(),
        t.data_ptr(),
        t._version,
        type(t),
        value_bytes(t),
    )


def _given() -> list[tuple[str, Any]]:
    """Tensors with every property storing handles."""
    leaf = _base().requires_grad_(True)
    with_grad = _base().requires_grad_(True)
    with_grad.sum().backward()
    return [
        ("plain", _base()),
        ("requires_grad", leaf),
        ("non_leaf", leaf * 2),
        ("with_grad", with_grad),
        ("parameter", torch.nn.Parameter(_base())),
        ("transposed", _base().transpose(0, 2)),
        ("step_slice", _base()[:, ::2]),
        ("conj", torch.tensor([1 + 2j]).conj()),
        ("conj_imag", torch.tensor([1 + 2j]).conj().imag),
    ]


@pytest.mark.filterwarnings("ignore::orcapod.errors.ValueConversionWarning")
@pytest.mark.parametrize(
    "value", [g[1] for g in _given()], ids=[g[0] for g in _given()]
)
def test_storing_never_modifies_the_tensor(value: Any) -> None:
    """Storing, reading and hashing leave the caller's tensor exactly as it was."""
    before = _snapshot(value)
    source = _source(value)
    source.as_table()
    for _, data in source.iter_data():
        data.as_dict()
        data.content_hash()
    after = _snapshot(value)
    assert all(a is b or a == b for a, b in zip(before[:4], after[:4], strict=True))
    assert before[4:] == after[4:]


@pytest.mark.filterwarnings("ignore::orcapod.errors.ValueConversionWarning")
def test_autograd_history_is_not_stored() -> None:
    """A non-leaf tensor reads back as a leaf that requires grad, without ``.grad``."""
    leaf = _base().requires_grad_(True)
    got = read_back(leaf * 2)
    assert got.requires_grad and got.is_leaf and got.grad_fn is None
    assert torch.equal(got.detach(), _base() * 2)

    with_grad = _base().requires_grad_(True)
    with_grad.sum().backward()
    assert read_back(with_grad).grad is None


# ---------------------------------------------------------------------------
# Refusals at write
# ---------------------------------------------------------------------------


class _Wrapped(torch.Tensor):
    """A tensor subclass without a logical type of its own."""


def test_a_subclass_the_restricted_loader_cannot_read_is_refused_at_write() -> None:
    """Only ``torch.Tensor`` and ``nn.Parameter`` are stored; another subclass is refused."""
    value = torch.ones(2).as_subclass(_Wrapped)
    with pytest.raises(UnsupportedValueError, match="'payload'.*_Wrapped"):
        DictSource(data=[{"k": "r", "payload": value}], tag_columns=["k"]).as_table()


# ---------------------------------------------------------------------------
# Reading never runs a pickle
# ---------------------------------------------------------------------------


_PICKLE_RAN: list[str] = []


def _record_that_a_pickle_ran() -> None:
    """Called only if a stored pickle is executed."""
    _PICKLE_RAN.append("ran")


class _Marker:
    """Unpickling this object calls ``_record_that_a_pickle_ran``."""

    def __reduce__(self) -> tuple[Any, ...]:
        return (_record_that_a_pickle_ran, ())


def _archive(value: Any) -> bytes:
    """``value`` as an unrestricted ``torch.save`` archive."""
    buffer = io.BytesIO()
    torch.save(value, buffer)
    return buffer.getvalue()


@pytest.mark.parametrize("force_unrestricted", [False, True])
def test_a_cell_holding_an_arbitrary_pickle_is_refused(
    force_unrestricted: bool, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A forged cell raises naming the column; torch's environment override changes nothing.

    The type converter names the column (``RuntimeError``) around the logical
    type's ``ValueError``.
    """
    if force_unrestricted:
        monkeypatch.setenv("TORCH_FORCE_NO_WEIGHTS_ONLY_LOAD", "1")
    with pytest.raises(RuntimeError, match="column 'x'.*restricted loader"):
        read_cell(_archive({"x": _Marker()}))
    assert _PICKLE_RAN == []


def test_a_cell_holding_something_other_than_a_tensor_is_refused() -> None:
    """An archive the restricted loader accepts but that is not a tensor is refused."""
    with pytest.raises(RuntimeError, match="column 'x'.*dict, not a tensor"):
        read_cell(_archive({"weights": torch.zeros(2)}))


@pytest.mark.parametrize("cell", [b"", b"not a torch archive", b"PK\x03\x04truncated"])
def test_garbage_bytes_are_refused(cell: bytes) -> None:
    """Bytes that are not a ``torch.save`` archive raise naming the column."""
    with pytest.raises(RuntimeError, match="column 'x'.*restricted loader"):
        read_cell(cell)


# ---------------------------------------------------------------------------
# Gradient-bearing tensors of every floating dtype, and inference tensors
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "name", ["float32", "bfloat16", "complex32", "complex64", "float8_e4m3fn"]
)
def test_a_tensor_that_requires_grad_reads_back_for_every_dtype(name: str) -> None:
    """``requires_grad`` reads back for the dtypes PyTorch rebuilds in place, too."""
    dtype = getattr(torch, name)
    value = torch.zeros(3, dtype=dtype).requires_grad_(True)
    got = read_back(value)
    assert got.dtype == dtype
    assert got.requires_grad and got.is_leaf
    assert value_bytes(got) == value_bytes(value)
    row = next(iter(_source(value).iter_data()))[1]
    stored = ArrowTableSource(
        _source(value).as_table().select(["k", "x"]), tag_columns=["k"]
    )
    assert next(iter(stored.iter_data()))[1].content_hash() == row.content_hash()


def test_an_inference_tensor_that_requires_grad_is_stored() -> None:
    """A tensor made under ``torch.inference_mode()`` is stored without touching it."""
    with torch.inference_mode():
        value = torch.arange(3.0, requires_grad=True)
    assert value.is_inference() and value.requires_grad
    got = read_back(value)
    assert got.requires_grad and not got.is_inference()
    assert torch.equal(got.detach(), torch.arange(3.0))
    assert value.is_inference() and value.requires_grad


def test_hashing_leaves_the_callers_storage_resizable() -> None:
    """Hashing and storing do not pin the caller's storage (``resize_`` still works)."""
    value = torch.arange(3.0)
    source = _source(value)
    source.as_table()
    for _, data in source.iter_data():
        data.content_hash()
    get_default_context().semantic_hasher.hash_object(value)
    value.resize_(4)
    assert tuple(value.shape) == (4,)
