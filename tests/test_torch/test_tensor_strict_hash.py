"""The value hash of a ``torch.Tensor``: strict by default, five properties (ITL-871).

``D = SHA-256(lp(extension name) || E || requires_grad || conj_bit || neg_bit ||
ndim || strides || lp(class name))`` with ``E`` the element digest. The expected
values are computed here from that formula with ``hashlib`` and ``struct`` only
(``tensor_cases.reference_value_digest``). Hashes are taken through the default
data context's semantic hasher and through a source's rows.
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("torch")

import torch  # noqa: E402

from orcapod.contexts import get_default_context  # noqa: E402
from orcapod.sources import DictSource  # noqa: E402
from tests.test_torch.tensor_cases import (  # noqa: E402
    LABEL,
    PARAMETER_CLASS,
    TENSOR_CLASS,
    TENSORS,
    c_strides,
    expected_digest,
    reference_value_digest,
)


def semantic_hash(value: Any) -> Any:
    """The default context's semantic hash of ``value``."""
    return get_default_context().semantic_hasher.hash_object(value)


def _row(value: Any) -> Any:
    """The data of a one-row source holding ``value``."""
    source = DictSource(data=[{"k": "r", "x": value}], tag_columns=["k"])
    return next(iter(source.iter_data()))[1]


def data_hash(value: Any) -> Any:
    """The content hash of a one-row data holding ``value``."""
    return _row(value).content_hash()


def read_back(value: Any) -> Any:
    """``value`` after it was stored in a source and read from it."""
    return _row(value).as_dict()["x"]


def _base() -> Any:
    """A 2x3x4 float32 tensor."""
    return torch.arange(24, dtype=torch.float32).reshape(2, 3, 4)


def _complex() -> Any:
    """A complex64 tensor of two elements."""
    return torch.tensor([1 + 2j, 3 - 4j], dtype=torch.complex64)


# ---------------------------------------------------------------------------
# The formula
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("name", sorted(TENSORS))
def test_plain_tensor_hash_is_the_formula(name: str) -> None:
    """A plain contiguous CPU tensor hashes to ``D`` with every flag 0, under the label."""
    t = TENSORS[name]
    h = semantic_hash(t)
    assert h.method == LABEL
    assert h.digest == reference_value_digest(
        expected_digest(t),
        requires_grad=False,
        conj_bit=False,
        neg_bit=False,
        strides=c_strides(t.shape),
        class_name=TENSOR_CLASS,
    )


def test_each_property_is_written_in_its_slot() -> None:
    """Each optional property is written with its real value in its own slot."""
    base = _base()
    element = expected_digest(base)
    contiguous = c_strides(base.shape)

    grad = base.clone().requires_grad_(True)
    assert semantic_hash(grad).digest == reference_value_digest(
        element, strides=contiguous, requires_grad=True
    )

    transposed = base.transpose(0, 2)
    assert transposed.stride() == (1, 4, 12)
    assert semantic_hash(transposed).digest == reference_value_digest(
        expected_digest(transposed), strides=(1, 4, 12)
    )

    parameter = torch.nn.Parameter(base.clone(), requires_grad=False)
    assert semantic_hash(parameter).digest == reference_value_digest(
        element, strides=contiguous, class_name=PARAMETER_CLASS
    )

    conj = _complex().conj()
    assert conj.is_conj()
    assert semantic_hash(conj).digest == reference_value_digest(
        expected_digest(conj), strides=(1,), conj_bit=True
    )

    # ``.conj().imag`` is a negative view of every second float: it does not cover
    # its storage, so it is stored as a clone, which resolves the bit.
    neg = _complex().conj().imag
    assert neg.is_neg()
    assert semantic_hash(neg).digest == reference_value_digest(
        expected_digest(neg), strides=(1,)
    )


# ---------------------------------------------------------------------------
# Strict by default: each property changes the hash
# ---------------------------------------------------------------------------


def _differ_in_one_property() -> list[tuple[str, Any, Any]]:
    """Pairs with equal dtype, shape and elements that differ in one optional property."""
    base = _base()
    four_d = torch.arange(2 * 3 * 4 * 5, dtype=torch.float32).reshape(2, 3, 4, 5)
    return [
        ("requires_grad", base.clone().requires_grad_(True), base),
        ("memory_layout", base.transpose(0, 2), base.transpose(0, 2).contiguous()),
        (
            "memory_layout_channels_last",
            four_d.to(memory_format=torch.channels_last),
            four_d,
        ),
        ("conj_bit", _complex().conj(), _complex().conj().resolve_conj()),
        # torch._neg_view is the only way to get a negative view that covers its
        # storage; public code makes one only as a slice (``.conj().imag``).
        (
            "neg_bit",
            torch._neg_view(torch.tensor([1.0, -2.0])),  # pyright: ignore[reportPrivateUsage]
            torch._neg_view(torch.tensor([1.0, -2.0])).resolve_neg(),  # pyright: ignore[reportPrivateUsage]
        ),
        ("subclass", torch.nn.Parameter(base.clone(), requires_grad=False), base),
    ]


@pytest.mark.parametrize(
    ("a", "b"),
    [p[1:] for p in _differ_in_one_property()],
    ids=[p[0] for p in _differ_in_one_property()],
)
def test_each_optional_property_changes_the_hash_by_default(a: Any, b: Any) -> None:
    """Two tensors that differ in one optional property hash differently, bare and in a row."""
    assert expected_digest(a) == expected_digest(b)
    assert semantic_hash(a) != semantic_hash(b)
    assert data_hash(a) != data_hash(b)


def _never_in_the_hash() -> list[tuple[str, Any, Any]]:
    """Pairs that differ only in what storage cannot restore."""
    base = _base()
    leaf = base.clone().requires_grad_(True)
    with_grad = base.clone().requires_grad_(True)
    with_grad.sum().backward()
    pairs = [
        ("autograd_history", leaf * 1, leaf),
        ("grad_attribute", with_grad, leaf),
        (
            "storage_offset",
            torch.cat([torch.zeros(6), base.flatten()])[6:].reshape(2, 3, 4),
            base,
        ),
        ("step_slice", base[:, ::2], base[:, ::2].contiguous()),
        ("expanded", torch.ones(1).expand(4), torch.ones(4)),
    ]
    if torch.backends.mps.is_available():
        pairs.append(("mps", base.to("mps"), base))
    if torch.cuda.is_available():
        pairs.append(("cuda", base.to("cuda"), base))
    return pairs


@pytest.mark.filterwarnings("ignore::orcapod.errors.ValueConversionWarning")
@pytest.mark.parametrize(
    ("a", "b"),
    [p[1:] for p in _never_in_the_hash()],
    ids=[p[0] for p in _never_in_the_hash()],
)
def test_what_storage_cannot_restore_never_changes_the_hash(a: Any, b: Any) -> None:
    """Device, autograd history, ``.grad`` and hidden storage never enter the hash."""
    assert semantic_hash(a) == semantic_hash(b)
    assert data_hash(a) == data_hash(b)


# ---------------------------------------------------------------------------
# hash(read(write(v))) == hash(v)
# ---------------------------------------------------------------------------


def _round_trip_cases() -> list[tuple[str, Any]]:
    """Every tensor whose hash must survive storage."""
    cases = [(name, t) for name, t in sorted(TENSORS.items())]
    for name, a, b in _differ_in_one_property() + _never_in_the_hash():
        cases.append((f"{name}.a", a))
        cases.append((f"{name}.b", b))
    cases.append(("sliced_conj_view", torch.arange(6).to(torch.complex64).conj()[1:4]))
    cases.append(("parameter_with_grad", torch.nn.Parameter(_base().clone())))
    return cases


@pytest.mark.filterwarnings("ignore::orcapod.errors.ValueConversionWarning")
@pytest.mark.parametrize(
    "value",
    [c[1] for c in _round_trip_cases()],
    ids=[c[0] for c in _round_trip_cases()],
)
def test_the_hash_survives_storage(value: Any) -> None:
    """A value read back from storage hashes like the value that was written."""
    back = read_back(value)
    assert semantic_hash(back) == semantic_hash(value)
    assert data_hash(back) == data_hash(value)
    # A second trip changes nothing either.
    assert semantic_hash(read_back(back)) == semantic_hash(value)


# ---------------------------------------------------------------------------
# A tensor is never an ndarray
# ---------------------------------------------------------------------------


def test_a_tensor_never_hashes_like_the_equal_ndarray() -> None:
    """The leading type text keeps a tensor apart from an equal ndarray, also when nested."""
    t = torch.arange(6, dtype=torch.float32).reshape(2, 3)
    a = t.numpy()
    hasher = get_default_context().semantic_hasher
    assert hasher.hash_object(t).digest != hasher.hash_object(a).digest
    assert hasher.hash_object({"x": t}) != hasher.hash_object({"x": a})
    assert hasher.hash_object([t]) != hasher.hash_object([a])
    assert data_hash(t) != data_hash(a)
