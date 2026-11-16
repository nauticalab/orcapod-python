"""``torch.Tensor`` values: cell token, refusals, declared pod outputs (ITL-871).

Storage is in ``test_tensor_storage.py`` and the value hash in
``test_tensor_strict_hash.py``. Tests go through sources, streams, function pods
and the default data context's semantic hasher.
"""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("torch")

import numpy as np  # noqa: E402
import polars as pl  # noqa: E402
import pyarrow as pa  # noqa: E402
import torch  # noqa: E402

from orcapod import function_pod  # noqa: E402
from orcapod.contexts import create_registry, get_default_context  # noqa: E402
from orcapod.errors import OutputTypeError, UnsupportedValueError  # noqa: E402
from orcapod.logical_types.base_logical_type import BaseLogicalType  # noqa: E402
from orcapod.logical_types.registry import (  # noqa: E402
    make_arrow_extension_type,
    make_polars_extension_type,
)
from orcapod.semantic_types.universal_converter import (  # noqa: E402
    UniversalTypeConverter,
)
from orcapod.sources import ArrowTableSource, DictSource  # noqa: E402
from tests.test_torch.tensor_cases import (  # noqa: E402
    EXTENSION_NAME,
    assert_tensors_identical,
)


def semantic_hash(value: Any) -> Any:
    """The default context's semantic hash of ``value``."""
    return get_default_context().semantic_hasher.hash_object(value)


def data_hash(value: Any) -> Any:
    """The content hash of a one-row data holding ``value``."""
    source = DictSource(
        data=[{"k": "r", "x": value}], tag_columns=["k"], tag_only_columns=True
    )
    return next(iter(source.iter_data()))[1].content_hash()


# ---------------------------------------------------------------------------
# Hash
# ---------------------------------------------------------------------------


def test_table_token_carries_identifier_and_digest() -> None:
    """A stored cell hashes as the logical type identifier and the digest, no label."""
    from orcapod.hashing.arrow_hashers import StarfixArrowHasher

    hasher = get_default_context().arrow_hasher
    assert isinstance(hasher, StarfixArrowHasher)
    t = torch.arange(3.0)
    table = DictSource(
        data=[{"k": "r", "x": t}], tag_columns=["k"], tag_only_columns=True
    ).as_table()
    # Non-public helper: this test is about the token the hasher builds for a cell.
    processed = hasher._process_table_columns(table.select(["x"]))  # pyright: ignore[reportPrivateUsage]
    token = processed.column("x")[0].as_py()
    metadata = b'{"ignore":[],"version":1}'
    assert (
        token
        == b"orcapod.torch.tensor;"
        + metadata.hex().encode()
        + b"::"
        + semantic_hash(t).digest
    )


DIFFERENT: list[tuple[str, Any, Any]] = [
    ("width", torch.arange(3, dtype=torch.int32), torch.arange(3, dtype=torch.int64)),
    ("empty_shapes", torch.zeros((0, 3)), torch.zeros((3, 0))),
    ("zero_d_vs_one", torch.tensor(1.0), torch.tensor([1.0])),
    ("signed_zero", torch.tensor([0.0]), torch.tensor([-0.0])),
    (
        "bfloat16_vs_float16",
        torch.ones(2, dtype=torch.bfloat16),
        torch.ones(2, dtype=torch.float16),
    ),
    ("shape", torch.arange(6.0).reshape(2, 3), torch.arange(6.0).reshape(3, 2)),
    (
        "complex32_vs_float32",
        torch.zeros(2, dtype=torch.complex32),
        torch.zeros(2, dtype=torch.float32),
    ),
    (
        "float8_e4m3fn_vs_e5m2",
        torch.zeros(2, dtype=torch.float8_e4m3fn),
        torch.zeros(2, dtype=torch.float8_e5m2),
    ),
    (
        "float8_vs_uint8",
        torch.zeros(2, dtype=torch.float8_e4m3fn),
        torch.zeros(2, dtype=torch.uint8),
    ),
]


@pytest.mark.parametrize(
    ("a", "b"), [d[1:] for d in DIFFERENT], ids=[d[0] for d in DIFFERENT]
)
def test_dtype_shape_and_bits_change_the_hash(a: Any, b: Any) -> None:
    """Dtype, shape and element bits each change the hash."""
    assert semantic_hash(a) != semantic_hash(b)
    assert data_hash(a) != data_hash(b)


# ---------------------------------------------------------------------------
# Refusals: named column and reason
# ---------------------------------------------------------------------------


def _refused() -> list[tuple[str, Any, str]]:
    """Tensors that cannot be stored, with a word the refusal must contain."""
    cases: list[tuple[str, Any, str]] = [
        ("sparse_coo", torch.eye(3).to_sparse(), "sparse"),
        ("sparse_csr", torch.eye(3).to_sparse_csr(), "sparse"),
        (
            "quantized",
            torch.quantize_per_tensor(torch.zeros(3), 1.0, 0, torch.qint8),
            "quantized",
        ),
        (
            "nested",
            torch.nested.nested_tensor([torch.zeros(2), torch.zeros(3)]),
            "nested",
        ),
        ("meta", torch.empty(3, device="meta"), "meta"),
    ]
    # Sub-byte, packed and not yet defined dtypes (where this torch has them).
    for name in ("int4", "uint3", "bits8", "float4_e2m1fn_x2", "bcomplex32"):
        dtype = getattr(torch, name, None)
        if dtype is not None:
            cases.append((name, torch.empty(2, dtype=dtype), name))
    return cases


@pytest.mark.parametrize(
    ("value", "reason"), [c[1:] for c in _refused()], ids=[c[0] for c in _refused()]
)
def test_unsupported_tensors_are_refused_naming_column_and_reason(
    value: Any, reason: str
) -> None:
    """Storing an unsupported tensor raises naming the column and the reason."""
    with pytest.raises(UnsupportedValueError, match="'payload'") as info:
        DictSource(
            data=[{"k": "r", "payload": value}],
            tag_columns=["k"],
            tag_only_columns=True,
        ).as_table()
    assert reason.lower() in str(info.value).lower()


@pytest.mark.parametrize(
    ("value", "reason"), [c[1:] for c in _refused()], ids=[c[0] for c in _refused()]
)
def test_unsupported_tensors_are_refused_by_the_hasher(value: Any, reason: str) -> None:
    """Hashing an unsupported tensor raises naming the reason."""
    with pytest.raises(UnsupportedValueError) as info:
        semantic_hash(value)
    assert reason.lower() in str(info.value).lower()


# ---------------------------------------------------------------------------
# Function pods: a declared torch.Tensor return must be a tensor
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("returned", "type_name"),
    [(np.zeros(2, np.float32), "ndarray"), (1.5, "float")],
    ids=["ndarray", "float"],
)
def test_non_tensor_returned_for_declared_tensor_is_refused_at_the_pod(
    returned: Any, type_name: str
) -> None:
    """A non-tensor returned for ``-> torch.Tensor`` raises naming the output."""

    @function_pod(output_keys="y")
    def bad(x: torch.Tensor) -> torch.Tensor:
        """Return the value under test instead of a tensor."""
        return returned

    source = DictSource(
        data=[{"k": "a", "x": torch.zeros(2)}], tag_columns=["k"], tag_only_columns=True
    )
    with pytest.raises(
        OutputTypeError,
        match=f"'bad' returned .*{type_name} for output 'y'.*declared torch.Tensor",
    ):
        list(bad.pod(source).iter_data())


def test_none_returned_for_optional_tensor_is_accepted() -> None:
    """``None`` is accepted for ``-> torch.Tensor | None``."""

    @function_pod(output_keys="y")
    def maybe(x: torch.Tensor) -> torch.Tensor | None:
        """Return ``None`` for an empty tensor, else the input."""
        return None if x.numel() == 0 else x

    source = DictSource(
        data=[{"k": "a", "x": torch.zeros(0)}, {"k": "b", "x": torch.ones(2)}],
        tag_columns=["k"],
        tag_only_columns=True,
    )
    got = {t.as_dict()["k"]: d.as_dict()["y"] for t, d in maybe.pod(source).iter_data()}
    assert got["a"] is None
    assert_tensors_identical(got["b"], torch.ones(2))


def test_declared_tensor_subclass_refuses_a_plain_tensor() -> None:
    """A pod declared ``-> nn.Parameter`` must return an ``nn.Parameter``."""

    @function_pod(output_keys="y")
    def as_parameter(x: torch.Tensor) -> torch.nn.Parameter:
        """Return a plain tensor where ``nn.Parameter`` is declared."""
        return x * 2  # pyright: ignore[reportReturnType]  # deliberately the base class

    source = DictSource(
        data=[{"k": "a", "x": torch.ones(2)}], tag_columns=["k"], tag_only_columns=True
    )
    with pytest.raises(
        OutputTypeError,
        match="returned torch.Tensor for output 'y'.*declared torch.nn.parameter.Parameter",
    ):
        list(as_parameter.pod(source).iter_data())


def test_subclass_instance_is_accepted_for_declared_tensor() -> None:
    """``-> torch.Tensor`` may return an ``nn.Parameter``; it is stored and read as one.

    What is stored for a subclass is decided by the logical type: the tensor
    type keeps the class, and hashes it (option ``subclass``).
    """

    @function_pod(output_keys="y")
    def as_parameter(x: torch.Tensor) -> torch.Tensor:
        """Return an ``nn.Parameter`` where ``torch.Tensor`` is declared."""
        return torch.nn.Parameter(x * 2, requires_grad=False)

    @function_pod(output_keys="y")
    def as_tensor(x: torch.Tensor) -> torch.Tensor:
        """Return a plain tensor with the same elements."""
        return x * 2

    source = DictSource(
        data=[{"k": "a", "x": torch.ones(2)}], tag_columns=["k"], tag_only_columns=True
    )
    stream = as_parameter.pod(source)
    y_type = stream.as_table().schema.field("y").type
    assert isinstance(y_type, pa.ExtensionType)
    assert y_type.extension_name == EXTENSION_NAME
    # Decode the stored cell: the stream's own rows hold the returned object.
    stored = ArrowTableSource(
        stream.as_table().select(["k", "y"]), tag_columns=["k"], tag_only_columns=True
    )
    got = [d.as_dict()["y"] for _, d in stored.iter_data()]
    assert type(got[0]) is torch.nn.Parameter
    assert not got[0].requires_grad
    assert torch.equal(got[0].detach(), torch.ones(2) * 2)

    def row_hash(pod_stream: Any) -> Any:
        return next(iter(pod_stream.iter_data()))[1].content_hash()

    assert row_hash(stream) != row_hash(as_tensor.pod(source))


# ---------------------------------------------------------------------------
# A declared subclass with its own logical type
# ---------------------------------------------------------------------------


class LabeledTensor(torch.Tensor):
    """Toy tensor subclass with its own logical type."""


class LogicalLabeledTensor(BaseLogicalType):
    """Toy logical type for ``LabeledTensor``: stores the raw float32 bytes."""

    _arrow_ext_class = make_arrow_extension_type(
        "test.labeled_tensor", pa.large_binary()
    )
    _polars_ext_class = make_polars_extension_type(
        "test.labeled_tensor", pa.large_binary()
    )
    logical_type_name: str = "test.labeled_tensor"
    python_type: type = LabeledTensor

    def get_arrow_extension_type(self) -> pa.ExtensionType:
        """Return the toy extension type."""
        return self._arrow_ext_class()  # pyright: ignore[reportCallIssue]  # synthesized class takes no arguments

    def get_polars_extension_type(self) -> pl.BaseExtension:
        """Return the toy Polars extension type."""
        return self._polars_ext_class()  # pyright: ignore[reportCallIssue]  # synthesized class takes no arguments

    def python_to_storage(self, value: Any, converter: Any = None) -> bytes:
        """Store the float32 bytes."""
        return value.detach().to(torch.float32).numpy().tobytes()

    def storage_to_python(self, storage_value: Any, converter: Any = None) -> Any:
        """Read the bytes back as a ``LabeledTensor``."""
        array = np.frombuffer(storage_value, dtype=np.float32).copy()
        return torch.from_numpy(array).as_subclass(LabeledTensor)


def test_declared_subclass_with_its_own_logical_type_requires_that_subclass() -> None:
    """A pod declared ``-> LabeledTensor`` (registered) must return a ``LabeledTensor``."""
    ctx = create_registry().get_context()
    converter = ctx.type_converter
    assert isinstance(converter, UniversalTypeConverter)
    converter.register_logical_type(LogicalLabeledTensor())  # pyright: ignore[reportArgumentType]  # toy logical type implements the protocol structurally

    @function_pod(output_keys="y", data_context=ctx)
    def plain(x: torch.Tensor) -> LabeledTensor:
        """Return a plain tensor where the registered subclass is declared."""
        return x * 2  # pyright: ignore[reportReturnType]  # deliberately the base class

    @function_pod(output_keys="y", data_context=ctx)
    def labeled(x: torch.Tensor) -> LabeledTensor:
        """Return the registered subclass."""
        return (x * 2).as_subclass(LabeledTensor)

    source = DictSource(
        data=[{"k": "a", "x": torch.ones(2)}],
        tag_columns=["k"],
        data_context=ctx,
        tag_only_columns=True,
    )
    with pytest.raises(
        OutputTypeError,
        match="returned torch.Tensor for output 'y'.*declared .*LabeledTensor",
    ):
        list(plain.pod(source).iter_data())
    got = [d.as_dict()["y"] for _, d in labeled.pod(source).iter_data()]
    assert type(got[0]) is LabeledTensor


def test_hashing_a_subclass_without_its_own_value_handler_says_what_is_missing() -> (
    None
):
    """A tensor subclass is not hashed as a tensor: the refusal names the value handler.

    The value hash includes the class, so only ``torch.Tensor`` and
    ``nn.Parameter`` have one. A subclass registered with its own logical type
    needs its own value handler as well.
    """
    value = torch.ones(2).as_subclass(LabeledTensor)
    with pytest.raises(UnsupportedValueError) as info:
        semantic_hash(value)
    message = str(info.value)
    assert "LabeledTensor" in message
    assert "cannot be hashed" in message
    assert "value handler" in message
    assert "cannot be stored" not in message
