"""Tensor values round trip exactly through sources and function pods (ITL-871)."""

from __future__ import annotations

from typing import Any

import pytest

pytest.importorskip("torch")

import numpy as np  # noqa: E402
import torch  # noqa: E402

from orcapod import function_pod  # noqa: E402
from orcapod.errors import ValueConversionWarning  # noqa: E402
from orcapod.sources import ArrowTableSource, DictSource  # noqa: E402
from tests.test_torch.tensor_cases import (  # noqa: E402
    NUMERIC,
    TENSORS,
    assert_tensors_identical,
)


def _source(tensors: dict[str, Any], source_id: str = "tensors") -> DictSource:
    """A source with one row per tensor, tagged by its name."""
    return DictSource(
        data=[{"k": k, "x": v} for k, v in tensors.items()],
        tag_columns=["k"],
        source_id=source_id,
    )


def _read(stream: Any, column: str) -> dict[str, Any]:
    """Map each row's ``k`` tag to the value of ``column``."""
    return {t.as_dict()["k"]: d.as_dict()[column] for t, d in stream.iter_data()}


def test_one_column_holds_every_dtype_and_shape() -> None:
    """One column holds every supported dtype and shape, and each reads back exactly."""
    got = _read(_source(TENSORS), "x")
    for name, expected in TENSORS.items():
        assert_tensors_identical(got[name], expected)


def test_function_pod_input_and_output() -> None:
    """A pod receives plain CPU tensors and its outputs read back exactly."""
    seen: list[Any] = []

    @function_pod(output_keys="y")
    def identity(x: torch.Tensor) -> torch.Tensor:
        """Record the input and return a copy."""
        seen.append(x)
        return x.clone()

    got = _read(identity.pod(_source(TENSORS)), "y")
    for name, expected in TENSORS.items():
        assert_tensors_identical(got[name], expected)
    for x in seen:
        assert type(x) is torch.Tensor and not x.requires_grad


def test_chained_pods() -> None:
    """Tensors flow through two chained pods with exact values."""

    @function_pod(output_keys="d")
    def double(x: torch.Tensor) -> torch.Tensor:
        """Return twice the input."""
        return x * 2

    @function_pod(output_keys="total")
    def total(d: torch.Tensor) -> float:
        """Return the sum of the real parts as a float."""
        return float(d.to(torch.complex128).real.sum())

    numeric = {k: TENSORS[k] for k in NUMERIC}
    doubled = double.pod(_source(numeric))
    totals = _read(total.pod(doubled), "total")
    got = _read(doubled, "d")
    for name in NUMERIC:
        expected = TENSORS[name] * 2
        assert_tensors_identical(got[name], expected)
        assert totals[name] == pytest.approx(
            float(expected.to(torch.complex128).real.sum()), nan_ok=True
        )


def _stored_outputs(stream: Any, column: str) -> dict[str, Any]:
    """Read a pod's outputs back from the stored rows (``as_table``).

    In memory, a pod's output datagram holds the very object the function
    returned; the canonical CPU form is what is stored and read back from a database.
    """
    table = stream.as_table().select(["k", column])
    return _read(ArrowTableSource(table, tag_columns=["k"]), column)


def test_pod_returning_a_grad_tensor_stores_its_value() -> None:
    """An output with autograd history is stored as a leaf; the weight is untouched."""
    weight = torch.ones(3, requires_grad=True)

    @function_pod(output_keys="y")
    def scale(x: torch.Tensor) -> torch.Tensor:
        """Multiply by a weight that requires gradients."""
        return x * weight  # has a grad_fn

    with pytest.warns(ValueConversionWarning, match="autograd history"):
        got = _stored_outputs(scale.pod(_source({"a": torch.arange(3.0)})), "y")
    assert got["a"].requires_grad and got["a"].is_leaf
    assert torch.equal(got["a"].detach(), torch.arange(3.0))
    assert weight.requires_grad and weight.grad is None


@pytest.mark.skipif(not torch.backends.mps.is_available(), reason="needs MPS")
def test_pod_returning_an_mps_tensor_reads_back_on_cpu() -> None:
    """An MPS output is stored and reads back as an equal CPU tensor."""

    @function_pod(output_keys="y")
    def to_gpu(x: torch.Tensor) -> torch.Tensor:
        """Move to MPS and double."""
        return x.to("mps") * 2

    got = _stored_outputs(to_gpu.pod(_source({"a": torch.arange(3.0)})), "y")
    assert_tensors_identical(got["a"], torch.arange(3.0) * 2)


@pytest.mark.skipif(not torch.cuda.is_available(), reason="needs CUDA")
def test_pod_returning_a_cuda_tensor_reads_back_on_cpu() -> None:
    """A CUDA output is stored and reads back as an equal CPU tensor."""

    @function_pod(output_keys="y")
    def to_gpu(x: torch.Tensor) -> torch.Tensor:
        """Move to CUDA and double."""
        return x.to("cuda") * 2

    got = _stored_outputs(to_gpu.pod(_source({"a": torch.arange(3.0)})), "y")
    assert_tensors_identical(got["a"], torch.arange(3.0) * 2)


def test_read_back_is_writeable() -> None:
    """A tensor read from a source can be modified in place."""
    got = _read(_source({"a": torch.arange(3.0)}), "x")["a"]
    got[0] = 99.0
    assert got[0] == 99.0


def test_optional_tensor_column_keeps_none() -> None:
    """``torch.Tensor | None`` outputs keep ``None`` and the other tensors exactly."""

    @function_pod(output_keys="y")
    def maybe(x: torch.Tensor) -> torch.Tensor | None:
        """Return ``None`` for an empty tensor, else the input."""
        return None if x.numel() == 0 else x

    got = _read(maybe.pod(_source(TENSORS)), "y")
    for name, expected in TENSORS.items():
        if expected.numel() == 0:
            assert got[name] is None
        else:
            assert_tensors_identical(got[name], expected)


def test_list_of_tensors_round_trips() -> None:
    """A ``list[torch.Tensor]`` output reads back element by element."""

    @function_pod(output_keys="parts")
    def split(x: torch.Tensor) -> list[torch.Tensor]:
        """Return the input and its double as a list."""
        return [x, x * 2]

    got = _read(split.pod(_source({"a": torch.arange(3.0)})), "parts")
    assert len(got["a"]) == 2
    assert_tensors_identical(got["a"][0], torch.arange(3.0))
    assert_tensors_identical(got["a"][1], torch.arange(3.0) * 2)


def test_ndarray_column_into_tensor_parameter_is_refused() -> None:
    """An ndarray column bound to a ``torch.Tensor`` parameter fails when the graph is built."""

    @function_pod(output_keys="y")
    def identity(x: torch.Tensor) -> torch.Tensor:
        """Return the input unchanged."""
        return x

    arrays = DictSource(data=[{"k": "a", "x": np.arange(3.0)}], tag_columns=["k"])
    with pytest.raises(ValueError, match="not compatible") as info:
        identity.pod(arrays)
    assert "ndarray" in str(info.value) and "Tensor" in str(info.value)
