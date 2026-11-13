"""GPU and gradient-tracking tensors are stored from a CPU copy, with a warning (ITL-871).

Decision 2c (developer, November 11, 2026): such tensors are accepted, the user's
tensor is never modified, and ``orcapod.errors.ValueConversionWarning`` names who gave
the value (a function pod's output, a source's column), what was converted and how
to avoid it. The warning is issued once per (function or source, output or column,
conversion), however many rows a run has, and every time when it is escalated to an
error.
"""

from __future__ import annotations

import dataclasses
import warnings
from typing import Any

import pytest

pytest.importorskip("torch")

import torch  # noqa: E402

from orcapod import function_pod  # noqa: E402
from orcapod.errors import ValueConversionWarning  # noqa: E402
from orcapod.sources import ArrowTableSource, DictSource, ListSource  # noqa: E402


def _accelerator() -> str | None:
    """A non-CPU device available here, if any."""
    if torch.cuda.is_available():
        return "cuda"
    if torch.backends.mps.is_available():
        return "mps"
    return None


def conversion_warnings(caught: list[warnings.WarningMessage]) -> list[str]:
    """The messages of the ``ValueConversionWarning`` warnings in ``caught``."""
    return [
        str(w.message) for w in caught if issubclass(w.category, ValueConversionWarning)
    ]


def run(pod: Any, source: Any) -> list[dict[str, Any]]:
    """Run ``pod`` over ``source`` and return the stored output values, row by row.

    The values are read back from the stream's table, which holds what is stored.
    """
    table = pod.pod(source).as_table()
    stored = ArrowTableSource(table, tag_columns=["k"])
    return [data.as_dict() for _, data in stored.iter_data()]


def source_of(values: list[Any]) -> DictSource:
    """A source with one row ``{"k": ..., "x": value}`` per value."""
    return DictSource(
        data=[{"k": f"r{i:05d}", "x": v} for i, v in enumerate(values)],
        tag_columns=["k"],
    )


def make_with_grad() -> Any:
    """A new pod ``with_grad`` returning a tensor with autograd history.

    Each test builds its own: a function reports a conversion once in its lifetime.
    """

    @function_pod(output_keys="y")
    def with_grad(x: torch.Tensor) -> torch.Tensor:
        """Return a tensor computed from one that tracks gradients (it has a ``grad_fn``)."""
        return x.clone().requires_grad_(True) * 2

    return with_grad


@function_pod(output_keys="y")
def plain(x: torch.Tensor) -> torch.Tensor:
    """Return a CPU tensor without gradients."""
    return x * 2


def test_the_category_is_a_user_warning_in_orcapod_errors() -> None:
    """``ValueConversionWarning`` is a ``UserWarning``."""
    assert issubclass(ValueConversionWarning, UserWarning)


def test_a_returned_gradient_tensor_warns_naming_function_output_and_fix() -> None:
    """The warning names the function, the output, the conversion and the fix."""
    with pytest.warns(ValueConversionWarning) as caught:
        out = run(make_with_grad(), source_of([torch.ones(2)]))
    (message,) = conversion_warnings(caught.list)
    assert "Function 'with_grad' output 'y'" in message
    assert "autograd history" in message
    assert "CPU tensor without autograd history" in message
    assert ".detach().cpu()" in message
    stored = out[0]["y"]
    assert stored.requires_grad and stored.is_leaf
    assert torch.equal(stored.detach(), torch.ones(2) * 2)


def test_a_returned_tensor_is_not_modified() -> None:
    """The returned tensor keeps its autograd history and values."""
    kept: list[Any] = []

    @function_pod(output_keys="y")
    def keep(x: torch.Tensor) -> torch.Tensor:
        """Return, and remember, a tensor with autograd history."""
        t = x.clone().requires_grad_(True) * 3
        kept.append(t)
        return t

    with pytest.warns(ValueConversionWarning):
        run(keep, source_of([torch.ones(2)]))
    assert kept[0].requires_grad
    assert kept[0].grad_fn is not None
    assert torch.equal(kept[0].detach(), torch.full((2,), 3.0))


def test_ten_thousand_rows_give_one_warning() -> None:
    """A 10,000-row run where every row converts gives exactly one warning."""
    source = source_of([torch.full((2,), float(i)) for i in range(10_000)])
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        out = run(make_with_grad(), source)
    assert len(out) == 10_000
    assert len(conversion_warnings(caught)) == 1


def test_each_output_and_each_function_warns_once() -> None:
    """Two outputs of one function, and a second function, each warn once."""

    @function_pod(output_keys=["a", "b"])
    def two(x: torch.Tensor) -> tuple[torch.Tensor, torch.Tensor]:
        """Return two tensors with autograd history."""
        return x.clone().requires_grad_(True) * 1, x.clone().requires_grad_(True) * 1

    source = source_of([torch.ones(2), torch.zeros(2), torch.ones(3)])
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        run(two, source)
        run(make_with_grad(), source)
    messages = conversion_warnings(caught)
    assert len(messages) == 3
    assert sum("'two' output 'a'" in m for m in messages) == 1
    assert sum("'two' output 'b'" in m for m in messages) == 1
    assert sum("'with_grad' output 'y'" in m for m in messages) == 1


def test_tensors_inside_a_list_output_warn() -> None:
    """Tensors nested in a returned list are covered."""

    @function_pod(output_keys="ys")
    def several(x: torch.Tensor) -> list[torch.Tensor]:
        """Return a list of tensors, one of which has autograd history."""
        return [x.clone().requires_grad_(True) * 1, x.clone()]

    with pytest.warns(ValueConversionWarning, match="'several' output 'ys'"):
        out = run(several, source_of([torch.ones(2)]))
    first, second = out[0]["ys"]
    assert first.requires_grad and first.is_leaf
    assert not second.requires_grad


def test_plain_cpu_tensors_never_warn() -> None:
    """CPU tensors without autograd history raise no conversion warning.

    ``requires_grad`` is stored, so a leaf that tracks gradients (and an
    ``nn.Parameter``) is not converted.
    """
    with warnings.catch_warnings():
        warnings.simplefilter("error", ValueConversionWarning)
        run(plain, source_of([torch.ones(2), torch.zeros(3)]))
        ListSource(name="x", data=[torch.ones(2)]).as_table()
        stored = [
            d.as_dict()["x"]
            for leaf in (
                torch.ones(2, requires_grad=True),
                torch.nn.Parameter(torch.ones(2)),
            )
            for _, d in source_of([leaf]).iter_data()
        ]
    assert all(t.requires_grad for t in stored)


def test_escalated_to_an_error_every_conversion_raises() -> None:
    """Under an ``error`` filter each conversion raises, also after the first one."""
    with_grad = make_with_grad()
    with warnings.catch_warnings():
        warnings.simplefilter("error", ValueConversionWarning)
        for _ in range(2):
            with pytest.raises(ValueConversionWarning, match="'with_grad' output 'y'"):
                run(with_grad, source_of([torch.ones(2)]))


def test_a_source_column_warns_once_with_the_count() -> None:
    """A source warns once per column, naming the source, the column and how many."""
    values = [
        torch.ones(2, requires_grad=True) * 1,
        torch.zeros(2),
        torch.ones(1, requires_grad=True) * 1,
    ]
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        source = DictSource(
            data=[{"k": f"r{i}", "x": v} for i, v in enumerate(values)],
            tag_columns=["k"],
            source_id="weights",
        )
    (message,) = conversion_warnings(caught)
    assert "Source 'weights' column 'x'" in message
    assert "2 of 3" in message
    assert "autograd history" in message
    assert ".detach().cpu()" in message
    assert values[0].grad_fn is not None and values[2].grad_fn is not None
    stored = [d.as_dict()["x"] for _, d in source.iter_data()]
    assert all(t.is_leaf for t in stored)


def test_a_list_source_warns() -> None:
    """``ListSource`` warns for its element column."""
    with pytest.warns(ValueConversionWarning, match="column 'w'"):
        ListSource(name="w", data=[torch.ones(2, requires_grad=True) * 1])


@pytest.mark.skipif(_accelerator() is None, reason="needs a CUDA or MPS device")
def test_a_returned_gpu_tensor_warns_naming_the_device() -> None:
    """A tensor on an accelerator is stored from a CPU copy, naming the device."""
    device = _accelerator()

    @function_pod(output_keys="y")
    def on_device(x: torch.Tensor) -> torch.Tensor:
        """Return the tensor moved to the accelerator."""
        return x.to(device)

    with pytest.warns(ValueConversionWarning, match=f"device '{device}") as caught:
        out = run(on_device, source_of([torch.arange(3.0)]))
    (message,) = conversion_warnings(caught.list)
    assert "grad_fn" not in message
    got = out[0]["y"]
    assert got.device.type == "cpu"
    assert torch.equal(got, torch.arange(3.0))


def test_an_error_filter_set_after_a_reported_conversion_still_raises() -> None:
    """Once reported as a warning, the same conversion raises under a later error filter."""
    with_grad = make_with_grad()
    with pytest.warns(ValueConversionWarning):
        run(with_grad, source_of([torch.ones(2)]))
    with warnings.catch_warnings():
        warnings.simplefilter("error", ValueConversionWarning)
        with pytest.raises(ValueConversionWarning, match="'with_grad' output 'y'"):
            run(with_grad, source_of([torch.ones(2)]))
    # Without the error filter the conversion is not reported a second time.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        run(with_grad, source_of([torch.ones(2)]))
    assert conversion_warnings(caught) == []


def test_a_module_restricted_error_filter_set_later_still_raises() -> None:
    """A later error filter with a module pattern is honoured like any other."""
    with_grad = make_with_grad()
    with pytest.warns(ValueConversionWarning):
        run(with_grad, source_of([torch.ones(2)]))
    with warnings.catch_warnings():
        warnings.filterwarnings("error", category=ValueConversionWarning, module=r".*")
        with pytest.raises(ValueConversionWarning, match="'with_grad' output 'y'"):
            run(with_grad, source_of([torch.ones(2)]))
    # A filter for another module does not apply.
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        warnings.filterwarnings(
            "error", category=ValueConversionWarning, module=r"no_such_module_xyz"
        )
        run(with_grad, source_of([torch.ones(2)]))
    assert conversion_warnings(caught) == []


@dataclasses.dataclass
class Fit:
    """A user class with a tensor field."""

    weights: torch.Tensor
    steps: int


def test_a_tensor_inside_a_user_class_field_warns() -> None:
    """A tensor with autograd history in a dataclass field is reported."""

    @function_pod(output_keys="fit")
    def fit(x: torch.Tensor) -> Fit:
        """Return a dataclass holding a tensor with a ``grad_fn``."""
        return Fit(weights=x.clone().requires_grad_(True) * 2, steps=3)

    with pytest.warns(ValueConversionWarning, match="'fit' output 'fit'.*autograd"):
        run(fit, source_of([torch.ones(2)]))
