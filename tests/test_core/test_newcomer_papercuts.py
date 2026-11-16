"""Regression tests for the newcomer papercuts found by the docs audit (ITL-821).

Each test drives the public interface a workshop attendee would use.
"""

from __future__ import annotations

import logging

import pytest

import orcapod as op
from orcapod import FunctionPod, function_pod
from orcapod.nodes import FunctionNode
from orcapod.sources import DictSource


@function_pod(output_keys="bmi")
def compute_bmi(weight: float, height: float) -> float:
    """Compute a body-mass index; the pod under test takes ``weight`` and ``height``.

    Args:
        weight: Body weight.
        height: Body height.

    Returns:
        ``weight`` divided by the square of ``height``.
    """
    return weight / (height**2)


def _orcapod_warnings(caplog: pytest.LogCaptureFixture) -> list[str]:
    """Return the messages Orcapod loggers recorded at WARNING or above.

    Args:
        caplog: The pytest log-capture fixture of the running test.

    Returns:
        The formatted messages of the matching records, in emission order.
    """
    return [
        record.getMessage()
        for record in caplog.records
        if record.name.startswith("orcapod") and record.levelno >= logging.WARNING
    ]


# ---------------------------------------------------------------------------
# Item 1: enable_file_hash_caching is a top-level export
# ---------------------------------------------------------------------------


def test_enable_file_hash_caching_is_exported_at_top_level() -> None:
    """``op.enable_file_hash_caching`` is the ``orcapod.contexts`` function and is in ``__all__``."""
    from orcapod.contexts import enable_file_hash_caching

    assert op.enable_file_hash_caching is enable_file_hash_caching
    assert "enable_file_hash_caching" in op.__all__


# ---------------------------------------------------------------------------
# Item 2: a source built from no rows and no schema says the table is empty
# ---------------------------------------------------------------------------


def test_dict_source_with_no_rows_and_no_schema_says_table_is_empty() -> None:
    """A ``DictSource`` over no rows and no schema raises "Table is empty"."""
    with pytest.raises(ValueError, match="Table is empty"):
        DictSource(data=[], tag_columns=["a"], tag_only_columns=True)


def test_dict_source_with_no_rows_and_a_schema_still_builds() -> None:
    """A zero-row ``DictSource`` with ``data_schema`` still builds an empty source."""
    source = DictSource(
        data=[],
        tag_columns=["a"],
        tag_only_columns=True,
        data_schema={"a": int, "x": int},
    )
    assert source.as_table().num_rows == 0


# ---------------------------------------------------------------------------
# Item 3: an incompatible input schema raises one clear error and logs nothing
# ---------------------------------------------------------------------------


def _wide_source() -> DictSource:
    """Return a one-row source with an ``age`` column that ``compute_bmi`` does not take.

    Returns:
        A ``DictSource`` tagged by ``subject`` with ``weight``, ``height`` and ``age``.
    """
    return DictSource(
        data=[{"subject": "s1", "weight": 70.0, "height": 1.8, "age": 30}],
        tag_columns=["subject"],
        tag_only_columns=True,
    )


@pytest.mark.parametrize("call", ["process", "strict"])
def test_extra_column_error_names_the_column_and_logs_nothing(
    caplog: pytest.LogCaptureFixture, call: str
) -> None:
    """An extra column raises one error naming it, with no log line and no "data data".

    The exact calls raise it: ``pod.process(stream)`` and ``pod(stream, strict=True)``.
    A plain ``pod(stream)`` drops the extra column instead (ITL-807).

    Args:
        caplog: Captures Orcapod's log records.
        call: Which exact call is made.
    """
    with caplog.at_level(logging.DEBUG), pytest.raises(ValueError) as excinfo:
        if call == "process":
            compute_bmi.pod.process(_wide_source())
        else:
            compute_bmi.pod(_wide_source(), strict=True)

    message = str(excinfo.value)
    assert "data data" not in message
    assert message.startswith("Incoming data schema ")
    assert "column 'age' is not a parameter of the function" in message
    assert _orcapod_warnings(caplog) == []


def test_type_mismatch_error_names_the_column_and_both_types(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A column of the wrong type raises an error naming the column and both types.

    Args:
        caplog: Captures Orcapod's log records.
    """
    source = DictSource(
        data=[{"subject": "s1", "weight": "heavy", "height": 1.8}],
        tag_columns=["subject"],
        tag_only_columns=True,
    )
    with caplog.at_level(logging.DEBUG), pytest.raises(ValueError) as excinfo:
        compute_bmi.pod(source)

    assert "column 'weight' has type str, the function expects float" in str(
        excinfo.value
    )
    assert _orcapod_warnings(caplog) == []


def test_missing_parameter_error_names_the_parameter(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A required parameter with no column raises an error naming the parameter.

    Args:
        caplog: Captures Orcapod's log records.
    """
    source = DictSource(
        data=[{"subject": "s1", "weight": 70.0}],
        tag_columns=["subject"],
        tag_only_columns=True,
    )
    with caplog.at_level(logging.DEBUG), pytest.raises(ValueError) as excinfo:
        compute_bmi.pod(source)

    assert "required parameter 'height' has no matching column" in str(excinfo.value)
    assert _orcapod_warnings(caplog) == []


def test_function_node_extra_column_error_is_the_same(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """``FunctionNode`` reports an extra column with the same message as ``FunctionPod``.

    Args:
        caplog: Captures Orcapod's log records.
    """

    def bmi(weight: float, height: float) -> float:
        """Compute a body-mass index.

        Args:
            weight: Body weight.
            height: Body height.

        Returns:
            ``weight`` divided by the square of ``height``.
        """
        return weight / (height**2)

    pod = FunctionPod.from_fn(bmi, output_keys="bmi")
    with caplog.at_level(logging.DEBUG), pytest.raises(ValueError) as excinfo:
        _ = FunctionNode(pod, _wide_source())

    message = str(excinfo.value)
    assert "data data" not in message
    assert message.startswith("Incoming data schema ")
    assert "column 'age' is not a parameter of the function" in message
    assert _orcapod_warnings(caplog) == []
