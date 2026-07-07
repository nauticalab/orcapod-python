"""Tests for DictSource auto-registration of Pydantic / dataclass column types.

Verifies that DictSource construction and iteration succeed for Pydantic BaseModel
and @dataclass column types that have never been registered via a function pod.
"""
from __future__ import annotations

import dataclasses

from pydantic import BaseModel

from orcapod.core.sources import DictSource


# ---------------------------------------------------------------------------
# Fixtures — fresh model classes scoped per-test to avoid cross-test registry
# state. Using classes defined at module level is fine because registration is
# idempotent; the classes just need to be new relative to any prior test run.
# ---------------------------------------------------------------------------


class _Point(BaseModel):
    x: float
    y: float


@dataclasses.dataclass
class _Measurement:
    value: float
    unit: str


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDictSourceAutoRegistration:
    def test_pydantic_model_no_prior_registration(self):
        """DictSource with a fresh Pydantic model column succeeds end-to-end."""
        src = DictSource(
            data=[
                {"id": 1, "pt": _Point(x=1.0, y=2.0)},
                {"id": 2, "pt": _Point(x=3.0, y=4.0)},
            ],
            tag_columns=["id"],
        )
        rows = list(src.iter_data())
        assert len(rows) == 2

    def test_dataclass_no_prior_registration(self):
        """DictSource with a fresh @dataclass column succeeds end-to-end."""
        src = DictSource(
            data=[
                {"id": 1, "m": _Measurement(value=1.5, unit="mm")},
                {"id": 2, "m": _Measurement(value=2.5, unit="cm")},
            ],
            tag_columns=["id"],
        )
        rows = list(src.iter_data())
        assert len(rows) == 2

    def test_pydantic_no_double_registration_error(self):
        """Creating a second DictSource with the same Pydantic model type does not raise."""
        data = [{"id": 1, "pt": _Point(x=0.0, y=0.0)}]
        DictSource(data=data, tag_columns=["id"])
        # Second construction — type already in registry; must be idempotent.
        src2 = DictSource(data=data, tag_columns=["id"])
        rows = list(src2.iter_data())
        assert len(rows) == 1

    def test_pod_registration_not_broken(self):
        """Function-pod eager registration still works after DictSource auto-registration."""
        from orcapod.core.data_function import PythonDataFunction
        from orcapod.core.function_pod import FunctionPod

        class _Sensor(BaseModel):
            reading: float

        def identity(sensor: _Sensor) -> _Sensor:
            return sensor

        # Eager registration via pod construction — must not raise.
        pod = FunctionPod(
            data_function=PythonDataFunction(identity, output_keys="sensor")
        )
        assert pod.data_function.output_data_schema == {"sensor": _Sensor}

        # DictSource with the same type — registration already done; must not raise.
        src = DictSource(
            data=[{"id": 1, "sensor": _Sensor(reading=42.0)}],
            tag_columns=["id"],
        )
        rows = list(src.iter_data())
        assert len(rows) == 1
