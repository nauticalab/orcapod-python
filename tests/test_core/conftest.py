"""Shared fixtures and helpers for test_core tests."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.function_pod import SimpleFunctionPod
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.streams import TableStream


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def double(x: int) -> int:
    return x * 2


def add(x: int, y: int) -> int:
    return x + y


def to_upper(name: str) -> str:
    return name.upper()


def make_int_stream(n: int = 3) -> TableStream:
    """TableStream with tag=id (int), packet=x (int)."""
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
        }
    )
    return TableStream(table, tag_columns=["id"])


def make_two_col_stream(n: int = 3) -> TableStream:
    """TableStream with tag=id, packet={x, y} for add_pf."""
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
            "y": pa.array([i * 10 for i in range(n)], type=pa.int64()),
        }
    )
    return TableStream(table, tag_columns=["id"])


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def double_pf() -> PythonPacketFunction:
    return PythonPacketFunction(double, output_keys="result")


@pytest.fixture
def add_pf() -> PythonPacketFunction:
    return PythonPacketFunction(add, output_keys="result")


@pytest.fixture
def double_pod(double_pf) -> SimpleFunctionPod:
    return SimpleFunctionPod(packet_function=double_pf)


@pytest.fixture
def add_pod(add_pf) -> SimpleFunctionPod:
    return SimpleFunctionPod(packet_function=add_pf)
