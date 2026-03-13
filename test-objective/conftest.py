"""Shared fixtures and helpers for specification-derived objective tests.

These tests are derived from design documents, protocol definitions, and
interface contracts — NOT from reading implementation code.
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.datagrams.datagram import Datagram
from orcapod.core.datagrams.tag_packet import Packet, Tag
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes import FunctionNode
from orcapod.core.operators import (
    Batch,
    DropPacketColumns,
    DropTagColumns,
    Join,
    MapPackets,
    MapTags,
    MergeJoin,
    PolarsFilter,
    SelectPacketColumns,
    SelectTagColumns,
    SemiJoin,
)
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource, DictSource, ListSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase, NoOpArrowDatabase
from orcapod.types import ColumnConfig, ContentHash, Schema


# ---------------------------------------------------------------------------
# Helper functions for packet functions
# ---------------------------------------------------------------------------


def double_value(x: int) -> int:
    """Double an integer value."""
    return x * 2


def add_values(x: int, y: int) -> int:
    """Add two integer values."""
    return x + y


def to_uppercase(name: str) -> str:
    """Convert a string to uppercase."""
    return name.upper()


def negate(x: int) -> int:
    """Negate an integer."""
    return -x


def square(x: int) -> int:
    """Square an integer."""
    return x * x


def concat_fields(first: str, last: str) -> str:
    """Concatenate two strings with a space."""
    return f"{first} {last}"


def return_none(x: int) -> int | None:
    """Always returns None (for testing None propagation)."""
    return None


# ---------------------------------------------------------------------------
# Arrow table factories
# ---------------------------------------------------------------------------


def make_simple_table(n: int = 3) -> pa.Table:
    """Table with tag=id (int), packet=value (int)."""
    return pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "value": pa.array([i * 10 for i in range(n)], type=pa.int64()),
        }
    )


def make_two_packet_col_table(n: int = 3) -> pa.Table:
    """Table with tag=id, packet={x, y}."""
    return pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
            "y": pa.array([i * 10 for i in range(n)], type=pa.int64()),
        }
    )


def make_string_table(n: int = 3) -> pa.Table:
    """Table with tag=id, packet=name (str)."""
    names = ["alice", "bob", "charlie"][:n]
    return pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "name": pa.array(names, type=pa.large_string()),
        }
    )


def make_joinable_tables() -> tuple[pa.Table, pa.Table]:
    """Two tables with shared tag=id, non-overlapping packet columns."""
    left = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "age": pa.array([25, 30, 35], type=pa.int64()),
        }
    )
    right = pa.table(
        {
            "id": pa.array([2, 3, 4], type=pa.int64()),
            "score": pa.array([85, 90, 95], type=pa.int64()),
        }
    )
    return left, right


def make_overlapping_packet_tables() -> tuple[pa.Table, pa.Table]:
    """Two tables with shared tag=id AND overlapping packet column 'value'."""
    left = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "value": pa.array([10, 20, 30], type=pa.int64()),
        }
    )
    right = pa.table(
        {
            "id": pa.array([2, 3, 4], type=pa.int64()),
            "value": pa.array([200, 300, 400], type=pa.int64()),
        }
    )
    return left, right


# ---------------------------------------------------------------------------
# Fixtures: Arrow tables
# ---------------------------------------------------------------------------


@pytest.fixture
def simple_table() -> pa.Table:
    return make_simple_table()


@pytest.fixture
def two_col_table() -> pa.Table:
    return make_two_packet_col_table()


@pytest.fixture
def string_table() -> pa.Table:
    return make_string_table()


# ---------------------------------------------------------------------------
# Fixtures: Streams
# ---------------------------------------------------------------------------


@pytest.fixture
def simple_stream() -> ArrowTableStream:
    """Stream with tag=id, packet=value."""
    return ArrowTableStream(make_simple_table(), tag_columns=["id"])


@pytest.fixture
def two_col_stream() -> ArrowTableStream:
    """Stream with tag=id, packet={x, y}."""
    return ArrowTableStream(make_two_packet_col_table(), tag_columns=["id"])


@pytest.fixture
def string_stream() -> ArrowTableStream:
    """Stream with tag=id, packet=name."""
    return ArrowTableStream(make_string_table(), tag_columns=["id"])


@pytest.fixture
def joinable_streams() -> tuple[ArrowTableStream, ArrowTableStream]:
    """Two streams with shared tag=id, non-overlapping packet columns."""
    left, right = make_joinable_tables()
    return (
        ArrowTableStream(left, tag_columns=["id"]),
        ArrowTableStream(right, tag_columns=["id"]),
    )


# ---------------------------------------------------------------------------
# Fixtures: Sources
# ---------------------------------------------------------------------------


@pytest.fixture
def simple_source() -> ArrowTableSource:
    return ArrowTableSource(make_simple_table(), tag_columns=["id"])


@pytest.fixture
def dict_source() -> DictSource:
    return DictSource(
        {"id": [1, 2, 3], "value": [10, 20, 30]},
        tag_columns=["id"],
    )


# ---------------------------------------------------------------------------
# Fixtures: Packet functions
# ---------------------------------------------------------------------------


@pytest.fixture
def double_pf() -> PythonPacketFunction:
    return PythonPacketFunction(double_value, output_keys="result")


@pytest.fixture
def add_pf() -> PythonPacketFunction:
    return PythonPacketFunction(add_values, output_keys="result")


@pytest.fixture
def uppercase_pf() -> PythonPacketFunction:
    return PythonPacketFunction(to_uppercase, output_keys="result")


# ---------------------------------------------------------------------------
# Fixtures: Pods
# ---------------------------------------------------------------------------


@pytest.fixture
def double_pod(double_pf) -> FunctionPod:
    return FunctionPod(packet_function=double_pf)


@pytest.fixture
def add_pod(add_pf) -> FunctionPod:
    return FunctionPod(packet_function=add_pf)


# ---------------------------------------------------------------------------
# Fixtures: Databases
# ---------------------------------------------------------------------------


@pytest.fixture
def inmemory_db() -> InMemoryArrowDatabase:
    return InMemoryArrowDatabase()


@pytest.fixture
def noop_db() -> NoOpArrowDatabase:
    return NoOpArrowDatabase()
