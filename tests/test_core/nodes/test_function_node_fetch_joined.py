# tests/test_core/nodes/test_function_node_fetch_joined.py
"""Tests for FunctionJobNode._fetch_joined_records."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.nodes.function_node import (
    FunctionJobNode,
    _PIPELINE_ENTRY_ID_COL,
    _PIPELINE_BASE_ENTRY_ID_COL,
)
from orcapod.core.sources import ArrowTableSource
from orcapod.databases import InMemoryArrowDatabase


def double_value(value: int) -> int:
    return value * 2


@pytest.fixture
def node_without_db():
    """FunctionJobNode with no databases attached."""
    table = pa.table(
        {
            "key": pa.array(["a"], type=pa.large_string()),
            "value": pa.array([1], type=pa.int64()),
        }
    )
    src = ArrowTableSource(table, tag_columns=["key"], infer_nullable=True)
    pod = FunctionPod(PythonDataFunction(double_value, output_keys="result"))
    return FunctionJobNode(pod, src)


@pytest.fixture
def node_with_empty_db():
    """FunctionJobNode with databases attached but no data executed."""
    table = pa.table(
        {
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "value": pa.array([1, 2], type=pa.int64()),
        }
    )
    src = ArrowTableSource(table, tag_columns=["key"], infer_nullable=True)
    pod = FunctionPod(PythonDataFunction(double_value, output_keys="result"))
    return FunctionJobNode(
        pod,
        src,
        pipeline_database=InMemoryArrowDatabase(),
        result_database=InMemoryArrowDatabase(),
    )


@pytest.fixture
def executed_node(node_with_empty_db):
    """FunctionJobNode with databases attached and both input rows executed."""
    node = node_with_empty_db
    for tag, data in node._input_stream.iter_data():
        node.execute_data(tag, data)
    return node


class TestFetchJoinedRecords:
    def test_returns_none_when_no_db(self, node_without_db):
        """Returns None when no databases are attached."""
        assert node_without_db._fetch_joined_records() is None

    def test_returns_none_when_db_fetch_returns_none(self, node_with_empty_db):
        """Returns None when the pipeline DB returns None (no records written yet)."""
        assert node_with_empty_db._fetch_joined_records() is None

    def test_returned_table_includes_pipeline_entry_id_column(self, executed_node):
        """The returned table always contains __pipeline_entry_id."""
        result = executed_node._fetch_joined_records()
        assert result is not None
        assert _PIPELINE_ENTRY_ID_COL in result.table.column_names

    def test_taginfo_columns_present_in_result(self, executed_node):
        """taginfo_columns contains the pre-join pipeline DB column names as strings.

        Specifically, _PIPELINE_ENTRY_ID_COL must be present because it is added
        by the pipeline DB fetch (record_id_column=_PIPELINE_ENTRY_ID_COL) and
        captured before the join — confirming the pre-filter snapshot is correct.
        """
        result = executed_node._fetch_joined_records()
        assert result is not None
        assert isinstance(result.taginfo_columns, tuple)
        assert all(isinstance(c, str) for c in result.taginfo_columns)
        assert _PIPELINE_ENTRY_ID_COL in result.taginfo_columns

    def test_no_entry_ids_returns_all_rows(self, executed_node):
        """Calling with entry_ids=None returns all executed rows."""
        result = executed_node._fetch_joined_records()
        assert result is not None
        assert result.table.num_rows == 2

    def test_entry_ids_filter_narrows_rows(self, executed_node):
        """Passing a single base_entry_id returns only that row."""
        node = executed_node
        input_pairs = list(node._input_stream.iter_data())
        base_entry_id_0 = node.compute_base_entry_id(input_pairs[0][0], input_pairs[0][1])

        result = node._fetch_joined_records(base_entry_ids=[base_entry_id_0])
        assert result is not None
        assert result.table.num_rows == 1
        assert result.table.column(_PIPELINE_BASE_ENTRY_ID_COL)[0].as_py() == base_entry_id_0

    def test_empty_entry_ids_returns_zero_rows(self, executed_node):
        """Passing entry_ids=[] returns a 0-row table, not None."""
        result = executed_node._fetch_joined_records(base_entry_ids=[])
        assert result is not None
        assert result.table.num_rows == 0
