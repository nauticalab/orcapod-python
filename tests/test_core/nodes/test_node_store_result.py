"""Tests for store_result on all node types."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.nodes import SourceNode
from orcapod.core.sources import ArrowTableSource


@pytest.fixture
def source_and_node():
    table = pa.table(
        {
            "key": pa.array(["a", "b"], type=pa.large_string()),
            "value": pa.array([1, 2], type=pa.int64()),
        }
    )
    src = ArrowTableSource(table, tag_columns=["key"])
    node = SourceNode(src)
    return src, node


class TestSourceNodeStoreResult:
    def test_store_result_noop_without_db(self, source_and_node):
        """store_result should be a no-op when no DB is configured."""
        _, node = source_and_node
        packets = list(node.iter_packets())
        # Should not raise
        node.store_result(packets)
