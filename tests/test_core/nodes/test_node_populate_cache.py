"""Tests for populate_cache on all node types."""

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


class TestSourceNodePopulateCache:
    def test_iter_packets_uses_cache_when_populated(self, source_and_node):
        src, node = source_and_node
        original = list(node.iter_packets())
        assert len(original) == 2

        # Populate cache with only the first packet
        node.populate_cache([original[0]])

        cached = list(node.iter_packets())
        assert len(cached) == 1

    def test_iter_packets_delegates_to_stream_when_no_cache(self, source_and_node):
        _, node = source_and_node
        result = list(node.iter_packets())
        assert len(result) == 2
