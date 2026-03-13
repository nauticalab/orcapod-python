"""Tests for OperatorNode with optional database backing."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.nodes import OperatorNode
from orcapod.core.operators.join import Join
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase


def _make_stream(name="x", n=3):
    return ArrowTableStream(
        pa.table(
            {
                "id": pa.array(list(range(n)), type=pa.int64()),
                name: pa.array(list(range(n)), type=pa.int64()),
            }
        ),
        tag_columns=["id"],
    )


class TestOperatorNodeWithoutDatabase:
    def test_construction_without_database(self):
        node = OperatorNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
        )
        assert node._pipeline_database is None

    def test_iter_packets_without_database(self):
        node = OperatorNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
        )
        results = list(node.iter_packets())
        assert len(results) == 3

    def test_get_all_records_without_database_returns_none(self):
        node = OperatorNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
        )
        assert node.get_all_records() is None

    def test_as_source_without_database_raises(self):
        node = OperatorNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
        )
        with pytest.raises(RuntimeError):
            node.as_source()


class TestOperatorNodeAttachDatabases:
    def test_attach_databases_sets_pipeline_db(self):
        node = OperatorNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
        )
        db = InMemoryArrowDatabase()
        node.attach_databases(pipeline_database=db)
        assert node._pipeline_database is db

    def test_attach_databases_computes_pipeline_path(self):
        node = OperatorNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
        )
        db = InMemoryArrowDatabase()
        node.attach_databases(pipeline_database=db)
        assert node.pipeline_path is not None
        assert len(node.pipeline_path) > 0

    def test_attach_databases_clears_caches(self):
        node = OperatorNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
        )
        node.run()  # populate cache
        assert node._cached_output_stream is not None
        db = InMemoryArrowDatabase()
        node.attach_databases(pipeline_database=db)
        assert node._cached_output_stream is None


class TestOperatorNodeWithDatabase:
    def test_construction_with_database(self):
        db = InMemoryArrowDatabase()
        node = OperatorNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
            pipeline_database=db,
        )
        assert node._pipeline_database is db

    def test_iter_packets_with_database(self):
        db = InMemoryArrowDatabase()
        node = OperatorNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
            pipeline_database=db,
        )
        results = list(node.iter_packets())
        assert len(results) == 3
