"""Tests for OperatorJobNode with optional database backing."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.nodes.operator_node import OperatorJobNode
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


class TestOperatorJobNodeWithoutDatabase:
    def test_construction_without_database(self):
        node = OperatorJobNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
        )
        assert node._pipeline_database is None

    def test_iter_data_without_database(self):
        node = OperatorJobNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
        )
        node.run()
        results = list(node.iter_data())
        assert len(results) == 3

    def test_get_all_records_without_database_returns_none(self):
        node = OperatorJobNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
        )
        assert node.get_all_records() is None

    def test_as_source_without_database_raises(self):
        node = OperatorJobNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
        )
        with pytest.raises(RuntimeError):
            node.as_source()


class TestOperatorJobNodeAttachDatabases:
    def test_attach_databases_sets_pipeline_db(self):
        node = OperatorJobNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
        )
        db = InMemoryArrowDatabase()
        node.attach_databases(pipeline_database=db)
        assert node._pipeline_database is db

    def test_attach_databases_computes_node_identity_path(self):
        node = OperatorJobNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
        )
        db = InMemoryArrowDatabase()
        node.attach_databases(pipeline_database=db)
        assert node.node_identity_path is not None
        assert len(node.node_identity_path) > 0

    def test_attach_databases_clears_caches(self):
        node = OperatorJobNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
        )
        node.run()  # populate cache
        assert node._cached_output_stream is not None
        db = InMemoryArrowDatabase()
        node.attach_databases(pipeline_database=db)
        assert node._cached_output_stream is None


class TestOperatorJobNodeWithDatabase:
    def test_construction_with_database(self):
        db = InMemoryArrowDatabase()
        node = OperatorJobNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
            pipeline_database=db,
        )
        assert node._pipeline_database is db

    def test_iter_data_with_database(self):
        db = InMemoryArrowDatabase()
        node = OperatorJobNode(
            operator=Join(),
            input_streams=(_make_stream("a"), _make_stream("b")),
            pipeline_database=db,
        )
        node.run()
        results = list(node.iter_data())
        assert len(results) == 3
