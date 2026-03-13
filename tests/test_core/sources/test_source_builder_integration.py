"""Tests that sources use SourceStreamBuilder (no _arrow_source delegation)."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.core.sources.dict_source import DictSource
from orcapod.core.sources.data_frame_source import DataFrameSource


class TestDictSourceBuilder:
    def test_no_arrow_source_attr(self):
        src = DictSource(data=[{"id": 1, "x": 10}], tag_columns=["id"])
        assert not hasattr(src, "_arrow_source")

    def test_has_stream_attr(self):
        src = DictSource(data=[{"id": 1, "x": 10}], tag_columns=["id"])
        assert hasattr(src, "_stream")

    def test_iter_packets(self):
        src = DictSource(
            data=[{"id": 1, "x": 10}, {"id": 2, "x": 20}],
            tag_columns=["id"],
        )
        assert len(list(src.iter_packets())) == 2

    def test_output_schema(self):
        src = DictSource(data=[{"id": 1, "x": 10}], tag_columns=["id"])
        tag_schema, packet_schema = src.output_schema()
        assert "id" in tag_schema
        assert "x" in packet_schema

    def test_to_config(self):
        src = DictSource(data=[{"id": 1, "x": 10}], tag_columns=["id"])
        config = src.to_config()
        assert config["source_type"] == "dict"
        assert config["tag_columns"] == ["id"]

    def test_identity_uses_class_name(self):
        src = DictSource(data=[{"id": 1, "x": 10}], tag_columns=["id"])
        identity = src.identity_structure()
        assert identity[0] == "DictSource"

    def test_source_id_defaults(self):
        src = DictSource(data=[{"id": 1, "x": 10}], tag_columns=["id"])
        assert src.source_id is not None


class TestDataFrameSourceBuilder:
    def test_no_arrow_source_attr(self):
        src = DataFrameSource(data={"id": [1, 2], "x": [10, 20]}, tag_columns=["id"])
        assert not hasattr(src, "_arrow_source")

    def test_has_stream_attr(self):
        src = DataFrameSource(data={"id": [1, 2], "x": [10, 20]}, tag_columns=["id"])
        assert hasattr(src, "_stream")

    def test_iter_packets(self):
        src = DataFrameSource(data={"id": [1, 2], "x": [10, 20]}, tag_columns=["id"])
        assert len(list(src.iter_packets())) == 2

    def test_identity_uses_class_name(self):
        src = DataFrameSource(data={"id": [1], "x": [10]}, tag_columns=["id"])
        identity = src.identity_structure()
        assert identity[0] == "DataFrameSource"
