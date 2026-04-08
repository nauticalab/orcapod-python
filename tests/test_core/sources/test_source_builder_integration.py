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


from orcapod.core.sources.csv_source import CSVSource
from orcapod.core.sources.delta_table_source import DeltaTableSource


class TestCSVSourceBuilder:
    def test_no_arrow_source_attr(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,x\n1,10\n2,20\n")
        src = CSVSource(file_path=str(csv_file), tag_columns=["id"])
        assert not hasattr(src, "_arrow_source")

    def test_has_stream_attr(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,x\n1,10\n2,20\n")
        src = CSVSource(file_path=str(csv_file), tag_columns=["id"])
        assert hasattr(src, "_stream")

    def test_iter_packets(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,x\n1,10\n2,20\n")
        src = CSVSource(file_path=str(csv_file), tag_columns=["id"])
        assert len(list(src.iter_packets())) == 2

    def test_round_trip_config(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,x\n1,10\n2,20\n")
        src = CSVSource(
            file_path=str(csv_file), tag_columns=["id"], record_id_column="id"
        )
        config = src.to_config()
        assert config["source_type"] == "csv"
        assert config["file_path"] == str(csv_file)
        src2 = CSVSource.from_config(config)
        assert src2.source_id == src.source_id

    def test_identity_uses_class_name(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,x\n1,10\n")
        src = CSVSource(file_path=str(csv_file), tag_columns=["id"])
        assert src.identity_structure()[0] == "CSVSource"


class TestDeltaTableSourceBuilder:
    @pytest.fixture
    def delta_path(self, tmp_path):
        from deltalake import write_deltalake

        table = pa.table({"id": pa.array([1, 2]), "x": pa.array([10, 20])})
        path = str(tmp_path / "delta_test")
        write_deltalake(path, table)
        return path

    def test_no_arrow_source_attr(self, delta_path):
        src = DeltaTableSource(delta_table_path=delta_path, tag_columns=["id"])
        assert not hasattr(src, "_arrow_source")

    def test_has_stream_attr(self, delta_path):
        src = DeltaTableSource(delta_table_path=delta_path, tag_columns=["id"])
        assert hasattr(src, "_stream")

    def test_round_trip_config(self, delta_path):
        src = DeltaTableSource(delta_table_path=delta_path, tag_columns=["id"])
        config = src.to_config()
        assert config["source_type"] == "delta_table"
        src2 = DeltaTableSource.from_config(config)
        assert src2.source_id == src.source_id

    def test_identity_uses_class_name(self, delta_path):
        src = DeltaTableSource(delta_table_path=delta_path, tag_columns=["id"])
        assert src.identity_structure()[0] == "DeltaTableSource"


from orcapod.core.sources.list_source import ListSource


class TestListSourceBuilder:
    def test_no_arrow_source_attr(self):
        src = ListSource(name="val", data=[1, 2, 3])
        assert not hasattr(src, "_arrow_source")

    def test_has_stream_attr(self):
        src = ListSource(name="val", data=[1, 2, 3])
        assert hasattr(src, "_stream")

    def test_iter_packets(self):
        src = ListSource(name="val", data=[1, 2, 3])
        assert len(list(src.iter_packets())) == 3

    def test_custom_identity_structure(self):
        src = ListSource(name="val", data=[1, 2, 3])
        identity = src.identity_structure()
        assert identity[0] == "ListSource"
        assert identity[1] == "val"
        assert identity[2] == (1, 2, 3)
        assert len(identity) == 4  # includes tag_function_hash

    def test_with_tag_function(self):
        src = ListSource(
            name="val",
            data=[10, 20],
            tag_function=lambda e, i: {"idx": i, "label": f"item_{i}"},
            expected_tag_keys=["idx", "label"],
        )
        results = list(src.iter_packets())
        assert len(results) == 2

    def test_source_id_defaults(self):
        src = ListSource(name="val", data=[1])
        assert src.source_id is not None


class TestSourceLabelDefaults:
    def test_dict_source_label_defaults_to_class_name(self):
        src = DictSource(data=[{"id": 1, "x": 10}], tag_columns=["id"])
        assert src.label == "DictSource"

    def test_dict_source_explicit_label_preserved(self):
        src = DictSource(
            data=[{"id": 1, "x": 10}],
            tag_columns=["id"],
            label="my_source",
        )
        assert src.label == "my_source"

    def test_arrow_table_source_label_defaults_to_class_name(self):
        table = pa.table({"id": pa.array([1, 2]), "x": pa.array([10, 20])})
        src = ArrowTableSource(table=table, tag_columns=["id"], infer_nullable=True)
        assert src.label == "ArrowTableSource"

    def test_list_source_label_defaults_to_class_name(self):
        src = ListSource(name="val", data=[1, 2])
        assert src.label == "ListSource"

    def test_csv_source_label_defaults_to_class_name(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("id,x\n1,10\n")
        src = CSVSource(file_path=str(csv_file), tag_columns=["id"])
        assert src.label == "CSVSource"
