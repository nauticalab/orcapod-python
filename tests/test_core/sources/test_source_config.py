"""Tests for source to_config / from_config serialization."""

import pytest

from orcapod.core.sources.csv_source import CSVSource
from orcapod.core.sources.delta_table_source import DeltaTableSource
from orcapod.core.sources.dict_source import DictSource
from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.core.sources.list_source import ListSource


class TestCSVSourceConfig:
    def test_to_config(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a,b\n1,2\n3,4\n")
        source = CSVSource(
            file_path=str(csv_file),
            tag_columns=["a"],
            source_id="test_csv",
        )
        config = source.to_config()
        assert config["source_type"] == "csv"
        assert config["file_path"] == str(csv_file)
        assert config["tag_columns"] == ["a"]
        assert config["source_id"] == "test_csv"

    def test_round_trip(self, tmp_path):
        csv_file = tmp_path / "test.csv"
        csv_file.write_text("a,b\n1,2\n3,4\n")
        source = CSVSource(
            file_path=str(csv_file),
            tag_columns=["a"],
        )
        config = source.to_config()
        restored = CSVSource.from_config(config)
        assert isinstance(restored, CSVSource)
        assert restored.source_id == source.source_id


class TestDictSourceConfig:
    def test_to_config(self):
        source = DictSource(
            data=[{"a": 1, "b": 2}, {"a": 3, "b": 4}],
            tag_columns=["a"],
            source_id="test_dict",
        )
        config = source.to_config()
        assert config["source_type"] == "dict"
        assert config["tag_columns"] == ["a"]
        assert config["source_id"] == "test_dict"

    def test_from_config_raises(self):
        config = {
            "source_type": "dict",
            "tag_columns": ["a"],
            "source_id": "test_dict",
        }
        with pytest.raises(NotImplementedError):
            DictSource.from_config(config)


class TestArrowTableSourceConfig:
    def test_from_config_raises(self):
        config = {"source_type": "arrow_table", "tag_columns": ["a"]}
        with pytest.raises(NotImplementedError):
            ArrowTableSource.from_config(config)


class TestListSourceConfig:
    def test_from_config_raises(self):
        config = {"source_type": "list", "name": "test"}
        with pytest.raises(NotImplementedError):
            ListSource.from_config(config)


class TestDeltaTableSourceConfig:
    def test_to_config(self, tmp_path):
        import pyarrow as pa
        from deltalake import write_deltalake

        table = pa.table({"a": [1, 2], "b": [3, 4]})
        delta_path = str(tmp_path / "delta")
        write_deltalake(delta_path, table)
        source = DeltaTableSource(
            delta_table_path=delta_path,
            tag_columns=["a"],
            source_id="test_delta",
        )
        config = source.to_config()
        assert config["source_type"] == "delta_table"
        assert "delta_table_path" in config

    def test_round_trip(self, tmp_path):
        import pyarrow as pa
        from deltalake import write_deltalake

        table = pa.table({"a": [1, 2], "b": [3, 4]})
        delta_path = str(tmp_path / "delta")
        write_deltalake(delta_path, table)
        source = DeltaTableSource(
            delta_table_path=delta_path,
            tag_columns=["a"],
        )
        config = source.to_config()
        restored = DeltaTableSource.from_config(config)
        assert isinstance(restored, DeltaTableSource)


class TestCachedSourceConfig:
    def test_to_config(self):
        from orcapod.core.sources.cached_source import CachedSource
        from orcapod.databases.in_memory_databases import InMemoryArrowDatabase

        inner = DictSource(
            data=[{"a": 1, "b": 2}], tag_columns=["a"], source_id="inner"
        )
        cache_db = InMemoryArrowDatabase()
        source = CachedSource(source=inner, cache_database=cache_db)
        config = source.to_config()
        assert config["source_type"] == "cached"
        assert "inner_source" in config
        assert "cache_database" in config
