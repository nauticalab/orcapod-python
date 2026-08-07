"""Source-info values may be lists, not just scalar strings.

Many->one operators (GroupBy, MergeJoin) produce one provenance token per
member.  `Data` must represent those without collapsing or crashing.
"""

from __future__ import annotations

import pyarrow as pa

from orcapod.core.datagrams import Data


def _data_with_mixed_source_info() -> Data:
    """Data with one list-valued and one scalar-null source token."""
    return Data(
        {"probe": [0, 1], "path": ["a", "b"]},
        source_info={"probe": None, "path": ["s0", "s1"]},
    )


class TestListValuedSourceInfo:
    def test_schema_reports_list_type_for_list_valued_token(self):
        data = _data_with_mixed_source_info()
        schema = data.schema(columns={"source": True})
        assert schema["_source_path"] == list[str]

    def test_schema_reports_str_for_none_token(self):
        data = _data_with_mixed_source_info()
        schema = data.schema(columns={"source": True})
        assert schema["_source_probe"] is str

    def test_as_table_round_trips_list_valued_token(self):
        data = _data_with_mixed_source_info()
        table = data.as_table(columns={"source": True})
        assert table.schema.field("_source_path").type == pa.large_list(
            pa.large_string()
        )
        assert table.column("_source_path").to_pylist() == [["s0", "s1"]]

    def test_as_table_keeps_none_token_as_large_string(self):
        data = _data_with_mixed_source_info()
        table = data.as_table(columns={"source": True})
        assert table.schema.field("_source_probe").type == pa.large_string()
        assert table.column("_source_probe").to_pylist() == [None]

    def test_empty_list_token_defaults_to_list_of_string(self):
        data = Data({"path": ["a"]}, source_info={"path": []})
        table = data.as_table(columns={"source": True})
        assert table.schema.field("_source_path").type == pa.large_list(
            pa.large_string()
        )

    def test_scalar_token_unchanged(self):
        """Existing scalar behavior must not regress."""
        data = Data({"path": "a"}, source_info={"path": "src::row_0::path"})
        table = data.as_table(columns={"source": True})
        assert table.schema.field("_source_path").type == pa.large_string()
        assert data.schema(columns={"source": True})["_source_path"] is str

    def test_arrow_table_construction_recovers_list_token(self):
        """Data built from an Arrow table keeps list-valued source info."""
        table = pa.table(
            {
                "path": pa.array([["a", "b"]], pa.list_(pa.large_string())),
                "_source_path": pa.array([["s0", "s1"]], pa.list_(pa.large_string())),
            }
        )
        data = Data(table)
        assert data.source_info()["path"] == ["s0", "s1"]
        assert data.as_table(columns={"source": True}).column(
            "_source_path"
        ).to_pylist() == [["s0", "s1"]]
