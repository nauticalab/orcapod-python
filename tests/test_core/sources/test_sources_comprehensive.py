"""
Comprehensive tests for orcapod/core/sources/ — covering all source types and
behaviours not already exercised in test_sources.py or
test_source_protocol_conformance.py.

Coverage added here:
- CSVSource: construction, source_id defaulting, record_id_column, resolve_field,
  file-not-found, protocol conformance
- DeltaTableSource: construction, source_id defaulting, resolve_field, bad path
  error, protocol conformance
- DataFrameSource: string tag_columns, resolve_field raises, system-column
  stripping from Polars input, source_id parameter
- DictSource: data_schema parameter, empty-data raises, source_id, content
  hash with explicit schema
- ListSource: tag_function_hash_mode='signature' and 'content', empty list,
  tag function inference without expected_tag_keys, TagProtocol.as_dict() protocol,
  identity_structure stability
- ArrowTableSource: table property, source_id controls provenance tokens,
  negative row index raises, duplicate record_id takes first match,
  system_tag_columns forwarded, integer record_id_column values
- SourceRegistry: replace() returns None when no prior entry, replace() with
  empty source_id raises, register() with None raises, __repr__
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import pyarrow as pa
import polars as pl
import pytest

from orcapod.core.sources import (
    ArrowTableSource,
    CSVSource,
    DataFrameSource,
    DeltaTableSource,
    DictSource,
    ListSource,
    RootSource,
    SourceRegistry,
)
from orcapod.errors import FieldNotResolvableError
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.types import Schema


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _simple_table(n: int = 3) -> pa.Table:
    return pa.table(
        {
            "user_id": pa.array(
                [f"u{i}" for i in range(1, n + 1)], type=pa.large_string()
            ),
            "score": pa.array(list(range(10, 10 * (n + 1), 10))[:n], type=pa.int64()),
        }
    )


@pytest.fixture
def csv_path(tmp_path: Path) -> str:
    """Write a simple CSV file and return its path."""
    p = tmp_path / "data.csv"
    p.write_text("user_id,score\nu1,10\nu2,20\nu3,30\n")
    return str(p)


@pytest.fixture
def delta_path(tmp_path: Path) -> Path:
    """Create a simple Delta Lake table and return its directory path."""
    from deltalake import write_deltalake

    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "value": pa.array(["a", "b", "c"], type=pa.large_string()),
        }
    )
    dest = tmp_path / "delta_table"
    write_deltalake(str(dest), table)
    return dest


# ---------------------------------------------------------------------------
# CSVSource
# ---------------------------------------------------------------------------


class TestCSVSource:
    def test_construction_reads_rows(self, csv_path):
        src = CSVSource(file_path=csv_path, tag_columns=["user_id"])
        assert len(list(src.iter_packets())) == 3

    def test_tag_and_packet_keys(self, csv_path):
        src = CSVSource(file_path=csv_path, tag_columns=["user_id"])
        tag_keys, packet_keys = src.keys()
        assert "user_id" in tag_keys
        assert "score" in packet_keys

    def test_source_id_defaults_to_file_path(self, csv_path):
        src = CSVSource(file_path=csv_path)
        assert src.source_id == csv_path

    def test_source_id_explicit_overrides_default(self, csv_path):
        src = CSVSource(file_path=csv_path, source_id="my_csv_name")
        assert src.source_id == "my_csv_name"

    def test_resolve_field_row_index(self, csv_path):
        src = CSVSource(file_path=csv_path, tag_columns=["user_id"])
        assert src.resolve_field("row_0", "score") == 10
        assert src.resolve_field("row_2", "score") == 30

    def test_resolve_field_column_value(self, csv_path):
        src = CSVSource(
            file_path=csv_path,
            tag_columns=["user_id"],
            record_id_column="user_id",
            source_id="csv_src",
        )
        assert src.resolve_field("user_id=u2", "score") == 20

    def test_resolve_field_unknown_field_raises(self, csv_path):
        src = CSVSource(file_path=csv_path)
        with pytest.raises(FieldNotResolvableError):
            src.resolve_field("row_0", "nonexistent")

    def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(Exception):
            CSVSource(file_path=str(tmp_path / "no_such_file.csv"))

    def test_is_stream(self, csv_path):
        assert isinstance(CSVSource(file_path=csv_path), StreamProtocol)

    def test_is_root_source(self, csv_path):
        assert isinstance(CSVSource(file_path=csv_path), RootSource)

    def test_output_schema_returns_two_schemas(self, csv_path):
        src = CSVSource(file_path=csv_path, tag_columns=["user_id"])
        tag_schema, packet_schema = src.output_schema()
        assert isinstance(tag_schema, Schema)
        assert isinstance(packet_schema, Schema)

    def test_source_id_explicit(self, csv_path):
        src = CSVSource(file_path=csv_path, source_id="my_csv_id")
        assert src.source_id == "my_csv_id"

    def test_same_source_id_yields_equivalent_source_fields(self, tmp_path):
        """Two CSV files at different paths with same source_id
        produce identical _source_ provenance columns."""
        data = "user_id,score\nu1,10\nu2,20\n"
        dir_a = tmp_path / "dir_a"
        dir_b = tmp_path / "dir_b"
        dir_a.mkdir()
        dir_b.mkdir()
        csv_a = dir_a / "data.csv"
        csv_b = dir_b / "data.csv"
        csv_a.write_text(data)
        csv_b.write_text(data)

        src_a = CSVSource(
            file_path=str(csv_a),
            tag_columns=["user_id"],
            source_id="shared_name",
        )
        src_b = CSVSource(
            file_path=str(csv_b),
            tag_columns=["user_id"],
            source_id="shared_name",
        )

        table_a = src_a.as_table(all_info=True)
        table_b = src_b.as_table(all_info=True)

        source_cols = [c for c in table_a.column_names if c.startswith("_source_")]
        assert source_cols, "Expected _source_ columns"
        for col in source_cols:
            assert table_a.column(col).to_pylist() == table_b.column(col).to_pylist()

    def test_as_table_returns_pyarrow_table(self, csv_path):
        src = CSVSource(file_path=csv_path)
        assert isinstance(src.as_table(), pa.Table)

    def test_nonexistent_record_id_column_raises(self, csv_path):
        with pytest.raises(ValueError, match="record_id_column"):
            CSVSource(file_path=csv_path, record_id_column="no_such_col")


# ---------------------------------------------------------------------------
# DeltaTableSource
# ---------------------------------------------------------------------------


class TestDeltaTableSource:
    def test_construction_reads_rows(self, delta_path):
        src = DeltaTableSource(delta_table_path=delta_path, tag_columns=["id"])
        assert len(list(src.iter_packets())) == 3

    def test_tag_and_packet_keys(self, delta_path):
        src = DeltaTableSource(delta_table_path=delta_path, tag_columns=["id"])
        tag_keys, packet_keys = src.keys()
        assert "id" in tag_keys
        assert "value" in packet_keys

    def test_source_id_defaults_to_directory_name(self, delta_path):
        src = DeltaTableSource(delta_table_path=delta_path)
        assert src.source_id == delta_path.name

    def test_source_id_explicit_overrides_default(self, delta_path):
        src = DeltaTableSource(delta_table_path=delta_path, source_id="my_delta")
        assert src.source_id == "my_delta"

    def test_resolve_field_row_index(self, delta_path):
        src = DeltaTableSource(delta_table_path=delta_path, tag_columns=["id"])
        # Delta read order may vary; we just check a valid row resolves.
        result = src.resolve_field("row_0", "id")
        assert isinstance(result, int)

    def test_resolve_field_column_value(self, delta_path):
        src = DeltaTableSource(
            delta_table_path=delta_path,
            tag_columns=["id"],
            record_id_column="id",
            source_id="delta_src",
        )
        result = src.resolve_field("id=1", "value")
        assert isinstance(result, str)

    def test_bad_path_raises_value_error(self, tmp_path):
        with pytest.raises(ValueError, match="Delta table not found"):
            DeltaTableSource(delta_table_path=tmp_path / "no_delta_here")

    def test_is_stream(self, delta_path):
        assert isinstance(DeltaTableSource(delta_table_path=delta_path), StreamProtocol)

    def test_is_root_source(self, delta_path):
        assert isinstance(DeltaTableSource(delta_table_path=delta_path), RootSource)

    def test_output_schema_returns_two_schemas(self, delta_path):
        src = DeltaTableSource(delta_table_path=delta_path, tag_columns=["id"])
        tag_schema, packet_schema = src.output_schema()
        assert isinstance(tag_schema, Schema)
        assert isinstance(packet_schema, Schema)

    def test_source_id_explicit(self, delta_path):
        src = DeltaTableSource(delta_table_path=delta_path, source_id="delta_id")
        assert src.source_id == "delta_id"

    def test_nonexistent_record_id_column_raises(self, delta_path):
        with pytest.raises(ValueError, match="record_id_column"):
            DeltaTableSource(delta_table_path=delta_path, record_id_column="no_col")


# ---------------------------------------------------------------------------
# DataFrameSource — additional coverage
# ---------------------------------------------------------------------------


class TestDataFrameSourceAdditional:
    def test_string_tag_columns_accepted(self):
        """tag_columns as a plain string (not a list) should work."""
        df = pl.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        src = DataFrameSource(data=df, tag_columns="id")
        tag_keys, packet_keys = src.keys()
        assert "id" in tag_keys
        assert "value" in packet_keys

    def test_resolve_field_raises_field_not_resolvable(self):
        """DataFrameSource does not override resolve_field; must raise."""
        df = pl.DataFrame({"id": [1, 2], "value": ["x", "y"]})
        src = DataFrameSource(data=df, tag_columns="id")
        with pytest.raises(FieldNotResolvableError):
            src.resolve_field("row_0", "value")

    def test_system_columns_stripped_from_polars_input(self):
        """Polars DataFrames with system-prefix columns have those columns dropped."""
        df = pl.DataFrame(
            {
                "x": [1, 2],
                "_tag_something": ["a", "b"],
            }
        )
        src = DataFrameSource(data=df)
        tag_keys, packet_keys = src.keys()
        assert "_tag_something" not in tag_keys
        assert "_tag_something" not in packet_keys

    def test_source_id_in_provenance_tokens(self):
        df = pl.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        src = DataFrameSource(data=df, tag_columns="id", source_id="df_source")
        table = src.as_table(all_info=True)
        source_cols = [c for c in table.column_names if c.startswith("_source_")]
        assert source_cols
        token = table.column(source_cols[0])[0].as_py()
        assert "df_source" in token

    def test_multiple_tag_columns(self):
        df = pl.DataFrame({"a": [1, 2], "b": [3, 4], "val": ["x", "y"]})
        src = DataFrameSource(data=df, tag_columns=["a", "b"])
        tag_keys, packet_keys = src.keys()
        assert set(tag_keys) == {"a", "b"}
        assert "val" in packet_keys

    def test_content_hash_same_data(self):
        df1 = pl.DataFrame({"x": [1, 2, 3]})
        df2 = pl.DataFrame({"x": [1, 2, 3]})
        src1 = DataFrameSource(data=df1)
        src2 = DataFrameSource(data=df2)
        assert src1.content_hash() == src2.content_hash()

    def test_content_hash_different_data(self):
        src1 = DataFrameSource(data=pl.DataFrame({"x": [1, 2]}))
        src2 = DataFrameSource(data=pl.DataFrame({"x": [3, 4]}))
        assert src1.content_hash() != src2.content_hash()


# ---------------------------------------------------------------------------
# DictSource — additional coverage
# ---------------------------------------------------------------------------


class TestDictSourceAdditional:
    def test_data_schema_explicit(self):
        """data_schema constrains the Arrow schema produced from dicts."""
        data = [{"id": 1, "value": "hello"}, {"id": 2, "value": "world"}]
        src = DictSource(
            data=data,
            tag_columns=["id"],
            data_schema={"id": int, "value": str},
        )
        tag_schema, packet_schema = src.output_schema()
        assert "id" in tag_schema
        assert "value" in packet_schema

    def test_empty_data_raises(self):
        """An empty DictSource cannot build a valid ArrowTableStream."""
        with pytest.raises(Exception):
            DictSource(data=[], tag_columns=["id"])

    def test_source_id_in_provenance_tokens(self):
        data = [{"id": 1, "val": "a"}, {"id": 2, "val": "b"}]
        src = DictSource(data=data, tag_columns=["id"], source_id="dict_src_name")
        table = src.as_table(all_info=True)
        source_cols = [c for c in table.column_names if c.startswith("_source_")]
        assert source_cols
        token = table.column(source_cols[0])[0].as_py()
        assert "dict_src_name" in token

    def test_source_id_explicit(self):
        data = [{"id": 1, "val": "x"}]
        src = DictSource(data=data, tag_columns=["id"], source_id="my_dict")
        assert src.source_id == "my_dict"

    def test_resolve_field_error_mentions_source_id(self):
        data = [{"id": 1, "val": "a"}]
        src = DictSource(data=data, tag_columns=["id"], source_id="named_dict")
        with pytest.raises(FieldNotResolvableError, match="named_dict"):
            src.resolve_field("row_0", "val")


# ---------------------------------------------------------------------------
# ListSource — additional coverage
# ---------------------------------------------------------------------------


def _tag_fn_for_signature(element, idx):
    """Top-level tag function so inspect.getsource works."""
    return {"label": f"item_{idx}"}


def _tag_fn_for_content(element, idx):
    """Top-level tag function for content hash mode."""
    return {"bucket": idx % 2}


class TestListSourceAdditional:
    def test_tag_function_hash_mode_signature(self):
        """Two ListSources with the same tag function and 'signature' mode share hash."""
        src1 = ListSource(
            name="val",
            data=[1, 2, 3],
            tag_function=_tag_fn_for_signature,
            expected_tag_keys=["label"],
            tag_function_hash_mode="signature",
        )
        src2 = ListSource(
            name="val",
            data=[1, 2, 3],
            tag_function=_tag_fn_for_signature,
            expected_tag_keys=["label"],
            tag_function_hash_mode="signature",
        )
        assert src1.content_hash() == src2.content_hash()

    def test_tag_function_hash_mode_content(self):
        """'content' mode hashes the function source code."""
        src = ListSource(
            name="val",
            data=[1, 2, 3],
            tag_function=_tag_fn_for_content,
            expected_tag_keys=["bucket"],
            tag_function_hash_mode="content",
        )
        # Identity structure should include a non-empty hash
        identity = src.identity_structure()
        assert isinstance(identity[3], str)
        assert len(identity[3]) > 0

    def test_tag_function_hash_mode_name(self):
        """'name' mode uses the qualified name of the function."""
        src = ListSource(
            name="val",
            data=[1, 2, 3],
            tag_function=_tag_fn_for_signature,
            expected_tag_keys=["label"],
            tag_function_hash_mode="name",
        )
        assert _tag_fn_for_signature.__qualname__ in src._tag_function_hash

    def test_empty_list_raises(self):
        """An empty ListSource cannot build a valid stream."""
        with pytest.raises(Exception):
            ListSource(name="item", data=[])

    def test_tag_keys_inferred_from_first_row(self):
        """When expected_tag_keys is None with a custom tag function, keys are
        inferred from the first row."""

        def tag_fn(el, idx):
            return {"group": el % 3}

        src = ListSource(name="val", data=[0, 1, 2], tag_function=tag_fn)
        tag_keys, packet_keys = src.keys()
        assert "group" in tag_keys
        assert "val" in packet_keys

    def test_tag_as_dict_protocol(self):
        """If the tag function returns an object with .as_dict(), it is unwrapped."""

        class FakeTag:
            def __init__(self, d):
                self._d = d

            def as_dict(self):
                return self._d

        def tag_fn(el, idx):
            return FakeTag({"slot": idx})

        src = ListSource(
            name="item",
            data=["x", "y", "z"],
            tag_function=tag_fn,
            expected_tag_keys=["slot"],
        )
        pairs = list(src.iter_packets())
        slots = {tag["slot"] for tag, _ in pairs}
        assert slots == {0, 1, 2}

    def test_identity_structure_contains_name_and_elements(self):
        src = ListSource(name="item", data=["a", "b"])
        identity = src.identity_structure()
        assert identity[0] == "ListSource"
        assert identity[1] == "item"
        assert "a" in identity[2]
        assert "b" in identity[2]

    def test_same_data_same_content_hash(self):
        src1 = ListSource(name="x", data=[1, 2, 3])
        src2 = ListSource(name="x", data=[1, 2, 3])
        assert src1.content_hash() == src2.content_hash()

    def test_different_name_different_content_hash(self):
        src1 = ListSource(name="x", data=[1, 2, 3])
        src2 = ListSource(name="y", data=[1, 2, 3])
        assert src1.content_hash() != src2.content_hash()

    def test_different_data_different_content_hash(self):
        src1 = ListSource(name="x", data=[1, 2, 3])
        src2 = ListSource(name="x", data=[4, 5, 6])
        assert src1.content_hash() != src2.content_hash()


# ---------------------------------------------------------------------------
# ArrowTableSource — additional coverage
# ---------------------------------------------------------------------------


class TestArrowTableSourceAdditional:
    def test_table_property_returns_enriched_table(self):
        """The .table property returns the internal PA table including system cols."""
        table = pa.table({"x": pa.array([1, 2], type=pa.int64())})
        src = ArrowTableSource(table=table)
        enriched = src.table
        assert isinstance(enriched, pa.Table)
        # The enriched table includes source-info and system-tag columns
        assert any(c.startswith("_source_") for c in enriched.column_names)

    def test_source_id_controls_provenance_tokens(self):
        """source_id appears in both provenance tokens and registry key."""
        table = _simple_table()
        src = ArrowTableSource(
            table=table,
            tag_columns=["user_id"],
            source_id="my_source",
        )
        assert src.source_id == "my_source"
        t = src.as_table(all_info=True)
        source_cols = [c for c in t.column_names if c.startswith("_source_")]
        token = t.column(source_cols[0])[0].as_py()
        assert token.startswith("my_source::")

    def test_negative_row_index_raises(self):
        """row_-1 parses as -1 which is out of range."""
        table = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
        src = ArrowTableSource(table=table)
        with pytest.raises(FieldNotResolvableError):
            src.resolve_field("row_-1", "x")

    def test_duplicate_record_id_takes_first_match(self):
        """When multiple rows share a record_id value, resolve_field returns first."""
        table = pa.table(
            {
                "id": pa.array(["a", "a", "b"], type=pa.large_string()),
                "val": pa.array([1, 2, 3], type=pa.int64()),
            }
        )
        src = ArrowTableSource(table=table, tag_columns=["id"], record_id_column="id")
        assert src.resolve_field("id=a", "val") == 1

    def test_integer_record_id_column(self):
        """record_id_column holding integer values: token format is col=<str(val)>."""
        table = pa.table(
            {
                "row_key": pa.array([10, 20, 30], type=pa.int64()),
                "data": pa.array(["x", "y", "z"], type=pa.large_string()),
            }
        )
        src = ArrowTableSource(
            table=table, tag_columns=["row_key"], record_id_column="row_key"
        )
        assert src.resolve_field("row_key=20", "data") == "y"

    def test_system_tag_columns_forwarded_to_stream(self):
        """system_tag_columns passed at construction are preserved."""
        table = pa.table({"x": pa.array([1, 2], type=pa.int64())})
        src = ArrowTableSource(table=table, system_tag_columns=["sys_col"])
        assert "sys_col" in src._system_tag_columns

    def test_as_table_all_info_includes_system_tag_columns(self):
        """as_table(all_info=True) exposes paired _tag_source_id and _tag_record_id columns."""
        from orcapod.system_constants import constants

        table = pa.table({"x": pa.array([1, 2], type=pa.int64())})
        src = ArrowTableSource(table=table)
        enriched = src.as_table(all_info=True)
        assert any(
            c.startswith(constants.SYSTEM_TAG_SOURCE_ID_PREFIX)
            for c in enriched.column_names
        )
        assert any(
            c.startswith(constants.SYSTEM_TAG_RECORD_ID_PREFIX)
            for c in enriched.column_names
        )

    def test_resolve_field_on_empty_record_id_prefix_raises(self):
        """An empty string record_id raises FieldNotResolvableError."""
        table = pa.table({"x": pa.array([1, 2], type=pa.int64())})
        src = ArrowTableSource(table=table)
        with pytest.raises(FieldNotResolvableError):
            src.resolve_field("", "x")

    def test_tag_columns_not_present_in_table_raises(self):
        """tag_columns that don't exist in the table raise ValueError."""
        table = pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "val": pa.array([42], type=pa.int64()),
            }
        )
        with pytest.raises(ValueError, match="tag_columns not found in table"):
            ArrowTableSource(table=table, tag_columns=["nonexistent", "id"])

    def test_tag_columns_all_missing_raises(self):
        """All tag_columns missing from the table raises ValueError."""
        table = pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "val": pa.array([42], type=pa.int64()),
            }
        )
        with pytest.raises(ValueError, match="tag_columns not found in table"):
            ArrowTableSource(table=table, tag_columns=["foo", "bar"])

    def test_tag_columns_all_valid_succeeds(self):
        """tag_columns that all exist in the table work correctly."""
        table = pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "val": pa.array([42], type=pa.int64()),
            }
        )
        src = ArrowTableSource(table=table, tag_columns=["id"])
        tag_keys, packet_keys = src.keys()
        assert "id" in tag_keys
        assert "val" in packet_keys


# ---------------------------------------------------------------------------
# SourceRegistry — additional coverage
# ---------------------------------------------------------------------------


def _make_src(source_id: str | None = None) -> ArrowTableSource:
    return ArrowTableSource(
        table=pa.table({"x": pa.array([1], type=pa.int64())}),
        source_id=source_id,
    )


class TestSourceRegistryAdditional:
    def setup_method(self):
        self.registry = SourceRegistry()

    def test_replace_returns_none_when_no_prior_entry(self):
        src = _make_src("s1")
        result = self.registry.replace("s1", src)
        assert result is None

    def test_replace_with_empty_source_id_raises(self):
        with pytest.raises(ValueError):
            self.registry.replace("", _make_src())

    def test_register_with_none_source_raises(self):
        with pytest.raises(ValueError):
            self.registry.register("s1", None)  # type: ignore[arg-type]

    def test_repr_contains_count_and_ids(self):
        src = _make_src("s1")
        self.registry.register("s1", src)
        r = repr(self.registry)
        assert "1" in r
        assert "s1" in r

    def test_repr_empty_registry(self):
        r = repr(self.registry)
        assert "0" in r

    def test_replace_same_object_returns_it(self):
        src = _make_src("s1")
        self.registry.register("s1", src)
        old = self.registry.replace("s1", src)
        assert old is src
        assert self.registry.get("s1") is src

    def test_multiple_sources_list_ids_order(self):
        """list_ids preserves insertion order."""
        for name in ["alpha", "beta", "gamma"]:
            self.registry.register(name, _make_src(name))
        assert self.registry.list_ids() == ["alpha", "beta", "gamma"]

    def test_clear_empties_registry(self):
        self.registry.register("s1", _make_src("s1"))
        self.registry.register("s2", _make_src("s2"))
        self.registry.clear()
        assert len(self.registry) == 0
        assert list(self.registry) == []

    def test_get_optional_returns_source_when_present(self):
        src = _make_src("s1")
        self.registry.register("s1", src)
        assert self.registry.get_optional("s1") is src

    def test_items_yields_all_pairs(self):
        srcs = {name: _make_src(name) for name in ["a", "b", "c"]}
        for name, src in srcs.items():
            self.registry.register(name, src)
        pairs = dict(self.registry.items())
        assert pairs == srcs
