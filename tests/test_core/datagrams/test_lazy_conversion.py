"""
Tests verifying that Datagram/Tag/Packet keep their original representation
(Arrow table or Python dict) for as long as possible, converting only when
an operation semantically requires it.

Design note
-----------
These tests intentionally inspect private attributes (_data_dict, _data_table,
_system_tags_table, _source_info_table, _content_hash_cache, etc.) because the
lazy-conversion contract is an explicit implementation guarantee — it is the
entire point of the unified Datagram class.  Checking public behaviour alone
would not distinguish "converted correctly" from "never converted at all".
"""

import pyarrow as pa
import pytest

from orcapod.core.datagrams import Datagram, Packet, Tag
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SYS = constants.SYSTEM_TAG_PREFIX  # '_tag::'
SRC = constants.SOURCE_PREFIX  # '_source_'
META = constants.META_PREFIX  # '__'


def arrow_table(**cols) -> pa.Table:
    """Single-row Arrow table from keyword arguments."""
    return pa.table({k: pa.array([v]) for k, v in cols.items()})


# ---------------------------------------------------------------------------
# Datagram — dict-backed
# ---------------------------------------------------------------------------


class TestDatagramDictBacked:
    """Arrow table is NOT computed until an Arrow-requiring operation is called."""

    def test_initial_state(self):
        d = Datagram({"a": 1, "b": 2})
        assert d._data_dict is not None
        assert d._data_table is None

    # Operations that must NOT trigger Arrow conversion
    @pytest.mark.parametrize(
        "op",
        [
            lambda d: d.as_dict(),
            lambda d: d["a"],
            lambda d: d.get("a"),
            lambda d: ("a" in d),
            lambda d: list(d),
            lambda d: d.keys(),
            lambda d: d.schema(),
        ],
        ids=["as_dict", "getitem", "get", "contains", "iter", "keys", "schema"],
    )
    def test_value_access_does_not_load_table(self, op):
        d = Datagram({"a": 1, "b": 2})
        op(d)
        assert d._data_table is None

    # Structural operations on dict-backed must stay dict-backed
    def test_select_stays_dict_backed(self):
        d = Datagram({"a": 1, "b": 2})
        d2 = d.select("a")
        assert d2._data_table is None
        assert d2._data_dict == {"a": 1}

    def test_drop_stays_dict_backed(self):
        d = Datagram({"a": 1, "b": 2})
        d2 = d.drop("b")
        assert d2._data_table is None
        assert d2._data_dict == {"a": 1}

    def test_rename_stays_dict_backed(self):
        d = Datagram({"a": 1, "b": 2})
        d2 = d.rename({"a": "x"})
        assert d2._data_table is None
        assert d2._data_dict == {"x": 1, "b": 2}

    def test_update_stays_dict_backed(self):
        d = Datagram({"a": 1, "b": 2})
        d2 = d.update(a=99)
        assert d2._data_table is None
        assert d2._data_dict == {"a": 99, "b": 2}

    def test_with_columns_stays_dict_backed(self):
        d = Datagram({"a": 1})
        d2 = d.with_columns(b=2)
        assert d2._data_table is None
        assert d2._data_dict == {"a": 1, "b": 2}

    # Operations that MUST trigger Arrow conversion
    def test_as_table_loads_table(self):
        d = Datagram({"a": 1})
        assert d._data_table is None
        _ = d.as_table()
        assert d._data_table is not None

    def test_content_hash_loads_table(self):
        d = Datagram({"a": 1})
        assert d._data_table is None
        _ = d.content_hash()
        assert d._data_table is not None

    def test_arrow_schema_loads_arrow_schema_not_table(self):
        # arrow_schema() computes _data_arrow_schema from _data_python_schema;
        # it does NOT need to build _data_table.
        d = Datagram({"a": 1})
        _ = d.arrow_schema()
        assert d._data_table is None
        assert d._data_arrow_schema is not None

    def test_table_loaded_dict_still_present(self):
        d = Datagram({"a": 1})
        _ = d.as_table()
        assert d._data_dict is not None
        assert d._data_table is not None


# ---------------------------------------------------------------------------
# Datagram — Arrow-backed
# ---------------------------------------------------------------------------


class TestDatagramArrowBacked:
    """Python dict is NOT computed until a value-access operation is called."""

    def test_initial_state(self):
        d = Datagram(arrow_table(a=1, b=2))
        assert d._data_table is not None
        assert d._data_dict is None

    # Operations that must NOT trigger dict conversion
    @pytest.mark.parametrize(
        "op",
        [
            lambda d: ("a" in d),
            lambda d: list(d),
            lambda d: d.keys(),
            lambda d: d.schema(),
            lambda d: d.arrow_schema(),
            lambda d: d.as_table(),
            lambda d: d.content_hash(),
        ],
        ids=[
            "contains",
            "iter",
            "keys",
            "schema",
            "arrow_schema",
            "as_table",
            "content_hash",
        ],
    )
    def test_structural_ops_do_not_load_dict(self, op):
        d = Datagram(arrow_table(a=1, b=2))
        op(d)
        assert d._data_dict is None

    # Structural operations on Arrow-backed must stay Arrow-native
    def test_select_stays_arrow_backed(self):
        d = Datagram(arrow_table(a=1, b=2))
        d2 = d.select("a")
        assert d2._data_dict is None
        assert d2._data_table is not None
        assert d2._data_table.column_names == ["a"]

    def test_drop_stays_arrow_backed(self):
        d = Datagram(arrow_table(a=1, b=2))
        d2 = d.drop("b")
        assert d2._data_dict is None

        assert d2._data_table is not None
        assert d2._data_table.column_names == ["a"]

    def test_rename_stays_arrow_backed(self):
        d = Datagram(arrow_table(a=1, b=2))
        d2 = d.rename({"a": "x"})
        assert d2._data_dict is None
        assert d2._data_table is not None
        assert "x" in d2._data_table.column_names
        assert "a" not in d2._data_table.column_names

    def test_update_without_dict_stays_arrow_backed(self):
        """update() when dict is NOT loaded uses Arrow-native path."""
        d = Datagram(arrow_table(a=1, b=2))
        assert d._data_dict is None
        d2 = d.update(a=99)
        assert d2._data_dict is None
        assert d2._data_table is not None

    def test_with_columns_without_dict_stays_arrow_backed(self):
        """with_columns() when dict is NOT loaded uses Arrow-native path."""
        d = Datagram(arrow_table(a=1))
        assert d._data_dict is None
        d2 = d.with_columns(b=2)
        assert d2._data_dict is None
        assert d2._data_table is not None

    # Operations that MUST trigger dict conversion
    def test_getitem_loads_dict(self):
        d = Datagram(arrow_table(a=1, b=2))
        assert d["a"] == 1
        assert d._data_dict is not None

    def test_get_loads_dict(self):
        d = Datagram(arrow_table(a=1, b=2))
        assert d.get("a") == 1
        assert d._data_dict is not None

    def test_as_dict_loads_dict(self):
        d = Datagram(arrow_table(a=1, b=2))
        result = d.as_dict()
        assert result["a"] == 1
        assert d._data_dict is not None

    def test_dict_loaded_table_still_present(self):
        d = Datagram(arrow_table(a=1))
        _ = d.as_dict()
        assert d._data_table is not None
        assert d._data_dict is not None

    def test_update_after_dict_load_invalidates_table(self):
        """Once the dict is loaded, update() goes through the dict path and
        sets _data_table=None so the old Arrow table is not silently reused."""
        d = Datagram(arrow_table(a=1, b=2))
        _ = d.as_dict()  # force dict load
        d2 = d.update(a=99)
        assert d2._data_dict is not None
        assert d2._data_table is None


# ---------------------------------------------------------------------------
# Datagram — copy() and cache propagation
# ---------------------------------------------------------------------------


class TestDatagramCopy:
    def test_copy_propagates_both_representations_when_both_loaded(self):
        d = Datagram(arrow_table(a=1))
        _ = d.as_dict()  # load dict too
        d2 = d.copy()
        assert d2._data_table is not None
        assert d2._data_dict is not None

    def test_copy_preserves_arrow_only(self):
        d = Datagram(arrow_table(a=1))
        d2 = d.copy()
        assert d2._data_dict is None
        assert d2._data_table is not None

    def test_copy_preserves_dict_only(self):
        d = Datagram({"a": 1})
        d2 = d.copy()
        assert d2._data_dict is not None
        assert d2._data_table is None

    def test_copy_without_cache_drops_content_hash(self):
        d = Datagram({"a": 1})
        _ = d.content_hash()
        assert d._content_hash_cache  # non-empty after hashing
        d2 = d.copy(include_cache=False)
        assert not d2._content_hash_cache  # cleared on copy without cache

    def test_copy_with_cache_keeps_content_hash(self):
        d = Datagram({"a": 1})
        _ = d.content_hash()
        d2 = d.copy(include_cache=True)
        assert d2._content_hash_cache  # preserved on copy with cache

    def test_copy_without_cache_drops_meta_table(self):
        d = Datagram({"a": 1, f"{META}info": "v1"})
        _ = d.as_table(all_info=True)  # builds meta table
        assert d._meta_table is not None
        d2 = d.copy(include_cache=False)
        assert d2._meta_table is None

    def test_copy_without_cache_drops_context_table(self):
        d = Datagram({"a": 1})
        _ = d.as_table(all_info=True)  # builds context table
        assert d._context_table is not None
        d2 = d.copy(include_cache=False)
        assert d2._context_table is None


# ---------------------------------------------------------------------------
# Tag — lazy system-tags table
# ---------------------------------------------------------------------------


class TestTagLazySystemTagsTable:
    """_system_tags_table is built only when system_tags are explicitly requested."""

    def test_dict_backed_starts_with_no_system_tags_table(self):
        t = Tag({"a": 1, f"{SYS}run": "run1"})
        assert t._system_tags_table is None

    def test_arrow_backed_system_tag_columns_extracted_from_data_table(self):
        sys_col = f"{SYS}run"
        tbl = arrow_table(a=1)
        tbl = tbl.append_column(sys_col, pa.array(["run1"], type=pa.large_string()))
        t = Tag(tbl)
        # System tag column removed from primary data table
        assert t._data_table is not None
        assert sys_col not in t._data_table.column_names
        # Captured in the system_tags dict
        assert t._system_tags[sys_col] == "run1"
        # Table not yet built
        assert t._system_tags_table is None

    def test_system_tags_table_not_built_without_system_tags_flag(self):
        t = Tag({"a": 1, f"{SYS}run": "run1"})
        _ = t.as_table()
        assert t._system_tags_table is None
        _ = t.as_dict()
        assert t._system_tags_table is None
        _ = t.keys()
        assert t._system_tags_table is None

    def test_system_tags_table_built_when_requested_via_as_table(self):
        t = Tag({"a": 1, f"{SYS}run": "run1"})
        _ = t.as_table(columns=ColumnConfig(system_tags=True))
        assert t._system_tags_table is not None

    def test_system_tags_table_built_when_requested_via_arrow_schema(self):
        t = Tag({"a": 1, f"{SYS}run": "run1"})
        _ = t.arrow_schema(columns=ColumnConfig(system_tags=True))
        assert t._system_tags_table is not None

    def test_arrow_backed_dict_not_loaded_by_system_tags_operations(self):
        sys_col = f"{SYS}run"
        tbl = arrow_table(a=1)
        tbl = tbl.append_column(sys_col, pa.array(["run1"], type=pa.large_string()))
        t = Tag(tbl)
        assert t._data_dict is None
        _ = t.keys(columns=ColumnConfig(system_tags=True))
        _ = t.schema(columns=ColumnConfig(system_tags=True))
        _ = t.arrow_schema(columns=ColumnConfig(system_tags=True))
        assert t._data_dict is None

    def test_copy_with_cache_propagates_system_tags_table(self):
        t = Tag({"a": 1, f"{SYS}run": "run1"})
        _ = t.as_table(columns=ColumnConfig(system_tags=True))
        t2 = t.copy(include_cache=True)
        assert t2._system_tags_table is not None

    def test_copy_without_cache_drops_system_tags_table(self):
        t = Tag({"a": 1, f"{SYS}run": "run1"})
        _ = t.as_table(columns=ColumnConfig(system_tags=True))
        t2 = t.copy(include_cache=False)
        assert t2._system_tags_table is None


# ---------------------------------------------------------------------------
# Packet — lazy source-info table
# ---------------------------------------------------------------------------


class TestPacketLazySourceInfoTable:
    """_source_info_table is built only when source info is explicitly requested."""

    def test_dict_backed_starts_with_no_source_info_table(self):
        p = Packet({"a": 1}, source_info={"a": "s::r::a"})
        assert p._source_info_table is None

    def test_arrow_backed_starts_with_no_source_info_table(self):
        p = Packet(arrow_table(a=1), source_info={"a": "s::r::a"})
        assert p._source_info_table is None

    def test_source_info_table_not_built_without_source_flag(self):
        p = Packet({"a": 1}, source_info={"a": "s::r::a"})
        _ = p.as_table()
        assert p._source_info_table is None
        _ = p.as_dict()
        assert p._source_info_table is None
        _ = p.keys()
        assert p._source_info_table is None

    def test_source_info_table_built_when_requested_via_as_table(self):
        p = Packet({"a": 1}, source_info={"a": "s::r::a"})
        _ = p.as_table(columns=ColumnConfig(source=True))
        assert p._source_info_table is not None

    def test_source_info_table_built_when_requested_via_arrow_schema(self):
        p = Packet({"a": 1}, source_info={"a": "s::r::a"})
        _ = p.arrow_schema(columns=ColumnConfig(source=True))
        assert p._source_info_table is not None

    def test_arrow_schema_without_source_does_not_build_table(self):
        p = Packet({"a": 1}, source_info={"a": "s::r::a"})
        _ = p.arrow_schema()
        assert p._source_info_table is None

    def test_arrow_backed_dict_not_loaded_by_as_table_with_source(self):
        p = Packet(arrow_table(a=1), source_info={"a": "s::r::a"})
        _ = p.as_table(columns=ColumnConfig(source=True))
        assert p._data_dict is None

    def test_copy_with_cache_propagates_source_info_table(self):
        p = Packet({"a": 1}, source_info={"a": "s::r::a"})
        _ = p.as_table(columns=ColumnConfig(source=True))
        p2 = p.copy(include_cache=True)
        assert p2._source_info_table is not None

    def test_copy_without_cache_drops_source_info_table(self):
        p = Packet({"a": 1}, source_info={"a": "s::r::a"})
        _ = p.as_table(columns=ColumnConfig(source=True))
        p2 = p.copy(include_cache=False)
        assert p2._source_info_table is None

    def test_rename_clears_source_info_table_and_updates_keys(self):
        p = Packet({"a": 1, "b": 2}, source_info={"a": "s1", "b": "s2"})
        _ = p.as_table(columns=ColumnConfig(source=True))  # build table
        p2 = p.rename({"a": "x"})
        # Table must be invalidated — keys changed
        assert p2._source_info_table is None
        # Source info dict updated to reflect rename
        assert p2._source_info == {"x": "s1", "b": "s2"}

    def test_with_columns_clears_source_info_table_and_adds_empty_entry(self):
        p = Packet({"a": 1}, source_info={"a": "s1"})
        _ = p.as_table(columns=ColumnConfig(source=True))
        p2 = p.with_columns(b=2)
        assert p2._source_info_table is None
        # New column gets None source info
        assert "b" in p2._source_info
        assert p2._source_info["b"] is None


# ---------------------------------------------------------------------------
# RecordBatch — both Tag and Packet accept pa.RecordBatch (from table.to_batches())
# ---------------------------------------------------------------------------


class TestRecordBatchInput:
    """Constructors accept pa.RecordBatch (as produced by Table.to_batches() / .slice())."""

    def test_datagram_from_record_batch(self):
        tbl = arrow_table(a=1, b=2)
        batch = tbl.to_batches()[0]
        d = Datagram(batch.slice(0, 1))
        assert d._data_table is not None
        assert d._data_dict is None
        assert d["a"] == 1

    def test_tag_from_record_batch(self):
        sys_col = f"{SYS}run"
        tbl = arrow_table(a=1)
        tbl = tbl.append_column(sys_col, pa.array(["r1"], type=pa.large_string()))
        batch = tbl.to_batches()[0]
        t = Tag(batch.slice(0, 1))
        assert t._data_table is not None
        assert sys_col not in t._data_table.column_names
        assert t._system_tags[sys_col] == "r1"
        assert t._data_dict is None

    def test_packet_from_record_batch(self):
        tbl = arrow_table(a=1, b=2)
        batch = tbl.to_batches()[0]
        p = Packet(batch.slice(0, 1), source_info={"a": "s1", "b": "s2"})
        assert p._data_table is not None
        assert p._data_dict is None
        assert p._source_info == {"a": "s1", "b": "s2"}


class TestDatagramNullablePreservation:
    """Datagram always respects Arrow nullable flags.

    nullable=True  → T | None in schema()
    nullable=False → T       in schema()

    Callers must set nullable correctly before passing a table to
    SourceStreamBuilder — use ``arrow_utils.infer_schema_nullable(table)``
    to derive flags from data when no explicit schema is available.
    """

    def test_nullable_true_fields_yield_optional_in_schema(self):
        """nullable=True fields → T | None in schema()."""
        tbl = pa.table({"val": pa.array([42], type=pa.int64())})
        assert tbl.schema.field("val").nullable is True

        d = Datagram(tbl)
        assert d.schema()["val"] == int | None

    def test_non_nullable_fields_yield_plain_type_in_schema(self):
        """nullable=False fields → plain T in schema()."""
        tbl = pa.table(
            {"val": pa.array([1], type=pa.int64())},
            schema=pa.schema([pa.field("val", pa.int64(), nullable=False)]),
        )
        assert tbl.schema.field("val").nullable is False

        d = Datagram(tbl)
        assert d.schema()["val"] is int
