"""Tests for the GroupBy operator — many->one reduction keyed on tag values."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.operators import GroupBy
from orcapod.core.sources import ArrowTableSource
from orcapod.errors import InputValidationError
from orcapod.system_constants import constants


@pytest.fixture
def session_table() -> pa.Table:
    """Two sessions x two probes: the common-clock shape from NPIPE-204."""
    return pa.table({
        "subject": ["G", "G", "G", "G"],
        "date": ["d1", "d1", "d2", "d2"],
        "probe": [1, 0, 1, 0],
        "path": ["b", "a", "d", "c"],
    })


@pytest.fixture
def session_source(session_table) -> ArrowTableSource:
    return ArrowTableSource(
        session_table,
        tag_columns=["subject", "date", "probe"],
        infer_nullable=True,
    )


class TestGroupByShape:
    def test_one_row_per_distinct_key(self, session_source):
        out = GroupBy(by=["subject", "date"]).process(session_source)
        assert len(out.as_table()) == 2

    def test_group_keys_are_scalar_tags(self, session_source):
        op = GroupBy(by=["subject", "date"])
        out = op.process(session_source)
        tag_cols, _ = out.keys()
        assert set(tag_cols) == {"subject", "date"}
        assert out.as_table().column("subject").to_pylist() == ["G", "G"]

    def test_non_key_tags_promoted_to_list_data(self, session_source):
        out = GroupBy(by=["subject", "date"]).process(session_source)
        _, data_cols = out.keys()
        assert "probe" in data_cols
        assert out.as_table().column("probe").to_pylist() == [[0, 1], [0, 1]]

    def test_data_columns_are_lists(self, session_source):
        out = GroupBy(by=["subject", "date"]).process(session_source)
        assert out.as_table().column("path").to_pylist() == [["a", "b"], ["c", "d"]]

    def test_source_columns_are_lists(self, session_source):
        out = GroupBy(by=["subject", "date"]).process(session_source)
        table = out.as_table(columns={"source": True})
        assert len(table.column(f"{constants.SOURCE_PREFIX}path").to_pylist()[0]) == 2

    def test_system_tags_are_scalar_and_renamed(self, session_source):
        out = GroupBy(by=["subject", "date"]).process(session_source)
        table = out.as_table(columns={"system_tags": True})
        sys_cols = [
            c for c in table.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
        ]
        assert sys_cols
        for col in sys_cols:
            field_type = table.schema.field(col).type
            assert not pa.types.is_list(field_type)
            assert not pa.types.is_large_list(field_type)
            # name-extended: original "::<schema_hash>" plus "::<pipeline_hash>"
            assert col.count(constants.BLOCK_SEPARATOR) >= 2


class TestGroupByOrdering:
    def test_members_sorted_by_non_key_tags(self, session_source):
        """probe=[1,0] on input must emit as [0,1]."""
        out = GroupBy(by=["subject", "date"]).process(session_source)
        assert out.as_table().column("probe").to_pylist()[0] == [0, 1]

    def test_row_order_does_not_affect_output(self, session_table):
        """Same rows, shuffled, must produce a byte-identical table."""
        shuffled = session_table.take([3, 1, 2, 0])

        def run(tbl):
            src = ArrowTableSource(
                tbl, tag_columns=["subject", "date", "probe"], infer_nullable=True
            )
            return GroupBy(by=["subject", "date"]).process(src).as_table()

        assert run(session_table).equals(run(shuffled))

    def test_falls_back_to_record_id_when_key_covers_all_tags(self):
        """by covering every tag leaves no non-key tag to sort on."""
        table = pa.table({"subject": ["G", "G"], "path": ["b", "a"]})
        src = ArrowTableSource(table, tag_columns=["subject"], infer_nullable=True)
        out = GroupBy(by=["subject"]).process(src)
        assert len(out.as_table()) == 1
        assert sorted(out.as_table().column("path").to_pylist()[0]) == ["a", "b"]


class TestGroupByValidation:
    def test_empty_by_raises(self):
        with pytest.raises(ValueError, match="at least one"):
            GroupBy(by=[])

    def test_unknown_column_raises(self, session_source):
        op = GroupBy(by=["subject", "nonexistent"])
        with pytest.raises(InputValidationError, match="nonexistent"):
            op.process(session_source)

    def test_data_column_as_key_raises(self, session_source):
        """Grouping on a data column is not allowed -- keys must be tags."""
        op = GroupBy(by=["path"])
        with pytest.raises(InputValidationError, match="path"):
            op.process(session_source)


class TestGroupByEmptyInput:
    def test_empty_input_yields_zero_groups(self):
        table = pa.table({
            "subject": pa.array([], pa.large_string()),
            "path": pa.array([], pa.large_string()),
        })
        from orcapod.core.streams import ArrowTableStream

        stream = ArrowTableStream(table, tag_columns=["subject"])
        out = GroupBy(by=["subject"]).process(stream)
        assert len(out.as_table()) == 0


class TestGroupByIdentity:
    def test_identity_structure_includes_by(self):
        assert (
            GroupBy(by=["a"]).identity_structure()
            != GroupBy(by=["b"]).identity_structure()
        )

    def test_to_config_round_trip(self):
        op = GroupBy(by=["subject", "date"])
        config = op.to_config()
        # A list, not a tuple, so the config stays JSON-serializable.
        assert config["config"]["by"] == ["subject", "date"]
        rebuilt = GroupBy.from_config(config)
        assert rebuilt.identity_structure() == op.identity_structure()

    def test_to_config_is_json_serializable(self):
        import json

        json.dumps(GroupBy(by=["subject", "date"]).to_config()["config"])
