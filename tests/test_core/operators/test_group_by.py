"""Tests for the GroupBy operator — many->one reduction keyed on tag values."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.operators import Batch, GroupBy
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
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
        """by covering every tag leaves no non-key tag to sort on.

        record_id is a uuid5 of source content, so the order it imposes is not
        human-predictable but IS stable across processes.  Asserted exactly --
        sorting the result before comparing would make this blind to the very
        ordering it exists to pin down.
        """
        table = pa.table({"subject": ["G", "G"], "path": ["b", "a"]})
        src = ArrowTableSource(table, tag_columns=["subject"], infer_nullable=True)
        out = GroupBy(by=["subject"]).process(src)
        assert len(out.as_table()) == 1
        assert out.as_table().column("path").to_pylist()[0] == ["a", "b"]

    def test_record_id_breaks_ties_between_duplicate_tags(self):
        """Duplicate tag tuples must not fall back to raw emission order."""
        table = pa.table({"s": ["G", "G"], "p": [1, 1], "path": ["z", "a"]})
        src = ArrowTableSource(table, tag_columns=["s", "p"], infer_nullable=True)
        out = GroupBy(by=["s"]).process(src)
        # Emission order would give ["z", "a"]; record_id imposes ["a", "z"].
        assert out.as_table().column("path").to_pylist()[0] == ["a", "z"]


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

    def test_duplicate_by_raises(self):
        with pytest.raises(ValueError, match="duplicate"):
            GroupBy(by=["subject", "subject"])

    def test_list_valued_tag_as_key_raises(self):
        """Batch list-wraps its tags; those are unhashable and cannot key a group.

        Without an explicit check this surfaced as a bare
        ``TypeError: unhashable type: 'list'`` naming neither operator nor column.
        """
        table = pa.table({"s": ["G", "G"], "v": [1, 2]})
        src = ArrowTableSource(table, tag_columns=["s"], infer_nullable=True)
        batched = Batch(batch_size=2).process(src)

        with pytest.raises(InputValidationError, match="list-valued"):
            GroupBy(by=["s"]).process(batched)


class TestGroupByEmptyInput:
    def test_empty_input_yields_zero_groups(self):
        table = pa.table({
            "subject": pa.array([], pa.large_string()),
            "path": pa.array([], pa.large_string()),
        })
        stream = ArrowTableStream(table, tag_columns=["subject"])
        out = GroupBy(by=["subject"]).process(stream)
        assert len(out.as_table()) == 0


class TestGroupByOutputSchema:
    """The predicted schema must match what grouping actually produces.

    Compared against the materialized ``ArrowTableStream`` from
    ``unary_static_process``, never against ``process(...)``: a
    ``DynamicPodStream.output_schema`` delegates straight back to the pod, so
    that comparison is circular and passes no matter how wrong the prediction is.
    """

    @pytest.mark.parametrize(
        "config",
        [
            {},
            {"source": True},
            {"system_tags": True},
            {"source": True, "system_tags": True},
        ],
        ids=["plain", "source", "system_tags", "source+system_tags"],
    )
    def test_predicted_schema_matches_materialized(self, session_source, config):
        op = GroupBy(by=["subject", "date"])

        pred_tag, pred_data = op.unary_output_schema(session_source, columns=config)
        actual = op.unary_static_process(session_source)
        act_tag, act_data = actual.output_schema(columns=config)

        def diff(label, predicted, actual_schema):
            predicted, actual_schema = dict(predicted), dict(actual_schema)
            only_pred = set(predicted) - set(actual_schema)
            only_act = set(actual_schema) - set(predicted)
            assert predicted == actual_schema, (
                f"{label} schema mismatch for columns={config}: "
                f"predicted-only={sorted(only_pred)}, "
                f"actual-only={sorted(only_act)}, "
                f"predicted={predicted}, actual={actual_schema}"
            )

        diff("tag", pred_tag, act_tag)
        diff("data", pred_data, act_data)


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


class TestGroupByRegistration:
    def test_in_operator_registry(self):
        from orcapod.pipeline.serialization import _build_operator_registry

        assert _build_operator_registry()["GroupBy"] is GroupBy

    def test_stream_fluent_method(self, session_source):
        out = session_source.group_by(["subject", "date"])
        assert len(out.as_table()) == 2


class TestGroupByAsyncIsBarrier:
    """GroupBy must NOT override async_execute.

    ``UnaryOperator.async_execute`` (``core/operators/base.py:71``) already
    collects the full input before calling ``static_process``, which is exactly
    the barrier GroupBy needs: no group can be emitted before the input channel
    closes, because any row not yet seen could belong to a group already
    started.  Adding an override would duplicate that logic and risk drifting
    from it.
    """

    def test_does_not_override_async_execute(self):
        from orcapod.core.operators.base import UnaryOperator

        assert "async_execute" not in GroupBy.__dict__
        assert GroupBy.async_execute is UnaryOperator.async_execute
