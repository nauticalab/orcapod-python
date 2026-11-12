"""Tests for the GroupBy operator — many->one reduction keyed on tag values."""

from __future__ import annotations

from pathlib import Path
from typing import get_origin

import pyarrow as pa
import pytest

from orcapod.core.data_function import PythonDataFunction
from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import Batch, GroupBy, Join
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.system_constants import constants


@pytest.fixture
def session_table() -> pa.Table:
    """Two sessions x two probes: the common-clock shape from NPIPE-204."""
    return pa.table(
        {
            "subject": ["G", "G", "G", "G"],
            "date": ["d1", "d1", "d2", "d2"],
            "probe": [1, 0, 1, 0],
            "path": ["b", "a", "d", "c"],
        }
    )


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
        in_tag_schema, _ = session_source.output_schema()
        tag_schema, _ = out.output_schema()
        assert tag_schema["subject"] == in_tag_schema["subject"]
        assert tag_schema["date"] == in_tag_schema["date"]
        assert out.as_table().column("subject").to_pylist() == ["G", "G"]
        assert out.as_table().column("date").to_pylist() == ["d1", "d2"]

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


class TestGroupByTagsStayTags:
    """No data element may come from a tag value (ITL-955).

    ``GroupBy`` uses tags to form groups, but tag content never becomes data:
    a non-key tag stays in the tag group as a list-valued tag, as ``Batch``
    keeps it.
    """

    def test_non_key_tags_stay_list_valued_tags(self, session_source):
        """A tag not named in ``by`` stays a tag and holds one value per member."""
        out = GroupBy(by=["subject", "date"])(session_source)
        tag_cols, data_cols = out.keys()
        assert tag_cols == ("subject", "date", "probe")
        assert data_cols == ("path",)
        assert out.as_table().column("probe").to_pylist() == [[0, 1], [0, 1]]

    def test_tag_order_is_the_input_tag_order(self, session_source):
        """A key named last in ``by`` does not reorder the tag columns."""
        out = GroupBy(by=["date"])(session_source)
        assert out.keys()[0] == ("subject", "date", "probe")

    def test_no_data_column_is_an_input_tag(self, session_source):
        """The tag and data column names are the input's: no tag moves to data."""
        in_tags, in_data = session_source.keys()
        out = GroupBy(by=["subject"])(session_source)
        assert out.keys() == (in_tags, in_data)

    def test_non_key_tag_schema_is_a_list_tag(self, session_source):
        """``output_schema()`` reports a non-key tag as a list in the tag schema."""
        out = GroupBy(by=["subject", "date"])(session_source)
        tag_schema, data_schema = out.output_schema()
        assert "probe" not in data_schema
        assert get_origin(tag_schema["probe"]) is list

    def test_non_key_tag_keeps_its_arrow_type(self):
        """The list element is the input tag's type: ``int8`` is not widened."""
        table = pa.table(
            {
                "subject": pa.array(["G", "G"], pa.large_string()),
                "probe": pa.array([1, 0], pa.int8()),
                "path": pa.array(["b", "a"], pa.large_string()),
            }
        )
        source = ArrowTableSource(table, tag_columns=["subject", "probe"])
        out = GroupBy(by=["subject"])(source)
        assert out.as_table().schema.field("probe").type == pa.large_list(pa.int8())

    def test_non_key_tags_get_no_source_info_column(self, session_source):
        """Source info belongs to data columns; a tag has none."""
        out = GroupBy(by=["subject", "date"])(session_source)
        names = out.as_table(columns={"source": True}).column_names
        assert f"{constants.SOURCE_PREFIX}probe" not in names
        assert f"{constants.SOURCE_PREFIX}path" in names

    def test_datagrams_carry_non_key_tags_on_the_tag(self, session_source):
        """Each row's ``Tag`` holds the non-key tags; its ``Data`` holds only data."""
        out = GroupBy(by=["subject", "date"])(session_source)
        rows = list(out.iter_data())
        assert len(rows) == 2
        for tag, data in rows:
            assert tag.as_dict()["probe"] == [0, 1]
            assert set(data.as_dict()) == {"path"}

    def test_a_pod_receives_only_data_columns(self, session_source):
        """A function pod never sees tag content, so its signature is the data."""
        pod = FunctionPod(
            PythonDataFunction(_count_paths, output_keys="n")  # pyright: ignore[reportArgumentType]  # protocol mismatch baselined elsewhere (ITL-844)
        )
        out = pod(GroupBy(by=["subject", "date"])(session_source))
        assert [d.as_dict()["n"] for _, d in out.iter_data()] == [2, 2]

    def test_list_valued_non_key_tag_still_cannot_be_a_key(self, session_source):
        """A list-valued tag of a grouped stream is refused as a grouping key."""
        grouped = GroupBy(by=["subject", "date"])(session_source)
        with pytest.raises(InputValidationError, match="list-valued"):
            GroupBy(by=["probe"])(grouped)

    def test_regroup_nests_list_tags(self, session_source):
        """Grouping a grouped stream on a coarser key nests the list tags."""
        out = GroupBy(by=["subject"])(GroupBy(by=["subject", "date"])(session_source))
        assert out.keys() == (("subject", "date", "probe"), ("path",))
        row = out.as_table().to_pylist()[0]
        assert row["date"] == ["d1", "d2"]
        assert row["probe"] == [[0, 1], [0, 1]]
        assert row["path"] == [["a", "b"], ["c", "d"]]

    def test_regroup_orders_list_tags_containing_nulls(self):
        """A list-valued non-key tag holding a null still orders the members."""
        table = pa.table(
            {
                "a": ["x", "x", "x", "x"],
                "b": [1, 1, 2, 2],
                "c": pa.array([None, 1, 2, 1], pa.int64()),
                "v": [1, 2, 3, 4],
            }
        )
        source = ArrowTableSource(
            table, tag_columns=["a", "b", "c"], infer_nullable=True
        )
        first = GroupBy(by=["a", "b"])(source)
        # Regrouping on ``a`` alone leaves ``b`` scalar and ``c`` list-valued:
        # the two members tie on nothing, and are ordered by ``b`` first.
        out = GroupBy(by=["a"])(first)
        row = out.as_table().to_pylist()[0]
        assert row["b"] == [1, 2]
        assert row["c"] == [[1, None], [1, 2]]
        assert row["v"] == [[2, 1], [4, 3]]

    def test_members_tied_on_scalar_tags_order_by_list_tag_with_null(self):
        """``[1, None]`` and ``[1, 2]`` compare without a ``TypeError``."""
        table = pa.table(
            {
                "a": ["x", "x", "x", "x"],
                "b": ["p", "p", "q", "q"],
                "c": pa.array([1, None, 1, 2], pa.int64()),
                "v": [1, 2, 3, 4],
            }
        )
        source = ArrowTableSource(
            table, tag_columns=["a", "b", "c"], infer_nullable=True
        )
        # Drop ``b`` after the first grouping so the two members differ only in
        # the list-valued tag ``c``: [1, None] against [1, 2].
        first = GroupBy(by=["a", "b"])(source).drop_tag_columns(["b"])
        out = GroupBy(by=["a"])(first)
        row = out.as_table().to_pylist()[0]
        assert row["c"] == [[1, 2], [1, None]]
        assert row["v"] == [[3, 4], [1, 2]]


class TestGroupByOrdering:
    """Member and group ordering.

    Determinism here is load-bearing rather than cosmetic: orcapod hashes the
    emitted lists to build the memoization key, so an unstable order makes an
    unchanged member set hash differently and triggers a spurious recompute.
    Upstream emission order is not stable across runs (Ray executor scheduling,
    DB fetch order), so the operator cannot inherit it.

    *That* ordering must be deterministic is a requirement; *which* order to
    use, and whether to promise one publicly, is an open design question -- as
    is empty-group handling (see ``TestGroupByEmptyInput``). These tests pin
    current behaviour so a change is deliberate, not accidental.
    """

    # TODO(ITL-630): decide whether GroupBy guarantees an ordering or only
    # determinism, and which sort key. Deferred per review on PR #250.

    def test_members_sorted_by_non_key_tags(self, session_source):
        """probe=[1,0] on input must emit as [0,1]."""
        out = GroupBy(by=["subject", "date"]).process(session_source)
        assert out.as_table().column("probe").to_pylist()[0] == [0, 1]

    def test_scalar_non_key_tags_sort_ascending_with_nulls_last(self):
        """Pins the member order for scalar tags: ascending, nulls last.

        The sort key is recursive so that list-valued tags compare (ITL-955);
        for scalar tags it must give exactly the order it always gave.
        """
        table = pa.table(
            {
                "g": ["x"] * 5,
                "n": pa.array([3, None, 1, 2, 1], pa.int64()),
                "s": pa.array(["b", "a", None, "c", "a"], pa.large_string()),
                "v": [0, 1, 2, 3, 4],
            }
        )
        source = ArrowTableSource(
            table, tag_columns=["g", "n", "s"], infer_nullable=True
        )
        row = GroupBy(by=["g"])(source).as_table().to_pylist()[0]
        assert row["n"] == [1, 1, 2, 3, None]
        assert row["s"] == ["a", None, "c", "b", "a"]
        assert row["v"] == [4, 2, 3, 0, 1]

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
        ordering it exists to pin down.  The default source_id is an Arrow table
        hash, so this order moved with starfix hash format 2 (ITL-910).
        """
        table = pa.table({"subject": ["G", "G"], "path": ["b", "a"]})
        src = ArrowTableSource(table, tag_columns=["subject"], infer_nullable=True)
        out = GroupBy(by=["subject"]).process(src)
        assert len(out.as_table()) == 1
        assert out.as_table().column("path").to_pylist()[0] == ["b", "a"]

    def test_record_id_breaks_ties_between_duplicate_tags(self):
        """Duplicate tag tuples must not fall back to raw emission order."""
        table = pa.table({"s": ["G", "G"], "p": [1, 1], "path": ["z", "a"]})
        src = ArrowTableSource(table, tag_columns=["s", "p"], infer_nullable=True)
        out = GroupBy(by=["s"]).process(src)
        # Emission order would give ["z", "a"]; record_id imposes ["a", "z"].
        assert out.as_table().column("path").to_pylist()[0] == ["a", "z"]

    def test_join_fan_out_ties_broken_by_every_record_id_column(self):
        """A joined stream carries one record_id per canonical input position.

        The broadcast side of a fan-out repeats its record_id on every row it
        was joined into, so it cannot separate those rows; only the fanned-out
        side's record_id can.  Consulting just the first record_id column in
        table order therefore leaves the sort key fully tied whenever the
        broadcast side happens to sort first canonically, and the members fall
        back to emission order -- which DB fetch order and Ray scheduling make
        nondeterministic.  Every record_id column has to take part.

        The literals are load-bearing: with this pair of schemas the broadcast
        input sorts to canonical position 0, and its record_ids order the
        members *against* emission order, so emission order and record_id
        order are distinguishable.  Sorting the result before comparing would
        make the test blind to the ordering it exists to pin down.
        """
        left = ArrowTableSource(
            pa.table({"k": ["t", "t"], "a": [10, 20]}),
            tag_columns=["k"],
            infer_nullable=True,
        )
        right = ArrowTableSource(
            pa.table({"k": ["t"], "b": [1]}),
            tag_columns=["k"],
            infer_nullable=True,
        )
        joined = Join().process(left, right)
        out = GroupBy(by=["k"]).process(joined).as_table()
        # Emission order would give [10, 20]; the fanned-out side's record_id
        # imposes [20, 10].
        assert out.column("a").to_pylist() == [[20, 10]]


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
    """Empty-input behaviour.

    Current behaviour is "zero rows in, zero groups out" -- the least
    surprising default, but never designed. Whether a reduction should instead
    emit nothing, emit an empty group, or raise is open. This test pins the
    status quo.
    """

    # TODO(ITL-630): decide empty-input behaviour for reductions. Deferred per
    # review on PR #250, same follow-up as ordering.

    def test_empty_input_yields_zero_groups(self):
        table = pa.table(
            {
                "subject": pa.array([], pa.large_string()),
                "path": pa.array([], pa.large_string()),
            }
        )
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


# ---------------------------------------------------------------------------
# Logical (extension) element types — NPIPE-204 / ET2
# ---------------------------------------------------------------------------


# Defined at module level on purpose.  This file uses `from __future__ import
# annotations`, so a pod function nested inside a test method would have its
# annotations stringified with no resolvable scope, and `Path` would fail to
# resolve.  Module scope keeps `-> Path` resolvable, matching how real pipelines
# declare pods.
def _count_paths(path: list[str | None]) -> int:
    """Return the number of members in a group's ``path`` list.

    Args:
        path: The grouped ``path`` values, one per member.

    Returns:
        The member count.
    """
    return len(path)


def _make_path(seed: str) -> Path:
    return Path(f"/data/sync_{seed}.parquet")


def _path_stream() -> StreamProtocol:
    """A stream whose data column is ``extension<orcapod.path>``."""
    source = ArrowTableSource(
        pa.table(
            {
                "date": ["d1", "d1", "d2"],
                "probe": [0, 1, 0],
                "seed": ["a", "b", "c"],
            }
        ),
        tag_columns=["date", "probe"],
        infer_nullable=True,
    )
    return FunctionPod(PythonDataFunction(_make_path, output_keys="result_path"))(
        source
    )


class TestAggregationLogicalTypes:
    """Aggregating operators must preserve logical (extension) element types.

    A pod annotated ``-> Path`` emits an ``extension<orcapod.path>`` column.
    Naively wrapping that in ``pa.list_()`` raises ``ArrowNotImplementedError``:
    Arrow cannot embed an extension type inside a list value field
    (design_issues/ ET1/ET2).  The list must be built over the element's
    *storage* type and wrapped in the ``list[orcapod.path]`` extension type at
    the outermost level, which is what ``ListLogicalType`` provides.

    Every other fixture in this file uses plain ``large_string``/``int64``,
    which is why this case was originally missed.
    """

    def test_upstream_column_really_is_extension_typed(self):
        """Guard the fixture -- if this stops holding, the rest is vacuous."""
        field = _path_stream().as_table().schema.field("result_path")
        assert isinstance(field.type, pa.ExtensionType)
        assert field.type.storage_type == pa.large_string()

    def test_group_by_preserves_path_element_type(self):
        out = GroupBy(by=["date"]).process(_path_stream())
        field = out.as_table().schema.field("result_path")

        assert isinstance(field.type, pa.ExtensionType), (
            f"expected a list extension type, got {field.type}"
        )
        assert field.type.storage_type == pa.large_list(pa.large_string())

    def test_group_by_output_schema_is_list_of_path(self):
        out = GroupBy(by=["date"]).process(_path_stream())
        _, data_schema = out.output_schema()
        assert data_schema["result_path"] == list[Path]

    def test_group_by_preserves_path_values(self):
        out = GroupBy(by=["date"]).process(_path_stream())
        assert out.as_table().column("result_path").to_pylist() == [
            ["/data/sync_a.parquet", "/data/sync_b.parquet"],
            ["/data/sync_c.parquet"],
        ]

    def test_batch_preserves_path_element_type(self):
        """Batch has the same defect, independent of GroupBy."""
        out = Batch(batch_size=0).process(_path_stream())
        field = out.as_table().schema.field("result_path")
        assert isinstance(field.type, pa.ExtensionType), (
            f"expected a list extension type, got {field.type}"
        )
        assert field.type.storage_type == pa.large_list(pa.large_string())
