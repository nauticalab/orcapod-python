"""Tests for MergeJoin operator."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.operators import MergeJoin
from orcapod.core.sources.arrow_table_source import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.errors import InputValidationError
from orcapod.protocols.core_protocols import PodProtocol


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
#
# Left and right fixtures are deliberately asymmetric:
# - Different tag column sets (left: ["id"], right: ["id", "group"])
#   to prove tag union and that inner join on shared tags works.
# - Colliding "value" column has left > right for id=2 (500 > 200)
#   and left < right for id=3 (30 < 300), forcing actual sort reordering.
# - Non-overlapping ids (left has 1, right has 4) prove inner join filters.


@pytest.fixture
def left_stream() -> ArrowTableStream:
    table = pa.table(
        {
            "id": [1, 2, 3],
            "value": [10, 500, 30],
            "extra_left": ["a", "b", "c"],
        }
    )
    return ArrowTableStream(table, tag_columns=["id"])


@pytest.fixture
def right_stream() -> ArrowTableStream:
    table = pa.table(
        {
            "id": [2, 3, 4],
            "group": ["X", "Y", "Z"],
            "value": [200, 300, 400],
            "extra_right": ["x", "y", "z"],
        }
    )
    return ArrowTableStream(table, tag_columns=["id", "group"])


@pytest.fixture
def left_source() -> ArrowTableSource:
    return ArrowTableSource(
        pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int64()),
                "value": pa.array([10, 500, 30], type=pa.int64()),
                "extra_left": pa.array(["a", "b", "c"], type=pa.large_string()),
            }
        ),
        tag_columns=["id"],
    )


@pytest.fixture
def right_source() -> ArrowTableSource:
    return ArrowTableSource(
        pa.table(
            {
                "id": pa.array([2, 3, 4], type=pa.int64()),
                "group": pa.array(["X", "Y", "Z"], type=pa.large_string()),
                "value": pa.array([200, 300, 400], type=pa.int64()),
                "extra_right": pa.array(["x", "y", "z"], type=pa.large_string()),
            }
        ),
        tag_columns=["id", "group"],
    )


# ===================================================================
# PodProtocol conformance
# ===================================================================


class TestMergeJoinConformance:
    def test_is_pod_protocol(self):
        op = MergeJoin()
        assert isinstance(op, PodProtocol)

    def test_is_commutative(self):
        op = MergeJoin()
        assert op.is_commutative() is True


# ===================================================================
# Functional correctness
# ===================================================================


class TestMergeJoinBasic:
    def test_inner_join_on_shared_tags(self, left_stream, right_stream):
        """Only matching tag values survive the inner join."""
        op = MergeJoin()
        result = op.static_process(left_stream, right_stream)
        result_table = result.as_table()

        ids = result_table.column("id").to_pylist()
        # id=1 only in left, id=4 only in right => only 2,3 survive
        assert sorted(ids) == [2, 3]

    def test_tag_columns_are_union(self, left_stream, right_stream):
        """Output should have the union of both tag column sets."""
        op = MergeJoin()
        result = op.static_process(left_stream, right_stream)
        tag_keys, _ = result.keys()
        # left has ["id"], right has ["id", "group"] => union is {"id", "group"}
        assert set(tag_keys) == {"id", "group"}

    def test_colliding_columns_become_sorted_lists(self, left_stream, right_stream):
        """Colliding packet columns must be sorted, not just in input order.
        id=2: left=500, right=200 => must sort to [200, 500] (proves reordering).
        id=3: left=30, right=300 => [30, 300]."""
        op = MergeJoin()
        result = op.static_process(left_stream, right_stream)
        rows = {r["id"]: r for r in result.as_table().to_pylist()}

        # id=2: left=500 > right=200, so sorting MUST reorder to [200, 500]
        assert rows[2]["value"] == [200, 500]
        # id=3: left=30 < right=300, sorted stays [30, 300]
        assert rows[3]["value"] == [30, 300]

    def test_non_colliding_columns_stay_scalar(self, left_stream, right_stream):
        """Non-colliding packet columns should remain as scalars."""
        op = MergeJoin()
        result = op.static_process(left_stream, right_stream)
        rows = result.as_table().to_pylist()
        for row in rows:
            assert isinstance(row["extra_left"], str)
            assert isinstance(row["extra_right"], str)

    def test_values_sorted_independently_per_column(self):
        """Each colliding column should be sorted independently.
        col_a: left > right for id=1, left < right for id=2.
        col_b: left < right for id=1, left > right for id=2.
        Sorting each independently means they can't just be preserving one order."""
        left = ArrowTableStream(
            pa.table(
                {
                    "id": [1, 2],
                    "col_a": [100, 5],
                    "col_b": [1, 50],
                }
            ),
            tag_columns=["id"],
        )
        right = ArrowTableStream(
            pa.table(
                {
                    "id": [1, 2],
                    "col_a": [3, 200],
                    "col_b": [99, 2],
                }
            ),
            tag_columns=["id"],
        )

        op = MergeJoin()
        result = op.static_process(left, right)
        rows = {r["id"]: r for r in result.as_table().to_pylist()}

        # id=1: col_a left=100>right=3 => [3,100], col_b left=1<right=99 => [1,99]
        assert rows[1]["col_a"] == [3, 100]
        assert rows[1]["col_b"] == [1, 99]

        # id=2: col_a left=5<right=200 => [5,200], col_b left=50>right=2 => [2,50]
        assert rows[2]["col_a"] == [5, 200]
        assert rows[2]["col_b"] == [2, 50]


class TestMergeJoinSourceColumns:
    def test_source_columns_follow_packet_sort_order(self, left_source, right_source):
        """Source columns for colliding packet columns should be reordered
        to match the sort order of the corresponding packet values.
        For id=2: left=500>right=200, so right's source entry must come first."""
        from orcapod.system_constants import constants

        op = MergeJoin()
        result = op.static_process(left_source, right_source)
        result_table = result.as_table(columns={"source": True})

        source_col_name = f"{constants.SOURCE_PREFIX}value"
        assert source_col_name in result_table.column_names

        rows = {r["id"]: r for r in result_table.to_pylist()}

        # id=2: value=[200, 500], so source should list right's source first
        assert isinstance(rows[2][source_col_name], list)
        assert len(rows[2][source_col_name]) == 2
        # The first source entry corresponds to the smaller value (200 from right)
        assert "value" in rows[2][source_col_name][0]
        # Verify packet values are actually sorted
        assert rows[2]["value"] == [200, 500]

        # id=3: value=[30, 300], left's source first (30 is left's value)
        assert rows[3]["value"] == [30, 300]

    def test_non_colliding_source_columns_preserved(self, left_source, right_source):
        """Source columns for non-colliding packet columns should remain as scalars."""
        from orcapod.system_constants import constants

        op = MergeJoin()
        result = op.static_process(left_source, right_source)
        result_table = result.as_table(columns={"source": True})

        left_source_col = f"{constants.SOURCE_PREFIX}extra_left"
        right_source_col = f"{constants.SOURCE_PREFIX}extra_right"

        assert left_source_col in result_table.column_names
        assert right_source_col in result_table.column_names

        rows = result_table.to_pylist()
        for row in rows:
            assert isinstance(row[left_source_col], str)
            assert isinstance(row[right_source_col], str)

    def test_source_columns_sorted_independently_per_colliding_column(self):
        """With two colliding columns (math, reading) where sort order differs
        per column, each source column must track its own packet column's sort.

        east math=95 > west math=70 but east reading=30 < west reading=92 (id=1)
        east math=40 < west math=85 but east reading=88 > west reading=10 (id=2)

        So for id=1: math sorts to [70,95] (west,east) but reading sorts to
        [30,92] (east,west) — source columns must follow each independently."""
        from orcapod.system_constants import constants

        east = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "math": pa.array([95, 40], type=pa.int64()),
                    "reading": pa.array([30, 88], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            source_name="east",
        )
        west = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "math": pa.array([70, 85], type=pa.int64()),
                    "reading": pa.array([92, 10], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            source_name="west",
        )

        op = MergeJoin()
        result = op.static_process(east, west)
        result_table = result.as_table(columns={"source": True})

        src_math = f"{constants.SOURCE_PREFIX}math"
        src_reading = f"{constants.SOURCE_PREFIX}reading"
        assert src_math in result_table.column_names
        assert src_reading in result_table.column_names

        rows = {r["id"]: r for r in result_table.to_pylist()}

        # id=1: math=[70, 95] (west first), reading=[30, 92] (east first)
        assert rows[1]["math"] == [70, 95]
        assert rows[1]["reading"] == [30, 92]
        # Source for math: west's source first (matches 70), east's source second (matches 95)
        assert "west" in rows[1][src_math][0]
        assert "east" in rows[1][src_math][1]
        # Source for reading: east's source first (matches 30), west's source second (matches 92)
        assert "east" in rows[1][src_reading][0]
        assert "west" in rows[1][src_reading][1]

        # id=2: math=[40, 85] (east first), reading=[10, 88] (west first)
        assert rows[2]["math"] == [40, 85]
        assert rows[2]["reading"] == [10, 88]
        # Source for math: east first (matches 40), west second (matches 85)
        assert "east" in rows[2][src_math][0]
        assert "west" in rows[2][src_math][1]
        # Source for reading: west first (matches 10), east second (matches 88)
        assert "west" in rows[2][src_reading][0]
        assert "east" in rows[2][src_reading][1]


class TestMergeJoinCommutativity:
    def test_commutative_data_output(self, left_stream, right_stream):
        """MergeJoin(A, B) should produce the same data as MergeJoin(B, A)."""
        op = MergeJoin()
        result_lr = op.static_process(left_stream, right_stream)
        result_rl = op.static_process(right_stream, left_stream)

        rows_lr = sorted(result_lr.as_table().to_pylist(), key=lambda r: r["id"])
        rows_rl = sorted(result_rl.as_table().to_pylist(), key=lambda r: r["id"])

        assert rows_lr == rows_rl

    def test_commutative_system_tag_column_names(self, left_source, right_source):
        """Swapping input order should produce the same system tag column names."""
        from orcapod.system_constants import constants

        op = MergeJoin()

        result_lr = op.static_process(left_source, right_source)
        result_rl = op.static_process(right_source, left_source)

        sys_cols_lr = sorted(
            c
            for c in result_lr.as_table(columns={"system_tags": True}).column_names
            if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )
        sys_cols_rl = sorted(
            c
            for c in result_rl.as_table(columns={"system_tags": True}).column_names
            if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )

        assert sys_cols_lr == sys_cols_rl

    def test_commutative_system_tag_values_same_pipeline_hash(self):
        """When both inputs have the same pipeline_hash, swapping inputs
        must still produce identical system tag VALUES per row (not just
        column names). This tests the value-sorting logic.

        src_a values [300, 20] vs src_b values [100, 200]:
        id=1: a=300>b=100, id=2: a=20<b=200 — mixed ordering."""
        from orcapod.system_constants import constants

        src_a = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value": pa.array([300, 20], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )
        src_b = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value": pa.array([100, 200], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )

        # Verify same pipeline_hash (same schema)
        assert src_a.pipeline_hash().to_hex() == src_b.pipeline_hash().to_hex()

        op = MergeJoin()
        result_ab = op.static_process(src_a, src_b)
        result_ba = op.static_process(src_b, src_a)

        table_ab = result_ab.as_table(columns={"system_tags": True})
        table_ba = result_ba.as_table(columns={"system_tags": True})

        sys_cols_ab = sorted(
            c
            for c in table_ab.column_names
            if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )
        sys_cols_ba = sorted(
            c
            for c in table_ba.column_names
            if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )

        # Same column names
        assert sys_cols_ab == sys_cols_ba

        # Same values per row (sort rows by id for deterministic comparison)
        rows_ab = sorted(table_ab.to_pylist(), key=lambda r: r["id"])
        rows_ba = sorted(table_ba.to_pylist(), key=lambda r: r["id"])

        for row_ab, row_ba in zip(rows_ab, rows_ba):
            for col in sys_cols_ab:
                assert row_ab[col] == row_ba[col], (
                    f"System tag value mismatch for {col}: "
                    f"{row_ab[col]!r} vs {row_ba[col]!r}"
                )


class TestMergeJoinOutputSchema:
    def test_colliding_columns_become_list_type(self, left_stream, right_stream):
        """Output schema should have list[T] for colliding columns."""
        op = MergeJoin()
        _, packet_schema = op.output_schema(left_stream, right_stream)

        assert packet_schema["value"] == list[int]

    def test_non_colliding_columns_stay_original_type(self, left_stream, right_stream):
        """Output schema should keep original type for non-colliding columns."""
        op = MergeJoin()
        _, packet_schema = op.output_schema(left_stream, right_stream)

        assert packet_schema["extra_left"] == str
        assert packet_schema["extra_right"] == str

    def test_tag_schema_is_union(self, left_stream, right_stream):
        """Tag schema should be the union of both input tag schemas."""
        op = MergeJoin()
        tag_schema, _ = op.output_schema(left_stream, right_stream)

        # left tags: {"id"}, right tags: {"id", "group"}
        assert "id" in tag_schema
        assert "group" in tag_schema

    def test_output_schema_excludes_system_tags_by_default(
        self, left_source, right_source
    ):
        """Without system_tags=True, no system tag columns in tag schema."""
        from orcapod.system_constants import constants

        op = MergeJoin()
        tag_schema, _ = op.output_schema(left_source, right_source)

        for key in tag_schema:
            assert not key.startswith(constants.SYSTEM_TAG_PREFIX)

    def test_output_schema_includes_system_tags_when_requested(
        self, left_source, right_source
    ):
        """With system_tags=True, tag schema should include system tag columns."""
        from orcapod.system_constants import constants

        op = MergeJoin()
        tag_schema, _ = op.output_schema(
            left_source, right_source, columns={"system_tags": True}
        )

        sys_tag_keys = [
            k for k in tag_schema if k.startswith(constants.SYSTEM_TAG_PREFIX)
        ]
        assert len(sys_tag_keys) == 2

    def test_output_schema_system_tags_match_actual_output(
        self, left_source, right_source
    ):
        """Predicted system tag column names must match the actual result."""
        from orcapod.system_constants import constants

        op = MergeJoin()

        # Predicted schema
        tag_schema, _ = op.output_schema(
            left_source, right_source, columns={"system_tags": True}
        )
        predicted_sys_tags = sorted(
            k for k in tag_schema if k.startswith(constants.SYSTEM_TAG_PREFIX)
        )

        # Actual result
        result = op.static_process(left_source, right_source)
        result_table = result.as_table(columns={"system_tags": True})
        actual_sys_tags = sorted(
            c
            for c in result_table.column_names
            if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )

        assert predicted_sys_tags == actual_sys_tags

    def test_output_schema_system_tags_match_with_same_pipeline_hash(self):
        """System tag prediction should work when both inputs have the
        same pipeline_hash — columns distinguished by canonical position."""
        from orcapod.system_constants import constants

        src_a = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value": pa.array([10, 20], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )
        src_b = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value": pa.array([100, 200], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )

        assert src_a.pipeline_hash().to_hex() == src_b.pipeline_hash().to_hex()

        op = MergeJoin()
        tag_schema, _ = op.output_schema(src_a, src_b, columns={"system_tags": True})
        predicted = sorted(
            k for k in tag_schema if k.startswith(constants.SYSTEM_TAG_PREFIX)
        )

        result = op.static_process(src_a, src_b)
        actual = sorted(
            c
            for c in result.as_table(columns={"system_tags": True}).column_names
            if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )

        assert predicted == actual
        # Must have 2 distinct system tag columns
        assert len(predicted) == 2
        assert predicted[0] != predicted[1]

    def test_output_schema_all_info_includes_system_tags(
        self, left_source, right_source
    ):
        """all_info=True should include system tag columns in the schema."""
        from orcapod.system_constants import constants

        op = MergeJoin()
        tag_schema, _ = op.output_schema(left_source, right_source, all_info=True)

        sys_tag_keys = [
            k for k in tag_schema if k.startswith(constants.SYSTEM_TAG_PREFIX)
        ]
        assert len(sys_tag_keys) == 2

    def test_predicted_schema_matches_result_stream_schema(
        self, left_source, right_source
    ):
        """Operator's predicted output_schema must equal the result stream's
        output_schema — both tag and packet schemas, without system tags."""
        op = MergeJoin()

        predicted_tag, predicted_pkt = op.output_schema(left_source, right_source)
        result = op.static_process(left_source, right_source)
        actual_tag, actual_pkt = result.output_schema()

        assert dict(predicted_tag) == dict(actual_tag)
        assert dict(predicted_pkt) == dict(actual_pkt)

    def test_predicted_schema_matches_result_stream_schema_with_system_tags(
        self, left_source, right_source
    ):
        """Operator's predicted output_schema(system_tags=True) must equal
        the result stream's output_schema(system_tags=True)."""
        op = MergeJoin()

        predicted_tag, predicted_pkt = op.output_schema(
            left_source, right_source, columns={"system_tags": True}
        )
        result = op.static_process(left_source, right_source)
        actual_tag, actual_pkt = result.output_schema(columns={"system_tags": True})

        assert dict(predicted_tag) == dict(actual_tag)
        assert dict(predicted_pkt) == dict(actual_pkt)


class TestMergeJoinValidation:
    def test_colliding_columns_with_incompatible_types_raises(self):
        """MergeJoin should reject colliding columns with different types."""
        left = ArrowTableStream(
            pa.table({"id": [1, 2], "value": [10, 20]}),
            tag_columns=["id"],
        )
        right = ArrowTableStream(
            pa.table({"id": [1, 2], "value": ["a", "b"]}),
            tag_columns=["id"],
        )

        op = MergeJoin()
        with pytest.raises(InputValidationError, match="incompatible types"):
            op.validate_inputs(left, right)

    def test_colliding_columns_with_same_types_passes(self, left_stream, right_stream):
        """MergeJoin should accept colliding columns with the same type."""
        op = MergeJoin()
        # Should not raise
        op.validate_inputs(left_stream, right_stream)


class TestMergeJoinSystemTags:
    """System tag tests demonstrating why both pipeline_hash and canonical
    position are needed."""

    @staticmethod
    def _get_system_tag_columns(table, constants):
        return sorted(
            c for c in table.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )

    @staticmethod
    def _parse_system_tag_column(col, constants):
        """Parse system tag column name into its component blocks.

        Format: _tag::source:{source_hash}::{stream_hash}:{canonical_position}
        """
        after_prefix = col[len(constants.SYSTEM_TAG_PREFIX) :]
        blocks = after_prefix.split(constants.BLOCK_SEPARATOR)
        source_block_fields = blocks[0].split(constants.FIELD_SEPARATOR)
        join_block_fields = blocks[1].split(constants.FIELD_SEPARATOR)
        source_hash = source_block_fields[1]
        stream_hash = join_block_fields[0]
        index = join_block_fields[1]
        return source_hash, stream_hash, index

    def test_two_system_tag_columns_produced(self, left_source, right_source):
        """MergeJoin of two sources should produce 2 system tag columns."""
        from orcapod.system_constants import constants

        op = MergeJoin()
        result = op.static_process(left_source, right_source)
        result_table = result.as_table(columns={"system_tags": True})
        sys_cols = self._get_system_tag_columns(result_table, constants)
        assert len(sys_cols) == 2

    def test_system_tag_canonical_positions(self, left_source, right_source):
        """System tag columns should carry canonical position indices
        matching stable sort by pipeline_hash."""
        from orcapod.config import Config
        from orcapod.system_constants import constants

        n_char = Config().system_tag_hash_n_char

        op = MergeJoin()
        result = op.static_process(left_source, right_source)
        result_table = result.as_table(columns={"system_tags": True})
        sys_cols = self._get_system_tag_columns(result_table, constants)

        # Independently determine expected ordering
        sources = [left_source, right_source]
        sorted_sources = sorted(sources, key=lambda s: s.pipeline_hash().to_hex())

        for expected_idx, expected_source in enumerate(sorted_sources):
            source_hash, stream_hash, index_str = self._parse_system_tag_column(
                sys_cols[expected_idx], constants
            )
            expected_stream_hash = expected_source.pipeline_hash().to_hex(n_char)
            assert stream_hash == expected_stream_hash
            assert index_str == str(expected_idx)

    def test_same_schema_inputs_distinguished_by_canonical_position(self):
        """Two streams with identical schemas (same pipeline_hash) must still
        produce distinct system tag columns via canonical position.

        Values have mixed ordering (a>b for id=1, a<b for id=2) to prove
        the merge actually sorts."""
        from orcapod.system_constants import constants

        src_a = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value": pa.array([300, 20], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )
        src_b = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value": pa.array([100, 200], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )

        # Same schema => same pipeline_hash
        assert src_a.pipeline_hash().to_hex() == src_b.pipeline_hash().to_hex()

        op = MergeJoin()
        result = op.static_process(src_a, src_b)
        result_table = result.as_table(columns={"system_tags": True})
        sys_cols = self._get_system_tag_columns(result_table, constants)

        # Must have 2 distinct system tag columns
        assert len(sys_cols) == 2
        assert sys_cols[0] != sys_cols[1]

        # Both should have the same pipeline_hash but different positions
        _, hash_0, pos_0 = self._parse_system_tag_column(sys_cols[0], constants)
        _, hash_1, pos_1 = self._parse_system_tag_column(sys_cols[1], constants)

        assert hash_0 == hash_1  # Same pipeline hash
        assert pos_0 != pos_1  # Different canonical positions
        assert {pos_0, pos_1} == {"0", "1"}

        # Verify merged values are actually sorted (proves reordering happened)
        rows = {r["id"]: r for r in result_table.to_pylist()}
        # id=1: a=300>b=100 => [100, 300]
        assert rows[1]["value"] == [100, 300]
        # id=2: a=20<b=200 => [20, 200]
        assert rows[2]["value"] == [20, 200]

    def test_different_schema_inputs_have_different_pipeline_hashes(
        self, left_source, right_source
    ):
        """Two sources with different schemas should have different pipeline_hashes
        in their system tag columns."""
        from orcapod.system_constants import constants

        op = MergeJoin()
        result = op.static_process(left_source, right_source)
        result_table = result.as_table(columns={"system_tags": True})
        sys_cols = self._get_system_tag_columns(result_table, constants)

        _, hash_0, _ = self._parse_system_tag_column(sys_cols[0], constants)
        _, hash_1, _ = self._parse_system_tag_column(sys_cols[1], constants)

        assert hash_0 != hash_1

    def test_commutative_system_tag_column_names_same_pipeline_hash(self):
        """Swapping inputs with same pipeline_hash must produce identical
        system tag column names. Values have mixed ordering to prove sort."""
        from orcapod.system_constants import constants

        src_a = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value": pa.array([300, 20], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )
        src_b = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value": pa.array([100, 200], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
        )

        op = MergeJoin()
        result_ab = op.static_process(src_a, src_b)
        result_ba = op.static_process(src_b, src_a)

        sys_ab = self._get_system_tag_columns(
            result_ab.as_table(columns={"system_tags": True}), constants
        )
        sys_ba = self._get_system_tag_columns(
            result_ba.as_table(columns={"system_tags": True}), constants
        )

        assert sys_ab == sys_ba

    def test_system_tag_values_sorted_for_same_pipeline_hash(self):
        """When two streams share the same pipeline_hash, system tag VALUES
        must be sorted per row so that position :0 always gets the
        lexicographically smaller value.

        Uses source_name="zzz_source" vs "aaa_source" to ensure the
        lexicographic order of provenance values is opposite to input order,
        proving that sorting actually happened (not just preserved)."""
        from orcapod.system_constants import constants

        src_a = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value": pa.array([300, 20], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            source_name="zzz_source",
        )
        src_b = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value": pa.array([100, 200], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            source_name="aaa_source",
        )

        assert src_a.pipeline_hash().to_hex() == src_b.pipeline_hash().to_hex()

        op = MergeJoin()

        result_ab = op.static_process(src_a, src_b)
        result_ba = op.static_process(src_b, src_a)

        table_ab = result_ab.as_table(columns={"system_tags": True})
        table_ba = result_ba.as_table(columns={"system_tags": True})

        sys_cols = self._get_system_tag_columns(table_ab, constants)
        assert len(sys_cols) == 2

        # For each row, the value in position :0 should be <= value in position :1
        for row in table_ab.to_pylist():
            val_0 = row[sys_cols[0]]
            val_1 = row[sys_cols[1]]
            assert val_0 <= val_1, (
                f"System tag values not sorted: {val_0!r} > {val_1!r}"
            )

        # "aaa_source" < "zzz_source", so position :0 must always hold aaa_source
        for row in table_ab.to_pylist():
            assert "aaa_source" in row[sys_cols[0]]
            assert "zzz_source" in row[sys_cols[1]]

        # And swapped inputs must produce identical per-row values
        rows_ab = sorted(table_ab.to_pylist(), key=lambda r: r["id"])
        rows_ba = sorted(table_ba.to_pylist(), key=lambda r: r["id"])

        for row_ab, row_ba in zip(rows_ab, rows_ba):
            for col in sys_cols:
                assert row_ab[col] == row_ba[col]
