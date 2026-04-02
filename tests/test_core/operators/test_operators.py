"""Tests for all operators: PodProtocol conformance and functional correctness."""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.operators import (
    Batch,
    DropPacketColumns,
    DropTagColumns,
    Join,
    MapPackets,
    MapTags,
    PolarsFilter,
    SelectPacketColumns,
    SelectTagColumns,
    SemiJoin,
)
from orcapod.core.streams import ArrowTableStream
from orcapod.protocols.core_protocols import PodProtocol, StreamProtocol


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def simple_stream() -> ArrowTableStream:
    """Stream with 1 tag (animal) and 2 packet columns (weight, legs)."""
    table = pa.table(
        {
            "animal": ["cat", "dog", "bird"],
            "weight": [4.0, 12.0, 0.5],
            "legs": [4, 4, 2],
        }
    )
    return ArrowTableStream(table, tag_columns=["animal"])


@pytest.fixture
def two_tag_stream() -> ArrowTableStream:
    """Stream with 2 tags (region, animal) and 1 packet column (count)."""
    table = pa.table(
        {
            "region": ["east", "east", "west"],
            "animal": ["cat", "dog", "cat"],
            "count": [10, 5, 8],
        }
    )
    return ArrowTableStream(table, tag_columns=["region", "animal"])


@pytest.fixture
def left_stream() -> ArrowTableStream:
    """Left stream for binary operator tests."""
    table = pa.table(
        {
            "id": [1, 2, 3],
            "value_a": [10, 20, 30],
        }
    )
    return ArrowTableStream(table, tag_columns=["id"])


@pytest.fixture
def right_stream() -> ArrowTableStream:
    """Right stream for binary operator tests."""
    table = pa.table(
        {
            "id": [2, 3, 4],
            "value_b": [200, 300, 400],
        }
    )
    return ArrowTableStream(table, tag_columns=["id"])


@pytest.fixture
def disjoint_stream() -> ArrowTableStream:
    """Stream with no overlapping packet columns for join tests."""
    table = pa.table(
        {
            "animal": ["cat", "dog", "bird"],
            "speed": [30.0, 45.0, 80.0],
        }
    )
    return ArrowTableStream(table, tag_columns=["animal"])


# ===================================================================
# Part 1 — PodProtocol conformance: can we instantiate + isinstance?
# ===================================================================


class TestPodProtocolConformance:
    """Every operator must be instantiable and satisfy PodProtocol."""

    def test_polars_filter_is_pod(self):
        op = PolarsFilter()
        assert isinstance(op, PodProtocol)

    def test_select_tag_columns_is_pod(self):
        op = SelectTagColumns(columns=["x"])
        assert isinstance(op, PodProtocol)

    def test_select_packet_columns_is_pod(self):
        op = SelectPacketColumns(columns=["x"])
        assert isinstance(op, PodProtocol)

    def test_drop_tag_columns_is_pod(self):
        op = DropTagColumns(columns=["x"])
        assert isinstance(op, PodProtocol)

    def test_drop_packet_columns_is_pod(self):
        op = DropPacketColumns(columns=["x"])
        assert isinstance(op, PodProtocol)

    def test_map_packets_is_pod(self):
        op = MapPackets(name_map={"a": "b"})
        assert isinstance(op, PodProtocol)

    def test_map_tags_is_pod(self):
        op = MapTags(name_map={"a": "b"})
        assert isinstance(op, PodProtocol)

    def test_batch_is_pod(self):
        op = Batch(batch_size=2)
        assert isinstance(op, PodProtocol)

    def test_join_is_pod(self):
        op = Join()
        assert isinstance(op, PodProtocol)

    def test_semijoin_is_pod(self):
        op = SemiJoin()
        assert isinstance(op, PodProtocol)


# ===================================================================
# Part 2 — Output stream is StreamProtocol with producer lineage
# ===================================================================


class TestOutputStreamLineage:
    """process() must return a StreamProtocol whose producer is the operator."""

    def test_polars_filter_producer(self, simple_stream):
        op = PolarsFilter()
        out = op.process(simple_stream)
        assert isinstance(out, StreamProtocol)
        assert out.producer is op

    def test_select_tag_columns_producer(self, two_tag_stream):
        op = SelectTagColumns(columns=["region"])
        out = op.process(two_tag_stream)
        assert isinstance(out, StreamProtocol)
        assert out.producer is op

    def test_select_packet_columns_producer(self, simple_stream):
        op = SelectPacketColumns(columns=["weight"])
        out = op.process(simple_stream)
        assert isinstance(out, StreamProtocol)
        assert out.producer is op

    def test_drop_tag_columns_producer(self, two_tag_stream):
        op = DropTagColumns(columns=["region"])
        out = op.process(two_tag_stream)
        assert isinstance(out, StreamProtocol)
        assert out.producer is op

    def test_drop_packet_columns_producer(self, simple_stream):
        op = DropPacketColumns(columns=["legs"])
        out = op.process(simple_stream)
        assert isinstance(out, StreamProtocol)
        assert out.producer is op

    def test_map_packets_producer(self, simple_stream):
        op = MapPackets(name_map={"weight": "mass"})
        out = op.process(simple_stream)
        assert isinstance(out, StreamProtocol)
        assert out.producer is op

    def test_map_tags_producer(self, two_tag_stream):
        op = MapTags(name_map={"region": "area"})
        out = op.process(two_tag_stream)
        assert isinstance(out, StreamProtocol)
        assert out.producer is op

    def test_batch_producer(self, simple_stream):
        op = Batch(batch_size=2)
        out = op.process(simple_stream)
        assert isinstance(out, StreamProtocol)
        assert out.producer is op

    def test_join_producer(self, simple_stream, disjoint_stream):
        op = Join()
        out = op.process(simple_stream, disjoint_stream)
        assert isinstance(out, StreamProtocol)
        assert out.producer is op

    def test_semijoin_producer(self, left_stream, right_stream):
        op = SemiJoin()
        out = op.process(left_stream, right_stream)
        assert isinstance(out, StreamProtocol)
        assert out.producer is op


# ===================================================================
# Part 3 — Input validation
# ===================================================================


class TestInputValidation:
    """Operators must reject wrong number of inputs."""

    def test_unary_rejects_zero_inputs(self, simple_stream):
        op = PolarsFilter()
        with pytest.raises(ValueError, match="exactly one"):
            op.process()

    def test_unary_rejects_two_inputs(self, simple_stream):
        op = PolarsFilter()
        with pytest.raises(ValueError, match="exactly one"):
            op.process(simple_stream, simple_stream)

    def test_binary_rejects_one_input(self, left_stream):
        op = SemiJoin()
        with pytest.raises(ValueError, match="exactly two"):
            op.process(left_stream)

    def test_binary_rejects_three_inputs(self, left_stream, right_stream):
        op = SemiJoin()
        with pytest.raises(ValueError, match="exactly two"):
            op.process(left_stream, right_stream, left_stream)

    def test_nonzero_rejects_zero_inputs(self):
        op = Join()
        with pytest.raises(ValueError, match="at least one"):
            op.process()

    def test_select_packet_strict_rejects_missing(self, simple_stream):
        op = SelectPacketColumns(columns=["nonexistent"], strict=True)
        with pytest.raises(Exception):
            op.process(simple_stream)

    def test_select_tag_strict_rejects_missing(self, simple_stream):
        op = SelectTagColumns(columns=["nonexistent"], strict=True)
        with pytest.raises(Exception):
            op.process(simple_stream)

    def test_drop_packet_strict_rejects_missing(self, simple_stream):
        op = DropPacketColumns(columns=["nonexistent"], strict=True)
        with pytest.raises(Exception):
            op.process(simple_stream)

    def test_drop_tag_strict_rejects_missing(self, simple_stream):
        op = DropTagColumns(columns=["nonexistent"], strict=True)
        with pytest.raises(Exception):
            op.process(simple_stream)


# ===================================================================
# Part 4 — Functional correctness
# ===================================================================


class TestPolarsFilterBehavior:
    def test_no_predicates_returns_all_rows(self, simple_stream):
        op = PolarsFilter()
        out = op.process(simple_stream)
        result = out.as_table()
        assert len(result) == 3

    def test_filter_reduces_rows(self, simple_stream):
        op = PolarsFilter(constraints={"legs": 4})
        out = op.process(simple_stream)
        result = out.as_table()
        assert len(result) == 2
        assert set(result.column("animal").to_pylist()) == {"cat", "dog"}

    def test_filter_preserves_schema(self, simple_stream):
        op = PolarsFilter(constraints={"legs": 4})
        tag_schema, packet_schema = op.output_schema(simple_stream)
        orig_tag, orig_pkt = simple_stream.output_schema()
        assert set(tag_schema.keys()) == set(orig_tag.keys())
        assert set(packet_schema.keys()) == set(orig_pkt.keys())


class TestSelectTagColumnsBehavior:
    def test_keeps_only_selected_tags(self, two_tag_stream):
        op = SelectTagColumns(columns=["region"])
        out = op.process(two_tag_stream)
        tag_keys, pkt_keys = out.keys()
        assert "region" in tag_keys
        assert "animal" not in tag_keys
        # packet columns unchanged
        assert "count" in pkt_keys

    def test_output_schema_matches_result(self, two_tag_stream):
        op = SelectTagColumns(columns=["region"])
        tag_schema, pkt_schema = op.output_schema(two_tag_stream)
        assert "region" in tag_schema
        assert "animal" not in tag_schema
        assert "count" in pkt_schema


class TestSelectPacketColumnsBehavior:
    def test_keeps_only_selected_packets(self, simple_stream):
        op = SelectPacketColumns(columns=["weight"])
        out = op.process(simple_stream)
        tag_keys, pkt_keys = out.keys()
        assert pkt_keys == ("weight",)
        assert "legs" not in pkt_keys
        # tag columns unchanged
        assert "animal" in tag_keys

    def test_output_schema_matches_result(self, simple_stream):
        op = SelectPacketColumns(columns=["weight"])
        tag_schema, pkt_schema = op.output_schema(simple_stream)
        assert "weight" in pkt_schema
        assert "legs" not in pkt_schema


class TestDropTagColumnsBehavior:
    def test_drops_specified_tags(self, two_tag_stream):
        op = DropTagColumns(columns=["region"])
        out = op.process(two_tag_stream)
        tag_keys, pkt_keys = out.keys()
        assert "region" not in tag_keys
        assert "animal" in tag_keys
        assert "count" in pkt_keys

    def test_output_schema_matches_result(self, two_tag_stream):
        op = DropTagColumns(columns=["region"])
        tag_schema, pkt_schema = op.output_schema(two_tag_stream)
        assert "region" not in tag_schema
        assert "animal" in tag_schema


class TestDropPacketColumnsBehavior:
    def test_drops_specified_packets(self, simple_stream):
        op = DropPacketColumns(columns=["legs"])
        out = op.process(simple_stream)
        tag_keys, pkt_keys = out.keys()
        assert "legs" not in pkt_keys
        assert "weight" in pkt_keys
        assert "animal" in tag_keys

    def test_output_schema_matches_result(self, simple_stream):
        op = DropPacketColumns(columns=["legs"])
        tag_schema, pkt_schema = op.output_schema(simple_stream)
        assert "legs" not in pkt_schema
        assert "weight" in pkt_schema


class TestMapPacketsBehavior:
    def test_renames_packet_column(self, simple_stream):
        op = MapPackets(name_map={"weight": "mass"})
        out = op.process(simple_stream)
        tag_keys, pkt_keys = out.keys()
        assert "mass" in pkt_keys
        assert "weight" not in pkt_keys
        # data preserved
        result = out.as_table()
        assert result.column("mass").to_pylist() == [4.0, 12.0, 0.5]

    def test_output_schema_reflects_rename(self, simple_stream):
        op = MapPackets(name_map={"weight": "mass"})
        tag_schema, pkt_schema = op.output_schema(simple_stream)
        assert "mass" in pkt_schema
        assert "weight" not in pkt_schema

    def test_collision_with_existing_column_raises(self, simple_stream):
        op = MapPackets(name_map={"weight": "legs"})
        with pytest.raises(Exception):
            op.process(simple_stream)


class TestMapTagsBehavior:
    def test_renames_tag_column(self, two_tag_stream):
        op = MapTags(name_map={"region": "area"})
        out = op.process(two_tag_stream)
        tag_keys, pkt_keys = out.keys()
        assert "area" in tag_keys
        assert "region" not in tag_keys
        # data preserved
        result = out.as_table()
        assert set(result.column("area").to_pylist()) == {"east", "west"}

    def test_output_schema_reflects_rename(self, two_tag_stream):
        op = MapTags(name_map={"region": "area"})
        tag_schema, pkt_schema = op.output_schema(two_tag_stream)
        assert "area" in tag_schema
        assert "region" not in tag_schema

    def test_collision_with_existing_tag_raises(self, two_tag_stream):
        op = MapTags(name_map={"region": "animal"})
        with pytest.raises(Exception):
            op.process(two_tag_stream)


class TestBatchBehavior:
    def test_batch_groups_rows(self, simple_stream):
        op = Batch(batch_size=2)
        out = op.process(simple_stream)
        result = out.as_table()
        # 3 rows batched by 2 → 2 batches (batch of 2 + partial batch of 1)
        assert len(result) == 2

    def test_batch_drop_partial(self, simple_stream):
        op = Batch(batch_size=2, drop_partial_batch=True)
        out = op.process(simple_stream)
        result = out.as_table()
        # 3 rows batched by 2 with drop → 1 batch
        assert len(result) == 1

    def test_batch_output_lineage(self, simple_stream):
        """Batch output stream should track its producer and upstreams via DynamicPodStream."""
        op = Batch(batch_size=2)
        out = op.process(simple_stream)
        assert out.producer is op
        assert simple_stream in out.upstreams

    def test_batch_size_zero_returns_single_batch(self, simple_stream):
        op = Batch(batch_size=0)
        out = op.process(simple_stream)
        result = out.as_table()
        # batch_size=0 → all rows in one batch
        assert len(result) == 1

    def test_negative_batch_size_raises(self):
        with pytest.raises(ValueError, match="non-negative"):
            Batch(batch_size=-1)


class TestJoinBehavior:
    def test_join_combines_streams_on_shared_tags(self, simple_stream, disjoint_stream):
        op = Join()
        out = op.process(simple_stream, disjoint_stream)
        result = out.as_table()
        # Both have 3 rows with same "animal" tags → inner join → 3 rows
        assert len(result) == 3
        # All columns present
        col_names = set(result.column_names)
        assert {"animal", "weight", "legs", "speed"}.issubset(col_names)

    def test_join_single_stream_passthrough(self, simple_stream):
        op = Join()
        out = op.process(simple_stream)
        result = out.as_table()
        orig = simple_stream.as_table()
        assert len(result) == len(orig)

    def test_join_output_schema(self, simple_stream, disjoint_stream):
        op = Join()
        tag_schema, pkt_schema = op.output_schema(simple_stream, disjoint_stream)
        assert "animal" in tag_schema
        assert "weight" in pkt_schema
        assert "speed" in pkt_schema

    def test_join_is_commutative(self, simple_stream, disjoint_stream):
        op = Join()
        sym = op.argument_symmetry([simple_stream, disjoint_stream])
        assert isinstance(sym, frozenset)


class TestJoinMetaColumnCollision:
    """Verify that a 3-way join with identical meta columns on all inputs does not
    raise a DuplicateError.  Instead, colliding meta columns should be renamed
    with stream-index-based suffixes (e.g. ``__computed_1``, ``__computed_2``)."""

    def _make_stream(self, id_vals, pkt_col, pkt_vals, meta_val):
        """Helper: stream with shared tag 'id', one packet column, and ``__computed``."""
        table = pa.table(
            {
                "id": pa.array(id_vals, type=pa.int64()),
                pkt_col: pa.array(pkt_vals, type=pa.int64()),
                "__computed": pa.array([meta_val] * len(id_vals), type=pa.bool_()),
            }
        )
        return ArrowTableStream(table, tag_columns=["id"])

    def test_three_way_join_with_shared_meta_column_succeeds(self):
        """Three streams each carrying ``__computed`` should join without DuplicateError."""
        s1 = self._make_stream([1, 2], "alpha", [10, 20], True)
        s2 = self._make_stream([1, 2], "beta", [100, 200], True)
        s3 = self._make_stream([1, 2], "gamma", [1000, 2000], True)

        result = Join().static_process(s1, s2, s3)
        table = result.as_table()

        assert len(table) == 2
        assert {"id", "alpha", "beta", "gamma"}.issubset(set(table.column_names))

    def test_three_way_join_meta_columns_renamed_with_index_suffix(self):
        """Colliding meta columns from streams 2+ get an index-based suffix."""
        s1 = self._make_stream([1, 2], "alpha", [10, 20], True)
        s2 = self._make_stream([1, 2], "beta", [100, 200], False)
        s3 = self._make_stream([1, 2], "gamma", [1000, 2000], True)

        result = Join().static_process(s1, s2, s3)
        table = result.as_table()
        col_names = set(table.column_names)

        # Original meta column preserved; colliding ones renamed with suffix
        assert "__computed" in col_names
        assert "__computed_1" in col_names
        assert "__computed_2" in col_names


class TestJoinOutputSchemaSystemTags:
    """Verify that Join.output_schema correctly predicts system tag columns."""

    def test_output_schema_excludes_system_tags_by_default(self):
        """Without system_tags=True, no system tag columns in tag schema."""
        from orcapod.core.sources.arrow_table_source import ArrowTableSource
        from orcapod.system_constants import constants

        src_a = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "alpha": pa.array([10, 20], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            infer_nullable=True,
        )
        src_b = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "beta": pa.array([100, 200], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            infer_nullable=True,
        )

        op = Join()
        tag_schema, _ = op.output_schema(src_a, src_b)

        for key in tag_schema:
            assert not key.startswith(constants.SYSTEM_TAG_PREFIX)

    def test_output_schema_includes_system_tags_when_requested(self):
        """With system_tags=True, tag schema should include system tag columns."""
        from orcapod.core.sources.arrow_table_source import ArrowTableSource
        from orcapod.system_constants import constants

        src_a = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "alpha": pa.array([10, 20], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            infer_nullable=True,
        )
        src_b = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "beta": pa.array([100, 200], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            infer_nullable=True,
        )

        op = Join()
        tag_schema, _ = op.output_schema(src_a, src_b, columns={"system_tags": True})

        sys_tag_keys = [
            k for k in tag_schema if k.startswith(constants.SYSTEM_TAG_PREFIX)
        ]
        assert len(sys_tag_keys) == 4  # 2 sources × 2 fields (source_id + record_id)

    def test_output_schema_system_tags_match_actual_output(self):
        """Predicted system tag column names must match the actual result."""
        from orcapod.core.sources.arrow_table_source import ArrowTableSource
        from orcapod.system_constants import constants

        src_a = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "alpha": pa.array([10, 20], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            infer_nullable=True,
        )
        src_b = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "beta": pa.array([100, 200], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            infer_nullable=True,
        )

        op = Join()

        # Predicted
        tag_schema, _ = op.output_schema(src_a, src_b, columns={"system_tags": True})
        predicted = sorted(
            k for k in tag_schema if k.startswith(constants.SYSTEM_TAG_PREFIX)
        )

        # Actual
        result = op.static_process(src_a, src_b)
        result_table = result.as_table(columns={"system_tags": True})
        actual = sorted(
            c
            for c in result_table.column_names
            if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )

        assert predicted == actual

    def test_output_schema_system_tags_three_way_join(self):
        """Three-way join should predict 3 system tag columns."""
        from orcapod.core.sources.arrow_table_source import ArrowTableSource
        from orcapod.system_constants import constants

        src_a = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "alpha": pa.array([10, 20], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            infer_nullable=True,
        )
        src_b = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "beta": pa.array([100, 200], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            infer_nullable=True,
        )
        src_c = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "gamma": pa.array([1000, 2000], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            infer_nullable=True,
        )

        op = Join()

        # Predicted
        tag_schema, _ = op.output_schema(
            src_a, src_b, src_c, columns={"system_tags": True}
        )
        predicted = sorted(
            k for k in tag_schema if k.startswith(constants.SYSTEM_TAG_PREFIX)
        )

        # Actual
        result = op.static_process(src_a, src_b, src_c)
        actual = sorted(
            c
            for c in result.as_table(columns={"system_tags": True}).column_names
            if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )

        assert len(predicted) == 6  # 3 sources × 2 fields
        assert predicted == actual

    def test_output_schema_single_stream_passthrough(self, simple_stream):
        """Single stream should pass through output_schema including system_tags."""
        op = Join()
        result_default = op.output_schema(simple_stream)
        result_sys = op.output_schema(simple_stream, columns={"system_tags": True})
        # Single stream delegates to stream's output_schema
        assert result_default == simple_stream.output_schema()
        assert result_sys == simple_stream.output_schema(columns={"system_tags": True})

    def test_predicted_schema_matches_result_stream_schema(self):
        """Operator's predicted output_schema must equal the result stream's
        output_schema — both tag and packet schemas, without system tags."""
        from orcapod.core.sources.arrow_table_source import ArrowTableSource

        src_a = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "alpha": pa.array([10, 20], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            infer_nullable=True,
        )
        src_b = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "beta": pa.array([100, 200], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            infer_nullable=True,
        )

        op = Join()

        predicted_tag, predicted_pkt = op.output_schema(src_a, src_b)
        result = op.static_process(src_a, src_b)
        actual_tag, actual_pkt = result.output_schema()

        assert dict(predicted_tag) == dict(actual_tag)
        assert dict(predicted_pkt) == dict(actual_pkt)

    def test_predicted_schema_matches_result_stream_schema_with_system_tags(self):
        """Operator's predicted output_schema(system_tags=True) must equal
        the result stream's output_schema(system_tags=True)."""
        from orcapod.core.sources.arrow_table_source import ArrowTableSource

        src_a = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "alpha": pa.array([10, 20], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            infer_nullable=True,
        )
        src_b = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "beta": pa.array([100, 200], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            infer_nullable=True,
        )

        op = Join()

        predicted_tag, predicted_pkt = op.output_schema(
            src_a, src_b, columns={"system_tags": True}
        )
        result = op.static_process(src_a, src_b)
        actual_tag, actual_pkt = result.output_schema(columns={"system_tags": True})

        assert dict(predicted_tag) == dict(actual_tag)
        assert dict(predicted_pkt) == dict(actual_pkt)


class TestSemiJoinBehavior:
    def test_semijoin_filters_left_by_right(self, left_stream, right_stream):
        op = SemiJoin()
        out = op.process(left_stream, right_stream)
        result = out.as_table()
        # left has id=[1,2,3], right has id=[2,3,4] → semi join keeps id=[2,3]
        assert len(result) == 2
        assert set(result.column("id").to_pylist()) == {2, 3}

    def test_semijoin_preserves_left_schema(self, left_stream, right_stream):
        op = SemiJoin()
        tag_schema, pkt_schema = op.output_schema(left_stream, right_stream)
        left_tag, left_pkt = left_stream.output_schema()
        assert set(tag_schema.keys()) == set(left_tag.keys())
        assert set(pkt_schema.keys()) == set(left_pkt.keys())

    def test_semijoin_is_not_commutative(self, left_stream, right_stream):
        op = SemiJoin()
        sym = op.argument_symmetry([left_stream, right_stream])
        assert isinstance(sym, tuple)


# ===================================================================
# Part 5 — Identity structure
# ===================================================================


class TestIdentityStructure:
    """Operators with different parameters must have different content hashes."""

    def test_polars_filter_different_params_different_hash(self):
        a = PolarsFilter(constraints={"x": 1})
        b = PolarsFilter(constraints={"x": 2})
        assert a.content_hash() != b.content_hash()

    def test_select_tag_columns_different_params_different_hash(self):
        a = SelectTagColumns(columns=["x"])
        b = SelectTagColumns(columns=["y"])
        assert a.content_hash() != b.content_hash()

    def test_select_packet_columns_different_params_different_hash(self):
        a = SelectPacketColumns(columns=["x"])
        b = SelectPacketColumns(columns=["y"])
        assert a.content_hash() != b.content_hash()

    def test_drop_tag_columns_different_params_different_hash(self):
        a = DropTagColumns(columns=["x"])
        b = DropTagColumns(columns=["y"])
        assert a.content_hash() != b.content_hash()

    def test_drop_packet_columns_different_params_different_hash(self):
        a = DropPacketColumns(columns=["x"])
        b = DropPacketColumns(columns=["y"])
        assert a.content_hash() != b.content_hash()

    def test_map_packets_different_params_different_hash(self):
        a = MapPackets(name_map={"a": "b"})
        b = MapPackets(name_map={"a": "c"})
        assert a.content_hash() != b.content_hash()

    def test_map_tags_different_params_different_hash(self):
        a = MapTags(name_map={"a": "b"})
        b = MapTags(name_map={"a": "c"})
        assert a.content_hash() != b.content_hash()

    def test_batch_different_params_different_hash(self):
        a = Batch(batch_size=2)
        b = Batch(batch_size=5)
        assert a.content_hash() != b.content_hash()


# ===================================================================
# Part 6 — Argument symmetry: raw symmetry type
# ===================================================================


class TestArgumentSymmetryType:
    """Each operator must declare the correct argument symmetry type.

    Unary operators always return a single-element tuple (ordered).
    Commutative binary/n-ary operators return a frozenset.
    Non-commutative binary operators return a tuple preserving order.
    """

    # --- Unary operators: all return (stream,) ---

    def test_polars_filter_argument_symmetry(self, simple_stream):
        op = PolarsFilter()
        sym = op.argument_symmetry([simple_stream])
        assert isinstance(sym, tuple)
        assert sym == (simple_stream,)

    def test_select_tag_columns_argument_symmetry(self, two_tag_stream):
        op = SelectTagColumns(columns=["region"])
        sym = op.argument_symmetry([two_tag_stream])
        assert isinstance(sym, tuple)
        assert sym == (two_tag_stream,)

    def test_select_packet_columns_argument_symmetry(self, simple_stream):
        op = SelectPacketColumns(columns=["weight"])
        sym = op.argument_symmetry([simple_stream])
        assert isinstance(sym, tuple)
        assert sym == (simple_stream,)

    def test_drop_tag_columns_argument_symmetry(self, two_tag_stream):
        op = DropTagColumns(columns=["region"])
        sym = op.argument_symmetry([two_tag_stream])
        assert isinstance(sym, tuple)
        assert sym == (two_tag_stream,)

    def test_drop_packet_columns_argument_symmetry(self, simple_stream):
        op = DropPacketColumns(columns=["legs"])
        sym = op.argument_symmetry([simple_stream])
        assert isinstance(sym, tuple)
        assert sym == (simple_stream,)

    def test_map_packets_argument_symmetry(self, simple_stream):
        op = MapPackets(name_map={"weight": "mass"})
        sym = op.argument_symmetry([simple_stream])
        assert isinstance(sym, tuple)
        assert sym == (simple_stream,)

    def test_map_tags_argument_symmetry(self, two_tag_stream):
        op = MapTags(name_map={"region": "area"})
        sym = op.argument_symmetry([two_tag_stream])
        assert isinstance(sym, tuple)
        assert sym == (two_tag_stream,)

    def test_batch_argument_symmetry(self, simple_stream):
        op = Batch(batch_size=2)
        sym = op.argument_symmetry([simple_stream])
        assert isinstance(sym, tuple)
        assert sym == (simple_stream,)

    # --- Join: commutative → frozenset ---

    def test_join_argument_symmetry_is_frozenset(self, simple_stream, disjoint_stream):
        op = Join()
        sym = op.argument_symmetry([simple_stream, disjoint_stream])
        assert isinstance(sym, frozenset)
        assert sym == frozenset([simple_stream, disjoint_stream])

    def test_join_argument_symmetry_order_invariant(
        self, simple_stream, disjoint_stream
    ):
        op = Join()
        sym_ab = op.argument_symmetry([simple_stream, disjoint_stream])
        sym_ba = op.argument_symmetry([disjoint_stream, simple_stream])
        assert sym_ab == sym_ba

    # --- SemiJoin: non-commutative → tuple (order preserved) ---

    def test_semijoin_argument_symmetry_is_tuple(self, left_stream, right_stream):
        op = SemiJoin()
        sym = op.argument_symmetry([left_stream, right_stream])
        assert isinstance(sym, tuple)
        assert sym == (left_stream, right_stream)

    def test_semijoin_argument_symmetry_order_matters(self, left_stream, right_stream):
        op = SemiJoin()
        sym_lr = op.argument_symmetry([left_stream, right_stream])
        sym_rl = op.argument_symmetry([right_stream, left_stream])
        assert sym_lr != sym_rl


# ===================================================================
# Part 7 — Argument symmetry: identity_structure and content_hash
# ===================================================================


class TestArgumentSymmetryIdentity:
    """Verify that argument symmetry is correctly reflected in both
    identity_structure / content_hash (content-level) and
    pipeline_identity_structure / pipeline_hash (pipeline-level)
    for the output DynamicPodStream of every operator.

    For each unary operator: the output stream's identity/pipeline structures
    must include the operator and the input stream.

    For commutative operators (Join): swapping inputs must produce the same
    identity_structure, content_hash, pipeline_identity_structure, pipeline_hash.

    For non-commutative operators (SemiJoin): swapping inputs must produce
    different values for all four.
    """

    # --- Unary operators: identity includes (op, (stream,)) ---

    def _check_unary_identity(self, op, stream):
        """Shared assertions for any unary operator."""
        out = op.process(stream)
        id_struct = out.identity_structure()
        pipe_struct = out.pipeline_identity_structure()

        # structure is (pod, argument_symmetry(upstreams))
        assert id_struct[0] is op
        assert isinstance(id_struct[1], tuple)  # unary → ordered tuple
        assert id_struct[1] == (stream,)

        # pipeline mirrors content identity
        assert pipe_struct[0] is op
        assert isinstance(pipe_struct[1], tuple)
        assert pipe_struct[1] == (stream,)

        # hashes are deterministic
        out2 = op.process(stream)
        assert out.content_hash() == out2.content_hash()
        assert out.pipeline_hash() == out2.pipeline_hash()

    def test_polars_filter_identity(self, simple_stream):
        self._check_unary_identity(PolarsFilter(), simple_stream)

    def test_select_tag_columns_identity(self, two_tag_stream):
        self._check_unary_identity(SelectTagColumns(columns=["region"]), two_tag_stream)

    def test_select_packet_columns_identity(self, simple_stream):
        self._check_unary_identity(
            SelectPacketColumns(columns=["weight"]), simple_stream
        )

    def test_drop_tag_columns_identity(self, two_tag_stream):
        self._check_unary_identity(DropTagColumns(columns=["region"]), two_tag_stream)

    def test_drop_packet_columns_identity(self, simple_stream):
        self._check_unary_identity(DropPacketColumns(columns=["legs"]), simple_stream)

    def test_map_packets_identity(self, simple_stream):
        self._check_unary_identity(
            MapPackets(name_map={"weight": "mass"}), simple_stream
        )

    def test_map_tags_identity(self, two_tag_stream):
        self._check_unary_identity(MapTags(name_map={"region": "area"}), two_tag_stream)

    def test_batch_identity(self, simple_stream):
        self._check_unary_identity(Batch(batch_size=2), simple_stream)

    # --- Join: commutative — swap must be invisible to hashes ---

    def test_join_swapped_inputs_same_identity_structure(
        self, simple_stream, disjoint_stream
    ):
        op = Join()
        out_ab = op.process(simple_stream, disjoint_stream)
        out_ba = op.process(disjoint_stream, simple_stream)
        assert out_ab.identity_structure() == out_ba.identity_structure()

    def test_join_swapped_inputs_same_content_hash(
        self, simple_stream, disjoint_stream
    ):
        op = Join()
        out_ab = op.process(simple_stream, disjoint_stream)
        out_ba = op.process(disjoint_stream, simple_stream)
        assert out_ab.content_hash() == out_ba.content_hash()

    def test_join_swapped_inputs_same_pipeline_identity_structure(
        self, simple_stream, disjoint_stream
    ):
        op = Join()
        out_ab = op.process(simple_stream, disjoint_stream)
        out_ba = op.process(disjoint_stream, simple_stream)
        assert (
            out_ab.pipeline_identity_structure() == out_ba.pipeline_identity_structure()
        )

    def test_join_swapped_inputs_same_pipeline_hash(
        self, simple_stream, disjoint_stream
    ):
        op = Join()
        out_ab = op.process(simple_stream, disjoint_stream)
        out_ba = op.process(disjoint_stream, simple_stream)
        assert out_ab.pipeline_hash() == out_ba.pipeline_hash()

    # --- SemiJoin: non-commutative — swap must change hashes ---

    def test_semijoin_swapped_inputs_different_identity_structure(
        self, left_stream, right_stream
    ):
        op = SemiJoin()
        out_lr = op.process(left_stream, right_stream)
        out_rl = op.process(right_stream, left_stream)
        assert out_lr.identity_structure() != out_rl.identity_structure()

    def test_semijoin_swapped_inputs_different_content_hash(
        self, left_stream, right_stream
    ):
        op = SemiJoin()
        out_lr = op.process(left_stream, right_stream)
        out_rl = op.process(right_stream, left_stream)
        assert out_lr.content_hash() != out_rl.content_hash()

    def test_semijoin_swapped_inputs_different_pipeline_identity_structure(
        self, left_stream, right_stream
    ):
        op = SemiJoin()
        out_lr = op.process(left_stream, right_stream)
        out_rl = op.process(right_stream, left_stream)
        assert (
            out_lr.pipeline_identity_structure() != out_rl.pipeline_identity_structure()
        )

    def test_semijoin_swapped_inputs_different_pipeline_hash(
        self, left_stream, right_stream
    ):
        op = SemiJoin()
        out_lr = op.process(left_stream, right_stream)
        out_rl = op.process(right_stream, left_stream)
        assert out_lr.pipeline_hash() != out_rl.pipeline_hash()


# ---------------------------------------------------------------------------
# System Tag Name-Extension Tests
# ---------------------------------------------------------------------------


class TestJoinSystemTagNameExtension:
    """Verify that Join uses pipeline_hash (structure-only) for system tag
    name-extension, not content_hash (data-inclusive).

    Uses ArrowTableSource to ensure system tag columns are present (raw
    ArrowTableStream has no system tags)."""

    def test_same_schema_different_data_produces_same_system_tag_names(self):
        """Two sources with same schema but different data should produce
        the same system tag column names after Join, because system tag
        name-extension uses pipeline_hash (structure-only)."""
        from orcapod.core.sources.arrow_table_source import ArrowTableSource
        from orcapod.system_constants import constants

        src_left1 = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value_a": pa.array([10, 20], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            infer_nullable=True,
        )
        src_left2 = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value_a": pa.array([100, 200], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            infer_nullable=True,
        )
        src_right = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value_b": pa.array([30, 40], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            infer_nullable=True,
        )

        op = Join()
        result1 = op.static_process(src_left1, src_right)
        result2 = op.static_process(src_left2, src_right)

        result1_table = result1.as_table(columns={"system_tags": True})
        result2_table = result2.as_table(columns={"system_tags": True})

        sys_cols_1 = sorted(
            c
            for c in result1_table.column_names
            if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )
        sys_cols_2 = sorted(
            c
            for c in result2_table.column_names
            if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )

        # Column names should be identical (structure-only hashing)
        assert len(sys_cols_1) > 0, "Expected system tag columns to be present"
        assert sys_cols_1 == sys_cols_2

    def test_different_schema_produces_different_system_tag_names(self):
        """Two sources with different packet schemas should produce different
        system tag column names after Join."""
        from orcapod.core.sources.arrow_table_source import ArrowTableSource
        from orcapod.system_constants import constants

        src_left = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value_a": pa.array([10, 20], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            infer_nullable=True,
        )
        src_right_int = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value_b": pa.array([30, 40], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            infer_nullable=True,
        )
        src_right_str = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "value_c": pa.array(["a", "b"]),
                }
            ),
            tag_columns=["id"],
            infer_nullable=True,
        )

        op = Join()
        result1 = op.static_process(src_left, src_right_int)
        result2 = op.static_process(src_left, src_right_str)

        result1_table = result1.as_table(columns={"system_tags": True})
        result2_table = result2.as_table(columns={"system_tags": True})

        sys_cols_1 = sorted(
            c
            for c in result1_table.column_names
            if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )
        sys_cols_2 = sorted(
            c
            for c in result2_table.column_names
            if c.startswith(constants.SYSTEM_TAG_PREFIX)
        )

        # Column names should differ (different pipeline structures)
        assert len(sys_cols_1) > 0, "Expected system tag columns to be present"
        assert sys_cols_1 != sys_cols_2


class TestSourceSystemTagSchemaHash:
    """Verify that source system tag column name uses a hash consistent
    with the source's pipeline_hash."""

    def test_source_schema_hash_matches_pipeline_hash(self):
        """ArrowTableSource._schema_hash should match the truncated
        pipeline_hash, since both hash (tag_schema, packet_schema)."""
        from orcapod.core.sources.arrow_table_source import ArrowTableSource

        table = pa.table(
            {
                "id": pa.array([1, 2, 3], type=pa.int64()),
                "x": pa.array([10, 20, 30], type=pa.int64()),
            }
        )
        source = ArrowTableSource(table, tag_columns=["id"], infer_nullable=True)
        schema_hash = source._schema_hash
        pipeline_hash_hex = source.pipeline_hash().to_hex(char_count=len(schema_hash))
        assert schema_hash == pipeline_hash_hex


class TestJoinSystemTagCanonicalOrdering:
    """Verify that Join canonically orders streams by pipeline_hash,
    and that the resulting system tag columns reflect this ordering
    with canonical position indices (0, 1, 2, ...)."""

    @pytest.fixture
    def three_sources(self):
        """Three ArrowTableSources with distinct packet schemas sharing tag 'id'."""
        from orcapod.core.sources.arrow_table_source import ArrowTableSource

        src_a = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "alpha": pa.array([10, 20], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            infer_nullable=True,
        )
        src_b = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "beta": pa.array([100, 200], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            infer_nullable=True,
        )
        src_c = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "gamma": pa.array([1000, 2000], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            infer_nullable=True,
        )
        return src_a, src_b, src_c

    @staticmethod
    def _get_system_tag_columns(table, constants):
        """Extract system tag column names in their natural table order."""
        return [
            c for c in table.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
        ]

    @staticmethod
    def _parse_system_tag_column(col, constants):
        """Parse a system tag column name into (field_type, schema_hash, stream_hash, index).

        Column format after join::

            _tag_{field_type}::{schema_hash}::{stream_hash}:{canonical_index}

        Blocks are separated by ``::`` (block separator).
        Fields within a block are separated by ``:`` (field separator).
        """
        after_prefix = col[len(constants.SYSTEM_TAG_PREFIX) :]
        blocks = after_prefix.split(constants.BLOCK_SEPARATOR)
        field_type = blocks[0]
        schema_hash = blocks[1]
        join_block_fields = blocks[2].split(constants.FIELD_SEPARATOR)
        stream_hash = join_block_fields[0]
        index = join_block_fields[1]
        return field_type, schema_hash, stream_hash, index

    def test_three_way_join_produces_six_system_tag_columns(self, three_sources):
        from orcapod.system_constants import constants

        src_a, src_b, src_c = three_sources
        op = Join()
        result = op.static_process(src_a, src_b, src_c)
        result_table = result.as_table(columns={"system_tags": True})
        sys_cols = self._get_system_tag_columns(result_table, constants)
        assert len(sys_cols) == 6  # 3 sources × 2 fields (source_id + record_id)

    def test_system_tag_position_maps_to_correct_source(self, three_sources):
        """Each system tag column should carry the canonical position index
        matching the source's rank when sorted by pipeline_hash.

        Independently sorts sources by pipeline_hash to determine expected
        position → source mapping, then verifies each source_id column has:
        - schema_hash matching the original source's schema_hash
        - stream_hash matching the input stream's pipeline_hash
        - canonical index matching the position"""
        from orcapod.config import Config
        from orcapod.system_constants import constants

        src_a, src_b, src_c = three_sources
        n_char = Config().system_tag_hash_n_char

        # Independently determine expected position → source mapping
        sources = [src_a, src_b, src_c]
        sorted_sources = sorted(sources, key=lambda s: s.pipeline_hash().to_hex())

        op = Join()
        result = op.static_process(src_a, src_b, src_c)
        result_table = result.as_table(columns={"system_tags": True})
        sys_cols = self._get_system_tag_columns(result_table, constants)

        # Filter to source_id columns for position checking
        sid_cols = [
            c for c in sys_cols if c.startswith(constants.SYSTEM_TAG_SOURCE_ID_PREFIX)
        ]
        assert len(sid_cols) == 3

        for expected_idx, expected_source in enumerate(sorted_sources):
            field_type, schema_hash, stream_hash, index_str = (
                self._parse_system_tag_column(sid_cols[expected_idx], constants)
            )
            # The schema_hash identifies the originating source
            assert schema_hash == expected_source._schema_hash, (
                f"Position {expected_idx}: expected schema_hash "
                f"{expected_source._schema_hash!r}, got {schema_hash!r}"
            )
            # For direct source→join, stream_hash == source's pipeline_hash
            expected_stream_hash = expected_source.pipeline_hash().to_hex(n_char)
            assert stream_hash == expected_stream_hash, (
                f"Position {expected_idx}: expected stream_hash "
                f"{expected_stream_hash!r}, got {stream_hash!r}"
            )
            # The canonical position index
            assert index_str == str(expected_idx), (
                f"Position {expected_idx}: expected index {expected_idx!r}, "
                f"got {index_str!r}"
            )

    def test_swapped_input_order_produces_identical_system_tags(self, three_sources):
        """Join is commutative — any permutation of inputs should produce
        the same system tag column names in the same order."""
        from orcapod.system_constants import constants

        src_a, src_b, src_c = three_sources
        op = Join()

        result_abc = op.static_process(src_a, src_b, src_c)
        result_cab = op.static_process(src_c, src_a, src_b)
        result_bca = op.static_process(src_b, src_c, src_a)

        sys_abc = self._get_system_tag_columns(
            result_abc.as_table(columns={"system_tags": True}), constants
        )
        sys_cab = self._get_system_tag_columns(
            result_cab.as_table(columns={"system_tags": True}), constants
        )
        sys_bca = self._get_system_tag_columns(
            result_bca.as_table(columns={"system_tags": True}), constants
        )

        assert sys_abc == sys_cab
        assert sys_abc == sys_bca

    def test_system_tag_values_are_per_row_source_provenance(self, three_sources):
        """System tag column values should reflect the source provenance.
        source_id columns contain the source_id, record_id columns contain the record_id."""
        from orcapod.system_constants import constants

        src_a, src_b, src_c = three_sources
        op = Join()
        result = op.static_process(src_a, src_b, src_c)
        result_table = result.as_table(columns={"system_tags": True})
        sys_cols = self._get_system_tag_columns(result_table, constants)

        for col in sys_cols:
            values = result_table.column(col).to_pylist()
            assert len(values) == result_table.num_rows
            for val in values:
                assert isinstance(val, str)
                assert len(val) > 0

    def test_intermediate_operators_produce_different_stream_hash(self):
        """When sources pass through intermediate operators before Join,
        the schema_hash (from origin source) and stream_hash (from the
        operator output) should differ in the system tag column name.

        Column format: _tag_{field_type}::{schema_hash}::{stream_hash}:{index}

        With an intermediate MapPackets, stream_hash comes from the
        DynamicPodStream which has a different pipeline_hash than the
        original source."""
        from orcapod.config import Config
        from orcapod.core.sources.arrow_table_source import ArrowTableSource
        from orcapod.system_constants import constants

        n_char = Config().system_tag_hash_n_char

        src_a = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "alpha": pa.array([10, 20], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            infer_nullable=True,
        )
        src_b = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "beta": pa.array([100, 200], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            infer_nullable=True,
        )
        src_c = ArrowTableSource(
            pa.table(
                {
                    "id": pa.array([1, 2], type=pa.int64()),
                    "gamma": pa.array([1000, 2000], type=pa.int64()),
                }
            ),
            tag_columns=["id"],
            infer_nullable=True,
        )

        # Pass each source through an intermediate operator
        map_a = MapPackets({"alpha": "a_renamed"})
        map_b = MapPackets({"beta": "b_renamed"})
        map_c = MapPackets({"gamma": "c_renamed"})

        stream_a = map_a.static_process(src_a)
        stream_b = map_b.static_process(src_b)
        stream_c = map_c.static_process(src_c)

        # Verify intermediate streams have different pipeline_hash from sources
        assert stream_a.pipeline_hash() != src_a.pipeline_hash()
        assert stream_b.pipeline_hash() != src_b.pipeline_hash()
        assert stream_c.pipeline_hash() != src_c.pipeline_hash()

        # Join the intermediate streams
        op = Join()
        result = op.static_process(stream_a, stream_b, stream_c)
        result_table = result.as_table(columns={"system_tags": True})
        sys_cols = self._get_system_tag_columns(result_table, constants)

        assert len(sys_cols) == 6  # 3 sources × 2 fields

        # Filter to source_id columns for position checking
        sid_cols = [
            c for c in sys_cols if c.startswith(constants.SYSTEM_TAG_SOURCE_ID_PREFIX)
        ]
        assert len(sid_cols) == 3

        # Independently determine expected canonical ordering
        streams = [stream_a, stream_b, stream_c]
        original_sources = [src_a, src_b, src_c]
        # Map each stream back to its original source for verification
        stream_to_source = dict(zip(streams, original_sources))

        sorted_streams = sorted(streams, key=lambda s: s.pipeline_hash().to_hex())

        for expected_idx, expected_stream in enumerate(sorted_streams):
            expected_source = stream_to_source[expected_stream]
            field_type, schema_hash, stream_hash, index_str = (
                self._parse_system_tag_column(sid_cols[expected_idx], constants)
            )

            # schema_hash should match the original source's schema_hash
            expected_schema_hash = expected_source._schema_hash
            assert schema_hash == expected_schema_hash, (
                f"Position {expected_idx}: expected schema_hash "
                f"{expected_schema_hash!r}, got {schema_hash!r}"
            )

            # stream_hash should match the intermediate stream's pipeline_hash
            # (different from schema_hash due to the MapPackets operator)
            expected_stream_hash = expected_stream.pipeline_hash().to_hex(n_char)
            assert stream_hash == expected_stream_hash, (
                f"Position {expected_idx}: expected stream_hash "
                f"{expected_stream_hash!r}, got {stream_hash!r}"
            )

            # schema_hash and stream_hash should differ
            assert schema_hash != stream_hash, (
                f"Position {expected_idx}: schema_hash and stream_hash "
                f"should differ with an intermediate operator"
            )

            # canonical position index
            assert index_str == str(expected_idx)


class TestSortSystemTagValues:
    """Tests for the sort_system_tag_values utility that ensures commutativity
    by sorting paired (source_id, record_id) system tag values per row."""

    @staticmethod
    def _make_paired_cols(constants, provenance_path, position):
        """Build paired source_id/record_id column names for a given provenance path and position."""
        sid = f"{constants.SYSTEM_TAG_SOURCE_ID_PREFIX}{constants.BLOCK_SEPARATOR}{provenance_path}{constants.FIELD_SEPARATOR}{position}"
        rid = f"{constants.SYSTEM_TAG_RECORD_ID_PREFIX}{constants.BLOCK_SEPARATOR}{provenance_path}{constants.FIELD_SEPARATOR}{position}"
        return sid, rid

    def test_sorts_paired_values_across_same_provenance_path(self):
        """Paired (source_id, record_id) columns sharing a provenance path
        should have their values sorted per row by (source_id, record_id) tuples."""
        from orcapod.system_constants import constants
        from orcapod.utils.arrow_data_utils import sort_system_tag_values

        sid_0, rid_0 = self._make_paired_cols(constants, "abc::ph123", "0")
        sid_1, rid_1 = self._make_paired_cols(constants, "abc::ph123", "1")

        table = pa.table(
            {
                "id": [1, 2],
                sid_0: pa.array(["zzz_source", "aaa_source"], type=pa.large_string()),
                rid_0: pa.array(["row_0", "row_0"], type=pa.large_string()),
                sid_1: pa.array(["aaa_source", "zzz_source"], type=pa.large_string()),
                rid_1: pa.array(["row_1", "row_1"], type=pa.large_string()),
            }
        )

        result = sort_system_tag_values(table)

        # After sorting by (source_id, record_id), position :0 should have the smaller tuple
        # Row 0: ("zzz_source", "row_0") vs ("aaa_source", "row_1") → sorted: aaa first
        assert result.column(sid_0).to_pylist()[0] == "aaa_source"
        assert result.column(rid_0).to_pylist()[0] == "row_1"
        assert result.column(sid_1).to_pylist()[0] == "zzz_source"
        assert result.column(rid_1).to_pylist()[0] == "row_0"

        # Row 1: ("aaa_source", "row_0") vs ("zzz_source", "row_1") → already sorted
        assert result.column(sid_0).to_pylist()[1] == "aaa_source"
        assert result.column(rid_0).to_pylist()[1] == "row_0"
        assert result.column(sid_1).to_pylist()[1] == "zzz_source"
        assert result.column(rid_1).to_pylist()[1] == "row_1"

    def test_does_not_sort_different_provenance_paths(self):
        """Columns with different provenance paths should NOT have their values sorted."""
        from orcapod.system_constants import constants
        from orcapod.utils.arrow_data_utils import sort_system_tag_values

        # Two different provenance paths (different pipeline hashes)
        sid_a, rid_a = self._make_paired_cols(constants, "abc::ph_AAA", "0")
        sid_b, rid_b = self._make_paired_cols(constants, "abc::ph_BBB", "0")

        table = pa.table(
            {
                "id": [1],
                sid_a: pa.array(["zzz"], type=pa.large_string()),
                rid_a: pa.array(["row_0"], type=pa.large_string()),
                sid_b: pa.array(["aaa"], type=pa.large_string()),
                rid_b: pa.array(["row_1"], type=pa.large_string()),
            }
        )

        result = sort_system_tag_values(table)

        # Values should be untouched since provenance paths differ
        assert result.column(sid_a).to_pylist() == ["zzz"]
        assert result.column(sid_b).to_pylist() == ["aaa"]

    def test_no_op_for_single_position_groups(self):
        """Groups with only one position should be left untouched."""
        from orcapod.system_constants import constants
        from orcapod.utils.arrow_data_utils import sort_system_tag_values

        sid, rid = self._make_paired_cols(constants, "abc::ph123", "0")

        table = pa.table(
            {
                "id": [1, 2],
                sid: pa.array(["hello", "world"], type=pa.large_string()),
                rid: pa.array(["row_0", "row_1"], type=pa.large_string()),
            }
        )

        result = sort_system_tag_values(table)
        assert result.column(sid).to_pylist() == ["hello", "world"]
        assert result.column(rid).to_pylist() == ["row_0", "row_1"]

    def test_preserves_non_system_tag_columns(self):
        """Non-system-tag columns should be completely unaffected."""
        from orcapod.system_constants import constants
        from orcapod.utils.arrow_data_utils import sort_system_tag_values

        sid_0, rid_0 = self._make_paired_cols(constants, "abc::ph123", "0")
        sid_1, rid_1 = self._make_paired_cols(constants, "abc::ph123", "1")

        table = pa.table(
            {
                "id": [1, 2],
                "data": ["foo", "bar"],
                sid_0: pa.array(["zzz", "aaa"], type=pa.large_string()),
                rid_0: pa.array(["r0", "r0"], type=pa.large_string()),
                sid_1: pa.array(["aaa", "zzz"], type=pa.large_string()),
                rid_1: pa.array(["r1", "r1"], type=pa.large_string()),
            }
        )

        result = sort_system_tag_values(table)
        assert result.column("id").to_pylist() == [1, 2]
        assert result.column("data").to_pylist() == ["foo", "bar"]

    def test_three_way_group_sorts_correctly(self):
        """Three positions sharing the same provenance path should all be sorted together."""
        from orcapod.system_constants import constants
        from orcapod.utils.arrow_data_utils import sort_system_tag_values

        sid_0, rid_0 = self._make_paired_cols(constants, "abc::ph123", "0")
        sid_1, rid_1 = self._make_paired_cols(constants, "abc::ph123", "1")
        sid_2, rid_2 = self._make_paired_cols(constants, "abc::ph123", "2")

        table = pa.table(
            {
                sid_0: pa.array(["cherry", "banana"], type=pa.large_string()),
                rid_0: pa.array(["r0", "r0"], type=pa.large_string()),
                sid_1: pa.array(["apple", "cherry"], type=pa.large_string()),
                rid_1: pa.array(["r1", "r1"], type=pa.large_string()),
                sid_2: pa.array(["banana", "apple"], type=pa.large_string()),
                rid_2: pa.array(["r2", "r2"], type=pa.large_string()),
            }
        )

        result = sort_system_tag_values(table)

        # Row 0: tuples are (cherry,r0), (apple,r1), (banana,r2) → sorted: (apple,r1), (banana,r2), (cherry,r0)
        assert result.column(sid_0).to_pylist()[0] == "apple"
        assert result.column(rid_0).to_pylist()[0] == "r1"
        assert result.column(sid_1).to_pylist()[0] == "banana"
        assert result.column(rid_1).to_pylist()[0] == "r2"
        assert result.column(sid_2).to_pylist()[0] == "cherry"
        assert result.column(rid_2).to_pylist()[0] == "r0"

        # Row 1: tuples are (banana,r0), (cherry,r1), (apple,r2) → sorted: (apple,r2), (banana,r0), (cherry,r1)
        assert result.column(sid_0).to_pylist()[1] == "apple"
        assert result.column(rid_0).to_pylist()[1] == "r2"
        assert result.column(sid_1).to_pylist()[1] == "banana"
        assert result.column(rid_1).to_pylist()[1] == "r0"
        assert result.column(sid_2).to_pylist()[1] == "cherry"
        assert result.column(rid_2).to_pylist()[1] == "r1"
