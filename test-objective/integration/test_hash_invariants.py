"""Specification-derived integration tests for hash stability and Merkle chain properties.

Tests the two parallel identity chains documented in the design spec:
1. content_hash() — data-inclusive, changes when data changes
2. pipeline_hash() — schema+topology only, ignores data content
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import Join, SemiJoin
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _double(x: int) -> int:
    return x * 2


def _make_source(data: dict, tag_columns: list[str]) -> ArrowTableSource:
    table = pa.table(data)
    return ArrowTableSource(table, tag_columns=tag_columns)


# ===================================================================
# Content hash stability
# ===================================================================


class TestContentHashStability:
    """Per design: content_hash is deterministic — identical data produces
    identical hash across runs."""

    def test_same_data_same_hash(self):
        s1 = ArrowTableStream(
            pa.table({"id": [1, 2], "x": [10, 20]}), tag_columns=["id"]
        )
        s2 = ArrowTableStream(
            pa.table({"id": [1, 2], "x": [10, 20]}), tag_columns=["id"]
        )
        assert s1.content_hash() == s2.content_hash()

    def test_different_data_different_hash(self):
        s1 = ArrowTableStream(
            pa.table({"id": [1, 2], "x": [10, 20]}), tag_columns=["id"]
        )
        s2 = ArrowTableStream(
            pa.table({"id": [1, 2], "x": [10, 99]}), tag_columns=["id"]
        )
        assert s1.content_hash() != s2.content_hash()


# ===================================================================
# Pipeline hash properties
# ===================================================================


class TestPipelineHashProperties:
    """Per design: pipeline_hash is schema+topology only, ignoring data content."""

    def test_same_schema_different_data_same_pipeline_hash(self):
        """Same schema, different data → same pipeline_hash."""
        s1 = _make_source(
            {"id": pa.array([1, 2], type=pa.int64()), "x": pa.array([10, 20], type=pa.int64())},
            ["id"],
        )
        s2 = _make_source(
            {"id": pa.array([3, 4], type=pa.int64()), "x": pa.array([30, 40], type=pa.int64())},
            ["id"],
        )
        assert s1.pipeline_hash() == s2.pipeline_hash()

    def test_different_schema_different_pipeline_hash(self):
        """Different schema → different pipeline_hash."""
        s1 = _make_source(
            {"id": pa.array([1], type=pa.int64()), "x": pa.array([10], type=pa.int64())},
            ["id"],
        )
        s2 = _make_source(
            {"id": pa.array([1], type=pa.int64()), "y": pa.array(["a"], type=pa.large_string())},
            ["id"],
        )
        assert s1.pipeline_hash() != s2.pipeline_hash()


# ===================================================================
# Merkle chain properties
# ===================================================================


class TestMerkleChain:
    """Per design: each downstream node's pipeline hash commits to its own
    identity plus the pipeline hashes of its upstreams."""

    def test_downstream_hash_depends_on_upstream(self):
        """Different upstream sources with different schemas produce different
        downstream pipeline hashes even with the same operator/pod."""
        source_a = _make_source(
            {"id": pa.array([1, 2], type=pa.int64()), "x": pa.array([10, 20], type=pa.int64())},
            ["id"],
        )
        # Different schema: tag=category instead of tag=id
        source_b = _make_source(
            {"category": pa.array([1, 2], type=pa.int64()), "x": pa.array([10, 20], type=pa.int64())},
            ["category"],
        )

        pf_a = PythonPacketFunction(_double, output_keys="result")
        pod_a = FunctionPod(packet_function=pf_a)
        pf_b = PythonPacketFunction(_double, output_keys="result")
        pod_b = FunctionPod(packet_function=pf_b)

        stream_a = pod_a.process(source_a)
        stream_b = pod_b.process(source_b)

        # Different upstream schemas → different downstream pipeline hashes
        assert stream_a.pipeline_hash() != stream_b.pipeline_hash()


# ===================================================================
# Commutativity of join pipeline hash
# ===================================================================


class TestJoinPipelineHashCommutativity:
    """Per design: commutative operators produce the same pipeline_hash
    regardless of input order."""

    def test_commutative_join_order_independent(self):
        sa = _make_source(
            {"id": pa.array([1, 2], type=pa.int64()), "a": pa.array([10, 20], type=pa.int64())},
            ["id"],
        )
        sb = _make_source(
            {"id": pa.array([1, 2], type=pa.int64()), "b": pa.array([30, 40], type=pa.int64())},
            ["id"],
        )

        join = Join()
        result_ab = join.process(sa, sb)
        result_ba = join.process(sb, sa)

        assert result_ab.pipeline_hash() == result_ba.pipeline_hash()

    def test_non_commutative_semijoin_order_dependent(self):
        sa = _make_source(
            {"id": pa.array([1, 2], type=pa.int64()), "a": pa.array([10, 20], type=pa.int64())},
            ["id"],
        )
        sb = _make_source(
            {"id": pa.array([1, 2], type=pa.int64()), "b": pa.array([30, 40], type=pa.int64())},
            ["id"],
        )

        semi = SemiJoin()
        result_ab = semi.process(sa, sb)
        result_ba = semi.process(sb, sa)

        # SemiJoin is non-commutative, so pipeline hashes should differ
        assert result_ab.pipeline_hash() != result_ba.pipeline_hash()
