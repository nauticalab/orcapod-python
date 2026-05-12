"""Specification-derived integration tests for end-to-end pipeline flows.

Tests complete pipeline scenarios as described in the design specification:
Source → Stream → [Operator / FunctionPod] → Stream → ...
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.function_pod import FunctionPod
from orcapod.core.operators import (
    Batch,
    DropDataColumns,
    Join,
    MapTags,
    MergeJoin,
    PolarsFilter,
    SelectDataColumns,
    SemiJoin,
)
from orcapod.core.data_function import PythonDataFunction
from orcapod.core.sources import ArrowTableSource, DictSource
from orcapod.core.streams import ArrowTableStream


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _double(x: int) -> int:
    return x * 2


def _negate(x: int) -> int:
    return -x


def _square(x: int) -> int:
    return x * x


def _square_doubled(doubled: int) -> int:
    return doubled * doubled


def _make_source(tag_data: dict, data_data: dict, tag_columns: list[str]):
    all_data = {**tag_data, **data_data}
    table = pa.table(all_data)
    return ArrowTableSource(table, tag_columns=tag_columns, infer_nullable=True)


# ===================================================================
# Single operator pipelines
# ===================================================================


class TestSourceToFilter:
    """Source → PolarsFilter → Stream."""

    def test_filter_reduces_rows(self):
        source = _make_source(
            {"id": pa.array([1, 2, 3, 4, 5], type=pa.int64())},
            {"value": pa.array([10, 20, 30, 40, 50], type=pa.int64())},
            ["id"],
        )
        filt = PolarsFilter(constraints={"id": 3})
        result = filt.process(source)
        table = result.as_table()
        assert table.num_rows == 1
        assert table.column("id").to_pylist() == [3]


class TestSourceToFunctionPod:
    """Source → FunctionPod → Stream with transformed data."""

    def test_function_pod_transforms_all_data(self):
        source = _make_source(
            {"id": pa.array([0, 1, 2], type=pa.int64())},
            {"x": pa.array([10, 20, 30], type=pa.int64())},
            ["id"],
        )
        pf = PythonDataFunction(_double, output_keys="result")
        pod = FunctionPod(data_function=pf)
        result = pod.process(source)
        data = list(result.iter_data())
        assert len(data) == 3
        results = [p["result"] for _, p in data]
        assert sorted(results) == [20, 40, 60]


class TestMultiSourceJoin:
    """Two sources → Join → Stream with combined data."""

    def test_join_combines_matching_rows(self):
        source_a = _make_source(
            {"id": pa.array([1, 2, 3], type=pa.int64())},
            {"name": pa.array(["alice", "bob", "charlie"], type=pa.large_string())},
            ["id"],
        )
        source_b = _make_source(
            {"id": pa.array([2, 3, 4], type=pa.int64())},
            {"score": pa.array([85, 90, 95], type=pa.int64())},
            ["id"],
        )
        join = Join()
        result = join.process(source_a, source_b)
        table = result.as_table()
        assert table.num_rows == 2  # id=2, id=3
        assert "name" in table.column_names
        assert "score" in table.column_names


# ===================================================================
# Chained operator pipelines
# ===================================================================


class TestChainedOperators:
    """Source → Filter → Select → MapTags → Stream."""

    def test_chain_of_three_operators(self):
        source = _make_source(
            {
                "id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
                "group": pa.array(["a", "b", "a", "b", "a"], type=pa.large_string()),
            },
            {"value": pa.array([10, 20, 30, 40, 50], type=pa.int64())},
            ["id", "group"],
        )
        # Step 1: Filter to group="a"
        filt = PolarsFilter(constraints={"group": "a"})
        filtered = filt.process(source)

        # Step 2: Select only relevant data columns
        select = SelectDataColumns(columns=["value"])
        selected = select.process(filtered)

        # Step 3: Rename tag
        mapper = MapTags(name_map={"id": "item_id"})
        result = mapper.process(selected)

        table = result.as_table()
        assert table.num_rows == 3  # group="a" has 3 rows
        assert "item_id" in table.column_names
        assert "id" not in table.column_names


class TestFunctionPodThenOperator:
    """Source → FunctionPod → PolarsFilter → Stream."""

    def test_transform_then_filter(self):
        source = _make_source(
            {"id": pa.array([0, 1, 2, 3, 4], type=pa.int64())},
            {"x": pa.array([1, 2, 3, 4, 5], type=pa.int64())},
            ["id"],
        )
        pf = PythonDataFunction(_double, output_keys="result")
        pod = FunctionPod(data_function=pf)
        transformed = pod.process(source)

        # Filter to only results >= 6 (i.e., x >= 3 → result >= 6)
        # We can filter on tag id >= 3
        filt = PolarsFilter(constraints={"id": 3})
        result = filt.process(transformed)
        table = result.as_table()
        assert table.num_rows == 1


class TestJoinThenBatch:
    """Two sources → Join → Batch → Stream."""

    def test_join_then_batch(self):
        source_a = _make_source(
            {"group": pa.array(["x", "x", "y"], type=pa.large_string())},
            {"a": pa.array([1, 2, 3], type=pa.int64())},
            ["group"],
        )
        source_b = _make_source(
            {"group": pa.array(["x", "x", "y"], type=pa.large_string())},
            {"b": pa.array([10, 20, 30], type=pa.int64())},
            ["group"],
        )
        join = Join()
        joined = join.process(source_a, source_b)

        batch = Batch()
        result = batch.process(joined)
        table = result.as_table()
        # After join and batch, rows should be grouped by tag
        assert table.num_rows >= 1


class TestSemiJoinFilters:
    """Source A semi-joined with Source B."""

    def test_semijoin_keeps_matching_left(self):
        source_a = _make_source(
            {"id": pa.array([1, 2, 3, 4, 5], type=pa.int64())},
            {"value": pa.array([10, 20, 30, 40, 50], type=pa.int64())},
            ["id"],
        )
        source_b = _make_source(
            {"id": pa.array([2, 4], type=pa.int64())},
            {"dummy": pa.array([0, 0], type=pa.int64())},
            ["id"],
        )
        semi = SemiJoin()
        result = semi.process(source_a, source_b)
        table = result.as_table()
        assert table.num_rows == 2
        assert sorted(table.column("id").to_pylist()) == [2, 4]


class TestMergeJoinCombines:
    """Two sources with overlapping columns → MergeJoin."""

    def test_merge_join_merges_columns(self):
        source_a = _make_source(
            {"id": pa.array([1, 2], type=pa.int64())},
            {"score": pa.array([80, 90], type=pa.int64())},
            ["id"],
        )
        source_b = _make_source(
            {"id": pa.array([1, 2], type=pa.int64())},
            {"score": pa.array([85, 95], type=pa.int64())},
            ["id"],
        )
        merge = MergeJoin()
        result = merge.process(source_a, source_b)
        table = result.as_table()
        assert table.num_rows == 2
        # score should now be list type
        score_type = table.schema.field("score").type
        assert pa.types.is_list(score_type) or pa.types.is_large_list(score_type)


# ===================================================================
# Diamond pipeline
# ===================================================================


class TestDiamondPipeline:
    """Source → [branch A, branch B] → Join → Stream."""

    def test_diamond_topology(self):
        source = _make_source(
            {"id": pa.array([1, 2, 3], type=pa.int64())},
            {"x": pa.array([10, 20, 30], type=pa.int64())},
            ["id"],
        )
        # Branch A: double x
        pf_a = PythonDataFunction(_double, output_keys="doubled")
        pod_a = FunctionPod(data_function=pf_a)
        branch_a = pod_a.process(source)

        # Branch B: negate x
        pf_b = PythonDataFunction(_negate, output_keys="negated")
        pod_b = FunctionPod(data_function=pf_b)
        branch_b = pod_b.process(source)

        # Join branches
        join = Join()
        result = join.process(branch_a, branch_b)
        table = result.as_table()
        assert table.num_rows == 3
        assert "doubled" in table.column_names
        assert "negated" in table.column_names


# ===================================================================
# Multiple function pods chained
# ===================================================================


class TestChainedFunctionPods:
    """Source → FunctionPod1 → FunctionPod2 → Stream."""

    def test_two_sequential_transformations(self):
        source = _make_source(
            {"id": pa.array([1, 2, 3], type=pa.int64())},
            {"x": pa.array([2, 3, 4], type=pa.int64())},
            ["id"],
        )
        # First: double
        pf1 = PythonDataFunction(_double, output_keys="doubled")
        pod1 = FunctionPod(data_function=pf1)
        step1 = pod1.process(source)

        # Second: square the doubled value
        pf2 = PythonDataFunction(_square_doubled, output_keys="squared")
        pod2 = FunctionPod(data_function=pf2)
        step2 = pod2.process(step1)

        data = list(step2.iter_data())
        assert len(data) == 3
        # x=2 → doubled=4 → squared=16
        results = sorted([p["squared"] for _, p in data])
        assert results == [16, 36, 64]
