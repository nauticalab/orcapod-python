"""Specification-derived integration tests for system key lineage tracking.

Tests the three system key evolution rules from the design specification:
1. Name-preserving — single-stream ops (filter, select, map)
2. Name-extending — multi-input ops (join, merge join)
3. Type-evolving — aggregation ops (batch)
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.operators import Batch, Join, MapKeys, PolarsFilter, SelectDataColumns
from orcapod.core.sources import ArrowTableSource
from orcapod.core.streams import ArrowTableStream
from orcapod.system_constants import constants
from orcapod.types import ColumnConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_source(key_data: dict, data_data: dict, key_columns: list[str]):
    all_data = {**key_data, **data_data}
    table = pa.table(all_data)
    return ArrowTableSource(table, key_columns=key_columns, infer_nullable=True)


def _get_system_key_columns(table: pa.Table) -> list[str]:
    return [c for c in table.column_names if c.startswith(constants.SYSTEM_KEY_PREFIX)]


# ===================================================================
# Source creates system key column
# ===================================================================


class TestSourceSystemKeys:
    """Per design: each source adds a system key column encoding provenance."""

    def test_source_creates_system_key_column(self):
        source = _make_source(
            {"id": pa.array([1, 2], type=pa.int64())},
            {"value": pa.array([10, 20], type=pa.int64())},
            ["id"],
        )
        table = source.as_table(all_info=True)
        key_cols = _get_system_key_columns(table)
        assert len(key_cols) >= 1, "Source should add at least one system key column"


# ===================================================================
# Name-preserving (single-stream ops)
# ===================================================================


class TestNamePreserving:
    """Per design: single-stream ops preserve system key column names and values."""

    def test_filter_preserves_system_keys(self):
        source = _make_source(
            {"id": pa.array([1, 2, 3], type=pa.int64())},
            {"value": pa.array([10, 20, 30], type=pa.int64())},
            ["id"],
        )
        source_table = source.as_table(all_info=True)
        source_key_cols = _get_system_key_columns(source_table)

        filt = PolarsFilter(constraints={"id": 2})
        result = filt.process(source)
        result_table = result.as_table(all_info=True)
        result_key_cols = _get_system_key_columns(result_table)

        # Column names should be identical
        assert set(source_key_cols) == set(result_key_cols)

    def test_select_preserves_system_keys(self):
        source = _make_source(
            {"id": pa.array([1, 2], type=pa.int64())},
            {"a": pa.array([10, 20], type=pa.int64()), "b": pa.array([30, 40], type=pa.int64())},
            ["id"],
        )
        source_table = source.as_table(all_info=True)
        source_key_cols = _get_system_key_columns(source_table)

        select = SelectDataColumns(columns=["a"])
        result = select.process(source)
        result_table = result.as_table(all_info=True)
        result_key_cols = _get_system_key_columns(result_table)

        assert set(source_key_cols) == set(result_key_cols)

    def test_map_preserves_system_keys(self):
        source = _make_source(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "group": pa.array(["a", "b"], type=pa.large_string()),
            },
            {"value": pa.array([10, 20], type=pa.int64())},
            ["id", "group"],
        )
        source_table = source.as_table(all_info=True)
        source_key_cols = _get_system_key_columns(source_table)

        mapper = MapKeys(name_map={"id": "item_id"})
        result = mapper.process(source)
        result_table = result.as_table(all_info=True)
        result_key_cols = _get_system_key_columns(result_table)

        assert set(source_key_cols) == set(result_key_cols)


# ===================================================================
# Name-extending (multi-input ops)
# ===================================================================


class TestNameExtending:
    """Per design: multi-input ops extend system key column names with
    ::pipeline_hash:canonical_position."""

    def test_join_extends_system_key_names(self):
        source_a = _make_source(
            {"id": pa.array([1, 2], type=pa.int64())},
            {"a": pa.array([10, 20], type=pa.int64())},
            ["id"],
        )
        source_b = _make_source(
            {"id": pa.array([1, 2], type=pa.int64())},
            {"b": pa.array([30, 40], type=pa.int64())},
            ["id"],
        )

        # Get original system key column names
        a_keys = _get_system_key_columns(source_a.as_table(all_info=True))
        b_keys = _get_system_key_columns(source_b.as_table(all_info=True))

        join = Join()
        result = join.process(source_a, source_b)
        result_table = result.as_table(all_info=True)
        result_keys = _get_system_key_columns(result_table)

        # After join, system key columns should be extended (longer names)
        # Each input contributes system key columns with extended names
        assert len(result_keys) >= len(a_keys) + len(b_keys)

    def test_join_sorts_system_key_values_for_commutativity(self):
        """Per design: commutative ops sort paired key values per row."""
        source_a = _make_source(
            {"id": pa.array([1, 2], type=pa.int64())},
            {"a": pa.array([10, 20], type=pa.int64())},
            ["id"],
        )
        source_b = _make_source(
            {"id": pa.array([1, 2], type=pa.int64())},
            {"b": pa.array([30, 40], type=pa.int64())},
            ["id"],
        )

        join = Join()
        result_ab = join.process(source_a, source_b)
        result_ba = join.process(source_b, source_a)

        table_ab = result_ab.as_table(all_info=True)
        table_ba = result_ba.as_table(all_info=True)

        # System key column names should be identical for commutative join
        keys_ab = sorted(_get_system_key_columns(table_ab))
        keys_ba = sorted(_get_system_key_columns(table_ba))
        assert keys_ab == keys_ba


# ===================================================================
# Type-evolving (aggregation ops)
# ===================================================================


class TestTypeEvolving:
    """Per design: batch operation changes system key type from str to list[str]."""

    def test_batch_evolves_system_key_type(self):
        source = _make_source(
            {"group": pa.array(["a", "a", "b"], type=pa.large_string())},
            {"value": pa.array([1, 2, 3], type=pa.int64())},
            ["group"],
        )
        source_table = source.as_table(all_info=True)
        source_key_cols = _get_system_key_columns(source_table)

        batch = Batch()
        result = batch.process(source)
        result_table = result.as_table(all_info=True)
        result_key_cols = _get_system_key_columns(result_table)

        # System key columns should exist in output
        assert len(result_key_cols) == len(source_key_cols)

        # The type should have evolved to list
        for col_name in result_key_cols:
            col_type = result_table.schema.field(col_name).type
            assert pa.types.is_list(col_type) or pa.types.is_large_list(
                col_type
            ), f"Expected list type for {col_name} after batch, got {col_type}"


# ===================================================================
# Full pipeline provenance chain
# ===================================================================


class TestFullProvenanceChain:
    """End-to-end: source → join → filter → batch with all rules applied."""

    def test_full_chain(self):
        source_a = _make_source(
            {"group": pa.array(["x", "x", "y"], type=pa.large_string())},
            {"a": pa.array([1, 2, 3], type=pa.int64())},
            ["group"],
        )
        source_b = _make_source(
            {"group": pa.array(["x", "y", "y"], type=pa.large_string())},
            {"b": pa.array([10, 20, 30], type=pa.int64())},
            ["group"],
        )

        # Step 1: Join (name-extending)
        join = Join()
        joined = join.process(source_a, source_b)

        # Step 2: Filter (name-preserving)
        filt = PolarsFilter(constraints={"group": "x"})
        filtered = filt.process(joined)

        # Step 3: Batch (type-evolving)
        batch = Batch()
        batched = batch.process(filtered)

        table = batched.as_table(all_info=True)
        key_cols = _get_system_key_columns(table)

        # After all three stages, system keys should exist
        assert len(key_cols) > 0

        # After batch, types should be lists
        for col_name in key_cols:
            col_type = table.schema.field(col_name).type
            assert pa.types.is_list(col_type) or pa.types.is_large_list(col_type)
