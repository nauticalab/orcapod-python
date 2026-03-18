"""Tests for ResultCache — shared result caching logic.

Covers:
- Lookup: cache miss, cache hit, conflict resolution (most recent wins)
- Store: variation/execution columns, input packet hash, timestamp
- additional_constraints for narrowing match
- auto_flush behavior
- get_all_records
"""

from __future__ import annotations

from typing import Any

import pyarrow as pa
import pytest

from orcapod.core.datagrams import Packet
from orcapod.core.packet_function import PythonPacketFunction
from orcapod.core.result_cache import ResultCache
from orcapod.databases import InMemoryArrowDatabase
from orcapod.system_constants import constants


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def double(x: int) -> int:
    return x * 2


def _make_pf() -> PythonPacketFunction:
    return PythonPacketFunction(double, output_keys="result")


def _make_cache(
    db=None, record_path=("test",)
) -> tuple[ResultCache, InMemoryArrowDatabase]:
    if db is None:
        db = InMemoryArrowDatabase()
    cache = ResultCache(result_database=db, record_path=record_path)
    return cache, db


def _compute_and_store(
    cache: ResultCache, pf: PythonPacketFunction, input_packet: Packet
):
    """Helper: compute output and store in cache."""
    output = pf.direct_call(input_packet)
    assert output is not None
    cache.store(
        input_packet,
        output,
        variation_data=pf.get_function_variation_data(),
        execution_data=pf.get_execution_data(),
    )
    return output


# ---------------------------------------------------------------------------
# Lookup
# ---------------------------------------------------------------------------


class TestLookupMiss:
    def test_empty_db_returns_none(self):
        cache, _ = _make_cache()
        packet = Packet({"x": 10})
        assert cache.lookup(packet) is None

    def test_different_packet_returns_none(self):
        cache, _ = _make_cache()
        pf = _make_pf()
        _compute_and_store(cache, pf, Packet({"x": 10}))
        assert cache.lookup(Packet({"x": 99})) is None


class TestLookupHit:
    def test_returns_cached_result(self):
        cache, _ = _make_cache()
        pf = _make_pf()
        input_pkt = Packet({"x": 10})
        _compute_and_store(cache, pf, input_pkt)

        cached = cache.lookup(input_pkt)
        assert cached is not None
        assert cached.as_dict()["result"] == 20

    def test_sets_computed_flag_false(self):
        cache, _ = _make_cache()
        pf = _make_pf()
        input_pkt = Packet({"x": 10})
        _compute_and_store(cache, pf, input_pkt)

        cached = cache.lookup(input_pkt)
        assert cached is not None
        assert cached.get_meta_value(ResultCache.RESULT_COMPUTED_FLAG) is False

    def test_same_packet_different_record_path_is_miss(self):
        db = InMemoryArrowDatabase()
        cache_a = ResultCache(result_database=db, record_path=("path_a",))
        cache_b = ResultCache(result_database=db, record_path=("path_b",))
        pf = _make_pf()
        input_pkt = Packet({"x": 10})

        output = pf.direct_call(input_pkt)
        cache_a.store(
            input_pkt,
            output,
            variation_data=pf.get_function_variation_data(),
            execution_data=pf.get_execution_data(),
        )

        assert cache_a.lookup(input_pkt) is not None
        assert cache_b.lookup(input_pkt) is None


class TestConflictResolution:
    def test_most_recent_wins(self):
        import time

        cache, _ = _make_cache()
        pf = _make_pf()
        input_pkt = Packet({"x": 10})

        # Store first result
        _compute_and_store(cache, pf, input_pkt)
        time.sleep(0.01)  # ensure different timestamp

        # Store a second result for the same input (simulating recomputation)
        output2 = pf.direct_call(input_pkt)
        cache.store(
            input_pkt,
            output2,
            variation_data=pf.get_function_variation_data(),
            execution_data=pf.get_execution_data(),
        )

        # Lookup should return the most recent
        cached = cache.lookup(input_pkt)
        assert cached is not None
        assert cached.datagram_id == output2.datagram_id


# ---------------------------------------------------------------------------
# Additional constraints
# ---------------------------------------------------------------------------


class TestAdditionalConstraints:
    def test_narrower_match_filters_results(self):
        cache, _ = _make_cache()
        pf = _make_pf()
        input_pkt = Packet({"x": 10})
        _compute_and_store(cache, pf, input_pkt)

        # Lookup with a constraint that doesn't match any stored column value
        result = cache.lookup(
            input_pkt,
            additional_constraints={
                f"{constants.PF_VARIATION_PREFIX}function_name": "nonexistent"
            },
        )
        assert result is None

    def test_matching_constraint_returns_result(self):
        cache, _ = _make_cache()
        pf = _make_pf()
        input_pkt = Packet({"x": 10})
        _compute_and_store(cache, pf, input_pkt)

        # Lookup with a constraint that matches
        result = cache.lookup(
            input_pkt,
            additional_constraints={
                f"{constants.PF_VARIATION_PREFIX}function_name": "double"
            },
        )
        assert result is not None
        assert result.as_dict()["result"] == 20


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------


class TestStore:
    def test_stores_input_packet_hash_column(self):
        cache, db = _make_cache()
        pf = _make_pf()
        input_pkt = Packet({"x": 10})
        _compute_and_store(cache, pf, input_pkt)

        records = db.get_all_records(cache.record_path)
        assert records is not None
        assert constants.INPUT_PACKET_HASH_COL in records.column_names

    def test_stores_variation_columns(self):
        cache, db = _make_cache()
        pf = _make_pf()
        _compute_and_store(cache, pf, Packet({"x": 10}))

        records = db.get_all_records(cache.record_path)
        assert records is not None
        variation_cols = [
            c
            for c in records.column_names
            if c.startswith(constants.PF_VARIATION_PREFIX)
        ]
        assert len(variation_cols) > 0

    def test_stores_execution_columns(self):
        cache, db = _make_cache()
        pf = _make_pf()
        _compute_and_store(cache, pf, Packet({"x": 10}))

        records = db.get_all_records(cache.record_path)
        assert records is not None
        exec_cols = [
            c
            for c in records.column_names
            if c.startswith(constants.PF_EXECUTION_PREFIX)
        ]
        assert len(exec_cols) > 0

    def test_stores_timestamp(self):
        cache, db = _make_cache()
        pf = _make_pf()
        _compute_and_store(cache, pf, Packet({"x": 10}))

        records = db.get_all_records(cache.record_path)
        assert records is not None
        assert constants.POD_TIMESTAMP in records.column_names

    def test_stores_output_data(self):
        cache, db = _make_cache()
        pf = _make_pf()
        _compute_and_store(cache, pf, Packet({"x": 10}))

        records = db.get_all_records(cache.record_path)
        assert records is not None
        assert "result" in records.column_names
        assert records.column("result").to_pylist() == [20]


# ---------------------------------------------------------------------------
# Auto flush
# ---------------------------------------------------------------------------


class TestAutoFlush:
    def test_auto_flush_true_by_default(self):
        cache, _ = _make_cache()
        assert cache._auto_flush is True

    def test_set_auto_flush_false(self):
        cache, _ = _make_cache()
        cache.set_auto_flush(False)
        assert cache._auto_flush is False

    def test_auto_flush_false_in_constructor(self):
        db = InMemoryArrowDatabase()
        cache = ResultCache(result_database=db, record_path=("t",), auto_flush=False)
        assert cache._auto_flush is False


# ---------------------------------------------------------------------------
# get_all_records
# ---------------------------------------------------------------------------


class TestGetAllRecords:
    def test_empty_returns_none(self):
        cache, _ = _make_cache()
        assert cache.get_all_records() is None

    def test_returns_stored_records(self):
        cache, _ = _make_cache()
        pf = _make_pf()
        _compute_and_store(cache, pf, Packet({"x": 10}))
        _compute_and_store(cache, pf, Packet({"x": 20}))

        records = cache.get_all_records()
        assert records is not None
        assert records.num_rows == 2

    def test_include_system_columns_adds_record_id(self):
        cache, _ = _make_cache()
        pf = _make_pf()
        _compute_and_store(cache, pf, Packet({"x": 10}))

        records = cache.get_all_records(include_system_columns=True)
        assert records is not None
        assert constants.PACKET_RECORD_ID in records.column_names

    def test_exclude_system_columns_by_default(self):
        cache, _ = _make_cache()
        pf = _make_pf()
        _compute_and_store(cache, pf, Packet({"x": 10}))

        records = cache.get_all_records(include_system_columns=False)
        assert records is not None
        assert constants.PACKET_RECORD_ID not in records.column_names
