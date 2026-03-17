"""
Tests for PacketFunctionWrapper and CachedPacketFunction using
InMemoryArrowDatabase as the backing store.

Covers:
- PacketFunctionWrapper: full property/method delegation and protocol conformance
- CachedPacketFunction construction: record_path, auto_flush default
- call() cache-miss: delegates to inner function, stores result
- call() cache-hit: returns cached result without re-executing inner function
- call(skip_cache_lookup=True): always computes fresh, still stores result
- call(skip_cache_insert=True): computes fresh, does NOT store result
- call() with inner returning None: skips record_packet
- record_packet: stores input hash, variation data, execution data, timestamp columns
- get_cached_output_for_packet: returns None when no entry, returns packet on hit
- get_cached_output_for_packet: conflict resolution (multiple rows → most recent wins)
- get_all_cached_outputs: returns None when empty, returns table after records inserted
- get_all_cached_outputs(include_system_columns=True/False): record_id column visibility
- Different inputs hash to different cache entries (no cross-contamination)
- Same function, different record_path_prefix → independent caches
"""

from __future__ import annotations

import asyncio
import time

import pytest

from orcapod.core.datagrams import Packet
from orcapod.core.packet_function import (
    CachedPacketFunction,
    PacketFunctionWrapper,
    PythonPacketFunction,
)
from orcapod.databases import InMemoryArrowDatabase
from orcapod.protocols.core_protocols import PacketFunctionProtocol
from orcapod.system_constants import constants

# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def add(x: int, y: int) -> int:
    return x + y


def multiply(x: int, y: int) -> int:
    return x * y


@pytest.fixture
def inner_pf() -> PythonPacketFunction:
    return PythonPacketFunction(add, output_keys="result")


@pytest.fixture
def db() -> InMemoryArrowDatabase:
    return InMemoryArrowDatabase()


@pytest.fixture
def cached_pf(inner_pf, db) -> CachedPacketFunction:
    return CachedPacketFunction(inner_pf, result_database=db)


@pytest.fixture
def input_packet() -> Packet:
    return Packet({"x": 3, "y": 4})


@pytest.fixture
def other_input_packet() -> Packet:
    return Packet({"x": 10, "y": 20})


# ---------------------------------------------------------------------------
# 1. Construction
# ---------------------------------------------------------------------------


class TestConstruction:
    def test_record_path_is_tuple(self, cached_pf, inner_pf):
        assert isinstance(cached_pf.record_path, tuple)

    def test_record_path_ends_with_inner_uri(self, cached_pf, inner_pf):
        assert cached_pf.record_path[-len(inner_pf.uri) :] == inner_pf.uri

    def test_record_path_prefix_empty_by_default(self, cached_pf, inner_pf):
        assert cached_pf.record_path == inner_pf.uri

    def test_record_path_prefix_prepended(self, inner_pf, db):
        cpf = CachedPacketFunction(
            inner_pf, result_database=db, record_path_prefix=("org", "project")
        )
        assert cpf.record_path == ("org", "project") + inner_pf.uri

    def test_auto_flush_true_by_default(self, cached_pf):
        assert cached_pf._cache._auto_flush is True

    def test_set_auto_flush_false(self, cached_pf):
        cached_pf.set_auto_flush(False)
        assert cached_pf._cache._auto_flush is False


# ---------------------------------------------------------------------------
# 2. PacketFunctionWrapper delegation
# ---------------------------------------------------------------------------


class TestWrapperDelegation:
    def test_canonical_function_name_delegates(self, cached_pf, inner_pf):
        assert cached_pf.canonical_function_name == inner_pf.canonical_function_name

    def test_uri_delegates(self, cached_pf, inner_pf):
        assert cached_pf.uri == inner_pf.uri

    def test_input_packet_schema_delegates(self, cached_pf, inner_pf):
        assert cached_pf.input_packet_schema == inner_pf.input_packet_schema

    def test_output_packet_schema_delegates(self, cached_pf, inner_pf):
        assert cached_pf.output_packet_schema == inner_pf.output_packet_schema


# ---------------------------------------------------------------------------
# 3. get_all_cached_outputs — empty store
# ---------------------------------------------------------------------------


class TestGetAllCachedOutputsEmpty:
    def test_returns_none_when_no_records(self, cached_pf):
        assert cached_pf.get_all_cached_outputs() is None


# ---------------------------------------------------------------------------
# 4. call — cache miss (first call)
# ---------------------------------------------------------------------------


class TestCallCacheMiss:
    def test_returns_non_none_result(self, cached_pf, input_packet):
        result, _captured = cached_pf.call(input_packet)
        assert result is not None

    def test_result_has_correct_value(self, cached_pf, input_packet):
        result, _captured = cached_pf.call(input_packet)
        assert result["result"] == 7  # 3 + 4

    def test_result_stored_in_database(self, cached_pf, input_packet, db):
        cached_pf.call(input_packet)
        stored = db.get_all_records(cached_pf.record_path)
        assert stored is not None
        assert stored.num_rows == 1

    def test_get_all_cached_outputs_non_empty_after_call(self, cached_pf, input_packet):
        cached_pf.call(input_packet)
        all_outputs = cached_pf.get_all_cached_outputs()
        assert all_outputs is not None
        assert all_outputs.num_rows == 1


# ---------------------------------------------------------------------------
# 5. call — cache hit (second call with same input)
# ---------------------------------------------------------------------------


class TestCallCacheHit:
    def test_second_call_returns_result(self, cached_pf, input_packet):
        cached_pf.call(input_packet)
        result, _captured = cached_pf.call(input_packet)
        assert result is not None
        assert result["result"] == 7

    def test_second_call_does_not_add_new_record(self, cached_pf, input_packet, db):
        cached_pf.call(input_packet)
        cached_pf.call(input_packet)
        stored = db.get_all_records(cached_pf.record_path)
        assert stored is not None
        assert stored.num_rows == 1  # still only one record

    def test_inner_function_not_called_on_cache_hit(self, inner_pf, db, input_packet):
        call_count = 0

        def counting_add(x: int, y: int) -> int:
            nonlocal call_count
            call_count += 1
            return x + y

        pf = PythonPacketFunction(counting_add, output_keys="result")
        cpf = CachedPacketFunction(pf, result_database=db)

        cpf.call(input_packet)  # cache miss — inner called once
        assert call_count == 1

        cpf.call(input_packet)  # cache hit — inner should NOT be called again
        assert call_count == 1


# ---------------------------------------------------------------------------
# 6. call — skip_cache_lookup
# ---------------------------------------------------------------------------


class TestSkipCacheLookup:
    def test_skip_cache_lookup_still_returns_result(self, cached_pf, input_packet):
        cached_pf.call(input_packet)  # populate cache
        result, _captured = cached_pf.call(input_packet, skip_cache_lookup=True)
        assert result is not None
        assert result["result"] == 7

    def test_skip_cache_lookup_adds_another_record(self, cached_pf, input_packet, db):
        cached_pf.call(input_packet)  # first call — inserts record
        # Second call with skip_cache_lookup=True tries to insert again;
        # skip_duplicates=False is the default, but the packet has a new datagram_id
        # so a second record with the same input_packet_hash is inserted.
        cached_pf.call(input_packet, skip_cache_lookup=True)
        stored = db.get_all_records(cached_pf.record_path)
        assert stored is not None
        assert stored.num_rows == 2


# ---------------------------------------------------------------------------
# 7. call — skip_cache_insert
# ---------------------------------------------------------------------------


class TestSkipCacheInsert:
    def test_skip_cache_insert_returns_result(self, cached_pf, input_packet):
        result, _captured = cached_pf.call(input_packet, skip_cache_insert=True)
        assert result is not None
        assert result["result"] == 7

    def test_skip_cache_insert_does_not_store(self, cached_pf, input_packet, db):
        cached_pf.call(input_packet, skip_cache_insert=True)
        stored = db.get_all_records(cached_pf.record_path)
        assert stored is None  # nothing stored

    def test_subsequent_call_is_still_a_cache_miss(self, cached_pf, input_packet, db):
        call_count = 0

        def counting_add(x: int, y: int) -> int:
            nonlocal call_count
            call_count += 1
            return x + y

        pf = PythonPacketFunction(counting_add, output_keys="result")
        cpf = CachedPacketFunction(pf, result_database=db)

        cpf.call(input_packet, skip_cache_insert=True)
        assert call_count == 1

        cpf.call(input_packet)  # still a miss since nothing was stored
        assert call_count == 2


# ---------------------------------------------------------------------------
# 8. get_cached_output_for_packet
# ---------------------------------------------------------------------------


class TestGetCachedOutputForPacket:
    def test_returns_none_before_any_call(self, cached_pf, input_packet):
        result = cached_pf.get_cached_output_for_packet(input_packet)
        assert result is None

    def test_returns_packet_after_call(self, cached_pf, input_packet):
        cached_pf.call(input_packet)
        result = cached_pf.get_cached_output_for_packet(input_packet)
        assert result is not None
        assert result["result"] == 7

    def test_different_input_returns_none(
        self, cached_pf, input_packet, other_input_packet
    ):
        cached_pf.call(input_packet)
        result = cached_pf.get_cached_output_for_packet(other_input_packet)
        assert result is None


# ---------------------------------------------------------------------------
# 9. Different inputs — independent cache entries
# ---------------------------------------------------------------------------


class TestIndependentCacheEntries:
    def test_two_inputs_stored_separately(
        self, cached_pf, input_packet, other_input_packet, db
    ):
        cached_pf.call(input_packet)
        cached_pf.call(other_input_packet)
        stored = db.get_all_records(cached_pf.record_path)
        assert stored is not None
        assert stored.num_rows == 2

    def test_each_input_retrieves_correct_result(
        self, cached_pf, input_packet, other_input_packet
    ):
        cached_pf.call(input_packet)
        cached_pf.call(other_input_packet)

        result_a = cached_pf.get_cached_output_for_packet(input_packet)
        result_b = cached_pf.get_cached_output_for_packet(other_input_packet)

        assert result_a is not None
        assert result_b is not None
        assert result_a["result"] == 7  # 3 + 4
        assert result_b["result"] == 30  # 10 + 20


# ---------------------------------------------------------------------------
# 10. record_path_prefix isolation
# ---------------------------------------------------------------------------


class TestRecordPathPrefixIsolation:
    def test_different_prefixes_use_different_paths(self, inner_pf, input_packet):
        db = InMemoryArrowDatabase()
        cpf_a = CachedPacketFunction(
            inner_pf, result_database=db, record_path_prefix=("ns", "a")
        )
        cpf_b = CachedPacketFunction(
            inner_pf, result_database=db, record_path_prefix=("ns", "b")
        )

        cpf_a.call(input_packet)

        # a has a cached entry; b does not
        assert cpf_a.get_cached_output_for_packet(input_packet) is not None
        assert cpf_b.get_cached_output_for_packet(input_packet) is None


# ---------------------------------------------------------------------------
# 11. auto_flush behaviour
# ---------------------------------------------------------------------------


class TestAutoFlush:
    def test_auto_flush_true_makes_result_immediately_committed(
        self, cached_pf, input_packet, db
    ):
        # With auto_flush=True (default), after call() the record is in committed store
        cached_pf.call(input_packet)
        # pending batch should be empty — flushed immediately
        record_key = "/".join(cached_pf.record_path)
        assert record_key not in db._pending_batches

    def test_auto_flush_false_leaves_result_in_pending(self, inner_pf, input_packet):
        db = InMemoryArrowDatabase()
        cpf = CachedPacketFunction(inner_pf, result_database=db)
        cpf.set_auto_flush(False)
        cpf.call(input_packet)

        record_key = "/".join(cpf.record_path)
        assert record_key in db._pending_batches

    def test_auto_flush_false_result_still_retrievable_from_pending(
        self, inner_pf, input_packet
    ):
        db = InMemoryArrowDatabase()
        cpf = CachedPacketFunction(inner_pf, result_database=db)
        cpf.set_auto_flush(False)
        cpf.call(input_packet)

        # InMemoryArrowDatabase includes pending in get_all_cached_outputs
        all_outputs = cpf.get_all_cached_outputs()
        assert all_outputs is not None
        assert all_outputs.num_rows == 1


# ---------------------------------------------------------------------------
# 12. PacketFunctionWrapper — full delegation
# ---------------------------------------------------------------------------


class TestPacketFunctionWrapperDelegation:
    """PacketFunctionWrapper delegates every property/method to the inner function."""

    @pytest.fixture
    def wrapper(self, inner_pf):
        return PacketFunctionWrapper(inner_pf)

    def test_major_version_delegates(self, wrapper, inner_pf):
        assert wrapper.major_version == inner_pf.major_version

    def test_minor_version_string_delegates(self, wrapper, inner_pf):
        assert wrapper.minor_version_string == inner_pf.minor_version_string

    def test_packet_function_type_id_delegates(self, wrapper, inner_pf):
        assert wrapper.packet_function_type_id == inner_pf.packet_function_type_id

    def test_get_function_variation_data_delegates(self, wrapper, inner_pf):
        assert (
            wrapper.get_function_variation_data()
            == inner_pf.get_function_variation_data()
        )

    def test_get_execution_data_delegates(self, wrapper, inner_pf):
        assert wrapper.get_execution_data() == inner_pf.get_execution_data()

    def test_call_delegates(self, wrapper, input_packet):
        result, _captured = wrapper.call(input_packet)
        assert result is not None
        assert result["result"] == 7  # 3 + 4

    def test_async_call_delegates_through_wrapper(self, wrapper, input_packet):
        result, _captured = asyncio.run(wrapper.async_call(input_packet))
        assert result is not None
        assert result["result"] == 7  # 3 + 4

    def test_computed_label_returns_inner_label(self, wrapper, inner_pf):
        assert wrapper.computed_label() == inner_pf.label

    def test_satisfies_packet_function_protocol(self, wrapper):
        assert isinstance(wrapper, PacketFunctionProtocol)


# ---------------------------------------------------------------------------
# 13. record_packet — stored column structure
# ---------------------------------------------------------------------------


class TestRecordPacketColumns:
    """Verify that record_packet writes the expected columns into the database."""

    def test_input_packet_hash_column_present(self, cached_pf, input_packet, db):
        cached_pf.call(input_packet)
        table = db.get_all_records(cached_pf.record_path)
        assert table is not None
        assert constants.INPUT_PACKET_HASH_COL in table.column_names

    def test_input_packet_hash_value_matches(self, cached_pf, input_packet, db):
        cached_pf.call(input_packet)
        table = db.get_all_records(cached_pf.record_path)
        stored_hash = table.column(constants.INPUT_PACKET_HASH_COL).to_pylist()[0]
        assert stored_hash == input_packet.content_hash().to_string()

    def test_variation_columns_present(self, cached_pf, input_packet, db):
        cached_pf.call(input_packet)
        table = db.get_all_records(cached_pf.record_path)
        assert table is not None
        variation_keys = cached_pf.get_function_variation_data().keys()
        for k in variation_keys:
            col = f"{constants.PF_VARIATION_PREFIX}{k}"
            assert col in table.column_names, f"Expected column {col!r} not found"

    def test_execution_columns_present(self, cached_pf, input_packet, db):
        cached_pf.call(input_packet)
        table = db.get_all_records(cached_pf.record_path)
        assert table is not None
        exec_keys = cached_pf.get_execution_data().keys()
        for k in exec_keys:
            col = f"{constants.PF_EXECUTION_PREFIX}{k}"
            assert col in table.column_names, f"Expected column {col!r} not found"

    def test_timestamp_column_present(self, cached_pf, input_packet, db):
        cached_pf.call(input_packet)
        table = db.get_all_records(cached_pf.record_path)
        assert table is not None
        assert constants.POD_TIMESTAMP in table.column_names


# ---------------------------------------------------------------------------
# 14. get_all_cached_outputs — include_system_columns
# ---------------------------------------------------------------------------


class TestGetAllCachedOutputsSystemColumns:
    def test_include_system_columns_true_exposes_record_id(
        self, cached_pf, input_packet
    ):
        cached_pf.call(input_packet)
        table = cached_pf.get_all_cached_outputs(include_system_columns=True)
        assert table is not None
        assert constants.PACKET_RECORD_ID in table.column_names

    def test_include_system_columns_false_hides_record_id(
        self, cached_pf, input_packet
    ):
        cached_pf.call(input_packet)
        table = cached_pf.get_all_cached_outputs(include_system_columns=False)
        assert table is not None
        assert constants.PACKET_RECORD_ID not in table.column_names


# ---------------------------------------------------------------------------
# 15. call() with inner returning None
# ---------------------------------------------------------------------------


class TestCallInnerReturnsNone:
    def test_inactive_inner_returns_none_and_does_not_store(
        self, inner_pf, db, input_packet
    ):
        inner_pf.set_active(False)
        cpf = CachedPacketFunction(inner_pf, result_database=db)
        result, _captured = cpf.call(input_packet)
        assert result is None
        assert db.get_all_records(cpf.record_path) is None


# ---------------------------------------------------------------------------
# 16. get_cached_output_for_packet — conflict resolution
# ---------------------------------------------------------------------------


class TestConflictResolution:
    """When multiple records share the same input hash, the most recent is returned."""

    def test_most_recent_wins(self, inner_pf, input_packet):
        db = InMemoryArrowDatabase()
        cpf = CachedPacketFunction(inner_pf, result_database=db)

        # Insert two records for the same input, with a small delay between them
        cpf.call(input_packet, skip_cache_lookup=True)
        time.sleep(0.01)  # ensure distinct timestamps
        cpf.call(input_packet, skip_cache_lookup=True)

        # Two records in the store
        all_records = db.get_all_records(cpf.record_path)
        assert all_records is not None
        assert all_records.num_rows == 2

        # get_cached_output_for_packet should still return exactly one result
        result = cpf.get_cached_output_for_packet(input_packet)
        assert result is not None
        assert result["result"] == 7  # 3 + 4


# ---------------------------------------------------------------------------
# 17. RESULT_COMPUTED_FLAG — freshly computed vs fetched from cache
# ---------------------------------------------------------------------------


class TestResultComputedFlag:
    """Verify the meta flag that distinguishes fresh computation from cache hits."""

    def test_cache_miss_sets_computed_true(self, cached_pf, input_packet):
        result, _captured = cached_pf.call(input_packet)
        assert result is not None
        flag = result.get_meta_value(CachedPacketFunction.RESULT_COMPUTED_FLAG)
        assert flag is True

    def test_cache_hit_sets_computed_false(self, cached_pf, input_packet):
        cached_pf.call(input_packet)  # first call — populates cache
        result, _captured = cached_pf.call(input_packet)  # second call — cache hit
        assert result is not None
        flag = result.get_meta_value(CachedPacketFunction.RESULT_COMPUTED_FLAG)
        assert flag is False

    def test_skip_cache_lookup_sets_computed_true(self, cached_pf, input_packet):
        cached_pf.call(input_packet)  # populate cache
        result, _captured = cached_pf.call(input_packet, skip_cache_lookup=True)
        assert result is not None
        flag = result.get_meta_value(CachedPacketFunction.RESULT_COMPUTED_FLAG)
        assert flag is True

    def test_skip_cache_insert_sets_computed_true(self, cached_pf, input_packet):
        result, _captured = cached_pf.call(input_packet, skip_cache_insert=True)
        assert result is not None
        flag = result.get_meta_value(CachedPacketFunction.RESULT_COMPUTED_FLAG)
        assert flag is True
