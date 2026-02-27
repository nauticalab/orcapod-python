"""
Tests for FunctionPodStream.

Covers:
- Stream protocol conformance
- keys() and output_schema()
- iter_packets()
- as_table()
"""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa
import pytest

from orcapod.protocols.core_protocols import Stream
from orcapod.protocols.core_protocols.datagrams import Packet, Tag

from ..conftest import make_int_stream


# ---------------------------------------------------------------------------
# 1. Stream protocol conformance
# ---------------------------------------------------------------------------


class TestFunctionPodStreamProtocolConformance:
    def test_satisfies_stream_protocol(self, double_pod):
        assert isinstance(double_pod.process(make_int_stream()), Stream)

    def test_has_source_property(self, double_pod):
        _ = double_pod.process(make_int_stream()).source

    def test_has_upstreams_property(self, double_pod):
        assert isinstance(double_pod.process(make_int_stream()).upstreams, tuple)

    def test_has_keys_method(self, double_pod):
        tag_keys, packet_keys = double_pod.process(make_int_stream()).keys()
        assert isinstance(tag_keys, tuple)
        assert isinstance(packet_keys, tuple)

    def test_has_output_schema_method(self, double_pod):
        tag_schema, packet_schema = double_pod.process(
            make_int_stream()
        ).output_schema()
        assert isinstance(tag_schema, Mapping)
        assert isinstance(packet_schema, Mapping)

    def test_has_iter_packets_method(self, double_pod):
        it = double_pod.process(make_int_stream()).iter_packets()
        assert len(next(it)) == 2

    def test_has_as_table_method(self, double_pod):
        assert isinstance(double_pod.process(make_int_stream()).as_table(), pa.Table)


# ---------------------------------------------------------------------------
# 2. keys() and output_schema()
# ---------------------------------------------------------------------------


class TestFunctionPodStreamKeysAndSchema:
    def test_tag_keys_come_from_input_stream(self, double_pod):
        tag_keys, _ = double_pod.process(make_int_stream()).keys()
        assert "id" in tag_keys

    def test_packet_keys_come_from_function_output(self, double_pod):
        _, packet_keys = double_pod.process(make_int_stream()).keys()
        assert "result" in packet_keys

    def test_packet_keys_do_not_include_input_keys(self, double_pod):
        _, packet_keys = double_pod.process(make_int_stream()).keys()
        assert "x" not in packet_keys

    def test_output_schema_keys_match_keys_method(self, double_pod):
        stream = double_pod.process(make_int_stream())
        tag_keys, packet_keys = stream.keys()
        tag_schema, packet_schema = stream.output_schema()
        assert set(tag_schema.keys()) == set(tag_keys)
        assert set(packet_schema.keys()) == set(packet_keys)

    def test_packet_schema_type_is_correct(self, double_pod):
        _, packet_schema = double_pod.process(make_int_stream()).output_schema()
        assert packet_schema["result"] is int


# ---------------------------------------------------------------------------
# 3. iter_packets()
# ---------------------------------------------------------------------------


class TestFunctionPodStreamIterPackets:
    def test_yields_correct_count(self, double_pod):
        n = 5
        assert len(list(double_pod.process(make_int_stream(n=n)).iter_packets())) == n

    def test_each_pair_has_tag_and_packet(self, double_pod):
        for tag, packet in double_pod.process(make_int_stream()).iter_packets():
            assert isinstance(tag, Tag)
            assert isinstance(packet, Packet)

    def test_output_packet_values_are_doubled(self, double_pod):
        for i, (_, packet) in enumerate(
            double_pod.process(make_int_stream(n=4)).iter_packets()
        ):
            assert packet["result"] == i * 2

    def test_iter_is_repeatable_after_first_pass(self, double_pod):
        result = double_pod.process(make_int_stream(n=3))
        first = [(t["id"], p["result"]) for t, p in result.iter_packets()]
        second = [(t["id"], p["result"]) for t, p in result.iter_packets()]
        assert first == second

    def test_dunder_iter_delegates_to_iter_packets(self, double_pod):
        result = double_pod.process(make_int_stream(n=3))
        assert len(list(result)) == len(list(result.iter_packets()))


# ---------------------------------------------------------------------------
# 4. as_table()
# ---------------------------------------------------------------------------


class TestFunctionPodStreamAsTable:
    def test_returns_pyarrow_table(self, double_pod):
        assert isinstance(double_pod.process(make_int_stream()).as_table(), pa.Table)

    def test_table_has_correct_row_count(self, double_pod):
        n = 4
        assert len(double_pod.process(make_int_stream(n=n)).as_table()) == n

    def test_table_contains_tag_columns(self, double_pod):
        assert "id" in double_pod.process(make_int_stream()).as_table().column_names

    def test_table_contains_packet_columns(self, double_pod):
        assert "result" in double_pod.process(make_int_stream()).as_table().column_names

    def test_table_result_values_are_correct(self, double_pod):
        n = 3
        results = (
            double_pod.process(make_int_stream(n=n))
            .as_table()
            .column("result")
            .to_pylist()
        )
        assert results == [i * 2 for i in range(n)]

    def test_as_table_is_idempotent(self, double_pod):
        result = double_pod.process(make_int_stream(n=3))
        assert result.as_table().equals(result.as_table())

    def test_all_info_adds_extra_columns(self, double_pod):
        result = double_pod.process(make_int_stream())
        assert len(result.as_table(all_info=True).column_names) >= len(
            result.as_table().column_names
        )
