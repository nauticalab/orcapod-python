"""
Tests for FunctionPodStream.

Covers:
- StreamProtocol protocol conformance
- keys() and output_schema()
- iter_packets()
- as_table()
"""

from __future__ import annotations

from collections.abc import Mapping

import pyarrow as pa

from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.protocols.core_protocols.datagrams import PacketProtocol, TagProtocol

from ..conftest import make_int_stream


# ---------------------------------------------------------------------------
# 1. StreamProtocol protocol conformance
# ---------------------------------------------------------------------------


class TestFunctionPodLabel:
    def test_label_defaults_to_function_name(self, double_pod):
        """FunctionPod.label should be the wrapped function's name by default."""
        assert double_pod.label == "double"

    def test_explicit_label_overrides_function_name(self):
        from orcapod.core.function_pod import FunctionPod
        from orcapod.core.packet_function import PythonPacketFunction

        def my_func(x: int) -> int:
            return x

        pod = FunctionPod(
            packet_function=PythonPacketFunction(my_func, output_keys="result"),
            label="custom_label",
        )
        assert pod.label == "custom_label"


class TestFunctionPodStreamProtocolConformance:
    def test_satisfies_stream_protocol(self, double_pod):
        assert isinstance(double_pod.process(make_int_stream()), StreamProtocol)

    def test_has_producer_property(self, double_pod):
        _ = double_pod.process(make_int_stream()).producer

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
            assert isinstance(tag, TagProtocol)
            assert isinstance(packet, PacketProtocol)

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


# ---------------------------------------------------------------------------
# 5. Staleness and cache busting
# ---------------------------------------------------------------------------


class TestFunctionPodStreamStaleness:
    def test_is_stale_false_immediately_after_process(self, double_pod):
        """A freshly created stream is not stale."""
        stream = double_pod.process(make_int_stream(n=3))
        assert not stream.is_stale

    def test_is_stale_true_after_upstream_modified(self, double_pod):
        """Updating the upstream stream's modified time makes the stream stale."""
        import time

        input_stream = make_int_stream(n=3)
        stream = double_pod.process(input_stream)
        list(stream.iter_packets())  # populate cache

        time.sleep(0.01)
        input_stream._update_modified_time()

        assert stream.is_stale

    def test_is_stale_true_after_producer_updated(self, double_pod):
        """Updating the source pod's modified time makes the stream stale."""
        import time

        stream = double_pod.process(make_int_stream(n=3))
        list(stream.iter_packets())  # populate cache

        time.sleep(0.01)
        double_pod._update_modified_time()  # simulate pod being modified

        assert stream.is_stale

    def test_iter_packets_auto_clears_when_upstream_updated(self, double_pod):
        """iter_packets re-populates automatically when the upstream stream is modified."""
        import time

        input_stream = make_int_stream(n=3)
        stream = double_pod.process(input_stream)
        first = list(stream.iter_packets())

        time.sleep(0.01)
        input_stream._update_modified_time()
        assert stream.is_stale

        second = list(stream.iter_packets())
        assert len(second) == len(first)
        assert [p["result"] for _, p in second] == [p["result"] for _, p in first]

    def test_iter_packets_auto_clears_when_producer_updated(self, double_pod):
        """iter_packets re-populates automatically when the source pod is modified."""
        import time

        stream = double_pod.process(make_int_stream(n=3))
        first = list(stream.iter_packets())
        assert len(stream._cached_output_packets) == 3

        time.sleep(0.01)
        double_pod._update_modified_time()
        assert stream.is_stale

        second = list(stream.iter_packets())
        assert len(second) == len(first)
        assert [p["result"] for _, p in second] == [p["result"] for _, p in first]

    def test_as_table_auto_clears_when_producer_updated(self, double_pod):
        """as_table re-populates automatically when the source pod is modified."""
        import time

        stream = double_pod.process(make_int_stream(n=3))
        table_before = stream.as_table()
        assert len(table_before) == 3

        time.sleep(0.01)
        double_pod._update_modified_time()

        table_after = stream.as_table()
        assert len(table_after) == 3
        assert sorted(table_after.column("result").to_pylist()) == sorted(
            table_before.column("result").to_pylist()
        )

    def test_no_auto_clear_when_not_stale(self, double_pod):
        """When neither upstream nor pod has changed, iter_packets preserves the cache."""
        stream = double_pod.process(make_int_stream(n=3))
        list(stream.iter_packets())
        cached_count = len(stream._cached_output_packets)

        list(stream.iter_packets())
        assert len(stream._cached_output_packets) == cached_count
