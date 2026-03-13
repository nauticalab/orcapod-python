"""
Comprehensive tests for native streaming async_execute overrides.

Each operator's new streaming async_execute is tested to produce the same
results as the synchronous static_process path.  Tests mirror the sync
operator tests in ``tests/test_core/operators/test_operators.py``.

Covers:
- SelectTagColumns streaming: per-row tag column selection
- SelectPacketColumns streaming: per-row packet column selection
- DropTagColumns streaming: per-row tag column dropping
- DropPacketColumns streaming: per-row packet column dropping
- MapTags streaming: per-row tag column renaming
- MapPackets streaming: per-row packet column renaming
- Batch streaming: accumulate-and-emit full batches, partial batch handling
- SemiJoin build-probe: collect right, stream left through hash lookup
- Join: single-input passthrough, concurrent binary/N-ary collection
- Sync / async equivalence for every operator
- Empty input handling
- Multi-stage pipeline integration
"""

from __future__ import annotations

import asyncio

import pyarrow as pa
import pytest

from orcapod.channels import Channel
from orcapod.core.operators import (
    Batch,
    DropPacketColumns,
    DropTagColumns,
    Join,
    MapPackets,
    MapTags,
    SelectPacketColumns,
    SelectTagColumns,
    SemiJoin,
)
from orcapod.core.streams.arrow_table_stream import ArrowTableStream
from orcapod.system_constants import constants


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def make_simple_stream() -> ArrowTableStream:
    """Stream with 1 tag (animal) and 2 packet columns (weight, legs)."""
    table = pa.table(
        {
            "animal": ["cat", "dog", "bird"],
            "weight": [4.0, 12.0, 0.5],
            "legs": [4, 4, 2],
        }
    )
    return ArrowTableStream(table, tag_columns=["animal"])


def make_two_tag_stream() -> ArrowTableStream:
    """Stream with 2 tags (region, animal) and 1 packet column (count)."""
    table = pa.table(
        {
            "region": ["east", "east", "west"],
            "animal": ["cat", "dog", "cat"],
            "count": [10, 5, 8],
        }
    )
    return ArrowTableStream(table, tag_columns=["region", "animal"])


def make_int_stream(n: int = 3) -> ArrowTableStream:
    """Stream with tag=id, packet=x (ints)."""
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
        }
    )
    return ArrowTableStream(table, tag_columns=["id"])


def make_two_col_stream(n: int = 3) -> ArrowTableStream:
    """Stream with tag=id, packet={x, y}."""
    table = pa.table(
        {
            "id": pa.array(list(range(n)), type=pa.int64()),
            "x": pa.array(list(range(n)), type=pa.int64()),
            "y": pa.array([i * 10 for i in range(n)], type=pa.int64()),
        }
    )
    return ArrowTableStream(table, tag_columns=["id"])


def make_left_stream() -> ArrowTableStream:
    table = pa.table(
        {
            "id": pa.array([1, 2, 3], type=pa.int64()),
            "value_a": pa.array([10, 20, 30], type=pa.int64()),
        }
    )
    return ArrowTableStream(table, tag_columns=["id"])


def make_right_stream() -> ArrowTableStream:
    table = pa.table(
        {
            "id": pa.array([2, 3, 4], type=pa.int64()),
            "value_b": pa.array([200, 300, 400], type=pa.int64()),
        }
    )
    return ArrowTableStream(table, tag_columns=["id"])


def make_disjoint_stream() -> ArrowTableStream:
    """Stream with same tags as simple_stream but different packet columns."""
    table = pa.table(
        {
            "animal": ["cat", "dog", "bird"],
            "speed": [30.0, 45.0, 80.0],
        }
    )
    return ArrowTableStream(table, tag_columns=["animal"])


async def feed(stream: ArrowTableStream, ch: Channel) -> None:
    """Push all (tag, packet) from a stream into a channel, then close."""
    for tag, packet in stream.iter_packets():
        await ch.writer.send((tag, packet))
    await ch.writer.close()


async def run_unary(op, stream: ArrowTableStream) -> list[tuple]:
    """Run a unary operator async and collect results."""
    input_ch = Channel(buffer_size=1024)
    output_ch = Channel(buffer_size=1024)
    await feed(stream, input_ch)
    await op.async_execute([input_ch.reader], output_ch.writer)
    return await output_ch.reader.collect()


async def run_binary(
    op, left: ArrowTableStream, right: ArrowTableStream
) -> list[tuple]:
    """Run a binary operator async and collect results."""
    left_ch = Channel(buffer_size=1024)
    right_ch = Channel(buffer_size=1024)
    output_ch = Channel(buffer_size=1024)
    await feed(left, left_ch)
    await feed(right, right_ch)
    await op.async_execute([left_ch.reader, right_ch.reader], output_ch.writer)
    return await output_ch.reader.collect()


def sync_process_to_rows(op, *streams):
    """Run sync static_process and return list of (tag, packet) pairs."""
    result = op.static_process(*streams)
    return list(result.iter_packets())


# ===================================================================
# SelectTagColumns — streaming per-row
# ===================================================================


class TestSelectTagColumnsStreaming:
    @pytest.mark.asyncio
    async def test_keeps_only_selected_tags(self):
        stream = make_two_tag_stream()
        op = SelectTagColumns(columns=["region"])
        results = await run_unary(op, stream)

        assert len(results) == 3
        for tag, packet in results:
            tag_keys = tag.keys()
            assert "region" in tag_keys
            assert "animal" not in tag_keys
            # packet columns unchanged
            assert "count" in packet.keys()

    @pytest.mark.asyncio
    async def test_all_columns_selected_passthrough(self):
        """When all tag columns are already selected, rows pass through unaltered."""
        stream = make_two_tag_stream()
        op = SelectTagColumns(columns=["region", "animal"])
        results = await run_unary(op, stream)

        assert len(results) == 3
        for tag, packet in results:
            assert set(tag.keys()) == {"region", "animal"}
            assert "count" in packet.keys()

    @pytest.mark.asyncio
    async def test_data_values_preserved(self):
        stream = make_two_tag_stream()
        op = SelectTagColumns(columns=["region"])
        results = await run_unary(op, stream)

        regions = sorted(tag.as_dict()["region"] for tag, _ in results)
        assert regions == ["east", "east", "west"]

    @pytest.mark.asyncio
    async def test_empty_input(self):
        input_ch = Channel(buffer_size=4)
        output_ch = Channel(buffer_size=4)
        await input_ch.writer.close()
        op = SelectTagColumns(columns=["region"])
        await op.async_execute([input_ch.reader], output_ch.writer)
        results = await output_ch.reader.collect()
        assert results == []

    @pytest.mark.asyncio
    async def test_matches_sync_output(self):
        stream = make_two_tag_stream()
        op = SelectTagColumns(columns=["region"])

        async_results = await run_unary(op, stream)
        sync_results = sync_process_to_rows(op, stream)

        assert len(async_results) == len(sync_results)
        async_tags = sorted(t.as_dict()["region"] for t, _ in async_results)
        sync_tags = sorted(t.as_dict()["region"] for t, _ in sync_results)
        assert async_tags == sync_tags

    @pytest.mark.asyncio
    async def test_system_tags_preserved(self):
        """System tags on Tag objects should survive per-row selection."""
        from orcapod.core.sources.arrow_table_source import ArrowTableSource

        src = ArrowTableSource(
            pa.table(
                {
                    "region": ["east", "west"],
                    "animal": ["cat", "dog"],
                    "count": pa.array([10, 5], type=pa.int64()),
                }
            ),
            tag_columns=["region", "animal"],
        )
        op = SelectTagColumns(columns=["region"])
        results = await run_unary(op, src)

        assert len(results) == 2
        for tag, _ in results:
            sys_tags = tag.system_tags()
            # Source-backed streams have system tags
            assert len(sys_tags) > 0


# ===================================================================
# SelectPacketColumns — streaming per-row
# ===================================================================


class TestSelectPacketColumnsStreaming:
    @pytest.mark.asyncio
    async def test_keeps_only_selected_packets(self):
        stream = make_simple_stream()
        op = SelectPacketColumns(columns=["weight"])
        results = await run_unary(op, stream)

        assert len(results) == 3
        for _, packet in results:
            pkt_keys = packet.keys()
            assert "weight" in pkt_keys
            assert "legs" not in pkt_keys
            # tag columns unchanged
        for tag, _ in results:
            assert "animal" in tag.keys()

    @pytest.mark.asyncio
    async def test_all_columns_selected_passthrough(self):
        stream = make_simple_stream()
        op = SelectPacketColumns(columns=["weight", "legs"])
        results = await run_unary(op, stream)

        assert len(results) == 3
        for _, packet in results:
            assert set(packet.keys()) == {"weight", "legs"}

    @pytest.mark.asyncio
    async def test_data_values_preserved(self):
        stream = make_simple_stream()
        op = SelectPacketColumns(columns=["weight"])
        results = await run_unary(op, stream)

        weights = sorted(pkt.as_dict()["weight"] for _, pkt in results)
        assert weights == [0.5, 4.0, 12.0]

    @pytest.mark.asyncio
    async def test_empty_input(self):
        input_ch = Channel(buffer_size=4)
        output_ch = Channel(buffer_size=4)
        await input_ch.writer.close()
        op = SelectPacketColumns(columns=["weight"])
        await op.async_execute([input_ch.reader], output_ch.writer)
        results = await output_ch.reader.collect()
        assert results == []

    @pytest.mark.asyncio
    async def test_matches_sync_output(self):
        stream = make_simple_stream()
        op = SelectPacketColumns(columns=["weight"])

        async_results = await run_unary(op, stream)
        sync_results = sync_process_to_rows(op, stream)

        assert len(async_results) == len(sync_results)
        async_vals = sorted(p.as_dict()["weight"] for _, p in async_results)
        sync_vals = sorted(p.as_dict()["weight"] for _, p in sync_results)
        assert async_vals == sync_vals

    @pytest.mark.asyncio
    async def test_source_info_for_dropped_columns_not_surfaced(self):
        """Source info for dropped packet columns should not appear in output."""
        from orcapod.core.sources.arrow_table_source import ArrowTableSource

        src = ArrowTableSource(
            pa.table(
                {
                    "animal": ["cat", "dog"],
                    "weight": [4.0, 12.0],
                    "legs": pa.array([4, 4], type=pa.int64()),
                }
            ),
            tag_columns=["animal"],
        )
        op = SelectPacketColumns(columns=["weight"])
        results = await run_unary(op, src)

        for _, packet in results:
            si = packet.source_info()
            assert "legs" not in si
            assert "weight" in si


# ===================================================================
# DropTagColumns — streaming per-row
# ===================================================================


class TestDropTagColumnsStreaming:
    @pytest.mark.asyncio
    async def test_drops_specified_tags(self):
        stream = make_two_tag_stream()
        op = DropTagColumns(columns=["region"])
        results = await run_unary(op, stream)

        assert len(results) == 3
        for tag, packet in results:
            assert "region" not in tag.keys()
            assert "animal" in tag.keys()
            assert "count" in packet.keys()

    @pytest.mark.asyncio
    async def test_no_columns_to_drop_passthrough(self):
        stream = make_two_tag_stream()
        op = DropTagColumns(columns=["nonexistent"], strict=False)
        results = await run_unary(op, stream)

        assert len(results) == 3
        for tag, _ in results:
            assert set(tag.keys()) == {"region", "animal"}

    @pytest.mark.asyncio
    async def test_data_values_preserved(self):
        stream = make_two_tag_stream()
        op = DropTagColumns(columns=["region"])
        results = await run_unary(op, stream)

        animals = sorted(tag.as_dict()["animal"] for tag, _ in results)
        assert animals == ["cat", "cat", "dog"]

    @pytest.mark.asyncio
    async def test_empty_input(self):
        input_ch = Channel(buffer_size=4)
        output_ch = Channel(buffer_size=4)
        await input_ch.writer.close()
        op = DropTagColumns(columns=["region"])
        await op.async_execute([input_ch.reader], output_ch.writer)
        results = await output_ch.reader.collect()
        assert results == []

    @pytest.mark.asyncio
    async def test_matches_sync_output(self):
        stream = make_two_tag_stream()
        op = DropTagColumns(columns=["region"])

        async_results = await run_unary(op, stream)
        sync_results = sync_process_to_rows(op, stream)

        assert len(async_results) == len(sync_results)
        async_animals = sorted(t.as_dict()["animal"] for t, _ in async_results)
        sync_animals = sorted(t.as_dict()["animal"] for t, _ in sync_results)
        assert async_animals == sync_animals


# ===================================================================
# DropPacketColumns — streaming per-row
# ===================================================================


class TestDropPacketColumnsStreaming:
    @pytest.mark.asyncio
    async def test_drops_specified_packets(self):
        stream = make_simple_stream()
        op = DropPacketColumns(columns=["legs"])
        results = await run_unary(op, stream)

        assert len(results) == 3
        for _, packet in results:
            assert "legs" not in packet.keys()
            assert "weight" in packet.keys()
        for tag, _ in results:
            assert "animal" in tag.keys()

    @pytest.mark.asyncio
    async def test_no_columns_to_drop_passthrough(self):
        stream = make_simple_stream()
        op = DropPacketColumns(columns=["nonexistent"], strict=False)
        results = await run_unary(op, stream)

        assert len(results) == 3
        for _, packet in results:
            assert set(packet.keys()) == {"weight", "legs"}

    @pytest.mark.asyncio
    async def test_data_values_preserved(self):
        stream = make_simple_stream()
        op = DropPacketColumns(columns=["legs"])
        results = await run_unary(op, stream)

        weights = sorted(pkt.as_dict()["weight"] for _, pkt in results)
        assert weights == [0.5, 4.0, 12.0]

    @pytest.mark.asyncio
    async def test_empty_input(self):
        input_ch = Channel(buffer_size=4)
        output_ch = Channel(buffer_size=4)
        await input_ch.writer.close()
        op = DropPacketColumns(columns=["legs"])
        await op.async_execute([input_ch.reader], output_ch.writer)
        results = await output_ch.reader.collect()
        assert results == []

    @pytest.mark.asyncio
    async def test_matches_sync_output(self):
        stream = make_simple_stream()
        op = DropPacketColumns(columns=["legs"])

        async_results = await run_unary(op, stream)
        sync_results = sync_process_to_rows(op, stream)

        assert len(async_results) == len(sync_results)
        async_vals = sorted(p.as_dict()["weight"] for _, p in async_results)
        sync_vals = sorted(p.as_dict()["weight"] for _, p in sync_results)
        assert async_vals == sync_vals

    @pytest.mark.asyncio
    async def test_source_info_for_dropped_columns_not_surfaced(self):
        from orcapod.core.sources.arrow_table_source import ArrowTableSource

        src = ArrowTableSource(
            pa.table(
                {
                    "animal": ["cat", "dog"],
                    "weight": [4.0, 12.0],
                    "legs": pa.array([4, 4], type=pa.int64()),
                }
            ),
            tag_columns=["animal"],
        )
        op = DropPacketColumns(columns=["legs"])
        results = await run_unary(op, src)

        for _, packet in results:
            si = packet.source_info()
            assert "legs" not in si
            assert "weight" in si


# ===================================================================
# MapTags — streaming per-row
# ===================================================================


class TestMapTagsStreaming:
    @pytest.mark.asyncio
    async def test_renames_tag_column(self):
        stream = make_two_tag_stream()
        op = MapTags(name_map={"region": "area"})
        results = await run_unary(op, stream)

        assert len(results) == 3
        for tag, _ in results:
            tag_keys = tag.keys()
            assert "area" in tag_keys
            assert "region" not in tag_keys

    @pytest.mark.asyncio
    async def test_data_values_preserved(self):
        stream = make_two_tag_stream()
        op = MapTags(name_map={"region": "area"})
        results = await run_unary(op, stream)

        areas = sorted(tag.as_dict()["area"] for tag, _ in results)
        assert areas == ["east", "east", "west"]

    @pytest.mark.asyncio
    async def test_drop_unmapped(self):
        stream = make_two_tag_stream()
        op = MapTags(name_map={"region": "area"}, drop_unmapped=True)
        results = await run_unary(op, stream)

        assert len(results) == 3
        for tag, _ in results:
            tag_keys = tag.keys()
            assert "area" in tag_keys
            assert "animal" not in tag_keys  # dropped because unmapped

    @pytest.mark.asyncio
    async def test_no_matching_rename_passthrough(self):
        stream = make_two_tag_stream()
        op = MapTags(name_map={"nonexistent": "nope"})
        results = await run_unary(op, stream)

        assert len(results) == 3
        for tag, _ in results:
            assert set(tag.keys()) == {"region", "animal"}

    @pytest.mark.asyncio
    async def test_empty_input(self):
        input_ch = Channel(buffer_size=4)
        output_ch = Channel(buffer_size=4)
        await input_ch.writer.close()
        op = MapTags(name_map={"region": "area"})
        await op.async_execute([input_ch.reader], output_ch.writer)
        results = await output_ch.reader.collect()
        assert results == []

    @pytest.mark.asyncio
    async def test_matches_sync_output(self):
        stream = make_two_tag_stream()
        op = MapTags(name_map={"region": "area"})

        async_results = await run_unary(op, stream)
        sync_results = sync_process_to_rows(op, stream)

        assert len(async_results) == len(sync_results)
        async_areas = sorted(t.as_dict()["area"] for t, _ in async_results)
        sync_areas = sorted(t.as_dict()["area"] for t, _ in sync_results)
        assert async_areas == sync_areas

    @pytest.mark.asyncio
    async def test_matches_sync_output_with_drop_unmapped(self):
        stream = make_two_tag_stream()
        op = MapTags(name_map={"region": "area"}, drop_unmapped=True)

        async_results = await run_unary(op, stream)
        sync_results = sync_process_to_rows(op, stream)

        assert len(async_results) == len(sync_results)
        for (at, ap), (st, sp) in zip(
            sorted(async_results, key=lambda x: x[0].as_dict()["area"]),
            sorted(sync_results, key=lambda x: x[0].as_dict()["area"]),
        ):
            assert at.as_dict() == st.as_dict()
            assert ap.as_dict() == sp.as_dict()


# ===================================================================
# MapPackets — streaming per-row
# ===================================================================


class TestMapPacketsStreaming:
    @pytest.mark.asyncio
    async def test_renames_packet_column(self):
        stream = make_simple_stream()
        op = MapPackets(name_map={"weight": "mass"})
        results = await run_unary(op, stream)

        assert len(results) == 3
        for _, packet in results:
            pkt_keys = packet.keys()
            assert "mass" in pkt_keys
            assert "weight" not in pkt_keys

    @pytest.mark.asyncio
    async def test_data_values_preserved(self):
        stream = make_simple_stream()
        op = MapPackets(name_map={"weight": "mass"})
        results = await run_unary(op, stream)

        masses = sorted(pkt.as_dict()["mass"] for _, pkt in results)
        assert masses == [0.5, 4.0, 12.0]

    @pytest.mark.asyncio
    async def test_drop_unmapped(self):
        stream = make_simple_stream()
        op = MapPackets(name_map={"weight": "mass"}, drop_unmapped=True)
        results = await run_unary(op, stream)

        assert len(results) == 3
        for _, packet in results:
            pkt_keys = packet.keys()
            assert "mass" in pkt_keys
            assert "legs" not in pkt_keys  # dropped because unmapped

    @pytest.mark.asyncio
    async def test_source_info_renamed(self):
        """Packet.rename() should update source_info keys."""
        from orcapod.core.sources.arrow_table_source import ArrowTableSource

        src = ArrowTableSource(
            pa.table(
                {
                    "animal": ["cat", "dog"],
                    "weight": [4.0, 12.0],
                    "legs": pa.array([4, 4], type=pa.int64()),
                }
            ),
            tag_columns=["animal"],
        )
        op = MapPackets(name_map={"weight": "mass"})
        results = await run_unary(op, src)

        for _, packet in results:
            si = packet.source_info()
            assert "mass" in si
            assert "weight" not in si

    @pytest.mark.asyncio
    async def test_no_matching_rename_passthrough(self):
        stream = make_simple_stream()
        op = MapPackets(name_map={"nonexistent": "nope"})
        results = await run_unary(op, stream)

        assert len(results) == 3
        for _, packet in results:
            assert set(packet.keys()) == {"weight", "legs"}

    @pytest.mark.asyncio
    async def test_empty_input(self):
        input_ch = Channel(buffer_size=4)
        output_ch = Channel(buffer_size=4)
        await input_ch.writer.close()
        op = MapPackets(name_map={"weight": "mass"})
        await op.async_execute([input_ch.reader], output_ch.writer)
        results = await output_ch.reader.collect()
        assert results == []

    @pytest.mark.asyncio
    async def test_matches_sync_output(self):
        stream = make_simple_stream()
        op = MapPackets(name_map={"weight": "mass"})

        async_results = await run_unary(op, stream)
        sync_results = sync_process_to_rows(op, stream)

        assert len(async_results) == len(sync_results)
        async_masses = sorted(p.as_dict()["mass"] for _, p in async_results)
        sync_masses = sorted(p.as_dict()["mass"] for _, p in sync_results)
        assert async_masses == sync_masses


# ===================================================================
# Batch — streaming accumulate-and-emit
# ===================================================================


class TestBatchStreaming:
    @pytest.mark.asyncio
    async def test_batch_groups_rows(self):
        stream = make_simple_stream()  # 3 rows
        op = Batch(batch_size=2)
        results = await run_unary(op, stream)

        # 3 rows / batch_size=2 → 2 batches (full + partial)
        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_batch_drop_partial(self):
        stream = make_simple_stream()  # 3 rows
        op = Batch(batch_size=2, drop_partial_batch=True)
        results = await run_unary(op, stream)

        # 3 rows / batch_size=2 with drop → 1 batch
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_batch_size_zero_single_batch(self):
        stream = make_simple_stream()  # 3 rows
        op = Batch(batch_size=0)
        results = await run_unary(op, stream)

        # batch_size=0 → all in one batch
        assert len(results) == 1

    @pytest.mark.asyncio
    async def test_batch_values_are_lists(self):
        stream = make_int_stream(4)
        op = Batch(batch_size=2)
        results = await run_unary(op, stream)

        assert len(results) == 2
        for tag, packet in results:
            # Each value should be a list
            tag_d = tag.as_dict()
            pkt_d = packet.as_dict()
            assert isinstance(tag_d["id"], list)
            assert isinstance(pkt_d["x"], list)
            assert len(tag_d["id"]) == 2
            assert len(pkt_d["x"]) == 2

    @pytest.mark.asyncio
    async def test_batch_exact_multiple(self):
        stream = make_int_stream(6)
        op = Batch(batch_size=2)
        results = await run_unary(op, stream)

        # 6 / 2 = 3 full batches, no partial
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_batch_exact_multiple_drop_partial(self):
        stream = make_int_stream(6)
        op = Batch(batch_size=2, drop_partial_batch=True)
        results = await run_unary(op, stream)

        # Same as without drop since there's no partial batch
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_empty_input(self):
        input_ch = Channel(buffer_size=4)
        output_ch = Channel(buffer_size=4)
        await input_ch.writer.close()
        op = Batch(batch_size=2)
        await op.async_execute([input_ch.reader], output_ch.writer)
        results = await output_ch.reader.collect()
        assert results == []

    @pytest.mark.asyncio
    async def test_matches_sync_output(self):
        stream = make_int_stream(7)
        op = Batch(batch_size=3)

        async_results = await run_unary(op, stream)
        sync_results = sync_process_to_rows(op, stream)

        assert len(async_results) == len(sync_results)
        # Each batch should have the same data
        for (at, ap), (st, sp) in zip(async_results, sync_results):
            assert at.as_dict() == st.as_dict()
            assert ap.as_dict() == sp.as_dict()

    @pytest.mark.asyncio
    async def test_matches_sync_output_batch_zero(self):
        stream = make_int_stream(5)
        op = Batch(batch_size=0)

        async_results = await run_unary(op, stream)
        sync_results = sync_process_to_rows(op, stream)

        assert len(async_results) == len(sync_results) == 1
        assert async_results[0][0].as_dict() == sync_results[0][0].as_dict()
        assert async_results[0][1].as_dict() == sync_results[0][1].as_dict()

    @pytest.mark.asyncio
    async def test_matches_sync_output_drop_partial(self):
        stream = make_int_stream(5)
        op = Batch(batch_size=3, drop_partial_batch=True)

        async_results = await run_unary(op, stream)
        sync_results = sync_process_to_rows(op, stream)

        assert len(async_results) == len(sync_results)
        for (at, ap), (st, sp) in zip(async_results, sync_results):
            assert at.as_dict() == st.as_dict()
            assert ap.as_dict() == sp.as_dict()


# ===================================================================
# SemiJoin — build-probe
# ===================================================================


class TestSemiJoinBuildProbe:
    @pytest.mark.asyncio
    async def test_filters_left_by_right(self):
        left = make_left_stream()  # id=[1,2,3]
        right = make_right_stream()  # id=[2,3,4]
        op = SemiJoin()
        results = await run_binary(op, left, right)

        ids = sorted(tag.as_dict()["id"] for tag, _ in results)
        assert ids == [2, 3]

    @pytest.mark.asyncio
    async def test_preserves_left_schema(self):
        left = make_left_stream()
        right = make_right_stream()
        op = SemiJoin()
        results = await run_binary(op, left, right)

        for tag, packet in results:
            assert "id" in tag.keys()
            assert "value_a" in packet.keys()
            assert "value_b" not in packet.keys()

    @pytest.mark.asyncio
    async def test_preserves_left_data(self):
        left = make_left_stream()
        right = make_right_stream()
        op = SemiJoin()
        results = await run_binary(op, left, right)

        result_map = {
            tag.as_dict()["id"]: pkt.as_dict()["value_a"] for tag, pkt in results
        }
        assert result_map[2] == 20
        assert result_map[3] == 30

    @pytest.mark.asyncio
    async def test_no_common_keys_returns_all_left(self):
        left_table = pa.table(
            {
                "a": pa.array([1, 2, 3], type=pa.int64()),
                "x": pa.array([10, 20, 30], type=pa.int64()),
            }
        )
        right_table = pa.table(
            {
                "b": pa.array([1, 2], type=pa.int64()),
                "y": pa.array([100, 200], type=pa.int64()),
            }
        )
        left = ArrowTableStream(left_table, tag_columns=["a"])
        right = ArrowTableStream(right_table, tag_columns=["b"])
        op = SemiJoin()
        results = await run_binary(op, left, right)

        assert len(results) == 3  # all left rows pass through

    @pytest.mark.asyncio
    async def test_no_matching_rows_empty_result(self):
        left_table = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "x": pa.array([10, 20], type=pa.int64()),
            }
        )
        right_table = pa.table(
            {
                "id": pa.array([3, 4], type=pa.int64()),
                "y": pa.array([30, 40], type=pa.int64()),
            }
        )
        left = ArrowTableStream(left_table, tag_columns=["id"])
        right = ArrowTableStream(right_table, tag_columns=["id"])
        op = SemiJoin()
        results = await run_binary(op, left, right)

        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_empty_left_returns_empty(self):
        """Empty left input produces empty output regardless of right."""
        right_table = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "y": pa.array([100, 200], type=pa.int64()),
            }
        )
        right = ArrowTableStream(right_table, tag_columns=["id"])

        left_ch = Channel(buffer_size=4)
        right_ch = Channel(buffer_size=64)
        output_ch = Channel(buffer_size=64)

        await left_ch.writer.close()
        await feed(right, right_ch)

        op = SemiJoin()
        await op.async_execute([left_ch.reader, right_ch.reader], output_ch.writer)
        results = await output_ch.reader.collect()
        assert results == []

    @pytest.mark.asyncio
    async def test_empty_right_returns_empty_or_all(self):
        """Empty right: if common keys, result is empty; if no common keys, left passes through.
        Since both sides are empty-right, we rely on the barrier fallback."""
        left = make_left_stream()

        left_ch = Channel(buffer_size=64)
        right_ch = Channel(buffer_size=4)
        output_ch = Channel(buffer_size=64)

        await feed(left, left_ch)
        await right_ch.writer.close()

        op = SemiJoin()
        await op.async_execute([left_ch.reader, right_ch.reader], output_ch.writer)
        results = await output_ch.reader.collect()
        # With empty right and no schema information available,
        # the implementation falls back to passing left through
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_matches_sync_output(self):
        left = make_left_stream()
        right = make_right_stream()
        op = SemiJoin()

        async_results = await run_binary(op, left, right)
        sync_results = sync_process_to_rows(op, left, right)

        assert len(async_results) == len(sync_results)
        async_ids = sorted(t.as_dict()["id"] for t, _ in async_results)
        sync_ids = sorted(t.as_dict()["id"] for t, _ in sync_results)
        assert async_ids == sync_ids

    @pytest.mark.asyncio
    async def test_large_input_streaming(self):
        """SemiJoin should handle larger inputs correctly with build-probe."""
        left_table = pa.table(
            {
                "id": pa.array(list(range(100)), type=pa.int64()),
                "x": pa.array(list(range(100)), type=pa.int64()),
            }
        )
        right_table = pa.table(
            {
                "id": pa.array(list(range(0, 100, 3)), type=pa.int64()),  # every 3rd
                "y": pa.array(list(range(0, 100, 3)), type=pa.int64()),
            }
        )
        left = ArrowTableStream(left_table, tag_columns=["id"])
        right = ArrowTableStream(right_table, tag_columns=["id"])
        op = SemiJoin()
        results = await run_binary(op, left, right)

        expected_ids = list(range(0, 100, 3))
        result_ids = sorted(t.as_dict()["id"] for t, _ in results)
        assert result_ids == expected_ids


# ===================================================================
# Join — native async
# ===================================================================


class TestJoinNativeAsync:
    """Tests for Join.async_execute (symmetric hash join + N>2 barrier)."""

    @pytest.mark.asyncio
    async def test_single_input_passthrough(self):
        stream = make_int_stream(3)
        op = Join()

        input_ch = Channel(buffer_size=64)
        output_ch = Channel(buffer_size=64)
        await feed(stream, input_ch)
        await op.async_execute([input_ch.reader], output_ch.writer)
        results = await output_ch.reader.collect()

        assert len(results) == 3
        ids = sorted(t.as_dict()["id"] for t, _ in results)
        assert ids == [0, 1, 2]

    @pytest.mark.asyncio
    async def test_two_way_join(self):
        left = make_simple_stream()
        right = make_disjoint_stream()
        op = Join()
        results = await run_binary(op, left, right)

        assert len(results) == 3
        for tag, packet in results:
            assert "animal" in tag.keys()
            pkt_d = packet.as_dict()
            assert "weight" in pkt_d
            assert "speed" in pkt_d

    @pytest.mark.asyncio
    async def test_two_way_join_data_correct(self):
        left_table = pa.table(
            {
                "id": pa.array([0, 1, 2], type=pa.int64()),
                "x": pa.array([10, 20, 30], type=pa.int64()),
            }
        )
        right_table = pa.table(
            {
                "id": pa.array([0, 1, 2], type=pa.int64()),
                "y": pa.array([100, 200, 300], type=pa.int64()),
            }
        )
        left = ArrowTableStream(left_table, tag_columns=["id"])
        right = ArrowTableStream(right_table, tag_columns=["id"])
        op = Join()
        results = await run_binary(op, left, right)

        assert len(results) == 3
        result_map = {tag.as_dict()["id"]: pkt.as_dict() for tag, pkt in results}
        assert result_map[0] == {"x": 10, "y": 100}
        assert result_map[1] == {"x": 20, "y": 200}
        assert result_map[2] == {"x": 30, "y": 300}

    @pytest.mark.asyncio
    async def test_three_way_join(self):
        t1 = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "a": pa.array([10, 20], type=pa.int64()),
            }
        )
        t2 = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "b": pa.array([100, 200], type=pa.int64()),
            }
        )
        t3 = pa.table(
            {
                "id": pa.array([1, 2], type=pa.int64()),
                "c": pa.array([1000, 2000], type=pa.int64()),
            }
        )
        s1 = ArrowTableStream(t1, tag_columns=["id"])
        s2 = ArrowTableStream(t2, tag_columns=["id"])
        s3 = ArrowTableStream(t3, tag_columns=["id"])

        op = Join()
        ch1 = Channel(buffer_size=64)
        ch2 = Channel(buffer_size=64)
        ch3 = Channel(buffer_size=64)
        out = Channel(buffer_size=64)

        await feed(s1, ch1)
        await feed(s2, ch2)
        await feed(s3, ch3)
        await op.async_execute([ch1.reader, ch2.reader, ch3.reader], out.writer)
        results = await out.reader.collect()

        assert len(results) == 2
        result_map = {tag.as_dict()["id"]: pkt.as_dict() for tag, pkt in results}
        assert result_map[1] == {"a": 10, "b": 100, "c": 1000}
        assert result_map[2] == {"a": 20, "b": 200, "c": 2000}

    @pytest.mark.asyncio
    async def test_join_no_shared_tags_cartesian(self):
        """When no shared tag keys, join produces a cartesian product."""
        left_table = pa.table(
            {
                "a": pa.array([1, 2], type=pa.int64()),
                "x": pa.array([10, 20], type=pa.int64()),
            }
        )
        right_table = pa.table(
            {
                "b": pa.array([3, 4], type=pa.int64()),
                "y": pa.array([30, 40], type=pa.int64()),
            }
        )
        left = ArrowTableStream(left_table, tag_columns=["a"])
        right = ArrowTableStream(right_table, tag_columns=["b"])
        op = Join()
        results = await run_binary(op, left, right)

        # 2 × 2 = 4 cartesian product
        assert len(results) == 4

    @pytest.mark.asyncio
    async def test_empty_input_single(self):
        input_ch = Channel(buffer_size=4)
        output_ch = Channel(buffer_size=4)
        await input_ch.writer.close()
        op = Join()
        await op.async_execute([input_ch.reader], output_ch.writer)
        results = await output_ch.reader.collect()
        assert results == []

    @pytest.mark.asyncio
    async def test_matches_sync_two_way(self):
        left = make_simple_stream()
        right = make_disjoint_stream()
        op = Join()

        async_results = await run_binary(op, left, right)
        sync_results = sync_process_to_rows(op, left, right)

        assert len(async_results) == len(sync_results)
        async_data = sorted(
            (t.as_dict()["animal"], p.as_dict()) for t, p in async_results
        )
        sync_data = sorted(
            (t.as_dict()["animal"], p.as_dict()) for t, p in sync_results
        )
        assert async_data == sync_data


# ===================================================================
# Multi-stage pipeline integration
# ===================================================================


class TestStreamingPipelineIntegration:
    @pytest.mark.asyncio
    async def test_select_then_map_chain(self):
        """SelectTagColumns → MapTags in a streaming pipeline."""
        stream = make_two_tag_stream()

        select_op = SelectTagColumns(columns=["region"])
        map_op = MapTags(name_map={"region": "area"})

        ch1 = Channel(buffer_size=16)
        ch2 = Channel(buffer_size=16)
        ch3 = Channel(buffer_size=16)

        async def source():
            for tag, packet in stream.iter_packets():
                await ch1.writer.send((tag, packet))
            await ch1.writer.close()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(source())
            tg.create_task(select_op.async_execute([ch1.reader], ch2.writer))
            tg.create_task(map_op.async_execute([ch2.reader], ch3.writer))

        results = await ch3.reader.collect()
        assert len(results) == 3
        for tag, _ in results:
            assert "area" in tag.keys()
            assert "region" not in tag.keys()
            assert "animal" not in tag.keys()

    @pytest.mark.asyncio
    async def test_join_then_select_chain(self):
        """Join → SelectPacketColumns in a streaming pipeline."""
        left_table = pa.table(
            {
                "id": pa.array([0, 1, 2], type=pa.int64()),
                "x": pa.array([10, 20, 30], type=pa.int64()),
            }
        )
        right_table = pa.table(
            {
                "id": pa.array([0, 1, 2], type=pa.int64()),
                "y": pa.array([100, 200, 300], type=pa.int64()),
            }
        )
        left = ArrowTableStream(left_table, tag_columns=["id"])
        right = ArrowTableStream(right_table, tag_columns=["id"])

        join_op = Join()
        select_op = SelectPacketColumns(columns=["x"])

        ch_l = Channel(buffer_size=16)
        ch_r = Channel(buffer_size=16)
        ch_joined = Channel(buffer_size=16)
        ch_out = Channel(buffer_size=16)

        async def push(stream, ch):
            for tag, packet in stream.iter_packets():
                await ch.writer.send((tag, packet))
            await ch.writer.close()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(push(left, ch_l))
            tg.create_task(push(right, ch_r))
            tg.create_task(
                join_op.async_execute([ch_l.reader, ch_r.reader], ch_joined.writer)
            )
            tg.create_task(select_op.async_execute([ch_joined.reader], ch_out.writer))

        results = await ch_out.reader.collect()
        assert len(results) == 3
        for _, packet in results:
            assert "x" in packet.keys()
            assert "y" not in packet.keys()

    @pytest.mark.asyncio
    async def test_semijoin_then_batch_chain(self):
        """SemiJoin → Batch in a streaming pipeline."""
        left = make_left_stream()  # id=[1,2,3]
        right = make_right_stream()  # id=[2,3,4]

        semi_op = SemiJoin()
        batch_op = Batch(batch_size=2)

        ch_l = Channel(buffer_size=16)
        ch_r = Channel(buffer_size=16)
        ch_semi = Channel(buffer_size=16)
        ch_out = Channel(buffer_size=16)

        async def push(stream, ch):
            for tag, packet in stream.iter_packets():
                await ch.writer.send((tag, packet))
            await ch.writer.close()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(push(left, ch_l))
            tg.create_task(push(right, ch_r))
            tg.create_task(
                semi_op.async_execute([ch_l.reader, ch_r.reader], ch_semi.writer)
            )
            tg.create_task(batch_op.async_execute([ch_semi.reader], ch_out.writer))

        results = await ch_out.reader.collect()
        # SemiJoin produces 2 rows (id=[2,3]), Batch(2) → 1 batch
        assert len(results) == 1
        tag_d = results[0][0].as_dict()
        assert isinstance(tag_d["id"], list)
        assert sorted(tag_d["id"]) == [2, 3]

    @pytest.mark.asyncio
    async def test_drop_map_select_three_stage(self):
        """DropPacketColumns → MapPackets → SelectPacketColumns chain."""
        stream = make_simple_stream()  # animal | weight, legs

        drop_op = DropPacketColumns(columns=["legs"])
        map_op = MapPackets(name_map={"weight": "mass"})
        # After map: mass (only packet column)
        select_op = SelectPacketColumns(columns=["mass"])

        ch1 = Channel(buffer_size=16)
        ch2 = Channel(buffer_size=16)
        ch3 = Channel(buffer_size=16)
        ch4 = Channel(buffer_size=16)

        async def source():
            for tag, packet in stream.iter_packets():
                await ch1.writer.send((tag, packet))
            await ch1.writer.close()

        async with asyncio.TaskGroup() as tg:
            tg.create_task(source())
            tg.create_task(drop_op.async_execute([ch1.reader], ch2.writer))
            tg.create_task(map_op.async_execute([ch2.reader], ch3.writer))
            tg.create_task(select_op.async_execute([ch3.reader], ch4.writer))

        results = await ch4.reader.collect()
        assert len(results) == 3
        for _, packet in results:
            assert packet.keys() == ("mass",)
        masses = sorted(pkt.as_dict()["mass"] for _, pkt in results)
        assert masses == [0.5, 4.0, 12.0]


# ===================================================================
# Sync vs Async system-tag equivalence
# ===================================================================


def _make_source(tag_col: str, packet_col: str, data: dict) -> ArrowTableStream:
    """Build an ArrowTableSource (which generates system tags) and return its stream."""
    from orcapod.core.sources.arrow_table_source import ArrowTableSource

    table = pa.table(
        {
            tag_col: pa.array(data[tag_col], type=pa.large_string()),
            packet_col: pa.array(data[packet_col], type=pa.int64()),
        }
    )
    return ArrowTableSource(table, tag_columns=[tag_col])


async def run_binary_validated(
    op,
    left: ArrowTableStream,
    right: ArrowTableStream,
) -> list[tuple]:
    """Run a binary operator async with validation and pipeline hashes.

    Calls ``validate_inputs`` for schema validation, then passes
    ``input_pipeline_hashes`` so operators like ``Join`` can compute
    canonical system-tag column names.
    """
    op.validate_inputs(left, right)
    left_ch = Channel(buffer_size=1024)
    right_ch = Channel(buffer_size=1024)
    output_ch = Channel(buffer_size=1024)
    await feed(left, left_ch)
    await feed(right, right_ch)
    hashes = [left.pipeline_hash(), right.pipeline_hash()]
    await op.async_execute(
        [left_ch.reader, right_ch.reader],
        output_ch.writer,
        input_pipeline_hashes=hashes,
    )
    return await output_ch.reader.collect()


def _extract_system_tags(
    rows: list[tuple],
) -> list[dict[str, str]]:
    """Extract sorted system-tag dicts from (tag, packet) pairs."""
    return sorted(
        [tag.system_tags() for tag, _ in rows],
        key=lambda d: sorted(d.items()),
    )


def _extract_system_tag_keys(rows: list[tuple]) -> set[str]:
    """Collect all unique system-tag keys across rows."""
    keys: set[str] = set()
    for tag, _ in rows:
        keys.update(tag.system_tags().keys())
    return keys


class TestJoinSystemTagEquivalence:
    """Verify that Join.async_execute produces the same system-tag column
    names and values as the sync static_process path.

    Uses ``ArrowTableSource`` (which adds system-tag columns) rather than
    bare ``ArrowTableStream`` to ensure system tags are present.
    """

    @pytest.mark.asyncio
    async def test_two_way_system_tag_column_names_match(self):
        """System-tag column names must be identical between sync and async."""
        left = _make_source("key", "value", {"key": ["a", "b"], "value": [10, 20]})
        right = _make_source("key", "score", {"key": ["a", "b"], "score": [100, 200]})
        op = Join()

        # Sync
        sync_result = op.static_process(left, right)
        sync_rows = list(sync_result.iter_packets())

        # Async
        async_rows = await run_binary_validated(op, left, right)

        sync_sys_keys = _extract_system_tag_keys(sync_rows)
        async_sys_keys = _extract_system_tag_keys(async_rows)

        assert sync_sys_keys, "Expected system tags to be present"
        assert sync_sys_keys == async_sys_keys

    @pytest.mark.asyncio
    async def test_two_way_system_tag_values_match(self):
        """System-tag values for each row must match between sync and async."""
        left = _make_source(
            "key", "value", {"key": ["a", "b", "c"], "value": [1, 2, 3]}
        )
        right = _make_source(
            "key", "score", {"key": ["a", "b", "c"], "score": [10, 20, 30]}
        )
        op = Join()

        sync_result = op.static_process(left, right)
        sync_rows = list(sync_result.iter_packets())
        async_rows = await run_binary_validated(op, left, right)

        assert len(sync_rows) == len(async_rows)

        sync_sys = _extract_system_tags(sync_rows)
        async_sys = _extract_system_tags(async_rows)
        assert sync_sys == async_sys

    @pytest.mark.asyncio
    async def test_two_way_system_tag_suffixes_use_pipeline_hash(self):
        """System-tag column names should contain the pipeline_hash and
        canonical position, matching the name-extending convention."""
        left = _make_source("key", "val", {"key": ["x"], "val": [1]})
        right = _make_source("key", "score", {"key": ["x"], "score": [2]})
        op = Join()

        async_rows = await run_binary_validated(op, left, right)
        sys_keys = _extract_system_tag_keys(async_rows)

        # Each system-tag key should end with :{canonical_position}
        for key in sys_keys:
            assert key.startswith(constants.SYSTEM_TAG_PREFIX)
            assert key[-2:] in (":0", ":1"), (
                f"System tag key {key!r} does not end with :0 or :1"
            )

    @pytest.mark.asyncio
    async def test_commutativity_system_tags_identical(self):
        """Join(A, B) and Join(B, A) should produce identical system tags
        (Join is commutative — canonical ordering by pipeline_hash)."""
        src_a = _make_source("id", "x", {"id": ["p", "q"], "x": [1, 2]})
        src_b = _make_source("id", "y", {"id": ["p", "q"], "y": [10, 20]})
        op = Join()

        rows_ab = await run_binary_validated(op, src_a, src_b)
        rows_ba = await run_binary_validated(op, src_b, src_a)

        assert len(rows_ab) == len(rows_ba)

        sys_ab = _extract_system_tags(rows_ab)
        sys_ba = _extract_system_tags(rows_ba)
        assert sys_ab == sys_ba

    @pytest.mark.asyncio
    async def test_three_way_system_tags_match_sync(self):
        """N>2 barrier fallback should produce the same system tags as sync."""
        s1 = _make_source("id", "a", {"id": ["m", "n"], "a": [1, 2]})
        s2 = _make_source("id", "b", {"id": ["m", "n"], "b": [10, 20]})
        s3 = _make_source("id", "c", {"id": ["m", "n"], "c": [100, 200]})
        op = Join()

        # Sync
        sync_result = op.static_process(s1, s2, s3)
        sync_rows = list(sync_result.iter_packets())

        # Async (N>2 barrier path)
        op.validate_inputs(s1, s2, s3)
        ch1 = Channel(buffer_size=64)
        ch2 = Channel(buffer_size=64)
        ch3 = Channel(buffer_size=64)
        out = Channel(buffer_size=64)
        await feed(s1, ch1)
        await feed(s2, ch2)
        await feed(s3, ch3)
        hashes = [s1.pipeline_hash(), s2.pipeline_hash(), s3.pipeline_hash()]
        await op.async_execute(
            [ch1.reader, ch2.reader, ch3.reader],
            out.writer,
            input_pipeline_hashes=hashes,
        )
        async_rows = await out.reader.collect()

        assert len(sync_rows) == len(async_rows)

        sync_sys_keys = _extract_system_tag_keys(sync_rows)
        async_sys_keys = _extract_system_tag_keys(async_rows)
        assert sync_sys_keys == async_sys_keys

        sync_sys = _extract_system_tags(sync_rows)
        async_sys = _extract_system_tags(async_rows)
        assert sync_sys == async_sys


class TestSemiJoinSystemTagEquivalence:
    """Verify SemiJoin system-tag handling matches between sync and async."""

    @pytest.mark.asyncio
    async def test_system_tags_preserved_through_semijoin(self):
        """SemiJoin should preserve left-side system tags in both paths."""
        left = _make_source("id", "val", {"id": ["a", "b", "c"], "val": [1, 2, 3]})
        right = _make_source(
            "id", "score", {"id": ["b", "c", "d"], "score": [20, 30, 40]}
        )
        op = SemiJoin()

        # Sync
        sync_result = op.static_process(left, right)
        sync_rows = list(sync_result.iter_packets())

        # Async
        async_rows = await run_binary_validated(op, left, right)

        assert len(sync_rows) == len(async_rows) == 2

        sync_sys_keys = _extract_system_tag_keys(sync_rows)
        async_sys_keys = _extract_system_tag_keys(async_rows)
        assert sync_sys_keys == async_sys_keys

        sync_sys = _extract_system_tags(sync_rows)
        async_sys = _extract_system_tags(async_rows)
        assert sync_sys == async_sys
