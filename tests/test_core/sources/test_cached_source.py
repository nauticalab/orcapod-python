"""
Tests for CachedSource covering:
- Construction and transparent StreamProtocol implementation
- Cache path scoped to source's content_hash
- Cumulative caching: data from prior runs is preserved
- Dedup by per-row content hash
- Transparent streaming: downstream consumers see same schema as live source
- iter_packets and as_table produce consistent results
- System tags are preserved through caching
- Source info columns are preserved through caching
- clear_cache forces rebuild on next access
- Identity delegation to wrapped source
"""

from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.core.sources import ArrowTableSource, CachedSource
from orcapod.core.streams import ArrowTableStream
from orcapod.databases import InMemoryArrowDatabase
from orcapod.protocols.core_protocols import StreamProtocol
from orcapod.protocols.hashing_protocols import PipelineElementProtocol
from orcapod.system_constants import constants


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def simple_table():
    return pa.table(
        {
            "name": pa.array(["Alice", "Bob", "Charlie"], type=pa.large_string()),
            "age": pa.array([30, 25, 35], type=pa.int64()),
        }
    )


@pytest.fixture
def simple_source(simple_table):
    return ArrowTableSource(simple_table, tag_columns=["name"], source_id="src_1")


@pytest.fixture
def db():
    return InMemoryArrowDatabase()


# ---------------------------------------------------------------------------
# Construction and protocol conformance
# ---------------------------------------------------------------------------


class TestCachedSourceConstruction:
    def test_source_id_delegated(self, simple_source, db):
        ps = CachedSource(simple_source, cache_database=db)
        assert ps.source_id == simple_source.source_id

    def test_stream_protocol_conformance(self, simple_source, db):
        ps = CachedSource(simple_source, cache_database=db)
        assert isinstance(ps, StreamProtocol)

    def test_pipeline_element_conformance(self, simple_source, db):
        ps = CachedSource(simple_source, cache_database=db)
        assert isinstance(ps, PipelineElementProtocol)

    def test_identity_delegated(self, simple_source, db):
        ps = CachedSource(simple_source, cache_database=db)
        assert ps.identity_structure() == simple_source.identity_structure()

    def test_content_hash_matches_source(self, simple_source, db):
        ps = CachedSource(simple_source, cache_database=db)
        assert ps.content_hash() == simple_source.content_hash()

    def test_pipeline_hash_matches_source(self, simple_source, db):
        ps = CachedSource(simple_source, cache_database=db)
        assert ps.pipeline_hash() == simple_source.pipeline_hash()


# ---------------------------------------------------------------------------
# Cache path scoping
# ---------------------------------------------------------------------------


class TestCachedSourceCachePath:
    def test_cache_path_contains_content_hash(self, simple_source, db):
        ps = CachedSource(simple_source, cache_database=db)
        path = ps.cache_path
        content_hash_str = simple_source.content_hash().to_string()
        assert any(content_hash_str in segment for segment in path)

    def test_cache_path_prefix(self, simple_source, db):
        prefix = ("my_project", "v1")
        ps = CachedSource(simple_source, cache_database=db, cache_path_prefix=prefix)
        assert ps.cache_path[:2] == prefix

    def test_same_source_same_cache_path(self, simple_table, db):
        """Identical sources produce the same cache path."""
        s1 = ArrowTableSource(simple_table, tag_columns=["name"], source_id="src")
        s2 = ArrowTableSource(simple_table, tag_columns=["name"], source_id="src")
        ps1 = CachedSource(s1, cache_database=db)
        ps2 = CachedSource(s2, cache_database=db)
        assert ps1.cache_path == ps2.cache_path

    def test_same_name_same_schema_same_cache_path(self, db):
        """Same source_id + same schema = same identity (regardless of data)."""
        t1 = pa.table({"k": ["a"], "v": [1]})
        t2 = pa.table({"k": ["b"], "v": [2]})
        s1 = ArrowTableSource(t1, tag_columns=["k"], source_id="s")
        s2 = ArrowTableSource(t2, tag_columns=["k"], source_id="s")
        ps1 = CachedSource(s1, cache_database=db)
        ps2 = CachedSource(s2, cache_database=db)
        assert ps1.cache_path == ps2.cache_path

    def test_different_name_different_cache_path(self, db):
        """Different source_id produces different cache paths."""
        t1 = pa.table({"k": ["a"], "v": [1]})
        s1 = ArrowTableSource(t1, tag_columns=["k"], source_id="src_a")
        s2 = ArrowTableSource(t1, tag_columns=["k"], source_id="src_b")
        ps1 = CachedSource(s1, cache_database=db)
        ps2 = CachedSource(s2, cache_database=db)
        assert ps1.cache_path != ps2.cache_path

    def test_unnamed_different_data_different_cache_path(self, db):
        """Unnamed sources with different data get different cache paths."""
        t1 = pa.table({"k": ["a"], "v": [1]})
        t2 = pa.table({"k": ["b"], "v": [2]})
        s1 = ArrowTableSource(t1, tag_columns=["k"])
        s2 = ArrowTableSource(t2, tag_columns=["k"])
        ps1 = CachedSource(s1, cache_database=db)
        ps2 = CachedSource(s2, cache_database=db)
        assert ps1.cache_path != ps2.cache_path


# ---------------------------------------------------------------------------
# Schema and keys delegation
# ---------------------------------------------------------------------------


class TestCachedSourceSchema:
    def test_output_schema_matches_source(self, simple_source, db):
        ps = CachedSource(simple_source, cache_database=db)
        assert ps.output_schema() == simple_source.output_schema()

    def test_output_schema_with_system_tags(self, simple_source, db):
        ps = CachedSource(simple_source, cache_database=db)
        assert ps.output_schema(
            columns={"system_tags": True}
        ) == simple_source.output_schema(columns={"system_tags": True})

    def test_keys_match_source(self, simple_source, db):
        ps = CachedSource(simple_source, cache_database=db)
        assert ps.keys() == simple_source.keys()


# ---------------------------------------------------------------------------
# Streaming: iter_packets and as_table
# ---------------------------------------------------------------------------


class TestCachedSourceStreaming:
    def test_as_table_matches_source(self, simple_source, db):
        ps = CachedSource(simple_source, cache_database=db)
        ps_table = ps.as_table()
        src_table = simple_source.as_table()
        assert ps_table.num_rows == src_table.num_rows
        assert set(ps_table.column_names) == set(src_table.column_names)

    def test_iter_packets_count(self, simple_source, db):
        ps = CachedSource(simple_source, cache_database=db)
        packets = list(ps.iter_packets())
        assert len(packets) == 3

    def test_iter_packets_tags_and_packets(self, simple_source, db):
        ps = CachedSource(simple_source, cache_database=db)
        for tag, packet in ps.iter_packets():
            assert "name" in tag.keys()
            assert "age" in packet.keys()

    def test_system_tags_preserved(self, simple_source, db):
        """System tags flow through the cache correctly."""
        ps = CachedSource(simple_source, cache_database=db)
        table = ps.as_table(columns={"system_tags": True})
        sys_tag_cols = [
            c for c in table.column_names if c.startswith(constants.SYSTEM_TAG_PREFIX)
        ]
        # Should have paired source_id and record_id columns
        source_id_cols = [
            c
            for c in sys_tag_cols
            if c.startswith(constants.SYSTEM_TAG_SOURCE_ID_PREFIX)
        ]
        record_id_cols = [
            c
            for c in sys_tag_cols
            if c.startswith(constants.SYSTEM_TAG_RECORD_ID_PREFIX)
        ]
        assert len(source_id_cols) == 1
        assert len(record_id_cols) == 1

    def test_source_info_preserved(self, simple_source, db):
        """Source info columns flow through the cache correctly."""
        ps = CachedSource(simple_source, cache_database=db)
        table = ps.as_table(columns={"source": True})
        source_cols = [
            c for c in table.column_names if c.startswith(constants.SOURCE_PREFIX)
        ]
        assert len(source_cols) > 0


# ---------------------------------------------------------------------------
# Cumulative caching
# ---------------------------------------------------------------------------


class TestCachedSourceCumulative:
    def test_dedup_on_same_data(self, simple_source, db):
        """Running twice with the same data produces no duplicates."""
        ps1 = CachedSource(simple_source, cache_database=db)
        ps1.flow()
        ps2 = CachedSource(simple_source, cache_database=db)
        ps2.flow()
        table = ps2.as_table()
        assert table.num_rows == 3  # no duplicates

    def test_clear_cache_rebuilds(self, simple_source, db):
        """clear_cache forces a fresh merge from DB on next access."""
        ps = CachedSource(simple_source, cache_database=db)
        t1 = ps.as_table()
        ps.clear_cache()
        t2 = ps.as_table()
        assert t1.num_rows == t2.num_rows

    def test_source_modified_time_triggers_rebuild(self, simple_source, db):
        """Updating the wrapped source's modified time triggers a cache rebuild."""
        ps = CachedSource(simple_source, cache_database=db)
        # First access: build and cache
        t1 = ps.as_table()
        assert t1.num_rows == 3

        # Simulate the source being updated (e.g. new data loaded)
        simple_source._update_modified_time()

        # Next access should detect staleness and rebuild
        t2 = ps.as_table()
        assert t2.num_rows == 3
        # Verify CachedSource's own modified time was updated past the source's
        assert not ps.is_stale

    def test_cumulative_across_runs(self, db):
        """Data from different runs accumulates in the cache."""
        # Use a single source_id to make them share the same canonical identity
        # but with different data (different content_hash → different cache_path)
        t1 = pa.table({"k": ["a", "b"], "v": [1, 2]})
        t2 = pa.table({"k": ["a", "b", "c"], "v": [1, 2, 3]})
        s1 = ArrowTableSource(t1, tag_columns=["k"], source_id="shared")
        s2 = ArrowTableSource(t2, tag_columns=["k"], source_id="shared")

        # Different data → different content_hash → different cache_paths
        # So cumulative within the SAME cache_path requires same content_hash
        ps1 = CachedSource(s1, cache_database=db)
        ps1.flow()
        assert ps1.as_table().num_rows == 2

        # Same data source: should dedup
        s1_again = ArrowTableSource(t1, tag_columns=["k"], source_id="shared")
        ps1_again = CachedSource(s1_again, cache_database=db)
        ps1_again.flow()
        assert ps1_again.as_table().num_rows == 2

        # Different source (s2) has different cache_path
        ps2 = CachedSource(s2, cache_database=db)
        ps2.flow()
        assert ps2.as_table().num_rows == 3


# ---------------------------------------------------------------------------
# Field resolution delegation
# ---------------------------------------------------------------------------


class TestCachedSourceFieldResolution:
    def test_resolve_field_delegates_raises_not_implemented(self, simple_source, db):
        """CachedSource delegates to wrapped source which raises NotImplementedError."""
        ps = CachedSource(simple_source, cache_database=db)
        with pytest.raises(NotImplementedError):
            ps.resolve_field("row_0", "age")

    def test_resolve_field_with_record_id_column_raises(self, db):
        table = pa.table(
            {
                "user_id": pa.array(["u1", "u2"], type=pa.large_string()),
                "score": pa.array([100, 200], type=pa.int64()),
            }
        )
        source = ArrowTableSource(
            table, tag_columns=["user_id"], record_id_column="user_id", source_id="test"
        )
        ps = CachedSource(source, cache_database=db)
        with pytest.raises(NotImplementedError):
            ps.resolve_field("user_id=u1", "score")


# ---------------------------------------------------------------------------
# Integration with downstream operators
# ---------------------------------------------------------------------------


class TestCachedSourceIntegration:
    def test_join_with_cached_source(self, db):
        """CachedSource can be joined with another stream."""
        from orcapod.core.operators import Join

        t1 = pa.table({"id": [1, 2, 3], "val_a": [10, 20, 30]})
        t2 = pa.table({"id": [2, 3, 4], "val_b": [200, 300, 400]})
        s1 = ArrowTableSource(t1, tag_columns=["id"], source_id="a")
        s2 = ArrowTableSource(t2, tag_columns=["id"], source_id="b")

        ps1 = CachedSource(s1, cache_database=db)
        ps2 = CachedSource(s2, cache_database=db)

        joined = Join()(ps1, ps2)
        table = joined.as_table()
        assert table.num_rows == 2  # id=2,3 overlap
        assert "val_a" in table.column_names
        assert "val_b" in table.column_names

    def test_function_pod_with_cached_source(self, db):
        """CachedSource works as input to a FunctionPod."""
        from orcapod.core.function_pod import FunctionPod
        from orcapod.core.packet_function import PythonPacketFunction

        def double_age(age: int) -> int:
            return age * 2

        pf = PythonPacketFunction(double_age, output_keys="doubled_age")
        pod = FunctionPod(packet_function=pf)

        table = pa.table({"name": ["Alice", "Bob"], "age": [30, 25]})
        source = ArrowTableSource(table, tag_columns=["name"], source_id="test")
        ps = CachedSource(source, cache_database=db)

        result = pod(ps)
        packets = list(result.iter_packets())
        assert len(packets) == 2
        ages = [p.as_dict()["doubled_age"] for _, p in packets]
        assert sorted(ages) == [50, 60]


class TestCachedConvenienceMethod:
    """Test the ``RootSource.cached()`` convenience method."""

    def test_cached_returns_cached_source(self, simple_source, db):
        cached = simple_source.cached(cache_database=db)
        assert isinstance(cached, CachedSource)

    def test_cached_with_path_prefix(self, simple_source, db):
        cached = simple_source.cached(
            cache_database=db,
            cache_path_prefix=("my", "prefix"),
        )
        assert cached.cache_path[:2] == ("my", "prefix")

    def test_cached_data_matches_source(self, simple_source, db):
        cached = simple_source.cached(cache_database=db)
        original_table = simple_source.as_table()
        cached_table = cached.as_table()

        # Same column names and row count
        assert set(original_table.column_names) == set(cached_table.column_names)
        assert original_table.num_rows == cached_table.num_rows
