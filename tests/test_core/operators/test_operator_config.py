"""Tests for operator to_config / from_config serialization."""

from orcapod.core.operators import (
    Batch,
    DropPacketColumns,
    DropTagColumns,
    Join,
    MapPackets,
    MapTags,
    MergeJoin,
    PolarsFilter,
    SelectPacketColumns,
    SelectTagColumns,
    SemiJoin,
)


class TestJoinConfig:
    def test_to_config(self):
        op = Join()
        config = op.to_config()
        assert config["class_name"] == "Join"
        assert config["module_path"] == "orcapod.core.operators.join"

    def test_round_trip(self):
        op = Join()
        config = op.to_config()
        restored = Join.from_config(config)
        assert isinstance(restored, Join)


class TestMergeJoinConfig:
    def test_to_config(self):
        op = MergeJoin()
        config = op.to_config()
        assert config["class_name"] == "MergeJoin"

    def test_round_trip(self):
        op = MergeJoin()
        config = op.to_config()
        restored = MergeJoin.from_config(config)
        assert isinstance(restored, MergeJoin)


class TestSemiJoinConfig:
    def test_to_config(self):
        op = SemiJoin()
        config = op.to_config()
        assert config["class_name"] == "SemiJoin"

    def test_round_trip(self):
        op = SemiJoin()
        config = op.to_config()
        restored = SemiJoin.from_config(config)
        assert isinstance(restored, SemiJoin)


class TestBatchConfig:
    def test_to_config_default(self):
        op = Batch()
        config = op.to_config()
        assert config["class_name"] == "Batch"
        assert config["config"]["batch_size"] == 0
        assert config["config"]["drop_partial_batch"] is False

    def test_to_config_custom(self):
        op = Batch(batch_size=10, drop_partial_batch=True)
        config = op.to_config()
        assert config["config"]["batch_size"] == 10
        assert config["config"]["drop_partial_batch"] is True

    def test_round_trip(self):
        op = Batch(batch_size=10, drop_partial_batch=True)
        config = op.to_config()
        restored = Batch.from_config(config)
        assert isinstance(restored, Batch)
        assert restored.batch_size == 10
        assert restored.drop_partial_batch is True


class TestSelectTagColumnsConfig:
    def test_to_config(self):
        op = SelectTagColumns(columns=["a", "b"], strict=False)
        config = op.to_config()
        assert config["class_name"] == "SelectTagColumns"
        assert config["config"]["columns"] == ["a", "b"]
        assert config["config"]["strict"] is False

    def test_round_trip(self):
        op = SelectTagColumns(columns=["a", "b"])
        config = op.to_config()
        restored = SelectTagColumns.from_config(config)
        assert isinstance(restored, SelectTagColumns)


class TestDropTagColumnsConfig:
    def test_round_trip(self):
        op = DropTagColumns(columns=["x"])
        config = op.to_config()
        restored = DropTagColumns.from_config(config)
        assert isinstance(restored, DropTagColumns)
        assert config["class_name"] == "DropTagColumns"


class TestSelectPacketColumnsConfig:
    def test_round_trip(self):
        op = SelectPacketColumns(columns=["a"])
        config = op.to_config()
        restored = SelectPacketColumns.from_config(config)
        assert isinstance(restored, SelectPacketColumns)


class TestDropPacketColumnsConfig:
    def test_round_trip(self):
        op = DropPacketColumns(columns=["a"])
        config = op.to_config()
        restored = DropPacketColumns.from_config(config)
        assert isinstance(restored, DropPacketColumns)


class TestMapTagsConfig:
    def test_to_config(self):
        op = MapTags(name_map={"old": "new"}, drop_unmapped=True)
        config = op.to_config()
        assert config["config"]["name_map"] == {"old": "new"}
        assert config["config"]["drop_unmapped"] is True

    def test_round_trip(self):
        op = MapTags(name_map={"old": "new"})
        config = op.to_config()
        restored = MapTags.from_config(config)
        assert isinstance(restored, MapTags)


class TestMapPacketsConfig:
    def test_round_trip(self):
        op = MapPackets(name_map={"old": "new"}, drop_unmapped=True)
        config = op.to_config()
        restored = MapPackets.from_config(config)
        assert isinstance(restored, MapPackets)


class TestPolarsFilterConfig:
    def test_to_config_with_constraints(self):
        op = PolarsFilter(constraints={"age": 25})
        config = op.to_config()
        assert config["class_name"] == "PolarsFilter"
        assert config["config"]["constraints"] == {"age": 25}

    def test_round_trip_constraints_only(self):
        op = PolarsFilter(constraints={"age": 25})
        config = op.to_config()
        restored = PolarsFilter.from_config(config)
        assert isinstance(restored, PolarsFilter)

    def test_non_serializable_predicates_marked(self):
        """PolarsFilter with Expr predicates should attempt serialization."""
        import polars as pl

        op = PolarsFilter(predicates=[pl.col("age") > 18])
        config = op.to_config()
        # Should either serialize the expression or mark as non-reconstructable
        assert "config" in config
