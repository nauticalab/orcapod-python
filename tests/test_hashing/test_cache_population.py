"""Tests for populate_hash_cache() and CachePopulationStats."""

import dataclasses
import pytest


class TestCachePopulationStats:
    def test_instantiation(self):
        from orcapod.hashing.cache_population import CachePopulationStats

        stats = CachePopulationStats(
            hashed=1,
            already_cached=2,
            skipped_small=3,
            errors=0,
            total_bytes_hashed=100,
            total_duration=1.0,
            avg_hashing_speed=100.0,
        )
        assert stats.hashed == 1
        assert stats.already_cached == 2
        assert stats.skipped_small == 3
        assert stats.errors == 0
        assert stats.total_bytes_hashed == 100
        assert stats.total_duration == 1.0
        assert stats.avg_hashing_speed == 100.0

    def test_is_frozen(self):
        from orcapod.hashing.cache_population import CachePopulationStats

        stats = CachePopulationStats(
            hashed=0,
            already_cached=0,
            skipped_small=0,
            errors=0,
            total_bytes_hashed=0,
            total_duration=0.0,
            avg_hashing_speed=0.0,
        )
        with pytest.raises((dataclasses.FrozenInstanceError, AttributeError)):
            stats.hashed = 1  # type: ignore[misc]
