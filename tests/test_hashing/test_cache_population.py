"""Tests for populate_hash_cache() and CachePopulationStats."""

from __future__ import annotations

from pathlib import Path

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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MIN = 10  # small threshold so test files qualify without being 500 MB


def _write(path: Path, name: str, size: int) -> Path:
    """Write a file of exactly ``size`` bytes under ``path``."""
    f = path / name
    f.write_bytes(b"x" * size)
    return f


# ---------------------------------------------------------------------------
# Traversal & filtering
# ---------------------------------------------------------------------------


class TestTraversal:
    def test_skips_files_below_threshold(self, tmp_path):
        db = tmp_path / "cache.db"
        _write(tmp_path, "small.bin", 5)  # < 10

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        assert stats.skipped_small == 1
        assert stats.hashed == 0

    def test_hashes_file_at_exact_threshold(self, tmp_path):
        db = tmp_path / "cache.db"
        _write(tmp_path, "exact.bin", 10)  # == 10

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        assert stats.hashed == 1
        assert stats.skipped_small == 0

    def test_recursive_traversal(self, tmp_path):
        db = tmp_path / "cache.db"
        sub = tmp_path / "a" / "b"
        sub.mkdir(parents=True)
        _write(sub, "deep.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        assert stats.hashed == 1

    def test_includes_hidden_files(self, tmp_path):
        db = tmp_path / "cache.db"
        _write(tmp_path, ".hidden.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        assert stats.hashed == 1

    def test_skips_symlinks(self, tmp_path):
        db = tmp_path / "cache.db"
        real = tmp_path / "real.bin"
        real.write_bytes(b"x" * 20)
        link = tmp_path / "link.bin"
        link.symlink_to(real)

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        # Only the real file, not the symlink
        assert stats.hashed == 1


# ---------------------------------------------------------------------------
# Cache hit / miss
# ---------------------------------------------------------------------------


class TestCacheHitMiss:
    def test_cache_hit_skips_rehash(self, tmp_path):
        db = tmp_path / "cache.db"
        _write(tmp_path, "f.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        first = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)
        second = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        assert first.hashed == 1
        assert second.already_cached == 1
        assert second.hashed == 0

    def test_total_bytes_hashed(self, tmp_path):
        db = tmp_path / "cache.db"
        _write(tmp_path, "a.bin", 20)
        _write(tmp_path, "b.bin", 30)

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        assert stats.hashed == 2
        assert stats.total_bytes_hashed == 50


# ---------------------------------------------------------------------------
# Stats: timing & speed
# ---------------------------------------------------------------------------


class TestStats:
    def test_duration_positive(self, tmp_path):
        db = tmp_path / "cache.db"
        _write(tmp_path, "f.bin", 100)

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        assert stats.total_duration > 0

    def test_speed_matches_bytes_over_duration(self, tmp_path):
        db = tmp_path / "cache.db"
        _write(tmp_path, "f.bin", 100)

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        assert stats.avg_hashing_speed == pytest.approx(
            stats.total_bytes_hashed / stats.total_duration
        )

    def test_speed_zero_when_nothing_hashed(self, tmp_path):
        db = tmp_path / "cache.db"
        # Empty directory — nothing to hash

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        assert stats.avg_hashing_speed == 0.0

    def test_db_path_none_uses_env_var(self, tmp_path, tmp_path_factory, monkeypatch):
        default_db = tmp_path_factory.mktemp("orcapod_db") / "file_hash_cache.db"
        monkeypatch.setenv("ORCAPOD_HASH_CACHE_DB", str(default_db))
        _write(tmp_path, "f.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=None)

        assert stats.hashed == 1
        assert default_db.exists()


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


class TestErrors:
    def test_file_error_increments_errors(self, tmp_path, monkeypatch):
        db = tmp_path / "cache.db"
        _write(tmp_path, "f.bin", 20)

        from orcapod.hashing import file_hashers

        def _raise(self, path):
            raise OSError("simulated hashing error")

        monkeypatch.setattr(file_hashers.FileHasher, "hash_file", _raise)

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        assert stats.errors == 1
        assert stats.hashed == 0

    def test_directory_permission_error_increments_errors(self, tmp_path, monkeypatch):
        import pathlib

        db = tmp_path / "cache.db"
        sub = tmp_path / "locked"
        sub.mkdir()
        _write(sub, "f.bin", 20)

        original_iterdir = pathlib.Path.iterdir

        def _raise_on_locked(self):
            if self.name == "locked":
                raise PermissionError("simulated permission denied")
            return original_iterdir(self)

        monkeypatch.setattr(pathlib.Path, "iterdir", _raise_on_locked)

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        assert stats.errors == 1
        assert stats.hashed == 0
