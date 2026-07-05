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
            total_bytes_cached=50,
            total_duration=1.0,
            avg_hashing_speed=100.0,
        )
        assert stats.hashed == 1
        assert stats.already_cached == 2
        assert stats.skipped_small == 3
        assert stats.errors == 0
        assert stats.total_bytes_hashed == 100
        assert stats.total_bytes_cached == 50
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
            total_bytes_cached=0,
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

    def test_skips_symlinked_directories(self, tmp_path):
        """A symlink pointing to a subdirectory is not followed; files inside are not hashed."""
        db = tmp_path / "cache.db"
        # Real subdirectory with a qualifying file inside.
        real_sub = tmp_path / "real_sub"
        real_sub.mkdir()
        _write(real_sub, "secret.bin", 20)

        # Symlink in the scan root pointing at real_sub.
        link = tmp_path / "link_to_sub"
        link.symlink_to(real_sub)

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        # real_sub/secret.bin is hashed (reached via real_sub, not via the symlink).
        # The symlink itself is skipped; no double-count.
        assert stats.hashed == 1
        assert stats.errors == 0

    def test_db_path_without_extension(self, tmp_path):
        """DB paths with no file extension do not raise ValueError during sidecar exclusion."""
        db = tmp_path / "cache"  # no extension — with_suffix() would raise here
        _write(tmp_path, "f.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)
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
        assert first.total_bytes_cached == 0
        assert second.already_cached == 1
        assert second.hashed == 0
        assert second.total_bytes_cached == 20

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


# ---------------------------------------------------------------------------
# Concurrency
# ---------------------------------------------------------------------------


class TestConcurrency:
    def test_concurrent_hashes_multiple_files(self, tmp_path):
        """ThreadPoolExecutor path hashes all qualifying files without losing any."""
        db = tmp_path / "cache.db"
        _write(tmp_path, "a.bin", 20)
        _write(tmp_path, "b.bin", 20)
        _write(tmp_path, "c.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(
            tmp_path, min_size_bytes=_MIN, db_path=db, max_workers=4
        )

        assert stats.hashed == 3
        assert stats.errors == 0

    def test_max_workers_1_matches_serial(self, tmp_path):
        """max_workers=1 and max_workers=4 produce identical CachePopulationStats."""
        # Keep data files and DB files in separate directories so that the DB
        # created by the serial run is never picked up as a data file by the
        # concurrent run (which has a different _excluded set).
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        db_dir = tmp_path / "dbs"
        db_dir.mkdir()
        db1 = db_dir / "cache1.db"
        db2 = db_dir / "cache2.db"
        _write(data_dir, "a.bin", 20)
        _write(data_dir, "b.bin", 30)

        from orcapod.hashing.cache_population import populate_hash_cache

        serial = populate_hash_cache(
            data_dir, min_size_bytes=_MIN, db_path=db1, max_workers=1
        )
        concurrent = populate_hash_cache(
            data_dir, min_size_bytes=_MIN, db_path=db2, max_workers=4
        )

        # Preconditions: verify both runs actually hashed the expected files.
        assert serial.hashed == 2
        assert serial.total_bytes_hashed == 50

        assert serial.hashed == concurrent.hashed
        assert serial.already_cached == concurrent.already_cached
        assert serial.skipped_small == concurrent.skipped_small
        assert serial.errors == concurrent.errors
        assert serial.total_bytes_hashed == concurrent.total_bytes_hashed

    def test_workers_default_is_4(self):
        """The default value of max_workers is 4."""
        import inspect

        from orcapod.hashing.cache_population import populate_hash_cache

        sig = inspect.signature(populate_hash_cache)
        assert sig.parameters["max_workers"].default == 4

    def test_max_workers_zero_raises(self, tmp_path):
        """max_workers=0 raises ValueError before any hashing starts."""
        from orcapod.hashing.cache_population import populate_hash_cache

        with pytest.raises(ValueError, match="max_workers"):
            populate_hash_cache(tmp_path, max_workers=0)


# ---------------------------------------------------------------------------
# Public exports
# ---------------------------------------------------------------------------


class TestPublicExports:
    def test_importable_from_orcapod_hashing(self):
        from orcapod.hashing import CachePopulationStats, populate_hash_cache

        assert callable(populate_hash_cache)
        assert CachePopulationStats.__dataclass_fields__  # is a dataclass


# ---------------------------------------------------------------------------
# Cached bytes
# ---------------------------------------------------------------------------


class TestCachedBytes:
    def test_total_bytes_cached_zero_on_first_run(self, tmp_path):
        """First run has nothing cached yet — total_bytes_cached must be zero."""
        db = tmp_path / "cache.db"
        _write(tmp_path, "f.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        stats = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)
        assert stats.total_bytes_cached == 0

    def test_total_bytes_cached_on_second_run(self, tmp_path):
        """Second run finds the file cached — total_bytes_cached equals file size."""
        db = tmp_path / "cache.db"
        _write(tmp_path, "f.bin", 20)

        from orcapod.hashing.cache_population import populate_hash_cache

        populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)
        second = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        assert second.total_bytes_cached == 20
        assert second.total_bytes_hashed == 0

    def test_total_bytes_cached_multiple_files(self, tmp_path):
        """total_bytes_cached sums across all already-cached files."""
        db = tmp_path / "cache.db"
        _write(tmp_path, "a.bin", 20)
        _write(tmp_path, "b.bin", 30)

        from orcapod.hashing.cache_population import populate_hash_cache

        populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)
        second = populate_hash_cache(tmp_path, min_size_bytes=_MIN, db_path=db)

        assert second.total_bytes_cached == 50
        assert second.total_bytes_hashed == 0
