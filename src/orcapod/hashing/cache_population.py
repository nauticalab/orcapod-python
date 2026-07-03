"""File hash cache population utility.

Provides ``CachePopulationStats`` and ``populate_hash_cache()`` for pre-populating
the SQLite file-hash cache with hashes of large files before a pipeline run.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from pathlib import Path

from upath import UPath

from orcapod.hashing.file_hashers import FileHashKey, FileHasher
from orcapod.hashing.hash_cachers import SqliteHashCacher
from orcapod.types import PathLike

logger = logging.getLogger(__name__)

_DEFAULT_MIN_SIZE_BYTES: int = 500 * 1024 * 1024  # 500 MB


@dataclass(frozen=True)
class CachePopulationStats:
    """Statistics returned by ``populate_hash_cache()``.

    Attributes:
        hashed: Files newly hashed and written to the cache.
        already_cached: Files whose hash was already in the cache (rehash skipped).
        skipped_small: Files skipped because their size was below ``min_size_bytes``.
        errors: Files or directories that raised an exception during traversal or hashing
            (logged as warnings).
        total_bytes_hashed: Sum of bytes across all newly hashed files.
        total_duration: Wall-clock duration of the full run in seconds.
        avg_hashing_speed: Hashing throughput in bytes per second
            (``total_bytes_hashed / total_duration``; ``0.0`` if ``total_duration`` is zero).
    """

    hashed: int
    already_cached: int
    skipped_small: int
    errors: int
    total_bytes_hashed: int
    total_duration: float
    avg_hashing_speed: float


def populate_hash_cache(
    path: PathLike,
    *,
    min_size_bytes: int = _DEFAULT_MIN_SIZE_BYTES,
    db_path: UPath | PathLike | None = None,
    algorithm: str = "sha256",
    buffer_size: int = 65536,
) -> CachePopulationStats:
    """Recursively hash and cache all files >= ``min_size_bytes`` under ``path``.

    Pre-populates the SQLite file-hash cache so that subsequent pipeline runs
    can skip expensive content hashing with a sub-millisecond DB lookup.

    All files under ``path`` are visited recursively, including hidden files.
    Symlinks are skipped. Per-file and per-directory errors are logged as warnings
    and counted in ``CachePopulationStats.errors``.

    Args:
        path: Root directory to scan recursively.
        min_size_bytes: Files strictly smaller than this threshold (in bytes) are
            skipped and not cached. Defaults to 500 MB (``500 * 1024 * 1024``).
        db_path: Path to the SQLite hash cache database. Only local paths are
            supported — SQLite cannot operate on remote filesystems. Defaults to
            the ``ORCAPOD_HASH_CACHE_DB`` environment variable, or
            ``~/.orcapod/file_hash_cache.db`` if unset.
        algorithm: Hash algorithm passed to ``FileHasher``. Defaults to ``"sha256"``.
        buffer_size: Read buffer size in bytes passed to ``FileHasher``.
            Defaults to 65536.

    Returns:
        ``CachePopulationStats`` with counts for hashed, cached, skipped, and
        errored files plus throughput metrics.
    """
    root = UPath(path)
    _db_path: Path | None = Path(db_path) if db_path is not None else None
    cacher = SqliteHashCacher(_db_path)
    hasher = FileHasher(algorithm=algorithm, buffer_size=buffer_size)

    # Collect the DB file and its SQLite journal/WAL siblings so they are
    # never treated as data files even if the DB lives inside the scan root.
    _db_resolved = cacher.db_path.resolve()
    _excluded: frozenset[Path] = frozenset(
        [
            _db_resolved,
            _db_resolved.with_suffix(_db_resolved.suffix + "-wal"),
            _db_resolved.with_suffix(_db_resolved.suffix + "-shm"),
        ]
    )

    hashed = 0
    already_cached = 0
    skipped_small = 0
    error_count = 0
    total_bytes_hashed = 0

    start = time.monotonic()

    # Explicit DFS stack — avoids recursion limits on deep trees.
    stack: list[UPath] = [root]
    while stack:
        current = stack.pop()
        try:
            entries = list(current.iterdir())
        except PermissionError:
            logger.warning(
                "Cannot access directory %s: permission denied", current
            )
            error_count += 1
            continue

        for entry in entries:
            # Never follow symlinks — they can cause cycles.
            if entry.is_symlink():
                continue
            if entry.is_dir():
                stack.append(entry)
                continue
            if not entry.is_file():
                continue

            try:
                resolved = entry.resolve()

                # Skip the SQLite cache database itself and its journal files.
                if resolved in _excluded:
                    continue

                stat = resolved.stat()
                size = stat.st_size

                if size < min_size_bytes:
                    skipped_small += 1
                    continue

                key = FileHashKey(resolved, stat.st_mtime_ns, size)
                if cacher.get(key) is not None:
                    already_cached += 1
                    continue

                content_hash = hasher.hash_file(resolved)
                cacher.put(key, content_hash)
                hashed += 1
                total_bytes_hashed += size

            except Exception:
                logger.warning(
                    "Failed to process file %s", entry, exc_info=True
                )
                error_count += 1

    duration = time.monotonic() - start
    speed = total_bytes_hashed / duration if duration > 0 else 0.0

    return CachePopulationStats(
        hashed=hashed,
        already_cached=already_cached,
        skipped_small=skipped_small,
        errors=error_count,
        total_bytes_hashed=total_bytes_hashed,
        total_duration=duration,
        avg_hashing_speed=speed,
    )
