"""File hash cache population utility.

Provides ``CachePopulationStats`` and ``populate_hash_cache()`` for pre-populating
the SQLite file-hash cache with hashes of large files before a pipeline run.
"""

from __future__ import annotations

import logging
import os
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from upath import UPath

from orcapod.hashing.file_hashers import FileHashKey, FileHasher
from orcapod.hashing.hash_cachers import SqliteHashCacher
from orcapod.types import PathLike

logger = logging.getLogger(__name__)

_DEFAULT_MIN_SIZE_BYTES: int = 500 * 1024 * 1024  # 500 MB
_DEFAULT_MAX_WORKERS: int = 4
# Maximum futures kept in-flight relative to the worker count.  Keeping the
# pending set bounded prevents O(num_files) memory growth when traversal is
# faster than hashing.
_PENDING_FACTOR: int = 4


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
        total_bytes_cached: Sum of bytes across all already-cached files.
        total_duration: Wall-clock duration of the full run in seconds.
        avg_hashing_speed: Hashing throughput in bytes per second
            (``total_bytes_hashed / total_duration``; ``0.0`` if ``total_duration`` is zero).
    """

    hashed: int
    already_cached: int
    skipped_small: int
    errors: int
    total_bytes_hashed: int
    total_bytes_cached: int
    total_duration: float
    avg_hashing_speed: float


def _hash_one(
    resolved: UPath,
    file_stat: os.stat_result,
    cacher: SqliteHashCacher,
    hasher: FileHasher,
) -> tuple[Literal["hashed", "cached", "error"], int]:
    """Hash and cache a single qualifying file, returning a (category, bytes) result pair.

    Called from worker threads inside ``populate_hash_cache()``. The file has
    already passed the size filter on the traversal thread. ``cacher`` and
    ``hasher`` are shared across threads; both are safe because
    ``SqliteHashCacher`` uses ``threading.local()`` connections and
    ``FileHasher`` is stateless.

    Args:
        resolved: Fully resolved file path (no symlinks).
        file_stat: ``os.stat_result`` obtained on the traversal thread; used
            to build the cache key (mtime_ns + size) without a second stat call.
        cacher: Shared ``SqliteHashCacher`` instance.
        hasher: Shared ``FileHasher`` instance.

    Returns:
        A tuple ``(category, nbytes)`` where ``category`` is one of
        ``"hashed"``, ``"cached"``, or ``"error"``. ``nbytes`` is the
        file size for newly hashed and already-cached files; ``0`` for
        errors.
    """
    try:
        key = FileHashKey(resolved, file_stat.st_mtime_ns, file_stat.st_size)
        if cacher.get(key) is not None:
            return ("cached", file_stat.st_size)
        content_hash = hasher.hash_file(resolved)
        cacher.put(key, content_hash)
        return ("hashed", file_stat.st_size)
    except Exception:
        logger.warning("Failed to process file %s", resolved, exc_info=True)
        return ("error", 0)


def populate_hash_cache(
    path: PathLike | UPath,
    *,
    min_size_bytes: int = _DEFAULT_MIN_SIZE_BYTES,
    db_path: UPath | PathLike | None = None,
    algorithm: str = "sha256",
    buffer_size: int = 65536,
    max_workers: int = _DEFAULT_MAX_WORKERS,
) -> CachePopulationStats:
    """Recursively hash and cache all files >= ``min_size_bytes`` under ``path``.

    Pre-populates the SQLite file-hash cache so that subsequent pipeline runs
    can skip expensive content hashing with a sub-millisecond DB lookup.

    All files under ``path`` are visited recursively, including hidden files.
    Symlinks are skipped. Per-file and per-directory errors are logged as warnings
    and counted in ``CachePopulationStats.errors``.

    Directory traversal, ``stat()`` calls, and size filtering all run on the
    calling thread.  Only files that meet the size threshold are dispatched to
    a ``ThreadPoolExecutor`` with ``max_workers`` threads for cache-lookup and
    hashing.  The number of in-flight futures is capped at
    ``max_workers * 4`` so that peak memory stays O(max_workers) regardless
    of how many files are in the tree.  Pass ``max_workers=1`` to restore
    single-threaded behaviour.

    Args:
        path: Root directory to scan recursively. Accepts ``str``, ``os.PathLike``,
            or ``UPath``.
        min_size_bytes: Files strictly smaller than this threshold (in bytes) are
            skipped and not cached. Defaults to 500 MB (``500 * 1024 * 1024``).
        db_path: Path to the SQLite hash cache database. Only local paths are
            supported — SQLite cannot operate on remote filesystems. Defaults to
            the ``ORCAPOD_HASH_CACHE_DB`` environment variable, or
            ``~/.orcapod/file_hash_cache.db`` if unset.
        algorithm: Hash algorithm passed to ``FileHasher``. Defaults to ``"sha256"``.
        buffer_size: Read buffer size in bytes passed to ``FileHasher``.
            Defaults to 65536.
        max_workers: Number of threads for concurrent per-file hashing. Defaults
            to 4, which is well-suited for NAS and HDD storage where more than 4
            parallel reads cause seek contention. Pass ``1`` for serial behaviour.
            Must be >= 1; raises ``ValueError`` otherwise.

    Returns:
        ``CachePopulationStats`` with counts for hashed, cached, skipped, and
        errored files plus throughput metrics.

    Raises:
        ValueError: If ``max_workers`` is less than 1.
    """
    if max_workers < 1:
        raise ValueError(f"max_workers must be >= 1, got {max_workers!r}")

    root = UPath(path)
    _db_path: Path | None = Path(db_path) if db_path is not None else None
    hasher = FileHasher(algorithm=algorithm, buffer_size=buffer_size)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        with SqliteHashCacher(_db_path) as cacher:
            # Collect the DB file and its SQLite sidecar files so they are never
            # treated as data files even if the DB lives inside the scan root.
            # Sidecars are named by appending to the full filename (including any
            # extension), so string concatenation is used rather than with_suffix()
            # — which would raise ValueError for DB paths without an extension.
            # Covered sidecars: WAL ("-wal"), shared-memory ("-shm"), and rollback
            # journal ("-journal").
            _db_resolved = cacher.db_path.resolve()
            _db_str = str(_db_resolved)
            _excluded: frozenset[UPath] = frozenset(
                [
                    UPath(_db_resolved),
                    UPath(_db_str + "-wal"),
                    UPath(_db_str + "-shm"),
                    UPath(_db_str + "-journal"),
                ]
            )

            error_count = 0
            hashed = 0
            already_cached = 0
            skipped_small = 0
            total_bytes_hashed = 0
            already_cached_bytes = 0
            start = time.monotonic()

            # Cap the number of futures kept in-flight so that peak memory
            # stays O(max_workers) regardless of tree size.
            _max_pending = max_workers * _PENDING_FACTOR
            pending: set[Future[tuple[Literal["hashed", "cached", "error"], int]]] = set()

            # Explicit DFS stack — avoids recursion limits on deep trees.
            # DB-file exclusion, stat(), and size-filtering all happen here
            # (main thread) so workers only receive qualifying file paths.
            stack: list[UPath] = [root]
            while stack:
                current = stack.pop()
                try:
                    entries = list(current.iterdir())
                except OSError:
                    logger.warning(
                        "Cannot access directory %s", current, exc_info=True
                    )
                    error_count += 1
                    continue

                for entry in entries:
                    if entry.is_symlink():
                        continue
                    if entry.is_dir():
                        stack.append(entry)
                        continue
                    if not entry.is_file():
                        continue

                    # Resolve, stat, and filter by size on the traversal
                    # thread.  Only qualifying files are submitted to the
                    # executor, keeping the pending set small and avoiding
                    # futures for every tiny file in the tree.
                    try:
                        resolved = entry.resolve()
                        if resolved in _excluded:
                            continue
                        file_stat = resolved.stat()
                    except OSError:
                        logger.warning(
                            "Cannot stat file %s", entry, exc_info=True
                        )
                        error_count += 1
                        continue

                    if file_stat.st_size < min_size_bytes:
                        skipped_small += 1
                        continue

                    # Drain completed futures when the pending set is full,
                    # providing backpressure when hashing is slower than
                    # traversal.
                    if len(pending) >= _max_pending:
                        done, pending = wait(pending, return_when=FIRST_COMPLETED)
                        for fut in done:
                            cat, nbytes = fut.result()
                            if cat == "hashed":
                                hashed += 1
                                total_bytes_hashed += nbytes
                            elif cat == "cached":
                                already_cached += 1
                                already_cached_bytes += nbytes
                            else:  # "error"
                                error_count += 1

                    pending.add(
                        executor.submit(_hash_one, resolved, file_stat, cacher, hasher)
                    )

            # Drain all remaining futures.
            for future in as_completed(pending):
                category, nbytes = future.result()
                if category == "hashed":
                    hashed += 1
                    total_bytes_hashed += nbytes
                elif category == "cached":
                    already_cached += 1
                    already_cached_bytes += nbytes
                else:  # "error"
                    error_count += 1

            duration = time.monotonic() - start
            speed = total_bytes_hashed / duration if duration > 0 else 0.0

            return CachePopulationStats(
                hashed=hashed,
                already_cached=already_cached,
                skipped_small=skipped_small,
                errors=error_count,
                total_bytes_hashed=total_bytes_hashed,
                total_bytes_cached=already_cached_bytes,
                total_duration=duration,
                avg_hashing_speed=speed,
            )
