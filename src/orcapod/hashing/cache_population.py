"""File hash cache population utility.

Provides ``CachePopulationStats`` and ``populate_hash_cache()`` for pre-populating
the SQLite file-hash cache with hashes of large files before a pipeline run.
"""

from __future__ import annotations

import logging
import os
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, as_completed, wait
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Literal

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


FileOutcome = Literal["hashed", "cached", "would_hash", "error"]


@dataclass
class _Stats:
    """Private mutable accumulator for per-run counters.

    Replaces the five loose local variables previously used inside
    ``populate_hash_cache``.
    """

    hashed: int = 0
    already_cached: int = 0
    skipped_small: int = 0
    errors: int = 0
    total_bytes_hashed: int = 0
    total_bytes_cached: int = 0
    start: float = field(default_factory=time.monotonic)

    def snapshot(self) -> CachePopulationStats:
        """Return a frozen ``CachePopulationStats`` snapshot of current totals."""
        duration = time.monotonic() - self.start
        return CachePopulationStats(
            hashed=self.hashed,
            already_cached=self.already_cached,
            skipped_small=self.skipped_small,
            errors=self.errors,
            total_bytes_hashed=self.total_bytes_hashed,
            total_bytes_cached=self.total_bytes_cached,
            total_duration=duration,
            avg_hashing_speed=(
                self.total_bytes_hashed / duration if duration > 0 else 0.0
            ),
        )


class _Accumulator:
    """Owns a ``_Stats`` object and an optional progress callback.

    All counter updates flow through this class so there is one place
    where snapshots are created and the callback is fired.
    """

    def __init__(self, callback: "ProgressCallback | None" = None) -> None:
        self._stats = _Stats()
        self._callback = callback

    def record(self, path: "Path", outcome: FileOutcome, nbytes: int) -> None:
        """Update counters for a qualifying file and fire the callback if set.

        Args:
            path: Resolved file path that was just processed.
            outcome: What happened to the file.
            nbytes: File size in bytes (for hashed/would_hash/cached outcomes);
                ``0`` for errors.
        """
        if outcome in ("hashed", "would_hash"):
            self._stats.hashed += 1
            self._stats.total_bytes_hashed += nbytes
        elif outcome == "cached":
            self._stats.already_cached += 1
            self._stats.total_bytes_cached += nbytes
        else:  # "error"
            self._stats.errors += 1
        if self._callback is not None:
            self._callback(path, outcome, self._stats.snapshot())

    def record_skipped_small(self) -> None:
        """Increment ``skipped_small`` without firing the callback."""
        self._stats.skipped_small += 1

    def record_directory_error(self) -> None:
        """Increment ``errors`` for a directory access failure without firing the callback."""
        self._stats.errors += 1

    def finalize(self) -> CachePopulationStats:
        """Return the final frozen ``CachePopulationStats`` snapshot."""
        return self._stats.snapshot()


class _HashVisitor:
    """Per-file visitor that checks the cache and hashes on miss.

    Callable with signature ``(resolved, file_stat) -> (FileOutcome, int)``.
    Thread-safe: ``SqliteHashCacher`` uses ``threading.local()`` connections
    and ``FileHasher`` is stateless.

    Args:
        cacher: Shared ``SqliteHashCacher`` instance.
        hasher: Shared ``FileHasher`` instance.
        force: If ``True``, skip the cache-get and always re-hash.
    """

    def __init__(
        self,
        cacher: SqliteHashCacher,
        hasher: FileHasher,
        *,
        force: bool = False,
    ) -> None:
        self._cacher = cacher
        self._hasher = hasher
        self._force = force

    def __call__(
        self, resolved: UPath, file_stat: os.stat_result
    ) -> tuple[FileOutcome, int]:
        try:
            key = FileHashKey(resolved, file_stat.st_mtime_ns, file_stat.st_size)
            if not self._force and self._cacher.get(key) is not None:
                return ("cached", file_stat.st_size)
            content_hash = self._hasher.hash_file(resolved)
            self._cacher.put(key, content_hash)
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

    with SqliteHashCacher(_db_path) as cacher:
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

        visitor = _HashVisitor(cacher, hasher)
        accumulator = _Accumulator()
        _max_pending = max_workers * _PENDING_FACTOR

        pending: dict[Future[tuple[FileOutcome, int]], UPath] = {}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            stack: list[UPath] = [root]
            while stack:
                current = stack.pop()
                try:
                    entries = list(current.iterdir())
                except OSError:
                    logger.warning(
                        "Cannot access directory %s", current, exc_info=True
                    )
                    accumulator.record_directory_error()
                    continue

                for entry in entries:
                    if entry.is_symlink():
                        continue
                    if entry.is_dir():
                        stack.append(entry)
                        continue
                    if not entry.is_file():
                        continue

                    try:
                        resolved = entry.resolve()
                        if resolved in _excluded:
                            continue
                        file_stat = resolved.stat()
                    except OSError:
                        logger.warning(
                            "Cannot stat file %s", entry, exc_info=True
                        )
                        accumulator.record(entry, "error", 0)
                        continue

                    if file_stat.st_size < min_size_bytes:
                        accumulator.record_skipped_small()
                        continue

                    if len(pending) >= _max_pending:
                        done_futures, _ = wait(pending.keys(), return_when=FIRST_COMPLETED)
                        for fut in done_futures:
                            p = pending.pop(fut)
                            outcome, nbytes = fut.result()
                            accumulator.record(p, outcome, nbytes)

                    future = executor.submit(visitor, resolved, file_stat)
                    pending[future] = resolved

            for fut in as_completed(pending):
                p = pending[fut]
                outcome, nbytes = fut.result()
                accumulator.record(p, outcome, nbytes)

    return accumulator.finalize()
