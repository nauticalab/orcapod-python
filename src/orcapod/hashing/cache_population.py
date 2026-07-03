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
