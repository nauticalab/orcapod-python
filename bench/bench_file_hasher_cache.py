#!/usr/bin/env python
"""Benchmark: cached vs uncached file hashing.

Creates a ≥100 MB temp file, then times three scenarios:
  1. Uncached hash (baseline)
  2. First cached hash (miss — hash computed + stored)
  3. Second cached hash (hit — lookup only, should be sub-millisecond)

Run: uv run python bench/bench_file_hasher_cache.py
"""

import tempfile
import time
from pathlib import Path

from orcapod.hashing.file_hashers import CachedFileHasher, FileHasher
from orcapod.hashing.hash_cachers import SqliteHashCacher

FILE_SIZE_MB = 100
ITERATIONS = 3


def create_large_file(path: Path, size_mb: int) -> None:
    chunk = b"\x00" * (1024 * 1024)
    with open(path, "wb") as f:
        for _ in range(size_mb):
            f.write(chunk)


def time_call(fn, *args) -> tuple[object, float]:
    t0 = time.perf_counter()
    result = fn(*args)
    elapsed = time.perf_counter() - t0
    return result, elapsed


def main() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        large_file = tmp / "bench_input.bin"
        db_path = tmp / "bench_cache.db"

        print(f"Creating {FILE_SIZE_MB} MB temp file...")
        create_large_file(large_file, FILE_SIZE_MB)

        base_hasher = FileHasher(algorithm="sha256")

        # --- Uncached baseline ---
        _, uncached_time = time_call(base_hasher.hash_file, large_file)

        # --- Cached: miss ---
        cached_hasher = CachedFileHasher(
            file_hasher=base_hasher,
            cacher=SqliteHashCacher(db_path),
        )
        _, miss_time = time_call(cached_hasher.hash_file, large_file)

        # --- Cached: hit (multiple times) ---
        hit_times = []
        for _ in range(ITERATIONS):
            _, t = time_call(cached_hasher.hash_file, large_file)
            hit_times.append(t)
        avg_hit = sum(hit_times) / len(hit_times)

        print()
        print(f"{'Scenario':<30} {'Time (ms)':>12}")
        print("-" * 44)
        print(f"{'Uncached (baseline)':<30} {uncached_time * 1000:>12.2f}")
        print(f"{'Cached miss (1st call)':<30} {miss_time * 1000:>12.2f}")
        print(f"{'Cached hit (avg of 3)':<30} {avg_hit * 1000:>12.3f}")
        print()
        speedup = uncached_time / avg_hit if avg_hit > 0 else float("inf")
        print(f"Cache hit speedup: {speedup:.0f}x")
        if avg_hit < 0.001:
            print("✓ Sub-millisecond cache hit achieved")
        else:
            print(f"! Cache hit is {avg_hit * 1000:.2f} ms — expected < 1 ms")


if __name__ == "__main__":
    main()
