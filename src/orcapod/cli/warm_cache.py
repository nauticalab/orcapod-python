"""``orcapod warm-cache`` subcommand.

Pre-populates the SQLite file-hash cache for large files under a target
directory so that subsequent pipeline runs skip expensive content hashing.
"""

from __future__ import annotations

from pathlib import Path

import typer

_DEFAULT_MIN_SIZE_MB: float = 500.0


def warm_cache(
    path: str = typer.Argument(..., help="Root directory to scan recursively."),
    min_size: float = typer.Option(
        _DEFAULT_MIN_SIZE_MB,
        "--min-size",
        help="Minimum file size in MB. Files smaller than this are skipped. Default: 500 MB.",
        show_default=True,
    ),
    db_path: str | None = typer.Option(
        None,
        "--db-path",
        help=(
            "Path to the SQLite hash-cache database. "
            "Defaults to $ORCAPOD_HASH_CACHE_DB or ~/.orcapod/file_hash_cache.db."
        ),
    ),
    algorithm: str = typer.Option(
        "sha256",
        "--algorithm",
        help="Hash algorithm (sha256, xxh64, md5, …). Default: sha256.",
        show_default=True,
    ),
    buffer_size: int = typer.Option(
        65536,
        "--buffer-size",
        help="Read buffer size in bytes. Default: 65536.",
        show_default=True,
    ),
    max_workers: int = typer.Option(
        4,
        "--workers",
        help="Number of threads for concurrent hashing. Default: 4.",
        show_default=True,
    ),
) -> None:
    """Pre-populate the file-hash cache for large files under PATH.

    Recursively scans PATH and hashes every file that is at least MIN_SIZE MB.
    Files already present in the cache are skipped. On completion, prints a
    summary with counts and throughput.
    """
    from orcapod.hashing.cache_population import populate_hash_cache

    min_size_bytes = int(min_size * 1024 * 1024)
    _db_path: Path | None = Path(db_path) if db_path is not None else None

    if max_workers < 1:
        typer.echo("Error: --workers must be at least 1", err=True)
        raise typer.Exit(code=1)

    root = Path(path)
    if not root.exists():
        typer.echo(f"Error: path does not exist: {path}", err=True)
        raise typer.Exit(code=1)
    if not root.is_dir():
        typer.echo(f"Error: path is not a directory: {path}", err=True)
        raise typer.Exit(code=1)

    typer.echo(f"Scanning {path} ...")

    stats = populate_hash_cache(
        path,
        min_size_bytes=min_size_bytes,
        db_path=_db_path,
        algorithm=algorithm,
        buffer_size=buffer_size,
        max_workers=max_workers,
    )

    gb = stats.total_bytes_hashed / (1024**3)
    speed_gb = stats.avg_hashing_speed / (1024**3)
    min_size_display = f"{min_size:g} MB"

    typer.echo(
        f"Done in {stats.total_duration:.1f}s — "
        f"{stats.hashed} hashed ({gb:.2f} GB), "
        f"{stats.already_cached} already cached, "
        f"{stats.skipped_small} skipped (< {min_size_display}), "
        f"{stats.errors} errors."
    )
    if stats.hashed > 0:
        typer.echo(f"Average hashing speed: {speed_gb:.2f} GB/s")
