"""SQLite hash cache schema migration script.

Upgrades a ``file_hash_cache`` SQLite database from schema V0 (no
``cached_at`` column) to schema V1 (``cached_at`` column added).

Usage::

    python -m orcapod.hashing.migrate_hash_cache /path/to/cache.db

Or from Python code::

    from orcapod.hashing.migrate_hash_cache import migrate_sqlite_hash_cache
    migrate_sqlite_hash_cache("/path/to/cache.db")

What the migration does:

* Adds a ``cached_at INTEGER NOT NULL DEFAULT 0`` column to the
  ``file_hash_cache`` table (existing rows receive ``cached_at = 0``).
* Sets ``PRAGMA user_version = 1`` to stamp the new schema version.

The migration is idempotent: running it on an already-migrated database
prints a message and exits without making changes.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


def migrate_sqlite_hash_cache(db_path: "Path | str") -> None:
    """Upgrade a SQLite hash cache database from schema V0 to V1.

    Adds the ``cached_at`` column (epoch seconds; defaults to ``0`` for
    existing rows) and stamps ``PRAGMA user_version = 1``.

    Args:
        db_path: Path to the SQLite database file to migrate.

    Raises:
        FileNotFoundError: If ``db_path`` does not exist.
        ValueError: If the database does not contain the expected
            ``file_hash_cache`` table.
    """
    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    with sqlite3.connect(db_path) as conn:
        # Check whether the table exists at all.
        table_exists = conn.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type='table' AND name='file_hash_cache'"
        ).fetchone()
        if not table_exists:
            raise ValueError(
                f"No 'file_hash_cache' table found in {db_path}. "
                "Is this an orcapod hash cache database?"
            )

        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version >= 1:
            print(
                f"{db_path}: already at schema V{version} — nothing to migrate."
            )
            return

        columns = {
            row[1] for row in conn.execute("PRAGMA table_info(file_hash_cache)")
        }
        if "cached_at" in columns:
            # Table already has cached_at (was created with current code before
            # version stamping was introduced).  Just stamp the version.
            conn.execute("PRAGMA user_version = 1")
            conn.commit()
            print(
                f"{db_path}: 'cached_at' column already present; "
                "stamped schema version to V1."
            )
            return

        print(f"{db_path}: migrating from schema V0 → V1 …")
        conn.execute(
            "ALTER TABLE file_hash_cache "
            "ADD COLUMN cached_at INTEGER NOT NULL DEFAULT 0"
        )
        conn.execute("PRAGMA user_version = 1")
        conn.commit()
        print(
            f"{db_path}: migration complete. "
            "Existing rows have cached_at = 0 (epoch origin)."
        )


def main() -> None:
    """Entry point for ``python -m orcapod.hashing.migrate_hash_cache``."""
    parser = argparse.ArgumentParser(
        prog="python -m orcapod.hashing.migrate_hash_cache",
        description=(
            "Upgrade an orcapod SQLite hash cache database "
            "from schema V0 to V1 (adds the cached_at column)."
        ),
    )
    parser.add_argument(
        "db_path",
        metavar="DB_PATH",
        help="Path to the SQLite hash cache database file.",
    )
    args = parser.parse_args()
    migrate_sqlite_hash_cache(args.db_path)


if __name__ == "__main__":
    main()
