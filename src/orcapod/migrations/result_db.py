"""v0 → v1 migration for the Orcapod result DB (rdb)."""
from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import pyarrow as pa

from orcapod.migrations.types import MigrationResult
from orcapod.system_constants import RESULT_DB_SCHEMA_VERSION, constants
from orcapod.types import ContentHash

if TYPE_CHECKING:
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol

logger = logging.getLogger(__name__)

# Column name used to expose the internal record ID when reading from the DB.
_RECORD_ID_COL = "__record_id"

# rdb columns that hold orcapod-produced ContentHash values and must be
# converted from large_string (v0) to large_binary (v1).
_HASH_COLS = (
    constants.INPUT_DATA_HASH_COL,
    f"{constants.PF_VARIATION_PREFIX}function_signature_hash",
    f"{constants.PF_VARIATION_PREFIX}function_content_hash",
)


def migrate_result_v0_to_v1(
    result_db: "ArrowDatabaseProtocol",
    result_path: tuple[str, ...],
    *,
    dry_run: bool = False,
    batch_size: int = 500,
    progress: bool = True,
    track_skipped: bool = True,
) -> MigrationResult:
    """Migrate a result DB table from v0 schema to v1 schema.

    Reads records from ``result_path`` (v0, no suffix), converts all
    orcapod-produced ``ContentHash`` columns from ``large_string`` to
    ``large_binary`` (using ``ContentHash.to_prefixed_digest()``), and writes
    the transformed rows to ``result_path + ("rdb_v1",)``.

    Rows already present at the v1 path are skipped (idempotent re-runs).

    Args:
        result_db: The database containing the v0 table.
        result_path: Bare v0 path tuple (no ``rdb_v1`` suffix).
        dry_run: If ``True``, read and count rows but write nothing.
        batch_size: Number of rows to process per batch.
        progress: If ``True``, log progress at INFO level.
        track_skipped: If ``True`` (default), scan the v1 table upfront to
            detect already-migrated rows (idempotent re-run support).  Set
            to ``False`` to skip this scan on the first run of a large table
            where no v1 rows exist yet; ``rows_skipped`` will be reported as
            ``0`` in that case.

    Returns:
        ``MigrationResult`` summarising the run.
    """
    v1_path = result_path + (RESULT_DB_SCHEMA_VERSION,)
    start = time.monotonic()

    # Read v0 records with their internal UUIDs exposed as _RECORD_ID_COL.
    v0_table = result_db.get_all_records(result_path, record_id_column=_RECORD_ID_COL)
    if v0_table is None or v0_table.num_rows == 0:
        logger.info("No v0 records found at %r — nothing to migrate.", result_path)
        return MigrationResult(
            rows_total=0,
            rows_migrated=0,
            rows_skipped=0,
            rows_unresolvable=0,
            elapsed_s=time.monotonic() - start,
            dry_run=dry_run,
        )

    rows_total = v0_table.num_rows

    # Collect IDs already at v1 for idempotency check (skipped when track_skipped=False).
    existing_ids: set[bytes] = set()
    if track_skipped:
        v1_existing = result_db.get_all_records(v1_path, record_id_column=_RECORD_ID_COL)
        if v1_existing is not None and _RECORD_ID_COL in v1_existing.schema.names:
            existing_ids = {
                bytes(r)
                for r in v1_existing.column(_RECORD_ID_COL).to_pylist()
                if r is not None
            }

    if progress:
        logger.info(
            "migrate_result_v0_to_v1: found %d rows at v0 path %r", rows_total, result_path
        )

    rows_migrated = 0
    rows_skipped = 0

    # Process in batches.
    for batch_start in range(0, rows_total, batch_size):
        batch = v0_table.slice(batch_start, batch_size)

        # Filter rows already at v1.
        if existing_ids and _RECORD_ID_COL in batch.schema.names:
            mask = pa.array(
                [
                    bytes(rid) not in existing_ids
                    for rid in batch.column(_RECORD_ID_COL).to_pylist()
                ],
                type=pa.bool_(),
            )
            new_rows = batch.filter(mask)
            rows_skipped += batch.num_rows - new_rows.num_rows
        else:
            new_rows = batch

        if new_rows.num_rows == 0:
            continue

        # Convert hash columns from string → binary.
        transformed = _convert_hash_cols(new_rows)

        if not dry_run:
            result_db.add_records(
                v1_path,
                transformed,
                record_id_column=_RECORD_ID_COL,
                skip_duplicates=True,
            )
            rows_migrated += new_rows.num_rows
        if progress:
            logger.info(
                "migrate_result_v0_to_v1: %d/%d rows processed",
                min(batch_start + batch_size, rows_total),
                rows_total,
            )

    if not dry_run:
        result_db.flush()

    elapsed = time.monotonic() - start
    return MigrationResult(
        rows_total=rows_total,
        rows_migrated=rows_migrated,
        rows_skipped=rows_skipped,
        rows_unresolvable=0,  # rdb migration has no unresolvable rows
        elapsed_s=elapsed,
        dry_run=dry_run,
    )


def _convert_hash_cols(table: pa.Table) -> pa.Table:
    """Convert all v0 large_string ContentHash columns to v1 large_binary.

    Args:
        table: Arrow table with v0-format hash columns.

    Returns:
        New table with hash columns converted to ``large_binary``.
    """
    for col_name in _HASH_COLS:
        if col_name not in table.schema.names:
            continue
        col_idx = table.schema.names.index(col_name)
        string_vals = table.column(col_name).to_pylist()
        binary_vals = pa.array(
            [
                ContentHash.from_string(s).to_prefixed_digest() if s is not None else None
                for s in string_vals
            ],
            type=pa.large_binary(),
        )
        table = table.set_column(col_idx, col_name, binary_vals)
    return table
