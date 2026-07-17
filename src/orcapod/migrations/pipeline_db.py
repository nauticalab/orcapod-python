"""v0 → v1 migration for the Orcapod pipeline DB (pdb)."""
from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING

import pyarrow as pa

from orcapod.migrations.types import MigrationResult
from orcapod.system_constants import PIPELINE_DB_SCHEMA_VERSION, constants
from orcapod.types import ContentHash

if TYPE_CHECKING:
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
    from orcapod.core.nodes.function_node import FunctionJobNode

logger = logging.getLogger(__name__)

# Column name used to expose the internal record ID when reading from the DB.
_RECORD_ID_COL = "__record_id"

# pdb columns whose values are ContentHash strings in v0 and must become binary.
_PDB_HASH_COLS = frozenset({
    constants.NODE_CONTENT_HASH_COL,
    constants.INPUT_DATA_HASH_COL,
    constants.OUTPUT_DATA_HASH_COL,
})


def migrate_pipeline_v0_to_v1(
    pipeline_db: "ArrowDatabaseProtocol",
    pipeline_path: tuple[str, ...],
    result_db: "ArrowDatabaseProtocol",
    result_path: tuple[str, ...],
    *,
    dry_run: bool = False,
    batch_size: int = 500,
    progress: bool = True,
) -> MigrationResult:
    """Migrate a pipeline DB table from v0 schema to v1 schema.

    Reads records from ``pipeline_path`` (v0, no suffix), converts
    ContentHash columns (``__node_content_hash``, ``__input_data_hash``,
    ``__output_data_hash``) from ``large_string`` to ``large_binary``,
    and writes the transformed rows to ``pipeline_path + ("pdb_v1",)``.

    Rows whose result data cannot be found (e.g. ephemeral results that have
    since expired) are written with ``null`` hash values and counted as
    ``rows_unresolvable``.

    Rows already present at the v1 path are skipped (idempotent re-runs).

    Args:
        pipeline_db: The database containing the v0 pipeline table.
        pipeline_path: Bare v0 pipeline path tuple (no ``pdb_v1`` suffix).
        result_db: The database containing the v0 result table.
        result_path: Bare v0 result path tuple (no ``rdb_v1`` suffix).
        dry_run: If ``True``, read and count rows but write nothing.
        batch_size: Number of rows to process per batch.
        progress: If ``True``, log progress at INFO level.

    Returns:
        ``MigrationResult`` summarising the run.
    """
    v1_path = pipeline_path + (PIPELINE_DB_SCHEMA_VERSION,)
    start = time.monotonic()

    # Read v0 records with their internal UUIDs exposed as _RECORD_ID_COL.
    v0_table = pipeline_db.get_all_records(pipeline_path, record_id_column=_RECORD_ID_COL)
    if v0_table is None or v0_table.num_rows == 0:
        logger.info("No v0 records found at %r — nothing to migrate.", pipeline_path)
        return MigrationResult(
            rows_total=0,
            rows_migrated=0,
            rows_skipped=0,
            rows_unresolvable=0,
            elapsed_s=time.monotonic() - start,
            dry_run=dry_run,
        )

    rows_total = v0_table.num_rows

    # Collect IDs already at v1 for idempotency.
    v1_existing = pipeline_db.get_all_records(v1_path, record_id_column=_RECORD_ID_COL)
    existing_ids: set[bytes] = set()
    if v1_existing is not None and _RECORD_ID_COL in v1_existing.schema.names:
        existing_ids = {
            bytes(r)
            for r in v1_existing.column(_RECORD_ID_COL).to_pylist()
            if r is not None
        }

    # Build an index from rdb v0: data_id (bytes) → row dict.
    # The rdb record ID (internal UUID) equals the DATA_RECORD_ID stored in
    # the pdb (both set to output_data.datagram_uuid.bytes at write time).
    rdb_v0 = result_db.get_all_records(result_path, record_id_column=_RECORD_ID_COL)
    rdb_index: dict[bytes, dict] = {}
    if rdb_v0 is not None:
        for row in rdb_v0.to_pylist():
            rid = row.get(_RECORD_ID_COL)
            if rid is not None:
                rdb_index[bytes(rid)] = row

    if progress:
        logger.info(
            "migrate_pipeline_v0_to_v1: found %d rows at v0 path %r",
            rows_total,
            pipeline_path,
        )

    rows_migrated = 0
    rows_skipped = 0
    rows_unresolvable = 0

    for batch_start in range(0, rows_total, batch_size):
        batch = v0_table.slice(batch_start, batch_size)

        # Skip rows already at v1.
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

        transformed, batch_unresolvable = _transform_pdb_batch(new_rows, rdb_index)
        rows_unresolvable += batch_unresolvable

        if not dry_run:
            pipeline_db.add_records(
                v1_path,
                transformed,
                record_id_column=_RECORD_ID_COL,
                skip_duplicates=True,
            )
            rows_migrated += new_rows.num_rows
        if progress:
            logger.info(
                "migrate_pipeline_v0_to_v1: %d/%d rows processed",
                min(batch_start + batch_size, rows_total),
                rows_total,
            )

    if not dry_run:
        pipeline_db.flush()

    elapsed = time.monotonic() - start
    return MigrationResult(
        rows_total=rows_total,
        rows_migrated=rows_migrated,
        rows_skipped=rows_skipped,
        rows_unresolvable=rows_unresolvable,
        elapsed_s=elapsed,
        dry_run=dry_run,
    )


def _transform_pdb_batch(
    batch: pa.Table,
    rdb_index: dict[bytes, dict],
) -> tuple[pa.Table, int]:
    """Transform a batch of v0 pdb rows into v1 format.

    Converts ContentHash columns from ``large_string`` to ``large_binary``.
    For ``__input_data_hash`` and ``__output_data_hash``, first tries to
    re-encode directly from the pdb row (most cases), falling back to the
    rdb index when the value is missing.  Rows whose ``DATA_RECORD_ID``
    cannot be found in the rdb index are marked unresolvable and written
    with ``null`` hash values.

    Args:
        batch: Arrow table slice of v0 pdb rows (with ``_RECORD_ID_COL``
            as first column, as returned by ``get_all_records(record_id_column=...)``.
        rdb_index: Dict mapping rdb record-ID bytes to row dicts (keyed by
            the ``_RECORD_ID_COL`` value, which equals ``DATA_RECORD_ID``
            in normal operation).

    Returns:
        Tuple of (transformed Arrow table, count of unresolvable rows).
    """
    node_hash_col = constants.NODE_CONTENT_HASH_COL
    input_hash_col = constants.INPUT_DATA_HASH_COL
    output_hash_col = constants.OUTPUT_DATA_HASH_COL
    data_id_col = constants.DATA_RECORD_ID

    rows = batch.to_pylist()
    unresolvable = 0
    out_rows: list[dict] = []

    for row in rows:
        new_row = dict(row)

        # Convert __node_content_hash from string → binary.
        val = new_row.get(node_hash_col)
        if val is not None and isinstance(val, str):
            new_row[node_hash_col] = ContentHash.from_string(val).to_prefixed_digest()

        # Convert __input_data_hash from string → binary (from pdb row directly).
        val = new_row.get(input_hash_col)
        if val is not None and isinstance(val, str):
            new_row[input_hash_col] = ContentHash.from_string(val).to_prefixed_digest()
        elif val is None:
            # Missing in pdb — fall back to rdb index.
            data_id = row.get(data_id_col)
            if data_id is not None:
                data_id_bytes = bytes(data_id)
                rdb_row = rdb_index.get(data_id_bytes)
                if rdb_row is not None:
                    raw = rdb_row.get(input_hash_col)
                    if raw is not None and isinstance(raw, str):
                        new_row[input_hash_col] = ContentHash.from_string(raw).to_prefixed_digest()
                else:
                    unresolvable += 1

        # Convert __output_data_hash from string → binary (from pdb row directly).
        val = new_row.get(output_hash_col)
        if val is not None and isinstance(val, str):
            new_row[output_hash_col] = ContentHash.from_string(val).to_prefixed_digest()

        out_rows.append(new_row)

    # Rebuild Arrow table with corrected column types.
    transformed = pa.Table.from_pylist(out_rows, schema=_v1_pdb_schema(batch))
    return transformed, unresolvable


def _v1_pdb_schema(v0_batch: pa.Table) -> pa.Schema:
    """Derive the v1 pdb Arrow schema from a v0 batch.

    Replaces the three ContentHash columns with ``large_binary`` equivalents;
    all other columns retain their original types.

    Args:
        v0_batch: A v0 pdb Arrow table (used to read non-hash column types).

    Returns:
        Arrow schema for the v1 pdb table.
    """
    fields = []
    for field in v0_batch.schema:
        if field.name in _PDB_HASH_COLS:
            fields.append(pa.field(field.name, pa.large_binary(), nullable=True))
        else:
            fields.append(field)
    # Ensure the hash columns exist even if absent in v0.
    existing_names = {f.name for f in fields}
    for col in _PDB_HASH_COLS:
        if col not in existing_names:
            fields.append(pa.field(col, pa.large_binary(), nullable=True))
    return pa.schema(fields)


def migrate_node(
    node: "FunctionJobNode",
    *,
    dry_run: bool = False,
    batch_size: int = 500,
    progress: bool = True,
) -> MigrationResult:
    """Convenience wrapper: migrate a single ``FunctionJobNode``'s pipeline DB.

    Extracts the pipeline DB, pipeline path, result DB, and result path
    directly from the node and delegates to ``migrate_pipeline_v0_to_v1()``.

    The ``result_path`` used is the unversioned v0 path (``_cache._record_path``),
    NOT the versioned v1 path returned by ``_cache.record_path``.

    Args:
        node: The ``FunctionJobNode`` whose pipeline DB to migrate.
        dry_run: If ``True``, read and count rows but write nothing.
        batch_size: Rows to process per batch.
        progress: If ``True``, log progress at INFO level.

    Returns:
        ``MigrationResult`` summarising the run.

    Raises:
        RuntimeError: If the node has no pipeline database attached.
    """
    if node._pipeline_database is None:
        raise RuntimeError(
            f"Node {node.label!r} has no pipeline database — cannot migrate."
        )
    cached_pod = node._cached_function_pod
    if cached_pod is None:
        raise RuntimeError(
            f"Node {node.label!r} has no cached function pod — cannot locate result DB."
        )
    return migrate_pipeline_v0_to_v1(
        pipeline_db=node._pipeline_database,
        pipeline_path=node.node_identity_path,
        result_db=cached_pod._cache.result_database,
        # Use the unversioned _record_path (v0 path being migrated FROM),
        # not the versioned record_path (which points to the v1 destination).
        result_path=cached_pod._cache._record_path,
        dry_run=dry_run,
        batch_size=batch_size,
        progress=progress,
    )
