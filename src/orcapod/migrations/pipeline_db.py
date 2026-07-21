"""v0 → v1 migration for the Orcapod pipeline DB (pdb)."""
from __future__ import annotations

import logging
import time
from typing import TYPE_CHECKING, Any

import pyarrow as pa

from orcapod.hashing.defaults import get_default_arrow_hasher
from orcapod.migrations.types import MigrationResult
from orcapod.system_constants import PIPELINE_DB_SCHEMA_VERSION, constants
from orcapod.types import ContentHash

if TYPE_CHECKING:
    from orcapod.protocols.database_protocols import ArrowDatabaseProtocol
    from orcapod.core.nodes.function_node import FunctionJobNode

logger = logging.getLogger(__name__)

# Column name used to expose the internal record ID when reading from the DB.
_RECORD_ID_COL = "__record_id"

# pdb columns whose values are ContentHash strings in v0 and must become binary in v1.
# NODE_CONTENT_HASH_COL is intentionally excluded — it is dropped during migration.
_PDB_HASH_COLS = frozenset({
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
    track_skipped: bool = True,
) -> MigrationResult:
    """Migrate a pipeline DB table from v0 schema to v1 schema.

    Reads records from ``pipeline_path`` (v0, no suffix), converts
    ContentHash columns (``__node_content_hash``, ``__input_data_hash``,
    ``__output_data_hash``) from ``large_string`` to ``large_binary``,
    and writes the transformed rows to ``pipeline_path + ("pdb_v1",)``.

    **Backfill strategy for ``__input_data_hash``:** when the column value is
    ``None`` in the pdb row (older writes may lack it), the migration falls
    back to the rdb via a per-batch ``get_records_by_ids`` lookup keyed by
    ``DATA_RECORD_ID``.  Rows where both the pdb value is ``None`` and the rdb
    lookup fails are counted as ``rows_unresolvable`` and written with a
    ``null`` ``__input_data_hash``.

    **``__output_data_hash`` is not backfilled** — the rdb does not store this
    value, so rows where it is ``None`` in v0 will remain ``null`` in v1
    (these are not counted as unresolvable).

    Rows already present at the v1 path are skipped (idempotent re-runs).

    Args:
        pipeline_db: The database containing the v0 pipeline table.
        pipeline_path: Bare v0 pipeline path tuple (no ``pdb_v1`` suffix).
        result_db: The database containing the v0 result table.
        result_path: Bare v0 result path tuple (no ``rdb_v1`` suffix).
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

    # Collect IDs already at v1 for idempotency (skipped when track_skipped=False).
    existing_ids: set[bytes] = set()
    if track_skipped:
        v1_existing = pipeline_db.get_all_records(v1_path, record_id_column=_RECORD_ID_COL)
        if v1_existing is not None and _RECORD_ID_COL in v1_existing.schema.names:
            existing_ids = {
                bytes(r)
                for r in v1_existing.column(_RECORD_ID_COL).to_pylist()
                if r is not None
            }

    if progress:
        logger.info(
            "migrate_pipeline_v0_to_v1: found %d rows at v0 path %r",
            rows_total,
            pipeline_path,
        )

    rows_migrated = 0
    rows_skipped = 0
    rows_unresolvable = 0
    arrow_hasher = get_default_arrow_hasher()

    for batch_start in range(0, rows_total, batch_size):
        batch = v0_table.slice(batch_start, batch_size)

        # Build a per-batch rdb index for rows where __input_data_hash is None.
        # Uses get_records_by_ids to avoid loading the entire rdb into memory.
        input_hash_col = constants.INPUT_DATA_HASH_COL
        data_id_col = constants.DATA_RECORD_ID
        needed_ids: set[bytes] = set()
        for row in batch.to_pylist():
            if row.get(input_hash_col) is None:
                data_id = row.get(data_id_col)
                if data_id is not None:
                    needed_ids.add(bytes(data_id))

        rdb_index: dict[bytes, dict] = {}
        if needed_ids:
            rdb_batch = result_db.get_records_by_ids(
                result_path, needed_ids, record_id_column=_RECORD_ID_COL
            )
            if rdb_batch is not None:
                for row in rdb_batch.to_pylist():
                    rid = row.get(_RECORD_ID_COL)
                    if rid is not None:
                        rdb_index[bytes(rid)] = row

        transformed, batch_unresolvable = _transform_pdb_batch(batch, rdb_index, arrow_hasher)
        rows_unresolvable += batch_unresolvable

        # Skip rows whose recomputed record ID is already at v1 (idempotent re-runs).
        # The recomputed ID (not the original v0 ID) is the authoritative key in v1.
        if existing_ids and _RECORD_ID_COL in transformed.schema.names:
            mask = pa.array(
                [
                    (rid is None or bytes(rid) not in existing_ids)
                    for rid in transformed.column(_RECORD_ID_COL).to_pylist()
                ],
                type=pa.bool_(),
            )
            new_rows = transformed.filter(mask)
            rows_skipped += transformed.num_rows - new_rows.num_rows
        else:
            new_rows = transformed

        if new_rows.num_rows == 0:
            continue

        if not dry_run:
            pipeline_db.add_records(
                v1_path,
                new_rows,
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
    arrow_hasher: Any,
) -> tuple[pa.Table, int]:
    """Transform a batch of v0 pdb rows into v1 format.

    Per-row transformations (applied in order):

    1. Drop ``__node_content_hash`` — not stored in v1.
    2. Convert ``__input_data_hash`` from ``large_string`` → ``large_binary``
       (falls back to rdb index when the pdb value is ``None``).
    3. Convert ``__output_data_hash`` from ``large_string`` → ``large_binary``
       (no rdb fallback; ``None`` stays ``null``).
    4. Recompute ``__pipeline_base_entry_id`` and ``__record_id`` using the new
       preimage (``system_tag_cols + INPUT_DATA_HASH_COL``, without
       ``NODE_CONTENT_HASH_COL``).  Rows where ``__input_data_hash`` cannot be
       resolved are counted as unresolvable and their hash columns are left
       ``null``.

    Args:
        batch: Arrow table slice of v0 pdb rows (with ``_RECORD_ID_COL`` as first
            column, as returned by ``get_all_records(record_id_column=...)``).
        rdb_index: Dict mapping rdb record-ID bytes to row dicts.
        arrow_hasher: ``ArrowHasherProtocol`` used for recomputing the hash columns.

    Returns:
        Tuple of (transformed Arrow table, count of unresolvable rows).
    """
    node_hash_col = constants.NODE_CONTENT_HASH_COL
    input_hash_col = constants.INPUT_DATA_HASH_COL
    output_hash_col = constants.OUTPUT_DATA_HASH_COL
    data_id_col = constants.DATA_RECORD_ID
    sys_tag_prefix = constants.SYSTEM_TAG_PREFIX

    rows = batch.to_pylist()
    unresolvable = 0
    out_rows: list[dict] = []

    for row in rows:
        new_row = dict(row)

        # 1. Drop __node_content_hash — not stored in v1.
        new_row.pop(node_hash_col, None)

        # 2. Convert __input_data_hash from string → binary.
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

        # 3. Convert __output_data_hash from string → binary.
        val = new_row.get(output_hash_col)
        if val is not None and isinstance(val, str):
            new_row[output_hash_col] = ContentHash.from_string(val).to_prefixed_digest()

        # 4. Recompute __pipeline_base_entry_id and __record_id using new preimage.
        input_hash_bytes = new_row.get(input_hash_col)
        if input_hash_bytes is not None:
            sys_tag_cols = sorted(c for c in row if c.startswith(sys_tag_prefix))
            preimage_arrays: dict[str, pa.Array] = {}
            for col in sys_tag_cols:
                preimage_arrays[col] = pa.array([row.get(col)], type=pa.large_string())
            preimage_arrays[input_hash_col] = pa.array(
                [input_hash_bytes], type=pa.large_binary()
            )
            preimage = pa.table(preimage_arrays)

            new_base_entry_id = arrow_hasher.hash_table(preimage).to_prefixed_digest()
            new_row["__pipeline_base_entry_id"] = new_base_entry_id

            recomp_idx = new_row.get("__pipeline_recomputation_index") or 0
            preimage_with_idx = preimage.append_column(
                "__pipeline_recomputation_index",
                pa.array([recomp_idx], type=pa.int32()),
            )
            new_row[_RECORD_ID_COL] = arrow_hasher.hash_table(preimage_with_idx).to_prefixed_digest()

        out_rows.append(new_row)

    transformed = pa.Table.from_pylist(out_rows, schema=_v1_pdb_schema(batch))
    return transformed, unresolvable


def _v1_pdb_schema(v0_batch: pa.Table) -> pa.Schema:
    """Derive the v1 pdb Arrow schema from a v0 batch.

    Drops ``__node_content_hash`` (removed in pdb_v1) and replaces the two
    remaining ContentHash columns (``__input_data_hash``, ``__output_data_hash``)
    with ``large_binary`` equivalents.  All other columns retain their original
    types.

    Args:
        v0_batch: A v0 pdb Arrow table (used to read non-hash column types).

    Returns:
        Arrow schema for the v1 pdb table.
    """
    node_hash_col = constants.NODE_CONTENT_HASH_COL
    fields = []
    for field in v0_batch.schema:
        if field.name == node_hash_col:
            continue  # Drop __node_content_hash from v1.
        if field.name in _PDB_HASH_COLS:
            fields.append(pa.field(field.name, pa.large_binary(), nullable=True))
        else:
            fields.append(field)
    # Ensure the remaining hash columns exist even if absent in v0.
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

    The ``result_path`` used is the unversioned v0 path (``base_record_path``),
    NOT the versioned v1 path returned by ``record_path``.

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
        # Use the public base_record_path (unversioned v0 path being migrated FROM),
        # not the versioned record_path (which points to the v1 destination).
        result_path=cached_pod._cache.base_record_path,
        dry_run=dry_run,
        batch_size=batch_size,
        progress=progress,
    )
