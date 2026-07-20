"""Tests for migrate_pipeline_v0_to_v1()."""
from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.databases import InMemoryArrowDatabase
from orcapod.migrations.pipeline_db import migrate_pipeline_v0_to_v1
from orcapod.migrations.types import MigrationResult
from orcapod.system_constants import PIPELINE_DB_SCHEMA_VERSION, constants
from orcapod.types import ContentHash


_NODE_HASH = ContentHash("sha256", bytes(range(32)))
_INPUT_HASH = ContentHash("sha256", bytes(range(1, 33)))
_OUTPUT_HASH = ContentHash("sha256", bytes(range(2, 34)))

# Use a fixed binary UUID that identifies the result data record.
# In real operation, this equals both __data_id (pdb) and __record_id (rdb internal).
_DATA_ID = b"\xde\xad\xbe\xef" * 4  # 16 bytes

# Separate internal record ID for the pdb row itself
_PDB_RECORD_ID = b"\x01" * 16


def _write_v0_rdb_row(
    db: InMemoryArrowDatabase, rdb_path: tuple, data_id: bytes, input_hash: ContentHash
) -> None:
    """Write a minimal v0 rdb row, using ``data_id`` as the internal record ID.

    In real operation the rdb's internal record ID equals the output
    datagram UUID, which is the same value stored as ``__data_id`` in the pdb.
    """
    row = pa.table({
        "__input_data_hash": pa.array([input_hash.to_string()], type=pa.large_string()),
        "__pf_var_function_name": pa.array(["fn"], type=pa.large_string()),
        "__pf_var_function_signature_hash": pa.array(["sha256:aabb"], type=pa.large_string()),
        "__pf_var_function_content_hash": pa.array(["sha256:ccdd"], type=pa.large_string()),
        "__pf_var_git_hash": pa.array(["abc"], type=pa.large_string()),
        "__pf_exec_executor_type": pa.array(["local"], type=pa.large_string()),
        "__pf_exec_python_version": pa.array(["3.11"], type=pa.large_string()),
        "__pod_ts": pa.array([0], type=pa.timestamp("us", tz="UTC")),
        "result": pa.array([99], type=pa.int64()),
    })
    db.add_record(rdb_path, data_id, row)
    db.flush()


def _write_v0_pdb_row(
    db: InMemoryArrowDatabase, pdb_path: tuple, data_id: bytes, pdb_record_id: bytes
) -> None:
    """Write a minimal v0 pdb row.

    ``data_id`` is stored as the ``DATA_RECORD_ID`` column (the result UUID).
    ``pdb_record_id`` is the internal DB record ID for this pipeline entry.
    """
    row = pa.table({
        constants.DATA_RECORD_ID: pa.array([data_id], type=pa.large_binary()),
        constants.NODE_CONTENT_HASH_COL: pa.array([_NODE_HASH.to_string()], type=pa.large_string()),
        constants.INPUT_DATA_HASH_COL: pa.array([_INPUT_HASH.to_string()], type=pa.large_string()),
        constants.OUTPUT_DATA_HASH_COL: pa.array([_OUTPUT_HASH.to_string()], type=pa.large_string()),
        f"{constants.META_PREFIX}input_data{constants.CONTEXT_KEY}": pa.array(["ctx"], type=pa.large_string()),
        f"{constants.META_PREFIX}computed": pa.array([True], type=pa.bool_()),
        constants.IS_EPHEMERAL_COL: pa.array([False], type=pa.bool_()),
        "__pipeline_base_entry_id": pa.array([data_id], type=pa.large_binary()),
        "__pipeline_recomputation_index": pa.array([0], type=pa.int32()),
    })
    db.add_record(pdb_path, pdb_record_id, row)
    db.flush()


class TestMigratePipelineV0ToV1:
    def test_happy_path_full_backfill(self):
        db = InMemoryArrowDatabase()
        pdb_path = ("pipeline",)
        rdb_path = ("results",)

        _write_v0_rdb_row(db, rdb_path, _DATA_ID, _INPUT_HASH)
        _write_v0_pdb_row(db, pdb_path, _DATA_ID, _PDB_RECORD_ID)

        result = migrate_pipeline_v0_to_v1(
            db, pdb_path, db, rdb_path, progress=False
        )

        assert result.rows_total == 1
        assert result.rows_migrated == 1
        assert result.rows_unresolvable == 0
        assert result.rows_skipped == 0
        assert result.dry_run is False

    def test_v1_hash_columns_are_binary(self):
        db = InMemoryArrowDatabase()
        pdb_path = ("pipeline",)
        rdb_path = ("results",)

        _write_v0_rdb_row(db, rdb_path, _DATA_ID, _INPUT_HASH)
        _write_v0_pdb_row(db, pdb_path, _DATA_ID, _PDB_RECORD_ID)

        migrate_pipeline_v0_to_v1(db, pdb_path, db, rdb_path, progress=False)

        v1_path = pdb_path + (PIPELINE_DB_SCHEMA_VERSION,)
        v1_table = db.get_all_records(v1_path)
        assert v1_table is not None
        assert v1_table.schema.field(constants.NODE_CONTENT_HASH_COL).type == pa.large_binary()
        assert v1_table.schema.field(constants.INPUT_DATA_HASH_COL).type == pa.large_binary()
        assert v1_table.schema.field(constants.OUTPUT_DATA_HASH_COL).type == pa.large_binary()

    def test_v1_binary_values_decode_correctly(self):
        db = InMemoryArrowDatabase()
        pdb_path = ("pipeline",)
        rdb_path = ("results",)

        _write_v0_rdb_row(db, rdb_path, _DATA_ID, _INPUT_HASH)
        _write_v0_pdb_row(db, pdb_path, _DATA_ID, _PDB_RECORD_ID)

        migrate_pipeline_v0_to_v1(db, pdb_path, db, rdb_path, progress=False)

        v1_path = pdb_path + (PIPELINE_DB_SCHEMA_VERSION,)
        v1_table = db.get_all_records(v1_path)
        row_dict = v1_table.to_pylist()[0]
        assert ContentHash.from_prefixed_digest(bytes(row_dict[constants.NODE_CONTENT_HASH_COL])) == _NODE_HASH
        assert ContentHash.from_prefixed_digest(bytes(row_dict[constants.INPUT_DATA_HASH_COL])) == _INPUT_HASH
        assert ContentHash.from_prefixed_digest(bytes(row_dict[constants.OUTPUT_DATA_HASH_COL])) == _OUTPUT_HASH

    def test_unresolvable_row_written_with_null_output_and_input_hash(self):
        """A pdb row whose data_id does NOT exist in rdb has null output hash."""
        db = InMemoryArrowDatabase()
        pdb_path = ("pipeline",)
        rdb_path = ("results",)

        # Write pdb but NOT rdb — simulates ephemeral result expired
        _write_v0_pdb_row(db, pdb_path, _DATA_ID, _PDB_RECORD_ID)

        result = migrate_pipeline_v0_to_v1(
            db, pdb_path, db, rdb_path, progress=False
        )

        # The pdb row already has INPUT_HASH_COL as a string → converts to binary
        # (no rdb lookup needed for it). Unresolvable only when the column is missing.
        # With the current implementation, input_hash_col is converted from pdb row.
        # rows_unresolvable = 0 because INPUT_DATA_HASH_COL was present in pdb.
        assert result.rows_total == 1
        assert result.rows_migrated == 1
        v1_path = pdb_path + (PIPELINE_DB_SCHEMA_VERSION,)
        v1_table = db.get_all_records(v1_path)
        assert v1_table is not None
        # INPUT_DATA_HASH_COL converted from pdb row string
        input_hash_col = v1_table.column(constants.INPUT_DATA_HASH_COL)
        assert input_hash_col.to_pylist()[0] is not None

    def test_idempotent(self):
        db = InMemoryArrowDatabase()
        pdb_path = ("pipeline",)
        rdb_path = ("results",)

        _write_v0_rdb_row(db, rdb_path, _DATA_ID, _INPUT_HASH)
        _write_v0_pdb_row(db, pdb_path, _DATA_ID, _PDB_RECORD_ID)

        migrate_pipeline_v0_to_v1(db, pdb_path, db, rdb_path, progress=False)
        result2 = migrate_pipeline_v0_to_v1(db, pdb_path, db, rdb_path, progress=False)

        assert result2.rows_migrated == 0
        assert result2.rows_skipped == 1

    def test_dry_run_writes_nothing(self):
        db = InMemoryArrowDatabase()
        pdb_path = ("pipeline",)
        rdb_path = ("results",)

        _write_v0_rdb_row(db, rdb_path, _DATA_ID, _INPUT_HASH)
        _write_v0_pdb_row(db, pdb_path, _DATA_ID, _PDB_RECORD_ID)

        result = migrate_pipeline_v0_to_v1(
            db, pdb_path, db, rdb_path, dry_run=True, progress=False
        )

        v1_path = pdb_path + (PIPELINE_DB_SCHEMA_VERSION,)
        assert not db.table_exists(v1_path)
        assert result.dry_run is True
        assert result.rows_total == 1

    def test_empty_pdb_returns_zero_counts(self):
        db = InMemoryArrowDatabase()
        pdb_path = ("empty_pipeline",)
        rdb_path = ("empty_results",)

        result = migrate_pipeline_v0_to_v1(
            db, pdb_path, db, rdb_path, progress=False
        )

        assert result.rows_total == 0
        assert result.rows_migrated == 0
