"""Tests for migrate_result_v0_to_v1()."""
from __future__ import annotations

import pyarrow as pa
import pytest

from orcapod.databases import InMemoryArrowDatabase
from orcapod.migrations.result_db import migrate_result_v0_to_v1
from orcapod.migrations.types import MigrationResult
from orcapod.system_constants import RESULT_DB_SCHEMA_VERSION
from orcapod.types import ContentHash

_INPUT_HASH = ContentHash("sha256", bytes(range(32)))
_SIG_HASH = ContentHash("sha256", bytes(range(1, 33)))
_CONTENT_HASH = ContentHash("sha256", bytes(range(2, 34)))

# Use a fixed binary record ID (simulating the datagram UUID stored by ResultCache)
_RECORD_ID = b"\xab" * 16


def _make_v0_rdb_row(
    input_hash: ContentHash, sig_hash: ContentHash, content_hash: ContentHash
) -> pa.Table:
    """Build a single v0 rdb row in the old large_string format.

    Note: does NOT include the internal record ID column — that is passed
    separately to ``db.add_record(path, record_id, row)``.
    """
    return pa.table({
        "__input_data_hash": pa.array([input_hash.to_string()], type=pa.large_string()),
        "__pf_var_function_name": pa.array(["my_func"], type=pa.large_string()),
        "__pf_var_function_signature_hash": pa.array([sig_hash.to_string()], type=pa.large_string()),
        "__pf_var_function_content_hash": pa.array([content_hash.to_string()], type=pa.large_string()),
        "__pf_var_git_hash": pa.array(["abc123"], type=pa.large_string()),
        "__pf_exec_executor_type": pa.array(["local"], type=pa.large_string()),
        "__pf_exec_python_version": pa.array(["3.11"], type=pa.large_string()),
        "__pod_ts": pa.array([0], type=pa.timestamp("us", tz="UTC")),
        "result": pa.array([42], type=pa.int64()),
    })


class TestMigrateResultV0ToV1:
    def test_happy_path_migrates_all_rows(self):
        db = InMemoryArrowDatabase()
        v0_path = ("mypod",)
        row = _make_v0_rdb_row(_INPUT_HASH, _SIG_HASH, _CONTENT_HASH)
        db.add_record(v0_path, _RECORD_ID, row)
        db.flush()

        result = migrate_result_v0_to_v1(db, v0_path, progress=False)

        assert result.rows_total == 1
        assert result.rows_migrated == 1
        assert result.rows_skipped == 0
        assert result.rows_unresolvable == 0
        assert result.dry_run is False

    def test_v1_row_has_binary_hash_columns(self):
        db = InMemoryArrowDatabase()
        v0_path = ("mypod",)
        row = _make_v0_rdb_row(_INPUT_HASH, _SIG_HASH, _CONTENT_HASH)
        db.add_record(v0_path, _RECORD_ID, row)
        db.flush()

        migrate_result_v0_to_v1(db, v0_path, progress=False)

        v1_path = v0_path + (RESULT_DB_SCHEMA_VERSION,)
        v1_table = db.get_all_records(v1_path)
        assert v1_table is not None
        assert v1_table.schema.field("__input_data_hash").type == pa.large_binary()
        assert v1_table.schema.field("__pf_var_function_signature_hash").type == pa.large_binary()
        assert v1_table.schema.field("__pf_var_function_content_hash").type == pa.large_binary()
        # git_hash stays as string
        assert v1_table.schema.field("__pf_var_git_hash").type == pa.large_string()

    def test_v1_binary_values_decode_correctly(self):
        db = InMemoryArrowDatabase()
        v0_path = ("mypod",)
        row = _make_v0_rdb_row(_INPUT_HASH, _SIG_HASH, _CONTENT_HASH)
        db.add_record(v0_path, _RECORD_ID, row)
        db.flush()

        migrate_result_v0_to_v1(db, v0_path, progress=False)

        v1_path = v0_path + (RESULT_DB_SCHEMA_VERSION,)
        v1_table = db.get_all_records(v1_path)
        row_dict = v1_table.to_pylist()[0]
        assert ContentHash.from_prefixed_digest(bytes(row_dict["__input_data_hash"])) == _INPUT_HASH
        assert ContentHash.from_prefixed_digest(bytes(row_dict["__pf_var_function_signature_hash"])) == _SIG_HASH
        assert ContentHash.from_prefixed_digest(bytes(row_dict["__pf_var_function_content_hash"])) == _CONTENT_HASH

    def test_idempotent_second_run_skips_all(self):
        db = InMemoryArrowDatabase()
        v0_path = ("mypod",)
        row = _make_v0_rdb_row(_INPUT_HASH, _SIG_HASH, _CONTENT_HASH)
        db.add_record(v0_path, _RECORD_ID, row)
        db.flush()

        migrate_result_v0_to_v1(db, v0_path, progress=False)
        result2 = migrate_result_v0_to_v1(db, v0_path, progress=False)

        assert result2.rows_migrated == 0
        assert result2.rows_skipped == 1

    def test_dry_run_writes_nothing(self):
        db = InMemoryArrowDatabase()
        v0_path = ("mypod",)
        row = _make_v0_rdb_row(_INPUT_HASH, _SIG_HASH, _CONTENT_HASH)
        db.add_record(v0_path, _RECORD_ID, row)
        db.flush()

        result = migrate_result_v0_to_v1(db, v0_path, dry_run=True, progress=False)

        v1_path = v0_path + (RESULT_DB_SCHEMA_VERSION,)
        assert not db.table_exists(v1_path)
        assert result.dry_run is True
        assert result.rows_total == 1
        assert result.rows_migrated == 0

    def test_empty_v0_table_returns_zero_counts(self):
        db = InMemoryArrowDatabase()
        v0_path = ("empty_pod",)

        result = migrate_result_v0_to_v1(db, v0_path, progress=False)

        assert result.rows_total == 0
        assert result.rows_migrated == 0
        assert result.rows_skipped == 0
